from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
HTML_PATH = REPORT_DIR / "btc_1h_ma55_ema55_duration_study.html"


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)

    line_results: dict[str, dict[str, object]] = {}
    for line_col, label in (("sma55", "55MA"), ("ema55", "55EMA")):
        cycles = build_cycles(df, line_col, label)
        cycles.to_csv(REPORT_DIR / f"btc_1h_{label.lower()}_cycle_durations.csv", index=False, encoding="utf-8-sig")

        quantile_stats, threshold_stats, bucket_backtest = analyze_cycles(cycles)
        quantile_stats.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_duration_quantiles.csv",
            index=False,
            encoding="utf-8-sig",
        )
        threshold_stats.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_duration_thresholds.csv",
            index=False,
            encoding="utf-8-sig",
        )
        bucket_backtest.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_duration_backtest.csv",
            index=False,
            encoding="utf-8-sig",
        )

        hist_path = REPORT_DIR / f"btc_1h_{label.lower()}_duration_hist.png"
        save_histogram(cycles, label, hist_path)

        line_results[label] = {
            "cycles": cycles,
            "quantiles": quantile_stats,
            "thresholds": threshold_stats,
            "backtest": bucket_backtest,
            "hist_path": hist_path,
            "summary": summarize_line(cycles, quantile_stats, bucket_backtest),
        }

    payload = {
        label: {"summary": result["summary"]}
        for label, result in line_results.items()
    }
    (REPORT_DIR / "btc_1h_ma55_ema55_duration_study.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    HTML_PATH.write_text(build_html(line_results), encoding="utf-8")
    print(HTML_PATH)


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "timestamp": pd.to_datetime(int(c.ts), unit="ms", utc=True),
            "open": float(c.open),
            "high": float(c.high),
            "low": float(c.low),
            "close": float(c.close),
            "volume": float(c.volume),
        }
        for c in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["sma55"] = df["close"].rolling(55, min_periods=55).mean()
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()

    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14, min_periods=14).mean()


def build_cycles(df: pd.DataFrame, line_col: str, label: str) -> pd.DataFrame:
    cross_above = (df["close"].shift(1) <= df[line_col].shift(1)) & (df["close"] > df[line_col]) & df[line_col].notna()
    cross_below = (df["close"].shift(1) >= df[line_col].shift(1)) & (df["close"] < df[line_col]) & df[line_col].notna()

    rows: list[dict[str, object]] = []
    breakout_idx: int | None = None
    for idx in range(len(df)):
        if cross_above.iloc[idx]:
            breakout_idx = idx
        if breakout_idx is None:
            continue
        if cross_below.iloc[idx] and idx > breakout_idx:
            duration_bars = idx - breakout_idx + 1
            atr = float(df.at[idx, "atr14"]) if pd.notna(df.at[idx, "atr14"]) else np.nan
            breakdown_close = float(df.at[idx, "close"])
            stop_1r = float(df.at[idx, "high"]) + 1.5 * atr if np.isfinite(atr) else np.nan
            risk = stop_1r - breakdown_close if np.isfinite(stop_1r) else np.nan

            row: dict[str, object] = {
                "line": label,
                "breakout_index": breakout_idx,
                "breakdown_index": idx,
                "breakout_time": df.at[breakout_idx, "timestamp"],
                "breakdown_time": df.at[idx, "timestamp"],
                "duration_bars": duration_bars,
                "duration_hours": duration_bars,
                "breakout_close": float(df.at[breakout_idx, "close"]),
                "breakdown_close": breakdown_close,
                "breakdown_high": float(df.at[idx, "high"]),
                "atr14": atr,
            }

            for h in (4, 8, 12, 24, 48):
                if idx + h < len(df):
                    row[f"future_{h}h_short_return"] = (breakdown_close - float(df.at[idx + h, "close"])) / breakdown_close
                else:
                    row[f"future_{h}h_short_return"] = np.nan

            if idx + 24 < len(df) and np.isfinite(risk) and risk > 0:
                window = df.iloc[idx + 1 : idx + 25]
                row["first_1r_before_stop_24h"] = first_target_before_stop(
                    window,
                    breakdown_close,
                    stop_1r,
                    breakdown_close - risk,
                )
                row["mfe_24h_r"] = (breakdown_close - float(window["low"].min())) / risk
                row["mae_24h_r"] = (float(window["high"].max()) - breakdown_close) / risk
            else:
                row["first_1r_before_stop_24h"] = np.nan
                row["mfe_24h_r"] = np.nan
                row["mae_24h_r"] = np.nan

            rows.append(row)
            breakout_idx = None

    cycles = pd.DataFrame(rows)
    if cycles.empty:
        return cycles

    quantile_count = min(4, len(cycles))
    cycles["duration_quantile"] = pd.qcut(
        cycles["duration_bars"].rank(method="first"),
        q=quantile_count,
        labels=[f"Q{i}" for i in range(1, quantile_count + 1)],
    )
    return cycles


def first_target_before_stop(window: pd.DataFrame, entry: float, stop: float, target: float) -> float:
    _ = entry
    for row in window.itertuples(index=False):
        if float(row.high) >= stop:
            return 0.0
        if float(row.low) <= target:
            return 1.0
    return np.nan


def analyze_cycles(cycles: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quantile_stats = (
        cycles.groupby("duration_quantile", dropna=False, observed=False)
        .agg(
            count=("duration_bars", "count"),
            min_bars=("duration_bars", "min"),
            median_bars=("duration_bars", "median"),
            max_bars=("duration_bars", "max"),
            mean_4h_short_return=("future_4h_short_return", "mean"),
            mean_8h_short_return=("future_8h_short_return", "mean"),
            mean_12h_short_return=("future_12h_short_return", "mean"),
            mean_24h_short_return=("future_24h_short_return", "mean"),
            down_rate_24h=("future_24h_short_return", lambda s: float((s > 0).mean())),
            first_1r_before_stop_24h=("first_1r_before_stop_24h", "mean"),
            median_mfe_24h_r=("mfe_24h_r", "median"),
            median_mae_24h_r=("mae_24h_r", "median"),
        )
        .reset_index()
    )

    thresholds: list[dict[str, object]] = []
    for threshold in (12, 24, 36, 48, 72):
        bucket = cycles[cycles["duration_bars"] >= threshold].copy()
        if bucket.empty:
            continue
        thresholds.append(
            {
                "threshold_bars": threshold,
                "count": int(len(bucket)),
                "count_pct": float(len(bucket) / len(cycles)),
                "mean_24h_short_return": float(bucket["future_24h_short_return"].mean()),
                "down_rate_24h": float((bucket["future_24h_short_return"] > 0).mean()),
                "first_1r_before_stop_24h": float(bucket["first_1r_before_stop_24h"].mean()),
                "median_mfe_24h_r": float(bucket["mfe_24h_r"].median()),
                "median_mae_24h_r": float(bucket["mae_24h_r"].median()),
            }
        )
    threshold_stats = pd.DataFrame(thresholds)

    backtest_rows: list[dict[str, object]] = []
    for label in quantile_stats["duration_quantile"]:
        selected = cycles[cycles["duration_quantile"] == label]
        backtest_rows.append(build_bucket_backtest(str(label), selected))
    for threshold in (24, 36, 48, 72):
        selected = cycles[cycles["duration_bars"] >= threshold]
        if not selected.empty:
            backtest_rows.append(build_bucket_backtest(f">={threshold} bars", selected))

    bucket_backtest = pd.DataFrame(backtest_rows)
    return quantile_stats, threshold_stats, bucket_backtest


def build_bucket_backtest(label: str, cycles: pd.DataFrame) -> dict[str, object]:
    wins = cycles[cycles["first_1r_before_stop_24h"] == 1]
    losses = cycles[cycles["first_1r_before_stop_24h"] == 0]
    gross_profit = float(wins["mfe_24h_r"].clip(upper=1.0).sum())
    gross_loss = float(len(losses))
    pf = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

    return {
        "bucket": label,
        "count": int(len(cycles)),
        "win_rate_1r": float((cycles["first_1r_before_stop_24h"] == 1).mean()),
        "profit_factor_1r": pf,
        "mean_24h_short_return": float(cycles["future_24h_short_return"].mean()),
        "median_mfe_24h_r": float(cycles["mfe_24h_r"].median()),
        "median_mae_24h_r": float(cycles["mae_24h_r"].median()),
    }


def summarize_line(cycles: pd.DataFrame, quantile_stats: pd.DataFrame, backtest: pd.DataFrame) -> dict[str, object]:
    longest = cycles.sort_values("duration_bars", ascending=False).head(10)
    best_quantile = quantile_stats.sort_values("mean_24h_short_return", ascending=False).head(1)
    best_bucket = backtest.sort_values("profit_factor_1r", ascending=False).head(1)

    return {
        "cycle_count": int(len(cycles)),
        "median_duration_bars": float(cycles["duration_bars"].median()),
        "mean_duration_bars": float(cycles["duration_bars"].mean()),
        "p90_duration_bars": float(cycles["duration_bars"].quantile(0.9)),
        "best_quantile": best_quantile.to_dict("records"),
        "best_backtest_bucket": best_bucket.to_dict("records"),
        "longest_cycles": longest[["breakout_time", "breakdown_time", "duration_bars"]].astype(str).to_dict("records"),
    }


def save_histogram(cycles: pd.DataFrame, label: str, path: Path) -> None:
    plt.figure(figsize=(11, 4))
    plt.hist(cycles["duration_bars"], bins=40, color="#2563eb", alpha=0.85)
    plt.title(f"{label} breakout-to-breakdown duration")
    plt.xlabel("Bars from breakout to breakdown")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(line_results: dict[str, dict[str, object]]) -> str:
    cards: list[str] = []
    sections: list[str] = []

    for label, result in line_results.items():
        summary = result["summary"]
        best_quantile = summary["best_quantile"][0] if summary["best_quantile"] else {}
        best_bucket = summary["best_backtest_bucket"][0] if summary["best_backtest_bucket"] else {}

        cards.append(
            kpi(
                f"{label} 周期数",
                f"{summary['cycle_count']}",
                f"中位持续 {summary['median_duration_bars']:.1f} 根，P90 {summary['p90_duration_bars']:.1f} 根",
            )
        )

        sections.append(
            f"""
            <h2>{label} 统计</h2>
            <div class="grid grid-2">
              <div class="card imgbox">
                <h3>持续时长分布</h3>
                <img src="data:image/png;base64,{b64(result['hist_path'])}" alt="{label} histogram">
              </div>
              <div class="card">
                <h3>结论摘录</h3>
                <p>最优分位：<strong>{best_quantile.get('duration_quantile', '-')}</strong>，24H 平均 short 收益 <strong>{pct(best_quantile.get('mean_24h_short_return', 0.0))}</strong>。</p>
                <p>最优阈值档：<strong>{best_bucket.get('bucket', '-')}</strong>，1R 先到率 <strong>{pct(best_bucket.get('win_rate_1r', 0.0))}</strong>，简化 PF <strong>{best_bucket.get('profit_factor_1r', 0.0):.2f}</strong>。</p>
                <p class="note">这里的简化 PF 使用“跌破后下一根开空，止损 = 信号 K 高点 + 1.5 ATR，24 小时内先到 1R 还是先止损”的统计，不是完整仓位回测。</p>
              </div>
            </div>
            <div class="grid grid-2">
              <div class="card">
                <h3>按分位统计</h3>
                {render_table(result['quantiles'], ['duration_quantile','count','min_bars','median_bars','max_bars','mean_24h_short_return','down_rate_24h','first_1r_before_stop_24h','median_mfe_24h_r','median_mae_24h_r'])}
              </div>
              <div class="card">
                <h3>高持续阈值统计</h3>
                {render_table(result['thresholds'], ['threshold_bars','count','count_pct','mean_24h_short_return','down_rate_24h','first_1r_before_stop_24h','median_mfe_24h_r','median_mae_24h_r'])}
              </div>
            </div>
            <div class="card">
              <h3>按时长档的简化做空优势</h3>
              {render_table(result['backtest'], ['bucket','count','win_rate_1r','profit_factor_1r','mean_24h_short_return','median_mfe_24h_r','median_mae_24h_r'])}
            </div>
            <div class="card">
              <h3>最长的 10 段线上行情</h3>
              {render_table(pd.DataFrame(summary['longest_cycles']), ['breakout_time','breakdown_time','duration_bars'])}
            </div>
            """
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 55MA/55EMA 周期时长统计</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#0f172a 0%,#22405f 58%,#3f6c73 100%); color:#fff; padding:36px 40px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#d7e5f5; max-width:1120px; line-height:1.65; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; box-shadow:0 1px 2px rgba(16,24,40,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; }}
.kpi .sub {{ color:var(--muted); margin-top:6px; font-size:13px; }}
h2 {{ font-size:21px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.note {{ color:var(--muted); line-height:1.65; }}
.imgbox img {{ width:100%; display:block; border:1px solid var(--line); border-radius:6px; }}
@media (max-width: 920px) {{
  .grid-4,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:26px 22px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1小时 55MA / 55EMA 周期时长统计</h1>
  <p>这次统计的是：每次行情从“突破 55 线并在线上运行”，一直到“下一次跌破 55 线”为止，中间一共经历了多少根 1H K 线。</p>
  <p>然后把这些持续时长做分层，看高持续时长区间在“跌破后做空”上有没有统计优势。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {''.join(cards)}
  </div>
  <div class="card">
    <h2>统计口径</h2>
    <p>突破：收盘价从 55 线下方或等于 55 线，切到 55 线上方。跌破：收盘价从 55 线上方或等于 55 线，切到 55 线下方。</p>
    <p>持续时长：从突破 K 线到跌破 K 线，按包含首尾两端的根数统计。后面的优势检验采用“跌破后下一根开空”的 short 视角。</p>
  </div>
  {''.join(sections)}
</main>
</body>
</html>"""


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "return" in col or "rate" in col or "pct" in col:
                    text = pct(value)
                else:
                    text = f"{value:.2f}"
            else:
                text = str(value)
            cells.append(f"<td>{text}</td>")
        parts.append("<tr>" + "".join(cells) + "</tr>")
    parts.append("</table>")
    return "".join(parts)


def pct(value: float) -> str:
    return f"{float(value) * 100:.1f}%"


def kpi(label: str, value: str, sub: str) -> str:
    return f'<div class="card kpi"><div class="label">{label}</div><div class="value">{value}</div><div class="sub">{sub}</div></div>'


def b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


if __name__ == "__main__":
    main()
