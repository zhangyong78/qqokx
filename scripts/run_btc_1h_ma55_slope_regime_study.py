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
LOOKBACK = 5
FLAT_THRESHOLD = 0.0003
STRONG_THRESHOLD = 0.0005
ACCEL_BARS = 3
HTML_PATH = REPORT_DIR / "btc_1h_ma55_ema55_slope_regime_study.html"

REGIME_LABELS = {
    "warming_up": "预热",
    "flat": "走平震荡",
    "bull_start": "多头启动",
    "bull_run": "多头推进",
    "bull_fade": "多头衰竭",
    "bear_start": "空头启动",
    "bear_run": "空头推进",
    "bear_fade": "空头衰竭",
    "weak_bear": "弱空头",
}


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)

    line_results: dict[str, dict[str, object]] = {}
    for line_col, label in (("sma55", "55MA"), ("ema55", "55EMA")):
        enriched = enrich_line(df, line_col)
        regime_stats = analyze_regimes(enriched, label)
        transition_stats = analyze_transitions(enriched, label)
        recent = enriched.tail(120).copy()

        regime_stats.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_slope_regime_stats.csv",
            index=False,
            encoding="utf-8-sig",
        )
        transition_stats.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_slope_transition_stats.csv",
            index=False,
            encoding="utf-8-sig",
        )
        recent.to_csv(
            REPORT_DIR / f"btc_1h_{label.lower()}_slope_recent.csv",
            index=False,
            encoding="utf-8-sig",
        )

        return_hist_path = REPORT_DIR / f"btc_1h_{label.lower()}_slope_regime_returns.png"
        regime_hist_path = REPORT_DIR / f"btc_1h_{label.lower()}_slope_regime_dist.png"
        timeline_path = REPORT_DIR / f"btc_1h_{label.lower()}_slope_timeline.png"
        save_return_chart(regime_stats, label, return_hist_path)
        save_regime_dist_chart(enriched, label, regime_hist_path)
        save_timeline_chart(recent, label, timeline_path)

        line_results[label] = {
            "enriched": enriched,
            "regime_stats": regime_stats,
            "transition_stats": transition_stats,
            "return_hist_path": return_hist_path,
            "regime_hist_path": regime_hist_path,
            "timeline_path": timeline_path,
            "summary": summarize_line(enriched, regime_stats, transition_stats),
        }

    agreement = build_agreement_stats(
        line_results["55MA"]["enriched"],
        line_results["55EMA"]["enriched"],
    )
    agreement.to_csv(
        REPORT_DIR / "btc_1h_ma55_ema55_slope_agreement.csv",
        index=False,
        encoding="utf-8-sig",
    )

    payload = {
        label: {"summary": result["summary"]}
        for label, result in line_results.items()
    }
    (REPORT_DIR / "btc_1h_ma55_ema55_slope_regime_study.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    HTML_PATH.write_text(
        build_html(df, line_results, agreement),
        encoding="utf-8",
    )
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


def linear_regression_slope(series: pd.Series) -> float:
    values = series.astype(float).to_numpy()
    if len(values) < 2 or np.isnan(values).any():
        return np.nan
    x = np.arange(len(values), dtype=float)
    x_mean = x.mean()
    y_mean = values.mean()
    numerator = np.sum((x - x_mean) * (values - y_mean))
    denominator = np.sum((x - x_mean) ** 2)
    if denominator == 0:
        return np.nan
    return float(numerator / denominator)


def enrich_line(df: pd.DataFrame, line_col: str) -> pd.DataFrame:
    out = df.copy()
    out["line"] = out[line_col]
    out["slope_raw"] = out["line"].rolling(LOOKBACK, min_periods=LOOKBACK).apply(linear_regression_slope, raw=False)
    out["slope_ratio"] = out["slope_raw"] / out["line"]
    out["slope_ratio_prev"] = out["slope_ratio"].shift(1)
    out["slope_accel"] = out["slope_ratio"] - out["slope_ratio_prev"]
    out["slope_strength"] = (out["line"] - out["line"].shift(LOOKBACK)) / out["atr14"]
    out["delta1"] = out["line"] - out["line"].shift(1)
    out["above_line"] = out["close"] > out["line"]
    out["decel_streak"] = (
        (out["slope_accel"] < 0)
        .groupby((out["slope_accel"] >= 0).cumsum())
        .cumcount()
        + 1
    )
    out.loc[out["slope_accel"] >= 0, "decel_streak"] = 0
    out["accel_up_streak"] = (
        (out["slope_accel"] > 0)
        .groupby((out["slope_accel"] <= 0).cumsum())
        .cumcount()
        + 1
    )
    out.loc[out["slope_accel"] <= 0, "accel_up_streak"] = 0
    out["regime"] = [
        classify_regime(
            slope_ratio=row.slope_ratio,
            slope_ratio_prev=row.slope_ratio_prev,
            decel_streak=int(row.decel_streak) if pd.notna(row.decel_streak) else 0,
            accel_up_streak=int(row.accel_up_streak) if pd.notna(row.accel_up_streak) else 0,
        )
        for row in out.itertuples(index=False)
    ]
    out["regime_label"] = out["regime"].map(REGIME_LABELS)

    for hours in (4, 8, 12, 24):
        out[f"future_{hours}h_long_return"] = out["close"].shift(-hours) / out["close"] - 1
        out[f"future_{hours}h_short_return"] = out["close"] / out["close"].shift(-hours) - 1
    return out


def classify_regime(
    *,
    slope_ratio: float,
    slope_ratio_prev: float,
    decel_streak: int,
    accel_up_streak: int,
) -> str:
    if pd.isna(slope_ratio):
        return "warming_up"
    if pd.notna(slope_ratio_prev):
        if slope_ratio_prev <= 0 < slope_ratio:
            return "bull_start"
        if slope_ratio_prev >= 0 > slope_ratio:
            return "bear_start"
    if abs(slope_ratio) < FLAT_THRESHOLD:
        return "flat"
    if slope_ratio > 0:
        if decel_streak >= ACCEL_BARS:
            return "bull_fade"
        return "bull_run"
    if slope_ratio < -STRONG_THRESHOLD:
        if accel_up_streak >= ACCEL_BARS:
            return "bear_fade"
        return "bear_run"
    return "weak_bear"


def analyze_regimes(df: pd.DataFrame, label: str) -> pd.DataFrame:
    ready = df[df["regime"] != "warming_up"].copy()
    rows: list[dict[str, object]] = []
    for regime, bucket in ready.groupby("regime", sort=False):
        rows.append(
            {
                "line": label,
                "regime": regime,
                "regime_label": REGIME_LABELS.get(str(regime), str(regime)),
                "count": int(len(bucket)),
                "share": float(len(bucket) / len(ready)),
                "mean_slope_ratio": float(bucket["slope_ratio"].mean()),
                "mean_slope_strength": float(bucket["slope_strength"].mean()),
                "above_line_rate": float(bucket["above_line"].mean()),
                "mean_4h_long_return": float(bucket["future_4h_long_return"].mean()),
                "mean_8h_long_return": float(bucket["future_8h_long_return"].mean()),
                "mean_24h_long_return": float(bucket["future_24h_long_return"].mean()),
                "mean_4h_short_return": float(bucket["future_4h_short_return"].mean()),
                "mean_8h_short_return": float(bucket["future_8h_short_return"].mean()),
                "mean_24h_short_return": float(bucket["future_24h_short_return"].mean()),
                "up_rate_24h": float((bucket["future_24h_long_return"] > 0).mean()),
                "down_rate_24h": float((bucket["future_24h_short_return"] > 0).mean()),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    order = [
        "bull_start",
        "bull_run",
        "bull_fade",
        "flat",
        "weak_bear",
        "bear_start",
        "bear_run",
        "bear_fade",
    ]
    frame["regime"] = pd.Categorical(frame["regime"], categories=order, ordered=True)
    return frame.sort_values("regime").reset_index(drop=True)


def analyze_transitions(df: pd.DataFrame, label: str) -> pd.DataFrame:
    ready = df[df["regime"].isin({"bull_start", "bear_start"})].copy()
    rows: list[dict[str, object]] = []
    for regime, bucket in ready.groupby("regime", sort=False):
        rows.append(
            {
                "line": label,
                "transition": regime,
                "transition_label": REGIME_LABELS.get(str(regime), str(regime)),
                "count": int(len(bucket)),
                "above_line_rate": float(bucket["above_line"].mean()),
                "mean_slope_ratio": float(bucket["slope_ratio"].mean()),
                "mean_4h_long_return": float(bucket["future_4h_long_return"].mean()),
                "mean_8h_long_return": float(bucket["future_8h_long_return"].mean()),
                "mean_24h_long_return": float(bucket["future_24h_long_return"].mean()),
                "mean_4h_short_return": float(bucket["future_4h_short_return"].mean()),
                "mean_8h_short_return": float(bucket["future_8h_short_return"].mean()),
                "mean_24h_short_return": float(bucket["future_24h_short_return"].mean()),
                "up_rate_24h": float((bucket["future_24h_long_return"] > 0).mean()),
                "down_rate_24h": float((bucket["future_24h_short_return"] > 0).mean()),
            }
        )
    return pd.DataFrame(rows)


def build_agreement_stats(ma_df: pd.DataFrame, ema_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.DataFrame(
        {
            "timestamp": ma_df["timestamp"],
            "ma_regime": ma_df["regime"],
            "ema_regime": ema_df["regime"],
            "ma_slope_ratio": ma_df["slope_ratio"],
            "ema_slope_ratio": ema_df["slope_ratio"],
            "future_24h_long_return": ma_df["future_24h_long_return"],
        }
    )
    ready = merged[(merged["ma_regime"] != "warming_up") & (merged["ema_regime"] != "warming_up")].copy()
    ready["same_sign"] = np.sign(ready["ma_slope_ratio"]) == np.sign(ready["ema_slope_ratio"])
    ready["same_regime"] = ready["ma_regime"] == ready["ema_regime"]
    ready["both_bull_run"] = (ready["ma_regime"] == "bull_run") & (ready["ema_regime"] == "bull_run")
    ready["both_bear_run"] = (ready["ma_regime"] == "bear_run") & (ready["ema_regime"] == "bear_run")
    ready["ma_bull_ema_leads"] = (ready["ma_regime"] == "bull_start") & (ready["ema_regime"].isin({"bull_run", "bull_fade"}))
    ready["ema_bull_leads"] = (ready["ema_regime"] == "bull_start") & (ready["ma_regime"].isin({"flat", "weak_bear", "bear_fade"}))

    rows = [
        {
            "bucket": "斜率同号",
            "count": int(ready["same_sign"].sum()),
            "share": float(ready["same_sign"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["same_sign"], "future_24h_long_return"].mean()),
        },
        {
            "bucket": "状态完全一致",
            "count": int(ready["same_regime"].sum()),
            "share": float(ready["same_regime"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["same_regime"], "future_24h_long_return"].mean()),
        },
        {
            "bucket": "双推进多头",
            "count": int(ready["both_bull_run"].sum()),
            "share": float(ready["both_bull_run"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["both_bull_run"], "future_24h_long_return"].mean()),
        },
        {
            "bucket": "双推进空头",
            "count": int(ready["both_bear_run"].sum()),
            "share": float(ready["both_bear_run"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["both_bear_run"], "future_24h_long_return"].mean()),
        },
        {
            "bucket": "EMA55先启动多头",
            "count": int(ready["ema_bull_leads"].sum()),
            "share": float(ready["ema_bull_leads"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["ema_bull_leads"], "future_24h_long_return"].mean()),
        },
        {
            "bucket": "MA55先启动多头",
            "count": int(ready["ma_bull_ema_leads"].sum()),
            "share": float(ready["ma_bull_ema_leads"].mean()),
            "mean_24h_long_return": float(ready.loc[ready["ma_bull_ema_leads"], "future_24h_long_return"].mean()),
        },
    ]
    return pd.DataFrame(rows)


def summarize_line(
    enriched: pd.DataFrame,
    regime_stats: pd.DataFrame,
    transition_stats: pd.DataFrame,
) -> dict[str, object]:
    latest = enriched.iloc[-1]
    best_long = regime_stats.sort_values("mean_24h_long_return", ascending=False).head(1)
    best_short = regime_stats.sort_values("mean_24h_short_return", ascending=False).head(1)
    return {
        "latest_timestamp": str(latest["timestamp"]),
        "latest_regime": str(latest["regime"]),
        "latest_regime_label": str(latest["regime_label"]),
        "latest_slope_ratio": float(latest["slope_ratio"]) if pd.notna(latest["slope_ratio"]) else None,
        "latest_slope_strength": float(latest["slope_strength"]) if pd.notna(latest["slope_strength"]) else None,
        "latest_close": float(latest["close"]),
        "latest_line": float(latest["line"]) if pd.notna(latest["line"]) else None,
        "ready_bars": int((enriched["regime"] != "warming_up").sum()),
        "best_long_regime": best_long.to_dict("records"),
        "best_short_regime": best_short.to_dict("records"),
        "bull_start_count": int((enriched["regime"] == "bull_start").sum()),
        "bear_start_count": int((enriched["regime"] == "bear_start").sum()),
        "transition_stats": transition_stats.to_dict("records"),
    }


def save_return_chart(regime_stats: pd.DataFrame, label: str, path: Path) -> None:
    if regime_stats.empty:
        return
    x = np.arange(len(regime_stats))
    width = 0.35
    plt.figure(figsize=(11, 4.5))
    plt.bar(x - width / 2, regime_stats["mean_24h_long_return"] * 100, width=width, label="24H long", color="#1d4ed8")
    plt.bar(x + width / 2, regime_stats["mean_24h_short_return"] * 100, width=width, label="24H short", color="#b42318")
    plt.xticks(x, regime_stats["regime"], rotation=20, ha="right")
    plt.axhline(0, color="#94a3b8", linewidth=1)
    plt.ylabel("Avg return %")
    plt.title(f"{label} 24H returns by regime")
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_regime_dist_chart(enriched: pd.DataFrame, label: str, path: Path) -> None:
    ready = enriched[enriched["regime"] != "warming_up"]
    counts = ready["regime"].value_counts()
    plt.figure(figsize=(10, 4))
    plt.bar(counts.index, counts.values, color="#2563eb", alpha=0.85)
    plt.title(f"{label} regime distribution")
    plt.ylabel("Count")
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def save_timeline_chart(recent: pd.DataFrame, label: str, path: Path) -> None:
    plt.figure(figsize=(12, 5))
    plt.plot(recent["timestamp"], recent["close"], color="#0f172a", linewidth=1.2, label="Close")
    plt.plot(recent["timestamp"], recent["line"], color="#1d4ed8", linewidth=1.0, label=label)
    colors = {
        "bull_run": "#0f9f6e",
        "bull_fade": "#84cc16",
        "bull_start": "#22c55e",
        "bear_run": "#b42318",
        "bear_fade": "#f97316",
        "bear_start": "#ef4444",
        "flat": "#94a3b8",
        "weak_bear": "#f59e0b",
    }
    for regime, color in colors.items():
        mask = recent["regime"] == regime
        if not mask.any():
            continue
        plt.scatter(
            recent.loc[mask, "timestamp"],
            recent.loc[mask, "close"],
            s=18,
            color=color,
            label=regime,
            alpha=0.85,
        )
    plt.title(f"{label} last 120 bars regime timeline")
    plt.legend(loc="upper left", ncol=2, fontsize=8)
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def build_html(
    df: pd.DataFrame,
    line_results: dict[str, dict[str, object]],
    agreement: pd.DataFrame,
) -> str:
    cards: list[str] = []
    sections: list[str] = []

    for label, result in line_results.items():
        summary = result["summary"]
        best_long = summary["best_long_regime"][0] if summary["best_long_regime"] else {}
        best_short = summary["best_short_regime"][0] if summary["best_short_regime"] else {}

        cards.append(
            kpi(
                f"{label} 当前状态",
                str(summary["latest_regime_label"]),
                (
                    f"slope_ratio={summary['latest_slope_ratio']:.6f}"
                    if summary["latest_slope_ratio"] is not None
                    else "数据预热中"
                ),
            )
        )

        sections.append(
            f"""
            <h2>{label} 斜率状态研究</h2>
            <div class="grid grid-2">
              <div class="card imgbox">
                <h3>各状态 24H 方向收益</h3>
                <img src="data:image/png;base64,{b64(result['return_hist_path'])}" alt="{label} returns">
              </div>
              <div class="card imgbox">
                <h3>状态分布</h3>
                <img src="data:image/png;base64,{b64(result['regime_hist_path'])}" alt="{label} distribution">
              </div>
            </div>
            <div class="card imgbox">
              <h3>最近 120 根状态时间线</h3>
              <img src="data:image/png;base64,{b64(result['timeline_path'])}" alt="{label} timeline">
            </div>
            <div class="grid grid-2">
              <div class="card">
                <h3>最新快照</h3>
                <p>时间：<strong>{summary['latest_timestamp']}</strong></p>
                <p>状态：<strong>{summary['latest_regime_label']}</strong>（{summary['latest_regime']}）</p>
                <p>收盘 / {label}：<strong>{summary['latest_close']:.2f}</strong> / <strong>{summary['latest_line']:.2f}</strong></p>
                <p>slope_ratio：<strong>{summary['latest_slope_ratio']:.6f}</strong>，slope_strength：<strong>{summary['latest_slope_strength']:.3f}</strong></p>
                <p>多头启动次数 <strong>{summary['bull_start_count']}</strong>，空头启动次数 <strong>{summary['bear_start_count']}</strong></p>
              </div>
              <div class="card">
                <h3>结论摘录</h3>
                <p>24H 做多期望最高状态：<strong>{best_long.get('regime_label', '-')}</strong>，平均收益 <strong>{pct(best_long.get('mean_24h_long_return', 0.0))}</strong>，上涨率 <strong>{pct(best_long.get('up_rate_24h', 0.0))}</strong>。</p>
                <p>24H 做空期望最高状态：<strong>{best_short.get('regime_label', '-')}</strong>，平均收益 <strong>{pct(best_short.get('mean_24h_short_return', 0.0))}</strong>，下跌率先到率 <strong>{pct(best_short.get('down_rate_24h', 0.0))}</strong>。</p>
                <p class="note">这是 event study，不是完整仓位回测。收益按“状态确认当根收盘”统计未来 N 根 close-to-close 变化。</p>
              </div>
            </div>
            <div class="card">
              <h3>按状态统计</h3>
              {render_table(result['regime_stats'], [
                'regime_label','count','share','mean_slope_ratio','mean_slope_strength','above_line_rate',
                'mean_4h_long_return','mean_24h_long_return','up_rate_24h',
                'mean_4h_short_return','mean_24h_short_return','down_rate_24h'
              ])}
            </div>
            <div class="card">
              <h3>拐点事件（斜率由负转正 / 由正转负）</h3>
              {render_table(result['transition_stats'], [
                'transition_label','count','above_line_rate','mean_slope_ratio',
                'mean_4h_long_return','mean_24h_long_return','up_rate_24h',
                'mean_4h_short_return','mean_24h_short_return','down_rate_24h'
              ])}
            </div>
            """
        )

    ma_summary = line_results["55MA"]["summary"]
    ema_summary = line_results["55EMA"]["summary"]

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 55MA/55EMA 斜率状态研究</title>
<style>
:root {{
  --ink:#172033; --muted:#667085; --line:#d7dce5; --bg:#f4f7fb; --panel:#fff;
  --blue:#1d4ed8; --green:#0f9f6e; --amber:#b45309; --red:#b42318;
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
.callout {{ border-left:5px solid var(--amber); background:#fffbeb; border-radius:6px; padding:14px 16px; line-height:1.7; }}
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
  <h1>BTC 1小时 55MA / 55EMA 斜率状态研究</h1>
  <p>目标：用 55 线的斜率变化观察行情阶段，并统计各阶段之后 4H / 8H / 24H 的方向收益。</p>
  <p>数据：{INST_ID} {BAR}，{len(df)} 根K线，{df['timestamp'].iloc[0]} 至 {df['timestamp'].iloc[-1]}。斜率窗口 {LOOKBACK} 根，走平阈值 |slope_ratio| &lt; {FLAT_THRESHOLD}，强空阈值 &lt; -{STRONG_THRESHOLD}。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {''.join(cards)}
    {kpi("MA/EMA 当前对比", "共振观察", f"MA={ma_summary['latest_regime_label']} / EMA={ema_summary['latest_regime_label']}")}
  </div>

  <div class="card callout">
    <strong>怎么读这份报告：</strong>
    先看“当前状态”判断现在处于推进、衰竭还是拐点；再看“按状态统计”找哪类阶段后做多/做空更有统计优势；
  最后看“MA55 vs EMA55 共振”判断两条线是否同向确认。
  </div>

  <div class="card">
    <h2>状态定义</h2>
    <p><strong>多头启动</strong>：slope_ratio 由 ≤0 上穿到 &gt;0。<strong>空头启动</strong>：由 ≥0 下穿到 &lt;0。</p>
    <p><strong>多头推进</strong>：slope_ratio &gt; 0 且未连续减速。<strong>多头衰竭</strong>：slope_ratio 仍 &gt; 0，但 slope_accel &lt; 0 连续 {ACCEL_BARS} 根。</p>
    <p><strong>空头推进</strong>：slope_ratio &lt; -{STRONG_THRESHOLD}。<strong>空头衰竭</strong>：仍偏空，但 slope_accel &gt; 0 连续 {ACCEL_BARS} 根。</p>
    <p><strong>走平震荡</strong>：|slope_ratio| &lt; {FLAT_THRESHOLD}。<strong>弱空头</strong>：略低于 0，但未达到强空阈值。</p>
  </div>

  <h2>MA55 / EMA55 共振统计</h2>
  <div class="card">{render_table(agreement, ['bucket','count','share','mean_24h_long_return'])}</div>

  {''.join(sections)}
</main>
</body>
</html>"""


def render_table(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame is None or frame.empty:
        return "<p class='note'>暂无数据</p>"
    subset = frame[columns].copy()
    parts = ["<table><tr>" + "".join(f"<th>{col}</th>" for col in columns) + "</tr>"]
    for _, row in subset.iterrows():
        cells = []
        for col in columns:
            value = row[col]
            if isinstance(value, (float, np.floating)):
                if "return" in col or "rate" in col or "share" in col:
                    text = pct(value)
                elif "ratio" in col:
                    text = f"{value:.6f}"
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
