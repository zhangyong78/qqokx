from __future__ import annotations

import base64
import html
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.candle_cache import load_candle_cache


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
TAKER_FEE_RATE = 0.00036
FIXED_RISK_AMOUNT = 10.0

BASE_SLOPE_THRESHOLD = -0.0005
BASE_STOP_ATR = 2.0
ATR_PERCENTILE_LOOKBACK = 100

CSV_PATH = REPORT_DIR / "r001_atr_regime_followup_10u.csv"
JSON_PATH = REPORT_DIR / "r001_atr_regime_followup_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_atr_regime_followup_10u_report.html"
CHART_TEST_PNL = REPORT_DIR / "r001_atr_regime_followup_10u_test_pnl.png"
CHART_CURVE = REPORT_DIR / "r001_atr_regime_followup_10u_curve.png"


@dataclass(frozen=True)
class AtrVariant:
    key: str
    label: str
    min_pct: float
    max_pct: float


VARIANTS = [
    AtrVariant("all", "不过滤波动", 0.0, 1.0),
    AtrVariant("low_30", "ATR分位<=30%", 0.0, 0.30),
    AtrVariant("low_50", "ATR分位<=50%", 0.0, 0.50),
    AtrVariant("mid_20_60", "ATR分位20%-60%", 0.20, 0.60),
    AtrVariant("mid_30_70", "ATR分位30%-70%", 0.30, 0.70),
    AtrVariant("high_50", "ATR分位>=50%", 0.50, 1.0),
]


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    rows: list[dict[str, object]] = []
    trades_by_key: dict[str, pd.DataFrame] = {}
    for variant in VARIANTS:
        trades = simulate_trades(df, variant)
        row = flatten_split_metrics(variant, trades, split_bounds)
        row["score"] = score_row(row)
        rows.append(row)
        trades_by_key[variant.key] = trades

    comparison = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    best_variant = next(item for item in VARIANTS if item.key == str(comparison.iloc[0]["variant_key"]))
    best_trades = trades_by_key[best_variant.key]

    save_test_pnl_chart(comparison, CHART_TEST_PNL)
    save_curve_chart(trades_by_key, CHART_CURVE)

    summary = {
        "risk_per_trade_usdt": FIXED_RISK_AMOUNT,
        "entry_anchor": {
            "ema55_slope_threshold": BASE_SLOPE_THRESHOLD,
            "stop_atr": BASE_STOP_ATR,
            "exit_mode": "2R保本后逐级锁盈",
            "atr_percentile_lookback": ATR_PERCENTILE_LOOKBACK,
        },
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "best_variant": comparison.iloc[0].to_dict(),
        "top3": comparison.head(3).to_dict("records"),
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(comparison=comparison, best_variant=best_variant, best_trades=best_trades, summary=summary), encoding="utf-8")
    print(HTML_PATH)


def build_frame(candles: list[object]) -> pd.DataFrame:
    rows = [
        {
            "ts": int(candle.ts),
            "timestamp": pd.to_datetime(int(candle.ts), unit="ms", utc=True),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
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
    df["atr_pct"] = df["atr14"].rolling(ATR_PERCENTILE_LOOKBACK, min_periods=ATR_PERCENTILE_LOOKBACK).apply(
        lambda x: float(np.mean(x <= x[-1])),
        raw=True,
    )


def build_split_bounds(length: int) -> dict[str, tuple[int, int]]:
    train_end = int(length * 0.6)
    validation_end = int(length * 0.8)
    return {
        "train": (0, train_end - 1),
        "validation": (train_end, validation_end - 1),
        "test": (validation_end, length - 1),
        "all": (0, length - 1),
    }


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, variant: AtrVariant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int] | None = None

    for index in range(100, len(df)):
        row = df.iloc[index]
        current_ema55 = float(row["ema55"]) if pd.notna(row["ema55"]) else math.nan
        prev_ema55 = float(df.iloc[index - 1]["ema55"]) if pd.notna(df.iloc[index - 1]["ema55"]) else math.nan
        atr_value = float(row["atr14"]) if pd.notna(row["atr14"]) else math.nan
        atr_pct = float(row["atr_pct"]) if pd.notna(row["atr_pct"]) else math.nan
        if not np.isfinite(current_ema55) or not np.isfinite(prev_ema55) or not np.isfinite(atr_value) or not np.isfinite(atr_pct):
            continue

        fast_slope_ratio = (current_ema55 - prev_ema55) / current_ema55 if current_ema55 else math.nan

        if position is not None:
            position["best_low"] = min(float(position["best_low"]), float(row["low"]))
            position["worst_high"] = max(float(position["worst_high"]), float(row["high"]))

            exited = False
            path = candle_path_points(row)
            for start, end in zip(path, path[1:]):
                if end > start:
                    stop_price = float(position["stop"])
                    if stop_price >= start and stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, "stop"))
                        position = None
                        exited = True
                        break
                else:
                    favorable_price = end
                    advance_step_dynamic(position, favorable_price)

            if position is not None and fast_slope_ratio > 0:
                trades.append(close_trade(position, index, int(row["ts"]), float(row["close"]), "slope_turn_positive"))
                position = None
                exited = True

            if exited:
                continue

        if position is not None:
            continue
        if fast_slope_ratio > BASE_SLOPE_THRESHOLD:
            continue
        if atr_pct < variant.min_pct or atr_pct > variant.max_pct:
            continue

        risk_per_unit = atr_value * BASE_STOP_ATR
        if not np.isfinite(risk_per_unit) or risk_per_unit <= 0:
            continue

        entry_price = float(row["close"])
        fee_offset = entry_price * TAKER_FEE_RATE * 2.0
        position = {
            "entry_index": index,
            "entry_ts": int(row["ts"]),
            "entry_price": entry_price,
            "risk_per_unit": risk_per_unit,
            "stop": entry_price + risk_per_unit,
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
            "best_low": entry_price,
            "worst_high": entry_price,
        }

    return pd.DataFrame(trades)


def advance_step_dynamic(position: dict[str, float | int], favorable_price: float) -> None:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry_price - (risk_per_unit * next_r) - fee_offset
        if favorable_price > trigger:
            break
        locked_r = 0.0 if math.isclose(next_r, 2.0) else max(next_r - 1.0, 0.0)
        candidate_stop = entry_price - (risk_per_unit * locked_r) - fee_offset
        position["stop"] = min(float(position["stop"]), candidate_stop)
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(position: dict[str, float | int], exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    quantity = FIXED_RISK_AMOUNT / risk_per_unit if risk_per_unit > 0 else 0.0
    pnl_per_unit = (entry_price - exit_price) - (TAKER_FEE_RATE * (entry_price + exit_price))
    pnl_u = pnl_per_unit * quantity
    r_multiple = pnl_u / FIXED_RISK_AMOUNT if FIXED_RISK_AMOUNT else 0.0
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "pnl_u": pnl_u,
        "r_multiple": r_multiple,
        "exit_reason": exit_reason,
    }


def split_trades(trades: pd.DataFrame, bounds: tuple[int, int]) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    start, end = bounds
    return trades[(trades["exit_index"] >= start) & (trades["exit_index"] <= end)].copy()


def metrics_for_trades(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trades": 0.0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "total_r": 0.0,
            "profit_factor": 0.0,
            "total_pnl_u": 0.0,
            "avg_pnl_u": 0.0,
            "max_drawdown_u": 0.0,
        }
    rs = trades["r_multiple"].astype(float)
    pnls = trades["pnl_u"].astype(float)
    gross_profit = float(pnls[pnls > 0].sum())
    gross_loss = float(pnls[pnls <= 0].sum())
    curve = pnls.cumsum()
    drawdown = (curve.cummax() - curve).max()
    return {
        "trades": float(len(trades)),
        "win_rate": float((rs > 0).mean()),
        "avg_r": float(rs.mean()),
        "total_r": float(rs.sum()),
        "profit_factor": float(gross_profit / abs(gross_loss)) if gross_loss < 0 else 0.0,
        "total_pnl_u": float(pnls.sum()),
        "avg_pnl_u": float(pnls.mean()),
        "max_drawdown_u": float(drawdown),
    }


def flatten_split_metrics(variant: AtrVariant, trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, object]:
    row: dict[str, object] = {
        "variant_key": variant.key,
        "variant_label": variant.label,
        "min_pct": variant.min_pct,
        "max_pct": variant.max_pct,
    }
    for split_name, bounds in split_bounds.items():
        metrics = metrics_for_trades(split_trades(trades, bounds))
        for metric_name, value in metrics.items():
            row[f"{split_name}_{metric_name}"] = value
    return row


def score_row(row: dict[str, object]) -> float:
    return (
        float(row["test_total_pnl_u"]) * 1.5
        + float(row["validation_total_pnl_u"]) * 1.3
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 25.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 22.0
        - float(row["all_max_drawdown_u"]) * 0.7
    )


def save_test_pnl_chart(comparison: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar(comparison["variant_label"], comparison["test_total_pnl_u"], color="#1d4ed8")
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("测试段总盈亏对比")
    ax.set_ylabel("总盈亏 U")
    ax.tick_params(axis="x", rotation=18)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_curve_chart(trade_map: dict[str, pd.DataFrame], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = ["#1d4ed8", "#0f766e", "#b45309", "#7c3aed", "#be123c", "#0891b2"]
    for index, variant in enumerate(VARIANTS):
        trades = trade_map[variant.key]
        if trades.empty:
            continue
        curve = trades["pnl_u"].cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=variant.label, linewidth=2, color=colors[index % len(colors)])
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("全样本累计盈亏曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计盈亏 U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def build_html(*, comparison: pd.DataFrame, best_variant: AtrVariant, best_trades: pd.DataFrame, summary: dict[str, object]) -> str:
    best_metrics = metrics_for_trades(best_trades)
    stable_rows = comparison[(comparison["validation_total_pnl_u"] > 0) & (comparison["test_total_pnl_u"] > 0) & (comparison["test_profit_factor"] > 1.0)]
    base_row = comparison[comparison["variant_key"] == "all"].iloc[0]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 四轮 ATR 状态过滤报告</title>
<style>
:root {{
  --bg:#f4f6f9; --panel:#fff; --ink:#182433; --muted:#64748b; --line:#d9e2ec;
  --hero-a:#0f172a; --hero-b:#234868; --good:#0f766e; --warn:#b45309; --bad:#be123c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,var(--hero-a),var(--hero-b)); color:#fff; padding:34px 42px; }}
.hero h1 {{ margin:0 0 8px; font-size:30px; }}
.hero p {{ margin:6px 0; max-width:1120px; color:#dbe7f3; line-height:1.75; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px 20px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:14px; padding:18px; box-shadow:0 4px 16px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; }}
.kpi .value {{ font-size:28px; font-weight:800; margin-top:8px; }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:8px; line-height:1.6; }}
h2 {{ margin:28px 0 14px; font-size:22px; }}
h3 {{ margin:0 0 10px; font-size:17px; }}
p {{ line-height:1.75; }}
.answer {{ font-size:17px; line-height:1.85; }}
.good {{ color:var(--good); font-weight:700; }}
.warn {{ color:var(--warn); font-weight:700; }}
.bad {{ color:var(--bad); font-weight:700; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#465467; }}
.imgbox img {{ width:100%; display:block; border-radius:10px; border:1px solid var(--line); background:#fff; }}
.callout {{ border-left:5px solid #1d4ed8; background:#eff6ff; border-radius:10px; padding:14px 16px; }}
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width:920px) {{
  .grid-4,.grid-3,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:24px 18px; }}
  .wrap {{ padding:18px 12px 36px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 四轮优化：ATR 状态过滤</h1>
  <p>这轮沿用当前工作组合：<strong>EMA55 斜率 ≤ -0.0005 + 止损 2ATR + 2R保本后逐级锁盈</strong>，只新增一层波动状态过滤。具体做法是看 <strong>ATR14 在过去100根里的分位位置</strong>，比较低波动、中波动、高波动区间入场的差异。</p>
  <p>这样做的目的不是再加一个复杂条件，而是判断这条策略到底更适合“波动尚未扩张完的阶段”，还是“已经进入高波动释放阶段”。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳波动过滤", html.escape(best_variant.label), "按样本外评分排序")}
    {kpi("最佳测试盈亏", f"{float(comparison.iloc[0]['test_total_pnl_u']):.1f}U", f"测试 PF {float(comparison.iloc[0]['test_profit_factor']):.2f}")}
    {kpi("验证段改善", f"{float(comparison.iloc[0]['validation_total_pnl_u'] - base_row['validation_total_pnl_u']):.1f}U", "相对不过滤波动的变化")}
    {kpi("稳定盈利组合", str(len(stable_rows)), "验证和测试同时盈利且测试 PF > 1")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这轮最重要的是看 <span class="good">{html.escape(best_variant.label)}</span> 有没有比“不过滤波动”更稳。基线版本验证段是 <strong>{float(base_row['validation_total_pnl_u']):.1f}U</strong>、测试段是 <strong>{float(base_row['test_total_pnl_u']):.1f}U</strong>；最佳 ATR 过滤版本对应地变成了 <strong>{float(comparison.iloc[0]['validation_total_pnl_u']):.1f}U</strong> 和 <strong>{float(comparison.iloc[0]['test_total_pnl_u']):.1f}U</strong>。如果提升主要来自验证段，同时测试段没有明显塌掉，那这层过滤就值得保留。
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>这轮在验证什么</h3>
      <p>它验证的是：这条策略是不是更适合某种波动环境，而不是任何时候都按同一方式进场。</p>
    </div>
    <div class="card">
      <h3>为什么用 ATR 分位</h3>
      <p>ATR 绝对值会随价格中枢变化，分位更稳定，也更适合直接做过滤阈值。</p>
    </div>
    <div class="card">
      <h3>怎么理解结果</h3>
      <p>如果低波动区间最好，说明这条策略更像“提前卡趋势扩张”；如果高波动区间最好，说明它更依赖趋势已经释放出来后的延续。</p>
    </div>
  </div>

  <h2>图表</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>测试段总盈亏对比</h3>
      {image_tag(CHART_TEST_PNL)}
    </div>
    <div class="card imgbox">
      <h3>全样本累计盈亏曲线</h3>
      {image_tag(CHART_CURVE)}
    </div>
  </div>

  <h2>ATR 状态对比</h2>
  <div class="card">
    {dataframe_table(
        comparison,
        [
            ("variant_label", "波动过滤"),
            ("validation_trades", "验证交易数"),
            ("validation_total_pnl_u", "验证总盈亏U"),
            ("validation_avg_r", "验证 Avg R"),
            ("validation_profit_factor", "验证 PF"),
            ("test_trades", "测试交易数"),
            ("test_total_pnl_u", "测试总盈亏U"),
            ("test_avg_r", "测试 Avg R"),
            ("test_profit_factor", "测试 PF"),
            ("all_total_pnl_u", "全样本总盈亏U"),
            ("all_max_drawdown_u", "全样本最大回撤U"),
            ("score", "综合分"),
        ],
    )}
    <p class="note">最佳版本全样本：{int(best_metrics['trades'])} 笔，胜率 {best_metrics['win_rate'] * 100:.1f}%，总盈亏 {best_metrics['total_pnl_u']:.1f}U，Avg R {best_metrics['avg_r']:.3f}，PF {best_metrics['profit_factor']:.2f}。</p>
  </div>

  <h2>下一步建议</h2>
  <div class="card">
    <div class="callout">
      <strong>建议顺序</strong><br>
      1. 如果 ATR 过滤确实改善验证段，就把它并入当前最优组合。<br>
      2. 如果 ATR 过滤无明显增益，下一轮就别继续叠过滤，改去做分阶段 walk-forward。<br>
      3. 如果还想继续扫轻量条件，我更建议试成交量分位，而不是再堆趋势条件。
    </div>
  </div>
</main>
</body>
</html>
"""


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, _label in columns:
            cells.append(f"<td>{format_cell(column, getattr(item, column))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    if value is None:
        return "-"
    number = float(value)
    lower = column.lower()
    if "pnl_u" in lower or "drawdown_u" in lower:
        return f"{number:.1f}"
    if "avg_r" in lower or "score" in lower:
        return f"{number:.3f}"
    if "profit_factor" in lower:
        return f"{number:.2f}"
    if "trades" in lower:
        return str(int(round(number)))
    if "win_rate" in lower:
        return f"{number * 100:.1f}%"
    return f"{number:.3f}"


def image_tag(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f'<img alt="{html.escape(path.stem)}" src="data:image/png;base64,{encoded}">'


def kpi(label: str, value: str, sub: str) -> str:
    return f"""
<div class="card kpi">
  <div class="label">{html.escape(label)}</div>
  <div class="value">{value}</div>
  <div class="sub">{html.escape(sub)}</div>
</div>
"""


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
