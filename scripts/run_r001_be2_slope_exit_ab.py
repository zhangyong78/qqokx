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
from scripts.run_r001_three_variant_full_compare import (
    ATR_PERCENTILE_LOOKBACK,
    ATR_PERCENTILE_MAX,
    BAR,
    EMA55_SLOPE_THRESHOLD,
    FIXED_RISK_AMOUNT,
    INST_ID,
    STOP_ATR_MULTIPLIER,
    TAKER_FEE_RATE,
    add_indicators,
    build_frame,
    build_split_bounds,
    candle_path_points,
    metrics_for_trades,
    split_trades,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
CSV_PATH = REPORT_DIR / "r001_be2_slope_exit_ab.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_be2_slope_exit_ab_reasons.csv"
JSON_PATH = REPORT_DIR / "r001_be2_slope_exit_ab_summary.json"
HTML_PATH = REPORT_DIR / "r001_be2_slope_exit_ab_report.html"
CHART_EQUITY = REPORT_DIR / "r001_be2_slope_exit_ab_equity.png"
CHART_SPLIT = REPORT_DIR / "r001_be2_slope_exit_ab_split.png"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    include_slope_exit: bool


VARIANTS = [
    Variant("be2_with_slope_exit", "2R保本逐级锁盈 + 斜率转正平仓", True),
    Variant("be2_without_slope_exit", "2R保本逐级锁盈 + 不加斜率转正平仓", False),
]


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    trades_by_key: dict[str, pd.DataFrame] = {}
    comparison_rows: list[dict[str, object]] = []
    reason_rows: list[dict[str, object]] = []

    for variant in VARIANTS:
        trades = simulate_trades(df, variant)
        trades_by_key[variant.key] = trades
        comparison_rows.append(flatten_split_metrics(variant, trades, split_bounds))
        reason_rows.extend(flatten_reason_counts(variant, trades))

    comparison = pd.DataFrame(comparison_rows)
    comparison["score"] = comparison.apply(score_row, axis=1)
    comparison = comparison.sort_values("score", ascending=False).reset_index(drop=True)
    reasons = pd.DataFrame(reason_rows)

    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")

    save_equity_chart(trades_by_key)
    save_split_chart(comparison)

    summary = {
        "question": "2R保本后逐级锁盈，是否加入 EMA55 斜率转正平仓条件的 A/B 对比",
        "entry_config": {
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_mult": STOP_ATR_MULTIPLIER,
            "atr_pct_max": ATR_PERCENTILE_MAX,
            "atr_percentile_lookback": ATR_PERCENTILE_LOOKBACK,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
            "break_even_trigger_r": 2.0,
        },
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "best_variant": comparison.iloc[0].to_dict(),
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(comparison, reasons, trades_by_key, summary), encoding="utf-8")
    print(HTML_PATH)


def simulate_trades(df: pd.DataFrame, variant: Variant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None

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
                    if start <= stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, str(position["stop_reason"])))
                        position = None
                        exited = True
                        break
                else:
                    advance_step_dynamic(position, end)

            if position is not None and variant.include_slope_exit and fast_slope_ratio > 0:
                trades.append(close_trade(position, index, int(row["ts"]), float(row["close"]), "slope_turn_positive"))
                position = None
                exited = True

            if exited:
                continue

        if position is not None:
            continue
        if fast_slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue

        risk_per_unit = atr_value * STOP_ATR_MULTIPLIER
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
            "stop_reason": "stop_loss",
            "fee_offset": fee_offset,
            "next_dynamic_r": 2.0,
            "best_low": entry_price,
            "worst_high": entry_price,
        }

    return pd.DataFrame(trades)


def advance_step_dynamic(position: dict[str, float | int | str], favorable_price: float) -> None:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry_price - (risk_per_unit * next_r) - fee_offset
        if favorable_price > trigger:
            break
        if math.isclose(next_r, 2.0):
            locked_r = 0.0
            stop_reason = "break_even_stop"
        else:
            locked_r = max(next_r - 1.0, 0.0)
            stop_reason = f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry_price - (risk_per_unit * locked_r) - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = stop_reason
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(position: dict[str, float | int | str], exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    quantity = FIXED_RISK_AMOUNT / risk_per_unit if risk_per_unit > 0 else 0.0
    pnl_per_unit = (entry_price - exit_price) - (TAKER_FEE_RATE * (entry_price + exit_price))
    pnl_u = pnl_per_unit * quantity
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / FIXED_RISK_AMOUNT if FIXED_RISK_AMOUNT else 0.0,
        "exit_reason": exit_reason,
    }


def flatten_split_metrics(variant: Variant, trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, object]:
    row: dict[str, object] = {"variant_key": variant.key, "variant_label": variant.label}
    for split_name, bounds in split_bounds.items():
        metrics = metrics_for_trades(split_trades(trades, bounds))
        for metric_name, value in metrics.items():
            row[f"{split_name}_{metric_name}"] = value
    return row


def score_row(row: pd.Series) -> float:
    return (
        float(row["test_total_pnl_u"]) * 1.5
        + float(row["validation_total_pnl_u"]) * 1.3
        + max(float(row["test_profit_factor"]) - 1.0, -1.0) * 25.0
        + max(float(row["validation_profit_factor"]) - 1.0, -1.0) * 22.0
        - float(row["all_max_drawdown_u"]) * 0.7
    )


def reason_label(reason: str) -> str:
    if reason == "break_even_stop":
        return "保本"
    if reason == "stop_loss":
        return "止损"
    if reason == "slope_turn_positive":
        return "斜率转正"
    if reason.startswith("locked_") and reason.endswith("r_stop"):
        return f"{reason.removeprefix('locked_').removesuffix('r_stop')}R"
    return reason


def flatten_reason_counts(variant: Variant, trades: pd.DataFrame) -> list[dict[str, object]]:
    if trades.empty:
        return []
    rows = []
    for reason, count in trades["exit_reason"].value_counts().items():
        rows.append(
            {
                "variant_key": variant.key,
                "variant_label": variant.label,
                "exit_reason": str(reason),
                "exit_label": reason_label(str(reason)),
                "count": int(count),
                "ratio": float(count / len(trades)),
            }
        )
    return rows


def save_equity_chart(trade_map: dict[str, pd.DataFrame]) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = {"be2_with_slope_exit": "#1d4ed8", "be2_without_slope_exit": "#b45309"}
    for variant in VARIANTS:
        trades = trade_map[variant.key]
        if trades.empty:
            continue
        curve = trades["pnl_u"].cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=variant.label, linewidth=2, color=colors[variant.key])
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("2R逐级锁盈：是否加入斜率转正平仓")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计盈亏 U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_EQUITY, dpi=160)
    plt.close(fig)


def save_split_chart(comparison: pd.DataFrame) -> None:
    labels = comparison["variant_label"].tolist()
    x = np.arange(len(labels))
    width = 0.36
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar(x - width / 2, comparison["validation_total_pnl_u"], width, label="验证段", color="#0f766e")
    ax.bar(x + width / 2, comparison["test_total_pnl_u"], width, label="测试段", color="#1d4ed8")
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("验证段 / 测试段盈亏对比")
    ax.set_ylabel("盈亏 U")
    ax.set_xticks(x, labels)
    ax.legend()
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(CHART_SPLIT, dpi=160)
    plt.close(fig)


def build_html(comparison: pd.DataFrame, reasons: pd.DataFrame, trades_by_key: dict[str, pd.DataFrame], summary: dict[str, object]) -> str:
    best = comparison.iloc[0]
    delta_test = float(comparison.loc[comparison["variant_key"] == "be2_with_slope_exit", "test_total_pnl_u"].iloc[0]) - float(
        comparison.loc[comparison["variant_key"] == "be2_without_slope_exit", "test_total_pnl_u"].iloc[0]
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 2R逐级锁盈斜率平仓 A/B 对比</title>
<style>
:root {{ --bg:#f5f7fb; --panel:#fff; --ink:#162131; --muted:#64748b; --line:#d8e0ea; --blue:#1d4ed8; --green:#0f766e; --amber:#b45309; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#102033,#31516b); color:#fff; padding:34px 42px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ max-width:1180px; line-height:1.75; margin:6px 0; color:#dbe7f3; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px 20px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-3 {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px; box-shadow:0 4px 16px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; }}
.kpi .value {{ font-size:27px; font-weight:800; margin-top:8px; }}
.kpi .sub {{ color:var(--muted); margin-top:8px; font-size:13px; line-height:1.6; }}
h2 {{ margin:28px 0 14px; font-size:22px; }}
p {{ line-height:1.75; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#465467; }}
img {{ width:100%; display:block; border:1px solid var(--line); border-radius:10px; }}
.good {{ color:var(--green); font-weight:800; }}
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width:900px) {{ .grid-3,.grid-2 {{ grid-template-columns:1fr; }} .hero {{ padding:24px 18px; }} .wrap {{ padding:18px 12px 36px; }} }}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 2R逐级锁盈：斜率转正平仓 A/B 测试</h1>
  <p>固定入场和风险：EMA55斜率 <= -0.0005，止损 2ATR，ATR分位 <= 50%，每笔风险 10U。唯一变量是：持仓后是否额外加入 EMA55 斜率转正平仓。</p>
</section>
<main class="wrap">
  <div class="grid grid-3">
    {kpi("综合更优", html.escape(str(best["variant_label"])), "按验证段、测试段、PF与回撤综合评分")}
    {kpi("测试段差值", f"{delta_test:+.1f}U", "加入斜率转正平仓 - 不加入")}
    {kpi("数据区间", f"{summary['data_start_utc']} 至 {summary['data_end_utc']}", "UTC 时间")}
  </div>

  <h2>核心结论</h2>
  <div class="card">
    <p>这轮 A/B 只回答一个问题：<strong>2R保本后逐级锁盈，要不要加“斜率转正平仓”</strong>。结果显示当前综合更优的是 <span class="good">{html.escape(str(best["variant_label"]))}</span>。</p>
    <p>如果加入后测试段差值为正，说明斜率转正帮我们更早规避反弹；如果为负，说明它过早打断了一部分趋势单。下面的原因分布可以直接看到多出来的“斜率转正”出场到底换来了什么。</p>
  </div>

  <h2>收益图</h2>
  <div class="grid grid-2">
    <div class="card">{image_tag(CHART_SPLIT)}</div>
    <div class="card">{image_tag(CHART_EQUITY)}</div>
  </div>

  <h2>指标对比</h2>
  <div class="card">
    {dataframe_table(comparison, [
        ("variant_label", "策略"),
        ("validation_trades", "验证交易数"),
        ("validation_total_pnl_u", "验证盈亏U"),
        ("validation_profit_factor", "验证PF"),
        ("test_trades", "测试交易数"),
        ("test_total_pnl_u", "测试盈亏U"),
        ("test_profit_factor", "测试PF"),
        ("all_total_pnl_u", "全样本盈亏U"),
        ("all_max_drawdown_u", "全样本最大回撤U"),
        ("score", "综合分"),
    ])}
  </div>

  <h2>出场原因</h2>
  <div class="card">
    {dataframe_table(reasons.sort_values(["variant_label", "count"], ascending=[True, False]), [
        ("variant_label", "策略"),
        ("exit_label", "出场原因"),
        ("count", "次数"),
        ("ratio", "占比"),
    ])}
  </div>
</main>
</body>
</html>"""


def dataframe_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    rows = []
    for item in frame.itertuples(index=False):
        cells = []
        for column, _ in columns:
            cells.append(f"<td>{format_cell(column, getattr(item, column))}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table>"


def format_cell(column: str, value: object) -> str:
    if isinstance(value, str):
        return html.escape(value)
    number = float(value)
    lower = column.lower()
    if "pnl_u" in lower or "drawdown_u" in lower:
        return f"{number:.1f}"
    if "profit_factor" in lower:
        return f"{number:.2f}"
    if "ratio" in lower:
        return f"{number * 100:.1f}%"
    if "trades" in lower or "count" in lower:
        return str(int(round(number)))
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
</div>"""


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
