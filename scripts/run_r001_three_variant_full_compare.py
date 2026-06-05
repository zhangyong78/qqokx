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
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_LOOKBACK = 100
ATR_PERCENTILE_MAX = 0.50

CSV_PATH = REPORT_DIR / "r001_three_variant_compare.csv"
REASON_CSV_PATH = REPORT_DIR / "r001_three_variant_exit_reasons.csv"
JSON_PATH = REPORT_DIR / "r001_three_variant_compare_summary.json"
HTML_PATH = REPORT_DIR / "r001_three_variant_compare_report.html"
CHART_TEST_PNL = REPORT_DIR / "r001_three_variant_test_pnl.png"
CHART_EQUITY = REPORT_DIR / "r001_three_variant_equity.png"


@dataclass(frozen=True)
class StrategyVariant:
    key: str
    label: str
    mode: str
    break_even_trigger_r: float | None = None


VARIANTS = [
    StrategyVariant("be2_ladder", "2R保本后逐级锁盈", "step_dynamic", 2.0),
    StrategyVariant("be1_ladder", "1R保本后逐级锁盈", "step_dynamic", 1.0),
    StrategyVariant("slope_exit", "斜率转正就平仓", "signal"),
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
    reason_rows: list[dict[str, object]] = []

    for variant in VARIANTS:
        trades = simulate_trades(df, variant)
        rows.append(flatten_split_metrics(variant, trades, split_bounds))
        trades_by_key[variant.key] = trades
        reason_rows.extend(flatten_reason_counts(variant, trades))

    comparison = pd.DataFrame(rows)
    comparison["score"] = comparison.apply(score_row, axis=1)
    comparison = comparison.sort_values("score", ascending=False).reset_index(drop=True)
    reasons = pd.DataFrame(reason_rows)
    reasons.to_csv(REASON_CSV_PATH, index=False, encoding="utf-8-sig")
    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    best_variant = next(item for item in VARIANTS if item.key == str(comparison.iloc[0]["variant_key"]))
    best_trades = trades_by_key[best_variant.key]
    best_reason = build_reason_summary(best_trades)

    save_test_pnl_chart(comparison, CHART_TEST_PNL)
    save_equity_chart(trades_by_key, CHART_EQUITY)

    summary = {
        "assumption": "第三种策略默认沿用同一套入场与风险框架，只把出场改成斜率转正平仓。",
        "entry_config": {
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_mult": STOP_ATR_MULTIPLIER,
            "atr_pct_max": ATR_PERCENTILE_MAX,
            "atr_percentile_lookback": ATR_PERCENTILE_LOOKBACK,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
        },
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "best_variant": comparison.iloc[0].to_dict(),
        "best_reason_counts": best_reason,
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(comparison=comparison, reasons=reasons, trades_by_key=trades_by_key, summary=summary), encoding="utf-8")
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


def simulate_trades(df: pd.DataFrame, variant: StrategyVariant) -> pd.DataFrame:
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
                    if stop_price >= start and stop_price <= end:
                        trades.append(close_trade(position, index, int(row["ts"]), stop_price, str(position["stop_reason"])))
                        position = None
                        exited = True
                        break
                else:
                    favorable_price = end
                    if variant.mode == "step_dynamic":
                        advance_step_dynamic(position, favorable_price, float(variant.break_even_trigger_r or 2.0))

            if position is not None and fast_slope_ratio > 0:
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
            "next_dynamic_r": float(variant.break_even_trigger_r or 2.0),
            "best_low": entry_price,
            "worst_high": entry_price,
        }

    return pd.DataFrame(trades)


def advance_step_dynamic(position: dict[str, float | int | str], favorable_price: float, first_trigger_r: float) -> None:
    entry_price = float(position["entry_price"])
    risk_per_unit = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry_price - (risk_per_unit * next_r) - fee_offset
        if favorable_price > trigger:
            break
        if math.isclose(next_r, first_trigger_r):
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


def flatten_split_metrics(variant: StrategyVariant, trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, object]:
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
        raw = reason.removeprefix("locked_").removesuffix("r_stop")
        return f"{raw}R"
    return reason


def flatten_reason_counts(variant: StrategyVariant, trades: pd.DataFrame) -> list[dict[str, object]]:
    if trades.empty:
        return []
    counts = trades["exit_reason"].value_counts()
    total = len(trades)
    rows: list[dict[str, object]] = []
    for reason, count in counts.items():
        rows.append(
            {
                "variant_key": variant.key,
                "variant_label": variant.label,
                "exit_reason": str(reason),
                "exit_label": reason_label(str(reason)),
                "count": int(count),
                "ratio": float(count / total),
            }
        )
    return rows


def build_reason_summary(trades: pd.DataFrame) -> dict[str, int]:
    if trades.empty:
        return {}
    counts = trades["exit_reason"].value_counts()
    return {reason_label(str(reason)): int(count) for reason, count in counts.items()}


def save_test_pnl_chart(comparison: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar(comparison["variant_label"], comparison["test_total_pnl_u"], color=["#1d4ed8", "#0f766e", "#b45309"])
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("测试段总盈亏对比")
    ax.set_ylabel("总盈亏 U")
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_equity_chart(trade_map: dict[str, pd.DataFrame], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = {"be2_ladder": "#1d4ed8", "be1_ladder": "#0f766e", "slope_exit": "#b45309"}
    for variant in VARIANTS:
        trades = trade_map[variant.key]
        if trades.empty:
            continue
        curve = trades["pnl_u"].cumsum()
        ax.plot(np.arange(1, len(curve) + 1), curve, label=variant.label, color=colors[variant.key], linewidth=2)
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("全样本累计盈亏曲线")
    ax.set_xlabel("交易序号")
    ax.set_ylabel("累计盈亏 U")
    ax.legend()
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def build_html(*, comparison: pd.DataFrame, reasons: pd.DataFrame, trades_by_key: dict[str, pd.DataFrame], summary: dict[str, object]) -> str:
    best_key = str(comparison.iloc[0]["variant_key"])
    best_label = str(comparison.iloc[0]["variant_label"])
    best_metrics = metrics_for_trades(trades_by_key[best_key])
    best_reason_frame = reasons[reasons["variant_key"] == best_key].copy()
    best_reason_text = " | ".join(f"{row.exit_label} {int(row.count)}" for row in best_reason_frame.itertuples(index=False))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 三版本全量对比</title>
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
.wrap {{ max-width:1260px; margin:0 auto; padding:24px 20px 48px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
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
.note {{ color:var(--muted); font-size:13px; }}
@media (max-width:920px) {{
  .grid-4,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:24px 18px; }}
  .wrap {{ padding:18px 12px 36px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>R001 三种交易版本全量对比</h1>
  <p>本次统一使用同一套入场与风险框架：<strong>EMA55 斜率≤-0.0005 + 止损2ATR + ATR分位≤50% + 每笔风险10U</strong>。第三种策略按同一框架处理，只把出场改成“斜率转正就平仓”。</p>
  <p>对比的三种出场分别是：<strong>2R保本后逐级锁盈</strong>、<strong>1R保本后逐级锁盈</strong>、<strong>斜率转正就平仓</strong>。报告同时展示收益表现和退出原因结构。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("当前最优", html.escape(best_label), "按样本外评分排序")}
    {kpi("最佳测试盈亏", f"{float(comparison.iloc[0]['test_total_pnl_u']):.1f}U", f"测试 PF {float(comparison.iloc[0]['test_profit_factor']):.2f}")}
    {kpi("最佳全样本盈亏", f"{best_metrics['total_pnl_u']:.1f}U", f"最大回撤 {best_metrics['max_drawdown_u']:.1f}U")}
    {kpi("最佳版本原因", best_reason_frame.iloc[0]['exit_label'] if not best_reason_frame.empty else "-", "下方有完整原因分布")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这三种版本里，<span class="good">{html.escape(best_label)}</span> 当前综合表现最好。它不只是测试段更高，也需要看验证段有没有同步站住。退出原因上，最佳版本的分布是：<strong>{html.escape(best_reason_text)}</strong>。这类分布能告诉我们，这套策略到底是靠少量大单撑起来，还是靠大量保本与小额锁盈慢慢累积出来。
  </div>

  <h2>收益对比</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>测试段总盈亏</h3>
      {image_tag(CHART_TEST_PNL)}
    </div>
    <div class="card imgbox">
      <h3>全样本累计盈亏曲线</h3>
      {image_tag(CHART_EQUITY)}
    </div>
  </div>

  <div class="card">
    {dataframe_table(
        comparison,
        [
            ("variant_label", "策略"),
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
  </div>

  <h2>退出原因统计</h2>
  <div class="card">
    <p class="note">示例口径像“保本 6 | 2R 13 | 3R 6 | 4R 3 | 5R 3 | 9R 1 | 止损 36”，本报告会按每个版本分别给出完整分布。</p>
    {dataframe_table(
        reasons.sort_values(["variant_label", "count"], ascending=[True, False]),
        [
            ("variant_label", "策略"),
            ("exit_label", "退出原因"),
            ("count", "次数"),
            ("ratio", "占比"),
        ],
    )}
  </div>

  <h2>最佳版本原因拆解</h2>
  <div class="card">
    <p><strong>{html.escape(best_label)}</strong> 的退出原因统计：</p>
    <p>{html.escape(best_reason_text)}</p>
    <p class="note">如果你关心的是“它为什么赚钱”，这组统计最有用。保本太多说明趋势延续不足；高R锁盈太多说明这套出场抓趋势能力强；止损太多说明入场过滤还需要继续优化。</p>
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
</div>
"""


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


if __name__ == "__main__":
    main()
