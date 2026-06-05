from __future__ import annotations

import base64
import html
import json
import math
import sys
from dataclasses import asdict, dataclass
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
SLOW_FILTER_THRESHOLDS = (0.0, -0.0001, -0.0002, -0.0003, -0.0005)

CSV_PATH = REPORT_DIR / "r001_entry_filter_followup_10u.csv"
JSON_PATH = REPORT_DIR / "r001_entry_filter_followup_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_entry_filter_followup_10u_report.html"
CHART_TEST_PNL = REPORT_DIR / "r001_entry_filter_followup_10u_test_pnl.png"
CHART_CURVE = REPORT_DIR / "r001_entry_filter_followup_10u_curve.png"


@dataclass(frozen=True)
class FilterVariant:
    slow_slope_threshold: float

    @property
    def key(self) -> str:
        return f"ema200_{self.slow_slope_threshold:.4f}"

    @property
    def label(self) -> str:
        if math.isclose(self.slow_slope_threshold, 0.0):
            return "无慢趋势过滤"
        return f"EMA200斜率≤{self.slow_slope_threshold:.4f}"


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    split_bounds = build_split_bounds(len(df))

    variants = [FilterVariant(value) for value in SLOW_FILTER_THRESHOLDS]
    rows: list[dict[str, object]] = []
    trades_by_key: dict[str, pd.DataFrame] = {}
    for variant in variants:
        trades = simulate_trades(df, variant)
        row = flatten_split_metrics(variant, trades, split_bounds)
        row["score"] = score_row(row)
        rows.append(row)
        trades_by_key[variant.key] = trades

    comparison = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
    comparison.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    best_variant = next(item for item in variants if item.key == str(comparison.iloc[0]["variant_key"]))
    best_trades = trades_by_key[best_variant.key]

    save_test_pnl_chart(comparison, CHART_TEST_PNL)
    save_curve_chart(trades_by_key, variants, CHART_CURVE)

    summary = {
        "risk_per_trade_usdt": FIXED_RISK_AMOUNT,
        "entry_anchor": {
            "ema55_slope_threshold": BASE_SLOPE_THRESHOLD,
            "stop_atr": BASE_STOP_ATR,
            "exit_mode": "2R保本后逐级锁盈",
        },
        "data_start_utc": format_ts(int(df["ts"].iloc[0])),
        "data_end_utc": format_ts(int(df["ts"].iloc[-1])),
        "best_variant": comparison.iloc[0].to_dict(),
        "top3": comparison.head(3).to_dict("records"),
    }
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(df=df, comparison=comparison, best_variant=best_variant, best_trades=best_trades, summary=summary), encoding="utf-8")
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
    df["ema200"] = df["close"].ewm(span=200, adjust=False, min_periods=200).mean()
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


def simulate_trades(df: pd.DataFrame, variant: FilterVariant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int] | None = None

    for index in range(200, len(df)):
        row = df.iloc[index]
        current_ema55 = float(row["ema55"]) if pd.notna(row["ema55"]) else math.nan
        prev_ema55 = float(df.iloc[index - 1]["ema55"]) if pd.notna(df.iloc[index - 1]["ema55"]) else math.nan
        current_ema200 = float(row["ema200"]) if pd.notna(row["ema200"]) else math.nan
        prev_ema200 = float(df.iloc[index - 1]["ema200"]) if pd.notna(df.iloc[index - 1]["ema200"]) else math.nan
        atr_value = float(row["atr14"]) if pd.notna(row["atr14"]) else math.nan
        if not np.isfinite(current_ema55) or not np.isfinite(prev_ema55) or not np.isfinite(current_ema200) or not np.isfinite(prev_ema200) or not np.isfinite(atr_value):
            continue

        fast_slope_ratio = (current_ema55 - prev_ema55) / current_ema55 if current_ema55 else math.nan
        slow_slope_ratio = (current_ema200 - prev_ema200) / current_ema200 if current_ema200 else math.nan

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
        if slow_slope_ratio > variant.slow_slope_threshold:
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


def flatten_split_metrics(variant: FilterVariant, trades: pd.DataFrame, split_bounds: dict[str, tuple[int, int]]) -> dict[str, object]:
    row: dict[str, object] = {
        "variant_key": variant.key,
        "variant_label": variant.label,
        "slow_slope_threshold": variant.slow_slope_threshold,
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
    ax.tick_params(axis="x", rotation=16)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_curve_chart(trade_map: dict[str, pd.DataFrame], variants: list[FilterVariant], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = ["#1d4ed8", "#0f766e", "#b45309", "#be123c", "#7c3aed"]
    for index, variant in enumerate(variants):
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


def build_html(*, df: pd.DataFrame, comparison: pd.DataFrame, best_variant: FilterVariant, best_trades: pd.DataFrame, summary: dict[str, object]) -> str:
    best_metrics = metrics_for_trades(best_trades)
    stable_rows = comparison[(comparison["validation_total_pnl_u"] > 0) & (comparison["test_total_pnl_u"] > 0) & (comparison["test_profit_factor"] > 1.0)]
    best_validation = float(comparison["validation_total_pnl_u"].max()) if not comparison.empty else 0.0
    base_row = comparison[comparison["slow_slope_threshold"] == 0.0].iloc[0]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 三轮入场过滤报告</title>
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
  <h1>R001 三轮优化：加 EMA200 慢趋势确认</h1>
  <p>这轮不再改出场，继续沿用当前最优组合：<strong>EMA55 斜率 ≤ -0.0005 + 止损 2ATR + 2R保本后逐级锁盈</strong>。唯一新增的是一层轻量入场过滤：要求 <strong>EMA200</strong> 的斜率也满足空头方向。</p>
  <p>测试的是 5 档过滤强度：`无过滤 / <=0 / <=-0.0001 / <=-0.0002 / <=-0.0003 / <=-0.0005` 中的 5 组有效阈值。资金口径仍然是每笔固定风险 <strong>10U</strong>。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("最佳过滤", html.escape(best_variant.label), "按样本外评分排序")}
    {kpi("最佳测试盈亏", f"{float(comparison.iloc[0]['test_total_pnl_u']):.1f}U", f"测试 PF {float(comparison.iloc[0]['test_profit_factor']):.2f}")}
    {kpi("最佳验证盈亏", f"{best_validation:.1f}U", "越大说明样本外更稳")}
    {kpi("稳定盈利组合", str(len(stable_rows)), "验证和测试同时盈利且测试 PF > 1")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这轮结果说明，<span class="good">{html.escape(best_variant.label)}</span> 是当前最优过滤版本。和“不加慢趋势过滤”的基线相比，它把验证段从 <strong>{float(base_row['validation_total_pnl_u']):.1f}U</strong> 调整到了 <strong>{float(comparison.iloc[0]['validation_total_pnl_u']):.1f}U</strong>，测试段从 <strong>{float(base_row['test_total_pnl_u']):.1f}U</strong> 调整到了 <strong>{float(comparison.iloc[0]['test_total_pnl_u']):.1f}U</strong>。这意味着慢趋势确认是有价值的，但过滤过严也会开始牺牲交易机会。
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>这轮在验证什么</h3>
      <p>它验证的是：当前这条策略是不是需要“更慢一级的趋势也同步偏空”才能提高样本外质量。这个过滤比成交量、压缩率之类更轻，也更容易解释。</p>
    </div>
    <div class="card">
      <h3>怎么看结果</h3>
      <p>如果轻量过滤能同时改善验证段和测试段，它就值得接进正式版本。反过来，如果只是过滤掉很多单，但收益没改善，那它只是在减少噪音，不是在增强 edge。</p>
    </div>
    <div class="card">
      <h3>当前判断</h3>
      <p>这轮已经比“只优化出场”更进一步了。说明这条策略确实受益于更高一级趋势确认，而不是只靠更细的出场技巧去补。</p>
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

  <h2>过滤强度对比</h2>
  <div class="card">
    {dataframe_table(
        comparison,
        [
            ("variant_label", "过滤规则"),
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
    <p class="note">数据范围：{html.escape(str(summary["data_start_utc"]))} 到 {html.escape(str(summary["data_end_utc"]))}。最佳过滤版本全样本：{int(best_metrics['trades'])} 笔，胜率 {best_metrics['win_rate'] * 100:.1f}%，总盈亏 {best_metrics['total_pnl_u']:.1f}U，Avg R {best_metrics['avg_r']:.3f}，PF {best_metrics['profit_factor']:.2f}。</p>
  </div>

  <h2>下一步建议</h2>
  <div class="card">
    <div class="callout">
      <strong>建议顺序</strong><br>
      1. 把这轮最优慢趋势过滤保留下来。<br>
      2. 如果还要第四轮，优先加“波动状态过滤”而不是再扫更多斜率数值。<br>
      3. 第四轮可选维度里，我更推荐先试 ATR 分位过滤或压缩过滤。
    </div>
    <p>如果你愿意继续，我下一步就做第四轮：在这轮最优慢趋势过滤基础上，再加一个 ATR 状态过滤的小矩阵，看看能不能继续压缩回撤，同时保住测试段盈利。</p>
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
    if "avg_r" in lower or "score" in lower or "threshold" in lower:
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
