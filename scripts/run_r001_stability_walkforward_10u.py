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

CSV_YEARLY = REPORT_DIR / "r001_stability_yearly_10u.csv"
CSV_WALK = REPORT_DIR / "r001_stability_walkforward_10u.csv"
JSON_PATH = REPORT_DIR / "r001_stability_walkforward_10u_summary.json"
HTML_PATH = REPORT_DIR / "r001_stability_walkforward_10u_report.html"
CHART_EQUITY = REPORT_DIR / "r001_stability_walkforward_10u_equity.png"
CHART_YEARLY = REPORT_DIR / "r001_stability_walkforward_10u_yearly.png"
CHART_WALK = REPORT_DIR / "r001_stability_walkforward_10u_walkforward.png"


@dataclass(frozen=True)
class StrategyConfig:
    slope_threshold: float = EMA55_SLOPE_THRESHOLD
    stop_atr_mult: float = STOP_ATR_MULTIPLIER
    atr_pct_max: float = ATR_PERCENTILE_MAX


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    candles = load_candle_cache(INST_ID, BAR, limit=None)
    if not candles:
        raise RuntimeError(f"no candles found for {INST_ID} {BAR}")

    df = build_frame(candles)
    add_indicators(df)
    config = StrategyConfig()
    trades = simulate_trades(df, config)
    if trades.empty:
        raise RuntimeError("no trades generated for stability report")

    trades["exit_time"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True)
    trades["entry_time"] = pd.to_datetime(trades["entry_ts"], unit="ms", utc=True)

    yearly = build_yearly_table(trades)
    walk = build_walkforward_table(trades)
    summary = build_summary(df, trades, yearly, walk)

    yearly.to_csv(CSV_YEARLY, index=False, encoding="utf-8-sig")
    walk.to_csv(CSV_WALK, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    save_equity_chart(trades, CHART_EQUITY)
    save_yearly_chart(yearly, CHART_YEARLY)
    save_walk_chart(walk, CHART_WALK)

    HTML_PATH.write_text(build_html(yearly=yearly, walk=walk, summary=summary), encoding="utf-8")
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


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
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
        if fast_slope_ratio > config.slope_threshold:
            continue
        if atr_pct > config.atr_pct_max:
            continue

        risk_per_unit = atr_value * config.stop_atr_mult
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


def build_yearly_table(trades: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year, group in trades.groupby(trades["exit_time"].dt.year):
        metrics = metrics_for_trades(group)
        row = {"year": str(int(year))}
        row.update(metrics)
        rows.append(row)
    return pd.DataFrame(rows)


def build_walkforward_table(trades: pd.DataFrame) -> pd.DataFrame:
    first_ts = trades["exit_time"].min()
    start_month = 1 if first_ts.month <= 6 else 7
    start = pd.Timestamp(year=first_ts.year, month=start_month, day=1, tz="UTC")
    end = trades["exit_time"].max()
    current = start
    rows: list[dict[str, object]] = []
    while current <= end:
        next_boundary = (current + pd.DateOffset(months=6))
        window = trades[(trades["exit_time"] >= current) & (trades["exit_time"] < next_boundary)].copy()
        metrics = metrics_for_trades(window)
        row = {
            "window_label": f"{current.strftime('%Y-%m')} to {(next_boundary - pd.Timedelta(seconds=1)).strftime('%Y-%m')}",
            "start_utc": current.strftime("%Y-%m-%d"),
            "end_utc": (next_boundary - pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        }
        row.update(metrics)
        rows.append(row)
        current = next_boundary
    return pd.DataFrame(rows)


def build_summary(df: pd.DataFrame, trades: pd.DataFrame, yearly: pd.DataFrame, walk: pd.DataFrame) -> dict[str, object]:
    all_metrics = metrics_for_trades(trades)
    positive_years = int((yearly["total_pnl_u"] > 0).sum()) if not yearly.empty else 0
    negative_years = int((yearly["total_pnl_u"] <= 0).sum()) if not yearly.empty else 0
    positive_walk = int((walk["total_pnl_u"] > 0).sum()) if not walk.empty else 0
    negative_walk = int((walk["total_pnl_u"] <= 0).sum()) if not walk.empty else 0
    return {
        "data_start_utc": df["timestamp"].iloc[0].strftime("%Y-%m-%d %H:%M UTC"),
        "data_end_utc": df["timestamp"].iloc[-1].strftime("%Y-%m-%d %H:%M UTC"),
        "config": {
            "ema55_slope_threshold": EMA55_SLOPE_THRESHOLD,
            "stop_atr_mult": STOP_ATR_MULTIPLIER,
            "exit_mode": "2R保本后逐级锁盈",
            "atr_pct_max": ATR_PERCENTILE_MAX,
            "atr_percentile_lookback": ATR_PERCENTILE_LOOKBACK,
            "risk_per_trade_u": FIXED_RISK_AMOUNT,
        },
        "all_metrics": all_metrics,
        "positive_years": positive_years,
        "negative_years": negative_years,
        "positive_walk_windows": positive_walk,
        "negative_walk_windows": negative_walk,
        "best_year": yearly.sort_values("total_pnl_u", ascending=False).iloc[0].to_dict() if not yearly.empty else {},
        "worst_year": yearly.sort_values("total_pnl_u", ascending=True).iloc[0].to_dict() if not yearly.empty else {},
        "best_walk": walk.sort_values("total_pnl_u", ascending=False).iloc[0].to_dict() if not walk.empty else {},
        "worst_walk": walk.sort_values("total_pnl_u", ascending=True).iloc[0].to_dict() if not walk.empty else {},
    }


def save_equity_chart(trades: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    curve = trades["pnl_u"].cumsum()
    ax.plot(trades["exit_time"], curve, color="#1d4ed8", linewidth=2)
    ax.axhline(0, color="#64748b", linewidth=1, linestyle="--")
    ax.set_title("全样本累计盈亏曲线")
    ax.set_ylabel("累计盈亏 U")
    ax.grid(alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_yearly_chart(yearly: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    colors = ["#0f766e" if value > 0 else "#be123c" for value in yearly["total_pnl_u"]]
    ax.bar(yearly["year"], yearly["total_pnl_u"], color=colors)
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("年度总盈亏")
    ax.set_ylabel("总盈亏 U")
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_walk_chart(walk: pd.DataFrame, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 5.5))
    colors = ["#0f766e" if value > 0 else "#be123c" for value in walk["total_pnl_u"]]
    ax.bar(walk["window_label"], walk["total_pnl_u"], color=colors)
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.set_title("半年 Walk-Forward 总盈亏")
    ax.set_ylabel("总盈亏 U")
    ax.tick_params(axis="x", rotation=28)
    ax.grid(axis="y", alpha=0.22)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def build_html(*, yearly: pd.DataFrame, walk: pd.DataFrame, summary: dict[str, object]) -> str:
    all_metrics = summary["all_metrics"]
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>R001 稳定性 Walk-Forward 报告</title>
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
  <h1>R001 稳定性验证：年度 + Walk-Forward</h1>
  <p>这份报告不再继续扫参数，而是拿当前最优工作组合直接做稳定性审查：<strong>EMA55 斜率 ≤ -0.0005 + 止损 2ATR + 2R保本后逐级锁盈 + ATR分位≤50% + 每笔风险10U</strong>。</p>
  <p>目标很直接：确认这套组合是不是只在个别年份表现亮眼，还是跨年份、跨半年窗口都能维持正向 edge。</p>
</section>
<main class="wrap">
  <div class="grid grid-4">
    {kpi("全样本总盈亏", f"{all_metrics['total_pnl_u']:.1f}U", f"全样本 PF {all_metrics['profit_factor']:.2f}")}
    {kpi("正收益年份", str(summary['positive_years']), f"负收益年份 {summary['negative_years']}")}
    {kpi("正收益半年窗", str(summary['positive_walk_windows']), f"负收益半年窗 {summary['negative_walk_windows']}")}
    {kpi("全样本最大回撤", f"{all_metrics['max_drawdown_u']:.1f}U", f"全样本 {int(all_metrics['trades'])} 笔")}
  </div>

  <h2>结论先看</h2>
  <div class="card answer">
    这轮稳定性验证的重点不是“还能不能再更高”，而是“这套东西靠不靠谱”。如果正收益年份和正收益半年窗都占多数，同时最差窗口的亏损没有失控，那它就更接近一个可继续工程化的组合。反过来，如果收益只集中在少数年份，或者连续半年窗大面积失效，那就说明它的 edge 还不够稳。
  </div>

  <div class="grid grid-3">
    <div class="card">
      <h3>年度视角看什么</h3>
      <p>年度拆分主要看这条策略是不是只在极端单边行情好用。如果连普通年份也能维持正收益，它的适应性会更强。</p>
    </div>
    <div class="card">
      <h3>半年窗看什么</h3>
      <p>半年 walk-forward 更接近真实交易体验，因为它会把“看起来全年还行，但中间其实长时间失效”的问题暴露出来。</p>
    </div>
    <div class="card">
      <h3>怎么继续用结果</h3>
      <p>如果这份报告显示稳定性还不错，下一步就不是继续扫参数，而是可以开始做更接近实盘的资金曲线和回撤容忍度设计。</p>
    </div>
  </div>

  <h2>图表</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>全样本累计盈亏</h3>
      {image_tag(CHART_EQUITY)}
    </div>
    <div class="card imgbox">
      <h3>年度总盈亏</h3>
      {image_tag(CHART_YEARLY)}
    </div>
  </div>

  <div class="card imgbox">
    <h3>半年 Walk-Forward 总盈亏</h3>
    {image_tag(CHART_WALK)}
  </div>

  <h2>年度表现</h2>
  <div class="card">
    {dataframe_table(
        yearly,
        [
            ("year", "年份"),
            ("trades", "交易数"),
            ("total_pnl_u", "总盈亏U"),
            ("avg_pnl_u", "平均盈亏U"),
            ("avg_r", "Avg R"),
            ("profit_factor", "PF"),
            ("win_rate", "胜率"),
            ("max_drawdown_u", "最大回撤U"),
        ],
    )}
  </div>

  <h2>半年 Walk-Forward</h2>
  <div class="card">
    {dataframe_table(
        walk,
        [
            ("window_label", "窗口"),
            ("trades", "交易数"),
            ("total_pnl_u", "总盈亏U"),
            ("avg_pnl_u", "平均盈亏U"),
            ("avg_r", "Avg R"),
            ("profit_factor", "PF"),
            ("win_rate", "胜率"),
            ("max_drawdown_u", "最大回撤U"),
        ],
    )}
    <p class="note">样本范围：{html.escape(str(summary['data_start_utc']))} 到 {html.escape(str(summary['data_end_utc']))}。最佳年份：{html.escape(str(summary['best_year'].get('year', '-')))}，最差年份：{html.escape(str(summary['worst_year'].get('year', '-')))}。</p>
  </div>

  <h2>下一步建议</h2>
  <div class="card">
    <div class="callout">
      <strong>建议顺序</strong><br>
      1. 如果年度和半年窗都大体稳定，就把这套参数视作当前主版本。<br>
      2. 如果半年窗分化很大，优先做资金曲线降杠杆或停手机制，而不是再继续堆过滤条件。<br>
      3. 如果你还想再往前一步，我建议下一轮直接做“实盘可执行版本说明”，把参数、风控和启停条件写成一份操作手册。
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
    if "avg_r" in lower or "avg_pnl_u" in lower:
        return f"{number:.3f}"
    if "profit_factor" in lower:
        return f"{number:.2f}"
    if "win_rate" in lower:
        return f"{number * 100:.1f}%"
    if "trades" in lower:
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


if __name__ == "__main__":
    main()
