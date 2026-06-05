from __future__ import annotations

import base64
import html
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
BAR = "1H"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
EMA55_SLOPE_THRESHOLD = -0.0005
STOP_ATR_MULTIPLIER = 2.0
ATR_PERCENTILE_MAX = 0.50
ATR_PERCENTILE_LOOKBACK = 100
VOLUME_PERCENTILE_LOOKBACK = 100
FIXED_RISK_AMOUNT = 10.0
TAKER_FEE_RATE = 0.00036

MONTHLY_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_5coins_monthly_10u.csv"
YEARLY_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_5coins_yearly_10u.csv"
TRADES_CSV_PATH = REPORT_DIR / "r001_fixed_baseline_5coins_trades_10u.csv"
HTML_PATH = REPORT_DIR / "r001_fixed_baseline_5coins_monthly_yearly_10u_report.html"
CHART_MONTHLY_TOTAL = REPORT_DIR / "r001_fixed_baseline_5coins_monthly_total_10u.png"
CHART_YEARLY_TOTAL = REPORT_DIR / "r001_fixed_baseline_5coins_yearly_total_10u.png"


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    volume_min_pct: float | None = None


BASELINE = Variant("baseline", "EMA55 斜率做空")


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    trade_rows: list[dict[str, object]] = []
    data_ranges: dict[str, dict[str, object]] = {}

    for symbol in SYMBOLS:
        candles = load_candle_cache(symbol, BAR, limit=None)
        if not candles:
            continue
        frame = build_frame(candles)
        add_indicators(frame)
        data_ranges[symbol] = {
            "candles": len(frame),
            "start_utc": format_ts(int(frame["ts"].iloc[0])),
            "end_utc": format_ts(int(frame["ts"].iloc[-1])),
        }
        trades = simulate_trades(frame, BASELINE)
        if trades.empty:
            continue
        trades["symbol"] = symbol
        trades["coin"] = symbol.replace("-USDT-SWAP", "")
        trades["exit_time"] = pd.to_datetime(trades["exit_ts"], unit="ms", utc=True)
        trades["year"] = trades["exit_time"].dt.strftime("%Y")
        trades["month"] = trades["exit_time"].dt.strftime("%Y-%m")
        trade_rows.extend(trades.to_dict("records"))

    trades_df = pd.DataFrame(trade_rows)
    trades_df = trades_df.sort_values(["coin", "exit_ts"]).reset_index(drop=True)
    trades_df.to_csv(TRADES_CSV_PATH, index=False, encoding="utf-8-sig")

    monthly = build_period_table(trades_df, "month")
    yearly = build_period_table(trades_df, "year")
    monthly.to_csv(MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    yearly.to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")

    save_monthly_total_chart(monthly)
    save_yearly_total_chart(yearly)

    HTML_PATH.write_text(build_html(monthly, yearly, trades_df, data_ranges), encoding="utf-8")
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
            "volume": float(candle.volume),
        }
        for candle in candles
    ]
    return pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True)


def add_indicators(df: pd.DataFrame) -> None:
    df["ema55"] = df["close"].ewm(span=55, adjust=False, min_periods=55).mean()
    prev_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr14"] = true_range.rolling(14, min_periods=14).mean()
    df["atr_pct"] = rolling_percentile(df["atr14"], ATR_PERCENTILE_LOOKBACK)
    df["volume_pct"] = rolling_percentile(df["volume"], VOLUME_PERCENTILE_LOOKBACK)


def rolling_percentile(series: pd.Series, lookback: int) -> pd.Series:
    return series.rolling(lookback, min_periods=lookback).apply(lambda x: float(np.mean(x <= x[-1])), raw=True)


def candle_path_points(row: pd.Series) -> tuple[float, float, float, float]:
    if float(row["close"]) >= float(row["open"]):
        return float(row["open"]), float(row["low"]), float(row["high"]), float(row["close"])
    return float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])


def simulate_trades(df: pd.DataFrame, variant: Variant) -> pd.DataFrame:
    trades: list[dict[str, object]] = []
    position: dict[str, float | int | str] | None = None
    start_index = max(ATR_PERCENTILE_LOOKBACK, VOLUME_PERCENTILE_LOOKBACK)

    for index in range(start_index, len(df)):
        row = df.iloc[index]
        current_ema = finite(row["ema55"])
        prev_ema = finite(df.iloc[index - 1]["ema55"])
        atr_value = finite(row["atr14"])
        atr_pct = finite(row["atr_pct"])
        volume_pct = finite(row["volume_pct"])
        if any(math.isnan(value) for value in [current_ema, prev_ema, atr_value, atr_pct, volume_pct]):
            continue

        slope_ratio = (current_ema - prev_ema) / current_ema if current_ema else math.nan

        if position is not None:
            path = candle_path_points(row)
            exited = False
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
            if exited:
                continue

        if position is not None:
            continue
        if slope_ratio > EMA55_SLOPE_THRESHOLD:
            continue
        if atr_pct > ATR_PERCENTILE_MAX:
            continue
        if variant.volume_min_pct is not None and volume_pct < variant.volume_min_pct:
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
            "entry_volume_pct": volume_pct,
        }

    return pd.DataFrame(trades)


def advance_step_dynamic(position: dict[str, float | int | str], favorable_price: float) -> None:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    fee_offset = float(position["fee_offset"])
    while True:
        next_r = float(position["next_dynamic_r"])
        trigger = entry - risk * next_r - fee_offset
        if favorable_price > trigger:
            break
        if math.isclose(next_r, 2.0):
            locked_r = 0.0
            reason = "break_even_stop"
        else:
            locked_r = max(next_r - 1.0, 0.0)
            reason = f"locked_{int(round(locked_r))}r_stop"
        candidate_stop = entry - risk * locked_r - fee_offset
        if candidate_stop < float(position["stop"]):
            position["stop"] = candidate_stop
            position["stop_reason"] = reason
        position["next_dynamic_r"] = next_r + 1.0


def close_trade(position: dict[str, float | int | str], exit_index: int, exit_ts: int, exit_price: float, exit_reason: str) -> dict[str, object]:
    entry = float(position["entry_price"])
    risk = float(position["risk_per_unit"])
    quantity = FIXED_RISK_AMOUNT / risk if risk > 0 else 0.0
    pnl_per_unit = (entry - exit_price) - TAKER_FEE_RATE * (entry + exit_price)
    pnl_u = pnl_per_unit * quantity
    return {
        "entry_index": int(position["entry_index"]),
        "exit_index": exit_index,
        "entry_ts": int(position["entry_ts"]),
        "exit_ts": exit_ts,
        "pnl_u": pnl_u,
        "r_multiple": pnl_u / FIXED_RISK_AMOUNT,
        "hold_hours": (exit_ts - int(position["entry_ts"])) / (1000 * 3600),
        "exit_reason": exit_reason,
        "entry_volume_pct": float(position["entry_volume_pct"]),
    }


def build_period_table(trades_df: pd.DataFrame, period_col: str) -> pd.DataFrame:
    grouped = (
        trades_df.groupby(["coin", period_col], as_index=False)
        .agg(
            trades=("pnl_u", "size"),
            total_pnl_u=("pnl_u", "sum"),
            win_rate=("pnl_u", lambda s: float((s > 0).mean())),
            avg_r=("r_multiple", "mean"),
            avg_hold_hours=("hold_hours", "mean"),
            gross_profit=("pnl_u", lambda s: float(s[s > 0].sum())),
            gross_loss=("pnl_u", lambda s: float(s[s <= 0].sum())),
            big_win_3r_count=("r_multiple", lambda s: float((s >= 3.0).sum())),
            big_win_5r_count=("r_multiple", lambda s: float((s >= 5.0).sum())),
        )
    )
    grouped["profit_factor"] = grouped.apply(
        lambda row: row["gross_profit"] / abs(row["gross_loss"]) if row["gross_loss"] < 0 else 0.0,
        axis=1,
    )
    grouped["coin"] = grouped["coin"].astype(str)

    total = (
        trades_df.groupby(period_col, as_index=False)
        .agg(
            trades=("pnl_u", "size"),
            total_pnl_u=("pnl_u", "sum"),
            win_rate=("pnl_u", lambda s: float((s > 0).mean())),
            avg_r=("r_multiple", "mean"),
            avg_hold_hours=("hold_hours", "mean"),
            gross_profit=("pnl_u", lambda s: float(s[s > 0].sum())),
            gross_loss=("pnl_u", lambda s: float(s[s <= 0].sum())),
            big_win_3r_count=("r_multiple", lambda s: float((s >= 3.0).sum())),
            big_win_5r_count=("r_multiple", lambda s: float((s >= 5.0).sum())),
        )
    )
    total["profit_factor"] = total.apply(
        lambda row: row["gross_profit"] / abs(row["gross_loss"]) if row["gross_loss"] < 0 else 0.0,
        axis=1,
    )
    total.insert(0, "coin", "ALL")

    result = pd.concat([grouped, total], ignore_index=True)
    result = result.rename(columns={period_col: "period"})
    return result.sort_values(["period", "coin"]).reset_index(drop=True)


def save_monthly_total_chart(monthly: pd.DataFrame) -> None:
    total = monthly[monthly["coin"] == "ALL"].copy().sort_values("period")
    ax = total.plot(x="period", y="total_pnl_u", kind="bar", figsize=(14, 4.8), legend=False, color="#1746a2")
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币合计月度盈亏")
    ax.set_xlabel("")
    ax.set_ylabel("月度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()
    plt.savefig(CHART_MONTHLY_TOTAL, dpi=160)
    plt.close()


def save_yearly_total_chart(yearly: pd.DataFrame) -> None:
    total = yearly[yearly["coin"] == "ALL"].copy().sort_values("period")
    ax = total.plot(x="period", y="total_pnl_u", kind="bar", figsize=(8, 4.8), legend=False, color="#d97706")
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币合计年度盈亏")
    ax.set_xlabel("")
    ax.set_ylabel("年度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(CHART_YEARLY_TOTAL, dpi=160)
    plt.close()


def build_html(monthly: pd.DataFrame, yearly: pd.DataFrame, trades_df: pd.DataFrame, data_ranges: dict[str, dict[str, object]]) -> str:
    monthly_total = monthly[monthly["coin"] == "ALL"].copy().sort_values("period")
    yearly_total = yearly[yearly["coin"] == "ALL"].copy().sort_values("period")
    best_month = monthly_total.loc[monthly_total["total_pnl_u"].idxmax()] if not monthly_total.empty else None
    worst_month = monthly_total.loc[monthly_total["total_pnl_u"].idxmin()] if not monthly_total.empty else None
    best_year = yearly_total.loc[yearly_total["total_pnl_u"].idxmax()] if not yearly_total.empty else None
    total_pnl = float(trades_df["pnl_u"].sum()) if not trades_df.empty else 0.0
    total_trades = int(len(trades_df))

    monthly_rows = "".join(period_row(row) for row in monthly.to_dict("records"))
    yearly_rows = "".join(period_row(row) for row in yearly.to_dict("records"))
    monthly_pivot_rows = render_pivot_rows(monthly, "month")
    yearly_pivot_rows = render_pivot_rows(yearly, "year")

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>EMA55 斜率做空五币月度年度盈亏报告</title>
  <style>
    :root {{
      --ink:#172033; --muted:#64748b; --line:#e2e8f0; --blue:#1746a2; --orange:#d97706; --green:#15803d; --red:#b91c1c;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Noto Sans CJK SC",sans-serif; color:var(--ink);
      background:radial-gradient(circle at top left,#e0f2fe 0,#fffaf2 36%,#f8fafc 100%); }}
    header {{ padding:36px 42px 18px; }}
    h1 {{ margin:0 0 10px; font-size:30px; }}
    h2 {{ margin:26px 0 12px; font-size:21px; }}
    p {{ line-height:1.75; }}
    .sub {{ color:var(--muted); }}
    .wrap {{ padding:0 42px 42px; }}
    .cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin:18px 0 20px; }}
    .card, .panel {{ background:rgba(255,255,255,.92); border:1px solid var(--line); border-radius:20px; padding:18px; box-shadow:0 16px 42px rgba(15,23,42,.07); }}
    .k {{ color:var(--muted); font-size:13px; }}
    .v {{ margin-top:8px; font-size:24px; font-weight:800; }}
    .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
    img {{ width:100%; border-radius:16px; border:1px solid var(--line); background:white; }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 9px; border-bottom:1px solid var(--line); text-align:right; font-size:13px; white-space:nowrap; }}
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; position:sticky; top:0; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .table-wrap {{ max-height:520px; overflow:auto; border-radius:16px; border:1px solid var(--line); }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>EMA55 斜率做空：五币月度 / 年度盈亏明细</h1>
    <p class="sub">口径：BTC / ETH / SOL / BNB / DOGE 永续，1小时，固定每笔风险 10U。条件保持不变：EMA55 斜率 ≤ -0.0005，2ATR 止损，2R 保本后逐级锁盈，ATR 分位 ≤ 50%。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">五币总收益</div><div class="v">{fmt(total_pnl)}U</div></div>
      <div class="card"><div class="k">总交易数</div><div class="v">{total_trades}</div></div>
      <div class="card"><div class="k">最佳月份</div><div class="v">{best_month['period'] if best_month is not None else '-'}</div><div class="sub">{fmt(best_month['total_pnl_u']) if best_month is not None else '-'}U</div></div>
      <div class="card"><div class="k">最差月份</div><div class="v">{worst_month['period'] if worst_month is not None else '-'}</div><div class="sub">{fmt(worst_month['total_pnl_u']) if worst_month is not None else '-'}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_MONTHLY_TOTAL)}" alt="月度盈亏" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_YEARLY_TOTAL)}" alt="年度盈亏" /></div>
    </section>

    <section class="panel">
      <h2>月度汇总透视</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>月份</th><th>ALL</th><th>BTC</th><th>ETH</th><th>SOL</th><th>BNB</th><th>DOGE</th></tr></thead>
          <tbody>{monthly_pivot_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>年度汇总透视</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>年份</th><th>ALL</th><th>BTC</th><th>ETH</th><th>SOL</th><th>BNB</th><th>DOGE</th></tr></thead>
          <tbody>{yearly_pivot_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>月度明细表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>币种</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{monthly_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>年度明细表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>币种</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{yearly_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>数据文件</h2>
      <p>月度 CSV：{html.escape(str(MONTHLY_CSV_PATH))}</p>
      <p>年度 CSV：{html.escape(str(YEARLY_CSV_PATH))}</p>
      <p>交易明细 CSV：{html.escape(str(TRADES_CSV_PATH))}</p>
      <p class="sub">{html.escape(render_data_ranges(data_ranges))}</p>
      <p class="sub">年度最佳：{best_year['period'] if best_year is not None else '-'} / {fmt(best_year['total_pnl_u']) if best_year is not None else '-'}U</p>
    </section>
  </main>
</body>
</html>"""


def render_pivot_rows(period_df: pd.DataFrame, period_label: str) -> str:
    pivot = period_df.pivot(index="period", columns="coin", values="total_pnl_u").fillna(0.0)
    ordered_coins = ["ALL", "BTC", "ETH", "SOL", "BNB", "DOGE"]
    rows = []
    for period, row in pivot.sort_index().iterrows():
        cells = [f"<td>{html.escape(str(period))}</td>"]
        for coin in ordered_coins:
            value = float(row.get(coin, 0.0))
            cells.append(f"<td class=\"{cls(value)}\">{fmt(value)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    return "".join(rows)


def period_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['coin']))}</td>"
        f"<td>{html.escape(str(row['period']))}</td>"
        f"<td>{int(row['trades'])}</td>"
        f"<td class=\"{cls(pnl)}\">{fmt(pnl)}</td>"
        f"<td>{fmt(row['profit_factor'], 3)}</td>"
        f"<td>{pct(row['win_rate'])}</td>"
        f"<td class=\"{cls(float(row['avg_r']))}\">{fmt(row['avg_r'], 3)}</td>"
        f"<td>{fmt(row['avg_hold_hours'], 1)}</td>"
        f"<td>{int(row['big_win_3r_count'])}</td>"
        f"<td>{int(row['big_win_5r_count'])}</td>"
        "</tr>"
    )


def render_data_ranges(data_ranges: dict[str, dict[str, object]]) -> str:
    parts = []
    for symbol in SYMBOLS:
        item = data_ranges.get(symbol, {})
        parts.append(
            f"{symbol.replace('-USDT-SWAP', '')}: {item.get('candles', '-')}根，{item.get('start_utc', '-')} 到 {item.get('end_utc', '-')}"
        )
    return "；".join(parts)


def finite(value: object) -> float:
    result = float(value) if value is not None else math.nan
    return result if np.isfinite(result) else math.nan


def image_b64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def fmt(value: object, digits: int = 1) -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if math.isnan(number):
        return "-"
    return f"{number:,.{digits}f}"


def pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def cls(value: float) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


def format_ts(ts: int) -> str:
    seconds = ts / 1000 if ts >= 10**12 else ts
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
