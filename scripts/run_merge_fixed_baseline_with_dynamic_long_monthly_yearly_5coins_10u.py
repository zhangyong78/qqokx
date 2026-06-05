from __future__ import annotations

import base64
import html
import math
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = ROOT / "reports"
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "BNB-USDT-SWAP", "DOGE-USDT-SWAP")
COIN_ORDER = ["ALL", "BTC", "ETH", "SOL", "BNB", "DOGE"]
BAR = "1H"
RISK_AMOUNT = Decimal("10")
INITIAL_CAPITAL = Decimal("10000")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")

FIXED_MONTHLY_CSV = REPORT_DIR / "r001_fixed_baseline_5coins_monthly_10u.csv"
FIXED_YEARLY_CSV = REPORT_DIR / "r001_fixed_baseline_5coins_yearly_10u.csv"
FIXED_TRADES_CSV = REPORT_DIR / "r001_fixed_baseline_5coins_trades_10u.csv"

DYNAMIC_TRADES_CSV = REPORT_DIR / "dynamic_long_recommended_5coins_trades_10u.csv"
DYNAMIC_MONTHLY_CSV = REPORT_DIR / "dynamic_long_recommended_5coins_monthly_10u.csv"
DYNAMIC_YEARLY_CSV = REPORT_DIR / "dynamic_long_recommended_5coins_yearly_10u.csv"
MERGED_MONTHLY_CSV = REPORT_DIR / "merged_fixed_baseline_dynamic_long_monthly_5coins_10u.csv"
MERGED_YEARLY_CSV = REPORT_DIR / "merged_fixed_baseline_dynamic_long_yearly_5coins_10u.csv"
HTML_PATH = REPORT_DIR / "merged_fixed_baseline_dynamic_long_monthly_yearly_5coins_10u_report.html"
CHART_MONTHLY = REPORT_DIR / "merged_fixed_baseline_dynamic_long_monthly_total_5coins_10u.png"
CHART_YEARLY = REPORT_DIR / "merged_fixed_baseline_dynamic_long_yearly_total_5coins_10u.png"


def main() -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    fixed_monthly = load_fixed_period_csv(FIXED_MONTHLY_CSV, "EMA55 斜率做空", "r001_fixed_baseline")
    fixed_yearly = load_fixed_period_csv(FIXED_YEARLY_CSV, "EMA55 斜率做空", "r001_fixed_baseline")

    client = OkxRestClient()
    dynamic_trades, errors = run_dynamic_long_recommended(client)
    dynamic_trades.to_csv(DYNAMIC_TRADES_CSV, index=False, encoding="utf-8-sig")

    dynamic_monthly = build_period_table(dynamic_trades, "month", "动态委托做多(推荐参数)", "dynamic_long_recommended")
    dynamic_yearly = build_period_table(dynamic_trades, "year", "动态委托做多(推荐参数)", "dynamic_long_recommended")
    dynamic_monthly.to_csv(DYNAMIC_MONTHLY_CSV, index=False, encoding="utf-8-sig")
    dynamic_yearly.to_csv(DYNAMIC_YEARLY_CSV, index=False, encoding="utf-8-sig")

    merged_monthly = pd.concat([fixed_monthly, dynamic_monthly], ignore_index=True).sort_values(
        ["period", "strategy_label", "coin"]
    )
    merged_yearly = pd.concat([fixed_yearly, dynamic_yearly], ignore_index=True).sort_values(
        ["period", "strategy_label", "coin"]
    )
    merged_monthly.to_csv(MERGED_MONTHLY_CSV, index=False, encoding="utf-8-sig")
    merged_yearly.to_csv(MERGED_YEARLY_CSV, index=False, encoding="utf-8-sig")

    save_monthly_chart(merged_monthly)
    save_yearly_chart(merged_yearly)

    HTML_PATH.write_text(build_html(merged_monthly, merged_yearly, dynamic_trades, errors), encoding="utf-8")
    print(HTML_PATH)


def load_fixed_period_csv(path: Path, strategy_label: str, strategy_key: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["period"] = frame["period"].astype(str)
    frame.insert(0, "strategy_key", strategy_key)
    frame.insert(1, "strategy_label", strategy_label)
    return frame


def build_dynamic_config(symbol: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=BAR,
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def run_dynamic_long_recommended(client: OkxRestClient) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    rows: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    for symbol in SYMBOLS:
        print(f"run dynamic long recommended {symbol}")
        instrument = client.get_instrument(symbol)
        candles = [c for c in load_candle_cache(symbol, BAR, limit=None) if c.confirmed]
        try:
            result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                build_dynamic_config(symbol),
                data_source_note=f"local candle_cache full history | {symbol} {BAR} | candles={len(candles)}",
                maker_fee_rate=MAKER_FEE_RATE,
                taker_fee_rate=TAKER_FEE_RATE,
            )
        except Exception as exc:
            errors.append({"symbol": symbol, "coin": symbol.replace("-USDT-SWAP", ""), "error": str(exc)})
            continue
        coin = symbol.replace("-USDT-SWAP", "")
        for trade in result.trades:
            exit_time = pd.to_datetime(int(trade.exit_ts), unit="ms", utc=True)
            hold_hours = (int(trade.exit_ts) - int(trade.entry_ts)) / (1000 * 3600)
            rows.append(
                {
                    "symbol": symbol,
                    "coin": coin,
                    "entry_ts": int(trade.entry_ts),
                    "exit_ts": int(trade.exit_ts),
                    "entry_index": int(trade.entry_index),
                    "exit_index": int(trade.exit_index),
                    "exit_time": exit_time,
                    "year": exit_time.strftime("%Y"),
                    "month": exit_time.strftime("%Y-%m"),
                    "pnl_u": float(trade.pnl),
                    "r_multiple": float(trade.r_multiple),
                    "hold_hours": hold_hours,
                    "exit_reason": str(trade.exit_reason),
                }
            )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame, errors
    return frame.sort_values(["coin", "exit_ts"]).reset_index(drop=True), errors


def build_period_table(trades_df: pd.DataFrame, period_col: str, strategy_label: str, strategy_key: str) -> pd.DataFrame:
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
    result["period"] = result["period"].astype(str)
    result.insert(0, "strategy_key", strategy_key)
    result.insert(1, "strategy_label", strategy_label)
    return result.sort_values(["period", "coin"]).reset_index(drop=True)


def save_monthly_chart(monthly: pd.DataFrame) -> None:
    total = monthly[monthly["coin"] == "ALL"].copy()
    pivot = total.pivot(index="period", columns="strategy_label", values="total_pnl_u").sort_index()
    ax = pivot.plot(kind="bar", figsize=(14, 4.8), width=0.82, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币合计月度盈亏：EMA55 斜率做空 vs 动态委托做多")
    ax.set_xlabel("")
    ax.set_ylabel("月度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.xticks(rotation=60, ha="right")
    plt.tight_layout()
    plt.savefig(CHART_MONTHLY, dpi=160)
    plt.close()


def save_yearly_chart(yearly: pd.DataFrame) -> None:
    total = yearly[yearly["coin"] == "ALL"].copy()
    pivot = total.pivot(index="period", columns="strategy_label", values="total_pnl_u").sort_index()
    ax = pivot.plot(kind="bar", figsize=(8, 4.8), width=0.78, color=["#1746a2", "#d97706"])
    ax.axhline(0, color="#334155", linewidth=0.8)
    ax.set_title("五币合计年度盈亏：EMA55 斜率做空 vs 动态委托做多")
    ax.set_xlabel("")
    ax.set_ylabel("年度盈亏(U)")
    ax.grid(axis="y", alpha=0.22)
    plt.tight_layout()
    plt.savefig(CHART_YEARLY, dpi=160)
    plt.close()


def build_html(
    monthly: pd.DataFrame,
    yearly: pd.DataFrame,
    dynamic_trades: pd.DataFrame,
    errors: list[dict[str, str]],
) -> str:
    monthly_total = monthly[monthly["coin"] == "ALL"].copy()
    yearly_total = yearly[yearly["coin"] == "ALL"].copy()

    fixed_total = float(
        yearly_total[yearly_total["strategy_key"] == "r001_fixed_baseline"]["total_pnl_u"].sum()
    )
    dyn_total = float(
        yearly_total[yearly_total["strategy_key"] == "dynamic_long_recommended"]["total_pnl_u"].sum()
    )
    fixed_best_year = pick_best_period(yearly_total, "r001_fixed_baseline")
    dyn_best_year = pick_best_period(yearly_total, "dynamic_long_recommended")

    monthly_total_rows = render_total_pivot(monthly)
    yearly_total_rows = render_total_pivot(yearly)
    monthly_detail_rows = "".join(period_row(row) for row in monthly.to_dict("records"))
    yearly_detail_rows = "".join(period_row(row) for row in yearly.to_dict("records"))
    error_html = "".join(
        f"<li><b>{html.escape(item['coin'])}</b>：{html.escape(item['error'])}</li>" for item in errors
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>EMA55 斜率做空 + 动态委托做多 月度年度融合报告</title>
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
    th:first-child,td:first-child,th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3) {{ text-align:left; }}
    th {{ background:#f1f5f9; color:#334155; position:sticky; top:0; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .table-wrap {{ max-height:520px; overflow:auto; border-radius:16px; border:1px solid var(--line); }}
    .note {{ padding:14px 16px; border-left:5px solid var(--orange); background:#fffbeb; border-radius:12px; }}
    @media (max-width: 900px) {{ .cards,.grid {{ grid-template-columns:1fr; }} header,.wrap {{ padding-left:18px; padding-right:18px; }} }}
  </style>
</head>
<body>
  <header>
    <h1>EMA55 斜率做空 + 动态委托做多：五币月度 / 年度融合报告</h1>
    <p class="sub">动态做多采用此前推荐过的 1H 参数结构：EMA21，趋势线 MA50，挂单参考 MA50，ATR10，2ATR 止损 / 2ATR 动态止盈，每波最多 1 次，2R 保本，风险统一按 10U。这里我把它和当前 EMA55 斜率做空 的月度 / 年度明细融合到同一份表里，方便直接对照。</p>
  </header>
  <main class="wrap">
    <section class="cards">
      <div class="card"><div class="k">EMA55 斜率做空总收益</div><div class="v">{fmt(fixed_total)}U</div></div>
      <div class="card"><div class="k">动态委托做多总收益</div><div class="v">{fmt(dyn_total)}U</div></div>
      <div class="card"><div class="k">R001最佳年度</div><div class="v">{fixed_best_year[0]}</div><div class="sub">{fmt(fixed_best_year[1])}U</div></div>
      <div class="card"><div class="k">动态做多最佳年度</div><div class="v">{dyn_best_year[0]}</div><div class="sub">{fmt(dyn_best_year[1])}U</div></div>
    </section>

    <section class="grid">
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_MONTHLY)}" alt="月度对比" /></div>
      <div class="panel"><img src="data:image/png;base64,{image_b64(CHART_YEARLY)}" alt="年度对比" /></div>
    </section>

    <section class="panel">
      <h2>融合结论</h2>
      <p>这份表最适合看两件事：第一，两个系统是不是在同一时间段赚钱；第二，谁更依赖牛市、谁更能在弱势或震荡里扛住。EMA55 斜率做空 本质上是空头顺势破位系统，动态委托做多则更依赖上行趋势环境，所以它们的月度曲线天然会错位，这种错位本身就很有价值。</p>
      <p class="note">我这里采用的“以前推荐参数”假设是：`EMA21 + MA50 + 参考MA50 + ATR10 + 2ATR止损/2ATR动态止盈 + 每波1次 + 2R保本`。如果你想改成另外一套你之前认可的做多参数，我可以在这份融合报告上直接替换，不需要重做展示结构。</p>
      {f'<p class="note">本次动态做多全历史迁移里有参数适配问题：<ul>{error_html}</ul></p>' if errors else ''}
    </section>

    <section class="panel">
      <h2>五币合计月度总表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>月份</th><th>EMA55 斜率做空</th><th>动态委托做多(推荐参数)</th></tr></thead>
          <tbody>{monthly_total_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>五币合计年度总表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>年份</th><th>EMA55 斜率做空</th><th>动态委托做多(推荐参数)</th></tr></thead>
          <tbody>{yearly_total_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>月度明细融合表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>币种</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{monthly_detail_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>年度明细融合表</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>策略</th><th>币种</th><th>期间</th><th>交易数</th><th>盈亏U</th><th>PF</th><th>胜率</th><th>平均R</th><th>平均持仓h</th><th>3R+</th><th>5R+</th></tr></thead>
          <tbody>{yearly_detail_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>文件输出</h2>
      <p>动态做多交易明细：{html.escape(str(DYNAMIC_TRADES_CSV))}</p>
      <p>动态做多月度 CSV：{html.escape(str(DYNAMIC_MONTHLY_CSV))}</p>
      <p>动态做多年度 CSV：{html.escape(str(DYNAMIC_YEARLY_CSV))}</p>
      <p>融合月度 CSV：{html.escape(str(MERGED_MONTHLY_CSV))}</p>
      <p>融合年度 CSV：{html.escape(str(MERGED_YEARLY_CSV))}</p>
    </section>
  </main>
</body>
</html>"""


def pick_best_period(yearly_total: pd.DataFrame, strategy_key: str) -> tuple[str, float]:
    subset = yearly_total[yearly_total["strategy_key"] == strategy_key].copy()
    if subset.empty:
        return "-", 0.0
    row = subset.loc[subset["total_pnl_u"].idxmax()]
    return str(row["period"]), float(row["total_pnl_u"])


def render_total_pivot(period_df: pd.DataFrame) -> str:
    total = period_df[period_df["coin"] == "ALL"].copy()
    pivot = total.pivot(index="period", columns="strategy_label", values="total_pnl_u").sort_index()
    rows = []
    for period, row in pivot.iterrows():
        fixed = float(row.get("EMA55 斜率做空", 0.0))
        dyn = float(row.get("动态委托做多(推荐参数)", 0.0))
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(period))}</td>"
            f"<td class=\"{cls(fixed)}\">{fmt(fixed)}</td>"
            f"<td class=\"{cls(dyn)}\">{fmt(dyn)}</td>"
            "</tr>"
        )
    return "".join(rows)


def period_row(row: dict[str, object]) -> str:
    pnl = float(row["total_pnl_u"])
    return (
        "<tr>"
        f"<td>{html.escape(str(row['strategy_label']))}</td>"
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


if __name__ == "__main__":
    main()
