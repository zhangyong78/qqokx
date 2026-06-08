from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from shutil import copyfile

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import (
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"ema_dynamic_long_fixed_research_log_4coins_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
SUMMARY_CSV_PATH = REPORT_DIR / f"{BASENAME}_summary.csv"
MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly.csv"
YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly.csv"
COIN_MONTHLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_monthly_by_coin.csv"
COIN_YEARLY_CSV_PATH = REPORT_DIR / f"{BASENAME}_yearly_by_coin.csv"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "ema_dynamic_long_fixed_research_log_4coins_10u.html"


@dataclass(frozen=True)
class Profile:
    symbol: str
    label: str
    summary_name: str
    bar: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    note: str
    filter_conclusion: str


@dataclass
class ProfileStudy:
    profile: Profile
    start_utc: str
    end_utc: str
    candle_count: int
    trades: list
    test_trades: list
    all_metrics: object
    test_metrics: object


PROFILES = (
    Profile(
        symbol="BTC-USDT-SWAP",
        label="BTC",
        summary_name="BTC | EMA21 / MA50 / 挂单 MA50 / SL2",
        bar="1H",
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        note="5月实盘候选里的老三多参数，BTC 最终固定为 EMA21 + MA50，挂单参考 MA50。",
        filter_conclusion="昨天日线 EMA5/8/13/21 过滤都不如不过滤，保留不过滤。",
    ),
    Profile(
        symbol="ETH-USDT-SWAP",
        label="ETH",
        summary_name="ETH | MA21 / EMA55 / 挂单 MA55 / SL2",
        bar="1H",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ema",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        note="5月实盘候选里的老三多参数，ETH 最终固定为 MA21 + EMA55，挂单参考 MA55。",
        filter_conclusion="昨天日线 EMA5/8/13/21 过滤都不如不过滤，保留不过滤。",
    ),
    Profile(
        symbol="SOL-USDT-SWAP",
        label="SOL",
        summary_name="SOL | MA21 / MA55 / 挂单 MA55 / SL1",
        bar="1H",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        note="5月实盘候选里的老三多参数，SOL 最终固定为 MA21 + MA55，挂单参考 MA55。",
        filter_conclusion="昨天日线 EMA5/8/13/21 过滤都不如不过滤，保留不过滤。",
    ),
    Profile(
        symbol="BNB-USDT-SWAP",
        label="BNB",
        summary_name="BNB | MA21 / MA55 / 挂单 MA55 / SL1.5",
        bar="1H",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("6"),
        note="BNB 补测后，全历史最稳的一版定为 MA21 + MA55，挂单参考 MA55，止损 1.5 ATR。",
        filter_conclusion="不适合加昨天日线 EMA5/EMA8；不过滤测试段 193.46U，EMA5 91.66U，EMA8 73.50U。",
    ),
)


def run_profile(profile: Profile) -> ProfileStudy:
    from okx_quant.backtest import _run_backtest_with_loaded_data
    from okx_quant.candle_cache import load_candle_cache
    from okx_quant.models import StrategyConfig
    from okx_quant.okx_client import OkxRestClient
    from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID

    client = OkxRestClient()
    candles = [candle for candle in load_candle_cache(profile.symbol, profile.bar, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing candles for {profile.symbol} {profile.bar}")
    bounds = build_split_bounds(len(candles))
    config = StrategyConfig(
        inst_id=profile.symbol,
        bar=profile.bar,
        ema_period=profile.ema_period,
        ema_type=profile.ema_type,
        trend_ema_period=profile.trend_ema_period,
        trend_ema_type=profile.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=profile.atr_stop_multiplier,
        atr_take_multiplier=profile.atr_take_multiplier,
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
        entry_reference_ema_period=profile.entry_reference_ema_period,
        entry_reference_ema_type=profile.entry_reference_ema_type,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )
    result = _run_backtest_with_loaded_data(
        candles,
        client.get_instrument(profile.symbol),
        config,
        data_source_note=f"local candle_cache full history | {profile.symbol} {profile.summary_name}",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, bounds["test"])
    return ProfileStudy(
        profile=profile,
        start_utc=format_ts(candles[0].ts),
        end_utc=format_ts(candles[-1].ts),
        candle_count=len(candles),
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
    )


def build_trade_frame(studies: list[ProfileStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        for trade in study.trades:
            exit_dt = timestamp_to_datetime(trade.exit_ts)
            pnl = float(trade.pnl)
            rows.append(
                {
                    "coin": study.profile.label,
                    "exit_time": exit_dt,
                    "year": exit_dt.strftime("%Y"),
                    "month": exit_dt.strftime("%Y-%m"),
                    "pnl_u": pnl,
                    "is_win": pnl > 0,
                }
            )
    return pd.DataFrame(rows)


def build_period_frame(trade_frame: pd.DataFrame, period: str) -> pd.DataFrame:
    if trade_frame.empty:
        return pd.DataFrame(columns=["coin", "period", "trades", "wins", "losses", "win_rate_pct", "pnl_u", "avg_pnl_u", "cumulative_pnl_u", "end_capital_u"])
    field = "month" if period == "month" else "year"
    grouped = (
        trade_frame.groupby(["coin", field], as_index=False)
        .agg(
            trades=("pnl_u", "size"),
            wins=("is_win", "sum"),
            pnl_u=("pnl_u", "sum"),
        )
        .rename(columns={field: "period"})
        .sort_values(["coin", "period"])
        .reset_index(drop=True)
    )
    grouped["losses"] = grouped["trades"] - grouped["wins"]
    grouped["win_rate_pct"] = grouped["wins"] / grouped["trades"] * 100.0
    grouped["avg_pnl_u"] = grouped["pnl_u"] / grouped["trades"]
    grouped["cumulative_pnl_u"] = grouped.groupby("coin")["pnl_u"].cumsum()
    grouped["end_capital_u"] = float(INITIAL_CAPITAL) + grouped["cumulative_pnl_u"]
    return grouped[["coin", "period", "trades", "wins", "losses", "win_rate_pct", "pnl_u", "avg_pnl_u", "cumulative_pnl_u", "end_capital_u"]]


def build_total_period_frame(coin_period_frame: pd.DataFrame) -> pd.DataFrame:
    if coin_period_frame.empty:
        return pd.DataFrame(columns=["period", "trades", "wins", "losses", "win_rate_pct", "pnl_u", "avg_pnl_u", "cumulative_pnl_u", "end_capital_u"])
    grouped = (
        coin_period_frame.groupby("period", as_index=False)
        .agg(
            trades=("trades", "sum"),
            wins=("wins", "sum"),
            losses=("losses", "sum"),
            pnl_u=("pnl_u", "sum"),
        )
        .sort_values("period")
        .reset_index(drop=True)
    )
    grouped["win_rate_pct"] = grouped["wins"] / grouped["trades"] * 100.0
    grouped["avg_pnl_u"] = grouped["pnl_u"] / grouped["trades"]
    grouped["cumulative_pnl_u"] = grouped["pnl_u"].cumsum()
    grouped["end_capital_u"] = float(INITIAL_CAPITAL) + grouped["cumulative_pnl_u"]
    return grouped[["period", "trades", "wins", "losses", "win_rate_pct", "pnl_u", "avg_pnl_u", "cumulative_pnl_u", "end_capital_u"]]


def build_summary_frame(studies: list[ProfileStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        rows.append(
            {
                "coin": study.profile.label,
                "fixed_profile": study.profile.summary_name,
                "range_start": study.start_utc,
                "range_end": study.end_utc,
                "candles": study.candle_count,
                "all_pnl_u": float(study.all_metrics.pnl),
                "all_trades": study.all_metrics.trades,
                "all_win_rate_pct": float(study.all_metrics.win_rate),
                "all_profit_factor": none_or_float(study.all_metrics.profit_factor),
                "all_avg_r": float(study.all_metrics.avg_r),
                "all_drawdown_u": float(study.all_metrics.max_drawdown),
                "test_pnl_u": float(study.test_metrics.pnl),
                "test_trades": study.test_metrics.trades,
                "test_win_rate_pct": float(study.test_metrics.win_rate),
                "test_profit_factor": none_or_float(study.test_metrics.profit_factor),
                "test_avg_r": float(study.test_metrics.avg_r),
                "test_drawdown_u": float(study.test_metrics.max_drawdown),
            }
        )
    total_row = {
        "coin": "ALL",
        "fixed_profile": "4币固定组合",
        "range_start": min(study.start_utc for study in studies),
        "range_end": max(study.end_utc for study in studies),
        "candles": int(sum(study.candle_count for study in studies)),
        "all_pnl_u": float(sum((study.all_metrics.pnl for study in studies), Decimal("0"))),
        "all_trades": int(sum(study.all_metrics.trades for study in studies)),
        "all_win_rate_pct": None,
        "all_profit_factor": None,
        "all_avg_r": None,
        "all_drawdown_u": float(max(study.all_metrics.max_drawdown for study in studies)),
        "test_pnl_u": float(sum((study.test_metrics.pnl for study in studies), Decimal("0"))),
        "test_trades": int(sum(study.test_metrics.trades for study in studies)),
        "test_win_rate_pct": None,
        "test_profit_factor": None,
        "test_avg_r": None,
        "test_drawdown_u": float(max(study.test_metrics.max_drawdown for study in studies)),
    }
    rows.append(total_row)
    return pd.DataFrame(rows)


def build_equity_curve_image(studies: list[ProfileStudy]) -> str:
    fig, ax = plt.subplots(figsize=(11.5, 4.8))
    for study in studies:
        points = equity_points(study.trades)
        if not points:
            continue
        xs, ys = zip(*points)
        ax.plot(xs, ys, label=study.profile.label, linewidth=1.8)
    total_points = equity_points([trade for study in studies for trade in study.trades])
    if total_points:
        xs, ys = zip(*total_points)
        ax.plot(xs, ys, label="ALL", linewidth=2.8, color="#9a3412")
    ax.set_title("4币固定参数累计净利润")
    ax.set_ylabel("PnL (U)")
    ax.grid(alpha=0.2)
    ax.legend()
    fig.autofmt_xdate()
    return fig_to_base64(fig)


def build_yearly_bar_image(total_yearly_frame: pd.DataFrame) -> str:
    fig, ax = plt.subplots(figsize=(10.5, 4.4))
    if total_yearly_frame.empty:
        ax.text(0.5, 0.5, "No yearly rows", ha="center", va="center", transform=ax.transAxes)
    else:
        colors = ["#166534" if value >= 0 else "#b91c1c" for value in total_yearly_frame["pnl_u"]]
        ax.bar(total_yearly_frame["period"], total_yearly_frame["pnl_u"], color=colors)
        ax.axhline(0, color="#475467", linewidth=1)
    ax.set_title("4币固定参数年度净利润")
    ax.set_ylabel("PnL (U)")
    ax.grid(axis="y", alpha=0.18)
    return fig_to_base64(fig)


def build_html(
    studies: list[ProfileStudy],
    summary_frame: pd.DataFrame,
    total_monthly_frame: pd.DataFrame,
    total_yearly_frame: pd.DataFrame,
    coin_monthly_frame: pd.DataFrame,
    coin_yearly_frame: pd.DataFrame,
) -> str:
    total_row = summary_frame[summary_frame["coin"] == "ALL"].iloc[0]
    equity_curve = build_equity_curve_image(studies)
    yearly_chart = build_yearly_bar_image(total_yearly_frame)
    parameter_frame = pd.DataFrame(
        [
            {
                "coin": study.profile.label,
                "fixed_profile": study.profile.summary_name,
                "fast_line": f"{study.profile.ema_type.upper()}{study.profile.ema_period}",
                "trend_line": f"{study.profile.trend_ema_type.upper()}{study.profile.trend_ema_period}",
                "entry_reference": f"{study.profile.entry_reference_ema_type.upper()}{study.profile.entry_reference_ema_period}",
                "atr_stop": float(study.profile.atr_stop_multiplier),
                "atr_take": float(study.profile.atr_take_multiplier),
                "daily_filter_decision": "不过滤",
            }
            for study in studies
        ]
    )
    filter_frame = pd.DataFrame(
        [{"coin": study.profile.label, "conclusion": study.profile.filter_conclusion} for study in studies]
    )
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>EMA动态委托做多 4币固定研究日志</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --panel: #fffdf8;
      --ink: #1f1b16;
      --muted: #6c6257;
      --line: #d9cfc1;
      --accent: #9a3412;
      --good: #166534;
      --bad: #b91c1c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top left, rgba(154,52,18,0.08), transparent 32%), linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
      color: var(--ink);
      font-family: "Segoe UI", "Microsoft YaHei", sans-serif;
      line-height: 1.55;
    }}
    .wrap {{ width: min(1460px, calc(100vw - 48px)); margin: 0 auto; padding: 28px 0 42px; }}
    .hero, .section {{ background: rgba(255,255,255,0.78); border: 1px solid rgba(217,207,193,0.9); border-radius: 24px; padding: 24px; }}
    .section {{ margin-top: 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px 18px; }}
    .label {{ color: var(--muted); font-size: 13px; }}
    .value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
    .note {{ padding: 12px 14px; border-left: 4px solid var(--accent); background: rgba(154,52,18,0.06); border-radius: 12px; color: #4a3829; }}
    .two-col {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }}
    .img-box {{ margin-top: 16px; background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 10px; }}
    img {{ display: block; width: 100%; height: auto; border-radius: 12px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 14px; background: var(--panel); }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    thead th {{ background: #efe5d8; }}
    tr:last-child td {{ border-bottom: none; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    p {{ margin: 8px 0; color: var(--muted); }}
    code {{ background: rgba(31,27,22,0.06); padding: 1px 6px; border-radius: 6px; color: #5c2411; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>EMA动态委托做多 4币固定研究日志</h1>
      <p>口径：BTC / ETH / SOL / BNB，1H，全历史，10U 固定风险金，动态止盈，2R保本，手续费偏移开启，不加日线过滤。</p>
      <div class="grid">
        <div class="card"><div class="label">4币全样本利润</div><div class="value">{format_u(total_row["all_pnl_u"])}</div></div>
        <div class="card"><div class="label">4币测试段利润</div><div class="value">{format_u(total_row["test_pnl_u"])}</div></div>
        <div class="card"><div class="label">4币全样本交易数</div><div class="value">{int(total_row["all_trades"])}</div></div>
        <div class="card"><div class="label">4币测试段交易数</div><div class="value">{int(total_row["test_trades"])}</div></div>
        <div class="card"><div class="label">4币全样本回撤</div><div class="value">{format_u(total_row["all_drawdown_u"])}</div></div>
        <div class="card"><div class="label">4币测试段回撤</div><div class="value">{format_u(total_row["test_drawdown_u"])}</div></div>
      </div>
    </section>

    <section class="section">
      <h2>固定结论</h2>
      <div class="note">
        这版日志固定了 4 个币的 EMA 动态委托做多参数，不再继续混入口径不同的旧研究文件。最终选择是：`BTC/ETH/SOL` 沿用 5 月实盘候选里的老三多参数，`BNB` 使用后续补测里全历史更稳的 `MA21 / MA55 / 挂单 MA55 / SL1.5`。
      </div>
      <p>日线过滤最终统一不启用。`BTC / ETH / SOL` 在昨天日线 `EMA5 / EMA8 / EMA13 / EMA21` 过滤下都不如不过滤；`BNB` 最佳参数加昨天日线 `EMA5 / EMA8` 后，测试段也明显变差。</p>
    </section>

    <section class="section">
      <h2>固定参数表</h2>
      {dataframe_to_html(parameter_frame)}
    </section>

    <section class="section">
      <h2>分币种总览</h2>
      {dataframe_to_html(summary_frame)}
    </section>

    <section class="section">
      <h2>日线过滤定稿说明</h2>
      {dataframe_to_html(filter_frame)}
    </section>

    <section class="section">
      <h2>曲线与年度图</h2>
      <div class="two-col">
        <div class="img-box">
          <h3>累计净利润曲线</h3>
          <img alt="equity_curve" src="data:image/png;base64,{equity_curve}" />
        </div>
        <div class="img-box">
          <h3>年度净利润</h3>
          <img alt="yearly_bar" src="data:image/png;base64,{yearly_chart}" />
        </div>
      </div>
    </section>

    <section class="section">
      <h2>组合年度汇总</h2>
      {dataframe_to_html(total_yearly_frame)}
    </section>

    <section class="section">
      <h2>组合月度汇总</h2>
      {dataframe_to_html(total_monthly_frame)}
    </section>

    <section class="section">
      <h2>分币种年度明细</h2>
      {build_coin_sections(coin_yearly_frame, "年度")}
    </section>

    <section class="section">
      <h2>分币种月度明细</h2>
      {build_coin_sections(coin_monthly_frame, "月度")}
    </section>

    <section class="section">
      <h2>样本范围</h2>
      <p>时间覆盖：{html.escape(min(study.start_utc for study in studies))} 至 {html.escape(max(study.end_utc for study in studies))}</p>
      <p>分析目录原始报告：<code>{html.escape(str(HTML_PATH))}</code></p>
      <p>项目内固定报告：<code>{html.escape(str(PROJECT_HTML_PATH))}</code></p>
    </section>
  </div>
</body>
</html>"""


def build_coin_sections(frame: pd.DataFrame, title: str) -> str:
    blocks: list[str] = []
    for coin in [profile.label for profile in PROFILES]:
        coin_frame = frame[frame["coin"] == coin].copy()
        if coin_frame.empty:
            continue
        blocks.append(f'<div class="img-box"><h3>{html.escape(coin)} {html.escape(title)}</h3>{dataframe_to_html(coin_frame.drop(columns=["coin"]))}</div>')
    return "".join(blocks)


def dataframe_to_html(frame: pd.DataFrame) -> str:
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(format_float_value)
    return display.to_html(index=False, escape=False)


def format_float_value(value: float) -> str:
    if pd.isna(value):
        return ""
    return f"{value:,.2f}"


def format_u(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return f"{float(value):,.2f}U"


def none_or_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def timestamp_to_datetime(ts: int) -> datetime:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def equity_points(trades: list) -> list[tuple[datetime, float]]:
    ordered = sorted(trades, key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    equity = Decimal("0")
    points: list[tuple[datetime, float]] = []
    for trade in ordered:
        equity += trade.pnl
        points.append((timestamp_to_datetime(trade.exit_ts), float(equity)))
    return points


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_payload(
    studies: list[ProfileStudy],
    summary_frame: pd.DataFrame,
    total_monthly_frame: pd.DataFrame,
    total_yearly_frame: pd.DataFrame,
) -> dict[str, object]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "scope": "ema_dynamic_long_fixed_research_log_4coins_10u",
        "html_path": str(HTML_PATH),
        "project_html_path": str(PROJECT_HTML_PATH),
        "summary": summary_frame.to_dict(orient="records"),
        "monthly_total": total_monthly_frame.to_dict(orient="records"),
        "yearly_total": total_yearly_frame.to_dict(orient="records"),
        "profiles": [
            {
                "coin": study.profile.label,
                "symbol": study.profile.symbol,
                "summary_name": study.profile.summary_name,
                "note": study.profile.note,
                "filter_conclusion": study.profile.filter_conclusion,
            }
            for study in studies
        ],
    }


def main() -> None:
    studies = [run_profile(profile) for profile in PROFILES]
    summary_frame = build_summary_frame(studies)
    trade_frame = build_trade_frame(studies)
    coin_monthly_frame = build_period_frame(trade_frame, period="month")
    coin_yearly_frame = build_period_frame(trade_frame, period="year")
    total_monthly_frame = build_total_period_frame(coin_monthly_frame)
    total_yearly_frame = build_total_period_frame(coin_yearly_frame)

    summary_frame.to_csv(SUMMARY_CSV_PATH, index=False, encoding="utf-8-sig")
    total_monthly_frame.to_csv(MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    total_yearly_frame.to_csv(YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_monthly_frame.to_csv(COIN_MONTHLY_CSV_PATH, index=False, encoding="utf-8-sig")
    coin_yearly_frame.to_csv(COIN_YEARLY_CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(studies, summary_frame, total_monthly_frame, total_yearly_frame)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(
        build_html(studies, summary_frame, total_monthly_frame, total_yearly_frame, coin_monthly_frame, coin_yearly_frame),
        encoding="utf-8",
    )
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


if __name__ == "__main__":
    main()
