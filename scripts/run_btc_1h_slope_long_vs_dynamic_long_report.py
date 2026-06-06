from __future__ import annotations

import base64
import html
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import (
    _apply_slippage_price,
    _backtest_trade_start_index,
    _build_closed_trade,
    _build_drawdown_curves,
    _build_equity_curve,
    _build_period_stats,
    _build_report,
    _build_terminal_open_position,
    _create_open_position,
    _determine_backtest_order_size,
    _run_backtest_with_loaded_data,
    _try_close_position,
    build_protection_plan,
)
from okx_quant.candle_cache import load_candle_cache
from okx_quant.indicators import atr, moving_average
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed, snap_to_increment
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

INST_ID = "BTC-USDT-SWAP"
BAR = "1H"
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")
INITIAL_CAPITAL = Decimal("10000")

HTML_PATH = REPORT_DIR / "btc_1h_slope_long_vs_dynamic_long_report.html"
SUMMARY_JSON = REPORT_DIR / "btc_1h_slope_long_vs_dynamic_long_summary.json"
EQUITY_CHART = REPORT_DIR / "btc_1h_slope_long_vs_dynamic_long_equity.png"
DRAWDOWN_CHART = REPORT_DIR / "btc_1h_slope_long_vs_dynamic_long_drawdown.png"
YEARLY_CHART = REPORT_DIR / "btc_1h_slope_long_vs_dynamic_long_yearly.png"


@dataclass(frozen=True)
class StrategyRun:
    key: str
    label: str
    subtitle: str
    entry_logic: str
    exit_logic: str
    data_start_ts: int
    data_end_ts: int
    candle_count: int
    config: StrategyConfig
    trades: list
    report: object
    equity_curve: list[Decimal]
    net_value_curve: list[Decimal]
    drawdown_curve: list[Decimal]
    drawdown_pct_curve: list[Decimal]
    monthly_stats: list
    yearly_stats: list
    open_position: object | None


def build_slope_long_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id=INST_ID,
        bar=BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id="ema55_slope_long_mirror_research",
        risk_amount=Decimal("100"),
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        entry_reference_ema_period=55,
        entry_reference_ema_type="ema",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=True,
        trend_ema_slope_filter_enabled=True,
        trend_ema_slope_filter_lookback_bars=1,
        trend_ema_slope_filter_min_ratio=Decimal("0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def build_dynamic_long_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id=INST_ID,
        bar=BAR,
        ema_period=21,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=Decimal("100"),
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        entry_reference_ema_period=55,
        entry_reference_ema_type="ema",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=True,
        trend_ema_slope_filter_lookback_bars=5,
        trend_ema_slope_filter_min_ratio=Decimal("0"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def load_confirmed_candles() -> list:
    candles = [candle for candle in load_candle_cache(INST_ID, BAR, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"{INST_ID} {BAR} 没有可用已确认K线缓存")
    candles.sort(key=lambda item: item.ts)
    return candles


def run_mirrored_slope_long(candles: list, instrument: object) -> StrategyRun:
    config = build_slope_long_config()
    minimum = max(int(config.ema_period), int(config.trend_ema_period), int(config.atr_period), 2) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘K线不足，至少需要 {minimum} 根")

    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        raise RuntimeError("可交易样本不足")

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), "ema")
    atr_values = atr(candles, int(config.atr_period))
    trades: list = []
    open_position = None
    entry_threshold_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        current_ema = ema_values[index]
        previous_ema = ema_values[index - 1] if index > 0 else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        if current_ema is None or previous_ema is None or atr_value is None or atr_value <= 0:
            continue

        slope = current_ema - previous_ema
        slope_ratio = slope / current_ema if current_ema != 0 else None

        if open_position is not None:
            closed_trade = _try_close_position(
                open_position,
                candle,
                index,
                exit_fee_rate=TAKER_FEE_RATE,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None

        if open_position is not None and slope < 0:
            exit_price_raw = snap_to_increment(candle.close, instrument.tick_size, "nearest")
            exit_price = _apply_slippage_price(
                exit_price_raw,
                signal=open_position.signal,
                tick_size=open_position.tick_size,
                slippage_rate=open_position.exit_slippage_rate,
                is_entry=False,
            )
            trades.append(
                _build_closed_trade(
                    open_position,
                    candle,
                    index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason="slope_turn_negative",
                    exit_fee_rate=TAKER_FEE_RATE,
                    exit_fee_type="taker",
                )
            )
            open_position = None

        if open_position is not None or slope_ratio is None or slope_ratio < entry_threshold_ratio:
            continue

        protection = build_protection_plan(
            instrument=instrument,
            config=config,
            direction="long",
            entry_reference=candle.close,
            atr_value=atr_value,
            candle_ts=candle.ts,
            trigger_inst_id=instrument.inst_id,
        )
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=bool(config.risk_amount is not None and config.risk_amount > 0),
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal="long",
            entry_index=index,
            entry_ts=candle.ts,
            entry_price_raw=protection.entry_reference,
            stop_loss=protection.stop_loss,
            take_profit=protection.take_profit,
            atr_value=protection.atr_value,
            size=size,
            entry_fee_rate=TAKER_FEE_RATE,
            exit_fee_rate=TAKER_FEE_RATE,
            entry_fee_type="taker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            dynamic_exit_fee_rate=TAKER_FEE_RATE,
            dynamic_two_r_break_even=config.dynamic_two_r_break_even,
            dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            apply_entry_slippage=True,
        )

    report = _build_report(trades, initial_capital=config.backtest_initial_capital)
    equity_curve = _build_equity_curve(candles, trades)
    net_value_curve = [config.backtest_initial_capital + value for value in equity_curve]
    drawdown_curve, drawdown_pct_curve = _build_drawdown_curves(net_value_curve)
    monthly_stats = _build_period_stats(trades, initial_capital=config.backtest_initial_capital, by="month")
    yearly_stats = _build_period_stats(trades, initial_capital=config.backtest_initial_capital, by="year")
    terminal_open_position = _build_terminal_open_position(open_position, candles)

    return StrategyRun(
        key="slope_long",
        label="EMA55 斜率镜像做多",
        subtitle="把 EMA55 斜率做空完整反过来：斜率达到正阈值后市价做多",
        entry_logic="EMA55 单根斜率比例 >= +0.0005 时，按当根收盘价做多",
        exit_logic="沿用 2ATR 风控 + 动态止盈；若 EMA55 斜率重新转负，则按收盘价平仓",
        data_start_ts=candles[0].ts,
        data_end_ts=candles[-1].ts,
        candle_count=len(candles),
        config=config,
        trades=trades,
        report=report,
        equity_curve=equity_curve,
        net_value_curve=net_value_curve,
        drawdown_curve=drawdown_curve,
        drawdown_pct_curve=drawdown_pct_curve,
        monthly_stats=monthly_stats,
        yearly_stats=yearly_stats,
        open_position=terminal_open_position,
    )


def run_dynamic_long(candles: list, instrument: object) -> StrategyRun:
    config = build_dynamic_long_config()
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=f"local candle_cache full history | {INST_ID} {BAR} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    return StrategyRun(
        key="dynamic_long",
        label="EMA 动态委托做多",
        subtitle="当前代码默认回测参数：EMA21 趋势确认，EMA55 挂单回踩做多",
        entry_logic="EMA21 > EMA55 且收盘站上 EMA55 后，下一根按 EMA55 动态挂多单",
        exit_logic="2ATR 风控 + 动态止盈 + 2R 保本 + 手续费偏移",
        data_start_ts=candles[0].ts,
        data_end_ts=candles[-1].ts,
        candle_count=len(candles),
        config=config,
        trades=result.trades,
        report=result.report,
        equity_curve=result.equity_curve,
        net_value_curve=result.net_value_curve,
        drawdown_curve=result.drawdown_curve,
        drawdown_pct_curve=result.drawdown_pct_curve,
        monthly_stats=result.monthly_stats,
        yearly_stats=result.yearly_stats,
        open_position=result.open_position,
    )


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt_decimal(value: Decimal, digits: int = 4) -> str:
    return format_decimal_fixed(value, digits)


def fmt_pct(value: Decimal, digits: int = 2) -> str:
    return f"{format_decimal_fixed(value, digits)}%"


def exit_reason_table(run: StrategyRun) -> list[tuple[str, int]]:
    counts = Counter(str(trade.exit_reason) for trade in run.trades)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def yearly_pnl_map(run: StrategyRun) -> dict[str, Decimal]:
    return {str(item.period_label): Decimal(item.total_pnl) for item in run.yearly_stats}


def yearly_trade_map(run: StrategyRun) -> dict[str, int]:
    return {str(item.period_label): int(item.trades) for item in run.yearly_stats}


def save_equity_chart(runs: list[StrategyRun], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    palette = {"slope_long": "#b45309", "dynamic_long": "#0f766e"}
    for run in runs:
        ax.plot(
            range(len(run.net_value_curve)),
            [float(value) for value in run.net_value_curve],
            label=run.label,
            linewidth=2.2,
            color=palette.get(run.key, "#1d4ed8"),
        )
    ax.set_title("BTC 1H 资金曲线对比")
    ax.set_xlabel("K线序号")
    ax.set_ylabel("权益 (USDT)")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_drawdown_chart(runs: list[StrategyRun], output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 5.8))
    palette = {"slope_long": "#92400e", "dynamic_long": "#065f46"}
    for run in runs:
        ax.plot(
            range(len(run.drawdown_pct_curve)),
            [float(value) for value in run.drawdown_pct_curve],
            label=run.label,
            linewidth=2.0,
            color=palette.get(run.key, "#475569"),
        )
    ax.set_title("BTC 1H 回撤曲线对比")
    ax.set_xlabel("K线序号")
    ax.set_ylabel("回撤 (%)")
    ax.grid(alpha=0.25)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def save_yearly_chart(slope_run: StrategyRun, dynamic_run: StrategyRun, output_path: Path) -> None:
    slope_map = yearly_pnl_map(slope_run)
    dynamic_map = yearly_pnl_map(dynamic_run)
    years = sorted(set(slope_map) | set(dynamic_map))
    if not years:
        years = ["N/A"]
        slope_values = [0.0]
        dynamic_values = [0.0]
    else:
        slope_values = [float(slope_map.get(year, Decimal("0"))) for year in years]
        dynamic_values = [float(dynamic_map.get(year, Decimal("0"))) for year in years]

    x = list(range(len(years)))
    width = 0.36

    fig, ax = plt.subplots(figsize=(11, 5.8))
    ax.bar([item - width / 2 for item in x], slope_values, width=width, label=slope_run.label, color="#f59e0b")
    ax.bar([item + width / 2 for item in x], dynamic_values, width=width, label=dynamic_run.label, color="#10b981")
    ax.set_title("年度总盈亏对比")
    ax.set_xlabel("年份")
    ax.set_ylabel("总盈亏 (USDT)")
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.axhline(0, color="#64748b", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)


def image_tag(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f'<img alt="{html.escape(path.stem)}" src="data:image/png;base64,{encoded}">'


def kpi(label: str, value: str, sub: str) -> str:
    return (
        '<div class="card kpi">'
        f'<div class="label">{html.escape(label)}</div>'
        f'<div class="value">{value}</div>'
        f'<div class="sub">{html.escape(sub)}</div>'
        "</div>"
    )


def summary_table(runs: list[StrategyRun]) -> str:
    rows = []
    for run in runs:
        report = run.report
        rows.append(
            "<tr>"
            f"<td>{html.escape(run.label)}</td>"
            f"<td>{report.total_trades}</td>"
            f"<td>{fmt_pct(report.win_rate)}</td>"
            f"<td>{fmt_decimal(report.total_pnl)}</td>"
            f"<td>{fmt_pct(report.total_return_pct)}</td>"
            f"<td>{fmt_decimal(report.average_r_multiple)}</td>"
            f"<td>{fmt_decimal(report.max_drawdown)}</td>"
            f"<td>{fmt_pct(report.max_drawdown_pct)}</td>"
            f"<td>{fmt_decimal(report.profit_factor, 3) if report.profit_factor is not None else '-'}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>策略</th><th>交易数</th><th>胜率</th><th>总盈亏</th><th>总收益率</th><th>平均R</th><th>最大回撤</th><th>回撤比例</th><th>PF</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def config_table(runs: list[StrategyRun]) -> str:
    rows = []
    for run in runs:
        config = run.config
        rows.append(
            "<tr>"
            f"<td>{html.escape(run.label)}</td>"
            f"<td>{config.ema_label()}</td>"
            f"<td>{config.trend_ema_label()}</td>"
            f"<td>{config.entry_reference_line_label()}</td>"
            f"<td>{config.atr_period}</td>"
            f"<td>{fmt_decimal(config.atr_stop_multiplier, 2)}</td>"
            f"<td>{fmt_decimal(config.atr_take_multiplier, 2)}</td>"
            f"<td>{fmt_decimal(config.risk_amount or Decimal('0'), 2)}</td>"
            f"<td>{html.escape(config.take_profit_mode)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>策略</th><th>快线</th><th>趋势线</th><th>入场参考</th><th>ATR周期</th><th>止损ATR</th><th>止盈ATR</th><th>单笔风险</th><th>止盈模式</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def exit_reason_compare_table(runs: list[StrategyRun]) -> str:
    labels = sorted({reason for run in runs for reason, _ in exit_reason_table(run)})
    rows = []
    reason_maps = [{reason: count for reason, count in exit_reason_table(run)} for run in runs]
    for reason in labels:
        row = [f"<td>{html.escape(reason)}</td>"]
        for mapping in reason_maps:
            row.append(f"<td>{mapping.get(reason, 0)}</td>")
        rows.append("<tr>" + "".join(row) + "</tr>")

    header = "<th>平仓原因</th>" + "".join(f"<th>{html.escape(run.label)}</th>" for run in runs)
    return "<table><thead><tr>" + header + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"


def yearly_compare_table(slope_run: StrategyRun, dynamic_run: StrategyRun) -> str:
    slope_pnl = yearly_pnl_map(slope_run)
    dynamic_pnl = yearly_pnl_map(dynamic_run)
    slope_trades = yearly_trade_map(slope_run)
    dynamic_trades = yearly_trade_map(dynamic_run)
    years = sorted(set(slope_pnl) | set(dynamic_pnl))
    rows = []
    for year in years:
        rows.append(
            "<tr>"
            f"<td>{html.escape(year)}</td>"
            f"<td>{fmt_decimal(slope_pnl.get(year, Decimal('0')))}</td>"
            f"<td>{slope_trades.get(year, 0)}</td>"
            f"<td>{fmt_decimal(dynamic_pnl.get(year, Decimal('0')))}</td>"
            f"<td>{dynamic_trades.get(year, 0)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>年份</th><th>斜率做多总盈亏</th><th>斜率做多交易数</th><th>动态委托做多总盈亏</th><th>动态委托做多交易数</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_html(slope_run: StrategyRun, dynamic_run: StrategyRun) -> str:
    better_by_pnl = slope_run if slope_run.report.total_pnl > dynamic_run.report.total_pnl else dynamic_run
    better_by_drawdown = slope_run if slope_run.report.max_drawdown < dynamic_run.report.max_drawdown else dynamic_run
    better_by_avg_r = slope_run if slope_run.report.average_r_multiple > dynamic_run.report.average_r_multiple else dynamic_run
    pnl_delta = slope_run.report.total_pnl - dynamic_run.report.total_pnl
    trade_delta = slope_run.report.total_trades - dynamic_run.report.total_trades

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC 1H 斜率做多 vs EMA 动态委托做多</title>
<style>
:root {{
  --bg:#f5f7fb; --panel:#ffffff; --line:#d7deea; --ink:#162132; --muted:#667085;
  --amber:#b45309; --green:#0f766e; --navy:#0f172a; --rose:#be123c;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink); font-family:"Microsoft YaHei","Segoe UI",Arial,sans-serif; }}
.hero {{ background:linear-gradient(135deg,#111827 0%,#1f2937 50%,#334155 100%); color:#fff; padding:34px 42px 30px; }}
.hero h1 {{ margin:0 0 10px; font-size:30px; }}
.hero p {{ margin:6px 0; color:#dbe4f0; line-height:1.75; max-width:1100px; }}
.wrap {{ max-width:1240px; margin:0 auto; padding:24px 20px 44px; }}
.grid {{ display:grid; gap:16px; }}
.grid-4 {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
.grid-2 {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px; padding:18px; box-shadow:0 2px 10px rgba(15,23,42,.04); }}
.kpi .label {{ color:var(--muted); font-size:13px; margin-bottom:8px; }}
.kpi .value {{ font-size:28px; font-weight:800; color:var(--navy); }}
.kpi .sub {{ color:var(--muted); font-size:13px; margin-top:6px; line-height:1.6; }}
h2 {{ font-size:22px; margin:30px 0 14px; }}
h3 {{ font-size:16px; margin:0 0 10px; }}
p {{ line-height:1.75; }}
.answer {{ font-size:17px; line-height:1.85; }}
.note {{ color:var(--muted); font-size:13px; line-height:1.75; }}
.callout {{ border-left:5px solid var(--amber); background:#fff7ed; padding:14px 16px; border-radius:8px; line-height:1.85; }}
.good {{ color:var(--green); font-weight:700; }}
.bad {{ color:var(--rose); font-weight:700; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:right; }}
th:first-child,td:first-child {{ text-align:left; }}
th {{ background:#f8fafc; color:#475467; }}
.imgbox img {{ width:100%; display:block; border:1px solid var(--line); border-radius:8px; background:#fff; }}
code {{ background:#eff4fb; padding:2px 6px; border-radius:6px; }}
@media (max-width: 920px) {{
  .grid-4,.grid-2 {{ grid-template-columns:1fr; }}
  .hero {{ padding:24px 18px; }}
  .wrap {{ padding:18px 14px 32px; }}
}}
</style>
</head>
<body>
<section class="hero">
  <h1>BTC 1H：EMA55 斜率镜像做多 vs EMA 动态委托做多</h1>
  <p>本次直接按你的意思，把 <strong>EMA55 斜率做空</strong> 的信号方向完整反过来，做成一版 <strong>EMA55 斜率镜像做多</strong>，再和当前代码里的 <strong>EMA 动态委托做多</strong> 做同口径对比。</p>
  <p>数据使用 <strong>{html.escape(INST_ID)} {html.escape(BAR)}</strong> 本地全量已确认K线，区间为 <strong>{fmt_ts(slope_run.data_start_ts)}</strong> 到 <strong>{fmt_ts(slope_run.data_end_ts)}</strong>。费率统一采用 maker <strong>{MAKER_FEE_RATE}</strong> / taker <strong>{TAKER_FEE_RATE}</strong>，单笔风险统一按 <strong>100U</strong> 固定风险回测。</p>
</section>

<main class="wrap">
  <div class="grid grid-4">
    {kpi("总盈亏更高", html.escape(better_by_pnl.label), f"按 total pnl 比较，差值 {fmt_decimal(abs(pnl_delta))} U")}
    {kpi("回撤更小", html.escape(better_by_drawdown.label), f"按 max drawdown 比较")}
    {kpi("平均R更高", html.escape(better_by_avg_r.label), f"按 average R 比较")}
    {kpi("交易频率差", f"{trade_delta:+d}", "斜率做多 相对 动态委托做多")}
  </div>

  <h2>先看结论</h2>
  <div class="card answer">
    这次结果里，<span class="good">{html.escape(better_by_pnl.label)}</span> 的整体表现更占优。
    如果只看最终收益，斜率镜像做多相对动态委托做多的盈亏差为 <strong>{fmt_decimal(pnl_delta)}</strong>；
    但如果你更在意风险承受，<strong>{html.escape(better_by_drawdown.label)}</strong> 的回撤控制更好。
    这两条策略最大的本质区别不是“都是做多”，而是 <strong>入场方式完全不同</strong>：
    斜率镜像做多是 <strong>信号出现就按收盘价追进</strong>，动态委托做多是 <strong>顺着趋势、等回踩 EMA 再挂单接</strong>。
  </div>

  <div class="grid grid-2">
    <div class="card">
      <h3>{html.escape(slope_run.label)}</h3>
      <p>{html.escape(slope_run.subtitle)}</p>
      <p><strong>开仓：</strong>{html.escape(slope_run.entry_logic)}</p>
      <p><strong>平仓：</strong>{html.escape(slope_run.exit_logic)}</p>
    </div>
    <div class="card">
      <h3>{html.escape(dynamic_run.label)}</h3>
      <p>{html.escape(dynamic_run.subtitle)}</p>
      <p><strong>开仓：</strong>{html.escape(dynamic_run.entry_logic)}</p>
      <p><strong>平仓：</strong>{html.escape(dynamic_run.exit_logic)}</p>
    </div>
  </div>

  <h2>核心指标</h2>
  <div class="card">
    {summary_table([slope_run, dynamic_run])}
    <p class="note">这里要特别注意：两边虽然都做多，但斜率镜像做多是“收盘价触发立刻进”，动态委托做多是“下一根按 EMA 挂限价单等回踩”。所以它们的收益差，不只是信号质量差，也包含了执行方式差。</p>
  </div>

  <h2>参数口径</h2>
  <div class="card">
    {config_table([slope_run, dynamic_run])}
  </div>

  <h2>曲线对比</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>资金曲线</h3>
      {image_tag(EQUITY_CHART)}
    </div>
    <div class="card imgbox">
      <h3>回撤曲线</h3>
      {image_tag(DRAWDOWN_CHART)}
    </div>
  </div>

  <h2>年度表现</h2>
  <div class="grid grid-2">
    <div class="card imgbox">
      <h3>年度总盈亏</h3>
      {image_tag(YEARLY_CHART)}
    </div>
    <div class="card">
      <h3>年度拆分</h3>
      {yearly_compare_table(slope_run, dynamic_run)}
    </div>
  </div>

  <h2>平仓原因</h2>
  <div class="card">
    {exit_reason_compare_table([slope_run, dynamic_run])}
    <p class="note">如果斜率做多里 <code>slope_turn_negative</code> 占比很高，通常说明它更像“短波段追趋势”；如果动态委托做多里保本/动态止盈类原因更多，说明它更依赖回踩成交后把单子留在趋势里。</p>
  </div>

  <h2>怎么看这次结果</h2>
  <div class="callout">
    如果你想验证的是“空头这套斜率逻辑直接翻多，能不能也成立”，这份结果已经能回答第一层问题。<br>
    如果斜率镜像做多明显不如动态委托做多，往往不是因为“做多方向不行”，而是因为 <strong>上涨里直接追斜率</strong> 比 <strong>上涨里等回踩接趋势</strong> 更容易吃到高位追单和震荡回吐。<br>
    反过来，如果斜率镜像做多更强，那说明这段 BTC 1H 样本里，趋势启动后的延续性足够强，追入比等回踩更有效。
  </div>
</main>
</body>
</html>
"""


def build_summary_payload(slope_run: StrategyRun, dynamic_run: StrategyRun) -> dict[str, object]:
    def serialize_run(run: StrategyRun) -> dict[str, object]:
        report = run.report
        return {
            "label": run.label,
            "subtitle": run.subtitle,
            "data_start_utc": fmt_ts(run.data_start_ts),
            "data_end_utc": fmt_ts(run.data_end_ts),
            "candle_count": run.candle_count,
            "entry_logic": run.entry_logic,
            "exit_logic": run.exit_logic,
            "config": {
                "ema_period": run.config.ema_period,
                "trend_ema_period": run.config.trend_ema_period,
                "entry_reference_ema_period": run.config.resolved_entry_reference_ema_period(),
                "atr_period": run.config.atr_period,
                "atr_stop_multiplier": str(run.config.atr_stop_multiplier),
                "atr_take_multiplier": str(run.config.atr_take_multiplier),
                "risk_amount": str(run.config.risk_amount),
                "take_profit_mode": run.config.take_profit_mode,
                "slope_threshold_ratio": str(run.config.trend_ema_slope_filter_min_ratio),
            },
            "report": {
                "total_trades": report.total_trades,
                "win_rate_pct": str(report.win_rate),
                "total_pnl": str(report.total_pnl),
                "total_return_pct": str(report.total_return_pct),
                "average_r_multiple": str(report.average_r_multiple),
                "max_drawdown": str(report.max_drawdown),
                "max_drawdown_pct": str(report.max_drawdown_pct),
                "profit_factor": str(report.profit_factor) if report.profit_factor is not None else None,
            },
            "exit_reasons": {reason: count for reason, count in exit_reason_table(run)},
            "yearly_stats": [
                {
                    "year": item.period_label,
                    "trades": item.trades,
                    "win_rate_pct": str(item.win_rate),
                    "total_pnl": str(item.total_pnl),
                    "return_pct": str(item.return_pct),
                    "max_drawdown": str(item.max_drawdown),
                    "max_drawdown_pct": str(item.max_drawdown_pct),
                }
                for item in run.yearly_stats
            ],
        }

    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "instrument": INST_ID,
        "bar": BAR,
        "fee_rates": {
            "maker": str(MAKER_FEE_RATE),
            "taker": str(TAKER_FEE_RATE),
        },
        "slope_long": serialize_run(slope_run),
        "dynamic_long": serialize_run(dynamic_run),
        "comparison": {
            "pnl_delta_slope_minus_dynamic": str(slope_run.report.total_pnl - dynamic_run.report.total_pnl),
            "trade_delta_slope_minus_dynamic": slope_run.report.total_trades - dynamic_run.report.total_trades,
            "better_total_pnl": slope_run.label
            if slope_run.report.total_pnl > dynamic_run.report.total_pnl
            else dynamic_run.label,
            "better_max_drawdown": slope_run.label
            if slope_run.report.max_drawdown < dynamic_run.report.max_drawdown
            else dynamic_run.label,
            "better_average_r": slope_run.label
            if slope_run.report.average_r_multiple > dynamic_run.report.average_r_multiple
            else dynamic_run.label,
        },
    }


def main() -> None:
    client = OkxRestClient()
    instrument = client.get_instrument(INST_ID)
    candles = load_confirmed_candles()

    slope_run = run_mirrored_slope_long(candles, instrument)
    dynamic_run = run_dynamic_long(candles, instrument)

    save_equity_chart([slope_run, dynamic_run], EQUITY_CHART)
    save_drawdown_chart([slope_run, dynamic_run], DRAWDOWN_CHART)
    save_yearly_chart(slope_run, dynamic_run, YEARLY_CHART)

    SUMMARY_JSON.write_text(
        json.dumps(build_summary_payload(slope_run, dynamic_run), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(slope_run, dynamic_run), encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
