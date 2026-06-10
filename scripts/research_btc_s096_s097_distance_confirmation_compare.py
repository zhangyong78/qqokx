from __future__ import annotations

import csv
import html
import json
import sys
from dataclasses import MISSING, dataclass, fields
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import (
    BacktestResult,
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
    _dynamic_exit_matches_bull_bar_reentry_window,
    _ema55_slope_entry_triggered,
    _ema55_slope_exit_condition_enabled,
    _ema55_slope_lock_profit_enabled,
    _ema55_slope_lock_profit_trigger_r,
    _ema55_slope_negative_entry_bars,
    _ema55_slope_ratio_from_series,
    _locked_r_matches_reentry_window,
    _raise_if_only_invalid_protection_configs,
    _should_require_bearish_reentry_after_dynamic_exit,
    _try_close_position,
    is_dynamic_protect_exit_reason,
    summarize_trade_exit_reasons,
)
from okx_quant.engine import build_protection_plan
from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import snap_to_increment
from okx_quant.protection_validation import InvalidProtectionPlanError
from okx_quant.strategy_catalog import is_btc_ema55_slope_short_strategy


HISTORY_PATH = Path(r"D:\qqokx_data\state\backtest_history.json")
SNAPSHOT_IDS = ("S096", "S097")
DEFAULT_DISTANCE_THRESHOLD = Decimal("1.5")
MIN_DISTANCE_SWEEP_THRESHOLDS = (
    Decimal("1.0"),
    Decimal("1.2"),
    Decimal("1.5"),
    Decimal("1.8"),
    Decimal("2.0"),
)

REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s096_s097_distance_confirmation_research_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s096_s097_distance_confirmation_research_latest.html"
LATEST_CSV_PATH = REPORT_DIR / "btc_s096_s097_distance_confirmation_research_latest.csv"


DECIMAL_FIELDS = {
    field.name
    for field in fields(StrategyConfig)
    if field.default is not MISSING and isinstance(field.default, Decimal)
}
DECIMAL_FIELDS.update({"atr_stop_multiplier", "atr_take_multiplier", "order_size"})
OPTIONAL_DECIMAL_FIELDS = {"risk_amount", "backtest_risk_percent"}
INT_FIELDS = {
    field.name
    for field in fields(StrategyConfig)
    if field.default is not MISSING and isinstance(field.default, int) and not isinstance(field.default, bool)
}
INT_FIELDS.update({"ema_period", "atr_period"})
BOOL_FIELDS = {
    field.name
    for field in fields(StrategyConfig)
    if field.default is not MISSING and isinstance(field.default, bool)
}


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    mode: str
    threshold: Decimal | None = None


@dataclass(frozen=True)
class Row:
    snapshot_id: str
    variant_key: str
    variant_label: str
    trades: int
    wins: int
    losses: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    ending_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    avg_pnl: Decimal
    stop_loss_hits: int
    total_fees: Decimal
    positive_months: int
    negative_months: int
    worst_month: str
    best_month: str
    exit_summary: str


BASE_VARIANTS = (
    Variant("baseline", "Baseline", "baseline"),
    Variant("hard_no_chase_1_5", "Hard no-chase > 1.5 ATR", "hard_no_chase", threshold=DEFAULT_DISTANCE_THRESHOLD),
    Variant("far_break_prev_low", "Far entry requires close < prev low", "far_break_prev_low", threshold=DEFAULT_DISTANCE_THRESHOLD),
    Variant(
        "far_bear_break_prev_low",
        "Far entry requires bear bar + prev low break",
        "far_bear_break_prev_low",
        threshold=DEFAULT_DISTANCE_THRESHOLD,
    ),
    Variant(
        "locked_reentry_far_bear_break",
        "Only locked-exit reentry far entry requires bear bar + prev low break",
        "locked_reentry_far_bear_break",
        threshold=DEFAULT_DISTANCE_THRESHOLD,
    ),
    Variant("block_distance_2_3", "Block distance 2.0-3.0 ATR", "block_distance_2_3"),
    Variant(
        "block_near_and_mid",
        "Block below-EMA distance < 1.5 ATR and 2.0-3.0 ATR",
        "block_near_and_mid",
        threshold=DEFAULT_DISTANCE_THRESHOLD,
    ),
)

MIN_DISTANCE_SWEEP_VARIANTS = tuple(
    Variant(
        key=f"min_distance_{str(threshold).replace('.', '_')}",
        label=f"Below EMA requires distance >= {threshold} ATR",
        mode="min_distance",
        threshold=threshold,
    )
    for threshold in MIN_DISTANCE_SWEEP_THRESHOLDS
)

VARIANTS = BASE_VARIANTS + MIN_DISTANCE_SWEEP_VARIANTS


def main() -> None:
    snapshots = load_snapshots()
    configs = {snapshot_id: config_from_snapshot(snapshots[snapshot_id]["config"]) for snapshot_id in SNAPSHOT_IDS}
    first_config = next(iter(configs.values()))

    client = OkxRestClient()
    instrument = client.get_instrument(first_config.inst_id)
    candles = client.get_candles_history(first_config.inst_id, first_config.bar, limit=0)
    if not candles:
        raise RuntimeError(f"No candles returned for {first_config.inst_id} {first_config.bar}")

    rows: list[Row] = []
    for snapshot_id in SNAPSHOT_IDS:
        config = configs[snapshot_id]
        maker_fee_rate = Decimal(str(snapshots[snapshot_id].get("maker_fee_rate", "0")))
        taker_fee_rate = Decimal(str(snapshots[snapshot_id].get("taker_fee_rate", "0")))
        for variant in VARIANTS:
            result = run_experiment(
                candles,
                instrument,
                config,
                variant=variant,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
            )
            rows.append(build_row(snapshot_id, variant, result))

    write_csv(rows)
    html_text = build_html(rows, configs, candles)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(CSV_PATH)
    for row in rows:
        print(
            f"{row.snapshot_id},{row.variant_key},trades={row.trades},"
            f"pnl={fmt(row.total_pnl, 2)},dd={fmt(row.max_drawdown_pct, 2)},pf={fmt_or_dash(row.profit_factor, 4)}"
        )


def run_experiment(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    variant: Variant,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
) -> BacktestResult:
    trades, terminal_open_position = run_experiment_trades(
        candles,
        instrument,
        config,
        variant=variant,
        taker_fee_rate=taker_fee_rate,
    )
    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    trend_ema_values = moving_average(closes, int(config.trend_ema_period), config.resolved_trend_ema_type())
    atr_values = atr(candles, int(config.atr_period))
    initial_capital = config.backtest_initial_capital
    equity_curve = _build_equity_curve(candles, trades)
    net_value_curve = [initial_capital + value for value in equity_curve]
    drawdown_curve, drawdown_pct_curve = _build_drawdown_curves(net_value_curve)
    report = _build_report(trades, initial_capital=initial_capital)
    return BacktestResult(
        candles=candles,
        trades=trades,
        report=report,
        instrument=instrument,
        ema_values=ema_values,
        trend_ema_values=trend_ema_values,
        entry_reference_ema_values=ema_values,
        atr_values=atr_values,
        equity_curve=equity_curve,
        net_value_curve=net_value_curve,
        drawdown_curve=drawdown_curve,
        drawdown_pct_curve=drawdown_pct_curve,
        monthly_stats=_build_period_stats(trades, initial_capital=initial_capital, by="month"),
        yearly_stats=_build_period_stats(trades, initial_capital=initial_capital, by="year"),
        initial_capital=initial_capital,
        ema_period=config.ema_period,
        ema_type=config.resolved_ema_type(),
        trend_ema_period=config.trend_ema_period,
        trend_ema_type=config.resolved_trend_ema_type(),
        entry_reference_ema_period=config.resolved_entry_reference_ema_period(),
        entry_reference_ema_type=config.resolved_entry_reference_ema_type(),
        big_ema_period=config.big_ema_period,
        atr_period=config.atr_period,
        strategy_id=config.strategy_id,
        bar=config.bar,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
        exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
        slippage_rate=config.resolved_backtest_exit_slippage_rate(),
        funding_rate=config.backtest_funding_rate,
        take_profit_mode=str(config.take_profit_mode),
        dynamic_two_r_break_even=bool(config.dynamic_two_r_break_even),
        dynamic_fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
        ema55_slope_exit_enabled=bool(config.ema55_slope_exit_enabled),
        ema55_slope_lock_profit_enabled=bool(config.ema55_slope_lock_profit_enabled),
        ema55_slope_lock_profit_trigger_r=max(int(config.ema55_slope_lock_profit_trigger_r), 2),
        ema55_slope_negative_entry_bars=max(int(config.ema55_slope_negative_entry_bars), 1),
        trend_ema_slope_filter_min_ratio=Decimal(str(config.trend_ema_slope_filter_min_ratio)),
        sizing_mode=config.backtest_sizing_mode,
        compounding=config.backtest_compounding,
        open_position=terminal_open_position,
    )


def run_experiment_trades(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    variant: Variant,
    taker_fee_rate: Decimal,
) -> tuple[list[Any], Any | None]:
    negative_entry_bars = _ema55_slope_negative_entry_bars(config)
    minimum = max(int(config.ema_period), int(config.trend_ema_period), int(config.atr_period), 2) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"Not enough candles, need at least {minimum}.")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    ema21_values = moving_average(closes, 21, "ema")
    atr_values = atr(candles, int(config.atr_period))
    trades: list[Any] = []
    open_position: Any | None = None
    entry_slope_threshold_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    uses_flat_exit = is_btc_ema55_slope_short_strategy(config.strategy_id)
    slope_exit_enabled = _ema55_slope_exit_condition_enabled(config)
    dynamic_take_profit_enabled = _ema55_slope_lock_profit_enabled(config)
    dynamic_trigger_r = _ema55_slope_lock_profit_trigger_r(config)
    take_profit_enabled = not uses_flat_exit
    reentry_reclaim_state: str | None = None
    reentry_ema21_near_state: str | None = None
    reentry_bearish_bar_required = False
    valid_entry_plan_count = 0
    invalid_protection_count = 0
    last_exit_reason: str | None = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        current_ema = ema_values[index]
        current_ema21 = ema21_values[index] if index < len(ema21_values) else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        if current_ema is None or current_ema21 is None or atr_value is None or atr_value <= 0:
            continue

        previous_ema = ema_values[index - 1] if index > 0 else None
        slope = (current_ema - previous_ema) if previous_ema is not None else Decimal("0")
        recent_slope_ratios = [
            _ema55_slope_ratio_from_series(ema_values, slope_index)
            for slope_index in range(index - negative_entry_bars + 1, index + 1)
        ]
        exited_this_bar = False

        if open_position is not None:
            closed_trade = _try_close_position(
                open_position,
                candle,
                index,
                exit_fee_rate=taker_fee_rate,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None
                exited_this_bar = True
                last_exit_reason = closed_trade.exit_reason
                if _should_require_bearish_reentry_after_dynamic_exit(config, closed_trade.exit_reason):
                    reentry_bearish_bar_required = True
                if (
                    config.ema55_slope_dynamic_exit_requires_ema_reclaim
                    and is_dynamic_protect_exit_reason(closed_trade.exit_reason)
                ):
                    reentry_reclaim_state = "await_reclaim_above_ema"
                if (
                    config.ema55_slope_locked_reentry_requires_ema21_near
                    and _locked_r_matches_reentry_window(
                        closed_trade.exit_reason,
                        min_r=int(config.ema55_slope_locked_reentry_min_r),
                        max_r=int(config.ema55_slope_locked_reentry_max_r),
                    )
                ):
                    reentry_ema21_near_state = "await_near_ema21"
                if (
                    config.ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry
                    and _dynamic_exit_matches_bull_bar_reentry_window(
                        closed_trade.exit_reason,
                        min_r=int(config.ema55_slope_dynamic_exit_bull_bar_reentry_min_r),
                        max_r=int(config.ema55_slope_dynamic_exit_bull_bar_reentry_max_r),
                    )
                    and candle.close > candle.open
                ):
                    reentry_bearish_bar_required = True

        if open_position is not None and slope_exit_enabled and (((slope >= 0) if uses_flat_exit else (slope > 0))):
            exit_price_raw = snap_to_increment(candle.close, instrument.tick_size, "nearest")
            exit_price = _apply_slippage_price(
                exit_price_raw,
                signal=open_position.signal,
                tick_size=open_position.tick_size,
                slippage_rate=open_position.exit_slippage_rate,
                is_entry=False,
            )
            closed_trade = _build_closed_trade(
                open_position,
                candle,
                index,
                exit_price_raw=exit_price_raw,
                exit_price=exit_price,
                exit_reason="slope_turn_positive",
                exit_fee_rate=taker_fee_rate,
                exit_fee_type="taker",
            )
            trades.append(closed_trade)
            open_position = None
            exited_this_bar = True
            last_exit_reason = closed_trade.exit_reason

        if (
            open_position is not None
            or not _ema55_slope_entry_triggered(
                config,
                recent_slope_ratios=recent_slope_ratios,
                threshold=entry_slope_threshold_ratio,
            )
        ):
            continue
        if config.ema55_slope_same_bar_reentry_block and exited_this_bar:
            continue
        if reentry_reclaim_state is not None:
            if reentry_reclaim_state == "await_reclaim_above_ema":
                if candle.close >= current_ema:
                    reentry_reclaim_state = "await_rebreak_below_ema"
                continue
            if reentry_reclaim_state == "await_rebreak_below_ema" and candle.close >= current_ema:
                continue
            reentry_reclaim_state = None
        if reentry_ema21_near_state is not None:
            near_threshold = atr_value * Decimal("0.3")
            if reentry_ema21_near_state == "await_near_ema21":
                if abs(candle.close - current_ema21) <= near_threshold:
                    reentry_ema21_near_state = "await_rebreak_below_ema21"
                continue
            if reentry_ema21_near_state == "await_rebreak_below_ema21" and candle.close >= current_ema21:
                continue
            reentry_ema21_near_state = None
        if reentry_bearish_bar_required and (exited_this_bar or candle.close >= candle.open):
            continue
        if (
            reentry_bearish_bar_required
            and config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low
            and (index <= 0 or candle.close >= candles[index - 1].low)
        ):
            continue
        if not experiment_allows_entry(
            variant,
            candles,
            index,
            current_ema=current_ema,
            atr_value=atr_value,
            last_exit_reason=last_exit_reason,
        ):
            continue

        try:
            protection = build_protection_plan(
                instrument=instrument,
                config=config,
                direction="short",
                entry_reference=candle.close,
                atr_value=atr_value,
                candle_ts=candle.ts,
                trigger_inst_id=instrument.inst_id,
            )
        except InvalidProtectionPlanError:
            invalid_protection_count += 1
            continue
        valid_entry_plan_count += 1
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=bool(config.risk_amount is not None and config.risk_amount > 0),
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal="short",
            entry_index=index,
            entry_ts=candle.ts,
            entry_price_raw=protection.entry_reference,
            stop_loss=protection.stop_loss,
            take_profit=protection.take_profit,
            atr_value=protection.atr_value,
            size=size,
            entry_fee_rate=taker_fee_rate,
            exit_fee_rate=taker_fee_rate,
            entry_fee_type="taker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            take_profit_enabled=take_profit_enabled,
            dynamic_exit_fee_rate=taker_fee_rate,
            dynamic_two_r_break_even=config.dynamic_two_r_break_even,
            dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            next_dynamic_trigger_r=dynamic_trigger_r,
            apply_entry_slippage=True,
        )
        last_exit_reason = None
        reentry_reclaim_state = None
        if reentry_bearish_bar_required and candle.close < candle.open:
            reentry_bearish_bar_required = False

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles)


def experiment_allows_entry(
    variant: Variant,
    candles: list[Candle],
    index: int,
    *,
    current_ema: Decimal,
    atr_value: Decimal,
    last_exit_reason: str | None,
) -> bool:
    threshold = variant.threshold if variant.threshold is not None else DEFAULT_DISTANCE_THRESHOLD
    if variant.mode == "baseline":
        return True
    candle = candles[index]
    if candle.close >= current_ema:
        return True
    distance_atr = (current_ema - candle.close) / atr_value
    if variant.mode == "min_distance":
        return distance_atr >= threshold
    if variant.mode == "block_distance_2_3":
        return not (Decimal("2.0") < distance_atr <= Decimal("3.0"))
    if variant.mode == "block_near_and_mid":
        return distance_atr >= threshold and not (Decimal("2.0") < distance_atr <= Decimal("3.0"))
    if distance_atr <= threshold:
        return True
    bearish_break_prev_low = index > 0 and candle.close < candle.open and candle.close < candles[index - 1].low
    break_prev_low = index > 0 and candle.close < candles[index - 1].low
    if variant.mode == "hard_no_chase":
        return False
    if variant.mode == "far_break_prev_low":
        return break_prev_low
    if variant.mode == "far_bear_break_prev_low":
        return bearish_break_prev_low
    if variant.mode == "locked_reentry_far_bear_break":
        if last_exit_reason is not None and is_dynamic_protect_exit_reason(last_exit_reason):
            return bearish_break_prev_low
        return True
    raise ValueError(f"Unknown variant mode: {variant.mode}")


def build_row(snapshot_id: str, variant: Variant, result: BacktestResult) -> Row:
    report = result.report
    monthly = result.monthly_stats
    positive_months = sum(1 for stat in monthly if stat.total_pnl > 0)
    negative_months = sum(1 for stat in monthly if stat.total_pnl < 0)
    worst_month = min(monthly, key=lambda stat: stat.total_pnl) if monthly else None
    best_month = max(monthly, key=lambda stat: stat.total_pnl) if monthly else None
    return Row(
        snapshot_id=snapshot_id,
        variant_key=variant.key,
        variant_label=variant.label,
        trades=report.total_trades,
        wins=report.win_trades,
        losses=report.loss_trades,
        win_rate_pct=report.win_rate,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        ending_equity=report.ending_equity,
        max_drawdown=report.max_drawdown,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        avg_pnl=report.average_pnl,
        stop_loss_hits=report.stop_loss_hits,
        total_fees=report.total_fees,
        positive_months=positive_months,
        negative_months=negative_months,
        worst_month=format_period_stat(worst_month),
        best_month=format_period_stat(best_month),
        exit_summary=" / ".join(f"{label}:{count}" for label, count in summarize_trade_exit_reasons(result.trades)),
    )


def write_csv(rows: list[Row]) -> None:
    fieldnames = [field.name for field in fields(Row)]
    for path in (CSV_PATH, LATEST_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "snapshot_id": row.snapshot_id,
                        "variant_key": row.variant_key,
                        "variant_label": row.variant_label,
                        "trades": row.trades,
                        "wins": row.wins,
                        "losses": row.losses,
                        "win_rate_pct": fmt(row.win_rate_pct, 2),
                        "total_pnl": fmt(row.total_pnl, 2),
                        "return_pct": fmt(row.return_pct, 2),
                        "ending_equity": fmt(row.ending_equity, 2),
                        "max_drawdown": fmt(row.max_drawdown, 2),
                        "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                        "profit_factor": fmt_or_dash(row.profit_factor, 4),
                        "avg_r": fmt(row.avg_r, 4),
                        "avg_pnl": fmt(row.avg_pnl, 2),
                        "stop_loss_hits": row.stop_loss_hits,
                        "total_fees": fmt(row.total_fees, 2),
                        "positive_months": row.positive_months,
                        "negative_months": row.negative_months,
                        "worst_month": row.worst_month,
                        "best_month": row.best_month,
                        "exit_summary": row.exit_summary,
                    }
                )


def build_html(rows: list[Row], configs: dict[str, StrategyConfig], candles: list[Candle]) -> str:
    table = build_table(rows)
    findings = build_findings(rows)
    assumption_items = "".join(f"<li>{html.escape(item)}</li>" for item in build_assumptions(configs, candles))
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC Distance Confirmation Research</title>
  <style>
    body {{ margin: 0; font-family: "Segoe UI", "Microsoft YaHei", sans-serif; color: #1f2937; background: #f7f8fb; }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 28px 20px 56px; }}
    .panel {{ background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 18px 20px; margin-bottom: 16px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin: 0 0 10px; font-size: 19px; }}
    p, li {{ line-height: 1.55; }}
    .muted {{ color: #667085; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12.5px; }}
    th, td {{ padding: 9px 7px; border-bottom: 1px solid #e6edf5; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2), th:last-child, td:last-child {{ text-align: left; }}
    th {{ color: #667085; font-weight: 700; background: #f2f6fb; }}
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
    code {{ background: #eef4ff; padding: 1px 4px; border-radius: 4px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>BTC S096/S097 Distance Confirmation Research</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. This is a research-only simulation. No strategy source, UI defaults, or live execution logic are changed.</p>
    </section>
    <section class="panel">
      <h2>Assumptions</h2>
      <ul>{assumption_items}</ul>
    </section>
    <section class="panel">
      <h2>Findings</h2>
      <ul>{finding_items}</ul>
    </section>
    <section class="panel">
      <h2>Performance</h2>
      {table}
    </section>
  </div>
</body>
</html>
"""


def build_assumptions(configs: dict[str, StrategyConfig], candles: list[Candle]) -> list[str]:
    first = next(iter(configs.values()))
    return [
        f"Snapshots: {', '.join(SNAPSHOT_IDS)} from {HISTORY_PATH}",
        f"Symbol: {first.inst_id} | Bar: {first.bar} | Candles: {len(candles)} | Range: {format_ts(candles[0].ts)} to {format_ts(candles[-1].ts)}",
        f"Fixed-threshold controls use close below EMA55 by more than {fmt(DEFAULT_DISTANCE_THRESHOLD, 1)} ATR.",
        "Min-distance sweep: 1.0 / 1.2 / 1.5 / 1.8 / 2.0 ATR.",
        "Hard no-chase is included as a control because previous results showed it damages expectancy.",
        "Confirmation variants only add conditions inside this research script; original strategy code is untouched.",
    ]


def build_findings(rows: list[Row]) -> list[str]:
    findings: list[str] = []
    for snapshot_id in SNAPSHOT_IDS:
        group = [row for row in rows if row.snapshot_id == snapshot_id]
        baseline = next(row for row in group if row.variant_key == "baseline")
        min_distance_rows = [row for row in group if row.variant_key.startswith("min_distance_")]
        best_min_distance = max(min_distance_rows, key=lambda row: (row.return_pct, row.profit_factor or Decimal("-1")))
        best_return = max(group, key=lambda row: row.return_pct)
        lowest_dd = min(group, key=lambda row: row.max_drawdown_pct)
        findings.append(
            f"{snapshot_id}: best return is {best_return.variant_label} ({fmt(best_return.return_pct, 2)}%, DD {fmt(best_return.max_drawdown_pct, 2)}%, PF {fmt_or_dash(best_return.profit_factor, 4)})."
        )
        findings.append(
            f"{snapshot_id}: lowest drawdown is {lowest_dd.variant_label} (DD {fmt(lowest_dd.max_drawdown_pct, 2)}%, return {fmt(lowest_dd.return_pct, 2)}%)."
        )
        findings.append(
            f"{snapshot_id}: best min-distance threshold is {best_min_distance.variant_label} ({fmt(best_min_distance.return_pct, 2)}%, DD {fmt(best_min_distance.max_drawdown_pct, 2)}%, PF {fmt_or_dash(best_min_distance.profit_factor, 4)})."
        )
        for row in group:
            if row.variant_key == "baseline":
                continue
            findings.append(
                f"{snapshot_id} {row.variant_label}: PnL {delta_decimal(row.total_pnl, baseline.total_pnl)}U vs baseline, DD {delta_decimal(row.max_drawdown_pct, baseline.max_drawdown_pct)} pct points, trades {row.trades - baseline.trades:+d}."
            )
    return findings


def build_table(rows: list[Row]) -> str:
    body = []
    for row in sorted(rows, key=lambda item: (item.snapshot_id, [v.key for v in VARIANTS].index(item.variant_key))):
        body.append(
            "<tr>"
            f"<td>{html.escape(row.snapshot_id)}</td>"
            f"<td>{html.escape(row.variant_label)}</td>"
            f"<td>{row.trades}</td>"
            f"<td>{row.wins}/{row.losses}</td>"
            f"<td>{fmt(row.win_rate_pct, 2)}%</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td class='{metric_class(row.return_pct)}'>{fmt(row.return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown, 2)} / {fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{fmt_or_dash(row.profit_factor, 4)}</td>"
            f"<td>{fmt(row.avg_r, 4)}</td>"
            f"<td>{row.stop_loss_hits}</td>"
            f"<td>{fmt(row.total_fees, 2)}</td>"
            f"<td>{row.positive_months}/{row.negative_months}</td>"
            f"<td>{html.escape(row.worst_month)}<br><span class='muted'>{html.escape(row.best_month)}</span></td>"
            f"<td>{html.escape(row.exit_summary)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Snapshot</th><th>Variant</th><th>Trades</th><th>W/L</th><th>Win Rate</th>"
        "<th>Total PnL</th><th>Return</th><th>Max DD</th><th>PF</th><th>Avg R</th>"
        "<th>SL Hits</th><th>Fees</th><th>+/- Months</th><th>Worst / Best Month</th><th>Exit Summary</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def load_snapshots() -> dict[str, dict[str, Any]]:
    payload = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    records = payload.get("records", payload) if isinstance(payload, dict) else payload
    snapshots: dict[str, dict[str, Any]] = {}
    for item in records:
        snapshot_id = str(item.get("snapshot_id", ""))
        if snapshot_id in SNAPSHOT_IDS:
            snapshots[snapshot_id] = item
    missing = [snapshot_id for snapshot_id in SNAPSHOT_IDS if snapshot_id not in snapshots]
    if missing:
        raise RuntimeError(f"Missing snapshots: {', '.join(missing)}")
    return snapshots


def config_from_snapshot(raw_config: dict[str, Any]) -> StrategyConfig:
    kwargs: dict[str, Any] = {}
    for field in fields(StrategyConfig):
        if field.name not in raw_config:
            continue
        kwargs[field.name] = convert_config_value(field.name, raw_config[field.name])
    return StrategyConfig(**kwargs)


def convert_config_value(name: str, value: Any) -> Any:
    if value is None:
        return None
    if name in DECIMAL_FIELDS or name in OPTIONAL_DECIMAL_FIELDS:
        return Decimal(str(value))
    if name in INT_FIELDS:
        return int(value)
    if name in BOOL_FIELDS:
        return bool(value)
    if name == "rail_candidate_ema_periods":
        return tuple(int(item) for item in value)
    return value


def format_period_stat(stat: Any | None) -> str:
    if stat is None:
        return "-"
    return f"{stat.period_label} {fmt(stat.total_pnl, 2)}"


def metric_class(value: Decimal) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


def delta_decimal(value: Decimal, baseline: Decimal) -> str:
    return fmt(value - baseline, 2, signed=True)


def fmt_or_dash(value: Decimal | None, places: int) -> str:
    if value is None:
        return "-"
    return fmt(value, places)


def fmt(value: Decimal, places: int, *, signed: bool = False) -> str:
    quant = Decimal(1).scaleb(-places)
    text = f"{value.quantize(quant):f}"
    if signed and value > 0:
        return f"+{text}"
    return text


def format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
