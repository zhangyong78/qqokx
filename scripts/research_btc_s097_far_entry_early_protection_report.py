from __future__ import annotations

import csv
import html
import importlib.util
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MODULE_PATH = ROOT / "scripts" / "research_btc_s096_s097_distance_confirmation_compare.py"
SPEC = importlib.util.spec_from_file_location("distance_research_module_far_protect", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"Unable to load research module: {MODULE_PATH}")
RESEARCH = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RESEARCH
SPEC.loader.exec_module(RESEARCH)

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
    _candle_path_points,
    _create_open_position,
    _determine_backtest_order_size,
    _dynamic_exit_matches_bull_bar_reentry_window,
    _dynamic_fee_offset,
    _ema55_slope_entry_triggered,
    _ema55_slope_exit_condition_enabled,
    _ema55_slope_lock_profit_enabled,
    _ema55_slope_lock_profit_trigger_r,
    _ema55_slope_negative_entry_bars,
    _ema55_slope_ratio_from_series,
    _holding_bars_for_position,
    _locked_r_matches_reentry_window,
    _position_strategy_entry_price,
    _process_dynamic_position_segment,
    _raise_if_only_invalid_protection_configs,
    _should_require_bearish_reentry_after_dynamic_exit,
    _time_stop_break_even_price,
    _try_close_position,
    is_dynamic_protect_exit_reason,
    summarize_trade_exit_reasons,
)
from okx_quant.engine import build_protection_plan
from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.pricing import snap_to_increment
from okx_quant.protection_validation import InvalidProtectionPlanError
from okx_quant.strategy_catalog import is_btc_ema55_slope_short_strategy


REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_s097_far_entry_early_protection_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
LATEST_HTML_PATH = REPORT_DIR / "btc_s097_far_entry_early_protection_latest.html"
LATEST_CSV_PATH = REPORT_DIR / "btc_s097_far_entry_early_protection_latest.csv"

SNAPSHOT_ID = "S097"
FAR_ENTRY_DISTANCE_ATR = Decimal("2.0")
WINDOWS = (
    ("recent_10000", "Recent 10000 bars", 10_000),
    ("full_history", "Full history", 0),
)


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    trigger_r: Decimal | None
    lock_r: Decimal | None
    cooldown_bars: int = 0
    require_rebreak_prev_low: bool = False
    far_distance_threshold: Decimal = FAR_ENTRY_DISTANCE_ATR


@dataclass(frozen=True)
class Row:
    window_key: str
    window_label: str
    variant_key: str
    variant_label: str
    trades: int
    wins: int
    losses: int
    win_rate_pct: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    avg_r: Decimal
    stop_loss_hits: int
    early_stop_hits: int
    total_fees: Decimal
    positive_months: int
    negative_months: int
    worst_month: str
    exit_summary: str


VARIANTS = (
    Variant("baseline", "Baseline", trigger_r=None, lock_r=None),
    Variant("far_be_0_5r", "Far >2 ATR: BE at 0.5R", trigger_r=Decimal("0.5"), lock_r=Decimal("0")),
    Variant(
        "far3_be_0_5r",
        "Far >3 ATR: BE at 0.5R",
        trigger_r=Decimal("0.5"),
        lock_r=Decimal("0"),
        far_distance_threshold=Decimal("3.0"),
    ),
    Variant(
        "far3_be_1_0r",
        "Far >3 ATR: BE at 1.0R",
        trigger_r=Decimal("1.0"),
        lock_r=Decimal("0"),
        far_distance_threshold=Decimal("3.0"),
    ),
    Variant("far_be_0_5r_cd3", "Far >2 ATR: BE at 0.5R + cooldown 3 bars", trigger_r=Decimal("0.5"), lock_r=Decimal("0"), cooldown_bars=3),
    Variant("far_be_0_5r_cd6", "Far >2 ATR: BE at 0.5R + cooldown 6 bars", trigger_r=Decimal("0.5"), lock_r=Decimal("0"), cooldown_bars=6),
    Variant(
        "far_be_0_5r_rebreak",
        "Far >2 ATR: BE at 0.5R + rebreak exit-candle low",
        trigger_r=Decimal("0.5"),
        lock_r=Decimal("0"),
        require_rebreak_prev_low=True,
    ),
    Variant(
        "far_be_0_5r_cd3_rebreak",
        "Far >2 ATR: BE at 0.5R + cooldown 3 bars + rebreak exit-candle low",
        trigger_r=Decimal("0.5"),
        lock_r=Decimal("0"),
        cooldown_bars=3,
        require_rebreak_prev_low=True,
    ),
)


def main() -> None:
    snapshots = RESEARCH.load_snapshots()
    config = RESEARCH.config_from_snapshot(snapshots[SNAPSHOT_ID]["config"])
    maker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("maker_fee_rate", "0")))
    taker_fee_rate = Decimal(str(snapshots[SNAPSHOT_ID].get("taker_fee_rate", "0")))
    client = RESEARCH.OkxRestClient()
    instrument = client.get_instrument(config.inst_id)

    rows: list[Row] = []
    for window_key, window_label, candle_limit in WINDOWS:
        candles = client.get_candles_history(config.inst_id, config.bar, limit=candle_limit)
        for variant in VARIANTS:
            result = run_variant(
                candles,
                instrument,
                config,
                variant=variant,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
            )
            rows.append(build_row(window_key, window_label, variant, result))

    write_csv(rows)
    html_text = build_html(rows)
    HTML_PATH.write_text(html_text, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_text, encoding="utf-8")

    print(HTML_PATH)
    print(CSV_PATH)
    for row in rows:
        print(
            f"{row.window_key},{row.variant_key},trades={row.trades},"
            f"pnl={fmt(row.total_pnl, 2)},dd={fmt(row.max_drawdown_pct, 2)},pf={fmt_or_dash(row.profit_factor, 4)}"
        )


def run_variant(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    variant: Variant,
    maker_fee_rate: Decimal,
    taker_fee_rate: Decimal,
) -> BacktestResult:
    trades, terminal_open_position = run_variant_trades(
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


def run_variant_trades(
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
    open_meta: dict[str, Any] | None = None
    entry_slope_threshold_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    uses_flat_exit = is_btc_ema55_slope_short_strategy(config.strategy_id)
    slope_exit_enabled = _ema55_slope_exit_condition_enabled(config)
    dynamic_take_profit_enabled = _ema55_slope_lock_profit_enabled(config)
    dynamic_trigger_r = _ema55_slope_lock_profit_trigger_r(config)
    take_profit_enabled = not uses_flat_exit
    reentry_reclaim_state: str | None = None
    reentry_ema21_near_state: str | None = None
    reentry_bearish_bar_required = False
    early_reentry_gate: dict[str, Any] | None = None
    valid_entry_plan_count = 0
    invalid_protection_count = 0

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        current_ema = ema_values[index]
        current_ema21 = ema21_values[index] if index < len(ema21_values) else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        if current_ema is None or current_ema21 is None or atr_value is None or atr_value <= 0:
            continue
        early_reentry_gate = advance_early_reentry_gate(
            early_reentry_gate,
            candle,
            candle_index=index,
        )

        previous_ema = ema_values[index - 1] if index > 0 else None
        slope = (current_ema - previous_ema) if previous_ema is not None else Decimal("0")
        recent_slope_ratios = [
            _ema55_slope_ratio_from_series(ema_values, slope_index)
            for slope_index in range(index - negative_entry_bars + 1, index + 1)
        ]
        exited_this_bar = False

        if open_position is not None:
            closed_trade = try_close_position_with_variant(
                open_position,
                candle,
                index,
                variant=variant,
                meta=open_meta or {},
                exit_fee_rate=taker_fee_rate,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                early_reentry_gate = update_early_reentry_gate(
                    variant,
                    closed_trade,
                    candle,
                    candle_index=index,
                )
                open_position = None
                open_meta = None
                exited_this_bar = True
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
            trades.append(
                _build_closed_trade(
                    open_position,
                    candle,
                    index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason="slope_turn_positive",
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
            )
            open_position = None
            open_meta = None
            exited_this_bar = True

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
        if early_reentry_gate_blocks(early_reentry_gate, candle_index=index):
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
        open_meta = build_position_meta(variant, candle, current_ema=current_ema, atr_value=atr_value)
        reentry_reclaim_state = None
        if reentry_bearish_bar_required and candle.close < candle.open:
            reentry_bearish_bar_required = False

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles)


def build_position_meta(variant: Variant, candle: Candle, *, current_ema: Decimal, atr_value: Decimal) -> dict[str, Any]:
    entry_distance_atr = Decimal("0")
    if atr_value > 0 and candle.close < current_ema:
        entry_distance_atr = (current_ema - candle.close) / atr_value
    return {
        "entry_distance_atr": entry_distance_atr,
        "far_entry_enabled": variant.trigger_r is not None and entry_distance_atr > Decimal(str(variant.far_distance_threshold)),
        "early_stop_applied": False,
        "early_stop_price": None,
        "early_stop_reason": (
            None
            if variant.trigger_r is None
            else f"far_early_lock_{str(variant.lock_r).replace('.', '_')}r_at_{str(variant.trigger_r).replace('.', '_')}r"
        ),
    }


def update_early_reentry_gate(
    variant: Variant,
    closed_trade: Any,
    candle: Candle,
    *,
    candle_index: int,
) -> dict[str, Any] | None:
    if not str(closed_trade.exit_reason).startswith("far_early_lock_"):
        return None
    if variant.cooldown_bars <= 0 and not variant.require_rebreak_prev_low:
        return None
    return {
        "cooldown_until_index": candle_index + max(int(variant.cooldown_bars), 0),
        "rebreak_low": candle.low,
        "require_rebreak_prev_low": bool(variant.require_rebreak_prev_low),
    }


def advance_early_reentry_gate(
    gate: dict[str, Any] | None,
    candle: Candle,
    *,
    candle_index: int,
) -> dict[str, Any] | None:
    if gate is None:
        return None
    if candle_index <= int(gate.get("cooldown_until_index", -1)):
        return gate
    if bool(gate.get("require_rebreak_prev_low")):
        rebreak_low = Decimal(str(gate.get("rebreak_low", "0")))
        if candle.close >= rebreak_low:
            return gate
    return None


def early_reentry_gate_blocks(gate: dict[str, Any] | None, *, candle_index: int) -> bool:
    if gate is None:
        return False
    return candle_index <= int(gate.get("cooldown_until_index", -1)) or bool(gate.get("require_rebreak_prev_low"))


def try_close_position_with_variant(
    position: Any,
    candle: Candle,
    candle_index: int,
    *,
    variant: Variant,
    meta: dict[str, Any],
    exit_fee_rate: Decimal,
    exit_fee_type: str,
) -> Any | None:
    if variant.trigger_r is None or not meta.get("far_entry_enabled"):
        return _try_close_position(
            position,
            candle,
            candle_index,
            exit_fee_rate=exit_fee_rate,
            exit_fee_type=exit_fee_type,
        )
    if candle_index < position.entry_index or candle_index == position.entry_index:
        return None
    if not position.dynamic_take_profit_enabled:
        return _try_close_position(
            position,
            candle,
            candle_index,
            exit_fee_rate=exit_fee_rate,
            exit_fee_type=exit_fee_type,
        )

    path_points = _candle_path_points(candle)
    holding_bars = _holding_bars_for_position(position, candle_index)
    segment_start = path_points[0]
    for segment_end in path_points[1:]:
        touched_exit = _process_dynamic_position_segment(
            position,
            segment_start,
            segment_end,
            holding_bars=holding_bars,
        )
        if touched_exit is not None:
            exit_price_raw, exit_reason = touched_exit
            if (
                exit_reason == "stop_loss"
                and meta.get("early_stop_applied")
                and meta.get("early_stop_price") == exit_price_raw
            ):
                exit_reason = str(meta.get("early_stop_reason"))
            exit_price = _apply_slippage_price(
                exit_price_raw,
                signal=position.signal,
                tick_size=position.tick_size,
                slippage_rate=position.exit_slippage_rate,
                is_entry=False,
            )
            return _build_closed_trade(
                position,
                candle,
                candle_index,
                exit_price_raw=exit_price_raw,
                exit_price=exit_price,
                exit_reason=exit_reason,
                exit_fee_rate=exit_fee_rate,
                exit_fee_type=exit_fee_type,
            )
        if position.signal == "short" and segment_end < segment_start:
            maybe_apply_early_far_protection(position, variant, meta, favorable_price=segment_end)
        segment_start = segment_end
    return None


def maybe_apply_early_far_protection(
    position: Any,
    variant: Variant,
    meta: dict[str, Any],
    *,
    favorable_price: Decimal,
) -> None:
    if variant.trigger_r is None or variant.lock_r is None:
        return
    if not meta.get("far_entry_enabled") or meta.get("early_stop_applied"):
        return
    trigger_price = early_trigger_price(position, variant.trigger_r)
    if favorable_price > trigger_price:
        return
    candidate_stop = early_lock_stop_price(position, variant.lock_r)
    if candidate_stop >= position.stop_loss:
        return
    position.stop_loss = candidate_stop
    meta["early_stop_applied"] = True
    meta["early_stop_price"] = candidate_stop


def early_trigger_price(position: Any, trigger_r: Decimal) -> Decimal:
    entry_price = _position_strategy_entry_price(position)
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = entry_price - (position.risk_per_unit * trigger_r) - fee_offset
    return snap_to_increment(raw, position.tick_size, "down")


def early_lock_stop_price(position: Any, lock_r: Decimal) -> Decimal:
    if lock_r <= 0:
        return _time_stop_break_even_price(position)
    entry_price = _position_strategy_entry_price(position)
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = entry_price - (position.risk_per_unit * lock_r) - fee_offset
    return snap_to_increment(raw, position.tick_size, "down")


def build_row(window_key: str, window_label: str, variant: Variant, result: BacktestResult) -> Row:
    report = result.report
    monthly = result.monthly_stats
    positive_months = sum(1 for stat in monthly if stat.total_pnl > 0)
    negative_months = sum(1 for stat in monthly if stat.total_pnl < 0)
    worst_month = min(monthly, key=lambda stat: stat.total_pnl) if monthly else None
    early_stop_hits = sum(1 for trade in result.trades if str(trade.exit_reason).startswith("far_early_lock_"))
    return Row(
        window_key=window_key,
        window_label=window_label,
        variant_key=variant.key,
        variant_label=variant.label,
        trades=report.total_trades,
        wins=report.win_trades,
        losses=report.loss_trades,
        win_rate_pct=report.win_rate,
        total_pnl=report.total_pnl,
        return_pct=report.total_return_pct,
        max_drawdown_pct=report.max_drawdown_pct,
        profit_factor=report.profit_factor,
        avg_r=report.average_r_multiple,
        stop_loss_hits=report.stop_loss_hits,
        early_stop_hits=early_stop_hits,
        total_fees=report.total_fees,
        positive_months=positive_months,
        negative_months=negative_months,
        worst_month=("-" if worst_month is None else f"{worst_month.period_label} {fmt(worst_month.total_pnl, 2)}"),
        exit_summary=" / ".join(f"{label}:{count}" for label, count in summarize_trade_exit_reasons(result.trades)),
    )


def write_csv(rows: list[Row]) -> None:
    fieldnames = [field.name for field in Row.__dataclass_fields__.values()]
    for path in (CSV_PATH, LATEST_CSV_PATH):
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "window_key": row.window_key,
                        "window_label": row.window_label,
                        "variant_key": row.variant_key,
                        "variant_label": row.variant_label,
                        "trades": row.trades,
                        "wins": row.wins,
                        "losses": row.losses,
                        "win_rate_pct": fmt(row.win_rate_pct, 2),
                        "total_pnl": fmt(row.total_pnl, 2),
                        "return_pct": fmt(row.return_pct, 2),
                        "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                        "profit_factor": fmt_or_dash(row.profit_factor, 4),
                        "avg_r": fmt(row.avg_r, 4),
                        "stop_loss_hits": row.stop_loss_hits,
                        "early_stop_hits": row.early_stop_hits,
                        "total_fees": fmt(row.total_fees, 2),
                        "positive_months": row.positive_months,
                        "negative_months": row.negative_months,
                        "worst_month": row.worst_month,
                        "exit_summary": row.exit_summary,
                    }
                )


def build_html(rows: list[Row]) -> str:
    findings = build_findings(rows)
    finding_items = "".join(f"<li>{html.escape(item)}</li>" for item in findings)
    sections = []
    for window_key, window_label, _ in WINDOWS:
        subset = [row for row in rows if row.window_key == window_key]
        sections.append(f"<section class='panel'><h2>{html.escape(window_label)}</h2>{build_table(subset)}</section>")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>BTC S097 Far Entry Early Protection</title>
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
    th:first-child, td:first-child, th:last-child, td:last-child {{ text-align: left; }}
    th {{ color: #667085; font-weight: 700; background: #f2f6fb; }}
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>BTC S097 Far Entry Early Protection</h1>
      <p class="muted">Generated at {html.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}. Research-only experiment for far-entry shorts with distance greater than {fmt(FAR_ENTRY_DISTANCE_ATR, 1)} ATR.</p>
    </section>
    <section class="panel">
      <h2>Findings</h2>
      <ul>{finding_items}</ul>
    </section>
    {''.join(sections)}
  </div>
</body>
</html>
"""


def build_findings(rows: list[Row]) -> list[str]:
    findings: list[str] = []
    for window_key, window_label, _ in WINDOWS:
        subset = [row for row in rows if row.window_key == window_key]
        baseline = next(row for row in subset if row.variant_key == "baseline")
        best_return = max(subset, key=lambda row: (row.return_pct, row.profit_factor or Decimal("-1")))
        lowest_dd = min(subset, key=lambda row: row.max_drawdown_pct)
        findings.append(
            f"{window_label}: best return is {best_return.variant_label} ({fmt(best_return.return_pct, 2)}%, DD {fmt(best_return.max_drawdown_pct, 2)}%, PF {fmt_or_dash(best_return.profit_factor, 4)})."
        )
        findings.append(
            f"{window_label}: lowest drawdown is {lowest_dd.variant_label} (DD {fmt(lowest_dd.max_drawdown_pct, 2)}%, return {fmt(lowest_dd.return_pct, 2)}%)."
        )
        for row in subset:
            if row.variant_key == "baseline":
                continue
            findings.append(
                f"{window_label} {row.variant_label}: PnL {fmt(row.total_pnl - baseline.total_pnl, 2, signed=True)}U vs baseline, DD {fmt(row.max_drawdown_pct - baseline.max_drawdown_pct, 2, signed=True)} pct points, early stops {row.early_stop_hits}."
            )
    return findings


def build_table(rows: list[Row]) -> str:
    ordered_keys = [variant.key for variant in VARIANTS]
    ordered = sorted(rows, key=lambda row: ordered_keys.index(row.variant_key))
    body = []
    for row in ordered:
        body.append(
            "<tr>"
            f"<td>{html.escape(row.variant_label)}</td>"
            f"<td>{row.trades}</td>"
            f"<td>{row.wins}/{row.losses}</td>"
            f"<td>{fmt(row.win_rate_pct, 2)}%</td>"
            f"<td class='{metric_class(row.total_pnl)}'>{fmt(row.total_pnl, 2)}</td>"
            f"<td class='{metric_class(row.return_pct)}'>{fmt(row.return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{fmt_or_dash(row.profit_factor, 4)}</td>"
            f"<td>{fmt(row.avg_r, 4)}</td>"
            f"<td>{row.early_stop_hits}</td>"
            f"<td>{html.escape(row.worst_month)}</td>"
            f"<td>{html.escape(row.exit_summary)}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Variant</th><th>Trades</th><th>W/L</th><th>Win Rate</th><th>Total PnL</th><th>Return</th><th>Max DD</th><th>PF</th><th>Avg R</th><th>Early Stops</th><th>Worst Month</th><th>Exit Summary</th>"
        "</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def metric_class(value: Decimal) -> str:
    return "good" if value > 0 else "bad" if value < 0 else ""


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


if __name__ == "__main__":
    main()
