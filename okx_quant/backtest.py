from __future__ import annotations

import bisect
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal
from typing import Any

from okx_quant.candle_cache import load_candle_cache, load_candle_cache_range
from okx_quant.engine import build_protection_plan, determine_order_size
from okx_quant.daily_filters import (
    aggregate_candles_to_daily_boundary,
    build_daily_close_vs_ma_bias,
    build_daily_weak_day_flags,
)
from okx_quant.indicators import atr, ema, linear_regression_slope, moving_average
from okx_quant.models import (
    Candle,
    DynamicProtectionRule,
    Instrument,
    OrderPlan,
    SignalDecision,
    StrategyConfig,
    describe_dynamic_protection_rules,
    moving_average_display_label,
)
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.protection_validation import InvalidProtectionPlanError, validate_protection_prices
from okx_quant.timeframe import closed_candle_available_timestamps
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategies.ema_dynamic_multi_timeframe import filter_bias_allows_signal
from okx_quant.strategies.body_retest_short import (
    BODY_RETEST_ATR_PERCENTILE_LOOKBACK,
    body_retest_short_bias_allows_short,
    body_retest_short_minimum_candles,
    build_body_retest_short_protection_plan,
    rolling_body_retest_percentile,
)
from okx_quant.strategies.btc_ema15_ma50_pullback_long import (
    PullbackCandidate as LongPullbackCandidate,
    btc_ema15_ma50_pullback_long_bias_allows_long,
    btc_ema15_ma50_pullback_long_minimum_candles,
    scan_btc_ema15_ma50_pullback_long_candidates,
)
from okx_quant.strategies.btc_ema15_ma50_pullback_short import (
    PullbackCandidate as ShortPullbackCandidate,
    btc_ema15_ma50_pullback_short_bias_allows_short,
    btc_ema15_ma50_pullback_short_minimum_candles,
    scan_btc_ema15_ma50_pullback_short_candidates,
)
from okx_quant.strategies.adaptive_ema_rail import (
    ADAPTIVE_RAIL_STATE_CONFIRMED,
    ADAPTIVE_RAIL_STATE_BROKEN,
    adaptive_rail_candidate_periods,
    adaptive_rail_minimum_candles,
    evaluate_adaptive_rail_signal,
    is_adaptive_rail_hard_break_at,
)
from okx_quant.strategy_runtime_registry import (
    get_strategy_runtime_profile,
    strategy_is_cross_family,
    strategy_preferred_direction,
    strategy_uses_signal_extrema,
)
from okx_quant.strategy_ui_schema import build_strategy_widget_visibility
from okx_quant.strategy_catalog import (
    STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_ID,
    is_adaptive_ema_rail_strategy,
    is_btc_ema15_ma50_pullback_long_strategy,
    is_btc_ema15_ma50_pullback_short_strategy,
    is_btc_ema55_slope_short_strategy,
    resolve_dynamic_signal_mode,
)


MAX_BACKTEST_CANDLES = 10000
BACKTEST_RESERVED_CANDLES = 200
HOUR_MS = 60 * 60 * 1000
DAY_MS = 24 * HOUR_MS
ATR_BATCH_MULTIPLIERS: tuple[Decimal, ...] = (
    Decimal("1"),
    Decimal("1.5"),
    Decimal("2"),
)
ATR_BATCH_TAKE_RATIOS: tuple[Decimal, ...] = (
    Decimal("1"),
    Decimal("2"),
    Decimal("3"),
)
ATR_PERIOD_BATCH_OPTIONS: tuple[int, ...] = (10, 14)
BATCH_MAX_ENTRIES_OPTIONS: tuple[int, ...] = (0, 1, 2, 3)
MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT = Decimal("0.50")

EXIT_REASON_LABELS = {
    "take_profit": "止盈",
    "stop_loss": "止损",
    "signal_profit_exit": "信号失效盈利平仓",
    "break_even_stop": "保本",
    "slope_turn_positive": "斜率转正平仓",
    "trend_ema_close_exit": "跌破趋势EMA收盘平仓",
    "ema15_close_exit": "EMA15收破离场",
}


class BacktestInvalidConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class BacktestTrade:
    signal: str
    entry_index: int
    exit_index: int
    entry_ts: int
    exit_ts: int
    entry_price: Decimal
    exit_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    size: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    exit_reason: str
    atr_value: Decimal = Decimal("0")
    entry_sequence: int = 0
    wave_entry_sequence: int = 0
    entry_fee: Decimal = Decimal("0")
    exit_fee: Decimal = Decimal("0")
    total_fee: Decimal = Decimal("0")
    entry_fee_type: str = "none"
    exit_fee_type: str = "none"
    slippage_cost: Decimal = Decimal("0")
    funding_cost: Decimal = Decimal("0")
    adaptive_rail_period: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestReport:
    total_trades: int
    win_trades: int
    loss_trades: int
    breakeven_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    average_pnl: Decimal
    gross_profit: Decimal
    gross_loss: Decimal
    profit_factor: Decimal | None
    average_win: Decimal
    average_loss: Decimal
    profit_loss_ratio: Decimal | None
    average_r_multiple: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal = Decimal("0")
    take_profit_hits: int = 0
    stop_loss_hits: int = 0
    ending_equity: Decimal = Decimal("0")
    total_return_pct: Decimal = Decimal("0")
    maker_fees: Decimal = Decimal("0")
    taker_fees: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    slippage_costs: Decimal = Decimal("0")
    funding_costs: Decimal = Decimal("0")
    manual_handoffs: int = 0
    manual_open_positions: int = 0
    manual_open_size: Decimal = Decimal("0")
    manual_open_pnl: Decimal = Decimal("0")
    max_manual_positions: int = 0
    max_total_occupied_slots: int = 0


@dataclass(frozen=True)
class BacktestPeriodStat:
    period_label: str
    trades: int
    win_rate: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    start_equity: Decimal
    end_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal


@dataclass(frozen=True)
class AdaptiveRailPeriodFrequency:
    period: int
    bars: int
    share_pct: Decimal


@dataclass(frozen=True)
class AdaptiveRailBacktestStats:
    evaluation_bars: int
    confirmed_bars: int
    confirmed_coverage_pct: Decimal
    broken_state_bars: int
    broken_state_pct: Decimal
    dominant_rail_switches: int
    average_dominant_rail_hold_bars: Decimal
    max_dominant_rail_hold_bars: int
    rail_broken_exit_count: int
    rail_broken_exit_pct: Decimal
    dominant_period_frequencies: tuple[AdaptiveRailPeriodFrequency, ...] = ()


@dataclass(frozen=True)
class BacktestResult:
    candles: list[Candle]
    trades: list[BacktestTrade]
    report: BacktestReport
    instrument: Instrument
    ema_values: list[Decimal | None] = field(default_factory=list)
    trend_ema_values: list[Decimal | None] = field(default_factory=list)
    entry_reference_ema_values: list[Decimal | None] = field(default_factory=list)
    big_ema_values: list[Decimal | None] = field(default_factory=list)
    atr_values: list[Decimal | None] = field(default_factory=list)
    equity_curve: list[Decimal] = field(default_factory=list)
    net_value_curve: list[Decimal] = field(default_factory=list)
    drawdown_curve: list[Decimal] = field(default_factory=list)
    drawdown_pct_curve: list[Decimal] = field(default_factory=list)
    monthly_stats: list[BacktestPeriodStat] = field(default_factory=list)
    yearly_stats: list[BacktestPeriodStat] = field(default_factory=list)
    initial_capital: Decimal = Decimal("10000")
    ema_period: int = 21
    ema_type: str = "ema"
    trend_ema_period: int = 55
    trend_ema_type: str = "ema"
    entry_reference_ema_period: int = 21
    entry_reference_ema_type: str = "ema"
    big_ema_period: int = 233
    atr_period: int = 10
    atr_stop_multiplier: Decimal = Decimal("0")
    strategy_id: str = STRATEGY_DYNAMIC_ID
    bar: str = ""
    mtf_filter_bar: str = ""
    mtf_filter_fast_ema_period: int = 0
    mtf_filter_slow_ema_period: int = 0
    mtf_reversal_mode: str = "block_new_entries"
    daily_filter_enabled: bool = False
    daily_filter_boundary: str = "exchange"
    daily_filter_mode: str = "disabled"
    daily_filter_scope: str = "both"
    daily_filter_ma_type: str = "ema"
    daily_filter_period: int = 0
    direction_filter_bias: list[str] = field(default_factory=list)
    data_source_note: str = ""
    maker_fee_rate: Decimal = Decimal("0")
    taker_fee_rate: Decimal = Decimal("0")
    entry_slippage_rate: Decimal = Decimal("0")
    exit_slippage_rate: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")
    funding_rate: Decimal = Decimal("0")
    take_profit_mode: str = "fixed"
    dynamic_two_r_break_even: bool = False
    dynamic_break_even_trigger_r: int = 2
    dynamic_fee_offset_enabled: bool = True
    dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = ()
    ema55_slope_exit_enabled: bool = True
    ema55_slope_lock_profit_enabled: bool = False
    ema55_slope_lock_profit_trigger_r: int = 5
    dynamic_first_lock_r: int = 0
    dynamic_trailing_step_r: int = 1
    ema55_slope_negative_entry_bars: int = 1
    ema55_slope_same_bar_reentry_block: bool = False
    ema55_slope_dynamic_exit_requires_bear_reentry: bool = False
    ema55_slope_dynamic_exit_bear_reentry_break_prev_low: bool = False
    ema55_slope_dynamic_exit_requires_ema_reclaim: bool = False
    ema55_slope_locked_reentry_requires_ema21_near: bool = False
    ema55_slope_locked_reentry_min_r: int = 0
    ema55_slope_locked_reentry_max_r: int = 0
    ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry: bool = False
    ema55_slope_dynamic_exit_bull_bar_reentry_min_r: int = 0
    ema55_slope_dynamic_exit_bull_bar_reentry_max_r: int = 0
    time_stop_break_even_enabled: bool = False
    time_stop_break_even_bars: int = 0
    trend_ema_close_exit_after_trigger_r_enabled: bool = False
    trend_ema_close_exit_after_trigger_r: int = 5
    trend_ema_slope_filter_min_ratio: Decimal = Decimal("0")
    atr_percentile_filter_max: Decimal = Decimal("0")
    body_retest_breakdown_atr_multiplier: Decimal = Decimal("0")
    body_retest_retest_atr_multiplier: Decimal = Decimal("0")
    body_retest_stop_buffer_atr_multiplier: Decimal = Decimal("0")
    body_retest_body_atr_limit: Decimal = Decimal("0")
    body_retest_watch_bars: int = 0
    cross_window_bars: int = 0
    max_pullback_index: int = 1
    exit_mode: str = "fixed_rr"
    rr: Decimal = Decimal("0")
    hold_close_exit_bars: int = 0
    max_entries_per_trend: int = 1
    sizing_mode: str = "fixed_risk"
    compounding: bool = False
    backtest_profile_id: str = ""
    backtest_profile_name: str = ""
    backtest_profile_summary: str = ""
    open_position: "BacktestOpenPosition | None" = None
    manual_positions: list["BacktestManualPosition"] = field(default_factory=list)
    adaptive_rail_stats: AdaptiveRailBacktestStats | None = None
    rail_fast_gate_enabled: bool = False
    rail_fast_gate_period: int = 21
    rail_fast_min_gap_ema200_atr: Decimal = Decimal("0")
    rail_fast_min_spread_trend_atr: Decimal = Decimal("0")
    rail_fast_max_recent_range_atr: Decimal = Decimal("0")
    rail_fast_recent_range_bars: int = 8


@dataclass(frozen=True)
class BacktestOpenPosition:
    signal: str
    entry_index: int
    entry_ts: int
    current_ts: int
    entry_price: Decimal
    current_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    initial_stop_loss: Decimal
    initial_take_profit: Decimal
    size: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    entry_fee: Decimal = Decimal("0")
    funding_cost: Decimal = Decimal("0")
    adaptive_rail_period: int | None = None


@dataclass(frozen=True)
class BacktestManualPosition:
    signal: str
    entry_index: int
    handoff_index: int
    entry_ts: int
    handoff_ts: int
    current_ts: int
    entry_price: Decimal
    handoff_price: Decimal
    current_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    size: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    break_even_price: Decimal
    handoff_reason: str
    atr_value: Decimal = Decimal("0")
    entry_sequence: int = 0
    entry_fee: Decimal = Decimal("0")
    funding_cost: Decimal = Decimal("0")


@dataclass
class _OpenPosition:
    signal: str
    entry_index: int
    entry_ts: int
    entry_price: Decimal
    entry_price_raw: Decimal = Decimal("0")
    entry_path_price: Decimal = Decimal("0")
    stop_loss: Decimal = Decimal("0")
    take_profit: Decimal = Decimal("0")
    initial_stop_loss: Decimal = Decimal("0")
    initial_take_profit: Decimal = Decimal("0")
    atr_value: Decimal = Decimal("0")
    size: Decimal = Decimal("0")
    risk_per_unit: Decimal = Decimal("0")
    tick_size: Decimal = Decimal("0.1")
    entry_sequence: int = 0
    wave_entry_sequence: int = 0
    dynamic_take_profit_enabled: bool = False
    take_profit_enabled: bool = True
    next_dynamic_trigger_r: int = 2
    dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = ()
    dynamic_next_rule_index: int = 0
    dynamic_active_rule_index: int = -1
    dynamic_next_trailing_trigger_r: int = 0
    dynamic_active_lock_r: int | None = None
    dynamic_last_processed_trigger_r: int = 0
    dynamic_trailing_start_r: int = 2
    dynamic_break_even_trigger_r: int = 2
    dynamic_first_lock_r: int = 0
    dynamic_trailing_step_r: int = 1
    dynamic_separate_break_even_enabled: bool = True
    dynamic_exit_fee_rate: Decimal = Decimal("0")
    dynamic_two_r_break_even: bool = False
    dynamic_fee_offset_enabled: bool = True
    time_stop_break_even_enabled: bool = False
    time_stop_break_even_bars: int = 0
    entry_fee_rate: Decimal = Decimal("0")
    estimated_exit_fee_rate: Decimal = Decimal("0")
    entry_fee_type: str = "none"
    entry_slippage_cost: Decimal = Decimal("0")
    entry_slippage_rate: Decimal = Decimal("0")
    exit_slippage_rate: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")
    funding_rate: Decimal = Decimal("0")
    adaptive_rail_period: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _ManualPosition:
    position: _OpenPosition
    handoff_index: int
    handoff_ts: int
    handoff_price_raw: Decimal
    handoff_reason: str


def _is_locked_r_exit_reason(exit_reason: str) -> bool:
    return exit_reason.startswith("locked_") and exit_reason.endswith("r_stop")


def _locked_r_from_exit_reason(exit_reason: str) -> int | None:
    if not _is_locked_r_exit_reason(exit_reason):
        return None
    raw = exit_reason.removeprefix("locked_").removesuffix("r_stop")
    try:
        return int(raw)
    except ValueError:
        return None


def is_stop_exit_reason(exit_reason: str) -> bool:
    return exit_reason in {"stop_loss", "break_even_stop"} or _is_locked_r_exit_reason(exit_reason)


def is_dynamic_protect_exit_reason(exit_reason: str) -> bool:
    return exit_reason == "break_even_stop" or _is_locked_r_exit_reason(exit_reason)


def _locked_r_matches_reentry_window(exit_reason: str, *, min_r: int, max_r: int) -> bool:
    locked_r = _locked_r_from_exit_reason(exit_reason)
    if locked_r is None:
        return False
    if locked_r < max(min_r, 1):
        return False
    if max_r > 0 and locked_r > max_r:
        return False
    return True


def _dynamic_exit_matches_bull_bar_reentry_window(exit_reason: str, *, min_r: int, max_r: int) -> bool:
    if max(min_r, 0) <= 0 and max_r <= 0:
        return is_dynamic_protect_exit_reason(exit_reason)
    return _locked_r_matches_reentry_window(exit_reason, min_r=min_r, max_r=max_r)


def _ema55_slope_ratio_from_series(ema_values: list[Decimal | None], index: int) -> Decimal | None:
    if index <= 0 or index >= len(ema_values):
        return None
    current_ema = ema_values[index]
    previous_ema = ema_values[index - 1]
    if current_ema is None or previous_ema is None or current_ema == 0:
        return None
    return (current_ema - previous_ema) / current_ema


def _should_require_bearish_reentry_after_dynamic_exit(config: StrategyConfig, exit_reason: str) -> bool:
    return bool(config.ema55_slope_dynamic_exit_requires_bear_reentry and is_dynamic_protect_exit_reason(exit_reason))


def _ema55_slope_negative_entry_bars(config: StrategyConfig) -> int:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return max(int(config.ema55_slope_negative_entry_bars), 1)
    return 1


def _ema55_slope_exit_condition_enabled(config: StrategyConfig) -> bool:
    return bool(config.ema55_slope_exit_enabled)


def _ema55_slope_lock_profit_enabled(config: StrategyConfig) -> bool:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return bool(config.ema55_slope_lock_profit_enabled)
    return str(config.take_profit_mode or "") == "dynamic"


def _ema55_slope_dynamic_two_r_break_even_enabled(config: StrategyConfig) -> bool:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return bool(config.ema55_slope_lock_profit_enabled)
    return bool(config.dynamic_two_r_break_even)


def _dynamic_break_even_trigger_r(config: StrategyConfig) -> int:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return _ema55_slope_lock_profit_trigger_r(config)
    return config.resolved_dynamic_break_even_trigger_r()


def _dynamic_separate_break_even_enabled(config: StrategyConfig) -> bool:
    return not is_btc_ema55_slope_short_strategy(config.strategy_id)


def _ema55_slope_dynamic_fee_offset_enabled(config: StrategyConfig) -> bool:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return bool(config.ema55_slope_lock_profit_enabled)
    return bool(config.dynamic_fee_offset_enabled)


def _ema55_slope_lock_profit_trigger_r(config: StrategyConfig) -> int:
    return config.resolved_dynamic_trailing_start_r()


def _trend_ema_close_exit_trigger_r(config: StrategyConfig) -> int:
    return config.resolved_trend_ema_close_exit_after_trigger_r()


def _dynamic_trailing_step_r(config: StrategyConfig) -> int:
    return config.resolved_dynamic_trailing_step_r()


def _dynamic_first_lock_r(config: StrategyConfig) -> int:
    return config.resolved_dynamic_first_lock_r()


def _dynamic_protection_rules(config: StrategyConfig) -> tuple[DynamicProtectionRule, ...]:
    return config.resolved_dynamic_protection_rules()


def _first_dynamic_rule_trigger_r(config: StrategyConfig) -> int:
    rules = _dynamic_protection_rules(config)
    if rules:
        return rules[0].resolved_trigger_r()
    return _ema55_slope_lock_profit_trigger_r(config)


def _ema55_slope_entry_triggered(
    config: StrategyConfig,
    *,
    recent_slope_ratios: list[Decimal | None],
    previous_slope_ratio_before_window: Decimal | None = None,
    threshold: Decimal,
) -> bool:
    required_negative_bars = _ema55_slope_negative_entry_bars(config)
    if len(recent_slope_ratios) < required_negative_bars:
        return False
    if any(slope_ratio is None or slope_ratio > threshold for slope_ratio in recent_slope_ratios[-required_negative_bars:]):
        return False
    if is_btc_ema55_slope_short_strategy(config.strategy_id) and (
        previous_slope_ratio_before_window is None or previous_slope_ratio_before_window < 0
    ):
        return False
    return True


def format_trade_exit_reason(exit_reason: str) -> str:
    locked_r = _locked_r_from_exit_reason(exit_reason)
    if locked_r is not None:
        return f"{locked_r}R"
    return EXIT_REASON_LABELS.get(exit_reason, exit_reason)


def summarize_trade_exit_reasons(trades: list[BacktestTrade]) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    order: list[str] = []
    for trade in trades:
        label = format_trade_exit_reason(trade.exit_reason)
        if label not in counts:
            counts[label] = 0
            order.append(label)
        counts[label] += 1
    preferred = ["保本"]
    locked_labels = sorted(
        (
            label
            for label in order
            if label.endswith("R") and label[:-1].isdigit()
        ),
        key=lambda item: int(item[:-1]),
    )
    preferred.extend(locked_labels)
    preferred.extend(["止损", "止盈", "信号失效盈利平仓"])
    ranked: list[str] = []
    for label in preferred:
        if label in counts and label not in ranked:
            ranked.append(label)
    for label in order:
        if label not in ranked:
            ranked.append(label)
    return [(label, counts[label]) for label in ranked]


def run_backtest(
    client: OkxRestClient,
    config: StrategyConfig,
    *,
    candle_limit: int = 200,
    start_ts: int | None = None,
    end_ts: int | None = None,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    local_only: bool = False,
) -> BacktestResult:
    if candle_limit < 0:
        raise ValueError("回测 K 线数量不能小于 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"回测最多支持 {MAX_BACKTEST_CANDLES} 根 K 线")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("开始时间不能晚于结束时间")

    preload_count = _required_backtest_preload_candles(config)
    candles = _load_backtest_candles(
        client,
        config.inst_id,
        config.bar,
        candle_limit,
        start_ts=start_ts,
        end_ts=end_ts,
        preload_count=preload_count,
        local_only=local_only,
    )
    instrument = _load_backtest_instrument(client, config.inst_id, candles_loaded=bool(candles), local_only=local_only)
    mtf_filter_candles: list[Candle] | None = None
    if _backtest_uses_mtf_filter(config.strategy_id):
        filter_inst_id = config.resolved_mtf_filter_inst_id()
        filter_bar = config.resolved_mtf_filter_bar()
        filter_preload = _required_mtf_filter_preload_candles(config)
        filter_limit = min(MAX_BACKTEST_CANDLES, max(800, candle_limit if candle_limit > 0 else len(candles)))
        mtf_filter_candles = _load_backtest_candles(
            client,
            filter_inst_id,
            filter_bar,
            filter_limit,
            start_ts=start_ts,
            end_ts=end_ts,
            preload_count=filter_preload,
            local_only=local_only,
        )
    direction_filter_bias: list[str] | None = None
    if _backtest_uses_daily_filter(config) and candles:
        daily_filter_candles = _load_daily_filter_candles(
            client,
            config,
            entry_candles=candles,
            local_only=local_only,
        )
        direction_filter_bias = _build_daily_direction_filter_bias(
            candles,
            daily_filter_candles,
            config,
        )
    cross_higher_tf_bias: list[str] | None = None
    if (
        strategy_is_cross_family(config.strategy_id)
        and int(config.cross_higher_tf_ref_ema_period) > 0
        and (config.cross_higher_tf_inst_id or "").strip()
        and (config.cross_higher_tf_bar or "").strip()
    ):
        inst_h = (config.cross_higher_tf_inst_id or config.inst_id).strip()
        bar_h = (config.cross_higher_tf_bar or "").strip()
        hi_limit = min(MAX_BACKTEST_CANDLES, max(800, len(candles) // 4 + 400))
        hi_preload = max(0, int(config.cross_higher_tf_ref_ema_period) + 5)
        higher = _load_backtest_candles(
            client,
            inst_h,
            bar_h,
            hi_limit,
            start_ts=start_ts,
            end_ts=end_ts,
            preload_count=hi_preload,
            local_only=local_only,
        )
        cross_higher_tf_bias = _build_cross_higher_tf_bias(
            candles,
            higher,
            int(config.cross_higher_tf_ref_ema_period),
        )
    return _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=_build_backtest_data_source_note(client),
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        cross_higher_tf_bias=cross_higher_tf_bias,
        mtf_filter_candles=mtf_filter_candles,
        direction_filter_bias=direction_filter_bias,
    )


def build_atr_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for stop_multiplier in atr_multipliers:
        for take_ratio in take_ratios:
            configs.append(
                replace(
                    base_config,
                    atr_stop_multiplier=stop_multiplier,
                    atr_take_multiplier=stop_multiplier * take_ratio,
                )
            )
    return configs


def build_btc_slope_short_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_periods: tuple[int, ...] = ATR_PERIOD_BATCH_OPTIONS,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    seen: set[tuple[int, Decimal]] = set()
    take_multiplier = (
        base_config.atr_take_multiplier
        if base_config.atr_take_multiplier > 0
        else base_config.atr_stop_multiplier
    )
    for atr_period in atr_periods:
        for stop_multiplier in atr_multipliers:
            key = (int(atr_period), stop_multiplier)
            if key in seen:
                continue
            seen.add(key)
            configs.append(
                replace(
                    base_config,
                    atr_period=max(int(atr_period), 1),
                    atr_stop_multiplier=stop_multiplier,
                    atr_take_multiplier=take_multiplier,
                )
            )
    return configs


def build_dynamic_entry_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    max_entries_options: tuple[int, ...] = BATCH_MAX_ENTRIES_OPTIONS,
) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for stop_multiplier in atr_multipliers:
        for max_entries in max_entries_options:
            configs.append(
                replace(
                    base_config,
                    atr_stop_multiplier=stop_multiplier,
                    max_entries_per_trend=max_entries,
                )
            )
    return configs


def _backtest_strategy_family(strategy_id: str) -> str:
    if is_adaptive_ema_rail_strategy(strategy_id):
        return "adaptive_ema_rail"
    return get_strategy_runtime_profile(strategy_id).family


def _backtest_uses_dynamic_orders(strategy_id: str) -> bool:
    try:
        return get_strategy_runtime_profile(strategy_id).uses_dynamic_orders
    except KeyError:
        return False


def _backtest_uses_mtf_filter(strategy_id: str) -> bool:
    try:
        return get_strategy_runtime_profile(strategy_id).uses_mtf_filter
    except KeyError:
        return False


def _backtest_uses_daily_filter(config: StrategyConfig) -> bool:
    return bool(config.uses_daily_filter())


def _load_daily_filter_candles(
    client: OkxRestClient,
    config: StrategyConfig,
    *,
    entry_candles: list[Candle],
    local_only: bool = False,
) -> list[Candle]:
    if not entry_candles:
        return []
    preload_days = max(int(config.daily_filter_period), 1) + 5
    start_ts = max(entry_candles[0].ts - (preload_days * DAY_MS), 0)
    end_ts = entry_candles[-1].ts
    boundary = str(config.daily_filter_boundary or "exchange").strip().lower()
    filter_inst_id = config.resolved_daily_filter_inst_id()
    if boundary == "exchange":
        return _load_backtest_candles(
            client,
            filter_inst_id,
            config.resolved_daily_filter_bar(),
            0,
            start_ts=start_ts,
            end_ts=end_ts,
            local_only=local_only,
        )
    if filter_inst_id == config.inst_id and str(config.bar or "").strip().upper() == "1H":
        hourly_candles = [candle for candle in entry_candles if candle.confirmed]
    else:
        hourly_candles = _load_backtest_candles(
            client,
            filter_inst_id,
            "1H",
            0,
            start_ts=start_ts,
            end_ts=end_ts,
            local_only=local_only,
        )
    aggregated, _ = aggregate_candles_to_daily_boundary(hourly_candles, boundary=boundary)
    return aggregated


def _expand_direction_filter_bias_scope(base_bias: list[str], scope: str) -> list[str]:
    normalized_scope = str(scope or "both").strip().lower()
    if normalized_scope == "both":
        return list(base_bias)
    expanded: list[str] = []
    for bias in base_bias:
        if normalized_scope == "long_only":
            expanded.append("both" if bias == "long" else "short")
        elif normalized_scope == "short_only":
            expanded.append("both" if bias == "short" else "long")
        else:
            expanded.append(bias)
    return expanded


def _build_daily_direction_filter_bias(
    entry_candles: list[Candle],
    daily_candles: list[Candle],
    config: StrategyConfig,
) -> list[str]:
    if not entry_candles:
        return []
    mode = str(config.daily_filter_mode or "disabled").strip().lower()
    if not daily_candles or mode == "disabled":
        return ["neutral"] * len(entry_candles)
    if mode == "weak_day":
        weak_day_flags = build_daily_weak_day_flags(entry_candles, daily_candles)
        base_bias = ["short" if is_weak else "long" for is_weak in weak_day_flags]
    else:
        base_bias = build_daily_close_vs_ma_bias(
            entry_candles,
            daily_candles,
            ma_type=str(config.daily_filter_ma_type or "ema").strip().lower(),
            period=max(int(config.daily_filter_period), 1),
        )
    return _expand_direction_filter_bias_scope(base_bias, config.daily_filter_scope)


def build_parameter_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
    max_entries_options: tuple[int, ...] = BATCH_MAX_ENTRIES_OPTIONS,
) -> list[StrategyConfig]:
    family = _backtest_strategy_family(base_config.strategy_id)
    if is_btc_ema55_slope_short_strategy(base_config.strategy_id):
        return build_btc_slope_short_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
        )
    if family == "ema55_slope_short":
        if int(base_config.ema_period) == 55 and int(base_config.trend_ema_period) == 55:
            return [base_config]
        return build_btc_slope_short_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
        )
    if family == "ema15_ma50_pullback_long":
        configs: list[StrategyConfig] = []
        atr_periods = (10, 14)
        cross_windows = (8, 10, 15, 20)
        pullback_indices = (1, 2, 3)
        exit_modes = (
            "fixed_rr",
            "fixed_rr_or_ema15_close",
            "dynamic",
            "dynamic_or_ema15_close",
        )
        rr_values = (
            Decimal("1"),
            Decimal("1.5"),
            Decimal("2"),
            Decimal("3"),
        )
        daily_filter_variants = (
            {
                "daily_filter_enabled": False,
                "daily_filter_mode": "disabled",
                "daily_filter_scope": "both",
            },
            {
                "daily_filter_enabled": True,
                "daily_filter_mode": "close_vs_ma",
                "daily_filter_scope": "long_only",
            },
        )
        for atr_period_value in atr_periods:
            for stop_multiplier in (Decimal("0.8"), Decimal("1.0"), Decimal("1.2"), Decimal("1.5"), Decimal("2.0")):
                for cross_window in cross_windows:
                    for pullback_index in pullback_indices:
                        for filter_variant in daily_filter_variants:
                            for exit_mode in exit_modes:
                                rr_candidates = (
                                    rr_values if exit_mode.startswith("fixed_rr") else (base_config.resolved_fixed_rr(),)
                                )
                                take_profit_mode = "fixed" if exit_mode.startswith("fixed_rr") else "dynamic"
                                for rr_value in rr_candidates:
                                    configs.append(
                                        replace(
                                            base_config,
                                            atr_period=atr_period_value,
                                            atr_stop_multiplier=stop_multiplier,
                                            cross_window_bars=cross_window,
                                            max_pullback_index=pullback_index,
                                            exit_mode=exit_mode,
                                            rr=rr_value,
                                            take_profit_mode=take_profit_mode,
                                            daily_filter_enabled=bool(filter_variant["daily_filter_enabled"]),
                                            daily_filter_mode=str(filter_variant["daily_filter_mode"]),
                                            daily_filter_scope=str(filter_variant["daily_filter_scope"]),
                                        )
                                    )
        return configs
    if family == "ema15_ma50_pullback_short":
        configs: list[StrategyConfig] = []
        atr_periods = (10, 14)
        trend_variants = (
            ("ema", 50),
            ("ema", 55),
        )
        cross_windows = (8, 10, 15, 20)
        pullback_indices = (1, 2, 3)
        exit_modes = (
            "fixed_rr",
            "fixed_rr_or_ema15_close",
            "dynamic",
            "dynamic_or_ema15_close",
        )
        rr_values = (
            Decimal("1"),
            Decimal("1.5"),
            Decimal("2"),
            Decimal("3"),
        )
        daily_filter_variants = (
            {
                "daily_filter_enabled": False,
                "daily_filter_mode": "disabled",
                "daily_filter_scope": "both",
            },
            {
                "daily_filter_enabled": True,
                "daily_filter_mode": "close_vs_ma",
                "daily_filter_scope": "short_only",
            },
        )
        for atr_period_value in atr_periods:
            for trend_ema_type, trend_ema_period in trend_variants:
                for stop_multiplier in (Decimal("0.8"), Decimal("1.0"), Decimal("1.2"), Decimal("1.5"), Decimal("2.0")):
                    for cross_window in cross_windows:
                        for pullback_index in pullback_indices:
                            for filter_variant in daily_filter_variants:
                                for exit_mode in exit_modes:
                                    rr_candidates = rr_values if exit_mode.startswith("fixed_rr") else (base_config.resolved_fixed_rr(),)
                                    take_profit_mode = "fixed" if exit_mode.startswith("fixed_rr") else "dynamic"
                                    for rr_value in rr_candidates:
                                        configs.append(
                                            replace(
                                                base_config,
                                                atr_period=atr_period_value,
                                                trend_ema_type=trend_ema_type,
                                                trend_ema_period=trend_ema_period,
                                                atr_stop_multiplier=stop_multiplier,
                                                cross_window_bars=cross_window,
                                                max_pullback_index=pullback_index,
                                                exit_mode=exit_mode,
                                                rr=rr_value,
                                                take_profit_mode=take_profit_mode,
                                                daily_filter_enabled=bool(filter_variant["daily_filter_enabled"]),
                                                daily_filter_mode=str(filter_variant["daily_filter_mode"]),
                                                daily_filter_scope=str(filter_variant["daily_filter_scope"]),
                                            )
                                        )
        return configs
    if family == "body_retest_short":
        return [base_config]
    if family not in {"dynamic_order", "adaptive_ema_rail"}:
        return build_atr_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
            take_ratios=take_ratios,
        )
    if base_config.take_profit_mode == "dynamic":
        return build_dynamic_entry_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
            max_entries_options=max_entries_options,
        )

    configs: list[StrategyConfig] = []
    for max_entries in max_entries_options:
        layer_config = replace(base_config, max_entries_per_trend=max_entries)
        configs.extend(
            build_atr_batch_configs(
                layer_config,
                atr_multipliers=atr_multipliers,
                take_ratios=take_ratios,
            )
        )
    return configs


def _backtest_min_order_size(instrument: Instrument) -> Decimal:
    minimum = snap_to_increment(instrument.min_size, instrument.lot_size, "up")
    if minimum < instrument.min_size:
        return instrument.min_size
    return minimum


def _determine_backtest_order_size(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    entry_price: Decimal,
    stop_loss: Decimal,
    risk_price_compatible: bool,
) -> Decimal:
    if config.risk_amount is not None and config.risk_amount > 0 and risk_price_compatible:
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit <= 0:
            raise RuntimeError("开仓价与止损价过于接近，无法根据风险金计算数量")
        size_raw = config.risk_amount / risk_per_unit
        size = snap_to_increment(size_raw, instrument.lot_size, "down")
        if size < instrument.min_size:
            return _backtest_min_order_size(instrument)
        return size

    return determine_order_size(
        instrument=instrument,
        config=config,
        entry_price=entry_price,
        stop_loss=stop_loss,
        risk_price_compatible=risk_price_compatible,
    )


def _raise_if_only_invalid_protection_configs(
    *,
    config: StrategyConfig,
    invalid_protection_count: int,
    valid_entry_plan_count: int,
) -> None:
    if invalid_protection_count <= 0 or valid_entry_plan_count > 0:
        return
    raise BacktestInvalidConfigError(
        "参数组合无效：当前样本中所有候选信号的止盈/止损价格都落入非法区间，"
        f"已排除该组合。inst_id={config.inst_id} strategy_id={config.strategy_id}"
    )


def _build_backtest_order_plan(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    order_size: Decimal | None,
    signal: str,
    entry_reference: Decimal,
    atr_value: Decimal,
    candle_ts: int,
    signal_candle_high: Decimal | None = None,
    signal_candle_low: Decimal | None = None,
) -> OrderPlan:
    protection = build_protection_plan(
        instrument=instrument,
        config=config,
        direction=signal,
        entry_reference=entry_reference,
        atr_value=atr_value,
        candle_ts=candle_ts,
        trigger_inst_id=instrument.inst_id,
        use_signal_extrema=strategy_uses_signal_extrema(config.strategy_id),
        signal_candle_high=signal_candle_high,
        signal_candle_low=signal_candle_low,
    )

    side = "buy" if signal == "long" else "sell"
    pos_side = None
    if config.position_mode == "long_short":
        pos_side = "long" if side == "buy" else "short"

    if config.risk_amount is not None and config.risk_amount > 0:
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=True,
        )
    else:
        if order_size is None:
            raise RuntimeError("缂哄皯涓嬪崟鏁伴噺锛屼笖鏈缃闄╅噾")
        manual_config = replace(config, order_size=order_size, risk_amount=None)
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=manual_config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=False,
        )

    return OrderPlan(
        inst_id=instrument.inst_id,
        side=side,
        pos_side=pos_side,
        size=size,
        take_profit=protection.take_profit,
        stop_loss=protection.stop_loss,
        entry_reference=protection.entry_reference,
        atr_value=protection.atr_value,
        signal=signal,
        candle_ts=candle_ts,
        tp_sl_inst_id=instrument.inst_id,
        tp_sl_mode="exchange",
    )


def run_backtest_batch(
    client: OkxRestClient,
    base_config: StrategyConfig,
    *,
    candle_limit: int = 200,
    start_ts: int | None = None,
    end_ts: int | None = None,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    local_only: bool = False,
) -> list[tuple[StrategyConfig, BacktestResult]]:
    if _backtest_strategy_family(base_config.strategy_id) == "ema5_ema8":
        raise RuntimeError("4H EMA5/EMA8 金叉死叉策略不参与 ATR 批量矩阵回测，请使用单组回测。")
    if candle_limit < 0:
        raise ValueError("\u56de\u6d4b K \u7ebf\u6570\u91cf\u4e0d\u80fd\u5c0f\u4e8e 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"\u56de\u6d4b\u6700\u591a\u652f\u6301 {MAX_BACKTEST_CANDLES} \u6839 K \u7ebf")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("开始时间不能晚于结束时间")

    batch_configs = build_parameter_batch_configs(
        base_config,
        atr_multipliers=atr_multipliers,
        take_ratios=take_ratios,
    )
    if not batch_configs:
        return []

    preload_count = max(_required_backtest_preload_candles(config) for config in batch_configs)
    sample_config = batch_configs[0]
    candles = _load_backtest_candles(
        client,
        sample_config.inst_id,
        sample_config.bar,
        candle_limit,
        start_ts=start_ts,
        end_ts=end_ts,
        preload_count=preload_count,
        local_only=local_only,
    )
    instrument = _load_backtest_instrument(
        client,
        sample_config.inst_id,
        candles_loaded=bool(candles),
        local_only=local_only,
    )
    mtf_filter_candles: list[Candle] | None = None
    if _backtest_uses_mtf_filter(sample_config.strategy_id):
        filter_limit = min(MAX_BACKTEST_CANDLES, max(800, candle_limit if candle_limit > 0 else len(candles)))
        mtf_filter_candles = _load_backtest_candles(
            client,
            sample_config.resolved_mtf_filter_inst_id(),
            sample_config.resolved_mtf_filter_bar(),
            filter_limit,
            start_ts=start_ts,
            end_ts=end_ts,
            preload_count=max(_required_mtf_filter_preload_candles(config) for config in batch_configs),
            local_only=local_only,
        )
    daily_filter_bias_by_key: dict[tuple[object, ...], list[str]] = {}
    if candles:
        for config in batch_configs:
            if not _backtest_uses_daily_filter(config):
                continue
            cache_key = (
                config.resolved_daily_filter_inst_id(),
                str(config.daily_filter_boundary or "exchange").strip().lower(),
                config.resolved_daily_filter_bar(),
                str(config.daily_filter_mode or "disabled").strip().lower(),
                str(config.daily_filter_scope or "both").strip().lower(),
                str(config.daily_filter_ma_type or "ema").strip().lower(),
                max(int(config.daily_filter_period), 1),
            )
            if cache_key in daily_filter_bias_by_key:
                continue
            daily_filter_candles = _load_daily_filter_candles(
                client,
                config,
                entry_candles=candles,
                local_only=local_only,
            )
            daily_filter_bias_by_key[cache_key] = _build_daily_direction_filter_bias(
                candles,
                daily_filter_candles,
                config,
            )
    data_source_note = _build_backtest_data_source_note(client)
    results: list[tuple[StrategyConfig, BacktestResult]] = []
    for config in batch_configs:
        direction_filter_bias = None
        if _backtest_uses_daily_filter(config):
            cache_key = (
                config.resolved_daily_filter_inst_id(),
                str(config.daily_filter_boundary or "exchange").strip().lower(),
                config.resolved_daily_filter_bar(),
                str(config.daily_filter_mode or "disabled").strip().lower(),
                str(config.daily_filter_scope or "both").strip().lower(),
                str(config.daily_filter_ma_type or "ema").strip().lower(),
                max(int(config.daily_filter_period), 1),
            )
            direction_filter_bias = daily_filter_bias_by_key.get(cache_key)
        try:
            result = _run_backtest_with_loaded_data(
                candles,
                instrument,
                config,
                data_source_note=data_source_note,
                maker_fee_rate=maker_fee_rate,
                taker_fee_rate=taker_fee_rate,
                mtf_filter_candles=mtf_filter_candles,
                direction_filter_bias=direction_filter_bias,
            )
        except BacktestInvalidConfigError:
            continue
        results.append((config, result))
    return results


def _run_backtest_with_loaded_data(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    data_source_note: str = "",
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    cross_higher_tf_bias: list[str] | None = None,
    mtf_filter_candles: list[Candle] | None = None,
    direction_filter_bias: list[str] | None = None,
) -> BacktestResult:
    terminal_open_position: BacktestOpenPosition | None = None
    adaptive_rail_stats: AdaptiveRailBacktestStats | None = None
    manual_positions: list[BacktestManualPosition] = []
    manual_handoffs = 0
    max_manual_positions = 0
    max_total_occupied_slots = 0
    family = _backtest_strategy_family(config.strategy_id)
    if _backtest_uses_mtf_filter(config.strategy_id):
        if mtf_filter_candles is None:
            raise RuntimeError("多周期动态策略缺少高周期 K 线数据")
        mtf_filter_bias = _build_mtf_filter_bias(
            candles,
            mtf_filter_candles,
            int(config.mtf_filter_fast_ema_period),
            int(config.mtf_filter_slow_ema_period),
        )
        if direction_filter_bias is not None:
            mtf_filter_bias = _combine_direction_filter_bias(mtf_filter_bias, direction_filter_bias)
        trades, terminal_open_position = _run_dynamic_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
            mtf_filter_bias=mtf_filter_bias,
        )
    elif family == "dynamic_order":
        trades, terminal_open_position = _run_dynamic_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
            mtf_filter_bias=direction_filter_bias,
        )
    elif family == "adaptive_ema_rail":
        trades, terminal_open_position, adaptive_rail_stats = _run_adaptive_rail_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
        )
    elif family == "ema55_slope_short":
        trades, terminal_open_position = _run_ema55_slope_short_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
            direction_filter_bias=direction_filter_bias,
        )
    elif family == "body_retest_short":
        trades, terminal_open_position = _run_body_retest_short_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
            direction_filter_bias=direction_filter_bias,
        )
    elif family == "ema15_ma50_pullback_long":
        trades, terminal_open_position = _run_btc_ema15_ma50_pullback_long_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
            direction_filter_bias=direction_filter_bias,
        )
    elif family == "ema15_ma50_pullback_short":
        trades, terminal_open_position = _run_btc_ema15_ma50_pullback_short_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
            direction_filter_bias=direction_filter_bias,
        )
    elif strategy_is_cross_family(config.strategy_id):
        trades, terminal_open_position = _run_cross_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
            higher_tf_bias=cross_higher_tf_bias,
        )
    elif family == "ema5_ema8":
        trades, terminal_open_position = _run_ema5_ema8_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
        )
    else:
        raise RuntimeError(f"鏆備笉鏀寔鐨勫洖娴嬬瓥鐣ワ細{config.strategy_id}")
    closes = [candle.close for candle in candles] if candles else []
    ema_values = moving_average(closes, config.ema_period, config.resolved_ema_type()) if candles else []
    trend_ema_values = moving_average(closes, config.trend_ema_period, config.resolved_trend_ema_type()) if candles else []
    entry_reference_values = (
        ema_values
        if (
            candles
            and config.resolved_entry_reference_ema_period() == config.ema_period
            and config.resolved_entry_reference_ema_type() == config.resolved_ema_type()
        )
        else (
            moving_average(
                closes,
                config.resolved_entry_reference_ema_period(),
                config.resolved_entry_reference_ema_type(),
            )
            if candles
            else []
        )
    )
    atr_values = atr(candles, config.atr_period) if candles else []
    if not build_strategy_widget_visibility(config.strategy_id, "backtest").show_big_ema:
        big_ema_values: list[Decimal | None] = []
    else:
        big_ema_values = list(ema(closes, config.big_ema_period)) if candles else []
    initial_capital = config.backtest_initial_capital
    equity_curve = _build_equity_curve(candles, trades)
    net_value_curve = [initial_capital + value for value in equity_curve]
    drawdown_curve, drawdown_pct_curve = _build_drawdown_curves(net_value_curve)
    report = _build_report(
        trades,
        initial_capital=initial_capital,
        manual_handoffs=manual_handoffs,
        manual_positions=manual_positions,
        max_manual_positions=max_manual_positions,
        max_total_occupied_slots=max_total_occupied_slots,
    )

    return BacktestResult(
        candles=candles,
        trades=trades,
        report=report,
        instrument=instrument,
        ema_values=ema_values,
        trend_ema_values=trend_ema_values,
        entry_reference_ema_values=entry_reference_values,
        big_ema_values=big_ema_values,
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
        atr_stop_multiplier=Decimal(str(config.atr_stop_multiplier)),
        strategy_id=config.strategy_id,
        bar=config.bar,
        mtf_filter_bar=config.resolved_mtf_filter_bar() if _backtest_uses_mtf_filter(config.strategy_id) else "",
        mtf_filter_fast_ema_period=(
            int(config.mtf_filter_fast_ema_period) if _backtest_uses_mtf_filter(config.strategy_id) else 0
        ),
        mtf_filter_slow_ema_period=(
            int(config.mtf_filter_slow_ema_period) if _backtest_uses_mtf_filter(config.strategy_id) else 0
        ),
        mtf_reversal_mode=str(config.mtf_reversal_mode),
        daily_filter_enabled=bool(config.uses_daily_filter()),
        daily_filter_boundary=str(config.daily_filter_boundary),
        daily_filter_mode=str(config.daily_filter_mode),
        daily_filter_scope=str(config.daily_filter_scope),
        daily_filter_ma_type=str(config.daily_filter_ma_type),
        daily_filter_period=int(config.daily_filter_period),
        direction_filter_bias=list(direction_filter_bias or []),
        data_source_note=data_source_note,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
        exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
        slippage_rate=config.resolved_backtest_exit_slippage_rate(),
        funding_rate=config.backtest_funding_rate,
        take_profit_mode=(
            "dynamic"
            if (
                (
                    is_btc_ema15_ma50_pullback_long_strategy(config.strategy_id)
                    or is_btc_ema15_ma50_pullback_short_strategy(config.strategy_id)
                )
                and _btc_ema15_ma50_uses_dynamic_exit(config)
            )
            else (
                "fixed"
                if (
                    is_btc_ema15_ma50_pullback_long_strategy(config.strategy_id)
                    or is_btc_ema15_ma50_pullback_short_strategy(config.strategy_id)
                )
                else str(config.take_profit_mode)
            )
        ),
        dynamic_two_r_break_even=_ema55_slope_dynamic_two_r_break_even_enabled(config),
        dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
        dynamic_fee_offset_enabled=_ema55_slope_dynamic_fee_offset_enabled(config),
        dynamic_protection_rules=_dynamic_protection_rules(config),
        ema55_slope_exit_enabled=bool(config.ema55_slope_exit_enabled),
        ema55_slope_lock_profit_enabled=bool(config.ema55_slope_lock_profit_enabled),
        ema55_slope_lock_profit_trigger_r=_ema55_slope_lock_profit_trigger_r(config),
        dynamic_first_lock_r=_dynamic_first_lock_r(config),
        dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
        ema55_slope_negative_entry_bars=max(int(config.ema55_slope_negative_entry_bars), 1),
        ema55_slope_same_bar_reentry_block=bool(config.ema55_slope_same_bar_reentry_block),
        ema55_slope_dynamic_exit_requires_bear_reentry=bool(config.ema55_slope_dynamic_exit_requires_bear_reentry),
        ema55_slope_dynamic_exit_bear_reentry_break_prev_low=bool(
            config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low
        ),
        ema55_slope_dynamic_exit_requires_ema_reclaim=bool(config.ema55_slope_dynamic_exit_requires_ema_reclaim),
        ema55_slope_locked_reentry_requires_ema21_near=bool(config.ema55_slope_locked_reentry_requires_ema21_near),
        ema55_slope_locked_reentry_min_r=int(config.ema55_slope_locked_reentry_min_r),
        ema55_slope_locked_reentry_max_r=int(config.ema55_slope_locked_reentry_max_r),
        ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry=bool(
            config.ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry
        ),
        ema55_slope_dynamic_exit_bull_bar_reentry_min_r=int(config.ema55_slope_dynamic_exit_bull_bar_reentry_min_r),
        ema55_slope_dynamic_exit_bull_bar_reentry_max_r=int(config.ema55_slope_dynamic_exit_bull_bar_reentry_max_r),
        time_stop_break_even_enabled=bool(config.time_stop_break_even_enabled),
        time_stop_break_even_bars=int(config.resolved_time_stop_break_even_bars()),
        trend_ema_close_exit_after_trigger_r_enabled=bool(config.trend_ema_close_exit_after_trigger_r_enabled),
        trend_ema_close_exit_after_trigger_r=int(config.resolved_trend_ema_close_exit_after_trigger_r()),
        trend_ema_slope_filter_min_ratio=Decimal(str(config.trend_ema_slope_filter_min_ratio)),
        atr_percentile_filter_max=Decimal(str(config.atr_percentile_filter_max)),
        body_retest_breakdown_atr_multiplier=Decimal(str(config.body_retest_breakdown_atr_multiplier)),
        body_retest_retest_atr_multiplier=Decimal(str(config.body_retest_retest_atr_multiplier)),
        body_retest_stop_buffer_atr_multiplier=Decimal(str(config.body_retest_stop_buffer_atr_multiplier)),
        body_retest_body_atr_limit=Decimal(str(config.body_retest_body_atr_limit)),
        body_retest_watch_bars=int(config.body_retest_watch_bars),
        cross_window_bars=int(config.cross_window_bars),
        max_pullback_index=int(config.max_pullback_index),
        exit_mode=str(config.exit_mode),
        rr=Decimal(str(config.rr)),
        hold_close_exit_bars=int(config.hold_close_exit_bars),
        max_entries_per_trend=int(config.max_entries_per_trend),
        sizing_mode=config.backtest_sizing_mode,
        compounding=config.backtest_compounding,
        backtest_profile_id=config.backtest_profile_id,
        backtest_profile_name=config.backtest_profile_name,
        backtest_profile_summary=config.backtest_profile_summary,
        open_position=terminal_open_position,
        manual_positions=manual_positions,
        adaptive_rail_stats=adaptive_rail_stats,
        rail_fast_gate_enabled=bool(config.rail_fast_gate_enabled),
        rail_fast_gate_period=int(config.rail_fast_gate_period),
        rail_fast_min_gap_ema200_atr=Decimal(str(config.rail_fast_min_gap_ema200_atr)),
        rail_fast_min_spread_trend_atr=Decimal(str(config.rail_fast_min_spread_trend_atr)),
        rail_fast_max_recent_range_atr=Decimal(str(config.rail_fast_max_recent_range_atr)),
        rail_fast_recent_range_bars=int(config.rail_fast_recent_range_bars),
    )


def _manual_position_break_even_gap_pct(manual_position: BacktestManualPosition) -> Decimal:
    gap_value = abs(manual_position.current_price - manual_position.break_even_price)
    base_price = abs(manual_position.break_even_price)
    if base_price <= 0:
        base_price = abs(manual_position.entry_price)
    if base_price <= 0:
        return Decimal("0")
    return (gap_value / base_price) * Decimal("100")


def _manual_direction_pressure_text(manual_positions: list[BacktestManualPosition]) -> str:
    parts: list[str] = []
    for signal, label in (("long", "做多"), ("short", "做空")):
        positions = [item for item in manual_positions if item.signal == signal]
        if not positions:
            continue
        total_size = sum((item.size for item in positions), Decimal("0"))
        total_pnl = sum((item.pnl for item in positions), Decimal("0"))
        nearest_gap = min((_manual_position_break_even_gap_pct(item) for item in positions), default=Decimal("0"))
        parts.append(
            f"{label} {len(positions)} 笔 / {format_decimal_fixed(total_size, 4)} / "
            f"浮盈亏 {format_decimal_fixed(total_pnl, 4)} / 最近保本 {format_decimal_fixed(nearest_gap, 2)}%"
        )
    return " | ".join(parts) if parts else "当前无待人工处理仓位。"


def _manual_pool_pressure_lines(result: BacktestResult) -> list[str]:
    report = result.report
    slot_limit = result.max_entries_per_trend
    slot_pressure_pct = (
        Decimal("0")
        if slot_limit <= 0
        else (Decimal(report.max_total_occupied_slots) / Decimal(slot_limit)) * Decimal("100")
    )
    slot_pressure_text = (
        f"{report.max_total_occupied_slots}/{slot_limit}"
        if slot_limit > 0
        else str(report.max_total_occupied_slots)
    )
    lines = [
        (
            f"人工接管压力：峰值占槽 {slot_pressure_text} ({format_decimal_fixed(slot_pressure_pct, 2)}%) | "
            f"峰值托管仓位 {report.max_manual_positions} | 累计转托管 {report.manual_handoffs}"
        )
    ]
    if not result.manual_positions:
        lines.append("托管仓位方向拆分：当前无待人工处理仓位。")
        return lines

    near_count = sum(
        1
        for position in result.manual_positions
        if _manual_position_break_even_gap_pct(position) <= MANUAL_NEAR_BREAK_EVEN_THRESHOLD_PCT
    )
    win_count = sum(1 for position in result.manual_positions if position.pnl > 0)
    loss_count = sum(1 for position in result.manual_positions if position.pnl < 0)
    entry_fees = sum((position.entry_fee for position in result.manual_positions), Decimal("0"))
    funding_costs = sum((position.funding_cost for position in result.manual_positions), Decimal("0"))
    risk_total = sum((position.risk_value for position in result.manual_positions), Decimal("0"))
    nearest_gap = min((_manual_position_break_even_gap_pct(position) for position in result.manual_positions), default=Decimal("0"))

    lines.extend(
        [
            f"托管仓位方向拆分：{_manual_direction_pressure_text(result.manual_positions)}",
            (
                f"托管仓位状态：盈利 {win_count} | 亏损 {loss_count} | "
                f"接近保本 {near_count} | 最接近保本 {format_decimal_fixed(nearest_gap, 2)}%"
            ),
            (
                f"托管仓位成本：开仓手续费 {format_decimal_fixed(entry_fees, 4)} | "
                f"资金费 {format_decimal_fixed(funding_costs, 4)} | 风险值合计 {format_decimal_fixed(risk_total, 4)}"
            ),
        ]
    )
    return lines


def format_backtest_report(result: BacktestResult) -> str:
    report = result.report
    start_time = _format_backtest_timestamp(result.candles[0].ts) if result.candles else "-"
    end_time = _format_backtest_timestamp(result.candles[-1].ts) if result.candles else "-"
    pnl_before_fees = report.total_pnl + report.total_fees
    average_fee = report.total_fees / Decimal(report.total_trades) if report.total_trades > 0 else Decimal("0")
    fee_to_prefee_pct = None if pnl_before_fees == 0 else (report.total_fees / abs(pnl_before_fees)) * Decimal("100")
    fee_to_net_pct = None if report.total_pnl == 0 else (report.total_fees / abs(report.total_pnl)) * Decimal("100")
    fee_to_capital_pct = (
        Decimal("0") if result.initial_capital <= 0 else (report.total_fees / result.initial_capital) * Decimal("100")
    )
    exit_reason_summary = summarize_trade_exit_reasons(result.trades)
    lines = [
        f"回测K线数：{len(result.candles)}",
        f"开始时间：{start_time}",
        f"结束时间：{end_time}",
        f"预热K线：前 {min(BACKTEST_RESERVED_CANDLES, len(result.candles))} 根仅用于指标预热与绘图，不参与回测",
        f"初始资金：{format_decimal_fixed(result.initial_capital, 2)}",
        f"结束权益：{format_decimal_fixed(report.ending_equity, 2)}",
        f"总收益率：{format_decimal_fixed(report.total_return_pct, 2)}%",
        f"仓位模式：{_format_backtest_sizing_mode(result.sizing_mode)}",
        f"复利模式：{'开启' if result.compounding else '关闭'}",
        f"Maker手续费：{_format_fee_rate_percent(result.maker_fee_rate)}",
        f"Taker手续费：{_format_fee_rate_percent(result.taker_fee_rate)}",
        f"开仓滑点：{_format_fee_rate_percent(result.entry_slippage_rate)}",
        f"平仓滑点：{_format_fee_rate_percent(result.exit_slippage_rate)}",
        f"资金费率/8h：{_format_fee_rate_percent(result.funding_rate)}",
        f"交易次数：{report.total_trades}",
        f"胜率：{format_decimal_fixed(report.win_rate, 2)}%",
        f"总盈亏：{format_decimal(report.total_pnl)}",
        f"平均每笔：{format_decimal_fixed(report.average_pnl, 4)}",
        f"平均R倍数：{format_decimal_fixed(report.average_r_multiple, 4)}",
        f"最大回撤：{format_decimal_fixed(report.max_drawdown, 4)}",
        f"最大回撤比例：{format_decimal_fixed(report.max_drawdown_pct, 2)}%",
        f"手续费合计：{format_decimal_fixed(report.total_fees, 4)}",
        f"Maker手续费合计：{format_decimal_fixed(report.maker_fees, 4)}",
        f"Taker手续费合计：{format_decimal_fixed(report.taker_fees, 4)}",
        f"手续费前盈亏：{format_decimal_fixed(pnl_before_fees, 4)}",
        f"平均单笔手续费：{format_decimal_fixed(average_fee, 4)}",
        (
            f"手续费占手续费前盈亏：{format_decimal_fixed(fee_to_prefee_pct, 2)}%"
            if fee_to_prefee_pct is not None
            else "手续费占手续费前盈亏：无"
        ),
        (
            f"手续费占净盈亏绝对值：{format_decimal_fixed(fee_to_net_pct, 2)}%"
            if fee_to_net_pct is not None
            else "手续费占净盈亏绝对值：无"
        ),
        f"手续费占初始资金：{format_decimal_fixed(fee_to_capital_pct, 2)}%",
        f"滑点成本合计：{format_decimal_fixed(report.slippage_costs, 4)}",
        f"资金费合计：{format_decimal_fixed(report.funding_costs, 4)}",
        f"止盈触发次数：{report.take_profit_hits}",
        f"止损触发次数：{report.stop_loss_hits}",
    ]
    if exit_reason_summary:
        lines.append(
            "平仓原因统计：" + " | ".join(f"{label} {count}" for label, count in exit_reason_summary)
        )
    if result.daily_filter_enabled:
        boundary_labels = {
            "exchange": "交易所1D",
            "bjt_00": "北京时间0点",
            "bjt_08": "北京时间8点",
        }
        scope_labels = {
            "both": "多空都过滤",
            "long_only": "只过滤多头",
            "short_only": "只过滤空头",
        }
        boundary_label = boundary_labels.get(result.daily_filter_boundary, result.daily_filter_boundary)
        scope_label = scope_labels.get(result.daily_filter_scope, result.daily_filter_scope)
        if result.daily_filter_mode == "weak_day":
            lines.append(f"日线过滤：{boundary_label} 弱日规则 | {scope_label}")
        else:
            lines.append(
                f"日线过滤：{boundary_label} {str(result.daily_filter_ma_type).upper()}"
                f"{result.daily_filter_period} close-vs-MA | {scope_label}"
            )
    fast_label = moving_average_display_label(result.ema_type, result.ema_period)
    trend_label = moving_average_display_label(result.trend_ema_type, result.trend_ema_period)
    reference_label = moving_average_display_label(
        result.entry_reference_ema_type,
        result.entry_reference_ema_period,
    )
    _append_backtest_strategy_notes(
        lines,
        result,
        fast_label=fast_label,
        trend_label=trend_label,
        reference_label=reference_label,
    )
    if report.profit_factor is None:
        lines.append("Profit Factor：无亏损交易")
    else:
        lines.append(f"Profit Factor：{format_decimal_fixed(report.profit_factor, 4)}")
    if report.profit_loss_ratio is None:
        lines.append("盈亏比：无亏损交易")
    else:
        lines.append(f"盈亏比：{format_decimal_fixed(report.profit_loss_ratio, 4)}")
    lines.extend(
        [
            f"盈利笔数：{report.win_trades}",
            f"亏损笔数：{report.loss_trades}",
            f"持平笔数：{report.breakeven_trades}",
            f"平均盈利：{format_decimal_fixed(report.average_win, 4)}",
            f"平均亏损：{format_decimal_fixed(report.average_loss, 4)}",
            f"毛利润：{format_decimal_fixed(report.gross_profit, 4)}",
            f"毛亏损：{format_decimal_fixed(report.gross_loss, 4)}",
        ]
    )
    if result.open_position is not None:
        open_position = result.open_position
        lines.extend(
            [
                "期末未平仓：",
                f"方向：{'做多' if open_position.signal in ('buy', 'long') else '做空'}",
                f"开仓时间：{_format_backtest_timestamp(open_position.entry_ts)}",
                f"当前时间：{_format_backtest_timestamp(open_position.current_ts)}",
                f"开仓价格：{format_decimal_fixed(open_position.entry_price, 4)}",
                f"当前价格：{format_decimal_fixed(open_position.current_price, 4)}",
                f"初始止损：{format_decimal_fixed(open_position.initial_stop_loss, 4)}",
                f"当前止损：{format_decimal_fixed(open_position.stop_loss, 4)}",
                f"初始止盈：{format_decimal_fixed(open_position.initial_take_profit, 4)}",
                f"当前止盈：{format_decimal_fixed(open_position.take_profit, 4)}",
                f"开仓数量：{format_decimal_fixed(open_position.size, 4)}",
                f"浮动盈亏：{format_decimal_fixed(open_position.pnl, 4)}",
                f"R倍数：{format_decimal_fixed(open_position.r_multiple, 4)}",
                f"开仓手续费：{format_decimal_fixed(open_position.entry_fee, 4)}",
                f"资金费：{format_decimal_fixed(open_position.funding_cost, 4)}",
            ]
        )
    if result.manual_positions:
        lines.append("期末托管仓位：")
        for index, manual_position in enumerate(result.manual_positions, start=1):
            direction_text = "做多" if manual_position.signal == "long" else "做空"
            lines.append(
                f"[{index}] {direction_text} | 开仓={_format_backtest_timestamp(manual_position.entry_ts)} "
                f"| 移交={_format_backtest_timestamp(manual_position.handoff_ts)} | 数量={format_decimal_fixed(manual_position.size, 4)}"
            )
            lines.append(
                f"    开仓价={format_decimal_fixed(manual_position.entry_price, 4)} | 当前价={format_decimal_fixed(manual_position.current_price, 4)} "
                f"| 保本价={format_decimal_fixed(manual_position.break_even_price, 4)}"
            )
            lines.append(
                f"    浮盈亏={format_decimal_fixed(manual_position.pnl, 4)} | 手续费={format_decimal_fixed(manual_position.entry_fee, 4)} "
                f"| 资金费={format_decimal_fixed(manual_position.funding_cost, 4)} | 原因={manual_position.handoff_reason}"
            )
    return "\n".join(lines)


def _format_backtest_timestamp(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    return str(ts)


def _load_backtest_instrument(
    client: OkxRestClient,
    inst_id: str,
    *,
    candles_loaded: bool,
    local_only: bool = False,
) -> Instrument:
    if local_only:
        cached_getter = getattr(client, "get_cached_instrument", None)
        instrument = cached_getter(inst_id) if callable(cached_getter) else None
        if instrument is not None:
            return instrument
        if candles_loaded:
            raise RuntimeError(
                "本地K线已加载，但缺少可用的合约元数据缓存，当前无法纯离线回测。"
                "请先点“同步价格精度/下单规则”或联网同步一次后重试。"
            )
        raise RuntimeError("缺少可用的合约元数据缓存，当前无法纯离线回测。请先同步价格精度/下单规则。")

    get_instrument = getattr(client, "get_instrument")
    try:
        try:
            return get_instrument(inst_id, prefer_cached=True)
        except TypeError as exc:
            if "prefer_cached" not in str(exc):
                raise
            return get_instrument(inst_id)
    except Exception as exc:
        if candles_loaded:
            raise RuntimeError(
                "本地K线已加载，但缺少可用的合约元数据缓存，当前无法纯离线回测。"
                f"请联网同步一次合约信息后重试。原始错误：{exc}"
            ) from exc
        raise


def _load_backtest_candles(
    client: OkxRestClient,
    inst_id: str,
    bar: str,
    candle_limit: int,
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    preload_count: int = 0,
    local_only: bool = False,
) -> list[Candle]:
    def _finalize_loaded_candles(raw_candles: list[Candle], *, used_range_fetcher: bool) -> tuple[list[Candle], int]:
        confirmed = [candle for candle in raw_candles if candle.confirmed]
        if not used_range_fetcher:
            if start_ts is not None:
                confirmed[:] = [candle for candle in confirmed if candle.ts >= start_ts]
            if end_ts is not None:
                confirmed[:] = [candle for candle in confirmed if candle.ts <= end_ts]
            selected = confirmed if candle_limit <= 0 else confirmed[-candle_limit:]
            return selected, len(selected)

        window_start = 0 if start_ts is None else start_ts
        window_end = 9999999999999 if end_ts is None else end_ts
        preload = [candle for candle in confirmed if candle.ts < window_start]
        if preload_count > 0:
            preload = preload[-preload_count:]
        elif start_ts is not None:
            preload = []
        in_range = [candle for candle in confirmed if window_start <= candle.ts <= window_end]
        selected_in_range = in_range if candle_limit <= 0 else in_range[-candle_limit:]
        return preload + selected_in_range, len(selected_in_range)

    if local_only:
        used_range_fetcher = start_ts is not None or end_ts is not None
        request_limit = None if candle_limit <= 0 else candle_limit
        while True:
            if used_range_fetcher:
                raw_candles = load_candle_cache_range(
                    inst_id,
                    bar,
                    start_ts=0 if start_ts is None else start_ts,
                    end_ts=9999999999999 if end_ts is None else end_ts,
                    limit=request_limit,
                    preload_count=max(0, preload_count),
                )
            else:
                raw_candles = load_candle_cache(
                    inst_id,
                    bar,
                    limit=request_limit,
                )
            candles, selected_count = _finalize_loaded_candles(raw_candles, used_range_fetcher=used_range_fetcher)
            if candle_limit <= 0 or selected_count >= candle_limit or all(candle.confirmed for candle in raw_candles):
                return candles
            request_limit = max(int(request_limit or 0) + (candle_limit - selected_count), candle_limit + 1)

    range_fetcher = getattr(client, "get_candles_history_range", None)
    history_fetcher = getattr(client, "get_candles_history", None)
    used_range_fetcher = (start_ts is not None or end_ts is not None) and callable(range_fetcher)
    request_limit = candle_limit
    while True:
        if used_range_fetcher:
            raw_candles = range_fetcher(
                inst_id,
                bar,
                start_ts=0 if start_ts is None else start_ts,
                end_ts=9999999999999 if end_ts is None else end_ts,
                limit=request_limit,
                preload_count=max(0, preload_count),
            )
        elif callable(history_fetcher):
            raw_candles = history_fetcher(inst_id, bar, limit=request_limit)
        else:
            raw_candles = client.get_candles(inst_id, bar, limit=request_limit)
        candles, selected_count = _finalize_loaded_candles(raw_candles, used_range_fetcher=used_range_fetcher)
        if candle_limit <= 0 or selected_count >= candle_limit or all(candle.confirmed for candle in raw_candles):
            return candles
        request_limit = max(request_limit + (candle_limit - selected_count), candle_limit + 1)


def _build_cross_higher_tf_bias(
    primary: list[Candle],
    higher: list[Candle],
    ref_period: int,
) -> list[str]:
    """与 primary 等长：'long' / 'short' / 'both'（大周期 EMA 未就绪时不限制方向）。"""
    if not primary or not higher or ref_period <= 0:
        return ["both"] * len(primary)
    minimum = ref_period + 2
    closes_h = [candle.close for candle in higher]
    ema_h = ema(closes_h, ref_period)
    h_ts = closed_candle_available_timestamps(higher)
    out: list[str] = []
    for pc in primary:
        j = bisect.bisect_right(h_ts, pc.ts) - 1
        if j < 0 or j < minimum:
            out.append("both")
            continue
        ref = ema_h[j]
        if higher[j].close > ref:
            out.append("long")
        elif higher[j].close < ref:
            out.append("short")
        else:
            out.append("both")
    return out


def _build_mtf_filter_bias(
    entry_candles: list[Candle],
    filter_candles: list[Candle],
    fast_period: int,
    slow_period: int,
) -> list[str]:
    """与 entry_candles 等长：'long' / 'short' / 'neutral'。"""
    confirmed_filter_candles = [candle for candle in filter_candles if candle.confirmed]
    if not entry_candles or not confirmed_filter_candles or fast_period <= 0 or slow_period <= 0:
        return ["neutral"] * len(entry_candles)

    minimum = max(fast_period, slow_period)
    closes_h = [candle.close for candle in confirmed_filter_candles]
    fast_ema = ema(closes_h, fast_period)
    slow_ema = ema(closes_h, slow_period)
    h_ts = closed_candle_available_timestamps(confirmed_filter_candles)
    out: list[str] = []
    for entry_candle in entry_candles:
        j = bisect.bisect_right(h_ts, entry_candle.ts) - 1
        if j < minimum - 1:
            out.append("neutral")
            continue
        fast = fast_ema[j]
        slow = slow_ema[j]
        if fast > slow:
            out.append("long")
        elif fast < slow:
            out.append("short")
        else:
            out.append("neutral")
    return out


def _combine_direction_filter_bias(primary: list[str], secondary: list[str]) -> list[str]:
    size = min(len(primary), len(secondary))
    combined: list[str] = []
    for index in range(size):
        allowed = _direction_filter_bias_allowed_signals(primary[index]).intersection(
            _direction_filter_bias_allowed_signals(secondary[index])
        )
        combined.append(_direction_filter_bias_from_allowed_signals(allowed))
    if len(primary) > size:
        combined.extend(primary[size:])
    elif len(secondary) > size:
        combined.extend(secondary[size:])
    return combined


def _direction_filter_bias_allowed_signals(bias: str) -> set[str]:
    normalized = str(bias or "neutral").strip().lower()
    if normalized == "both":
        return {"long", "short"}
    if normalized == "long":
        return {"long"}
    if normalized == "short":
        return {"short"}
    return set()


def _direction_filter_bias_from_allowed_signals(allowed: set[str]) -> str:
    if allowed == {"long", "short"}:
        return "both"
    if allowed == {"long"}:
        return "long"
    if allowed == {"short"}:
        return "short"
    return "neutral"


def _direction_filter_allows_signal(bias: str, signal: str | None) -> bool:
    if signal not in {"long", "short"}:
        return False
    return signal in _direction_filter_bias_allowed_signals(bias)


def _reentry_confirmation_blocks_entry(
    *,
    config: StrategyConfig,
    signal: str,
    wave_entry_sequence: int,
    candle: Candle,
    confirmation_value: Decimal | None,
) -> bool:
    if not config.uses_reentry_confirmation():
        return False
    if wave_entry_sequence < config.resolved_reentry_confirmation_min_sequence():
        return False
    if confirmation_value is None:
        return True
    if signal == "long":
        return candle.close <= confirmation_value
    if signal == "short":
        return candle.close >= confirmation_value
    return False


def _required_mtf_filter_preload_candles(config: StrategyConfig) -> int:
    return max(
        int(config.mtf_filter_fast_ema_period),
        int(config.mtf_filter_slow_ema_period),
        0,
    ) + 5


def _required_backtest_preload_candles(config: StrategyConfig) -> int:
    family = _backtest_strategy_family(config.strategy_id)
    if strategy_is_cross_family(config.strategy_id):
        minimum = max(
            config.resolved_entry_reference_ema_period() + 2,
            config.atr_period + 2,
            config.ema_period + 2,
            config.trend_ema_period + 2,
        )
    elif family == "ema55_slope_short":
        minimum = max(config.ema_period, 2) + 1
    elif family == "ema15_ma50_pullback_long":
        minimum = btc_ema15_ma50_pullback_long_minimum_candles(config) + 1
    elif family == "ema15_ma50_pullback_short":
        minimum = btc_ema15_ma50_pullback_short_minimum_candles(config) + 1
    elif family == "body_retest_short":
        minimum = body_retest_short_minimum_candles(config)
    elif family == "adaptive_ema_rail":
        minimum = adaptive_rail_minimum_candles(config)
    elif family == "ema5_ema8":
        minimum = max(config.ema_period, config.trend_ema_period) + 1
    else:
        trend_slope_filter_enabled = (
            bool(config.trend_ema_slope_filter_enabled)
            and resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode) == "long_only"
        )
        minimum = max(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.resolved_entry_reference_ema_period(),
        )
    if config.uses_reentry_confirmation():
        minimum = max(minimum, config.resolved_reentry_confirmation_ma_period() + 2)
    return _backtest_trade_start_index(minimum)


def _build_backtest_data_source_note(client: OkxRestClient) -> str:
    stats = getattr(client, "last_candle_history_stats", None)
    if not isinstance(stats, dict):
        return ""
    full_history = bool(stats.get("full_history"))
    if stats.get("range_mode"):
        returned_count = int(stats.get("returned_count", 0) or 0)
        requested_count = int(stats.get("requested_count", 0) or 0)
        selected_count = int(stats.get("selected_count", 0) or 0)
        preload_count = int(stats.get("preload_count", 0) or 0)
        start_ts = stats.get("start_ts")
        end_ts = stats.get("end_ts")
        range_text = ""
        if start_ts and end_ts:
            range_text = (
                f"{datetime.fromtimestamp(int(start_ts) / 1000).strftime('%Y-%m-%d %H:%M')}"
                f" ~ {datetime.fromtimestamp(int(end_ts) / 1000).strftime('%Y-%m-%d %H:%M')}"
            )
        parts = ["按时间段取数"]
        if range_text:
            parts.append(range_text)
        if full_history:
            parts.append("区间全量")
        elif requested_count > 0:
            parts.append(f"上限 {requested_count} 根")
        if selected_count > 0:
            parts.append(f"区间内返回 {selected_count} 根")
        if preload_count > 0:
            parts.append(f"前置补足 {preload_count} 根")
        if returned_count > 0:
            parts.append(f"实际载入 {returned_count} 根")
        return " | ".join(parts)
    cache_hit_count = int(stats.get("cache_hit_count", 0) or 0)
    latest_fetch_count = int(stats.get("latest_fetch_count", 0) or 0)
    older_fetch_count = int(stats.get("older_fetch_count", 0) or 0)
    returned_count = int(stats.get("returned_count", 0) or 0)
    parts = [
        f"\u672c\u6b21\u547d\u4e2d\u672c\u5730\u7f13\u5b58 {cache_hit_count} \u6839",
        f"\u8865\u62c9\u6700\u65b0 {latest_fetch_count} \u6839",
    ]
    if full_history:
        parts.insert(0, "全量历史")
    if older_fetch_count > 0:
        parts.append(f"\u8865\u62c9\u66f4\u65e9 {older_fetch_count} \u6839")
    if returned_count > 0:
        parts.append(f"\u672c\u6b21\u56de\u6d4b\u53d6\u6570 {returned_count} \u6839")
    return " | ".join(parts)


def _run_ema5_ema8_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    strategy = EmaCrossEmaStopStrategy()
    minimum = max(config.ema_period, config.trend_ema_period) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]

        if open_position is not None:
            _, stop_line = strategy.latest_stop_line(candles[: index + 1], config)
            stop_hit = candle.close < stop_line if open_position.signal == "long" else candle.close > stop_line
            if stop_hit:
                exit_price_raw = candle.close
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
                        exit_reason="stop_loss",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None
                continue

        if open_position is not None:
            continue

        decision = strategy.evaluate(
            candles[: index + 1],
            config,
            price_increment=instrument.tick_size,
        )
        if decision.signal is None or decision.ema_value is None or decision.candle_ts is None:
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=resolved_config,
            entry_price=decision.entry_reference,
            stop_loss=decision.ema_value,
            risk_price_compatible=True,
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal=decision.signal,
            entry_index=index,
            entry_ts=decision.candle_ts,
            entry_price_raw=decision.entry_reference,
            stop_loss=decision.ema_value,
            take_profit=decision.entry_reference,
            atr_value=decision.atr_value,
            size=size,
            entry_fee_rate=taker_fee_rate,
            exit_fee_rate=taker_fee_rate,
            entry_fee_type="taker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
        )

    return trades, _build_terminal_open_position(open_position, candles)


def _run_cross_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
    higher_tf_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    family = _backtest_strategy_family(config.strategy_id)
    if family == "cross_legacy" and config.signal_mode == "both":
        raise RuntimeError(
            "EMA 突破/跌破（旧版）回测不支持 signal_mode=双向；请分别回测或改用「EMA 突破做多」「EMA 跌破做空」策略。"
        )
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"
    effective_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
    eval_config = replace(config, signal_mode=effective_mode)
    if family == "cross_breakdown_short":
        wanted_signal = "short"
    elif family == "cross_breakout_long":
        wanted_signal = "long"
    else:
        wanted_signal = "long" if effective_mode == "long_only" else "short"
    reference_ema_period = eval_config.resolved_entry_reference_ema_period()
    minimum = max(
        reference_ema_period + 2,
        eval_config.atr_period + 2,
        eval_config.ema_period + 2,
        eval_config.trend_ema_period + 2,
    )
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    current_wave_signal: str | None = None
    entries_in_current_wave = 0
    valid_entry_plan_count = 0
    invalid_protection_count = 0
    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, eval_config.ema_period, eval_config.resolved_ema_type())
    trend_ema_values = moving_average(closes, eval_config.trend_ema_period, eval_config.resolved_trend_ema_type())
    reference_ema_values = (
        ema_values
        if (
            reference_ema_period == eval_config.ema_period
            and eval_config.resolved_entry_reference_ema_type() == eval_config.resolved_ema_type()
        )
        else moving_average(closes, reference_ema_period, eval_config.resolved_entry_reference_ema_type())
    )
    atr_values = atr(candles, eval_config.atr_period)

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        if open_position is not None:
            trend_ema = trend_ema_values[index] if index < len(trend_ema_values) else None
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
            elif (
                bool(config.trend_ema_close_exit_after_trigger_r_enabled)
                and open_position.signal == "long"
                and trend_ema is not None
                and candle.close <= trend_ema
                and _dynamic_trigger_r_reached(open_position, _trend_ema_close_exit_trigger_r(config))
            ):
                exit_price_raw = candle.close
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
                        exit_reason="trend_ema_close_exit",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None
            elif (
                int(config.hold_close_exit_bars) > 0
                and index > open_position.entry_index
                and _holding_bars_for_position(open_position, index) >= int(config.hold_close_exit_bars)
            ):
                exit_price_raw = candle.close
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
                        exit_reason="hold_close_exit",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None

        if open_position is not None:
            continue

        decision = _evaluate_cross_signal_precomputed(
            candles,
            index,
            ema_values,
            reference_ema_values,
            trend_ema_values,
            atr_values,
            eval_config,
        )
        if decision.signal is None:
            current_wave_signal = None
            entries_in_current_wave = 0
            continue
        if decision.signal != wanted_signal:
            continue
        if higher_tf_bias is not None and index < len(higher_tf_bias):
            bias = higher_tf_bias[index]
            if decision.signal == "long" and bias == "short":
                continue
            if decision.signal == "short" and bias == "long":
                continue
        if current_wave_signal != decision.signal:
            current_wave_signal = decision.signal
            entries_in_current_wave = 0
        if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue
        if decision.ema_value is None:
            continue

        resolved_config = _resolve_backtest_config(eval_config, trades)
        entry_reference = snap_to_increment(decision.entry_reference, instrument.tick_size, "nearest")
        if wanted_signal == "long":
            stop_loss_raw = decision.ema_value - (decision.atr_value * resolved_config.atr_stop_multiplier)
            stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "up")
            take_profit_raw = entry_reference + (decision.atr_value * resolved_config.atr_take_multiplier)
            take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "down")
            side = "buy"
            pos_side = None if resolved_config.position_mode != "long_short" else "long"
            plan_signal = "long"
        else:
            stop_loss_raw = decision.ema_value + (decision.atr_value * resolved_config.atr_stop_multiplier)
            stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "down")
            take_profit_raw = entry_reference - (decision.atr_value * resolved_config.atr_take_multiplier)
            take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "up")
            side = "sell"
            pos_side = None if resolved_config.position_mode != "long_short" else "short"
            plan_signal = "short"
        try:
            validate_protection_prices(
                direction=plan_signal,
                entry_reference=entry_reference,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )
        except InvalidProtectionPlanError:
            invalid_protection_count += 1
            continue
        valid_entry_plan_count += 1
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=resolved_config,
            entry_price=entry_reference,
            stop_loss=stop_loss,
            risk_price_compatible=True,
        )
        plan = OrderPlan(
            inst_id=instrument.inst_id,
            side=side,
            pos_side=pos_side,
            size=size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            entry_reference=entry_reference,
            atr_value=decision.atr_value,
            signal=plan_signal,
            candle_ts=decision.candle_ts,
            tp_sl_inst_id=instrument.inst_id,
            tp_sl_mode="exchange",
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal=plan.signal,
            entry_index=index,
            entry_ts=plan.candle_ts,
            entry_price_raw=plan.entry_reference,
            stop_loss=plan.stop_loss,
            take_profit=plan.take_profit,
            atr_value=plan.atr_value,
            size=plan.size,
            entry_fee_rate=taker_fee_rate,
            exit_fee_rate=taker_fee_rate,
            entry_fee_type="taker",
            entry_slippage_rate=eval_config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=eval_config.resolved_backtest_exit_slippage_rate(),
            funding_rate=eval_config.backtest_funding_rate,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            dynamic_exit_fee_rate=taker_fee_rate,
            dynamic_two_r_break_even=config.dynamic_two_r_break_even,
            dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
            dynamic_first_lock_r=_dynamic_first_lock_r(config),
            dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
            dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
            dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            dynamic_protection_rules=_dynamic_protection_rules(config),
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            next_dynamic_trigger_r=_ema55_slope_lock_profit_trigger_r(config),
        )
        entries_in_current_wave += 1

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles)


def _run_ema55_slope_short_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
    direction_filter_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    negative_entry_bars = _ema55_slope_negative_entry_bars(config)
    minimum = max(int(config.ema_period), int(config.trend_ema_period), int(config.atr_period), 2) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    ema21_values = moving_average(closes, 21, "ema")
    atr_values = atr(candles, int(config.atr_period))
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    entry_slope_threshold_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    uses_flat_exit = is_btc_ema55_slope_short_strategy(config.strategy_id)
    slope_exit_enabled = _ema55_slope_exit_condition_enabled(config)
    dynamic_take_profit_enabled = _ema55_slope_lock_profit_enabled(config)
    dynamic_two_r_break_even = _ema55_slope_dynamic_two_r_break_even_enabled(config)
    dynamic_fee_offset_enabled = _ema55_slope_dynamic_fee_offset_enabled(config)
    dynamic_trigger_r = _ema55_slope_lock_profit_trigger_r(config)
    take_profit_enabled = not uses_flat_exit
    reentry_reclaim_state: str | None = None
    reentry_ema21_near_state: str | None = None
    reentry_bearish_bar_required = False
    valid_entry_plan_count = 0
    invalid_protection_count = 0

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        current_ema = ema_values[index]
        current_ema21 = ema21_values[index] if index < len(ema21_values) else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        if current_ema is None or current_ema21 is None or atr_value is None or atr_value <= 0:
            continue

        previous_ema = ema_values[index - 1] if index > 0 else None
        slope = (current_ema - previous_ema) if previous_ema is not None else Decimal("0")
        slope_ratio = _ema55_slope_ratio_from_series(ema_values, index)
        recent_slope_ratios = [
            _ema55_slope_ratio_from_series(ema_values, slope_index)
            for slope_index in range(index - negative_entry_bars + 1, index + 1)
        ]
        previous_slope_ratio_before_window = _ema55_slope_ratio_from_series(ema_values, index - negative_entry_bars)
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
            exited_this_bar = True

        if (
            open_position is not None
            or not _ema55_slope_entry_triggered(
                config,
                recent_slope_ratios=recent_slope_ratios,
                previous_slope_ratio_before_window=previous_slope_ratio_before_window,
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
        if direction_filter_bias is not None and index < len(direction_filter_bias):
            if not _direction_filter_allows_signal(direction_filter_bias[index], "short"):
                continue
        if reentry_bearish_bar_required and (exited_this_bar or candle.close >= candle.open):
            continue
        if (
            reentry_bearish_bar_required
            and config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low
            and (index <= 0 or candle.close >= candles[index - 1].low)
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
        entry_price_raw = protection.entry_reference
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
            entry_price_raw=entry_price_raw,
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
            dynamic_two_r_break_even=dynamic_two_r_break_even,
            dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
            dynamic_first_lock_r=_dynamic_first_lock_r(config),
            dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
            dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
            dynamic_protection_rules=_dynamic_protection_rules(config),
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            next_dynamic_trigger_r=dynamic_trigger_r,
            apply_entry_slippage=True,
        )
        reentry_reclaim_state = None
        if reentry_bearish_bar_required and candle.close < candle.open:
            reentry_bearish_bar_required = False

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles)


def _run_body_retest_short_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
    direction_filter_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    minimum = body_retest_short_minimum_candles(config)
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    line_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    atr_values = atr(candles, int(config.atr_period))
    atr_percentiles = rolling_body_retest_percentile(
        atr_values,
        BODY_RETEST_ATR_PERCENTILE_LOOKBACK,
    )
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"
    slope_exit_enabled = bool(config.ema55_slope_exit_enabled)
    dynamic_trigger_r = _ema55_slope_lock_profit_trigger_r(config)
    slope_threshold = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    breakdown_mult = Decimal(str(config.body_retest_breakdown_atr_multiplier))
    retest_mult = Decimal(str(config.body_retest_retest_atr_multiplier))
    body_atr_limit = Decimal(str(config.body_retest_body_atr_limit))
    atr_percentile_limit = Decimal(str(config.atr_percentile_filter_max))
    watch_bars = max(int(config.body_retest_watch_bars), 1)
    pending_index: int | None = None
    pending_reclaim_close: Decimal | None = None
    valid_entry_plan_count = 0
    invalid_protection_count = 0

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        line_value = line_values[index]
        prev_line = line_values[index - 1] if index > 0 else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        atr_pct = atr_percentiles[index] if index < len(atr_percentiles) else None
        if line_value is None or prev_line is None or atr_value is None or atr_value <= 0 or atr_pct is None:
            continue
        slope_ratio = (line_value - prev_line) / line_value if line_value != 0 else None
        line_slope = line_value - prev_line
        if slope_ratio is None:
            continue

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

        if open_position is not None and slope_exit_enabled and line_slope > 0:
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

        if open_position is not None:
            continue

        if pending_index is not None and pending_reclaim_close is not None:
            age = index - pending_index
            if age > watch_bars:
                pending_index = None
                pending_reclaim_close = None
            else:
                bearish_close = candle.close < candle.open
                near_line = candle.high >= (line_value - retest_mult * atr_value)
                still_below = candle.close < line_value
                reclaim_ok = candle.close <= pending_reclaim_close
                bias_ok = body_retest_short_bias_allows_short(direction_filter_bias, index)
                if near_line and still_below and bearish_close and reclaim_ok and bias_ok:
                    try:
                        protection = build_body_retest_short_protection_plan(
                            instrument=instrument,
                            config=config,
                            entry_reference=candle.close,
                            signal_candle_high=candle.high,
                            signal_candle_close=candle.close,
                            atr_value=atr_value,
                            candle_ts=candle.ts,
                            trigger_inst_id=instrument.inst_id,
                        )
                    except InvalidProtectionPlanError:
                        invalid_protection_count += 1
                        pending_index = None
                        pending_reclaim_close = None
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
                        dynamic_exit_fee_rate=taker_fee_rate,
                        dynamic_two_r_break_even=config.dynamic_two_r_break_even,
                        dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
                        dynamic_first_lock_r=_dynamic_first_lock_r(config),
                        dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
                        dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
                        dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
                        dynamic_protection_rules=_dynamic_protection_rules(config),
                        time_stop_break_even_enabled=config.time_stop_break_even_enabled,
                        time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                        next_dynamic_trigger_r=dynamic_trigger_r,
                        apply_entry_slippage=True,
                    )
                    pending_index = None
                    pending_reclaim_close = None
                    continue

        if pending_index is not None:
            continue
        if slope_ratio > slope_threshold or atr_pct > atr_percentile_limit:
            continue
        if candle.close >= line_value - breakdown_mult * atr_value or candle.close >= candle.open:
            continue
        if not body_retest_short_bias_allows_short(direction_filter_bias, index):
            continue
        body_size = abs(candle.open - candle.close)
        if (body_size / atr_value) > body_atr_limit:
            continue
        pending_index = index
        pending_reclaim_close = candle.close + (candle.open - candle.close) * Decimal("0.5")

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles)


def _btc_ema15_ma50_pullback_exit_mode(config: StrategyConfig) -> str:
    raw = str(config.exit_mode or "").strip().lower()
    if raw in {
        "fixed_rr",
        "fixed_rr_or_ema15_close",
        "dynamic",
        "dynamic_or_ema15_close",
        "ema15_close",
    }:
        return raw
    return "dynamic_or_ema15_close" if str(config.take_profit_mode or "") == "dynamic" else "fixed_rr"


def _btc_ema15_ma50_uses_dynamic_exit(config: StrategyConfig) -> bool:
    return _btc_ema15_ma50_pullback_exit_mode(config) in {"dynamic", "dynamic_or_ema15_close"}


def _btc_ema15_ma50_uses_fixed_rr_exit(config: StrategyConfig) -> bool:
    return _btc_ema15_ma50_pullback_exit_mode(config) in {"fixed_rr", "fixed_rr_or_ema15_close"}


def _btc_ema15_ma50_uses_ema15_close_exit(config: StrategyConfig) -> bool:
    return _btc_ema15_ma50_pullback_exit_mode(config) in {
        "fixed_rr_or_ema15_close",
        "dynamic_or_ema15_close",
        "ema15_close",
    }


def _btc_ema15_ma50_trade_metadata(
    candidate: LongPullbackCandidate | ShortPullbackCandidate,
) -> dict[str, Any]:
    return {
        "cross_ts": candidate.cross_ts,
        "cross_index": candidate.cross_index,
        "signal_ts": candidate.signal_ts,
        "signal_index": candidate.signal_index,
        "bars_after_cross": candidate.bars_after_cross,
        "pullback_index": candidate.pullback_index,
        "pullback_depth_pct": candidate.pullback_depth_pct,
        "ema15_slope_5": candidate.ema15_slope_5,
        "ema15_slope_10": candidate.ema15_slope_10,
        "ma50_slope_10": candidate.ma50_slope_10,
        "daily_filter_pass": candidate.daily_filter_pass,
        "max_r_before_exit": Decimal("0"),
        "max_drawdown_r": Decimal("0"),
        "max_favorable_price": None,
        "max_favorable_ts": None,
        "max_adverse_price": None,
        "max_adverse_ts": None,
        "stop_history": [],
    }


def _btc_ema15_ma50_record_stop_history(
    position: _OpenPosition,
    *,
    candle_ts: int,
    label: str | None = None,
) -> None:
    history = position.metadata.setdefault("stop_history", [])
    current_stop = Decimal(position.stop_loss)
    if history:
        last_item = history[-1]
        if Decimal(str(last_item.get("price", current_stop))) == current_stop:
            return
    history.append(
        {
            "ts": candle_ts,
            "price": current_stop,
            "label": label or "stop",
        }
    )


def _btc_ema15_ma50_track_excursions(position: _OpenPosition, candle: Candle) -> None:
    risk_per_unit = abs(position.risk_per_unit)
    if risk_per_unit <= 0:
        return
    entry_reference = _position_strategy_entry_price(position)
    metadata = position.metadata
    if position.signal == "long":
        max_r = max((candle.high - entry_reference) / risk_per_unit, Decimal("0"))
        max_dd = max((entry_reference - candle.low) / risk_per_unit, Decimal("0"))
        if max_r >= Decimal(str(metadata.get("max_r_before_exit", Decimal("0")))):
            metadata["max_r_before_exit"] = max_r
            metadata["max_favorable_price"] = candle.high
            metadata["max_favorable_ts"] = candle.ts
        if max_dd >= Decimal(str(metadata.get("max_drawdown_r", Decimal("0")))):
            metadata["max_drawdown_r"] = max_dd
            metadata["max_adverse_price"] = candle.low
            metadata["max_adverse_ts"] = candle.ts
        return
    max_r = max((entry_reference - candle.low) / risk_per_unit, Decimal("0"))
    max_dd = max((candle.high - entry_reference) / risk_per_unit, Decimal("0"))
    if max_r >= Decimal(str(metadata.get("max_r_before_exit", Decimal("0")))):
        metadata["max_r_before_exit"] = max_r
        metadata["max_favorable_price"] = candle.low
        metadata["max_favorable_ts"] = candle.ts
    if max_dd >= Decimal(str(metadata.get("max_drawdown_r", Decimal("0")))):
        metadata["max_drawdown_r"] = max_dd
        metadata["max_adverse_price"] = candle.high
        metadata["max_adverse_ts"] = candle.ts


def _btc_ema15_ma50_close_at_open(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_reason: str,
    exit_fee_rate: Decimal,
    exit_fee_type: str,
) -> BacktestTrade:
    exit_price_raw = snap_to_increment(candle.open, position.tick_size, "nearest")
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


def _run_btc_ema15_ma50_pullback_long_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    direction_filter_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    if not is_btc_ema15_ma50_pullback_long_strategy(config.strategy_id):
        raise RuntimeError("BTC EMA15/MA50 回踩做多回测配置不匹配。")
    minimum = btc_ema15_ma50_pullback_long_minimum_candles(config)
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema15_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    ma50_values = moving_average(closes, int(config.trend_ema_period), config.resolved_trend_ema_type())
    candidates = scan_btc_ema15_ma50_pullback_long_candidates(
        candles,
        config,
        direction_filter_bias=direction_filter_bias,
    )
    candidates_by_signal_index = {candidate.signal_index: candidate for candidate in candidates}
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    pending_close_reason: str | None = None
    entry_sequence = 0
    rr_value = config.resolved_fixed_rr()
    dynamic_take_profit_enabled = _btc_ema15_ma50_uses_dynamic_exit(config)

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        closed_round_this_candle = False

        if open_position is not None and pending_close_reason is not None:
            _btc_ema15_ma50_track_excursions(open_position, candle)
            trades.append(
                _btc_ema15_ma50_close_at_open(
                    open_position,
                    candle,
                    index,
                    exit_reason=pending_close_reason,
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
            )
            open_position = None
            pending_close_reason = None
            closed_round_this_candle = True

        if open_position is not None:
            _btc_ema15_ma50_track_excursions(open_position, candle)
            previous_stop = open_position.stop_loss
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
                pending_close_reason = None
                closed_round_this_candle = True
            else:
                if open_position.stop_loss != previous_stop:
                    _btc_ema15_ma50_record_stop_history(open_position, candle_ts=candle.ts)
                ema15_value = ema15_values[index] if index < len(ema15_values) else None
                if (
                    open_position is not None
                    and _btc_ema15_ma50_uses_ema15_close_exit(config)
                    and ema15_value is not None
                    and candle.close < ema15_value
                ):
                    pending_close_reason = "ema15_close_exit"

        if open_position is not None or closed_round_this_candle or index == 0:
            continue

        candidate = candidates_by_signal_index.get(index - 1)
        if candidate is None:
            continue
        if candidate.pullback_index > config.resolved_max_pullback_index():
            continue
        if not candidate.daily_filter_pass:
            continue

        entry_candle = candle
        entry_price_raw = snap_to_increment(entry_candle.open, instrument.tick_size, "nearest")
        stop_distance = candidate.atr_at_signal * Decimal(str(config.atr_stop_multiplier))
        stop_price = snap_to_increment(entry_price_raw - stop_distance, instrument.tick_size, "down")
        if stop_price >= entry_price_raw:
            continue
        take_profit = entry_price_raw
        take_profit_enabled = False
        if _btc_ema15_ma50_uses_fixed_rr_exit(config):
            risk_distance = entry_price_raw - stop_price
            take_profit = snap_to_increment(entry_price_raw + (risk_distance * rr_value), instrument.tick_size, "up")
            take_profit_enabled = True

        resolved_config = replace(
            _resolve_backtest_config(config, trades),
            take_profit_mode="dynamic" if dynamic_take_profit_enabled else "fixed",
        )
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=resolved_config,
            entry_price=entry_price_raw,
            stop_loss=stop_price,
            risk_price_compatible=bool(resolved_config.risk_amount is not None and resolved_config.risk_amount > 0),
        )
        entry_sequence += 1
        metadata = _btc_ema15_ma50_trade_metadata(candidate)
        metadata.update(
            {
                "entry_signal_index": candidate.signal_index,
                "entry_index": index,
                "ema15_at_entry": candidate.ema15_at_signal,
                "ma50_at_entry": candidate.ma50_at_signal,
                "atr_at_entry": candidate.atr_at_signal,
                "stop_price": stop_price,
            }
        )
        filled_position = _create_open_position(
            instrument=instrument,
            signal="long",
            entry_index=index,
            entry_ts=entry_candle.ts,
            entry_price_raw=entry_price_raw,
            stop_loss=stop_price,
            take_profit=take_profit,
            atr_value=candidate.atr_at_signal,
            size=size,
            entry_fee_rate=maker_fee_rate,
            exit_fee_rate=taker_fee_rate,
            entry_fee_type="maker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
            entry_sequence=entry_sequence,
            wave_entry_sequence=candidate.pullback_index,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            take_profit_enabled=take_profit_enabled,
            dynamic_exit_fee_rate=taker_fee_rate,
            dynamic_two_r_break_even=bool(config.dynamic_two_r_break_even),
            dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
            dynamic_first_lock_r=_dynamic_first_lock_r(config),
            dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
            dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
            dynamic_fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
            dynamic_protection_rules=_dynamic_protection_rules(resolved_config),
            time_stop_break_even_enabled=bool(config.time_stop_break_even_enabled),
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            next_dynamic_trigger_r=_first_dynamic_rule_trigger_r(resolved_config),
            apply_entry_slippage=True,
            metadata=metadata,
        )
        _btc_ema15_ma50_record_stop_history(filled_position, candle_ts=entry_candle.ts, label="initial_stop")
        _btc_ema15_ma50_track_excursions(filled_position, candle)
        closed_trade = _try_close_position_same_candle_after_fill(
            filled_position,
            candle,
            index,
            exit_fee_rate=taker_fee_rate,
            exit_fee_type="taker",
        )
        if closed_trade is not None:
            trades.append(closed_trade)
            pending_close_reason = None
            continue
        open_position = filled_position
        pending_close_reason = None

    return trades, _build_terminal_open_position(open_position, candles)


def _run_btc_ema15_ma50_pullback_short_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    direction_filter_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    if not is_btc_ema15_ma50_pullback_short_strategy(config.strategy_id):
        raise RuntimeError("BTC EMA15/MA50 回踩做空回测配置不匹配。")
    minimum = btc_ema15_ma50_pullback_short_minimum_candles(config)
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema15_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    candidates = scan_btc_ema15_ma50_pullback_short_candidates(
        candles,
        config,
        direction_filter_bias=direction_filter_bias,
    )
    candidates_by_signal_index = {candidate.signal_index: candidate for candidate in candidates}
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    pending_close_reason: str | None = None
    entry_sequence = 0
    rr_value = config.resolved_fixed_rr()
    dynamic_take_profit_enabled = _btc_ema15_ma50_uses_dynamic_exit(config)

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        closed_round_this_candle = False

        if open_position is not None and pending_close_reason is not None:
            _btc_ema15_ma50_track_excursions(open_position, candle)
            trades.append(
                _btc_ema15_ma50_close_at_open(
                    open_position,
                    candle,
                    index,
                    exit_reason=pending_close_reason,
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
            )
            open_position = None
            pending_close_reason = None
            closed_round_this_candle = True

        if open_position is not None:
            _btc_ema15_ma50_track_excursions(open_position, candle)
            previous_stop = open_position.stop_loss
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
                pending_close_reason = None
                closed_round_this_candle = True
            else:
                if open_position.stop_loss != previous_stop:
                    _btc_ema15_ma50_record_stop_history(open_position, candle_ts=candle.ts)
                ema15_value = ema15_values[index] if index < len(ema15_values) else None
                if (
                    open_position is not None
                    and _btc_ema15_ma50_uses_ema15_close_exit(config)
                    and ema15_value is not None
                    and candle.close > ema15_value
                ):
                    pending_close_reason = "ema15_close_exit"

        if open_position is not None or closed_round_this_candle or index == 0:
            continue

        candidate = candidates_by_signal_index.get(index - 1)
        if candidate is None:
            continue
        if candidate.pullback_index > config.resolved_max_pullback_index():
            continue
        if not candidate.daily_filter_pass:
            continue

        entry_candle = candle
        entry_price_raw = snap_to_increment(entry_candle.open, instrument.tick_size, "nearest")
        stop_distance = candidate.atr_at_signal * Decimal(str(config.atr_stop_multiplier))
        stop_price = snap_to_increment(entry_price_raw + stop_distance, instrument.tick_size, "up")
        if stop_price <= entry_price_raw:
            continue
        take_profit = entry_price_raw
        take_profit_enabled = False
        if _btc_ema15_ma50_uses_fixed_rr_exit(config):
            risk_distance = stop_price - entry_price_raw
            take_profit = snap_to_increment(entry_price_raw - (risk_distance * rr_value), instrument.tick_size, "down")
            take_profit_enabled = True

        resolved_config = replace(
            _resolve_backtest_config(config, trades),
            take_profit_mode="dynamic" if dynamic_take_profit_enabled else "fixed",
        )
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=resolved_config,
            entry_price=entry_price_raw,
            stop_loss=stop_price,
            risk_price_compatible=bool(resolved_config.risk_amount is not None and resolved_config.risk_amount > 0),
        )
        entry_sequence += 1
        metadata = _btc_ema15_ma50_trade_metadata(candidate)
        metadata.update(
            {
                "entry_signal_index": candidate.signal_index,
                "entry_index": index,
                "ema15_at_entry": candidate.ema15_at_signal,
                "ma50_at_entry": candidate.ma50_at_signal,
                "atr_at_entry": candidate.atr_at_signal,
                "stop_price": stop_price,
            }
        )
        filled_position = _create_open_position(
            instrument=instrument,
            signal="short",
            entry_index=index,
            entry_ts=entry_candle.ts,
            entry_price_raw=entry_price_raw,
            stop_loss=stop_price,
            take_profit=take_profit,
            atr_value=candidate.atr_at_signal,
            size=size,
            entry_fee_rate=maker_fee_rate,
            exit_fee_rate=taker_fee_rate,
            entry_fee_type="maker",
            entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
            exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
            funding_rate=config.backtest_funding_rate,
            entry_sequence=entry_sequence,
            wave_entry_sequence=candidate.pullback_index,
            dynamic_take_profit_enabled=dynamic_take_profit_enabled,
            take_profit_enabled=take_profit_enabled,
            dynamic_exit_fee_rate=taker_fee_rate,
            dynamic_two_r_break_even=bool(config.dynamic_two_r_break_even),
            dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
            dynamic_first_lock_r=_dynamic_first_lock_r(config),
            dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
            dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
            dynamic_fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
            dynamic_protection_rules=_dynamic_protection_rules(resolved_config),
            time_stop_break_even_enabled=bool(config.time_stop_break_even_enabled),
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
            next_dynamic_trigger_r=_first_dynamic_rule_trigger_r(resolved_config),
            apply_entry_slippage=True,
            metadata=metadata,
        )
        _btc_ema15_ma50_record_stop_history(filled_position, candle_ts=entry_candle.ts, label="initial_stop")
        _btc_ema15_ma50_track_excursions(filled_position, candle)
        closed_trade = _try_close_position_same_candle_after_fill(
            filled_position,
            candle,
            index,
            exit_fee_rate=taker_fee_rate,
            exit_fee_type="taker",
        )
        if closed_trade is not None:
            trades.append(closed_trade)
            pending_close_reason = None
            continue
        open_position = filled_position
        pending_close_reason = None

    return trades, _build_terminal_open_position(open_position, candles)


def _run_adaptive_rail_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None, AdaptiveRailBacktestStats]:
    minimum = adaptive_rail_minimum_candles(config)
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    candidate_periods = adaptive_rail_candidate_periods(config)
    ema_periods = {period for period in candidate_periods if period > 0}
    ema_periods.add(int(config.trend_ema_period))
    if config.uses_reentry_confirmation():
        ema_periods.add(config.resolved_reentry_confirmation_ma_period())
    if bool(config.rail_fast_gate_enabled) and int(config.rail_fast_gate_period) > 0:
        ema_periods.add(int(config.rail_fast_gate_period))
    ema_by_period = {period: ema(closes, period) for period in sorted(ema_periods)}
    trend_ema_values = ema_by_period.get(int(config.trend_ema_period)) or ema(closes, int(config.trend_ema_period))
    reentry_confirmation_values = (
        ema_by_period.get(config.resolved_reentry_confirmation_ma_period())
        if config.uses_reentry_confirmation() and config.resolved_reentry_confirmation_ma_type() == "ema"
        else moving_average(
            closes,
            config.resolved_reentry_confirmation_ma_period(),
            config.resolved_reentry_confirmation_ma_type(),
        )
        if config.uses_reentry_confirmation()
        else []
    )
    ema200_values = ema_by_period.get(200) or ema(closes, 200)
    atr_values = atr(candles, config.atr_period)
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    active_plan = None
    current_rail_period: int | None = None
    open_position_rail_period: int | None = None
    stats_current_rail_period: int | None = None
    entries_on_current_rail = 0
    entry_sequence = 0
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"
    valid_entry_plan_count = 0
    invalid_protection_count = 0
    stats_history: list[tuple[str, int | None]] = []

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]

        if open_position is not None:
            trend_ema = trend_ema_values[index] if index < len(trend_ema_values) else None
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
                open_position_rail_period = None

        if open_position is not None and open_position_rail_period in ema_by_period:
            atr_value = atr_values[index]
            if is_adaptive_rail_hard_break_at(
                candles,
                index,
                ema_values=ema_by_period[open_position_rail_period],
                atr_values=atr_values,
                config=config,
            ):
                exit_price_raw = snap_to_increment(candle.close, instrument.tick_size, "nearest")
                exit_price = _apply_slippage_price(
                    exit_price_raw,
                    signal=open_position.signal,
                    tick_size=instrument.tick_size,
                    slippage_rate=config.resolved_backtest_exit_slippage_rate(),
                    is_entry=False,
                )
                trades.append(
                    _build_closed_trade(
                        open_position,
                        candle,
                        index,
                        exit_price_raw=exit_price_raw,
                        exit_price=exit_price,
                        exit_reason="rail_broken",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None
                open_position_rail_period = None

        stats_snapshot = evaluate_adaptive_rail_signal(
            candles,
            index,
            ema_by_period=ema_by_period,
            ema200_values=ema200_values,
            atr_values=atr_values,
            config=config,
            current_period=stats_current_rail_period,
        )
        if stats_snapshot.state == ADAPTIVE_RAIL_STATE_BROKEN:
            stats_current_rail_period = None
        elif stats_snapshot.dominant_period is not None:
            stats_current_rail_period = stats_snapshot.dominant_period
        stats_history.append((stats_snapshot.state, stats_snapshot.dominant_period))

        if active_plan is not None and open_position is None:
            plan_rail_period = current_rail_period
            filled_position = _try_fill_dynamic_order(
                instrument,
                active_plan,
                candle,
                index,
                entry_fee_rate=maker_fee_rate,
                entry_fee_type="maker",
                entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
                exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
                funding_rate=config.backtest_funding_rate,
                entry_sequence=entry_sequence + 1,
                wave_entry_sequence=entries_on_current_rail + 1,
                dynamic_take_profit_enabled=dynamic_take_profit_enabled,
                dynamic_exit_fee_rate=taker_fee_rate,
                dynamic_two_r_break_even=config.dynamic_two_r_break_even,
                dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
                dynamic_first_lock_r=_dynamic_first_lock_r(config),
                dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
                dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
                dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
                dynamic_protection_rules=_dynamic_protection_rules(config),
                time_stop_break_even_enabled=config.time_stop_break_even_enabled,
                time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                next_dynamic_trigger_r=_ema55_slope_lock_profit_trigger_r(config),
                immediate_entry_fee_rate=taker_fee_rate,
                immediate_entry_fee_type="taker",
                adaptive_rail_period=plan_rail_period,
            )
            active_plan = None
            if filled_position is not None:
                entry_sequence += 1
                entries_on_current_rail += 1
                closed_trade = _try_close_position_same_candle_after_fill(
                    filled_position,
                    candle,
                    index,
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
                if closed_trade is not None:
                    trades.append(closed_trade)
                else:
                    open_position = filled_position
                    open_position_rail_period = plan_rail_period

        if open_position is not None or index >= len(candles) - 1:
            continue

        snapshot = evaluate_adaptive_rail_signal(
            candles,
            index,
            ema_by_period=ema_by_period,
            ema200_values=ema200_values,
            atr_values=atr_values,
            config=config,
            current_period=current_rail_period,
        )
        if snapshot.state == ADAPTIVE_RAIL_STATE_BROKEN:
            active_plan = None
            entries_on_current_rail = 0
            current_rail_period = None
            continue
        if snapshot.dominant_period != current_rail_period:
            current_rail_period = snapshot.dominant_period
            entries_on_current_rail = 0
        decision = snapshot.decision
        if decision.signal is None:
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue
        if config.max_entries_per_trend > 0 and entries_on_current_rail >= config.max_entries_per_trend:
            continue
        next_wave_entry_sequence = entries_on_current_rail + 1
        reentry_confirmation_value = (
            reentry_confirmation_values[index]
            if index < len(reentry_confirmation_values)
            else None
        )
        if _reentry_confirmation_blocks_entry(
            config=config,
            signal=decision.signal,
            wave_entry_sequence=next_wave_entry_sequence,
            candle=candles[index],
            confirmation_value=reentry_confirmation_value,
        ):
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        try:
            active_plan = _build_backtest_order_plan(
                instrument=instrument,
                config=resolved_config,
                order_size=resolved_config.order_size,
                signal=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                signal_candle_high=decision.signal_candle_high,
                signal_candle_low=decision.signal_candle_low,
            )
        except InvalidProtectionPlanError:
            invalid_protection_count += 1
            continue
        valid_entry_plan_count += 1

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, _build_terminal_open_position(open_position, candles), _build_adaptive_rail_backtest_stats(
        stats_history,
        trades,
    )


def _build_adaptive_rail_backtest_stats(
    history: list[tuple[str, int | None]],
    trades: list[BacktestTrade],
) -> AdaptiveRailBacktestStats:
    evaluation_bars = len(history)
    confirmed_entries = [
        (state, period) for state, period in history if state == ADAPTIVE_RAIL_STATE_CONFIRMED and period is not None
    ]
    confirmed_bars = len(confirmed_entries)
    broken_state_bars = sum(1 for state, _ in history if state == ADAPTIVE_RAIL_STATE_BROKEN)
    confirmed_coverage_pct = (
        Decimal("0")
        if evaluation_bars <= 0
        else (Decimal(confirmed_bars) / Decimal(evaluation_bars)) * Decimal("100")
    )
    broken_state_pct = (
        Decimal("0")
        if evaluation_bars <= 0
        else (Decimal(broken_state_bars) / Decimal(evaluation_bars)) * Decimal("100")
    )

    switch_count = 0
    hold_lengths: list[int] = []
    period_counts: dict[int, int] = {}
    active_period: int | None = None
    active_hold = 0

    for _, period in confirmed_entries:
        period_counts[period] = period_counts.get(period, 0) + 1
        if active_period is None:
            active_period = period
            active_hold = 1
            continue
        if period == active_period:
            active_hold += 1
            continue
        hold_lengths.append(active_hold)
        switch_count += 1
        active_period = period
        active_hold = 1
    if active_period is not None and active_hold > 0:
        hold_lengths.append(active_hold)

    average_hold = Decimal("0")
    if hold_lengths:
        average_hold = Decimal(sum(hold_lengths)) / Decimal(len(hold_lengths))
    max_hold = max(hold_lengths, default=0)
    frequencies = tuple(
        AdaptiveRailPeriodFrequency(
            period=period,
            bars=bars,
            share_pct=(Decimal(bars) / Decimal(confirmed_bars)) * Decimal("100"),
        )
        for period, bars in sorted(
            period_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )

    rail_broken_exit_count = sum(1 for trade in trades if trade.exit_reason == "rail_broken")
    rail_broken_exit_pct = (
        Decimal("0")
        if not trades
        else (Decimal(rail_broken_exit_count) / Decimal(len(trades))) * Decimal("100")
    )

    return AdaptiveRailBacktestStats(
        evaluation_bars=evaluation_bars,
        confirmed_bars=confirmed_bars,
        confirmed_coverage_pct=confirmed_coverage_pct,
        broken_state_bars=broken_state_bars,
        broken_state_pct=broken_state_pct,
        dominant_rail_switches=switch_count,
        average_dominant_rail_hold_bars=average_hold,
        max_dominant_rail_hold_bars=max_hold,
        rail_broken_exit_count=rail_broken_exit_count,
        rail_broken_exit_pct=rail_broken_exit_pct,
        dominant_period_frequencies=frequencies,
    )


def _run_dynamic_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
    mtf_filter_bias: list[str] | None = None,
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    entry_reference_ema_period = config.resolved_entry_reference_ema_period()
    trend_slope_filter_enabled = (
        bool(config.trend_ema_slope_filter_enabled)
        and resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode) == "long_only"
    )
    minimum = max(
        config.ema_period,
        config.trend_ema_period,
        config.atr_period,
        entry_reference_ema_period,
    )
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, config.ema_period, config.resolved_ema_type())
    entry_reference_ema_values = (
        ema_values
        if (
            entry_reference_ema_period == config.ema_period
            and config.resolved_entry_reference_ema_type() == config.resolved_ema_type()
        )
        else moving_average(closes, entry_reference_ema_period, config.resolved_entry_reference_ema_type())
    )
    trend_ema_values = moving_average(closes, config.trend_ema_period, config.resolved_trend_ema_type())
    reentry_confirmation_values = (
        moving_average(
            closes,
            config.resolved_reentry_confirmation_ma_period(),
            config.resolved_reentry_confirmation_ma_type(),
        )
        if config.uses_reentry_confirmation()
        else []
    )
    atr_values = atr(candles, config.atr_period)
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    active_plan = None
    current_wave_signal: str | None = None
    entries_in_current_wave = 0
    entry_sequence = 0
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"
    valid_entry_plan_count = 0
    invalid_protection_count = 0

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        closed_round_this_candle = False

        if open_position is not None:
            trend_ema = trend_ema_values[index] if index < len(trend_ema_values) else None
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
                closed_round_this_candle = True
            elif (
                bool(config.trend_ema_close_exit_after_trigger_r_enabled)
                and open_position.signal == "long"
                and trend_ema is not None
                and candle.close <= trend_ema
                and _dynamic_trigger_r_reached(open_position, _trend_ema_close_exit_trigger_r(config))
            ):
                exit_price_raw = candle.close
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
                        exit_reason="trend_ema_close_exit",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None
                closed_round_this_candle = True

        if active_plan is not None and open_position is None:
            filled_position = _try_fill_dynamic_order(
                instrument,
                active_plan,
                candle,
                index,
                entry_fee_rate=maker_fee_rate,
                entry_fee_type="maker",
                entry_slippage_rate=config.resolved_backtest_entry_slippage_rate(),
                exit_slippage_rate=config.resolved_backtest_exit_slippage_rate(),
                funding_rate=config.backtest_funding_rate,
                entry_sequence=entry_sequence + 1,
                wave_entry_sequence=entries_in_current_wave + 1,
                dynamic_take_profit_enabled=dynamic_take_profit_enabled,
                dynamic_exit_fee_rate=taker_fee_rate,
                dynamic_two_r_break_even=config.dynamic_two_r_break_even,
                dynamic_break_even_trigger_r=_dynamic_break_even_trigger_r(config),
                dynamic_first_lock_r=_dynamic_first_lock_r(config),
                dynamic_trailing_step_r=_dynamic_trailing_step_r(config),
                dynamic_separate_break_even_enabled=_dynamic_separate_break_even_enabled(config),
                dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
                dynamic_protection_rules=_dynamic_protection_rules(config),
                time_stop_break_even_enabled=config.time_stop_break_even_enabled,
                time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                next_dynamic_trigger_r=_ema55_slope_lock_profit_trigger_r(config),
                immediate_entry_fee_rate=taker_fee_rate,
                immediate_entry_fee_type="taker",
            )
            active_plan = None
            if filled_position is not None:
                entry_sequence += 1
                entries_in_current_wave += 1
                closed_trade = _try_close_position_same_candle_after_fill(
                    filled_position,
                    candle,
                    index,
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
                if closed_trade is not None:
                    trades.append(closed_trade)
                    closed_round_this_candle = True
                else:
                    open_position = filled_position

        if open_position is not None or closed_round_this_candle or index >= len(candles) - 1:
            continue

        decision = _evaluate_dynamic_signal_precomputed(
            candles,
            index,
            ema_values,
            entry_reference_ema_values,
            trend_ema_values,
            atr_values,
            config,
        )
        if decision.signal is None:
            current_wave_signal = None
            entries_in_current_wave = 0
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue

        if mtf_filter_bias is not None and index < len(mtf_filter_bias):
            if not _direction_filter_allows_signal(mtf_filter_bias[index], decision.signal):
                continue

        if current_wave_signal != decision.signal:
            current_wave_signal = decision.signal
            entries_in_current_wave = 0
        if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
            continue
        next_wave_entry_sequence = entries_in_current_wave + 1
        reentry_confirmation_value = (
            reentry_confirmation_values[index]
            if index < len(reentry_confirmation_values)
            else None
        )
        if _reentry_confirmation_blocks_entry(
            config=config,
            signal=decision.signal,
            wave_entry_sequence=next_wave_entry_sequence,
            candle=candles[index],
            confirmation_value=reentry_confirmation_value,
        ):
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        try:
            active_plan = _build_backtest_order_plan(
                instrument=instrument,
                config=resolved_config,
                order_size=resolved_config.order_size,
                signal=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                signal_candle_high=decision.signal_candle_high,
                signal_candle_low=decision.signal_candle_low,
            )
        except InvalidProtectionPlanError:
            invalid_protection_count += 1
            continue
        valid_entry_plan_count += 1

    terminal_open_position: BacktestOpenPosition | None = None
    if open_position is not None and candles:
        last_candle = candles[-1]
        current_price = last_candle.close
        if open_position.signal == "long":
            gross_pnl = (current_price - open_position.entry_price) * open_position.size
        else:
            gross_pnl = (open_position.entry_price - current_price) * open_position.size
        entry_fee = abs(open_position.entry_price * open_position.size) * open_position.entry_fee_rate
        funding_periods = Decimal(str(max(last_candle.ts - open_position.entry_ts, 0))) / Decimal("28800000")
        funding_cost = abs(open_position.entry_price * open_position.size) * open_position.funding_rate * funding_periods
        pnl = gross_pnl - entry_fee - funding_cost
        risk_value = _position_initial_risk_value(open_position)
        r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
        terminal_open_position = BacktestOpenPosition(
            signal=open_position.signal,
            entry_index=open_position.entry_index,
            entry_ts=open_position.entry_ts,
            current_ts=last_candle.ts,
            entry_price=open_position.entry_price,
            current_price=current_price,
            stop_loss=open_position.stop_loss,
            take_profit=open_position.take_profit,
            initial_stop_loss=open_position.initial_stop_loss,
            initial_take_profit=open_position.initial_take_profit,
            size=open_position.size,
            gross_pnl=gross_pnl,
            pnl=pnl,
            risk_value=risk_value,
            r_multiple=r_multiple,
            entry_fee=entry_fee,
            funding_cost=funding_cost,
            adaptive_rail_period=open_position.adaptive_rail_period,
        )

    _raise_if_only_invalid_protection_configs(
        config=config,
        invalid_protection_count=invalid_protection_count,
        valid_entry_plan_count=valid_entry_plan_count,
    )
    return trades, terminal_open_position


def _build_terminal_open_position(
    open_position: _OpenPosition | None,
    candles: list[Candle],
) -> BacktestOpenPosition | None:
    if open_position is None or not candles:
        return None
    last_candle = candles[-1]
    current_price = last_candle.close
    if open_position.signal == "long":
        gross_pnl = (current_price - open_position.entry_price) * open_position.size
    else:
        gross_pnl = (open_position.entry_price - current_price) * open_position.size
    entry_fee = abs(open_position.entry_price * open_position.size) * open_position.entry_fee_rate
    funding_periods = Decimal(str(max(last_candle.ts - open_position.entry_ts, 0))) / Decimal("28800000")
    funding_cost = abs(open_position.entry_price * open_position.size) * open_position.funding_rate * funding_periods
    pnl = gross_pnl - entry_fee - funding_cost
    risk_value = _position_initial_risk_value(open_position)
    r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
    return BacktestOpenPosition(
        signal=open_position.signal,
        entry_index=open_position.entry_index,
        entry_ts=open_position.entry_ts,
        current_ts=last_candle.ts,
        entry_price=open_position.entry_price,
        current_price=current_price,
        stop_loss=open_position.stop_loss,
        take_profit=open_position.take_profit,
        initial_stop_loss=open_position.initial_stop_loss,
        initial_take_profit=open_position.initial_take_profit,
        size=open_position.size,
        gross_pnl=gross_pnl,
        pnl=pnl,
        risk_value=risk_value,
        r_multiple=r_multiple,
        entry_fee=entry_fee,
        funding_cost=funding_cost,
        adaptive_rail_period=open_position.adaptive_rail_period,
    )


def _evaluate_cross_signal_precomputed(
    candles: list[Candle],
    index: int,
    ema_values: list[Decimal | None],
    reference_ema_values: list[Decimal | None],
    trend_ema_values: list[Decimal | None],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> SignalDecision:
    previous_candle = candles[index - 1]
    current_candle = candles[index]
    previous_reference_ema = reference_ema_values[index - 1]
    current_reference_ema = reference_ema_values[index]
    current_atr = atr_values[index]
    ema_small = ema_values[index]
    ema_medium = trend_ema_values[index]
    if (
        previous_reference_ema is None
        or current_reference_ema is None
        or ema_small is None
        or ema_medium is None
    ):
        return SignalDecision(
            signal=None,
            reason="moving_average_not_ready",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=None,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
    if current_atr is None:
        return SignalDecision(
            signal=None,
            reason="atr_not_ready",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=current_reference_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if (
        config.ema_period == config.trend_ema_period
        and config.resolved_ema_type() == config.resolved_trend_ema_type()
    ):
        ema_bias_allows_long = True
        ema_bias_allows_short = True
    else:
        ema_bias_allows_long = ema_small > ema_medium
        ema_bias_allows_short = ema_small < ema_medium

    long_breakout = previous_candle.close <= previous_reference_ema and current_candle.close > current_reference_ema
    short_breakdown = previous_candle.close >= previous_reference_ema and current_candle.close < current_reference_ema

    if long_breakout and config.signal_mode != "short_only":
        if not ema_bias_allows_long:
            return SignalDecision(
                signal=None,
                reason="fast_ema_below_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="long",
            reason="cross_long",
            candle_ts=current_candle.ts,
            entry_reference=current_candle.close,
            atr_value=current_atr,
            ema_value=current_reference_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if short_breakdown and config.signal_mode != "long_only":
        if not ema_bias_allows_short:
            return SignalDecision(
                signal=None,
                reason="fast_ema_above_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="short",
            reason="cross_short",
            candle_ts=current_candle.ts,
            entry_reference=current_candle.close,
            atr_value=current_atr,
            ema_value=current_reference_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    return SignalDecision(
        signal=None,
        reason="no_cross_signal",
        candle_ts=current_candle.ts,
        entry_reference=None,
        atr_value=current_atr,
        ema_value=current_reference_ema,
        signal_candle_high=current_candle.high,
        signal_candle_low=current_candle.low,
    )


def _evaluate_dynamic_signal_precomputed(
    candles: list[Candle],
    index: int,
    ema_values: list[Decimal | None],
    entry_reference_ema_values: list[Decimal | None],
    trend_ema_values: list[Decimal | None],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> SignalDecision:
    current_candle = candles[index]
    current_ema = ema_values[index]
    current_entry_reference = entry_reference_ema_values[index]
    trend_ema = trend_ema_values[index]
    effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
    trend_slope_filter_enabled = bool(config.trend_ema_slope_filter_enabled) and effective_signal_mode in {"long_only", "short_only"}
    trend_slope_lookback = max(2, int(config.trend_ema_slope_filter_lookback_bars))
    trend_slope_min_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    trend_window = (
        trend_ema_values[index - trend_slope_lookback + 1 : index + 1]
        if trend_slope_filter_enabled and index >= trend_slope_lookback - 1
        else []
    )
    trend_window_ready = bool(trend_window) and all(value is not None for value in trend_window)
    trend_slope = (
        linear_regression_slope([value for value in trend_window if value is not None])
        if trend_window_ready
        else None
    )
    trend_slope_ratio = (
        trend_slope / trend_ema
        if trend_slope is not None and trend_ema is not None and trend_ema != 0
        else None
    )
    current_atr = atr_values[index]
    if (
        current_ema is None
        or current_entry_reference is None
        or trend_ema is None
    ):
        return SignalDecision(
            signal=None,
            reason="moving_average_not_ready",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=None,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
    if current_atr is None:
        return SignalDecision(
            signal=None,
            reason="atr_not_ready",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if effective_signal_mode == "long_only":
        if current_ema <= trend_ema:
            return SignalDecision(
                signal=None,
                reason="fast_ema_below_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if current_candle.close <= trend_ema:
            return SignalDecision(
                signal=None,
                reason="close_below_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if (
            trend_slope_filter_enabled
            and trend_slope_ratio is not None
            and trend_slope_ratio < trend_slope_min_ratio
        ):
            return SignalDecision(
                signal=None,
                reason="trend_ema_negative_regression_slope",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="long",
            reason="dynamic_long",
            candle_ts=current_candle.ts,
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if effective_signal_mode == "short_only":
        if current_ema >= trend_ema:
            return SignalDecision(
                signal=None,
                reason="fast_ema_above_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if current_candle.close >= trend_ema:
            return SignalDecision(
                signal=None,
                reason="close_above_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if (
            trend_slope_filter_enabled
            and trend_slope_ratio is not None
            and trend_slope_ratio > abs(trend_slope_min_ratio)
        ):
            return SignalDecision(
                signal=None,
                reason="trend_ema_positive_regression_slope",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="short",
            reason="dynamic_short",
            candle_ts=current_candle.ts,
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    return SignalDecision(
        signal=None,
        reason="unsupported_signal_mode",
        candle_ts=current_candle.ts,
        entry_reference=None,
        atr_value=current_atr,
        ema_value=current_ema,
        signal_candle_high=current_candle.high,
        signal_candle_low=current_candle.low,
    )


def _realized_pnl(trades: list[BacktestTrade]) -> Decimal:
    return sum((trade.pnl for trade in trades), Decimal("0"))


def _base_equity_for_sizing(config: StrategyConfig, trades: list[BacktestTrade]) -> Decimal:
    if config.backtest_compounding:
        return config.backtest_initial_capital + _realized_pnl(trades)
    return config.backtest_initial_capital


def _resolve_backtest_config(config: StrategyConfig, trades: list[BacktestTrade]) -> StrategyConfig:
    if config.backtest_sizing_mode == "fixed_size":
        return replace(config, risk_amount=None)

    if config.backtest_sizing_mode == "risk_percent":
        if config.backtest_risk_percent is None or config.backtest_risk_percent <= 0:
            raise RuntimeError("风险百分比模式下，风险百分比必须大于 0")
        base_equity = _base_equity_for_sizing(config, trades)
        if base_equity <= 0:
            raise RuntimeError("当前权益小于等于 0，无法继续按风险百分比回测。")
        risk_amount = base_equity * config.backtest_risk_percent / Decimal("100")
        return replace(config, risk_amount=risk_amount, order_size=Decimal("0"))

    if config.risk_amount is None or config.risk_amount <= 0:
        raise RuntimeError("固定风险模式下，风险金必须大于 0")
    return replace(config, risk_amount=config.risk_amount, order_size=Decimal("0"))


def _apply_slippage_price(
    price: Decimal,
    *,
    signal: str,
    tick_size: Decimal,
    slippage_rate: Decimal,
    is_entry: bool,
) -> Decimal:
    if slippage_rate <= 0:
        return price
    if signal == "long":
        raw_price = price * (Decimal("1") + slippage_rate) if is_entry else price * (Decimal("1") - slippage_rate)
        direction = "up" if is_entry else "down"
    else:
        raw_price = price * (Decimal("1") - slippage_rate) if is_entry else price * (Decimal("1") + slippage_rate)
        direction = "down" if is_entry else "up"
    return snap_to_increment(raw_price, tick_size, direction)


def _create_open_position(
    *,
    instrument: Instrument,
    signal: str,
    entry_index: int,
    entry_ts: int,
    entry_price_raw: Decimal,
    stop_loss: Decimal,
    take_profit: Decimal,
    atr_value: Decimal,
    size: Decimal,
    entry_fee_rate: Decimal,
    exit_fee_rate: Decimal,
    entry_fee_type: str,
    entry_slippage_rate: Decimal,
    exit_slippage_rate: Decimal,
    funding_rate: Decimal,
    entry_sequence: int = 0,
    wave_entry_sequence: int = 0,
    dynamic_take_profit_enabled: bool = False,
    take_profit_enabled: bool = True,
    dynamic_exit_fee_rate: Decimal = Decimal("0"),
    dynamic_two_r_break_even: bool = False,
    dynamic_break_even_trigger_r: int = 2,
    dynamic_first_lock_r: int = 0,
    dynamic_trailing_step_r: int = 1,
    dynamic_separate_break_even_enabled: bool = True,
    dynamic_fee_offset_enabled: bool = True,
    dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = (),
    time_stop_break_even_enabled: bool = False,
    time_stop_break_even_bars: int = 0,
    next_dynamic_trigger_r: int = 2,
    current_take_profit: Decimal | None = None,
    filled_entry_price: Decimal | None = None,
    entry_path_price: Decimal | None = None,
    apply_entry_slippage: bool = True,
    adaptive_rail_period: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> _OpenPosition:
    strategy_entry_price = entry_price_raw
    if filled_entry_price is not None:
        entry_price = snap_to_increment(filled_entry_price, instrument.tick_size, "nearest")
    elif apply_entry_slippage:
        entry_price = _apply_slippage_price(
            entry_price_raw,
            signal=signal,
            tick_size=instrument.tick_size,
            slippage_rate=entry_slippage_rate,
            is_entry=True,
        )
    else:
        entry_price = strategy_entry_price
    execution_path_price = (
        snap_to_increment(entry_path_price, instrument.tick_size, "nearest")
        if entry_path_price is not None
        else strategy_entry_price
    )
    risk_per_unit = abs(strategy_entry_price - stop_loss)
    display_take_profit = take_profit if current_take_profit is None else current_take_profit
    open_position = _OpenPosition(
        signal=signal,
        entry_index=entry_index,
        entry_ts=entry_ts,
        entry_price=entry_price,
        entry_price_raw=entry_price_raw,
        entry_path_price=execution_path_price,
        stop_loss=stop_loss,
        take_profit=display_take_profit,
        initial_stop_loss=stop_loss,
        initial_take_profit=take_profit,
        atr_value=atr_value,
        size=size,
        risk_per_unit=risk_per_unit,
        tick_size=instrument.tick_size,
        entry_sequence=entry_sequence,
        wave_entry_sequence=wave_entry_sequence,
        dynamic_take_profit_enabled=dynamic_take_profit_enabled,
        take_profit_enabled=take_profit_enabled,
        next_dynamic_trigger_r=next_dynamic_trigger_r,
        dynamic_protection_rules=dynamic_protection_rules,
        dynamic_trailing_start_r=max(int(next_dynamic_trigger_r), 2),
        dynamic_break_even_trigger_r=max(int(dynamic_break_even_trigger_r), 1),
        dynamic_first_lock_r=max(int(dynamic_first_lock_r), 0),
        dynamic_trailing_step_r=max(int(dynamic_trailing_step_r), 1),
        dynamic_separate_break_even_enabled=bool(dynamic_separate_break_even_enabled),
        dynamic_exit_fee_rate=dynamic_exit_fee_rate,
        dynamic_two_r_break_even=dynamic_two_r_break_even,
        dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        time_stop_break_even_enabled=time_stop_break_even_enabled,
        time_stop_break_even_bars=max(int(time_stop_break_even_bars), 0),
        entry_fee_rate=entry_fee_rate,
        estimated_exit_fee_rate=exit_fee_rate,
        entry_fee_type=entry_fee_type,
        entry_slippage_cost=abs(entry_price - strategy_entry_price) * abs(size) if apply_entry_slippage else Decimal("0"),
        entry_slippage_rate=entry_slippage_rate,
        exit_slippage_rate=exit_slippage_rate,
        slippage_rate=exit_slippage_rate,
        funding_rate=funding_rate if instrument.inst_type == "SWAP" else Decimal("0"),
        adaptive_rail_period=adaptive_rail_period,
        metadata=dict(metadata or {}),
    )
    if dynamic_protection_rules:
        open_position.dynamic_next_rule_index = 0
        open_position.dynamic_active_rule_index = -1
        open_position.dynamic_next_trailing_trigger_r = 0
        open_position.dynamic_active_lock_r = None
        open_position.dynamic_last_processed_trigger_r = 0
        _sync_dynamic_next_event(open_position)
    elif current_take_profit is None and dynamic_take_profit_enabled:
        open_position.take_profit = _dynamic_trigger_price(open_position, next_dynamic_trigger_r)
    return open_position


def _try_close_take_profit_only(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade | None:
    take_profit_hit = candle.high >= position.take_profit if position.signal == "long" else candle.low <= position.take_profit
    if not take_profit_hit:
        return None
    exit_price_raw = position.take_profit
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
        exit_reason="take_profit",
        exit_fee_rate=exit_fee_rate,
        exit_fee_type=exit_fee_type,
    )


def _try_close_slot_position_on_signal(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    invalidation_reason: str,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> tuple[BacktestTrade | None, _ManualPosition | None]:
    exit_price_raw = candle.close
    exit_price = _apply_slippage_price(
        exit_price_raw,
        signal=position.signal,
        tick_size=position.tick_size,
        slippage_rate=position.exit_slippage_rate,
        is_entry=False,
    )
    candidate_trade = _build_closed_trade(
        position,
        candle,
        candle_index,
        exit_price_raw=exit_price_raw,
        exit_price=exit_price,
        exit_reason="signal_profit_exit",
        exit_fee_rate=exit_fee_rate,
        exit_fee_type=exit_fee_type,
    )
    if candidate_trade.pnl > 0:
        return candidate_trade, None
    return None, _ManualPosition(
        position=position,
        handoff_index=candle_index,
        handoff_ts=candle.ts,
        handoff_price_raw=exit_price_raw,
        handoff_reason=invalidation_reason,
    )


def _build_manual_backtest_position(position: _ManualPosition, current_candle: Candle) -> BacktestManualPosition:
    open_position = position.position
    current_price = current_candle.close
    if open_position.signal == "long":
        gross_pnl = (current_price - open_position.entry_price) * open_position.size
    else:
        gross_pnl = (open_position.entry_price - current_price) * open_position.size
    entry_fee = abs(open_position.entry_price * open_position.size) * open_position.entry_fee_rate
    funding_periods = Decimal(str(max(current_candle.ts - open_position.entry_ts, 0))) / Decimal("28800000")
    funding_cost = abs(open_position.entry_price * open_position.size) * open_position.funding_rate * funding_periods
    pnl = gross_pnl - entry_fee - funding_cost
    risk_value = _position_initial_risk_value(open_position)
    r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
    break_even_price = _estimate_manual_break_even_price(
        open_position,
        entry_fee=entry_fee,
        funding_cost=funding_cost,
    )
    return BacktestManualPosition(
        signal=open_position.signal,
        entry_index=open_position.entry_index,
        handoff_index=position.handoff_index,
        entry_ts=open_position.entry_ts,
        handoff_ts=position.handoff_ts,
        current_ts=current_candle.ts,
        entry_price=open_position.entry_price,
        handoff_price=position.handoff_price_raw,
        current_price=current_price,
        stop_loss=open_position.initial_stop_loss,
        take_profit=open_position.initial_take_profit,
        size=open_position.size,
        gross_pnl=gross_pnl,
        pnl=pnl,
        risk_value=risk_value,
        r_multiple=r_multiple,
        break_even_price=break_even_price,
        handoff_reason=position.handoff_reason,
        atr_value=open_position.atr_value,
        entry_sequence=open_position.entry_sequence,
        entry_fee=entry_fee,
        funding_cost=funding_cost,
    )


def _estimate_manual_break_even_price(
    position: _OpenPosition,
    *,
    entry_fee: Decimal,
    funding_cost: Decimal,
) -> Decimal:
    size = abs(position.size)
    if size <= 0:
        return position.entry_price
    carrying_cost_per_unit = (entry_fee + funding_cost) / size
    exit_fee_rate = position.estimated_exit_fee_rate
    slippage_rate = position.exit_slippage_rate

    if position.signal == "long":
        multiplier = (Decimal("1") - slippage_rate) * (Decimal("1") - exit_fee_rate)
        if multiplier <= 0:
            return position.entry_price
        raw_price = (position.entry_price + carrying_cost_per_unit) / multiplier
        return snap_to_increment(raw_price, position.tick_size, "up")

    multiplier = (Decimal("1") + slippage_rate) * (Decimal("1") + exit_fee_rate)
    if multiplier <= 0:
        return position.entry_price
    raw_price = (position.entry_price - carrying_cost_per_unit) / multiplier
    if raw_price <= 0:
        return position.tick_size
    return snap_to_increment(raw_price, position.tick_size, "down")


def _try_fill_dynamic_order(
    instrument: Instrument,
    plan,
    candle: Candle,
    candle_index: int,
    *,
    entry_fee_rate: Decimal = Decimal("0"),
    entry_fee_type: str = "none",
    entry_slippage_rate: Decimal = Decimal("0"),
    exit_slippage_rate: Decimal = Decimal("0"),
    funding_rate: Decimal = Decimal("0"),
    entry_sequence: int = 0,
    wave_entry_sequence: int = 0,
    dynamic_take_profit_enabled: bool = False,
    dynamic_exit_fee_rate: Decimal = Decimal("0"),
    dynamic_two_r_break_even: bool = False,
    dynamic_break_even_trigger_r: int = 2,
    dynamic_first_lock_r: int = 0,
    dynamic_trailing_step_r: int = 1,
    dynamic_separate_break_even_enabled: bool = True,
    dynamic_fee_offset_enabled: bool = True,
    dynamic_protection_rules: tuple[DynamicProtectionRule, ...] = (),
    time_stop_break_even_enabled: bool = False,
    time_stop_break_even_bars: int = 0,
    next_dynamic_trigger_r: int = 2,
    immediate_entry_fee_rate: Decimal = Decimal("0"),
    immediate_entry_fee_type: str = "none",
    adaptive_rail_period: int | None = None,
) -> _OpenPosition | None:
    open_price = snap_to_increment(candle.open, instrument.tick_size, "nearest")
    marketable_at_open = (
        plan.signal == "long" and open_price <= plan.entry_reference
    ) or (
        plan.signal == "short" and open_price >= plan.entry_reference
    )
    if marketable_at_open:
        fill_price = open_price
        fill_entry_fee_rate = immediate_entry_fee_rate
        fill_entry_fee_type = immediate_entry_fee_type
        fill_entry_path_price = open_price
    else:
        filled = candle.low <= plan.entry_reference <= candle.high
        if not filled:
            return None
        fill_price = plan.entry_reference
        fill_entry_fee_rate = entry_fee_rate
        fill_entry_fee_type = entry_fee_type
        fill_entry_path_price = plan.entry_reference

    return _create_open_position(
        instrument=instrument,
        signal=plan.signal,
        entry_index=candle_index,
        entry_ts=candle.ts,
        entry_price_raw=plan.entry_reference,
        filled_entry_price=fill_price,
        entry_path_price=fill_entry_path_price,
        stop_loss=plan.stop_loss,
        take_profit=plan.take_profit,
        atr_value=plan.atr_value,
        size=plan.size,
        entry_fee_rate=fill_entry_fee_rate,
        exit_fee_rate=dynamic_exit_fee_rate,
        entry_fee_type=fill_entry_fee_type,
        entry_slippage_rate=entry_slippage_rate,
        exit_slippage_rate=exit_slippage_rate,
        funding_rate=funding_rate,
        entry_sequence=entry_sequence,
        wave_entry_sequence=wave_entry_sequence,
        dynamic_take_profit_enabled=dynamic_take_profit_enabled,
        dynamic_exit_fee_rate=dynamic_exit_fee_rate,
        dynamic_two_r_break_even=dynamic_two_r_break_even,
        dynamic_break_even_trigger_r=dynamic_break_even_trigger_r,
        dynamic_first_lock_r=dynamic_first_lock_r,
        dynamic_trailing_step_r=dynamic_trailing_step_r,
        dynamic_separate_break_even_enabled=dynamic_separate_break_even_enabled,
        dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        dynamic_protection_rules=dynamic_protection_rules,
        time_stop_break_even_enabled=time_stop_break_even_enabled,
        time_stop_break_even_bars=time_stop_break_even_bars,
        next_dynamic_trigger_r=next_dynamic_trigger_r,
        apply_entry_slippage=False,
        adaptive_rail_period=adaptive_rail_period,
    )


def _segment_contains_price(start: Decimal, end: Decimal, price: Decimal) -> bool:
    return min(start, end) <= price <= max(start, end)


def _first_touched_exit_on_segment(
    start: Decimal,
    end: Decimal,
    *,
    stop_loss: Decimal,
    take_profit: Decimal,
) -> tuple[Decimal, str] | None:
    if end > start:
        touched: list[tuple[Decimal, str]] = []
        if start <= stop_loss <= end:
            touched.append((stop_loss, "stop_loss"))
        if start <= take_profit <= end:
            touched.append((take_profit, "take_profit"))
        if not touched:
            return None
        return min(touched, key=lambda item: item[0])
    if end < start:
        touched = []
        if end <= stop_loss <= start:
            touched.append((stop_loss, "stop_loss"))
        if end <= take_profit <= start:
            touched.append((take_profit, "take_profit"))
        if not touched:
            return None
        return max(touched, key=lambda item: item[0])
    if stop_loss == start:
        return stop_loss, "stop_loss"
    if take_profit == start:
        return take_profit, "take_profit"
    return None


def _same_candle_path_points(candle: Candle) -> tuple[Decimal, ...] | None:
    if candle.close > candle.open:
        return candle.open, candle.low, candle.high, candle.close
    if candle.close < candle.open:
        return candle.open, candle.high, candle.low, candle.close
    return None


def _candle_path_points(candle: Candle) -> tuple[Decimal, ...]:
    if candle.close >= candle.open:
        return candle.open, candle.low, candle.high, candle.close
    return candle.open, candle.high, candle.low, candle.close


def _dynamic_fee_offset(entry_price: Decimal, exit_fee_rate: Decimal, *, enabled: bool = True) -> Decimal:
    if not enabled or exit_fee_rate <= 0:
        return Decimal("0")
    return abs(entry_price) * exit_fee_rate * Decimal("2")


def _time_stop_break_even_price(position: _OpenPosition) -> Decimal:
    return _dynamic_break_even_price(position)


def _dynamic_break_even_price(position: _OpenPosition) -> Decimal:
    entry_price = _position_strategy_entry_price(position)
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = entry_price + fee_offset if position.signal == "long" else entry_price - fee_offset
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _dynamic_rule_base_lock_r(rule: DynamicProtectionRule) -> int:
    return 0 if rule.resolved_action() == "break_even" else rule.resolved_lock_r()


def _dynamic_rule_stop_price_for_lock_r(position: _OpenPosition, lock_r: int) -> Decimal:
    entry_price = _position_strategy_entry_price(position)
    locked_offset = position.risk_per_unit * Decimal(str(max(int(lock_r), 0)))
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = (
        entry_price + locked_offset + fee_offset
        if position.signal == "long"
        else entry_price - locked_offset - fee_offset
    )
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _dynamic_rule_lock_r(rule: DynamicProtectionRule, trigger_r: int) -> int:
    lock_r = _dynamic_rule_base_lock_r(rule)
    if not rule.trailing_enabled():
        return lock_r
    if trigger_r <= rule.resolved_trigger_r():
        return lock_r
    step_count = max(trigger_r - rule.resolved_trigger_r(), 0) // rule.resolved_trail_every_r()
    return max(lock_r + step_count * rule.resolved_trail_add_r(), 0)


def _dynamic_rule_trigger_price(position: _OpenPosition, rule: DynamicProtectionRule | int) -> Decimal:
    trigger_r = rule if isinstance(rule, int) else rule.resolved_trigger_r()
    return _dynamic_trigger_price(position, trigger_r)


def _dynamic_rule_fires_at(rule: DynamicProtectionRule, trigger_r: int) -> bool:
    current_trigger = max(int(trigger_r), 1)
    base_trigger = rule.resolved_trigger_r()
    if current_trigger < base_trigger:
        return False
    if current_trigger == base_trigger:
        return True
    if not rule.trailing_enabled():
        return False
    return (current_trigger - base_trigger) % rule.resolved_trail_every_r() == 0


def _next_dynamic_rule_event_after(rule: DynamicProtectionRule, trigger_r: int) -> int:
    current_trigger = max(int(trigger_r), 0)
    base_trigger = rule.resolved_trigger_r()
    if current_trigger < base_trigger:
        return base_trigger
    if not rule.trailing_enabled():
        return 0
    trail_every_r = rule.resolved_trail_every_r()
    step_count = ((current_trigger - base_trigger) // trail_every_r) + 1
    return base_trigger + (step_count * trail_every_r)


def _dynamic_rules_enabled(position: _OpenPosition) -> bool:
    return bool(position.dynamic_take_profit_enabled and position.dynamic_protection_rules)


def _sync_dynamic_next_event(position: _OpenPosition) -> None:
    if _dynamic_rules_enabled(position):
        next_event_r = 0
        current_trigger = max(int(position.dynamic_last_processed_trigger_r), 0)
        for rule in position.dynamic_protection_rules:
            candidate = _next_dynamic_rule_event_after(rule, current_trigger)
            if candidate <= 0:
                continue
            if next_event_r <= 0 or candidate < next_event_r:
                next_event_r = candidate
        position.next_dynamic_trigger_r = next_event_r
        if position.next_dynamic_trigger_r > 0:
            position.take_profit = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)
        return
    next_rule_r: int | None = None
    if 0 <= position.dynamic_next_rule_index < len(position.dynamic_protection_rules):
        next_rule_r = position.dynamic_protection_rules[position.dynamic_next_rule_index].resolved_trigger_r()
    next_trailing_r = position.dynamic_next_trailing_trigger_r if position.dynamic_next_trailing_trigger_r > 0 else None
    candidates = [value for value in (next_rule_r, next_trailing_r) if value is not None]
    position.next_dynamic_trigger_r = min(candidates) if candidates else 0
    if position.next_dynamic_trigger_r > 0:
        position.take_profit = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)


def _dynamic_apply_rule_event(position: _OpenPosition, trigger_r: int) -> bool:
    best_rule_index = -1
    best_lock_r: int | None = None
    best_candidate: Decimal | None = None
    for rule_index, rule in enumerate(position.dynamic_protection_rules):
        if not _dynamic_rule_fires_at(rule, trigger_r):
            continue
        lock_r = _dynamic_rule_lock_r(rule, trigger_r)
        candidate = _dynamic_rule_stop_price_for_lock_r(position, lock_r)
        if best_candidate is None:
            best_rule_index = rule_index
            best_lock_r = lock_r
            best_candidate = candidate
            continue
        if position.signal == "long":
            if candidate > best_candidate or (candidate == best_candidate and lock_r > max(int(best_lock_r or 0), 0)):
                best_rule_index = rule_index
                best_lock_r = lock_r
                best_candidate = candidate
        else:
            if candidate < best_candidate or (candidate == best_candidate and lock_r > max(int(best_lock_r or 0), 0)):
                best_rule_index = rule_index
                best_lock_r = lock_r
                best_candidate = candidate
    position.dynamic_last_processed_trigger_r = max(int(position.dynamic_last_processed_trigger_r), int(trigger_r))
    if best_candidate is None or best_lock_r is None:
        return False
    moved = False
    if position.signal == "long":
        if best_candidate > position.stop_loss:
            position.stop_loss = best_candidate
            moved = True
    else:
        if best_candidate < position.stop_loss:
            position.stop_loss = best_candidate
            moved = True
    if moved or best_candidate == position.stop_loss:
        position.dynamic_active_rule_index = best_rule_index
        position.dynamic_active_lock_r = best_lock_r
    return moved


def _dynamic_stop_exit_reason(position: _OpenPosition) -> str:
    if not position.dynamic_take_profit_enabled:
        return "stop_loss"
    if position.stop_loss == position.initial_stop_loss:
        return "stop_loss"
    if _dynamic_rules_enabled(position):
        if position.dynamic_active_lock_r is None:
            return "stop_loss"
        return "break_even_stop" if position.dynamic_active_lock_r <= 0 else f"locked_{position.dynamic_active_lock_r}r_stop"
    if position.time_stop_break_even_enabled and position.stop_loss == _time_stop_break_even_price(position):
        return "break_even_stop"
    if position.dynamic_two_r_break_even and position.stop_loss == _dynamic_break_even_price(position):
        return "break_even_stop"
    for trigger_r in range(2, position.next_dynamic_trigger_r):
        if position.stop_loss != _dynamic_stop_price(position, trigger_r):
            continue
        locked_r = _dynamic_locked_r(position, trigger_r)
        return "break_even_stop" if locked_r <= 0 else f"locked_{locked_r}r_stop"
    return "stop_loss"


def _holding_bars_for_position(position: _OpenPosition, candle_index: int) -> int:
    return max(candle_index - position.entry_index, 0)


def _dynamic_trigger_r_reached(position: _OpenPosition, trigger_r: int) -> bool:
    if not position.dynamic_take_profit_enabled:
        return False
    resolved_trigger_r = max(int(trigger_r), 1)
    if _dynamic_rules_enabled(position):
        return int(position.dynamic_last_processed_trigger_r) >= resolved_trigger_r
    return int(position.next_dynamic_trigger_r) > resolved_trigger_r


def _apply_time_stop_break_even(position: _OpenPosition, current_price: Decimal, holding_bars: int) -> bool:
    if not position.time_stop_break_even_enabled or position.time_stop_break_even_bars <= 0:
        return False
    if holding_bars < position.time_stop_break_even_bars:
        return False
    candidate = _time_stop_break_even_price(position)
    if position.signal == "long":
        if current_price < candidate or candidate <= position.stop_loss:
            return False
        position.stop_loss = candidate
        position.dynamic_active_lock_r = 0
        return True
    if current_price > candidate or candidate >= position.stop_loss:
        return False
    position.stop_loss = candidate
    position.dynamic_active_lock_r = 0
    return True


def _position_strategy_entry_price(position: _OpenPosition) -> Decimal:
    return position.entry_price_raw if position.entry_price_raw > 0 else position.entry_price


def _position_entry_path_price(position: _OpenPosition) -> Decimal:
    return position.entry_path_price if position.entry_path_price > 0 else _position_strategy_entry_price(position)


def _position_initial_risk_value(position: _OpenPosition) -> Decimal:
    strategy_entry_price = _position_strategy_entry_price(position)
    return abs(strategy_entry_price - position.initial_stop_loss) * position.size


def _dynamic_trigger_price(position: _OpenPosition, trigger_r: int) -> Decimal:
    entry_price = _position_strategy_entry_price(position)
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    offset = (position.risk_per_unit * Decimal(str(trigger_r))) + fee_offset
    raw = entry_price + offset if position.signal == "long" else entry_price - offset
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _dynamic_stop_price(position: _OpenPosition, trigger_r: int) -> Decimal:
    if _dynamic_rules_enabled(position):
        for rule in position.dynamic_protection_rules:
            if rule.resolved_trigger_r() == trigger_r:
                return _dynamic_rule_stop_price_for_lock_r(position, _dynamic_rule_lock_r(rule, trigger_r))
        if 0 <= position.dynamic_active_rule_index < len(position.dynamic_protection_rules):
            active_rule = position.dynamic_protection_rules[position.dynamic_active_rule_index]
            return _dynamic_rule_stop_price_for_lock_r(position, _dynamic_rule_lock_r(active_rule, trigger_r))
    entry_price = _position_strategy_entry_price(position)
    lock_multiple = _dynamic_locked_r(position, trigger_r)
    locked_offset = position.risk_per_unit * Decimal(str(lock_multiple))
    fee_offset = _dynamic_fee_offset(
        entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = (
        entry_price + locked_offset + fee_offset
        if position.signal == "long"
        else entry_price - locked_offset - fee_offset
    )
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _dynamic_break_even_lock_trigger_matches(position: _OpenPosition, trigger_r: int) -> bool:
    if not position.dynamic_two_r_break_even:
        return False
    if position.dynamic_separate_break_even_enabled:
        return trigger_r == max(int(position.dynamic_break_even_trigger_r), 1)
    return trigger_r == 2


def _dynamic_effective_first_lock_r(position: _OpenPosition) -> int:
    first_lock_r = max(int(position.dynamic_first_lock_r), 0)
    if first_lock_r > 0:
        return first_lock_r
    trailing_start_r = max(int(position.dynamic_trailing_start_r), 2)
    trailing_step_r = max(int(position.dynamic_trailing_step_r), 1)
    return max(trailing_start_r - trailing_step_r, 0)


def _dynamic_locked_r(position: _OpenPosition, trigger_r: int) -> int:
    if _dynamic_break_even_lock_trigger_matches(position, trigger_r):
        return 0
    trailing_start_r = max(int(position.dynamic_trailing_start_r), 2)
    trailing_step_r = max(int(position.dynamic_trailing_step_r), 1)
    effective_first_lock_r = _dynamic_effective_first_lock_r(position)
    if trigger_r <= trailing_start_r:
        return effective_first_lock_r
    trigger_offset = max(trigger_r - trailing_start_r, 0)
    step_count = trigger_offset // trailing_step_r
    return max(effective_first_lock_r + step_count * trailing_step_r, 0)


def _apply_dynamic_break_even(position: _OpenPosition, favorable_price: Decimal) -> bool:
    if _dynamic_rules_enabled(position):
        return False
    if (
        not position.dynamic_take_profit_enabled
        or not position.dynamic_two_r_break_even
        or not position.dynamic_separate_break_even_enabled
    ):
        return False
    trigger_r = max(int(position.dynamic_break_even_trigger_r), 1)
    trigger_price = _dynamic_trigger_price(position, trigger_r)
    if position.signal == "long":
        if favorable_price < trigger_price:
            return False
        candidate = _dynamic_break_even_price(position)
        if candidate <= position.stop_loss:
            return False
        position.stop_loss = candidate
        position.dynamic_active_lock_r = 0
        return True
    if favorable_price > trigger_price:
        return False
    candidate = _dynamic_break_even_price(position)
    if candidate >= position.stop_loss:
        return False
    position.stop_loss = candidate
    position.dynamic_active_lock_r = 0
    return True


def _advance_dynamic_stop(position: _OpenPosition, favorable_price: Decimal, *, holding_bars: int = 0) -> None:
    _apply_time_stop_break_even(position, favorable_price, holding_bars)
    if _dynamic_rules_enabled(position):
        while True:
            event_r = position.next_dynamic_trigger_r if position.next_dynamic_trigger_r > 0 else None
            if event_r is None:
                break
            trigger_price = _dynamic_trigger_price(position, event_r)
            if position.signal == "long":
                if favorable_price < trigger_price:
                    break
            elif favorable_price > trigger_price:
                break
            _dynamic_apply_rule_event(position, event_r)
            _sync_dynamic_next_event(position)
        return
    _apply_dynamic_break_even(position, favorable_price)
    while position.next_dynamic_trigger_r >= 2:
        trigger_price = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)
        if position.signal == "long":
            if favorable_price < trigger_price:
                break
            candidate_stop = _dynamic_stop_price(position, position.next_dynamic_trigger_r)
            if candidate_stop > position.stop_loss:
                position.stop_loss = candidate_stop
            position.next_dynamic_trigger_r += max(int(position.dynamic_trailing_step_r), 1)
        else:
            if favorable_price > trigger_price:
                break
            candidate_stop = _dynamic_stop_price(position, position.next_dynamic_trigger_r)
            if candidate_stop < position.stop_loss:
                position.stop_loss = candidate_stop
            position.next_dynamic_trigger_r += max(int(position.dynamic_trailing_step_r), 1)
    position.take_profit = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)


def _process_dynamic_position_segment(
    position: _OpenPosition,
    start: Decimal,
    end: Decimal,
    *,
    holding_bars: int = 0,
) -> tuple[Decimal, str] | None:
    _apply_time_stop_break_even(position, start, holding_bars)
    if position.signal == "long":
        if end < start:
            if _segment_contains_price(start, end, position.stop_loss):
                return position.stop_loss, _dynamic_stop_exit_reason(position)
            return None
        _advance_dynamic_stop(position, end, holding_bars=holding_bars)
        return None
    if end > start:
        if _segment_contains_price(start, end, position.stop_loss):
            return position.stop_loss, _dynamic_stop_exit_reason(position)
        return None
    _advance_dynamic_stop(position, end, holding_bars=holding_bars)
    return None


def _build_closed_trade(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_price_raw: Decimal,
    exit_price: Decimal,
    exit_reason: str,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade:
    metadata = dict(position.metadata or {})
    if position.signal == "long":
        gross_pnl = (exit_price - position.entry_price) * position.size
    else:
        gross_pnl = (position.entry_price - exit_price) * position.size
    entry_fee = abs(position.entry_price * position.size) * position.entry_fee_rate
    exit_fee = abs(exit_price * position.size) * exit_fee_rate
    total_fee = entry_fee + exit_fee
    funding_periods = Decimal(str(max(candle.ts - position.entry_ts, 0))) / Decimal("28800000")
    funding_cost = abs(position.entry_price * position.size) * position.funding_rate * funding_periods
    pnl = gross_pnl - total_fee - funding_cost
    risk_value = _position_initial_risk_value(position)
    r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
    slippage_cost = position.entry_slippage_cost + (abs(exit_price - exit_price_raw) * abs(position.size))
    return BacktestTrade(
        signal=position.signal,
        entry_index=position.entry_index,
        exit_index=candle_index,
        entry_ts=position.entry_ts,
        exit_ts=candle.ts,
        entry_price=position.entry_price,
        exit_price=exit_price,
        stop_loss=position.initial_stop_loss,
        take_profit=position.initial_take_profit,
        size=position.size,
        gross_pnl=gross_pnl,
        pnl=pnl,
        risk_value=risk_value,
        r_multiple=r_multiple,
        exit_reason=exit_reason,
        atr_value=position.atr_value,
        entry_sequence=position.entry_sequence,
        wave_entry_sequence=position.wave_entry_sequence,
        entry_fee=entry_fee,
        exit_fee=exit_fee,
        total_fee=total_fee,
        entry_fee_type=position.entry_fee_type,
        exit_fee_type=exit_fee_type,
        slippage_cost=slippage_cost,
        funding_cost=funding_cost,
        adaptive_rail_period=position.adaptive_rail_period,
        metadata=metadata,
    )


def _try_close_position_same_candle_after_fill(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade | None:
    path_points = _same_candle_path_points(candle)
    if path_points is None:
        return None

    entry_price = _position_entry_path_price(position)
    holding_bars = _holding_bars_for_position(position, candle_index)
    entry_reached = False
    segment_start = path_points[0]

    for segment_end in path_points[1:]:
        if not entry_reached:
            if not _segment_contains_price(segment_start, segment_end, entry_price):
                segment_start = segment_end
                continue
            if position.dynamic_take_profit_enabled:
                touched_exit = _process_dynamic_position_segment(
                    position,
                    entry_price,
                    segment_end,
                    holding_bars=holding_bars,
                )
            else:
                if position.take_profit_enabled:
                    touched_exit = _first_touched_exit_on_segment(
                        entry_price,
                        segment_end,
                        stop_loss=position.stop_loss,
                        take_profit=position.take_profit,
                    )
                else:
                    touched_exit = (
                        (position.stop_loss, "stop_loss")
                        if _segment_contains_price(entry_price, segment_end, position.stop_loss)
                        else None
                    )
            if touched_exit is not None:
                exit_price_raw, exit_reason = touched_exit
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
            entry_reached = True
        else:
            if position.dynamic_take_profit_enabled:
                touched_exit = _process_dynamic_position_segment(
                    position,
                    segment_start,
                    segment_end,
                    holding_bars=holding_bars,
                )
            else:
                if position.take_profit_enabled:
                    touched_exit = _first_touched_exit_on_segment(
                        segment_start,
                        segment_end,
                        stop_loss=position.stop_loss,
                        take_profit=position.take_profit,
                    )
                else:
                    touched_exit = (
                        (position.stop_loss, "stop_loss")
                        if _segment_contains_price(segment_start, segment_end, position.stop_loss)
                        else None
                    )
            if touched_exit is not None:
                exit_price_raw, exit_reason = touched_exit
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
        segment_start = segment_end
    return None


def _try_close_position(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    allow_same_candle: bool = False,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade | None:
    if candle_index < position.entry_index:
        return None
    if not allow_same_candle and candle_index == position.entry_index:
        return None

    if position.dynamic_take_profit_enabled:
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
            segment_start = segment_end
        return None

    if position.signal == "long":
        stop_hit = candle.low <= position.stop_loss
        take_hit = position.take_profit_enabled and candle.high >= position.take_profit
        if stop_hit:
            exit_price_raw = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price_raw = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
    else:
        stop_hit = candle.high >= position.stop_loss
        take_hit = position.take_profit_enabled and candle.low <= position.take_profit
        if stop_hit:
            exit_price_raw = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price_raw = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
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


def _build_equity_curve(candles: list[Candle], trades: list[BacktestTrade]) -> list[Decimal]:
    if not candles:
        return []
    changes = [Decimal("0") for _ in candles]
    last_index = len(candles) - 1
    for trade in trades:
        exit_index = max(0, min(trade.exit_index, last_index))
        changes[exit_index] += trade.pnl
    equity_curve: list[Decimal] = []
    running_total = Decimal("0")
    for change in changes:
        running_total += change
        equity_curve.append(running_total)
    return equity_curve


def _build_drawdown_curves(net_value_curve: list[Decimal]) -> tuple[list[Decimal], list[Decimal]]:
    drawdown_curve: list[Decimal] = []
    drawdown_pct_curve: list[Decimal] = []
    if not net_value_curve:
        return drawdown_curve, drawdown_pct_curve
    peak = net_value_curve[0]
    for value in net_value_curve:
        if value > peak:
            peak = value
        drawdown = peak - value
        drawdown_curve.append(drawdown)
        if peak > 0:
            drawdown_pct_curve.append((drawdown / peak) * Decimal("100"))
        else:
            drawdown_pct_curve.append(Decimal("0"))
    return drawdown_curve, drawdown_pct_curve


def _build_period_stats(
    trades: list[BacktestTrade],
    *,
    initial_capital: Decimal,
    by: str,
) -> list[BacktestPeriodStat]:
    if by not in {"month", "year"}:
        raise ValueError("Unsupported period grouping")
    if not trades:
        return []

    sorted_trades = sorted(trades, key=lambda trade: trade.exit_ts)
    groups: dict[str, list[BacktestTrade]] = {}
    for trade in sorted_trades:
        dt = datetime.fromtimestamp(trade.exit_ts / 1000 if trade.exit_ts >= 10**12 else trade.exit_ts)
        key = dt.strftime("%Y-%m") if by == "month" else dt.strftime("%Y")
        groups.setdefault(key, []).append(trade)

    stats: list[BacktestPeriodStat] = []
    realized_before_period = Decimal("0")
    for period_label in sorted(groups):
        period_trades = groups[period_label]
        start_equity = initial_capital + realized_before_period
        period_equity = start_equity
        peak = start_equity
        max_drawdown = Decimal("0")
        wins = 0
        total_pnl = Decimal("0")
        for trade in period_trades:
            total_pnl += trade.pnl
            period_equity += trade.pnl
            if trade.pnl > 0:
                wins += 1
            if period_equity > peak:
                peak = period_equity
            drawdown = peak - period_equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        end_equity = start_equity + total_pnl
        return_pct = Decimal("0") if start_equity <= 0 else (total_pnl / start_equity) * Decimal("100")
        max_drawdown_pct = Decimal("0") if peak <= 0 else (max_drawdown / peak) * Decimal("100")
        stats.append(
            BacktestPeriodStat(
                period_label=period_label,
                trades=len(period_trades),
                win_rate=(Decimal(wins) / Decimal(len(period_trades))) * Decimal("100"),
                total_pnl=total_pnl,
                return_pct=return_pct,
                start_equity=start_equity,
                end_equity=end_equity,
                max_drawdown=max_drawdown,
                max_drawdown_pct=max_drawdown_pct,
            )
        )
        realized_before_period += total_pnl
    return stats


def _build_report(
    trades: list[BacktestTrade],
    *,
    initial_capital: Decimal,
    manual_handoffs: int = 0,
    manual_positions: list[BacktestManualPosition] | None = None,
    max_manual_positions: int = 0,
    max_total_occupied_slots: int = 0,
) -> BacktestReport:
    manual_positions = manual_positions or []
    manual_open_size = sum((position.size for position in manual_positions), Decimal("0"))
    manual_open_pnl = sum((position.pnl for position in manual_positions), Decimal("0"))
    total_trades = len(trades)
    if total_trades == 0:
        return BacktestReport(
            total_trades=0,
            win_trades=0,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("0"),
            total_pnl=Decimal("0"),
            average_pnl=Decimal("0"),
            gross_profit=Decimal("0"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("0"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("0"),
            max_drawdown=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            take_profit_hits=0,
            stop_loss_hits=0,
            ending_equity=initial_capital,
            total_return_pct=Decimal("0"),
            maker_fees=Decimal("0"),
            taker_fees=Decimal("0"),
            total_fees=Decimal("0"),
            slippage_costs=Decimal("0"),
            funding_costs=Decimal("0"),
            manual_handoffs=manual_handoffs,
            manual_open_positions=len(manual_positions),
            manual_open_size=manual_open_size,
            manual_open_pnl=manual_open_pnl,
            max_manual_positions=max_manual_positions,
            max_total_occupied_slots=max_total_occupied_slots,
        )

    wins = [trade for trade in trades if trade.pnl > 0]
    losses = [trade for trade in trades if trade.pnl < 0]
    breakevens = [trade for trade in trades if trade.pnl == 0]
    gross_profit = sum((trade.pnl for trade in wins), Decimal("0"))
    gross_loss = abs(sum((trade.pnl for trade in losses), Decimal("0")))
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    slippage_costs = sum((trade.slippage_cost for trade in trades), Decimal("0"))
    funding_costs = sum((trade.funding_cost for trade in trades), Decimal("0"))
    maker_fees = Decimal("0")
    taker_fees = Decimal("0")
    for trade in trades:
        if trade.entry_fee_type == "maker":
            maker_fees += trade.entry_fee
        elif trade.entry_fee_type == "taker":
            taker_fees += trade.entry_fee
        if trade.exit_fee_type == "maker":
            maker_fees += trade.exit_fee
        elif trade.exit_fee_type == "taker":
            taker_fees += trade.exit_fee
    total_fees = maker_fees + taker_fees
    average_pnl = total_pnl / Decimal(total_trades)
    average_win = gross_profit / Decimal(len(wins)) if wins else Decimal("0")
    average_loss = gross_loss / Decimal(len(losses)) if losses else Decimal("0")
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    profit_loss_ratio = None if average_loss == 0 else average_win / average_loss
    average_r_multiple = sum((trade.r_multiple for trade in trades), Decimal("0")) / Decimal(total_trades)

    equity = initial_capital
    peak = initial_capital
    max_drawdown = Decimal("0")
    for trade in trades:
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    max_drawdown_pct = Decimal("0") if peak <= 0 else (max_drawdown / peak) * Decimal("100")
    ending_equity = initial_capital + total_pnl
    total_return_pct = Decimal("0") if initial_capital <= 0 else (total_pnl / initial_capital) * Decimal("100")

    return BacktestReport(
        total_trades=total_trades,
        win_trades=len(wins),
        loss_trades=len(losses),
        breakeven_trades=len(breakevens),
        win_rate=(Decimal(len(wins)) / Decimal(total_trades)) * Decimal("100"),
        total_pnl=total_pnl,
        average_pnl=average_pnl,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        average_win=average_win,
        average_loss=average_loss,
        profit_loss_ratio=profit_loss_ratio,
        average_r_multiple=average_r_multiple,
        max_drawdown=max_drawdown,
        max_drawdown_pct=max_drawdown_pct,
        take_profit_hits=sum(1 for trade in trades if trade.exit_reason == "take_profit"),
        stop_loss_hits=sum(1 for trade in trades if is_stop_exit_reason(trade.exit_reason)),
        ending_equity=ending_equity,
        total_return_pct=total_return_pct,
        maker_fees=maker_fees,
        taker_fees=taker_fees,
        total_fees=total_fees,
        slippage_costs=slippage_costs,
        funding_costs=funding_costs,
        manual_handoffs=manual_handoffs,
        manual_open_positions=len(manual_positions),
        manual_open_size=manual_open_size,
        manual_open_pnl=manual_open_pnl,
        max_manual_positions=max_manual_positions,
        max_total_occupied_slots=max_total_occupied_slots,
    )


def _backtest_trade_start_index(minimum_candles: int) -> int:
    return max(max(minimum_candles - 1, 0), BACKTEST_RESERVED_CANDLES)


def _format_fee_rate_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def _format_backtest_sizing_mode(value: str) -> str:
    if value == "fixed_risk":
        return "固定风险金"
    if value == "fixed_size":
        return "固定数量"
    if value == "risk_percent":
        return "风险百分比"
    return value


def _append_backtest_dynamic_take_profit_lines(lines: list[str], result: BacktestResult) -> None:
    rules = tuple(rule.normalized() for rule in result.dynamic_protection_rules)
    rule_descriptions = describe_dynamic_protection_rules(
        rules,
        fee_offset_enabled=bool(result.dynamic_fee_offset_enabled),
    )
    break_even_trigger_r = max(int(result.dynamic_break_even_trigger_r), 1)
    dynamic_trigger_r = max(int(result.ema55_slope_lock_profit_trigger_r), 2)
    first_lock_r = max(int(result.dynamic_first_lock_r), 0)
    trailing_step_r = max(int(result.dynamic_trailing_step_r), 1)
    auto_first_lock_r = max(dynamic_trigger_r - trailing_step_r, 0)
    effective_first_lock_r = first_lock_r if first_lock_r > 0 else auto_first_lock_r
    custom_first_lock_enabled = first_lock_r > 0 and first_lock_r != auto_first_lock_r
    if result.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
        lines.append(
            "平仓条件："
            f"斜率转正平仓={'开启' if result.ema55_slope_exit_enabled else '关闭'} | "
            f"nR保本+双向手续费="
            f"{'开启' if result.ema55_slope_lock_profit_enabled else '关闭'}"
        )
        lines.append("止损说明：该策略始终使用 ATR 止损。")
        if result.ema55_slope_lock_profit_enabled:
            if rule_descriptions:
                lines.append("锁盈规则：" + "；".join(rule_descriptions) + "。")
            else:
                lines.append(
                    f"锁盈说明：保本触发R={break_even_trigger_r}，移动止盈触发R={dynamic_trigger_r}。"
                    f"价格先到 {break_even_trigger_r}R 时把止损上移到保本 - 双向 Taker 手续费；"
                    f"到 {dynamic_trigger_r}R 后再按 n-{trailing_step_r}R - 双向 Taker 手续费开始移动止盈；"
                    f"此后每多走 {trailing_step_r}R，止损继续按 n-{trailing_step_r}R - 双向 Taker 手续费递进。"
                )
        return
    lines.append(f"斜率转正平仓：{'开启' if result.ema55_slope_exit_enabled else '关闭'}")
    lines.append(f"止盈方式：{'动态止盈' if result.take_profit_mode == 'dynamic' else '固定止盈'}")
    if result.take_profit_mode != "dynamic":
        lines.append("止盈说明：固定止盈为 ATR 倍数止盈。")
        return
    if rule_descriptions:
        lines.append("动态保护规则：" + " | ".join(rule_descriptions))
    else:
        lines.append(f"保本触发R：{break_even_trigger_r}")
        lines.append(f"移动止盈触发R：{dynamic_trigger_r}")
        lines.append(f"首档锁盈R：{first_lock_r if first_lock_r > 0 else '自动'}")
        lines.append(f"移动步长R：{trailing_step_r}")
    lines.append(f"保本开关：{'开启' if result.dynamic_two_r_break_even else '关闭'}")
    lines.append(f"手续费偏移开关：{'开启' if result.dynamic_fee_offset_enabled else '关闭'}")
    lines.append(
        "时间保本开关："
        f"{'开启' if result.time_stop_break_even_enabled else '关闭'}"
        f" | 阈值K线：{result.time_stop_break_even_bars if result.time_stop_break_even_bars > 0 else '未启用'}"
    )
    lines.append(
        "nR后跌破趋势EMA收盘平仓："
        f"{'开启' if result.trend_ema_close_exit_after_trigger_r_enabled else '关闭'}"
        f" | 触发R：{result.trend_ema_close_exit_after_trigger_r}"
    )
    if rule_descriptions:
        description = "止盈说明：" + "；".join(rule_descriptions) + "；固定止盈为 ATR 倍数止盈。"
    elif result.dynamic_two_r_break_even:
        fee_text = "+2倍Taker手续费" if result.dynamic_fee_offset_enabled else ""
        if custom_first_lock_enabled:
            next_trigger_r = dynamic_trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            description = (
                f"止盈说明：价格先到 {break_even_trigger_r}R 时止损上移到开仓价{fee_text}；"
                f"到 {dynamic_trigger_r}R 后先锁定 {effective_first_lock_r}R{fee_text}；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R{fee_text}；"
                f"此后每多走 {trailing_step_r}R，止损继续再上移 {trailing_step_r}R；固定止盈为 ATR 倍数止盈。"
            )
        else:
            next_trigger_r = dynamic_trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            description = (
                f"止盈说明：价格先到 {break_even_trigger_r}R 时止损上移到开仓价{fee_text}；"
                f"到 {dynamic_trigger_r}R 后才开始移动止盈，先锁定 {effective_first_lock_r}R{fee_text}；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R{fee_text}；"
                f"此后每多走 {trailing_step_r}R，止损继续再上移 {trailing_step_r}R；固定止盈为 ATR 倍数止盈。"
            )
    else:
        fee_text = "+2倍Taker手续费" if result.dynamic_fee_offset_enabled else ""
        if custom_first_lock_enabled:
            next_trigger_r = dynamic_trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            description = (
                f"止盈说明：动态止盈在 {dynamic_trigger_r}R 时先上移到 {effective_first_lock_r}R{fee_text}；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R{fee_text}；"
                f"此后每多走 {trailing_step_r}R，止损继续再上移 {trailing_step_r}R；固定止盈为 ATR 倍数止盈。"
            )
        else:
            next_trigger_r = dynamic_trigger_r + trailing_step_r
            next_lock_r = effective_first_lock_r + trailing_step_r
            description = (
                f"止盈说明：动态止盈在 {dynamic_trigger_r}R 时先上移到 {effective_first_lock_r}R{fee_text}；"
                f"到 {next_trigger_r}R 后上移到 {next_lock_r}R{fee_text}；"
                f"此后每多走 {trailing_step_r}R，止损继续再上移 {trailing_step_r}R；固定止盈为 ATR 倍数止盈。"
            )
    lines.append(description)
    if result.time_stop_break_even_enabled and result.time_stop_break_even_bars > 0:
        lines.append(
            "时间保本说明：持仓满 "
            f"{result.time_stop_break_even_bars} 根K线后，若价格已达到净保本区间，则把止损合并上移到开仓价±2倍Taker手续费；"
            "该止损只会朝有利方向推进，不会回退。"
        )
    if result.trend_ema_close_exit_after_trigger_r_enabled:
        lines.append(
            f"趋势离场说明：当价格至少触发过 {result.trend_ema_close_exit_after_trigger_r}R 后，"
            f"若后续某根 K 线收盘跌破 EMA{result.trend_ema_period}，"
            "则按该根收盘价离场（含平仓滑点）。"
        )


def _backtest_dynamic_direction_text(result: BacktestResult) -> str:
    preferred_direction = strategy_preferred_direction(result.strategy_id, str(getattr(result, "signal_mode", "") or ""))
    if preferred_direction == "long":
        return "做多"
    if preferred_direction == "short":
        return "做空"
    return "按方向参数"


def _append_backtest_strategy_notes(
    lines: list[str],
    result: BacktestResult,
    *,
    fast_label: str,
    trend_label: str,
    reference_label: str,
) -> None:
    family = _backtest_strategy_family(result.strategy_id)
    if family == "dynamic_order":
        lines.append(
            f"趋势过滤：{fast_label} 与 {trend_label} 组成趋势过滤，当前策略方向={_backtest_dynamic_direction_text(result)}"
        )
        if _backtest_uses_mtf_filter(result.strategy_id):
            reversal_text = "停止新增仓位" if result.mtf_reversal_mode == "block_new_entries" else "不处理"
            lines.append(
                f"多周期过滤：低周期={result.bar} | "
                f"高周期={result.mtf_filter_bar} | "
                f"高周期EMA{result.mtf_filter_fast_ema_period}/EMA{result.mtf_filter_slow_ema_period} | "
                f"反转处理={reversal_text}"
            )
        lines.append(f"挂单参考EMA：{reference_label}")
        lines.append(
            f"委托规则：每根新 K 线按最新 {reference_label} 重新撤旧挂新；若新 K 线开盘已优于挂单价，则按开盘价即时成交，否则仅在盘中触及挂单价时成交，未成交委托不跨 K 线保留"
        )
        _append_backtest_dynamic_take_profit_lines(lines, result)
        lines.append(f"每波最多开仓次数：{result.max_entries_per_trend if result.max_entries_per_trend > 0 else '不限'}")
        lines.append("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓")
        return
    if family == "adaptive_ema_rail":
        lines.append(
            "交易逻辑：Adaptive EMA Rail 仅做多；先用 EMA200 与高低点结构过滤趋势，再从固定 EMA 候选池中选择 Respect Score 最高的支撑轨道。"
        )
        lines.append(
            "委托规则：主导轨道至少完成两次有效反弹后，每根新 K 线按当前主导 EMA 重新挂回踩买单；若轨道发生有效跌破，则按轨道失效退出或停止继续挂单。"
        )
        if result.adaptive_rail_stats is not None:
            stats = result.adaptive_rail_stats
            lines.append(
                "轨道统计："
                f"确认轨道 {stats.confirmed_bars}/{stats.evaluation_bars} 根 "
                f"({format_decimal_fixed(stats.confirmed_coverage_pct, 2)}%) | "
                f"轨道切换 {stats.dominant_rail_switches} 次 | "
                f"失效状态 {stats.broken_state_bars} 根 "
                f"({format_decimal_fixed(stats.broken_state_pct, 2)}%)"
            )
            lines.append(
                "主导轨道持续："
                f"平均 {format_decimal_fixed(stats.average_dominant_rail_hold_bars, 2)} 根 | "
                f"最长 {stats.max_dominant_rail_hold_bars} 根 | "
                f"rail_broken 平仓 {stats.rail_broken_exit_count} 次 "
                f"({format_decimal_fixed(stats.rail_broken_exit_pct, 2)}%)"
            )
            if stats.dominant_period_frequencies:
                top_periods = " | ".join(
                    f"EMA{item.period} {item.bars} 根 ({format_decimal_fixed(item.share_pct, 2)}%)"
                    for item in stats.dominant_period_frequencies[:5]
                )
                lines.append(f"主导EMA分布：{top_periods}")
        if result.rail_fast_gate_enabled:
            lines.append(
                "EMA21门槛："
                f"EMA{result.rail_fast_gate_period} 仅在 "
                f"close-EMA200 >= {format_decimal_fixed(result.rail_fast_min_gap_ema200_atr, 2)} ATR、"
                f"EMA{result.rail_fast_gate_period}-{trend_label} >= {format_decimal_fixed(result.rail_fast_min_spread_trend_atr, 2)} ATR、"
                f"最近 {result.rail_fast_recent_range_bars} 根振幅 <= {format_decimal_fixed(result.rail_fast_max_recent_range_atr, 2)} ATR 时允许参与竞争"
            )
        _append_backtest_dynamic_take_profit_lines(lines, result)
        lines.append(f"每条轨道最多开仓次数：{result.max_entries_per_trend if result.max_entries_per_trend > 0 else '不限'}")
        lines.append("同K线撮合：沿用动态委托撮合口径，未成交委托不跨 K 线保留。")
        return
    if family == "ema5_ema8":
        lines.append(
            f"交易逻辑：固定 4H EMA{result.ema_period}/EMA{result.trend_ema_period} 金叉死叉开仓，收盘价跌破/站回 EMA{result.trend_ema_period} 时按动态止损离场。"
        )
        lines.append("本策略不设固定止盈，回测使用收盘确认与收盘价离场。")
        return
    if family == "ema55_slope_short":
        if result.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
            lines.append(
                f"交易逻辑：固定 {fast_label}；只要连续 {result.ema55_slope_negative_entry_bars} 根单根斜率比例小于等于阈值，就按收盘价开空，不再要求前一根斜率先回到正数或走平。默认不附带额外再入场纪律；持仓后始终按 ATR 止损管理，若勾选对应平仓条件，则再叠加 {fast_label} 斜率转正平仓，以及 N R 锁盈利+双向手续费。"
            )
        else:
            if result.ema55_slope_exit_enabled:
                lines.append(
                    f"交易逻辑：固定 {fast_label}；当 {fast_label} 单根斜率比例小于等于阈值时按收盘价开空，持仓后继续按 ATR 止损/固定或动态止盈管理；若开启斜率转正平仓条件，则在 {fast_label} 斜率重新转正时按收盘价平仓。"
                )
            else:
                lines.append(
                    f"交易逻辑：固定 {fast_label}；当 {fast_label} 单根斜率比例小于等于阈值时按收盘价开空，持仓后继续按 ATR 止损/固定或动态止盈管理；当前已关闭斜率转正平仓，只依靠 ATR 止损与动态保护离场。"
                )
        if result.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID:
            lines.append(f"开仓确认：连续负斜率 {result.ema55_slope_negative_entry_bars} 根，且不要求前一根斜率先转正。")
        lines.append(
            "开空阈值："
            f"{format_decimal_fixed(result.trend_ema_slope_filter_min_ratio, 6)}"
            "（按单根 EMA 斜率 / 当前 EMA 计算，需小于等于该负值才开空）"
        )
        _append_backtest_dynamic_take_profit_lines(lines, result)
        if (
            result.strategy_id == STRATEGY_BTC_EMA55_SLOPE_SHORT_ID
            and not result.ema55_slope_same_bar_reentry_block
            and not result.ema55_slope_dynamic_exit_requires_bear_reentry
            and not result.ema55_slope_dynamic_exit_bear_reentry_break_prev_low
            and not result.ema55_slope_dynamic_exit_requires_ema_reclaim
            and not result.ema55_slope_locked_reentry_requires_ema21_near
            and not result.ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry
        ):
            lines.append("再入场：当前配置未启用额外约束；平仓后只要再次满足连续负斜率开空条件，就允许重新开空。")
        if result.ema55_slope_same_bar_reentry_block:
            lines.append("再入场约束：本根 K 线若刚刚平仓，则本根禁止再次开空。")
        if result.ema55_slope_dynamic_exit_requires_bear_reentry:
            lines.append(
                "再入场约束：若因保本或锁盈类动态保护平仓，则必须等待后续新的阴线，且该阴线当下仍满足做空条件，才允许重开。"
            )
        if result.ema55_slope_dynamic_exit_bear_reentry_break_prev_low:
            lines.append("再入场细化：等待中的新阴线还必须收盘跌破前一根 K 线低点，才允许重新开空。")
        if result.ema55_slope_dynamic_exit_requires_ema_reclaim:
            lines.append(
                f"再入场约束：若因保本或锁盈类动态保护平仓，必须先重新站上 {fast_label}，再次跌回 {fast_label} 下方后才允许重开。"
            )
        if result.ema55_slope_locked_reentry_requires_ema21_near:
            min_r = max(int(result.ema55_slope_locked_reentry_min_r), 1)
            max_r = int(result.ema55_slope_locked_reentry_max_r)
            range_text = f"{min_r}R+" if max_r <= 0 else (f"{min_r}R" if max_r == min_r else f"{min_r}R-{max_r}R")
            lines.append(
                "再入场约束：若因锁盈类动态保护平仓，且锁盈档位命中 "
                f"{range_text}，则必须先反抽接近 EMA21（距离不超过 0.3 ATR），再次跌回 EMA21 下方后才允许重开。"
            )
        if result.ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry:
            min_r = max(int(result.ema55_slope_dynamic_exit_bull_bar_reentry_min_r), 0)
            max_r = int(result.ema55_slope_dynamic_exit_bull_bar_reentry_max_r)
            if min_r <= 0 and max_r <= 0:
                prefix = "若因保本或锁盈类动态保护平仓"
            else:
                range_text = f"{max(min_r, 1)}R+" if max_r <= 0 else (
                    f"{max(min_r, 1)}R" if max_r == max(min_r, 1) else f"{max(min_r, 1)}R-{max_r}R"
                )
                prefix = f"若因锁盈类动态保护平仓，且锁盈档位命中 {range_text}"
            lines.append(
                f"再入场约束：{prefix}，且当根 K 线收阳，则后续必须等到新的阴线且做空条件仍成立时才允许重开；若当根收阴，则仍按原逻辑允许继续做空。"
            )
        lines.append("方向说明：本策略只做空，不做多。")
        return
    if family == "body_retest_short":
        lines.append(
            f"交易逻辑：先要求 {fast_label} 单根斜率比例小于等于阈值，并出现一根向下破位的阴线；随后仅在限定观察窗口内，等待价格回抽靠近 {fast_label} 后再次收阴时按收盘价开空。"
        )
        lines.append(
            "Body/ATR 条件："
            f"breakdown={format_decimal_fixed(result.body_retest_breakdown_atr_multiplier, 2)} ATR | "
            f"retest={format_decimal_fixed(result.body_retest_retest_atr_multiplier, 2)} ATR | "
            f"stop_buffer={format_decimal_fixed(result.body_retest_stop_buffer_atr_multiplier, 2)} ATR | "
            f"body_limit={format_decimal_fixed(result.body_retest_body_atr_limit, 2)} ATR | "
            f"watch_bars={result.body_retest_watch_bars}"
        )
        lines.append(
            "过滤条件："
            f"斜率阈值={format_decimal_fixed(result.trend_ema_slope_filter_min_ratio, 6)} | "
            f"ATR分位上限={format_decimal_fixed(result.atr_percentile_filter_max, 2)}"
        )
        _append_backtest_dynamic_take_profit_lines(lines, result)
        lines.append("方向说明：本策略只做空，不做多。")
        return
    if family == "ema15_ma50_pullback_long":
        lines.append(
            f"交易逻辑：固定 4H {fast_label}/{trend_label}；当 {fast_label} 从下向上穿越 {trend_label} 后，进入 {result.cross_window_bars} 根K线观察窗口，仅在 low 回踩 {fast_label} 且 close 重新收回其上方时确认做多信号。"
        )
        lines.append(
            f"成交规则：信号K线只负责收盘确认，统一在下一根K线开盘成交；每轮 CrossUp 默认最多交易第 {result.max_pullback_index} 次有效回踩。"
        )
        lines.append(
            "止损止盈："
            f"ATR周期={result.atr_period} | ATR止损倍数={format_decimal_fixed(result.atr_stop_multiplier, 2)} | "
            f"exit_mode={result.exit_mode} | 固定RR={format_decimal_fixed(result.rr, 2)}"
        )
        if result.exit_mode in {"dynamic", "dynamic_or_ema15_close"}:
            _append_backtest_dynamic_take_profit_lines(lines, result)
        if result.exit_mode in {"fixed_rr_or_ema15_close", "dynamic_or_ema15_close", "ema15_close"}:
            lines.append(f"EMA15离场：若收盘价跌破 {fast_label}，则按下一根K线开盘价离场。")
        lines.append("费用口径：开仓按 Maker，平仓按 Taker，并计入项目现有滑点配置。")
        lines.append("方向说明：本策略只做多，不做空。")
        return
    if family == "ema15_ma50_pullback_short":
        lines.append(
            f"交易逻辑：固定 4H {fast_label}/{trend_label}；当 {fast_label} 从上向下穿越 {trend_label} 后，进入 {result.cross_window_bars} 根K线观察窗口，仅在 high 回抽 {fast_label} 且 close 重新收回其下方时确认做空信号。"
        )
        lines.append(
            f"成交规则：信号K线只负责收盘确认，统一在下一根K线开盘成交；每轮 CrossDown 默认最多交易第 {result.max_pullback_index} 次有效回抽。"
        )
        lines.append(
            "止损止盈："
            f"ATR周期={result.atr_period} | ATR止损倍数={format_decimal_fixed(result.atr_stop_multiplier, 2)} | "
            f"exit_mode={result.exit_mode} | 固定RR={format_decimal_fixed(result.rr, 2)}"
        )
        if result.exit_mode in {"dynamic", "dynamic_or_ema15_close"}:
            _append_backtest_dynamic_take_profit_lines(lines, result)
        if result.exit_mode in {"fixed_rr_or_ema15_close", "dynamic_or_ema15_close", "ema15_close"}:
            lines.append(f"EMA15离场：若收盘价重新站回 {fast_label} 上方，则按下一根K线开盘价离场。")
        lines.append("费用口径：开仓按 Maker，平仓按 Taker，并计入项目现有滑点配置。")
        lines.append("方向说明：本策略只做空，不做多。")
        return
    if family in {"cross_breakout_long", "cross_breakdown_short", "cross_legacy"}:
        if family == "cross_breakout_long":
            lines.append(
                f"交易逻辑：仅做多——收盘价向上突破参考线({reference_label})，且须 {fast_label}>{trend_label}。止损、止盈按 ATR 倍数与参考价、入场价计算。"
            )
        elif family == "cross_breakdown_short":
            lines.append(
                f"交易逻辑：仅做空——收盘价向下跌破参考线({reference_label})，且须 {fast_label}<{trend_label}。止损、止盈按 ATR 倍数与参考价、入场价计算。"
            )
        else:
            lines.append(
                f"交易逻辑（旧版入口）：多——向上突破参考线({reference_label})，且须 {fast_label}>{trend_label}；空——向下跌破该参考线，且须 {fast_label}<{trend_label}。止损、止盈按 ATR 倍数与参考价、入场价计算。"
            )
        _append_backtest_dynamic_take_profit_lines(lines, result)
        if result.hold_close_exit_bars > 0:
            lines.append(f"持仓满 {result.hold_close_exit_bars} 根K线后按收盘价强制平仓（含平仓滑点）。")
