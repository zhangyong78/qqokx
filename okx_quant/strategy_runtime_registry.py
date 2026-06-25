from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from okx_quant.strategy_catalog import (
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_CROSS_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    is_dynamic_mtf_strategy_id,
    is_dynamic_strategy_id,
    is_ema_atr_breakout_strategy,
    resolve_dynamic_signal_mode,
)

EngineExchangeInstrumentRole = Literal["signal", "trade"]
StrategyPreferredDirection = Literal["long", "short"]
StrategyRuntimeFamily = Literal[
    "dynamic_order",
    "adaptive_ema_rail",
    "cross_legacy",
    "cross_breakout_long",
    "cross_breakdown_short",
    "ema55_slope_short",
    "body_retest_short",
    "ema15_ma50_pullback_long",
    "ema15_ma50_pullback_short",
    "ema5_ema8",
]


@dataclass(frozen=True)
class StrategyRuntimeProfile:
    family: StrategyRuntimeFamily
    signal_only_handler: str
    local_trade_handler: str
    exchange_trade_handler: str | None = None
    exchange_trade_instrument_role: EngineExchangeInstrumentRole | None = None
    uses_dynamic_orders: bool = False
    uses_mtf_filter: bool = False
    uses_signal_extrema: bool = False

    @property
    def supports_exchange_trade(self) -> bool:
        return self.exchange_trade_handler is not None and self.exchange_trade_instrument_role is not None


_DYNAMIC_ORDER_PROFILE = StrategyRuntimeProfile(
    family="dynamic_order",
    signal_only_handler="_run_dynamic_signal_only_v2",
    local_trade_handler="_run_dynamic_local_strategy_v2",
    exchange_trade_handler="_run_dynamic_exchange_strategy",
    exchange_trade_instrument_role="trade",
    uses_dynamic_orders=True,
)
_DYNAMIC_MTF_PROFILE = StrategyRuntimeProfile(
    family="dynamic_order",
    signal_only_handler="_run_dynamic_signal_only_v2",
    local_trade_handler="_run_dynamic_local_strategy_v2",
    exchange_trade_handler="_run_dynamic_exchange_strategy",
    exchange_trade_instrument_role="trade",
    uses_dynamic_orders=True,
    uses_mtf_filter=True,
)
_CROSS_BREAKOUT_LONG_PROFILE = StrategyRuntimeProfile(
    family="cross_breakout_long",
    signal_only_handler="_run_cross_signal_only",
    local_trade_handler="_run_cross_local_strategy",
    exchange_trade_handler="_run_cross_exchange_strategy",
    exchange_trade_instrument_role="signal",
    uses_signal_extrema=True,
)
_CROSS_BREAKDOWN_SHORT_PROFILE = StrategyRuntimeProfile(
    family="cross_breakdown_short",
    signal_only_handler="_run_cross_signal_only",
    local_trade_handler="_run_cross_local_strategy",
    exchange_trade_handler="_run_cross_exchange_strategy",
    exchange_trade_instrument_role="signal",
    uses_signal_extrema=True,
)
_CROSS_LEGACY_PROFILE = StrategyRuntimeProfile(
    family="cross_legacy",
    signal_only_handler="_run_cross_signal_only",
    local_trade_handler="_run_cross_local_strategy",
    exchange_trade_handler="_run_cross_exchange_strategy",
    exchange_trade_instrument_role="signal",
    uses_signal_extrema=True,
)
_EMA55_SLOPE_SHORT_PROFILE = StrategyRuntimeProfile(
    family="ema55_slope_short",
    signal_only_handler="_run_ema55_slope_short_signal_only",
    local_trade_handler="_run_ema55_slope_short_local_strategy",
)
_BODY_RETEST_SHORT_PROFILE = StrategyRuntimeProfile(
    family="body_retest_short",
    signal_only_handler="_run_body_retest_short_signal_only",
    local_trade_handler="_run_body_retest_short_local_strategy",
)
_BTC_EMA15_MA50_PULLBACK_LONG_PROFILE = StrategyRuntimeProfile(
    family="ema15_ma50_pullback_long",
    signal_only_handler="_run_btc_ema15_ma50_pullback_long_signal_only",
    local_trade_handler="_run_btc_ema15_ma50_pullback_long_local_strategy",
)
_BTC_EMA15_MA50_PULLBACK_SHORT_PROFILE = StrategyRuntimeProfile(
    family="ema15_ma50_pullback_short",
    signal_only_handler="_run_btc_ema15_ma50_pullback_short_signal_only",
    local_trade_handler="_run_btc_ema15_ma50_pullback_short_local_strategy",
)
_EMA5_EMA8_PROFILE = StrategyRuntimeProfile(
    family="ema5_ema8",
    signal_only_handler="_run_ema5_ema8_signal_only",
    local_trade_handler="_run_ema5_ema8_local_strategy",
)
_ADAPTIVE_EMA_RAIL_PROFILE = StrategyRuntimeProfile(
    family="adaptive_ema_rail",
    signal_only_handler="_run_adaptive_ema_rail_signal_only",
    local_trade_handler="_run_adaptive_ema_rail_local_strategy",
)


def get_strategy_runtime_profile(strategy_id: str) -> StrategyRuntimeProfile:
    if is_dynamic_mtf_strategy_id(strategy_id):
        return _DYNAMIC_MTF_PROFILE
    if is_dynamic_strategy_id(strategy_id):
        return _DYNAMIC_ORDER_PROFILE
    if strategy_id == STRATEGY_CROSS_ID:
        return _CROSS_LEGACY_PROFILE
    if strategy_id == STRATEGY_EMA_BREAKDOWN_SHORT_ID:
        return _CROSS_BREAKDOWN_SHORT_PROFILE
    if is_ema_atr_breakout_strategy(strategy_id):
        return _CROSS_BREAKOUT_LONG_PROFILE
    if strategy_id in {STRATEGY_EMA55_SLOPE_SHORT_ID, STRATEGY_BTC_EMA55_SLOPE_SHORT_ID}:
        return _EMA55_SLOPE_SHORT_PROFILE
    if strategy_id == STRATEGY_BODY_RETEST_SHORT_ID:
        return _BODY_RETEST_SHORT_PROFILE
    if strategy_id == STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID:
        return _BTC_EMA15_MA50_PULLBACK_LONG_PROFILE
    if strategy_id == STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID:
        return _BTC_EMA15_MA50_PULLBACK_SHORT_PROFILE
    if strategy_id == STRATEGY_EMA5_EMA8_ID:
        return _EMA5_EMA8_PROFILE
    if strategy_id == STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID:
        return _ADAPTIVE_EMA_RAIL_PROFILE
    raise KeyError(f"unknown strategy runtime profile: {strategy_id}")


def strategy_runtime_family(strategy_id: str) -> StrategyRuntimeFamily:
    return get_strategy_runtime_profile(strategy_id).family


def strategy_entry_reference_caption(strategy_id: str) -> str:
    profile = get_strategy_runtime_profile(strategy_id)
    if profile.uses_dynamic_orders:
        return "挂单参考线"
    if profile.family == "cross_breakdown_short":
        return "跌破参考线"
    if profile.family == "cross_breakout_long":
        return "突破参考线"
    return "参考线"


def strategy_entry_reference_period_caption(strategy_id: str) -> str:
    profile = get_strategy_runtime_profile(strategy_id)
    if profile.uses_dynamic_orders:
        return "挂单参考线"
    if profile.family == "cross_breakdown_short":
        return "跌破参考线周期"
    if profile.family == "cross_breakout_long":
        return "突破参考线周期"
    return "参考线周期"


def strategy_preferred_direction(strategy_id: str, signal_mode: str) -> StrategyPreferredDirection | None:
    profile = get_strategy_runtime_profile(strategy_id)
    if profile.family == "cross_breakout_long":
        return "long"
    if profile.family == "adaptive_ema_rail":
        return "long"
    if profile.family == "ema15_ma50_pullback_long":
        return "long"
    if profile.family == "ema15_ma50_pullback_short":
        return "short"
    if profile.family in {"cross_breakdown_short", "ema55_slope_short", "body_retest_short"}:
        return "short"
    normalized_signal_mode = resolve_dynamic_signal_mode(strategy_id, signal_mode)
    if normalized_signal_mode == "long_only":
        return "long"
    if normalized_signal_mode == "short_only":
        return "short"
    return None


def strategy_is_cross_family(strategy_id: str) -> bool:
    try:
        family = get_strategy_runtime_profile(strategy_id).family
    except KeyError:
        return False
    return family in {"cross_legacy", "cross_breakout_long", "cross_breakdown_short"}


def strategy_uses_dynamic_orders(strategy_id: str) -> bool:
    try:
        return get_strategy_runtime_profile(strategy_id).uses_dynamic_orders
    except KeyError:
        return False


def strategy_uses_mtf_filter(strategy_id: str) -> bool:
    try:
        return get_strategy_runtime_profile(strategy_id).uses_mtf_filter
    except KeyError:
        return False


def strategy_uses_signal_extrema(strategy_id: str) -> bool:
    try:
        return get_strategy_runtime_profile(strategy_id).uses_signal_extrema
    except KeyError:
        return False
