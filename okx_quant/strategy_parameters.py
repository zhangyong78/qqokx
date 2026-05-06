from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)

PageScope = Literal["launcher", "backtest", "observer"]


@dataclass(frozen=True)
class ParameterDefinition:
    key: str
    visible_in: tuple[PageScope, ...] = ("launcher", "backtest", "observer")
    editable_in: tuple[PageScope, ...] = ("launcher", "backtest", "observer")
    default: object | None = None

    def visible_on(self, scope: PageScope) -> bool:
        return scope in self.visible_in

    def editable_on(self, scope: PageScope) -> bool:
        return scope in self.editable_in


@dataclass(frozen=True)
class StrategyParameterProfile:
    strategy_id: str
    parameter_keys: tuple[str, ...]
    fixed_values: dict[str, object] = field(default_factory=dict)

    def includes(self, key: str) -> bool:
        return key in self.parameter_keys

    def is_fixed(self, key: str) -> bool:
        return key in self.fixed_values

    def fixed_value(self, key: str) -> object | None:
        return self.fixed_values.get(key)


PARAMETERS: dict[str, ParameterDefinition] = {
    "bar": ParameterDefinition(key="bar", default="1H"),
    "signal_mode": ParameterDefinition(key="signal_mode", default="both"),
    "ema_period": ParameterDefinition(key="ema_period", default=21),
    "trend_ema_period": ParameterDefinition(key="trend_ema_period", default=55),
    "big_ema_period": ParameterDefinition(key="big_ema_period", default=233),
    "atr_period": ParameterDefinition(key="atr_period", default=10),
    "atr_stop_multiplier": ParameterDefinition(key="atr_stop_multiplier", default="2"),
    "atr_take_multiplier": ParameterDefinition(key="atr_take_multiplier", default="4"),
    "entry_reference_ema_period": ParameterDefinition(key="entry_reference_ema_period", default=55),
    "take_profit_mode": ParameterDefinition(key="take_profit_mode", default="dynamic"),
    "max_entries_per_trend": ParameterDefinition(key="max_entries_per_trend", default=1),
    "dynamic_two_r_break_even": ParameterDefinition(key="dynamic_two_r_break_even", default=True),
    "dynamic_fee_offset_enabled": ParameterDefinition(key="dynamic_fee_offset_enabled", default=True),
    "time_stop_break_even_enabled": ParameterDefinition(key="time_stop_break_even_enabled", default=False),
    "time_stop_break_even_bars": ParameterDefinition(key="time_stop_break_even_bars", default=10),
    "hold_close_exit_bars": ParameterDefinition(key="hold_close_exit_bars", default=0),
    "startup_chase_window_seconds": ParameterDefinition(
        key="startup_chase_window_seconds",
        default=0,
        visible_in=("launcher", "observer"),
        editable_in=("launcher", "observer"),
    ),
}


STRATEGY_PARAMETER_PROFILES: dict[str, StrategyParameterProfile] = {
    STRATEGY_DYNAMIC_LONG_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        parameter_keys=(
            "bar",
            "signal_mode",
            "ema_period",
            "trend_ema_period",
            "atr_period",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "entry_reference_ema_period",
            "take_profit_mode",
            "max_entries_per_trend",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
            "startup_chase_window_seconds",
        ),
        fixed_values={"signal_mode": "long_only"},
    ),
    STRATEGY_DYNAMIC_SHORT_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        parameter_keys=(
            "bar",
            "signal_mode",
            "ema_period",
            "trend_ema_period",
            "atr_period",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "entry_reference_ema_period",
            "take_profit_mode",
            "max_entries_per_trend",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
            "startup_chase_window_seconds",
        ),
        fixed_values={"signal_mode": "short_only"},
    ),
    STRATEGY_EMA_BREAKOUT_LONG_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        parameter_keys=(
            "bar",
            "ema_period",
            "trend_ema_period",
            "entry_reference_ema_period",
            "atr_period",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "take_profit_mode",
            "max_entries_per_trend",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
            "startup_chase_window_seconds",
            "hold_close_exit_bars",
        ),
        fixed_values={"signal_mode": "long_only"},
    ),
    STRATEGY_EMA_BREAKDOWN_SHORT_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
        parameter_keys=(
            "bar",
            "ema_period",
            "trend_ema_period",
            "entry_reference_ema_period",
            "atr_period",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "take_profit_mode",
            "max_entries_per_trend",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
            "startup_chase_window_seconds",
            "hold_close_exit_bars",
        ),
        fixed_values={"signal_mode": "short_only"},
    ),
    STRATEGY_CROSS_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_CROSS_ID,
        parameter_keys=(
            "bar",
            "signal_mode",
            "ema_period",
            "trend_ema_period",
            "entry_reference_ema_period",
            "atr_period",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "take_profit_mode",
            "max_entries_per_trend",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
            "hold_close_exit_bars",
        ),
    ),
    STRATEGY_EMA5_EMA8_ID: StrategyParameterProfile(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        parameter_keys=(
            "bar",
            "signal_mode",
            "ema_period",
            "trend_ema_period",
            "big_ema_period",
        ),
        fixed_values={
            "bar": "4H",
            "ema_period": 5,
            "trend_ema_period": 8,
            "big_ema_period": 233,
        },
    ),
}


def get_parameter_definition(key: str) -> ParameterDefinition:
    return PARAMETERS[key]


def get_strategy_parameter_profile(strategy_id: str) -> StrategyParameterProfile:
    return STRATEGY_PARAMETER_PROFILES[strategy_id]


def iter_strategy_parameter_keys(strategy_id: str) -> tuple[str, ...]:
    return get_strategy_parameter_profile(strategy_id).parameter_keys


def strategy_uses_parameter(strategy_id: str, key: str) -> bool:
    return get_strategy_parameter_profile(strategy_id).includes(key)


def strategy_parameter_default_value(key: str) -> object | None:
    definition = PARAMETERS.get(key)
    if definition is None:
        return None
    return definition.default


def strategy_fixed_value(strategy_id: str, key: str) -> object | None:
    return get_strategy_parameter_profile(strategy_id).fixed_value(key)


def strategy_is_parameter_editable(strategy_id: str, key: str, scope: PageScope) -> bool:
    if not strategy_uses_parameter(strategy_id, key):
        return False
    if strategy_fixed_value(strategy_id, key) is not None:
        return False
    definition = PARAMETERS.get(key)
    if definition is None:
        return False
    return definition.editable_on(scope)
