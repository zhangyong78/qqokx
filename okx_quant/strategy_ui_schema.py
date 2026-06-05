from __future__ import annotations

from dataclasses import dataclass, field

from okx_quant.strategy_catalog import (
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_MTF_LONG_ID,
    STRATEGY_DYNAMIC_MTF_SHORT_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
    is_dynamic_mtf_strategy_id,
)
from okx_quant.strategy_parameters import (
    PageScope,
    strategy_parameter_default_value,
    strategy_uses_parameter,
)


@dataclass(frozen=True)
class StrategyWidgetVisibility:
    show_big_ema: bool
    show_dynamic_take_profit: bool
    show_entry_reference: bool
    show_hold_close_exit: bool
    show_max_entries: bool
    show_mtf_controls: bool
    show_slope_threshold: bool
    show_startup_chase_window: bool


@dataclass(frozen=True)
class StrategyUiSchema:
    strategy_id: str
    parameter_defaults: dict[PageScope, dict[str, object]] = field(default_factory=dict)
    extra_defaults: dict[PageScope, dict[str, object]] = field(default_factory=dict)
    extra_fixed_values: dict[PageScope, dict[str, object]] = field(default_factory=dict)
    force_follow_signal: bool = False
    force_local_trade: bool = False
    supports_dynamic_take_profit: bool = False
    uses_startup_chase_window: bool = False


_SCOPE_LAUNCHER: PageScope = "launcher"
_SCOPE_BACKTEST: PageScope = "backtest"
_SCOPE_OBSERVER: PageScope = "observer"


STRATEGY_UI_SCHEMAS: dict[str, StrategyUiSchema] = {
    STRATEGY_DYNAMIC_LONG_ID: StrategyUiSchema(
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_DYNAMIC_SHORT_ID: StrategyUiSchema(
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_DYNAMIC_MTF_LONG_ID: StrategyUiSchema(
        strategy_id=STRATEGY_DYNAMIC_MTF_LONG_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_DYNAMIC_MTF_SHORT_ID: StrategyUiSchema(
        strategy_id=STRATEGY_DYNAMIC_MTF_SHORT_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_EMA_BREAKOUT_LONG_ID: StrategyUiSchema(
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_EMA_BREAKDOWN_SHORT_ID: StrategyUiSchema(
        strategy_id=STRATEGY_EMA_BREAKDOWN_SHORT_ID,
        supports_dynamic_take_profit=True,
        uses_startup_chase_window=True,
    ),
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID: StrategyUiSchema(
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        supports_dynamic_take_profit=True,
    ),
    STRATEGY_EMA5_EMA8_ID: StrategyUiSchema(
        strategy_id=STRATEGY_EMA5_EMA8_ID,
        parameter_defaults={
            _SCOPE_LAUNCHER: {
                "take_profit_mode": "fixed",
                "max_entries_per_trend": "0",
            },
        },
        extra_defaults={
            _SCOPE_LAUNCHER: {
                "entry_side_mode": "follow_signal",
                "risk_amount": "10",
                "tp_sl_mode": "local_trade",
            },
            _SCOPE_BACKTEST: {
                "risk_amount": "100",
            },
        },
        extra_fixed_values={
            _SCOPE_LAUNCHER: {
                "order_size": "0",
                "risk_amount": "10",
            },
            _SCOPE_BACKTEST: {
                "order_size": "0",
                "risk_amount": "100",
            },
        },
        force_follow_signal=True,
        force_local_trade=True,
    ),
    STRATEGY_EMA55_SLOPE_SHORT_ID: StrategyUiSchema(
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        parameter_defaults={
            _SCOPE_LAUNCHER: {
                "atr_period": 14,
                "atr_stop_multiplier": "2",
                "atr_take_multiplier": "4",
                "atr_percentile_filter_max": "0.5",
                "bar": "1H",
                "dynamic_fee_offset_enabled": True,
                "dynamic_two_r_break_even": True,
                "ema55_slope_exit_enabled": False,
                "take_profit_mode": "dynamic",
                "time_stop_break_even_bars": 10,
                "time_stop_break_even_enabled": False,
                "trend_ema_slope_filter_min_ratio": "-0.0005",
            },
            _SCOPE_BACKTEST: {
                "atr_period": 14,
                "atr_stop_multiplier": "2",
                "atr_take_multiplier": "4",
                "atr_percentile_filter_max": "0.5",
                "bar": "1H",
                "dynamic_fee_offset_enabled": True,
                "dynamic_two_r_break_even": True,
                "ema55_slope_exit_enabled": False,
                "take_profit_mode": "dynamic",
                "time_stop_break_even_bars": 10,
                "time_stop_break_even_enabled": False,
                "trend_ema_slope_filter_min_ratio": "-0.0005",
            },
        },
        extra_defaults={
            _SCOPE_LAUNCHER: {
                "entry_side_mode": "follow_signal",
                "order_size": "0",
                "poll_seconds": "10",
                "position_mode": "net",
                "risk_amount": "100",
                "tp_sl_mode": "local_trade",
                "trade_mode": "cross",
                "trigger_type": "mark",
            },
            _SCOPE_BACKTEST: {
                "risk_amount": "100",
                "sizing_mode": "fixed_risk",
            },
        },
        force_follow_signal=True,
        force_local_trade=True,
        supports_dynamic_take_profit=True,
    ),
}


def get_strategy_ui_schema(strategy_id: str) -> StrategyUiSchema:
    return STRATEGY_UI_SCHEMAS.get(strategy_id, StrategyUiSchema(strategy_id=strategy_id))


def strategy_parameter_default_for_scope(strategy_id: str, key: str, scope: PageScope) -> object | None:
    schema = get_strategy_ui_schema(strategy_id)
    scoped_defaults = schema.parameter_defaults.get(scope, {})
    if key in scoped_defaults:
        return scoped_defaults[key]
    return strategy_parameter_default_value(key)


def strategy_ui_extra_defaults(strategy_id: str, scope: PageScope) -> dict[str, object]:
    schema = get_strategy_ui_schema(strategy_id)
    return dict(schema.extra_defaults.get(scope, {}))


def strategy_ui_fixed_extra_value(strategy_id: str, key: str, scope: PageScope) -> object | None:
    schema = get_strategy_ui_schema(strategy_id)
    return schema.extra_fixed_values.get(scope, {}).get(key)


def strategy_supports_dynamic_take_profit(strategy_id: str) -> bool:
    return get_strategy_ui_schema(strategy_id).supports_dynamic_take_profit


def strategy_uses_startup_chase_window(strategy_id: str) -> bool:
    return get_strategy_ui_schema(strategy_id).uses_startup_chase_window


def strategy_forces_follow_signal(strategy_id: str) -> bool:
    return get_strategy_ui_schema(strategy_id).force_follow_signal


def strategy_forces_local_trade(strategy_id: str) -> bool:
    return get_strategy_ui_schema(strategy_id).force_local_trade


def build_strategy_widget_visibility(strategy_id: str, scope: PageScope) -> StrategyWidgetVisibility:
    return StrategyWidgetVisibility(
        show_big_ema=strategy_uses_parameter(strategy_id, "big_ema_period"),
        show_dynamic_take_profit=strategy_supports_dynamic_take_profit(strategy_id),
        show_entry_reference=strategy_uses_parameter(strategy_id, "entry_reference_ema_period"),
        show_hold_close_exit=scope == _SCOPE_BACKTEST and strategy_uses_parameter(strategy_id, "hold_close_exit_bars"),
        show_max_entries=strategy_uses_parameter(strategy_id, "max_entries_per_trend"),
        show_mtf_controls=is_dynamic_mtf_strategy_id(strategy_id),
        show_slope_threshold=strategy_uses_parameter(strategy_id, "trend_ema_slope_filter_min_ratio"),
        show_startup_chase_window=scope == _SCOPE_LAUNCHER and strategy_uses_startup_chase_window(strategy_id),
    )
