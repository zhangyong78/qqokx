from __future__ import annotations

from typing import Literal

from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID


PageScope = Literal["launcher", "backtest"]


_DYNAMIC_LONG_4COIN_DEFAULTS: dict[str, dict[str, object]] = {
    "BTC-USDT-SWAP": {
        "bar": "1H",
        "ema_type": "ema",
        "ema_period": 21,
        "trend_ema_type": "ma",
        "trend_ema_period": 50,
        "atr_period": 10,
        "atr_stop_multiplier": "2",
        "atr_take_multiplier": "2",
        "entry_reference_ema_type": "ma",
        "entry_reference_ema_period": 50,
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 1,
        "dynamic_two_r_break_even": True,
        "dynamic_fee_offset_enabled": True,
        "time_stop_break_even_enabled": False,
        "time_stop_break_even_bars": 0,
        "startup_chase_window_seconds": 0,
    },
    "ETH-USDT-SWAP": {
        "bar": "1H",
        "ema_type": "ma",
        "ema_period": 21,
        "trend_ema_type": "ema",
        "trend_ema_period": 55,
        "atr_period": 10,
        "atr_stop_multiplier": "2",
        "atr_take_multiplier": "2",
        "entry_reference_ema_type": "ma",
        "entry_reference_ema_period": 55,
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 1,
        "dynamic_two_r_break_even": True,
        "dynamic_fee_offset_enabled": True,
        "time_stop_break_even_enabled": False,
        "time_stop_break_even_bars": 0,
        "startup_chase_window_seconds": 0,
    },
    "SOL-USDT-SWAP": {
        "bar": "1H",
        "ema_type": "ma",
        "ema_period": 21,
        "trend_ema_type": "ma",
        "trend_ema_period": 55,
        "atr_period": 10,
        "atr_stop_multiplier": "1",
        "atr_take_multiplier": "1",
        "entry_reference_ema_type": "ma",
        "entry_reference_ema_period": 55,
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 1,
        "dynamic_two_r_break_even": True,
        "dynamic_fee_offset_enabled": True,
        "time_stop_break_even_enabled": False,
        "time_stop_break_even_bars": 0,
        "startup_chase_window_seconds": 0,
    },
    "BNB-USDT-SWAP": {
        "bar": "1H",
        "ema_type": "ma",
        "ema_period": 21,
        "trend_ema_type": "ma",
        "trend_ema_period": 55,
        "atr_period": 10,
        "atr_stop_multiplier": "1.5",
        "atr_take_multiplier": "6",
        "entry_reference_ema_type": "ma",
        "entry_reference_ema_period": 55,
        "take_profit_mode": "dynamic",
        "max_entries_per_trend": 1,
        "dynamic_two_r_break_even": True,
        "dynamic_fee_offset_enabled": True,
        "time_stop_break_even_enabled": False,
        "time_stop_break_even_bars": 0,
        "startup_chase_window_seconds": 0,
    },
}


def get_strategy_symbol_parameter_defaults(
    strategy_id: str,
    symbol: str,
    scope: PageScope,
) -> dict[str, object]:
    normalized_symbol = str(symbol or "").strip().upper()
    if strategy_id != STRATEGY_DYNAMIC_LONG_ID:
        return {}
    defaults = _DYNAMIC_LONG_4COIN_DEFAULTS.get(normalized_symbol)
    if not defaults:
        return {}
    scoped = dict(defaults)
    if scope == "backtest":
        scoped.pop("startup_chase_window_seconds", None)
    return scoped
