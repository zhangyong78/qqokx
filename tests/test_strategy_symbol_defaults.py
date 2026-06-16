from unittest import TestCase

from okx_quant.strategy_catalog import (
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    get_strategy_definition,
)
from okx_quant.strategy_symbol_defaults import get_strategy_symbol_parameter_defaults


class StrategySymbolDefaultsTest(TestCase):
    def test_dynamic_long_backtest_defaults_still_follow_btc_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_DYNAMIC_LONG_ID,
            "BTC-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_period"], 21)
        self.assertEqual(defaults["trend_ema_type"], "ema")
        self.assertEqual(defaults["trend_ema_period"], 55)
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 1,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 4,
                    "action": "lock_profit",
                    "lock_r": 1,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
                {
                    "trigger_r": 11,
                    "action": "lock_profit",
                    "lock_r": 10,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )
        self.assertNotIn("startup_chase_window_seconds", defaults)

    def test_dynamic_long_eth_defaults_follow_final_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_DYNAMIC_LONG_ID,
            "ETH-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_period"], 21)
        self.assertEqual(defaults["trend_ema_period"], 55)
        self.assertEqual(defaults["entry_reference_ema_period"], 55)
        self.assertEqual(defaults["atr_stop_multiplier"], "1.5")
        self.assertEqual(defaults["atr_take_multiplier"], "1.5")
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 1)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 4)
        self.assertEqual(defaults["dynamic_first_lock_r"], 1)
        self.assertEqual(defaults["max_entries_per_trend"], 3)
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 1,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 4,
                    "action": "lock_profit",
                    "lock_r": 1,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
                {
                    "trigger_r": 11,
                    "action": "lock_profit",
                    "lock_r": 10,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )

    def test_dynamic_long_sol_defaults_follow_final_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_DYNAMIC_LONG_ID,
            "SOL-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_period"], 21)
        self.assertEqual(defaults["trend_ema_period"], 55)
        self.assertEqual(defaults["entry_reference_ema_period"], 13)
        self.assertEqual(defaults["atr_stop_multiplier"], "1")
        self.assertEqual(defaults["atr_take_multiplier"], "1")
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 3)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 5)
        self.assertEqual(defaults["dynamic_first_lock_r"], 1)
        self.assertEqual(defaults["max_entries_per_trend"], 2)
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 3,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 5,
                    "action": "lock_profit",
                    "lock_r": 1,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
                {
                    "trigger_r": 11,
                    "action": "lock_profit",
                    "lock_r": 10,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )

    def test_dynamic_long_doge_defaults_follow_final_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_DYNAMIC_LONG_ID,
            "DOGE-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_period"], 5)
        self.assertEqual(defaults["trend_ema_period"], 13)
        self.assertEqual(defaults["entry_reference_ema_period"], 0)
        self.assertEqual(defaults["atr_stop_multiplier"], "1")
        self.assertEqual(defaults["atr_take_multiplier"], "1")
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 2)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 6)
        self.assertEqual(defaults["dynamic_first_lock_r"], 1)
        self.assertEqual(defaults["max_entries_per_trend"], 2)
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 2,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 6,
                    "action": "lock_profit",
                    "lock_r": 1,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
                {
                    "trigger_r": 11,
                    "action": "lock_profit",
                    "lock_r": 10,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )

    def test_slope_short_btc_defaults_follow_best_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_EMA55_SLOPE_SHORT_ID,
            "BTC-USDT-SWAP",
            "launcher",
        )

        self.assertEqual(defaults["ema_type"], "ema")
        self.assertEqual(defaults["ema_period"], 55)
        self.assertEqual(defaults["trend_ema_period"], 55)
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 9)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 9)
        self.assertEqual(defaults["dynamic_first_lock_r"], 8)
        self.assertEqual(defaults["time_stop_break_even_bars"], 0)
        self.assertEqual(defaults["daily_filter_boundary"], "exchange")
        self.assertEqual(defaults["daily_filter_mode"], "disabled")
        self.assertEqual(defaults["daily_filter_ma_type"], "ema")
        self.assertEqual(defaults["daily_filter_scope"], "both")
        self.assertEqual(defaults["daily_filter_period"], 5)
        self.assertTrue(defaults["ema55_slope_same_bar_reentry_block"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_requires_bear_reentry"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_bear_reentry_break_prev_low"])
        self.assertEqual(defaults["risk_amount"], "10")

    def test_slope_short_sol_defaults_switch_to_ma20_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_EMA55_SLOPE_SHORT_ID,
            "SOL-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_type"], "ma")
        self.assertEqual(defaults["ema_period"], 20)
        self.assertEqual(defaults["trend_ema_type"], "ma")
        self.assertEqual(defaults["trend_ema_period"], 20)
        self.assertEqual(defaults["atr_period"], 15)
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 2)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 6)
        self.assertEqual(defaults["dynamic_first_lock_r"], 5)
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 2,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 6,
                    "action": "lock_profit",
                    "lock_r": 5,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )
        self.assertEqual(defaults["daily_filter_ma_type"], "ema")
        self.assertEqual(defaults["daily_filter_period"], 21)

    def test_slope_short_eth_defaults_follow_final_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_EMA55_SLOPE_SHORT_ID,
            "ETH-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_type"], "ma")
        self.assertEqual(defaults["ema_period"], 61)
        self.assertEqual(defaults["trend_ema_period"], 61)
        self.assertEqual(defaults["atr_period"], 11)
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 3)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 6)
        self.assertEqual(defaults["dynamic_first_lock_r"], 5)
        self.assertEqual(defaults["time_stop_break_even_bars"], 0)
        self.assertTrue(defaults["ema55_slope_same_bar_reentry_block"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_requires_bear_reentry"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_bear_reentry_break_prev_low"])
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 3,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 6,
                    "action": "lock_profit",
                    "lock_r": 5,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )

    def test_slope_short_doge_defaults_follow_final_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_EMA55_SLOPE_SHORT_ID,
            "DOGE-USDT-SWAP",
            "backtest",
        )

        self.assertEqual(defaults["ema_type"], "ma")
        self.assertEqual(defaults["ema_period"], 21)
        self.assertEqual(defaults["trend_ema_period"], 21)
        self.assertEqual(defaults["atr_period"], 13)
        self.assertEqual(defaults["dynamic_break_even_trigger_r"], 2)
        self.assertEqual(defaults["ema55_slope_lock_profit_trigger_r"], 6)
        self.assertEqual(defaults["dynamic_first_lock_r"], 5)
        self.assertEqual(defaults["time_stop_break_even_bars"], 10)
        self.assertTrue(defaults["ema55_slope_same_bar_reentry_block"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_requires_bear_reentry"])
        self.assertFalse(defaults["ema55_slope_dynamic_exit_bear_reentry_break_prev_low"])
        self.assertEqual(
            defaults["dynamic_protection_rules"],
            (
                {
                    "trigger_r": 2,
                    "action": "break_even",
                    "lock_r": None,
                    "trail_mode": "none",
                    "trail_every_r": None,
                    "trail_add_r": None,
                },
                {
                    "trigger_r": 6,
                    "action": "lock_profit",
                    "lock_r": 5,
                    "trail_mode": "step",
                    "trail_every_r": 1,
                    "trail_add_r": 1,
                },
            ),
        )

    def test_body_retest_bnb_defaults_follow_best_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_BODY_RETEST_SHORT_ID,
            "BNB-USDT-SWAP",
            "launcher",
        )

        self.assertEqual(defaults["ema_type"], "ma")
        self.assertEqual(defaults["ema_period"], 20)
        self.assertEqual(defaults["daily_filter_mode"], "disabled")
        self.assertEqual(defaults["body_retest_watch_bars"], 6)
        self.assertEqual(defaults["risk_amount"], "10")

    def test_slope_strategy_display_name_is_renamed(self) -> None:
        definition = get_strategy_definition(STRATEGY_EMA55_SLOPE_SHORT_ID)

        self.assertEqual(definition.name, "均线斜率做空")
    def test_btc_ema55_slope_short_defaults_use_atr14(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
            "BTC-USDT-SWAP",
            "launcher",
        )

        self.assertEqual(defaults["ema_period"], 55)
        self.assertEqual(defaults["trend_ema_period"], 55)
        self.assertEqual(defaults["atr_period"], 14)
        self.assertEqual(defaults["atr_stop_multiplier"], "2")
