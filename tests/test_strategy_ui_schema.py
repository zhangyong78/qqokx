from unittest import TestCase

from okx_quant.strategy_catalog import (
    STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
    STRATEGY_BTC_EMA55_SLOPE_SHORT_ID,
    STRATEGY_BODY_RETEST_SHORT_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
)
from okx_quant.strategy_ui_schema import (
    build_strategy_widget_visibility,
    strategy_forces_follow_signal,
    strategy_forces_local_trade,
    strategy_parameter_default_for_scope,
    strategy_supports_dynamic_take_profit,
    strategy_ui_extra_defaults,
    strategy_ui_fixed_extra_value,
    strategy_uses_startup_chase_window,
)


class StrategyUiSchemaTest(TestCase):
    def test_ema5_8_schema_keeps_legacy_launcher_defaults(self) -> None:
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA5_EMA8_ID,
                "take_profit_mode",
                "launcher",
            ),
            "fixed",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA5_EMA8_ID,
                "max_entries_per_trend",
                "launcher",
            ),
            "0",
        )
        launcher_defaults = strategy_ui_extra_defaults(STRATEGY_EMA5_EMA8_ID, "launcher")
        self.assertEqual(launcher_defaults["risk_amount"], "10")
        self.assertEqual(launcher_defaults["tp_sl_mode"], "local_trade")
        self.assertEqual(launcher_defaults["entry_side_mode"], "follow_signal")
        self.assertEqual(strategy_ui_fixed_extra_value(STRATEGY_EMA5_EMA8_ID, "risk_amount", "launcher"), "10")
        self.assertEqual(strategy_ui_fixed_extra_value(STRATEGY_EMA5_EMA8_ID, "order_size", "launcher"), "0")
        self.assertEqual(strategy_ui_fixed_extra_value(STRATEGY_EMA5_EMA8_ID, "risk_amount", "backtest"), "10")
        self.assertIsNone(strategy_ui_fixed_extra_value(STRATEGY_EMA55_SLOPE_SHORT_ID, "risk_amount", "launcher"))

    def test_slope_strategy_launcher_schema_exposes_reviewed_defaults(self) -> None:
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "trend_ema_slope_filter_min_ratio",
                "launcher",
            ),
            "-0.0005",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "atr_period",
                "launcher",
            ),
            14,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "dynamic_break_even_trigger_r",
                "launcher",
            ),
            9,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "ema55_slope_lock_profit_trigger_r",
                "launcher",
            ),
            9,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "time_stop_break_even_bars",
                "launcher",
            ),
            0,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_EMA55_SLOPE_SHORT_ID,
                "dynamic_trailing_step_r",
                "launcher",
            ),
            1,
        )
        launcher_defaults = strategy_ui_extra_defaults(STRATEGY_EMA55_SLOPE_SHORT_ID, "launcher")
        self.assertEqual(launcher_defaults["risk_amount"], "10")
        self.assertEqual(launcher_defaults["poll_seconds"], "10")
        self.assertEqual(launcher_defaults["tp_sl_mode"], "local_trade")
        self.assertEqual(launcher_defaults["entry_side_mode"], "follow_signal")
        backtest_defaults = strategy_ui_extra_defaults(STRATEGY_EMA55_SLOPE_SHORT_ID, "backtest")
        self.assertEqual(backtest_defaults["risk_amount"], "100")

    def test_dynamic_long_schema_defaults_follow_btc_template(self) -> None:
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "entry_reference_ema_period",
                "launcher",
            ),
            55,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "atr_take_multiplier",
                "launcher",
            ),
            "2",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "dynamic_break_even_trigger_r",
                "launcher",
            ),
            1,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "ema55_slope_lock_profit_trigger_r",
                "launcher",
            ),
            4,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "dynamic_first_lock_r",
                "launcher",
            ),
            1,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(
                STRATEGY_DYNAMIC_LONG_ID,
                "time_stop_break_even_bars",
                "launcher",
            ),
            0,
        )
        protection_rules = strategy_parameter_default_for_scope(
            STRATEGY_DYNAMIC_LONG_ID,
            "dynamic_protection_rules",
            "launcher",
        )
        self.assertEqual(protection_rules[0]["trigger_r"], 1)
        self.assertEqual(protection_rules[1]["trigger_r"], 4)
        self.assertEqual(protection_rules[2]["lock_r"], 10)

    def test_body_retest_strategy_schema_uses_10u_for_launcher_and_backtest(self) -> None:
        launcher_defaults = strategy_ui_extra_defaults(STRATEGY_BODY_RETEST_SHORT_ID, "launcher")
        backtest_defaults = strategy_ui_extra_defaults(STRATEGY_BODY_RETEST_SHORT_ID, "backtest")

        self.assertEqual(launcher_defaults["risk_amount"], "10")
        self.assertEqual(backtest_defaults["risk_amount"], "100")

    def test_schema_flags_capture_strategy_runtime_constraints(self) -> None:
        self.assertTrue(strategy_supports_dynamic_take_profit(STRATEGY_EMA55_SLOPE_SHORT_ID))
        self.assertTrue(strategy_supports_dynamic_take_profit(STRATEGY_DYNAMIC_LONG_ID))
        self.assertFalse(strategy_supports_dynamic_take_profit(STRATEGY_EMA5_EMA8_ID))
        self.assertTrue(strategy_forces_local_trade(STRATEGY_EMA55_SLOPE_SHORT_ID))
        self.assertTrue(strategy_forces_follow_signal(STRATEGY_EMA55_SLOPE_SHORT_ID))
        self.assertTrue(strategy_uses_startup_chase_window(STRATEGY_DYNAMIC_LONG_ID))
        self.assertFalse(strategy_uses_startup_chase_window(STRATEGY_EMA55_SLOPE_SHORT_ID))

    def test_widget_visibility_is_declared_from_schema_rules(self) -> None:
        launcher_visibility = build_strategy_widget_visibility(STRATEGY_EMA55_SLOPE_SHORT_ID, "launcher")
        backtest_visibility = build_strategy_widget_visibility(STRATEGY_EMA55_SLOPE_SHORT_ID, "backtest")

        self.assertTrue(launcher_visibility.show_dynamic_take_profit)
        self.assertTrue(launcher_visibility.show_slope_threshold)
        self.assertFalse(launcher_visibility.show_startup_chase_window)
        self.assertFalse(launcher_visibility.show_hold_close_exit)
        self.assertTrue(backtest_visibility.show_dynamic_take_profit)
        self.assertTrue(backtest_visibility.show_slope_threshold)

    def test_btc_ema55_slope_short_schema_hides_dynamic_take_profit_controls(self) -> None:
        self.assertFalse(strategy_supports_dynamic_take_profit(STRATEGY_BTC_EMA55_SLOPE_SHORT_ID))
        launcher_defaults = strategy_ui_extra_defaults(STRATEGY_BTC_EMA55_SLOPE_SHORT_ID, "launcher")
        self.assertEqual(launcher_defaults["risk_amount"], "10")
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA55_SLOPE_SHORT_ID, "atr_period", "launcher"),
            14,
        )
        visibility = build_strategy_widget_visibility(STRATEGY_BTC_EMA55_SLOPE_SHORT_ID, "launcher")
        self.assertFalse(visibility.show_dynamic_take_profit)
        self.assertTrue(visibility.show_slope_threshold)

    def test_btc_ema15_ma50_pullback_schema_exposes_research_defaults_for_both_directions(self) -> None:
        self.assertTrue(strategy_supports_dynamic_take_profit(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID))
        self.assertTrue(strategy_supports_dynamic_take_profit(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID))
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "bar", "backtest"),
            "4H",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "bar", "backtest"),
            "4H",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "cross_window_bars", "backtest"),
            10,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "ema_type", "backtest"),
            "ema",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "ema_period", "backtest"),
            15,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "trend_ema_type", "backtest"),
            "ma",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "trend_ema_period", "backtest"),
            50,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "cross_window_bars", "backtest"),
            10,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "ema_type", "backtest"),
            "ema",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "ema_period", "backtest"),
            15,
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "trend_ema_type", "backtest"),
            "ema",
        )
        self.assertEqual(
            strategy_parameter_default_for_scope(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "trend_ema_period", "backtest"),
            55,
        )
        visibility_long = build_strategy_widget_visibility(STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID, "backtest")
        visibility_short = build_strategy_widget_visibility(STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID, "backtest")
        self.assertTrue(visibility_long.show_dynamic_take_profit)
        self.assertTrue(visibility_short.show_dynamic_take_profit)
        self.assertTrue(visibility_long.show_daily_filter_controls)
        self.assertTrue(visibility_short.show_daily_filter_controls)
