from unittest import TestCase

from okx_quant.strategy_catalog import (
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
        self.assertEqual(strategy_ui_fixed_extra_value(STRATEGY_EMA5_EMA8_ID, "risk_amount", "backtest"), "100")
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
        launcher_defaults = strategy_ui_extra_defaults(STRATEGY_EMA55_SLOPE_SHORT_ID, "launcher")
        self.assertEqual(launcher_defaults["risk_amount"], "100")
        self.assertEqual(launcher_defaults["poll_seconds"], "10")
        self.assertEqual(launcher_defaults["tp_sl_mode"], "local_trade")
        self.assertEqual(launcher_defaults["entry_side_mode"], "follow_signal")

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
