from unittest import TestCase

from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_MTF_LONG_ID,
    STRATEGY_DYNAMIC_MTF_SHORT_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_uses_parameter,
)


class StrategyParametersTest(TestCase):
    def test_breakout_long_and_breakdown_short_share_same_parameter_keys(self) -> None:
        keys_long = set(iter_strategy_parameter_keys(STRATEGY_EMA_BREAKOUT_LONG_ID))
        keys_short = set(iter_strategy_parameter_keys(STRATEGY_EMA_BREAKDOWN_SHORT_ID))
        self.assertEqual(keys_long, keys_short)
        self.assertIn("entry_reference_ema_period", keys_long)
        self.assertIn("hold_close_exit_bars", keys_long)
        self.assertNotIn("big_ema_period", keys_long)
        self.assertNotIn("signal_mode", keys_long)

    def test_legacy_cross_profile_keeps_signal_mode_key(self) -> None:
        self.assertIn("signal_mode", iter_strategy_parameter_keys(STRATEGY_CROSS_ID))

    def test_dynamic_short_strategy_uses_entry_reference_parameter(self) -> None:
        self.assertTrue(strategy_uses_parameter(STRATEGY_DYNAMIC_SHORT_ID, "entry_reference_ema_period"))

    def test_dynamic_long_strategy_fixed_signal_mode_is_long_only(self) -> None:
        self.assertEqual(strategy_fixed_value(STRATEGY_DYNAMIC_LONG_ID, "signal_mode"), "long_only")
        self.assertFalse(strategy_is_parameter_editable(STRATEGY_DYNAMIC_LONG_ID, "signal_mode", "launcher"))

    def test_dynamic_multi_timeframe_profiles_have_filter_parameters(self) -> None:
        keys_long = set(iter_strategy_parameter_keys(STRATEGY_DYNAMIC_MTF_LONG_ID))
        keys_short = set(iter_strategy_parameter_keys(STRATEGY_DYNAMIC_MTF_SHORT_ID))

        self.assertEqual(keys_long, keys_short)
        self.assertIn("mtf_filter_bar", keys_long)
        self.assertIn("mtf_filter_fast_ema_period", keys_long)
        self.assertIn("mtf_filter_slow_ema_period", keys_long)
        self.assertIn("mtf_reversal_mode", keys_long)
        self.assertEqual(strategy_fixed_value(STRATEGY_DYNAMIC_MTF_LONG_ID, "signal_mode"), "long_only")
        self.assertEqual(strategy_fixed_value(STRATEGY_DYNAMIC_MTF_SHORT_ID, "signal_mode"), "short_only")

    def test_ema5_ema8_strategy_has_fixed_bar_and_ema_periods(self) -> None:
        self.assertEqual(strategy_fixed_value(STRATEGY_EMA5_EMA8_ID, "bar"), "4H")
        self.assertEqual(strategy_fixed_value(STRATEGY_EMA5_EMA8_ID, "ema_period"), 5)
        self.assertEqual(strategy_fixed_value(STRATEGY_EMA5_EMA8_ID, "trend_ema_period"), 8)
        self.assertEqual(strategy_fixed_value(STRATEGY_EMA5_EMA8_ID, "big_ema_period"), 233)
        self.assertFalse(strategy_is_parameter_editable(STRATEGY_EMA5_EMA8_ID, "bar", "backtest"))
        self.assertFalse(strategy_is_parameter_editable(STRATEGY_EMA5_EMA8_ID, "ema_period", "launcher"))

    def test_strategy_parameter_keys_stay_strategy_specific(self) -> None:
        self.assertIn("entry_reference_ema_period", iter_strategy_parameter_keys(STRATEGY_EMA_BREAKOUT_LONG_ID))
        self.assertIn("entry_reference_ema_period", iter_strategy_parameter_keys(STRATEGY_DYNAMIC_LONG_ID))
        self.assertNotIn("hold_close_exit_bars", iter_strategy_parameter_keys(STRATEGY_DYNAMIC_LONG_ID))
        self.assertIn("hold_close_exit_bars", iter_strategy_parameter_keys(STRATEGY_EMA_BREAKOUT_LONG_ID))
