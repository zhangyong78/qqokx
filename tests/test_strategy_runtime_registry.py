from unittest import TestCase

from okx_quant.strategy_catalog import (
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_MTF_LONG_ID,
    STRATEGY_DYNAMIC_MTF_SHORT_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    STRATEGY_EMA_BREAKDOWN_SHORT_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)
from okx_quant.strategy_runtime_registry import (
    get_strategy_runtime_profile,
    strategy_entry_reference_caption,
    strategy_entry_reference_period_caption,
    strategy_is_cross_family,
    strategy_preferred_direction,
    strategy_uses_signal_extrema,
)


class StrategyRuntimeRegistryTest(TestCase):
    def test_runtime_profiles_capture_execution_families(self) -> None:
        dynamic_profile = get_strategy_runtime_profile(STRATEGY_DYNAMIC_LONG_ID)
        self.assertEqual(dynamic_profile.family, "dynamic_order")
        self.assertTrue(dynamic_profile.uses_dynamic_orders)
        self.assertFalse(dynamic_profile.uses_mtf_filter)
        self.assertEqual(dynamic_profile.exchange_trade_handler, "_run_dynamic_exchange_strategy")

        dynamic_mtf_profile = get_strategy_runtime_profile(STRATEGY_DYNAMIC_MTF_LONG_ID)
        self.assertEqual(dynamic_mtf_profile.family, "dynamic_order")
        self.assertTrue(dynamic_mtf_profile.uses_dynamic_orders)
        self.assertTrue(dynamic_mtf_profile.uses_mtf_filter)

        breakout_profile = get_strategy_runtime_profile(STRATEGY_EMA_BREAKOUT_LONG_ID)
        self.assertEqual(breakout_profile.family, "cross_breakout_long")
        self.assertTrue(breakout_profile.supports_exchange_trade)
        self.assertEqual(breakout_profile.exchange_trade_instrument_role, "signal")

        breakdown_profile = get_strategy_runtime_profile(STRATEGY_EMA_BREAKDOWN_SHORT_ID)
        self.assertEqual(breakdown_profile.family, "cross_breakdown_short")

        cross_legacy_profile = get_strategy_runtime_profile(STRATEGY_CROSS_ID)
        self.assertEqual(cross_legacy_profile.family, "cross_legacy")
        self.assertTrue(cross_legacy_profile.supports_exchange_trade)

        slope_profile = get_strategy_runtime_profile(STRATEGY_EMA55_SLOPE_SHORT_ID)
        self.assertEqual(slope_profile.family, "ema55_slope_short")
        self.assertFalse(slope_profile.supports_exchange_trade)
        self.assertEqual(slope_profile.local_trade_handler, "_run_ema55_slope_short_local_strategy")

        ema5_8_profile = get_strategy_runtime_profile(STRATEGY_EMA5_EMA8_ID)
        self.assertEqual(ema5_8_profile.family, "ema5_ema8")
        self.assertFalse(ema5_8_profile.supports_exchange_trade)

        adaptive_rail_profile = get_strategy_runtime_profile(STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID)
        self.assertEqual(adaptive_rail_profile.family, "adaptive_ema_rail")
        self.assertFalse(adaptive_rail_profile.supports_exchange_trade)

    def test_entry_reference_captions_follow_runtime_family(self) -> None:
        self.assertEqual(strategy_entry_reference_caption(STRATEGY_DYNAMIC_LONG_ID), "挂单参考线")
        self.assertEqual(strategy_entry_reference_caption(STRATEGY_EMA_BREAKOUT_LONG_ID), "突破参考线")
        self.assertEqual(strategy_entry_reference_caption(STRATEGY_EMA_BREAKDOWN_SHORT_ID), "跌破参考线")
        self.assertEqual(strategy_entry_reference_caption(STRATEGY_CROSS_ID), "参考线")
        self.assertEqual(strategy_entry_reference_caption(STRATEGY_EMA5_EMA8_ID), "参考线")
        self.assertEqual(strategy_entry_reference_period_caption(STRATEGY_DYNAMIC_LONG_ID), "挂单参考线")
        self.assertEqual(strategy_entry_reference_period_caption(STRATEGY_EMA_BREAKOUT_LONG_ID), "突破参考线周期")
        self.assertEqual(strategy_entry_reference_period_caption(STRATEGY_EMA_BREAKDOWN_SHORT_ID), "跌破参考线周期")
        self.assertEqual(strategy_entry_reference_period_caption(STRATEGY_CROSS_ID), "参考线周期")
        self.assertEqual(strategy_entry_reference_period_caption(STRATEGY_EMA5_EMA8_ID), "参考线周期")

    def test_adaptive_rail_runtime_profile_uses_generic_reference_caption(self) -> None:
        self.assertEqual(
            strategy_entry_reference_period_caption(STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID),
            "参考线周期",
        )

    def test_preferred_direction_follows_runtime_family_and_signal_mode(self) -> None:
        self.assertEqual(strategy_preferred_direction(STRATEGY_CROSS_ID, "long_only"), "long")
        self.assertEqual(strategy_preferred_direction(STRATEGY_CROSS_ID, "short_only"), "short")
        self.assertIsNone(strategy_preferred_direction(STRATEGY_CROSS_ID, "both"))
        self.assertEqual(strategy_preferred_direction(STRATEGY_EMA_BREAKOUT_LONG_ID, "both"), "long")
        self.assertEqual(strategy_preferred_direction(STRATEGY_EMA_BREAKDOWN_SHORT_ID, "both"), "short")
        self.assertEqual(strategy_preferred_direction(STRATEGY_EMA55_SLOPE_SHORT_ID, "both"), "short")
        self.assertEqual(strategy_preferred_direction(STRATEGY_DYNAMIC_LONG_ID, "both"), "long")
        self.assertEqual(strategy_preferred_direction(STRATEGY_DYNAMIC_SHORT_ID, "both"), "short")
        self.assertEqual(strategy_preferred_direction(STRATEGY_DYNAMIC_MTF_LONG_ID, "both"), "long")
        self.assertEqual(strategy_preferred_direction(STRATEGY_DYNAMIC_MTF_SHORT_ID, "both"), "short")
        self.assertEqual(strategy_preferred_direction(STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID, "both"), "long")
        self.assertIsNone(strategy_preferred_direction(STRATEGY_EMA5_EMA8_ID, "both"))

    def test_cross_helpers_and_signal_extrema_flags_follow_runtime_profile(self) -> None:
        self.assertTrue(strategy_is_cross_family(STRATEGY_CROSS_ID))
        self.assertTrue(strategy_is_cross_family(STRATEGY_EMA_BREAKOUT_LONG_ID))
        self.assertTrue(strategy_is_cross_family(STRATEGY_EMA_BREAKDOWN_SHORT_ID))
        self.assertFalse(strategy_is_cross_family(STRATEGY_EMA55_SLOPE_SHORT_ID))
        self.assertTrue(strategy_uses_signal_extrema(STRATEGY_CROSS_ID))
        self.assertTrue(strategy_uses_signal_extrema(STRATEGY_EMA_BREAKOUT_LONG_ID))
        self.assertTrue(strategy_uses_signal_extrema(STRATEGY_EMA_BREAKDOWN_SHORT_ID))
        self.assertFalse(strategy_uses_signal_extrema(STRATEGY_EMA55_SLOPE_SHORT_ID))
