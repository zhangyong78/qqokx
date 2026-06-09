from decimal import Decimal
from unittest import TestCase

from scripts.build_best_parameter_bundle import ShortSpec as BestShortSpec
from scripts.build_best_parameter_bundle import build_slope_short_config as build_best_slope_short_config
from scripts.build_five_coin_daily_filter_operation_pack import build_slope_short_config as build_ready_slope_short_config


class SlopeShortBundleDefaultsTest(TestCase):
    def test_best_parameter_bundle_uses_same_bar_reentry_block_only(self) -> None:
        config = build_best_slope_short_config(
            BestShortSpec(
                symbol="BTC-USDT-SWAP",
                profile_id="demo",
                profile_name="demo",
                strategy_id="ema55_slope_short",
                ema_period=55,
                ema_type="ema",
                trend_ema_period=55,
                trend_ema_type="ema",
                daily_filter_mode="disabled",
                daily_filter_ma_type="ema",
                daily_filter_period=5,
                notes="",
            )
        )

        self.assertTrue(config.ema55_slope_same_bar_reentry_block)
        self.assertFalse(config.ema55_slope_dynamic_exit_requires_bear_reentry)
        self.assertFalse(config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low)
        self.assertEqual(config.trend_ema_slope_filter_min_ratio, Decimal("-0.0005"))

    def test_ready_pack_uses_same_bar_reentry_block_only(self) -> None:
        config = build_ready_slope_short_config(
            symbol="BTC-USDT-SWAP",
            ma_type="ema",
            period=55,
            daily_mode="disabled",
            daily_ma_type="ema",
            daily_period=5,
            environment="demo",
        )

        self.assertTrue(config.ema55_slope_same_bar_reentry_block)
        self.assertFalse(config.ema55_slope_dynamic_exit_requires_bear_reentry)
        self.assertFalse(config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low)
        self.assertEqual(config.trend_ema_slope_filter_min_ratio, Decimal("-0.0005"))
