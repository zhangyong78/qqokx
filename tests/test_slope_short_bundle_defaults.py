from decimal import Decimal
from unittest import TestCase

from scripts.build_best_parameter_bundle import build_slope_short_config as build_best_slope_short_config
from scripts.build_five_coin_daily_filter_operation_pack import build_ready_specs
from scripts.build_five_coin_daily_filter_operation_pack import build_slope_short_config as build_ready_slope_short_config


class SlopeShortBundleDefaultsTest(TestCase):
    def test_best_parameter_bundle_uses_same_bar_reentry_block_only(self) -> None:
        config = build_best_slope_short_config(
            symbol="BTC-USDT-SWAP",
            ema_period=55,
            ema_type="ema",
            trend_ema_period=55,
            trend_ema_type="ema",
            daily_filter_period=5,
            time_stop_break_even_bars=0,
            daily_filter_boundary="exchange",
            daily_filter_scope="both",
        )

        self.assertTrue(config.ema55_slope_same_bar_reentry_block)
        self.assertFalse(config.ema55_slope_dynamic_exit_requires_bear_reentry)
        self.assertFalse(config.ema55_slope_dynamic_exit_bear_reentry_break_prev_low)
        self.assertEqual(config.trend_ema_slope_filter_min_ratio, Decimal("-0.0005"))

    def test_best_parameter_bundle_can_build_eth_final_short_profile(self) -> None:
        config = build_best_slope_short_config(
            symbol="ETH-USDT-SWAP",
            ema_period=61,
            ema_type="ma",
            trend_ema_period=61,
            trend_ema_type="ma",
            atr_period=11,
            dynamic_break_even_trigger_r=3,
            dynamic_first_lock_r=5,
            dynamic_protection_rules=(
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
            ema55_slope_lock_profit_trigger_r=6,
            time_stop_break_even_bars=0,
        )

        self.assertEqual(config.atr_period, 11)
        self.assertEqual(config.dynamic_break_even_trigger_r, 3)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertEqual(len(config.dynamic_protection_rules), 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].trigger_r, 6)

    def test_best_parameter_bundle_can_build_sol_final_short_profile(self) -> None:
        config = build_best_slope_short_config(
            symbol="SOL-USDT-SWAP",
            ema_period=20,
            ema_type="ma",
            trend_ema_period=20,
            trend_ema_type="ma",
            atr_period=15,
            dynamic_break_even_trigger_r=2,
            dynamic_first_lock_r=5,
            dynamic_protection_rules=(
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
            ema55_slope_lock_profit_trigger_r=6,
            time_stop_break_even_bars=10,
        )

        self.assertEqual(config.atr_period, 15)
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertEqual(len(config.dynamic_protection_rules), 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].lock_r, 5)

    def test_best_parameter_bundle_can_build_doge_final_short_profile(self) -> None:
        config = build_best_slope_short_config(
            symbol="DOGE-USDT-SWAP",
            ema_period=21,
            ema_type="ma",
            trend_ema_period=21,
            trend_ema_type="ma",
            atr_period=13,
            dynamic_break_even_trigger_r=2,
            dynamic_first_lock_r=5,
            dynamic_protection_rules=(
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
            ema55_slope_lock_profit_trigger_r=6,
            time_stop_break_even_bars=10,
        )

        self.assertEqual(config.atr_period, 13)
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].lock_r, 5)

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

    def test_ready_pack_syncs_eth_final_short_profile(self) -> None:
        spec = next(
            item for item in build_ready_specs("demo") if item.profile_id == "eth-short-ema55-ema55-bjt08"
        )
        config = spec.config

        self.assertEqual(config.ema_type, "ma")
        self.assertEqual(config.ema_period, 61)
        self.assertEqual(config.atr_period, 11)
        self.assertEqual(config.dynamic_break_even_trigger_r, 3)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertTrue(config.ema55_slope_exit_enabled)
        self.assertEqual(config.time_stop_break_even_bars, 0)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].lock_r, 5)

    def test_ready_pack_syncs_sol_final_short_profile(self) -> None:
        spec = next(
            item for item in build_ready_specs("demo") if item.profile_id == "sol-short-ma20-ema21-bjt08"
        )
        config = spec.config

        self.assertEqual(config.ema_type, "ma")
        self.assertEqual(config.ema_period, 20)
        self.assertEqual(config.atr_period, 15)
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertTrue(config.ema55_slope_exit_enabled)
        self.assertEqual(config.time_stop_break_even_bars, 0)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].lock_r, 5)

    def test_ready_pack_syncs_doge_final_short_profile(self) -> None:
        spec = next(
            item for item in build_ready_specs("demo") if item.profile_id == "doge-short-ma55-ma20-bjt08"
        )
        config = spec.config

        self.assertEqual(config.ema_type, "ma")
        self.assertEqual(config.ema_period, 21)
        self.assertEqual(config.atr_period, 13)
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 5)
        self.assertTrue(config.ema55_slope_exit_enabled)
        self.assertEqual(config.time_stop_break_even_bars, 10)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].lock_r, 5)
