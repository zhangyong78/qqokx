from dataclasses import asdict
from unittest import TestCase

from scripts.build_best_parameter_bundle import build_specs
from scripts.build_five_coin_daily_filter_operation_pack import build_ready_specs
from scripts.run_best_parameter_bundle_1h_standard_portfolio import deserialize_strategy_config


class DynamicLongBundleDefaultsTest(TestCase):
    def test_best_parameter_bundle_syncs_eth_final_long_profile(self) -> None:
        spec = next(item for item in build_specs() if item.profile_id == "dynamic_long_best_eth_v2")
        config = spec.config

        self.assertEqual(config.ema_period, 21)
        self.assertEqual(config.trend_ema_period, 55)
        self.assertEqual(config.entry_reference_ema_period, 55)
        self.assertEqual(str(config.atr_stop_multiplier), "1.5")
        self.assertEqual(str(config.atr_take_multiplier), "1.5")
        self.assertEqual(config.dynamic_break_even_trigger_r, 1)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 4)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 3)
        self.assertEqual(config.resolved_dynamic_protection_rules()[2].lock_r, 10)

    def test_standard_report_deserialize_preserves_dynamic_protection_rules(self) -> None:
        spec = next(item for item in build_specs() if item.profile_id == "dynamic_long_best_btc_v2")
        restored = deserialize_strategy_config(asdict(spec.config))
        rules = restored.resolved_dynamic_protection_rules()

        self.assertEqual([rule.trigger_r for rule in rules], [1, 4, 11])
        self.assertEqual(rules[1].lock_r, 1)
        self.assertEqual(rules[2].lock_r, 10)

    def test_best_parameter_bundle_syncs_sol_final_long_profile(self) -> None:
        spec = next(item for item in build_specs() if item.profile_id == "dynamic_long_best_sol_v2")
        config = spec.config

        self.assertEqual(config.ema_period, 21)
        self.assertEqual(config.trend_ema_period, 55)
        self.assertEqual(config.entry_reference_ema_period, 13)
        self.assertEqual(str(config.atr_stop_multiplier), "1")
        self.assertEqual(str(config.atr_take_multiplier), "1")
        self.assertEqual(config.dynamic_break_even_trigger_r, 3)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 5)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].trigger_r, 5)

    def test_best_parameter_bundle_syncs_doge_final_long_profile(self) -> None:
        spec = next(item for item in build_specs() if item.profile_id == "dynamic_long_best_doge_v2")
        config = spec.config

        self.assertEqual(config.ema_period, 5)
        self.assertEqual(config.trend_ema_period, 13)
        self.assertEqual(config.entry_reference_ema_period, 0)
        self.assertEqual(str(config.atr_stop_multiplier), "1")
        self.assertEqual(str(config.atr_take_multiplier), "1")
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[1].trigger_r, 6)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)

    def test_ready_pack_syncs_eth_final_long_profile(self) -> None:
        spec = next(item for item in build_ready_specs("demo") if item.profile_id == "eth-long-ma5-bjt08")
        config = spec.config

        self.assertEqual(config.entry_reference_ema_period, 55)
        self.assertEqual(str(config.atr_stop_multiplier), "1.5")
        self.assertEqual(str(config.atr_take_multiplier), "1.5")
        self.assertEqual(config.dynamic_break_even_trigger_r, 1)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 4)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 3)

    def test_ready_pack_syncs_sol_final_long_profile(self) -> None:
        spec = next(item for item in build_ready_specs("demo") if item.profile_id == "sol-long-ema5-bjt08")
        config = spec.config

        self.assertEqual(config.entry_reference_ema_period, 13)
        self.assertEqual(str(config.atr_stop_multiplier), "1")
        self.assertEqual(str(config.atr_take_multiplier), "1")
        self.assertEqual(config.dynamic_break_even_trigger_r, 3)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 5)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 2)

    def test_ready_pack_syncs_doge_final_long_profile(self) -> None:
        spec = next(item for item in build_ready_specs("demo") if item.profile_id == "doge-long-ma13-bjt08")
        config = spec.config

        self.assertEqual(config.entry_reference_ema_period, 0)
        self.assertEqual(str(config.atr_stop_multiplier), "1")
        self.assertEqual(str(config.atr_take_multiplier), "1")
        self.assertEqual(config.dynamic_break_even_trigger_r, 2)
        self.assertEqual(config.ema55_slope_lock_profit_trigger_r, 6)
        self.assertEqual(config.dynamic_first_lock_r, 1)
        self.assertEqual(config.max_entries_per_trend, 2)
        self.assertEqual(config.resolved_dynamic_protection_rules()[0].trigger_r, 2)
