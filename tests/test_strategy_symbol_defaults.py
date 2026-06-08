from unittest import TestCase

from okx_quant.strategy_catalog import (
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
        self.assertEqual(defaults["trend_ema_type"], "ma")
        self.assertEqual(defaults["trend_ema_period"], 50)
        self.assertNotIn("startup_chase_window_seconds", defaults)

    def test_slope_short_btc_defaults_follow_best_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_EMA55_SLOPE_SHORT_ID,
            "BTC-USDT-SWAP",
            "launcher",
        )

        self.assertEqual(defaults["ema_type"], "ema")
        self.assertEqual(defaults["ema_period"], 34)
        self.assertEqual(defaults["trend_ema_period"], 34)
        self.assertEqual(defaults["daily_filter_mode"], "close_vs_ma")
        self.assertEqual(defaults["daily_filter_ma_type"], "ema")
        self.assertEqual(defaults["daily_filter_period"], 21)
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
        self.assertEqual(defaults["daily_filter_ma_type"], "ema")
        self.assertEqual(defaults["daily_filter_period"], 21)

    def test_body_retest_bnb_defaults_follow_best_profile(self) -> None:
        defaults = get_strategy_symbol_parameter_defaults(
            STRATEGY_BODY_RETEST_SHORT_ID,
            "BNB-USDT-SWAP",
            "launcher",
        )

        self.assertEqual(defaults["ema_type"], "ma")
        self.assertEqual(defaults["ema_period"], 20)
        self.assertEqual(defaults["daily_filter_mode"], "weak_day")
        self.assertEqual(defaults["body_retest_watch_bars"], 6)
        self.assertEqual(defaults["risk_amount"], "10")

    def test_slope_strategy_display_name_is_renamed(self) -> None:
        definition = get_strategy_definition(STRATEGY_EMA55_SLOPE_SHORT_ID)

        self.assertEqual(definition.name, "均线斜率做空")
