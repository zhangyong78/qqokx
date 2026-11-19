from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle
from okx_quant.strategies.general_trade_plan import (
    GeneralTradePlanConfig,
    evaluate_general_trade_plan,
    should_exit_general_trade_plan,
)


def _candle(index: int, open_price: str, high: str, low: str, close: str) -> Candle:
    return Candle(index * 3_600_000, Decimal(open_price), Decimal(high), Decimal(low), Decimal(close), Decimal("1"), True)


class GeneralTradePlanTest(TestCase):
    def test_range_middle_is_not_traded(self) -> None:
        candles = [_candle(index, "100", "106", "94", "100") for index in range(1, 30)]
        candles.append(_candle(30, "100", "103", "97", "101"))
        result = evaluate_general_trade_plan(
            candles,
            GeneralTradePlanConfig(mode="range", box_lookback_bars=20, slow_ema_period=10, box_min_width_atr=Decimal("0.5")),
        )
        self.assertIsNone(result.signal)
        self.assertIn("中部", result.reason)

    def test_range_support_bullish_reaction_generates_long(self) -> None:
        candles = [_candle(index, "100", "106", "94", "100") for index in range(1, 30)]
        candles.append(_candle(30, "95", "101", "93", "98"))
        result = evaluate_general_trade_plan(
            candles,
            GeneralTradePlanConfig(mode="range", box_lookback_bars=20, slow_ema_period=10, box_min_width_atr=Decimal("0.5")),
        )
        self.assertEqual(result.signal, "long")
        self.assertIn("下沿", result.reason)

    def test_range_breakout_requires_strong_body_and_ema_confirmation(self) -> None:
        candles = [_candle(index, "100", "106", "94", "100") for index in range(1, 30)]
        candles.append(_candle(30, "100", "115", "99", "114"))
        result = evaluate_general_trade_plan(
            candles,
            GeneralTradePlanConfig(mode="range_breakout", box_lookback_bars=20, slow_ema_period=10, box_min_width_atr=Decimal("0.5")),
        )
        self.assertEqual(result.signal, "long")
        self.assertIn("突破箱体", result.reason)

    def test_adverse_exit_uses_confirmed_close_against_ema(self) -> None:
        candles = [_candle(index, str(100 + index), str(102 + index), str(99 + index), str(101 + index)) for index in range(1, 30)]
        candles.append(_candle(30, "130", "131", "120", "120"))
        should_exit, reason = should_exit_general_trade_plan(
            candles,
            GeneralTradePlanConfig(mode="short_term", short_ema_period=20),
            "long",
        )
        self.assertTrue(should_exit)
        self.assertIn("跌破EMA", reason)
