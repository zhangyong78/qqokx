from decimal import Decimal
from unittest import TestCase

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle


class IndicatorsTest(TestCase):
    def test_ema_tracks_series_length(self) -> None:
        values = [Decimal("10"), Decimal("11"), Decimal("12"), Decimal("13")]
        result = ema(values, period=3)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], Decimal("10"))
        self.assertTrue(result[-1] > result[-2])

    def test_atr_returns_latest_value(self) -> None:
        candles = [
            Candle(1, Decimal("10"), Decimal("11"), Decimal("9"), Decimal("10"), Decimal("1"), True),
            Candle(2, Decimal("10"), Decimal("12"), Decimal("9"), Decimal("11"), Decimal("1"), True),
            Candle(3, Decimal("11"), Decimal("13"), Decimal("10"), Decimal("12"), Decimal("1"), True),
            Candle(4, Decimal("12"), Decimal("14"), Decimal("11"), Decimal("13"), Decimal("1"), True),
        ]
        result = atr(candles, period=3)
        self.assertIsNotNone(result[-1])
        self.assertEqual(result[0], None)
