from decimal import Decimal
from unittest import TestCase

from okx_quant.indicators import atr, bollinger_bands, ema, macd, sma
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

    def test_sma_returns_none_before_window_fills(self) -> None:
        values = [Decimal("10"), Decimal("11"), Decimal("12"), Decimal("13")]
        result = sma(values, period=3)
        self.assertEqual(result[:2], [None, None])
        self.assertEqual(result[2], Decimal("11"))
        self.assertEqual(result[3], Decimal("12"))

    def test_macd_returns_same_length_outputs(self) -> None:
        values = [Decimal(str(100 + index)) for index in range(40)]
        macd_line, signal_line, histogram = macd(values)
        self.assertEqual(len(macd_line), len(values))
        self.assertEqual(len(signal_line), len(values))
        self.assertEqual(len(histogram), len(values))
        self.assertTrue(macd_line[-1] > signal_line[-1])

    def test_bollinger_bands_return_middle_upper_lower(self) -> None:
        values = [Decimal(str(100 + index)) for index in range(25)]
        middle, upper, lower = bollinger_bands(values, period=20)
        self.assertEqual(len(middle), len(values))
        self.assertIsNone(middle[18])
        self.assertIsNotNone(middle[-1])
        self.assertIsNotNone(upper[-1])
        self.assertIsNotNone(lower[-1])
        self.assertTrue(upper[-1] > middle[-1] > lower[-1])
