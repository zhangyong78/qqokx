from decimal import Decimal
from unittest import TestCase

from okx_quant.analysis import (
    BoxDetectionConfig,
    ChannelDetectionConfig,
    PivotDetectionConfig,
    TrendlineDetectionConfig,
    TriangleDetectionConfig,
    detect_boxes,
    detect_channels,
    detect_pivots,
    detect_trendlines,
    detect_triangles,
)
from okx_quant.models import Candle


def _candle(index: int, low: str, high: str, close: str | None = None) -> Candle:
    low_value = Decimal(low)
    high_value = Decimal(high)
    close_value = Decimal(close) if close is not None else (low_value + high_value) / Decimal("2")
    return Candle(
        ts=index,
        open=close_value,
        high=high_value,
        low=low_value,
        close=close_value,
        volume=Decimal("1"),
        confirmed=True,
    )


class AutoChannelAnalysisTest(TestCase):
    def test_detect_pivots_finds_fractal_highs_and_lows(self) -> None:
        candles = [
            _candle(0, "10", "12"),
            _candle(1, "11", "13"),
            _candle(2, "9", "14"),
            _candle(3, "12", "16"),
            _candle(4, "11", "15"),
            _candle(5, "13", "17"),
            _candle(6, "10", "14"),
        ]

        pivots = detect_pivots(
            candles,
            PivotDetectionConfig(left_bars=1, right_bars=1, atr_period=2, atr_multiplier=Decimal("0")),
        )

        self.assertIn(("low", 2, Decimal("9")), [(item.kind, item.index, item.price) for item in pivots])
        self.assertIn(("high", 3, Decimal("16")), [(item.kind, item.index, item.price) for item in pivots])
        self.assertIn(("high", 5, Decimal("17")), [(item.kind, item.index, item.price) for item in pivots])

    def test_detect_channels_builds_ascending_parallel_channel(self) -> None:
        candles = []
        for index in range(16):
            base = Decimal("10") + (Decimal("0.5") * Decimal(index))
            low = base + Decimal("0.8")
            high = base + Decimal("3")
            if index in {2, 8}:
                low = base
            if index in {4, 10}:
                high = base + Decimal("5")
            candles.append(_candle(index, str(low), str(high)))

        channels = detect_channels(
            candles,
            ChannelDetectionConfig(
                pivot=PivotDetectionConfig(
                    left_bars=1,
                    right_bars=1,
                    atr_period=2,
                    atr_multiplier=Decimal("0"),
                    min_index_distance=1,
                ),
                min_anchor_distance=4,
                min_channel_bars=8,
                max_violations=0,
            ),
        )

        self.assertTrue(channels)
        best = channels[0]
        self.assertEqual(best.kind, "ascending")
        self.assertEqual(best.base_pivots[0].index, 2)
        self.assertEqual(best.base_pivots[1].index, 8)
        self.assertEqual(best.violations, 0)
        self.assertGreaterEqual(best.touches, 4)
        self.assertEqual(best.width, Decimal("5.0"))

    def test_detect_boxes_finds_repeated_horizontal_boundaries(self) -> None:
        candles = [
            _candle(0, "11", "13"),
            _candle(1, "11", "14"),
            _candle(2, "11", "15"),
            _candle(3, "12", "14"),
            _candle(4, "10", "14"),
            _candle(5, "11", "13"),
            _candle(6, "11", "15"),
            _candle(7, "12", "14"),
            _candle(8, "10", "14"),
            _candle(9, "11", "13"),
            _candle(10, "11", "15"),
            _candle(11, "11", "14"),
        ]

        boxes = detect_boxes(
            candles,
            BoxDetectionConfig(
                pivot=PivotDetectionConfig(
                    left_bars=1,
                    right_bars=1,
                    atr_period=2,
                    atr_multiplier=Decimal("0"),
                    min_index_distance=1,
                ),
                min_box_bars=8,
                min_touches_per_side=2,
                max_violations=0,
            ),
        )

        self.assertTrue(boxes)
        best = boxes[0]
        self.assertEqual(best.upper, Decimal("15"))
        self.assertEqual(best.lower, Decimal("10"))
        self.assertGreaterEqual(best.upper_touches, 2)
        self.assertGreaterEqual(best.lower_touches, 2)
        self.assertEqual(best.violations, 0)

    def test_detect_trendlines_finds_descending_resistance(self) -> None:
        candles = [
            _candle(0, "10", "15"),
            _candle(1, "12", "20"),
            _candle(2, "11", "14"),
            _candle(3, "13", "18"),
            _candle(4, "12", "13"),
            _candle(5, "14", "16"),
            _candle(6, "13", "12"),
        ]

        trendlines = detect_trendlines(
            candles,
            TrendlineDetectionConfig(
                pivot=PivotDetectionConfig(left_bars=1, right_bars=1, atr_period=2, atr_multiplier=Decimal("0")),
                min_anchor_distance=2,
                min_line_bars=5,
                max_violations=1,
            ),
        )

        self.assertTrue(trendlines)
        self.assertEqual(trendlines[0].kind, "resistance")
        self.assertLess(trendlines[0].slope, 0)
        self.assertGreaterEqual(trendlines[0].touches, 2)

    def test_detect_triangles_finds_symmetrical_triangle(self) -> None:
        candles = [
            _candle(0, "11", "17"),
            _candle(1, "12", "20"),
            _candle(2, "10", "16"),
            _candle(3, "13", "18"),
            _candle(4, "12.5", "15"),
            _candle(5, "14", "16"),
            _candle(6, "12", "14.5"),
            _candle(7, "13", "14.2"),
        ]

        triangles = detect_triangles(
            candles,
            TriangleDetectionConfig(
                pivot=PivotDetectionConfig(left_bars=1, right_bars=1, atr_period=2, atr_multiplier=Decimal("0")),
                min_anchor_distance=2,
                min_triangle_bars=6,
                max_violations=2,
            ),
        )

        self.assertTrue(triangles)
        best = triangles[0]
        self.assertEqual(best.kind, "symmetrical")
        self.assertGreater(best.apex_index, best.end_index)
        self.assertGreaterEqual(best.touches, 4)
