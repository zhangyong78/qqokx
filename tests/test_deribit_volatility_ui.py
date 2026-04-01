from decimal import Decimal
from unittest import TestCase

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.deribit_volatility_ui import (
    _aggregate_candles_to_resolution,
    _normalize_chart_viewport,
    _pan_chart_viewport,
    _zoom_chart_viewport,
)


class DeribitVolatilityUiTest(TestCase):
    def test_aggregate_candles_to_4h(self) -> None:
        candles = [
            DeribitVolatilityCandle(ts=0, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11")),
            DeribitVolatilityCandle(ts=3_600_000, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12")),
            DeribitVolatilityCandle(ts=7_200_000, open=Decimal("12"), high=Decimal("14"), low=Decimal("11"), close=Decimal("13")),
            DeribitVolatilityCandle(ts=10_800_000, open=Decimal("13"), high=Decimal("15"), low=Decimal("12"), close=Decimal("14")),
            DeribitVolatilityCandle(ts=14_400_000, open=Decimal("14"), high=Decimal("16"), low=Decimal("13"), close=Decimal("15")),
        ]

        aggregated = _aggregate_candles_to_resolution(candles, 14_400_000)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0].ts, 0)
        self.assertEqual(aggregated[0].open, Decimal("10"))
        self.assertEqual(aggregated[0].high, Decimal("15"))
        self.assertEqual(aggregated[0].low, Decimal("9"))
        self.assertEqual(aggregated[0].close, Decimal("14"))
        self.assertEqual(aggregated[1].ts, 14_400_000)
        self.assertEqual(aggregated[1].close, Decimal("15"))

    def test_normalize_chart_viewport_clamps_bounds(self) -> None:
        start_index, visible_count = _normalize_chart_viewport(90, 40, 100, min_visible=24)
        self.assertEqual((start_index, visible_count), (60, 40))

    def test_zoom_chart_viewport_zooms_in_around_anchor(self) -> None:
        start_index, visible_count = _zoom_chart_viewport(
            start_index=0,
            visible_count=100,
            total_count=200,
            anchor_ratio=0.5,
            zoom_in=True,
            min_visible=24,
        )
        self.assertEqual(visible_count, 80)
        self.assertEqual(start_index, 10)

    def test_pan_chart_viewport_moves_window(self) -> None:
        start_index = _pan_chart_viewport(20, 50, 200, 15, min_visible=24)
        self.assertEqual(start_index, 35)
