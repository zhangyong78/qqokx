from decimal import Decimal
from unittest import TestCase

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle
from okx_quant.deribit_volatility_ui import (
    _aggregate_price_candles_to_resolution,
    _aggregate_candles_to_resolution,
    _align_candles_by_timestamp,
    _default_chart_viewport,
    DeribitVolatilityWindow,
    _hourly_history_limit,
    _max_limit_for_resolution_value,
    _merge_deribit_candles,
    _merge_price_candles,
    _next_refresh_delay_ms,
    _snapshot_last_ts,
    _normalize_chart_viewport,
    _pan_chart_viewport,
    _required_hourly_limit,
    _to_average_price_candles,
    _to_average_volatility_candles,
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

    def test_aggregate_candles_to_daily_with_anchor(self) -> None:
        candles = [
            DeribitVolatilityCandle(ts=57_600_000, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11")),
            DeribitVolatilityCandle(ts=61_200_000, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12")),
            DeribitVolatilityCandle(ts=144_000_000, open=Decimal("12"), high=Decimal("14"), low=Decimal("11"), close=Decimal("13")),
        ]

        aggregated = _aggregate_candles_to_resolution(candles, 86_400_000, anchor_offset_ms=57_600_000)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0].ts, 57_600_000)
        self.assertEqual(aggregated[0].open, Decimal("10"))
        self.assertEqual(aggregated[0].close, Decimal("12"))
        self.assertEqual(aggregated[1].ts, 144_000_000)
        self.assertEqual(aggregated[1].open, Decimal("12"))
        self.assertEqual(aggregated[1].close, Decimal("13"))

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

    def test_align_candles_by_timestamp(self) -> None:
        volatility = [
            DeribitVolatilityCandle(ts=0, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11")),
            DeribitVolatilityCandle(ts=1_000, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12")),
        ]
        spot = [
            Candle(ts=1_000, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100.5"), volume=Decimal("1"), confirmed=True),
            Candle(ts=2_000, open=Decimal("101"), high=Decimal("102"), low=Decimal("100"), close=Decimal("101.5"), volume=Decimal("1"), confirmed=True),
        ]

        aligned_volatility, aligned_spot = _align_candles_by_timestamp(volatility, spot)

        self.assertEqual(len(aligned_volatility), 1)
        self.assertEqual(aligned_volatility[0].ts, 1_000)
        self.assertEqual(aligned_spot[0].ts, 1_000)

    def test_aggregate_price_candles_to_4h(self) -> None:
        candles = [
            Candle(ts=0, open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("105"), volume=Decimal("1"), confirmed=True),
            Candle(ts=3_600_000, open=Decimal("105"), high=Decimal("112"), low=Decimal("104"), close=Decimal("108"), volume=Decimal("2"), confirmed=True),
            Candle(ts=7_200_000, open=Decimal("108"), high=Decimal("115"), low=Decimal("107"), close=Decimal("111"), volume=Decimal("3"), confirmed=True),
            Candle(ts=10_800_000, open=Decimal("111"), high=Decimal("116"), low=Decimal("109"), close=Decimal("113"), volume=Decimal("4"), confirmed=True),
            Candle(ts=14_400_000, open=Decimal("113"), high=Decimal("120"), low=Decimal("112"), close=Decimal("118"), volume=Decimal("5"), confirmed=True),
        ]

        aggregated = _aggregate_price_candles_to_resolution(candles, 14_400_000)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0].ts, 0)
        self.assertEqual(aggregated[0].open, Decimal("100"))
        self.assertEqual(aggregated[0].high, Decimal("116"))
        self.assertEqual(aggregated[0].low, Decimal("90"))
        self.assertEqual(aggregated[0].close, Decimal("113"))
        self.assertEqual(aggregated[0].volume, Decimal("10"))
        self.assertTrue(aggregated[0].confirmed)

    def test_aggregate_price_candles_to_daily_with_anchor(self) -> None:
        candles = [
            Candle(ts=57_600_000, open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("105"), volume=Decimal("1"), confirmed=True),
            Candle(ts=61_200_000, open=Decimal("105"), high=Decimal("112"), low=Decimal("104"), close=Decimal("108"), volume=Decimal("2"), confirmed=True),
            Candle(ts=144_000_000, open=Decimal("108"), high=Decimal("115"), low=Decimal("107"), close=Decimal("111"), volume=Decimal("3"), confirmed=True),
        ]

        aggregated = _aggregate_price_candles_to_resolution(candles, 86_400_000, anchor_offset_ms=57_600_000)

        self.assertEqual(len(aggregated), 2)
        self.assertEqual(aggregated[0].ts, 57_600_000)
        self.assertEqual(aggregated[0].open, Decimal("100"))
        self.assertEqual(aggregated[0].close, Decimal("108"))
        self.assertEqual(aggregated[0].volume, Decimal("3"))
        self.assertEqual(aggregated[1].ts, 144_000_000)
        self.assertEqual(aggregated[1].close, Decimal("111"))

    def test_average_volatility_candles(self) -> None:
        candles = [
            DeribitVolatilityCandle(ts=0, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11")),
            DeribitVolatilityCandle(ts=1_000, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12")),
        ]

        averaged = _to_average_volatility_candles(candles)

        self.assertEqual(len(averaged), 2)
        self.assertEqual(averaged[0].open, Decimal("10.5"))
        self.assertEqual(averaged[0].close, Decimal("10.5"))
        self.assertEqual(averaged[1].open, Decimal("10.5"))
        self.assertEqual(averaged[1].close, Decimal("11.5"))

    def test_average_price_candles(self) -> None:
        candles = [
            Candle(ts=0, open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("105"), volume=Decimal("1"), confirmed=True),
            Candle(ts=1_000, open=Decimal("105"), high=Decimal("112"), low=Decimal("104"), close=Decimal("108"), volume=Decimal("2"), confirmed=True),
        ]

        averaged = _to_average_price_candles(candles)

        self.assertEqual(len(averaged), 2)
        self.assertEqual(averaged[0].open, Decimal("102.5"))
        self.assertEqual(averaged[0].close, Decimal("101.25"))
        self.assertEqual(averaged[1].open, Decimal("101.88"))
        self.assertEqual(averaged[1].close, Decimal("107.25"))
        self.assertEqual(averaged[1].volume, Decimal("2"))

    def test_required_hourly_limit_scales_with_resolution(self) -> None:
        self.assertEqual(_required_hourly_limit("3600", 300), 348)
        self.assertEqual(_required_hourly_limit("14400", 300), 1248)
        self.assertEqual(_required_hourly_limit("1D", 300), 7248)

    def test_hourly_history_limit_covers_requested_range(self) -> None:
        self.assertEqual(_hourly_history_limit(0, 0), 1)
        self.assertEqual(_hourly_history_limit(0, 3_600_000 * 24), 26)

    def test_max_limit_for_resolution_value(self) -> None:
        self.assertEqual(_max_limit_for_resolution_value("3600"), 30000)
        self.assertEqual(_max_limit_for_resolution_value("14400"), 10000)
        self.assertEqual(_max_limit_for_resolution_value("1D"), 10000)

    def test_format_fetch_error_message_timeout(self) -> None:
        window = object.__new__(DeribitVolatilityWindow)
        self.assertEqual(
            DeribitVolatilityWindow._format_fetch_error_message(window, TimeoutError("The read operation timed out")),
            "网络读取超时，请稍后重试。",
        )

    def test_default_chart_viewport_targets_latest_requested_window(self) -> None:
        self.assertEqual(_default_chart_viewport(1000, 300, min_visible=24), (700, 300))
        self.assertEqual(_default_chart_viewport(10, 300, min_visible=24), (0, 10))

    def test_merge_deribit_candles_replaces_same_timestamp(self) -> None:
        existing = [
            DeribitVolatilityCandle(ts=0, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11")),
            DeribitVolatilityCandle(ts=1_000, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12")),
        ]
        incoming = [
            DeribitVolatilityCandle(ts=1_000, open=Decimal("20"), high=Decimal("21"), low=Decimal("19"), close=Decimal("20")),
            DeribitVolatilityCandle(ts=2_000, open=Decimal("30"), high=Decimal("31"), low=Decimal("29"), close=Decimal("30")),
        ]

        merged = _merge_deribit_candles(existing, incoming)

        self.assertEqual([candle.ts for candle in merged], [0, 1_000, 2_000])
        self.assertEqual(merged[1].open, Decimal("20"))
        self.assertEqual(merged[2].close, Decimal("30"))

    def test_merge_price_candles_replaces_same_timestamp(self) -> None:
        existing = [
            Candle(ts=0, open=Decimal("100"), high=Decimal("110"), low=Decimal("90"), close=Decimal("105"), volume=Decimal("1"), confirmed=True),
            Candle(ts=1_000, open=Decimal("105"), high=Decimal("112"), low=Decimal("104"), close=Decimal("108"), volume=Decimal("2"), confirmed=True),
        ]
        incoming = [
            Candle(ts=1_000, open=Decimal("200"), high=Decimal("210"), low=Decimal("190"), close=Decimal("205"), volume=Decimal("3"), confirmed=True),
            Candle(ts=2_000, open=Decimal("300"), high=Decimal("310"), low=Decimal("290"), close=Decimal("305"), volume=Decimal("4"), confirmed=True),
        ]

        merged = _merge_price_candles(existing, incoming)

        self.assertEqual([candle.ts for candle in merged], [0, 1_000, 2_000])
        self.assertEqual(merged[1].open, Decimal("200"))
        self.assertEqual(merged[2].close, Decimal("305"))

    def test_snapshot_last_ts_prefers_aligned_volatility(self) -> None:
        snapshot = type(
            "Snapshot",
            (),
            {
                "aligned_volatility_candles": [DeribitVolatilityCandle(ts=2_000, open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"))],
                "volatility_candles": [DeribitVolatilityCandle(ts=1_000, open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"))],
                "aligned_spot_candles": [],
                "spot_candles": [],
            },
        )()

        self.assertEqual(_snapshot_last_ts(snapshot), 2_000)

    def test_next_refresh_delay_follows_last_candle_boundary(self) -> None:
        snapshot = type(
            "Snapshot",
            (),
            {
                "aligned_volatility_candles": [DeribitVolatilityCandle(ts=3_600_000, open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"))],
                "volatility_candles": [],
                "aligned_spot_candles": [],
                "spot_candles": [],
            },
        )()

        delay = _next_refresh_delay_ms(snapshot, "3600", now_ms=5_100_000)

        self.assertEqual(delay, 2_105_000)
