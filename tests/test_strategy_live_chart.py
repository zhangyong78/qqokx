from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest import TestCase

from okx_quant.analysis import BoxDetectionConfig, ChannelDetectionConfig, PivotDetectionConfig
from okx_quant.analysis.structure_models import PriceLine
from okx_quant.auto_channel_preview import build_auto_channel_preview_snapshot
from okx_quant.models import Candle
from okx_quant.strategy_live_chart import (
    StrategyLiveChartLayout,
    StrategyLiveChartMarker,
    StrategyLiveChartLineOverlay,
    StrategyLiveChartSnapshot,
    StrategyLiveChartTimeMarker,
    _ChartBounds,
    _layout_marker_label_positions,
    append_candles_to_snapshot,
    build_auto_channel_live_chart_snapshot,
    build_strategy_live_chart_snapshot,
    layout_price_to_y,
    layout_price_to_y_unclamped,
    line_trading_desk_max_view_start,
    line_trading_desk_visible_bar_count,
    slice_strategy_live_chart_snapshot,
    slice_strategy_live_chart_snapshot_with_desk_right_pad,
    strategy_live_chart_price_bounds,
)


class StrategyLiveChartHelpersTest(TestCase):
    def test_build_auto_channel_snapshot_adds_structure_overlays(self) -> None:
        candles = []
        for index in range(16):
            base = Decimal("10") + (Decimal("0.5") * Decimal(index))
            low = base + Decimal("0.8")
            high = base + Decimal("3")
            if index in {2, 8}:
                low = base
            if index in {4, 10}:
                high = base + Decimal("5")
            close = (low + high) / Decimal("2")
            candles.append(
                Candle(
                    ts=1714330800000 + index * 60_000,
                    open=close,
                    high=high,
                    low=low,
                    close=close,
                    volume=Decimal("1"),
                    confirmed=True,
                )
            )

        snapshot = build_auto_channel_live_chart_snapshot(
            session_id="auto",
            candles=candles,
            channel_config=ChannelDetectionConfig(
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
            box_config=BoxDetectionConfig(
                pivot=PivotDetectionConfig(left_bars=1, right_bars=1, atr_period=2, atr_multiplier=Decimal("0")),
                min_box_bars=8,
            ),
        )

        self.assertEqual(len(snapshot.band_overlays), 1)
        self.assertGreaterEqual(len(snapshot.line_overlays), 1)
        self.assertTrue(snapshot.point_overlays)
        self.assertIn("通道", snapshot.note)
        lower, upper = strategy_live_chart_price_bounds(snapshot)
        self.assertLessEqual(lower, Decimal("11"))
        self.assertGreaterEqual(upper, Decimal("20"))

    def test_auto_channel_preview_snapshots_are_renderable(self) -> None:
        channel = build_auto_channel_preview_snapshot("channel")
        box = build_auto_channel_preview_snapshot("box")

        self.assertTrue(channel.candles)
        self.assertTrue(channel.band_overlays or channel.box_overlays)
        self.assertEqual(channel.right_pad_bars, 50)
        if channel.band_overlays:
            self.assertGreaterEqual(channel.band_overlays[0].end_index, len(channel.candles) + 49)
        self.assertTrue(box.candles)
        self.assertTrue(box.band_overlays or box.box_overlays)
        self.assertIn("自动通道预览", channel.note)

    def test_auto_channel_snapshot_can_reserve_future_blank_bars(self) -> None:
        candles = []
        for index in range(16):
            base = Decimal("10") + (Decimal("0.5") * Decimal(index))
            low = base + Decimal("0.8")
            high = base + Decimal("3")
            if index in {2, 8}:
                low = base
            if index in {4, 10}:
                high = base + Decimal("5")
            close = (low + high) / Decimal("2")
            candles.append(
                Candle(
                    ts=1714330800000 + index * 60_000,
                    open=close,
                    high=high,
                    low=low,
                    close=close,
                    volume=Decimal("1"),
                    confirmed=True,
                )
            )

        snapshot = build_auto_channel_live_chart_snapshot(
            session_id="auto-pad",
            candles=candles,
            channel_config=ChannelDetectionConfig(
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
            right_pad_bars=50,
            channel_extend_bars=50,
        )

        self.assertEqual(snapshot.right_pad_bars, 50)
        self.assertEqual(len(snapshot.candles), 16)
        self.assertEqual(len(snapshot.band_overlays), 1)
        self.assertEqual(snapshot.band_overlays[0].end_index, 65)
        self.assertGreaterEqual(max(item.line.end_index for item in snapshot.line_overlays), 65)

    def test_slice_strategy_live_chart_snapshot_shifts_structure_overlays(self) -> None:
        candles = [
            Candle(ts=1000 + i * 60_000, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True)
            for i in range(20)
        ]
        snapshot = StrategyLiveChartSnapshot(
            session_id="slice",
            candles=tuple(candles),
            line_overlays=(
                StrategyLiveChartLineOverlay(
                    key="line",
                    label="line",
                    line=PriceLine(5, Decimal("100"), 15, Decimal("110")),
                    color="#2563eb",
                ),
            ),
        )

        sliced = slice_strategy_live_chart_snapshot(snapshot, 10, 5)

        self.assertEqual(len(sliced.line_overlays), 1)
        self.assertEqual(sliced.line_overlays[0].line.start_index, 0)
        self.assertEqual(sliced.line_overlays[0].line.end_index, 4)
        self.assertEqual(sliced.line_overlays[0].line.start_price, Decimal("105"))

    def test_append_candles_to_snapshot_preserves_structure_lines(self) -> None:
        base = StrategyLiveChartSnapshot(
            session_id="append",
            candles=(
                Candle(ts=1, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11"), volume=Decimal("1"), confirmed=True),
                Candle(ts=2, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12"), volume=Decimal("1"), confirmed=True),
            ),
            line_overlays=(
                StrategyLiveChartLineOverlay(
                    key="line",
                    label="line",
                    line=PriceLine(0, Decimal("10"), 10, Decimal("20")),
                    color="#2563eb",
                ),
            ),
            right_pad_bars=50,
        )

        updated = append_candles_to_snapshot(
            base,
            [
                Candle(ts=2, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12.2"), volume=Decimal("1"), confirmed=True),
                Candle(ts=3, open=Decimal("12"), high=Decimal("14"), low=Decimal("11"), close=Decimal("13"), volume=Decimal("1"), confirmed=True),
            ],
        )

        self.assertEqual(len(updated.candles), 3)
        self.assertEqual(updated.candles[-1].ts, 3)
        self.assertEqual(updated.candles[1].close, Decimal("12.2"))
        self.assertEqual(updated.line_overlays[0].line.end_index, 10)
        self.assertEqual(updated.right_pad_bars, 50)

    def test_build_strategy_live_chart_snapshot_deduplicates_duplicate_period_series_and_markers(self) -> None:
        candles = [
            Candle(ts=1, open=Decimal("100"), high=Decimal("102"), low=Decimal("99"), close=Decimal("101"), volume=Decimal("1"), confirmed=True),
            Candle(ts=2, open=Decimal("101"), high=Decimal("103"), low=Decimal("100"), close=Decimal("102"), volume=Decimal("1"), confirmed=True),
            Candle(ts=3, open=Decimal("102"), high=Decimal("104"), low=Decimal("101"), close=Decimal("103"), volume=Decimal("1"), confirmed=False),
        ]

        snapshot = build_strategy_live_chart_snapshot(
            session_id="S01",
            candles=candles,
            ema_period=21,
            trend_ema_period=55,
            reference_ema_period=55,
            pending_entry_prices=(Decimal("101"), Decimal("101"), Decimal("102")),
            entry_price=Decimal("100"),
            position_avg_price=Decimal("100"),
            stop_price=Decimal("95"),
            latest_price=Decimal("103"),
        )

        self.assertEqual([series.label for series in snapshot.series], ["EMA21", "EMA55"])
        self.assertEqual([marker.label for marker in snapshot.markers], ["\u6302\u5355", "\u6302\u53552", "\u5f00\u4ed3\u5747\u4ef7", "\u5f53\u524d\u6b62\u635f", "\u6700\u65b0\u4ef7"])
        self.assertFalse(snapshot.latest_candle_confirmed)
        self.assertEqual(snapshot.latest_price, Decimal("103"))

    def test_strategy_live_chart_price_bounds_include_marker_extremes(self) -> None:
        snapshot = StrategyLiveChartSnapshot(
            session_id="S01",
            candles=(
                Candle(ts=1, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True),
                Candle(ts=2, open=Decimal("100"), high=Decimal("102"), low=Decimal("98"), close=Decimal("101"), volume=Decimal("1"), confirmed=True),
            ),
            markers=build_strategy_live_chart_snapshot(
                session_id="S01",
                candles=[],
                entry_price=Decimal("90"),
                latest_price=Decimal("130"),
            ).markers,
        )

        lower, upper = strategy_live_chart_price_bounds(snapshot)

        self.assertLessEqual(lower, Decimal("90"))
        self.assertGreaterEqual(upper, Decimal("130"))

    def test_build_strategy_live_chart_snapshot_handles_empty_candles(self) -> None:
        snapshot = build_strategy_live_chart_snapshot(
            session_id="S02",
            candles=[],
            ema_period=21,
            pending_entry_prices=(Decimal("10"),),
            latest_price=Decimal("12"),
            note="loading",
        )

        self.assertEqual(snapshot.candles, ())
        self.assertEqual(snapshot.series, ())
        self.assertEqual([marker.label for marker in snapshot.markers], ["\u6302\u5355", "\u6700\u65b0\u4ef7"])
        self.assertEqual(snapshot.note, "loading")

    def test_build_strategy_live_chart_snapshot_includes_entry_time_marker(self) -> None:
        candles = [
            Candle(ts=1714330800000, open=Decimal("100"), high=Decimal("102"), low=Decimal("99"), close=Decimal("101"), volume=Decimal("1"), confirmed=True),
            Candle(ts=1714331100000, open=Decimal("101"), high=Decimal("103"), low=Decimal("100"), close=Decimal("102"), volume=Decimal("1"), confirmed=True),
        ]

        snapshot = build_strategy_live_chart_snapshot(
            session_id="S03",
            candles=candles,
            entry_time=datetime(2024, 4, 29, 9, 6),
        )

        self.assertEqual(len(snapshot.time_markers), 1)
        self.assertEqual(snapshot.time_markers[0].key, "entry_time")
        self.assertEqual(snapshot.time_markers[0].label, "开仓 04-29 09:06")

    def test_build_strategy_live_chart_snapshot_keeps_extra_time_markers(self) -> None:
        snapshot = build_strategy_live_chart_snapshot(
            session_id="S04",
            candles=[],
            time_markers=(
                StrategyLiveChartTimeMarker(
                    key="close",
                    label="平仓 04-29 10:00",
                    at=datetime(2024, 4, 29, 10, 0),
                    color="#cf222e",
                ),
            ),
        )

        self.assertEqual(len(snapshot.time_markers), 1)
        self.assertEqual(snapshot.time_markers[0].label, "平仓 04-29 10:00")

    def test_slice_with_desk_right_pad_fills_visible_width(self) -> None:
        candles = [
            Candle(ts=1000 + i * 60, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True)
            for i in range(80)
        ]
        snap = build_strategy_live_chart_snapshot(
            session_id="desk",
            candles=candles,
            ema_period=None,
            trend_ema_period=None,
            reference_ema_period=None,
        )
        n = len(snap.candles)
        vb = line_trading_desk_visible_bar_count(n, 30)
        vs_max = line_trading_desk_max_view_start(n, vb)
        self.assertGreaterEqual(vs_max, max(0, n - vb))
        sliced = slice_strategy_live_chart_snapshot_with_desk_right_pad(snap, vs_max, vb)
        self.assertEqual(len(sliced.candles), vb)
        for s in sliced.series:
            self.assertEqual(len(s.values), vb)

    def test_slice_with_desk_right_pad_respects_low_min_visible(self) -> None:
        candles = [
            Candle(ts=1000 + i * 60, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True)
            for i in range(60)
        ]
        snap = build_strategy_live_chart_snapshot(
            session_id="desk",
            candles=candles,
            ema_period=None,
            trend_ema_period=None,
            reference_ema_period=None,
        )
        sliced = slice_strategy_live_chart_snapshot_with_desk_right_pad(snap, 0, 12, min_visible_bars=5)
        self.assertEqual(len(sliced.candles), 12)

    def test_slice_with_right_pad_sets_series_plot_end(self) -> None:
        candles = [
            Candle(ts=1000 + i * 60, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True)
            for i in range(50)
        ]
        snap = build_strategy_live_chart_snapshot(
            session_id="desk",
            candles=candles,
            ema_period=21,
            trend_ema_period=None,
            reference_ema_period=None,
        )
        sliced = slice_strategy_live_chart_snapshot_with_desk_right_pad(snap, 35, 30)
        self.assertEqual(len(sliced.candles), 30)
        self.assertIsNotNone(sliced.series_plot_end_index)
        self.assertEqual(sliced.series_plot_end_index, 15)

    def test_layout_price_to_y_unclamped_extends_beyond_chart_for_out_of_band_prices(self) -> None:
        lay = StrategyLiveChartLayout(
            width=800,
            height=600,
            left=76.0,
            top=40.0,
            right=644.0,
            bottom=544.0,
            lower=Decimal("100"),
            upper=Decimal("200"),
            candle_step=4.0,
            candle_count=10,
        )
        y_mid = layout_price_to_y_unclamped(lay, Decimal("150"))
        self.assertGreater(y_mid, float(lay.top))
        self.assertLess(y_mid, float(lay.bottom))
        y_high = layout_price_to_y(lay, Decimal("300"))
        y_high_u = layout_price_to_y_unclamped(lay, Decimal("300"))
        self.assertAlmostEqual(y_high, float(lay.top), delta=1e-6)
        self.assertLess(y_high_u, float(lay.top))
        y_low = layout_price_to_y(lay, Decimal("0"))
        y_low_u = layout_price_to_y_unclamped(lay, Decimal("0"))
        self.assertAlmostEqual(y_low, float(lay.bottom), delta=1e-6)
        self.assertGreater(y_low_u, float(lay.bottom))

    def test_marker_labels_are_staggered_when_prices_are_close(self) -> None:
        bounds = _ChartBounds(left=76.0, top=40.0, right=644.0, bottom=544.0)
        markers = (
            StrategyLiveChartMarker(key="entry", label="开仓均价", price=Decimal("95.00"), color="#6f42c1"),
            StrategyLiveChartMarker(key="stop", label="当前止损", price=Decimal("95.03"), color="#cf222e"),
        )

        placements = _layout_marker_label_positions(
            markers,
            Decimal("90"),
            Decimal("100"),
            bounds,
            height=600,
            bounds_policy="full",
        )

        self.assertEqual(len(placements), 2)
        top_item, bottom_item = placements
        self.assertLess(top_item[2], bottom_item[2])
        self.assertGreaterEqual(bottom_item[2] - top_item[2], 24.0)
        moved_count = sum(1 for _, line_y, label_y in placements if abs(line_y - label_y) > 0.5)
        self.assertGreaterEqual(moved_count, 1)
