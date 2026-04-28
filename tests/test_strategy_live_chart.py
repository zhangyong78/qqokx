from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle
from okx_quant.strategy_live_chart import (
    StrategyLiveChartSnapshot,
    StrategyLiveChartTimeMarker,
    build_strategy_live_chart_snapshot,
    strategy_live_chart_price_bounds,
)


class StrategyLiveChartHelpersTest(TestCase):
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
