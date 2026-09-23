from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle
from roll_terminal_qt.spread_chart_window import (
    CHART_BAR_OPTIONS,
    _BAR_INTERVAL_MS,
    _aligned_spread_candles,
    _compact_spread_axis_range,
    _compact_spread_wick_bounds,
    _compact_spread_wick_cap,
    _load_pair_candles,
)


def _candles(start: int, count: int) -> list[Candle]:
    return [
        Candle(
            ts=start + index * 900_000,
            open=Decimal("100") + index,
            high=Decimal("101") + index,
            low=Decimal("99") + index,
            close=Decimal("100.5") + index,
            volume=Decimal("1"),
            confirmed=True,
        )
        for index in range(count)
    ]


class SpreadChartHistoryAlignmentTest(TestCase):
    def test_includes_daily_period_with_one_day_alignment_interval(self) -> None:
        self.assertIn(("日线", "1D"), CHART_BAR_OPTIONS)
        self.assertEqual(_BAR_INTERVAL_MS["1D"], 24 * 60 * 60 * 1000)

    def test_loads_newer_leg_from_older_leg_window(self) -> None:
        left = _candles(1_000_000, 4)
        right_latest = _candles(10_000_000, 4)
        right_common = _candles(1_000_000, 4)

        class FakeClient:
            def get_candles_history(self, inst_id: str, bar: str, *, limit: int) -> list[Candle]:
                return left if inst_id == "EXPIRED" else right_latest

            def get_candles_history_range(
                self,
                inst_id: str,
                bar: str,
                *,
                start_ts: int,
                end_ts: int,
                limit: int,
            ) -> list[Candle]:
                self.range_request = (inst_id, start_ts, end_ts, limit)
                return right_common

        client = FakeClient()
        loaded_left, loaded_right = _load_pair_candles(client, "EXPIRED", "SPOT", "15m", 4)

        self.assertEqual([item.ts for item in loaded_left], [item.ts for item in left])
        self.assertEqual([item.ts for item in loaded_right], [item.ts for item in right_common])
        self.assertEqual(client.range_request[0], "SPOT")
        self.assertEqual(len(_aligned_spread_candles(loaded_left, loaded_right)), 4)


class SpreadChartCompactRangeTest(TestCase):
    def test_compact_wicks_do_not_fill_the_whole_chart_with_vertical_lines(self) -> None:
        candles = [
            Candle(
                ts=1_000_000 + index,
                open=Decimal("100") + index,
                high=Decimal("1000") if index == 0 else Decimal("104") + index,
                low=Decimal("-800") if index == 0 else Decimal("98") + index,
                close=Decimal("101") + index,
                volume=Decimal("0"),
                confirmed=True,
            )
            for index in range(5)
        ]

        wick_cap = _compact_spread_wick_cap(candles)
        display_high, display_low = _compact_spread_wick_bounds(candles[0], wick_cap)

        self.assertLess(display_high, candles[0].high)
        self.assertGreater(display_low, candles[0].low)
        self.assertGreaterEqual(display_high, candles[0].close)
        self.assertLessEqual(display_low, candles[0].open)

    def test_uses_candle_bodies_not_an_extreme_wick_for_default_axis(self) -> None:
        candles = [
            Candle(
                ts=1_000_000,
                open=Decimal("100"),
                high=Decimal("3_000"),
                low=Decimal("-900"),
                close=Decimal("103"),
                volume=Decimal("0"),
                confirmed=True,
            ),
            Candle(
                ts=1_900_000,
                open=Decimal("103"),
                high=Decimal("120"),
                low=Decimal("95"),
                close=Decimal("106"),
                volume=Decimal("0"),
                confirmed=True,
            ),
        ]

        lower, upper = _compact_spread_axis_range(candles)

        self.assertLess(lower, Decimal("100"))
        self.assertGreater(upper, Decimal("106"))
        self.assertGreater(lower, Decimal("0"))
        self.assertLess(upper, Decimal("200"))
