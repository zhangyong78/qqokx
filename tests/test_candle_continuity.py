from decimal import Decimal
from unittest import TestCase

from okx_quant.candle_continuity import (
    bar_step_ms,
    count_bar_opens_in_half_open_range,
    find_candle_gaps_half_open_range,
    find_candle_gaps_in_window,
    total_missing_bars,
)
from okx_quant.models import Candle


def _c(ts: int) -> Candle:
    p = Decimal(str(ts))
    return Candle(ts=ts, open=p, high=p, low=p, close=p, volume=Decimal("1"), confirmed=True)


class CandleContinuityTest(TestCase):
    def test_bar_step_ms(self) -> None:
        self.assertEqual(bar_step_ms("15m"), 900_000)

    def test_count_bar_opens(self) -> None:
        self.assertEqual(count_bar_opens_in_half_open_range(0, 900_000, 900_000), 1)
        self.assertEqual(count_bar_opens_in_half_open_range(0, 2_700_000, 900_000), 3)

    def test_half_open_no_gaps_three_bars(self) -> None:
        candles = [_c(0), _c(900_000), _c(1_800_000)]
        gaps, w = find_candle_gaps_half_open_range(
            candles,
            start_ms=0,
            end_exclusive_ms=2_700_000,
            step_ms=900_000,
        )
        self.assertEqual(gaps, [])
        self.assertEqual(w, [])

    def test_half_open_head_and_middle_gaps(self) -> None:
        candles = [_c(1_800_000), _c(3_600_000)]
        gaps, w = find_candle_gaps_half_open_range(
            candles,
            start_ms=0,
            end_exclusive_ms=4_500_000,
            step_ms=900_000,
        )
        self.assertEqual(w, [])
        self.assertEqual(total_missing_bars(gaps), 3)
        self.assertEqual(len(gaps), 2)

    def test_find_candle_gaps_in_window_aligns(self) -> None:
        candles = [_c(900_000), _c(1_800_000)]
        gaps, w = find_candle_gaps_in_window(
            candles,
            window_start_ms=950_000,
            window_end_ms_inclusive=1_850_000,
            step_ms=900_000,
        )
        self.assertEqual(w, [])
        self.assertEqual(gaps, [])
