from __future__ import annotations

import bisect
from statistics import median

from okx_quant.models import Candle


def infer_bar_duration_ms(candles: list[Candle] | tuple[Candle, ...]) -> int:
    """Infer a candle's bar duration from ordered open timestamps."""
    if len(candles) < 2:
        return 0
    ordered = sorted(int(candle.ts) for candle in candles)
    deltas = [right - left for left, right in zip(ordered, ordered[1:]) if right > left]
    if not deltas:
        return 0
    return int(median(deltas))


def closed_candle_available_timestamps(candles: list[Candle] | tuple[Candle, ...]) -> list[int]:
    """Return timestamps when candle OHLC values become available.

    OKX candle timestamps are bar open times. A higher-timeframe candle's close
    should only be visible after that bar has finished.
    """
    duration_ms = infer_bar_duration_ms(candles)
    return [int(candle.ts) + duration_ms for candle in candles]


def latest_closed_candle_index(candles: list[Candle] | tuple[Candle, ...], entry_ts: int) -> int:
    available_ts = closed_candle_available_timestamps(candles)
    return bisect.bisect_right(available_ts, int(entry_ts)) - 1
