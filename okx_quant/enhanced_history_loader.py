from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from okx_quant.candle_cache import merge_candles
from okx_quant.enhanced_signal_engine import bar_to_minutes
from okx_quant.models import Candle


@dataclass(frozen=True)
class HistorySegment:
    index: int
    start_ts: int
    end_ts: int
    requested_limit: int
    preload_count: int
    returned_count: int


@dataclass(frozen=True)
class SegmentedHistoryLoadResult:
    candles: tuple[Candle, ...]
    segments: tuple[HistorySegment, ...]


def estimate_range_candle_limit(start_ts: int, end_ts: int, bar: str, *, safety_padding: int = 32) -> int:
    if end_ts < start_ts:
        raise ValueError("end_ts must be greater than or equal to start_ts")
    bar_minutes = bar_to_minutes(bar)
    bar_ms = bar_minutes * 60 * 1000
    span_ms = max(end_ts - start_ts, bar_ms)
    return max((span_ms // bar_ms) + 1 + max(int(safety_padding), 0), 64)


def iter_time_segments(start_ts: int, end_ts: int, *, segment_ms: int) -> list[tuple[int, int]]:
    if end_ts < start_ts:
        raise ValueError("end_ts must be greater than or equal to start_ts")
    if segment_ms <= 0:
        raise ValueError("segment_ms must be positive")
    segments: list[tuple[int, int]] = []
    current = start_ts
    while current <= end_ts:
        segment_end = min(current + segment_ms - 1, end_ts)
        segments.append((current, segment_end))
        current = segment_end + 1
    return segments


def load_segmented_history_candles(
    load_range: Callable[..., list[Candle]],
    *,
    inst_id: str,
    bar: str,
    start_ts: int,
    end_ts: int,
    preload_count: int = 0,
    segment_ms: int,
    safety_padding: int = 32,
) -> SegmentedHistoryLoadResult:
    merged: list[Candle] = []
    segment_stats: list[HistorySegment] = []
    for index, (segment_start, segment_end) in enumerate(
        iter_time_segments(start_ts, end_ts, segment_ms=segment_ms),
        start=1,
    ):
        requested_limit = estimate_range_candle_limit(
            segment_start,
            segment_end,
            bar,
            safety_padding=safety_padding,
        )
        segment_preload = max(int(preload_count), 0) if index == 1 else 0
        candles = load_range(
            inst_id,
            bar,
            requested_limit,
            start_ts=segment_start,
            end_ts=segment_end,
            preload_count=segment_preload,
        )
        merged = merge_candles(merged, candles)
        segment_stats.append(
            HistorySegment(
                index=index,
                start_ts=segment_start,
                end_ts=segment_end,
                requested_limit=requested_limit,
                preload_count=segment_preload,
                returned_count=len(candles),
            )
        )
    return SegmentedHistoryLoadResult(
        candles=tuple(merged),
        segments=tuple(segment_stats),
    )
