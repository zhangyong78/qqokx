from __future__ import annotations

from typing import Any


AUTO_BOX_MAX_CANDIDATES = 8
AUTO_BOX_MAX_SPAN_BARS = 54
AUTO_BOX_MAX_WIDTH_PCT = 0.075


def is_auto_box_candidate_valid(box: Any, candles: list[Any]) -> bool:
    if not candles:
        return False
    try:
        start_index = int(getattr(box, "start_index"))
        end_index = int(getattr(box, "end_index"))
        upper = float(getattr(box, "upper"))
        lower = float(getattr(box, "lower"))
    except (TypeError, ValueError):
        return False

    span = end_index - start_index + 1
    if span <= 0 or span > AUTO_BOX_MAX_SPAN_BARS:
        return False
    if end_index != len(candles) - 1:
        return False
    if upper <= lower:
        return False

    latest_close = _candle_close(candles[-1])
    if latest_close <= 0:
        return False
    if not lower <= latest_close <= upper:
        return False

    midpoint = max((upper + lower) / 2.0, 1e-12)
    width_pct = (upper - lower) / midpoint
    return width_pct <= AUTO_BOX_MAX_WIDTH_PCT


def _candle_close(candle: Any) -> float:
    try:
        return float(getattr(candle, "close"))
    except (TypeError, ValueError):
        return 0.0
