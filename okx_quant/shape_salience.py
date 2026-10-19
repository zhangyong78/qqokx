from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal, InvalidOperation
from typing import Any


def significant_range_core(
    candles: Sequence[Any],
    formation_indices: Sequence[int],
    *,
    lookback: int = 10,
    top_n: int = 4,
) -> tuple[int, int] | None:
    """Return the strongest formation candle ranked in the prior range window.

    Every formation candle is compared against the same ``lookback`` confirmed
    candles immediately before the formation. Ties are conservative: equal
    prior ranges count ahead of the candidate.
    """
    indices = tuple(int(item) for item in formation_indices)
    if not indices or any(index < 0 or index >= len(candles) for index in indices):
        return None
    lookback = max(int(lookback), 1)
    top_n = max(int(top_n), 1)
    formation_start = min(indices)
    reference_start = formation_start - lookback
    if reference_start < 0:
        return None
    checked_indices = (*range(reference_start, formation_start), *indices)
    if any(not bool(getattr(candles[index], "confirmed", True)) for index in checked_indices):
        return None

    reference_ranges: list[Decimal] = []
    for index in range(reference_start, formation_start):
        candle = candles[index]
        try:
            reference_ranges.append(Decimal(str(candle.high)) - Decimal(str(candle.low)))
        except (InvalidOperation, TypeError, ValueError, AttributeError):
            return None

    best: tuple[int, int] | None = None
    for index in indices:
        candle = candles[index]
        try:
            candidate_range = Decimal(str(candle.high)) - Decimal(str(candle.low))
        except (InvalidOperation, TypeError, ValueError, AttributeError):
            continue
        if not candidate_range.is_finite() or candidate_range <= 0:
            continue
        rank = 1 + sum(1 for prior_range in reference_ranges if prior_range >= candidate_range)
        if rank <= top_n and (best is None or rank < best[1]):
            best = (index, rank)
    return best


def range_retrace_threshold(first: Any, *, direction: str, fraction: Decimal = Decimal("0.6")) -> Decimal | None:
    """Price level reached by a 60% body retrace from a first bearish/bullish candle."""
    try:
        open_price = Decimal(str(first.open))
        close_price = Decimal(str(first.close))
    except (InvalidOperation, TypeError, ValueError, AttributeError):
        return None
    if not open_price.is_finite() or not close_price.is_finite() or not Decimal("0") < fraction < Decimal("1"):
        return None
    normalized_direction = str(direction).strip().lower()
    if normalized_direction == "long" and close_price < open_price:
        return close_price + (open_price - close_price) * fraction
    if normalized_direction == "short" and close_price > open_price:
        return close_price - (close_price - open_price) * fraction
    return None
