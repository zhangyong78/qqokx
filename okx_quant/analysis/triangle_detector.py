from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP

from okx_quant.analysis.pivot_detector import PivotDetectionConfig, detect_pivots
from okx_quant.analysis.structure_models import PivotPoint, PriceLine, TriangleCandidate
from okx_quant.analysis.trendline_builder import build_line
from okx_quant.models import Candle


@dataclass(frozen=True)
class TriangleDetectionConfig:
    pivot: PivotDetectionConfig = field(default_factory=PivotDetectionConfig)
    min_anchor_distance: int = 4
    min_triangle_bars: int = 12
    max_apex_bars_ahead: int = 80
    touch_tolerance_ratio: Decimal = Decimal("0.14")
    max_violations: int = 6
    max_candidates: int = 4


def detect_triangles(
    candles: list[Candle] | tuple[Candle, ...],
    config: TriangleDetectionConfig | None = None,
) -> list[TriangleCandidate]:
    cfg = config or TriangleDetectionConfig()
    items = tuple(candles)
    if len(items) < cfg.min_triangle_bars:
        return []
    pivots = tuple(detect_pivots(items, cfg.pivot))
    highs = tuple(item for item in pivots if item.kind == "high")
    lows = tuple(item for item in pivots if item.kind == "low")
    candidates: list[TriangleCandidate] = []
    for hi_i, high_a in enumerate(highs):
        for high_b in highs[hi_i + 1 :]:
            if high_b.index - high_a.index < cfg.min_anchor_distance:
                continue
            upper_line = build_line(high_a, high_b)
            if upper_line.slope > 0:
                continue
            for lo_i, low_a in enumerate(lows):
                for low_b in lows[lo_i + 1 :]:
                    if low_b.index - low_a.index < cfg.min_anchor_distance:
                        continue
                    lower_line = build_line(low_a, low_b)
                    if lower_line.slope < 0:
                        continue
                    candidate = _score_triangle(items, upper_line, lower_line, highs, lows, cfg)
                    if candidate is not None:
                        candidates.append(candidate)
    candidates.sort(key=lambda item: (item.score, item.end_index - item.start_index), reverse=True)
    return candidates[: cfg.max_candidates]


def _score_triangle(
    candles: tuple[Candle, ...],
    upper_line: PriceLine,
    lower_line: PriceLine,
    highs: tuple[PivotPoint, ...],
    lows: tuple[PivotPoint, ...],
    config: TriangleDetectionConfig,
) -> TriangleCandidate | None:
    start_index = max(upper_line.start_index, lower_line.start_index)
    end_index = len(candles) - 1
    if end_index - start_index + 1 < config.min_triangle_bars:
        return None
    width_start = upper_line.value_at(start_index) - lower_line.value_at(start_index)
    width_end = upper_line.value_at(end_index) - lower_line.value_at(end_index)
    if width_start <= 0 or width_end <= 0 or width_end >= width_start:
        return None

    apex_index = _line_intersection_index(upper_line, lower_line)
    if apex_index is None:
        return None
    if apex_index <= end_index or apex_index > end_index + config.max_apex_bars_ahead:
        return None

    tolerance = max(width_start * config.touch_tolerance_ratio, Decimal("0.00000001"))
    high_pivots = tuple(item for item in highs if start_index <= item.index <= end_index)
    low_pivots = tuple(item for item in lows if start_index <= item.index <= end_index)
    high_touches = sum(1 for item in high_pivots if abs(upper_line.offset_for(item.index, item.price)) <= tolerance)
    low_touches = sum(1 for item in low_pivots if abs(lower_line.offset_for(item.index, item.price)) <= tolerance)
    if high_touches < 2 or low_touches < 2:
        return None

    violations = 0
    for index in range(start_index, end_index + 1):
        candle = candles[index]
        upper = upper_line.value_at(index)
        lower = lower_line.value_at(index)
        if candle.high > upper + tolerance or candle.low < lower - tolerance:
            violations += 1
    if violations > config.max_violations:
        return None

    kind = _triangle_kind(upper_line, lower_line)
    touches = high_touches + low_touches
    score = (Decimal(touches) * Decimal("12")) + (Decimal(end_index - start_index + 1) / Decimal("4")) - (
        Decimal(violations) * Decimal("16")
    )
    return TriangleCandidate(
        kind=kind,
        start_index=start_index,
        end_index=end_index,
        apex_index=apex_index,
        upper_line=upper_line,
        lower_line=lower_line,
        high_pivots=high_pivots,
        low_pivots=low_pivots,
        touches=touches,
        violations=violations,
        score=score,
    )


def _line_intersection_index(first: PriceLine, second: PriceLine) -> int | None:
    slope_delta = first.slope - second.slope
    if slope_delta == 0:
        return None
    intercept_delta = second.start_price - first.start_price + (
        first.slope * Decimal(first.start_index) - second.slope * Decimal(second.start_index)
    )
    x = intercept_delta / slope_delta
    return int(x.to_integral_value(rounding=ROUND_HALF_UP))


def _triangle_kind(upper_line: PriceLine, lower_line: PriceLine) -> str:
    flat_threshold = Decimal("0.000001")
    upper_flat = abs(upper_line.slope) <= flat_threshold
    lower_flat = abs(lower_line.slope) <= flat_threshold
    if upper_flat and lower_line.slope > 0:
        return "ascending"
    if lower_flat and upper_line.slope < 0:
        return "descending"
    return "symmetrical"
