from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from okx_quant.analysis.pivot_detector import PivotDetectionConfig, detect_pivots
from okx_quant.analysis.structure_models import BoxCandidate
from okx_quant.models import Candle


@dataclass(frozen=True)
class BoxDetectionConfig:
    pivot: PivotDetectionConfig = field(default_factory=PivotDetectionConfig)
    min_box_bars: int = 12
    min_touches_per_side: int = 2
    boundary_tolerance_ratio: Decimal = Decimal("0.12")
    max_violations: int = 2
    max_candidates: int = 5


def detect_boxes(
    candles: list[Candle] | tuple[Candle, ...],
    config: BoxDetectionConfig | None = None,
) -> list[BoxCandidate]:
    cfg = config or BoxDetectionConfig()
    _validate_config(cfg)
    items = tuple(candles)
    if len(items) < cfg.min_box_bars:
        return []

    pivots = tuple(detect_pivots(items, cfg.pivot))
    highs = tuple(item for item in pivots if item.kind == "high")
    lows = tuple(item for item in pivots if item.kind == "low")
    candidates: list[BoxCandidate] = []
    for high in highs:
        for low in lows:
            start_index = min(high.index, low.index)
            end_index = len(items) - 1
            if end_index - start_index + 1 < cfg.min_box_bars:
                continue
            upper = high.price
            lower = low.price
            if upper <= lower:
                continue
            candidate = _score_box(items, start_index, end_index, upper, lower, cfg)
            if candidate is not None:
                candidates.append(candidate)

    candidates.sort(key=lambda item: (item.score, item.end_index - item.start_index), reverse=True)
    return candidates[: cfg.max_candidates]


def _validate_config(config: BoxDetectionConfig) -> None:
    if config.min_box_bars <= 1:
        raise ValueError("min_box_bars must be greater than 1")
    if config.min_touches_per_side <= 0:
        raise ValueError("min_touches_per_side must be positive")
    if config.boundary_tolerance_ratio < 0:
        raise ValueError("boundary_tolerance_ratio cannot be negative")
    if config.max_violations < 0:
        raise ValueError("max_violations cannot be negative")
    if config.max_candidates <= 0:
        raise ValueError("max_candidates must be positive")


def _score_box(
    candles: tuple[Candle, ...],
    start_index: int,
    end_index: int,
    upper: Decimal,
    lower: Decimal,
    config: BoxDetectionConfig,
) -> BoxCandidate | None:
    width = upper - lower
    tolerance = max(width * config.boundary_tolerance_ratio, Decimal("0.00000001"))
    upper_touches = 0
    lower_touches = 0
    violations = 0
    for index in range(start_index, end_index + 1):
        candle = candles[index]
        if abs(candle.high - upper) <= tolerance:
            upper_touches += 1
        if abs(candle.low - lower) <= tolerance:
            lower_touches += 1
        if candle.high > upper + tolerance or candle.low < lower - tolerance:
            violations += 1

    if upper_touches < config.min_touches_per_side or lower_touches < config.min_touches_per_side:
        return None
    if violations > config.max_violations:
        return None

    span = Decimal(end_index - start_index + 1)
    score = (Decimal(upper_touches + lower_touches) * Decimal("10")) + (span / Decimal("4")) - (
        Decimal(violations) * Decimal("20")
    )
    return BoxCandidate(
        start_index=start_index,
        end_index=end_index,
        upper=upper,
        lower=lower,
        upper_touches=upper_touches,
        lower_touches=lower_touches,
        violations=violations,
        score=score,
    )
