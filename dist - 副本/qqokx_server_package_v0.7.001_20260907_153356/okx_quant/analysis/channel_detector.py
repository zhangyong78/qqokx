from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from okx_quant.analysis.pivot_detector import PivotDetectionConfig, detect_pivots
from okx_quant.analysis.structure_models import ChannelCandidate, PivotPoint, PriceLine
from okx_quant.analysis.trendline_builder import build_line, build_mid_line
from okx_quant.models import Candle


@dataclass(frozen=True)
class ChannelDetectionConfig:
    pivot: PivotDetectionConfig = field(default_factory=PivotDetectionConfig)
    min_anchor_distance: int = 5
    min_channel_bars: int = 12
    min_width: Decimal = Decimal("0")
    touch_tolerance_ratio: Decimal = Decimal("0.12")
    max_violations: int = 3
    max_candidates: int = 5


def detect_channels(
    candles: list[Candle] | tuple[Candle, ...],
    config: ChannelDetectionConfig | None = None,
) -> list[ChannelCandidate]:
    cfg = config or ChannelDetectionConfig()
    _validate_config(cfg)
    items = tuple(candles)
    if len(items) < cfg.min_channel_bars:
        return []
    pivots = tuple(detect_pivots(items, cfg.pivot))
    highs = tuple(item for item in pivots if item.kind == "high")
    lows = tuple(item for item in pivots if item.kind == "low")

    candidates: list[ChannelCandidate] = []
    candidates.extend(_build_candidates(items, lows, highs, "ascending", cfg))
    candidates.extend(_build_candidates(items, highs, lows, "descending", cfg))
    candidates.sort(key=lambda item: (item.score, item.end_index - item.start_index), reverse=True)
    return candidates[: cfg.max_candidates]


def _validate_config(config: ChannelDetectionConfig) -> None:
    if config.min_anchor_distance <= 0:
        raise ValueError("min_anchor_distance must be positive")
    if config.min_channel_bars <= 1:
        raise ValueError("min_channel_bars must be greater than 1")
    if config.touch_tolerance_ratio < 0:
        raise ValueError("touch_tolerance_ratio cannot be negative")
    if config.max_violations < 0:
        raise ValueError("max_violations cannot be negative")
    if config.max_candidates <= 0:
        raise ValueError("max_candidates must be positive")


def _build_candidates(
    candles: tuple[Candle, ...],
    base_pivots: tuple[PivotPoint, ...],
    boundary_pivots: tuple[PivotPoint, ...],
    kind: str,
    config: ChannelDetectionConfig,
) -> list[ChannelCandidate]:
    candidates: list[ChannelCandidate] = []
    for left_index, first in enumerate(base_pivots):
        for second in base_pivots[left_index + 1 :]:
            if second.index - first.index < config.min_anchor_distance:
                continue
            base_line = build_line(first, second)
            if kind == "ascending" and base_line.slope <= 0:
                continue
            if kind == "descending" and base_line.slope >= 0:
                continue
            start_index = first.index
            end_index = len(candles) - 1
            if end_index - start_index + 1 < config.min_channel_bars:
                continue
            candidate = _score_candidate(
                candles,
                base_line,
                (first, second),
                tuple(item for item in boundary_pivots if start_index <= item.index <= end_index),
                kind,
                start_index,
                end_index,
                config,
            )
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def _score_candidate(
    candles: tuple[Candle, ...],
    base_line: PriceLine,
    base_pivots: tuple[PivotPoint, PivotPoint],
    boundary_pivots: tuple[PivotPoint, ...],
    kind: str,
    start_index: int,
    end_index: int,
    config: ChannelDetectionConfig,
) -> ChannelCandidate | None:
    if not boundary_pivots:
        return None
    if kind == "ascending":
        offsets = [base_line.offset_for(item.index, item.price) for item in boundary_pivots]
        boundary_offset = max(offsets)
    else:
        offsets = [base_line.offset_for(item.index, item.price) for item in boundary_pivots]
        boundary_offset = min(offsets)

    width = abs(boundary_offset)
    if width <= config.min_width:
        return None

    parallel_line = base_line.shifted(boundary_offset)
    tolerance = max(width * config.touch_tolerance_ratio, Decimal("0.00000001"))
    touches = _count_base_touches(base_pivots, boundary_pivots, base_line, parallel_line, tolerance)
    violations = _count_violations(candles, start_index, end_index, base_line, parallel_line, tolerance, kind)
    if violations > config.max_violations:
        return None

    span = Decimal(end_index - start_index + 1)
    score = (Decimal(touches) * Decimal("12")) + (span / Decimal("4")) - (Decimal(violations) * Decimal("20"))
    return ChannelCandidate(
        kind=kind,  # type: ignore[arg-type]
        start_index=start_index,
        end_index=end_index,
        base_line=base_line,
        parallel_line=parallel_line,
        mid_line=build_mid_line(base_line, parallel_line),
        base_pivots=base_pivots,
        boundary_pivots=tuple(
            item for item in boundary_pivots if abs(parallel_line.offset_for(item.index, item.price)) <= tolerance
        ),
        touches=touches,
        violations=violations,
        width=width,
        score=score,
    )


def _count_base_touches(
    base_pivots: tuple[PivotPoint, PivotPoint],
    boundary_pivots: tuple[PivotPoint, ...],
    base_line: PriceLine,
    parallel_line: PriceLine,
    tolerance: Decimal,
) -> int:
    touches = len(base_pivots)
    for pivot in boundary_pivots:
        if abs(parallel_line.offset_for(pivot.index, pivot.price)) <= tolerance:
            touches += 1
    return touches


def _count_violations(
    candles: tuple[Candle, ...],
    start_index: int,
    end_index: int,
    base_line: PriceLine,
    parallel_line: PriceLine,
    tolerance: Decimal,
    kind: str,
) -> int:
    violations = 0
    for index in range(start_index, end_index + 1):
        candle = candles[index]
        base_value = base_line.value_at(index)
        parallel_value = parallel_line.value_at(index)
        lower = min(base_value, parallel_value)
        upper = max(base_value, parallel_value)
        if kind == "ascending":
            if candle.low < lower - tolerance or candle.high > upper + tolerance:
                violations += 1
        else:
            if candle.high > upper + tolerance or candle.low < lower - tolerance:
                violations += 1
    return violations
