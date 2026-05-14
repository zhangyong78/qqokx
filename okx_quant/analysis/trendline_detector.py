from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from okx_quant.analysis.pivot_detector import PivotDetectionConfig, detect_pivots
from okx_quant.analysis.structure_models import PivotPoint, TrendlineCandidate
from okx_quant.analysis.trendline_builder import build_line
from okx_quant.models import Candle


@dataclass(frozen=True)
class TrendlineDetectionConfig:
    pivot: PivotDetectionConfig = field(default_factory=PivotDetectionConfig)
    min_anchor_distance: int = 5
    min_line_bars: int = 12
    touch_tolerance_ratio: Decimal = Decimal("0.12")
    max_violations: int = 6
    max_candidates: int = 6


def detect_trendlines(
    candles: list[Candle] | tuple[Candle, ...],
    config: TrendlineDetectionConfig | None = None,
) -> list[TrendlineCandidate]:
    cfg = config or TrendlineDetectionConfig()
    items = tuple(candles)
    if len(items) < cfg.min_line_bars:
        return []
    pivots = tuple(detect_pivots(items, cfg.pivot))
    highs = tuple(item for item in pivots if item.kind == "high")
    lows = tuple(item for item in pivots if item.kind == "low")

    candidates: list[TrendlineCandidate] = []
    candidates.extend(_build_candidates(items, highs, "resistance", cfg))
    candidates.extend(_build_candidates(items, lows, "support", cfg))
    candidates.sort(key=lambda item: (item.score, item.end_index - item.start_index), reverse=True)
    return candidates[: cfg.max_candidates]


def _build_candidates(
    candles: tuple[Candle, ...],
    pivots: tuple[PivotPoint, ...],
    kind: str,
    config: TrendlineDetectionConfig,
) -> list[TrendlineCandidate]:
    candidates: list[TrendlineCandidate] = []
    for left_index, first in enumerate(pivots):
        for second in pivots[left_index + 1 :]:
            if second.index - first.index < config.min_anchor_distance:
                continue
            line = build_line(first, second)
            start_index = first.index
            end_index = len(candles) - 1
            if end_index - start_index + 1 < config.min_line_bars:
                continue
            tolerance = max(abs(second.price - first.price) * config.touch_tolerance_ratio, Decimal("0.00000001"))
            line_pivots = tuple(item for item in pivots if start_index <= item.index <= end_index)
            touches = sum(1 for item in line_pivots if abs(line.offset_for(item.index, item.price)) <= tolerance)
            violations = 0
            for index in range(start_index, end_index + 1):
                candle = candles[index]
                line_value = line.value_at(index)
                if kind == "resistance":
                    if candle.high > line_value + tolerance:
                        violations += 1
                else:
                    if candle.low < line_value - tolerance:
                        violations += 1
            if violations > config.max_violations:
                continue
            score = (Decimal(touches) * Decimal("10")) + (Decimal(end_index - start_index + 1) / Decimal("4")) - (
                Decimal(violations) * Decimal("12")
            )
            candidates.append(
                TrendlineCandidate(
                    kind=kind,  # type: ignore[arg-type]
                    start_index=start_index,
                    end_index=end_index,
                    line=line,
                    pivots=line_pivots,
                    touches=touches,
                    violations=violations,
                    score=score,
                )
            )
    return candidates
