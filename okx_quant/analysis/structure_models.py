from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

PivotKind = Literal["high", "low"]
ChannelKind = Literal["ascending", "descending"]
TrendlineKind = Literal["support", "resistance"]
TriangleKind = Literal["symmetrical", "ascending", "descending"]


@dataclass(frozen=True)
class PivotPoint:
    index: int
    ts: int
    price: Decimal
    kind: PivotKind
    strength: Decimal = Decimal("0")


@dataclass(frozen=True)
class PriceLine:
    start_index: int
    start_price: Decimal
    end_index: int
    end_price: Decimal

    @property
    def slope(self) -> Decimal:
        span = self.end_index - self.start_index
        if span == 0:
            return Decimal("0")
        return (self.end_price - self.start_price) / Decimal(span)

    def value_at(self, index: int) -> Decimal:
        return self.start_price + (self.slope * Decimal(index - self.start_index))

    def offset_for(self, index: int, price: Decimal) -> Decimal:
        return price - self.value_at(index)

    def shifted(self, offset: Decimal) -> "PriceLine":
        return PriceLine(
            start_index=self.start_index,
            start_price=self.start_price + offset,
            end_index=self.end_index,
            end_price=self.end_price + offset,
        )


@dataclass(frozen=True)
class ChannelCandidate:
    kind: ChannelKind
    start_index: int
    end_index: int
    base_line: PriceLine
    parallel_line: PriceLine
    mid_line: PriceLine
    base_pivots: tuple[PivotPoint, PivotPoint]
    boundary_pivots: tuple[PivotPoint, ...]
    touches: int
    violations: int
    width: Decimal
    score: Decimal

    @property
    def slope(self) -> Decimal:
        return self.base_line.slope


@dataclass(frozen=True)
class TrendlineCandidate:
    kind: TrendlineKind
    start_index: int
    end_index: int
    line: PriceLine
    pivots: tuple[PivotPoint, ...]
    touches: int
    violations: int
    score: Decimal

    @property
    def slope(self) -> Decimal:
        return self.line.slope


@dataclass(frozen=True)
class TriangleCandidate:
    kind: TriangleKind
    start_index: int
    end_index: int
    apex_index: int
    upper_line: PriceLine
    lower_line: PriceLine
    high_pivots: tuple[PivotPoint, ...]
    low_pivots: tuple[PivotPoint, ...]
    touches: int
    violations: int
    score: Decimal

    @property
    def width(self) -> Decimal:
        return self.upper_line.value_at(self.start_index) - self.lower_line.value_at(self.start_index)


@dataclass(frozen=True)
class BoxCandidate:
    start_index: int
    end_index: int
    upper: Decimal
    lower: Decimal
    upper_touches: int
    lower_touches: int
    violations: int
    score: Decimal

    @property
    def width(self) -> Decimal:
        return self.upper - self.lower
