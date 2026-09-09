from __future__ import annotations

from decimal import Decimal

from okx_quant.analysis.structure_models import PivotPoint, PriceLine


def build_line(first: PivotPoint, second: PivotPoint) -> PriceLine:
    if first.index == second.index:
        raise ValueError("trendline anchors must use different indexes")
    if first.index < second.index:
        left, right = first, second
    else:
        left, right = second, first
    return PriceLine(
        start_index=left.index,
        start_price=left.price,
        end_index=right.index,
        end_price=right.price,
    )


def build_mid_line(first: PriceLine, second: PriceLine) -> PriceLine:
    return PriceLine(
        start_index=first.start_index,
        start_price=(first.start_price + second.start_price) / Decimal("2"),
        end_index=first.end_index,
        end_price=(first.end_price + second.end_price) / Decimal("2"),
    )
