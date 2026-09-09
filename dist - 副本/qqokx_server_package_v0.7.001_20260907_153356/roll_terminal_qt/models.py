from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class DepthRow:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class LegMarket:
    inst_id: str
    last: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    bids: tuple[DepthRow, ...]
    asks: tuple[DepthRow, ...]
    source: str


@dataclass(frozen=True)
class MarketPairSnapshot:
    current: LegMarket
    target: LegMarket
    spread_abs: Decimal | None
    spread_pct: Decimal | None
    updated_at: str
    status: str


@dataclass(frozen=True)
class ArbitrageOpportunityView:
    key: str
    title: str
    left_inst_id: str
    right_inst_id: str
    left_kind: str
    right_kind: str
    template: str
    description: str = ""
    is_custom: bool = False
