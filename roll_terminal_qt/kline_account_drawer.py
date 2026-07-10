from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from okx_quant.okx_client import OkxPosition, OkxTradeOrderItem


@dataclass(frozen=True)
class AccountDrawerSnapshot:
    positions: tuple[OkxPosition, ...] = ()
    orders: tuple[OkxTradeOrderItem, ...] = ()


def filter_account_items(items: Iterable[object], *, scope: str, symbol: str) -> list[object]:
    records = list(items)
    if scope == "all":
        return records
    normalized_symbol = symbol.strip().upper()
    return [
        item
        for item in records
        if str(getattr(item, "inst_id", "") or "").strip().upper() == normalized_symbol
    ]


def order_source_kind(order: object) -> str:
    source_kind = str(getattr(order, "source_kind", "") or "").strip().lower()
    algo_id = str(getattr(order, "algo_id", "") or "").strip()
    return "algo" if source_kind == "algo" or algo_id else "normal"


def order_cancel_reference(order: object) -> str:
    if order_source_kind(order) == "algo":
        names = ("algo_id", "algo_client_order_id", "client_order_id")
    else:
        names = ("order_id", "client_order_id")
    for name in names:
        value = str(getattr(order, name, "") or "").strip()
        if value:
            return value
    return ""
