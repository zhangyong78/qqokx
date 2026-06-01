from __future__ import annotations

from decimal import Decimal
from typing import Literal

from okx_quant.okx_client import OkxOrderBook


def vwap_for_base_size(
    order_book: OkxOrderBook,
    *,
    side: Literal["buy", "sell"],
    base_size: Decimal,
) -> tuple[Decimal | None, Decimal]:
    """
    按 base 数量（现货币数 / 合约张数对应的 base）在盘口上估算 VWAP。
    返回 (vwap, filled_size)。
    """
    if base_size <= 0:
        return None, Decimal("0")
    levels = order_book.asks if side == "buy" else order_book.bids
    remaining = base_size
    total_quote = Decimal("0")
    filled = Decimal("0")
    for price, level_size in levels:
        if price <= 0 or level_size <= 0:
            continue
        take = min(remaining, level_size)
        total_quote += price * take
        filled += take
        remaining -= take
        if remaining <= 0:
            break
    if filled <= 0:
        return None, Decimal("0")
    return total_quote / filled, filled


def estimated_slippage_pct(
    reference_price: Decimal,
    vwap: Decimal | None,
    *,
    side: Literal["buy", "sell"],
) -> Decimal:
    if reference_price <= 0 or vwap is None or vwap <= 0:
        return Decimal("0")
    if side == "buy":
        return max(Decimal("0"), (vwap - reference_price) / reference_price)
    return max(Decimal("0"), (reference_price - vwap) / reference_price)


def spread_slippage_proxy(bid: Decimal | None, ask: Decimal | None) -> Decimal:
    mid = (bid + ask) / Decimal("2") if bid and ask and bid > 0 and ask > 0 else None
    if mid is None or mid <= 0 or bid is None or ask is None:
        return Decimal("0.001")
    return (ask - bid) / mid / Decimal("2")
