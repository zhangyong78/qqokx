from __future__ import annotations

from decimal import Decimal

from okx_quant.arbitrage.models import ArbitrageSizePreview, SizeUnit
from okx_quant.models import Instrument
from okx_quant.pricing import snap_to_increment


def _contract_base_size(instrument: Instrument, contracts: Decimal) -> Decimal:
    ct_val = instrument.ct_val or Decimal("1")
    return contracts * ct_val


def _contracts_from_base(instrument: Instrument, base_qty: Decimal) -> Decimal:
    ct_val = instrument.ct_val or Decimal("1")
    if ct_val <= 0:
        return Decimal("0")
    raw = base_qty / ct_val
    return snap_to_increment(raw, instrument.lot_size, "down")


def snap_spot_size(base_qty: Decimal, instrument: Instrument) -> Decimal:
    return snap_to_increment(base_qty, instrument.lot_size, "down")


def preview_arbitrage_size(
    *,
    size: Decimal,
    unit: SizeUnit,
    spot_mid: Decimal,
    spot_instrument: Instrument,
    swap_instrument: Instrument,
) -> ArbitrageSizePreview:
    if size <= 0:
        raise ValueError("数量必须大于 0")
    if spot_mid <= 0:
        raise ValueError("缺少有效现货价格")

    if unit == "coin":
        spot_base_qty = snap_spot_size(size, spot_instrument)
    elif unit == "usdt":
        spot_base_qty = snap_spot_size(size / spot_mid, spot_instrument)
    elif unit == "contracts":
        spot_base_qty = snap_spot_size(_contract_base_size(swap_instrument, size), spot_instrument)
    else:
        raise ValueError(f"未知数量单位：{unit}")

    swap_contracts = _contracts_from_base(swap_instrument, spot_base_qty)
    notional = spot_base_qty * spot_mid
    return ArbitrageSizePreview(
        spot_base_qty=spot_base_qty,
        swap_contracts=swap_contracts,
        notional_usdt=notional,
    )
