from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.models import Instrument
from okx_quant.pricing import format_decimal, snap_to_increment


@dataclass(frozen=True)
class ReconciledFill:
    planned_size: Decimal
    filled_size: Decimal
    avg_price: Decimal | None
    fully_filled: bool
    deviation_ratio: Decimal

    @property
    def shortfall(self) -> Decimal:
        return max(self.planned_size - self.filled_size, Decimal("0"))


def reconcile_fill(
    *,
    planned_size: Decimal,
    filled_size: Decimal,
    avg_price: Decimal | None,
    tolerance_ratio: Decimal = Decimal("0.001"),
) -> ReconciledFill:
    planned = max(planned_size, Decimal("0"))
    filled = max(filled_size, Decimal("0"))
    deviation = Decimal("0")
    if planned > 0:
        deviation = abs(filled - planned) / planned
    fully_filled = filled >= planned if planned > 0 else filled <= 0
    if planned > 0 and not fully_filled and deviation > tolerance_ratio:
        fully_filled = False
    return ReconciledFill(
        planned_size=planned,
        filled_size=filled,
        avg_price=avg_price,
        fully_filled=fully_filled,
        deviation_ratio=deviation,
    )


def _derivative_base_per_contract(
    *,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    ct_val = derivative_instrument.ct_val or Decimal("1")
    if ct_val <= 0:
        return Decimal("0")
    multiplier = (
        derivative_instrument.ct_mult
        if derivative_instrument.ct_mult is not None and derivative_instrument.ct_mult > 0
        else Decimal("1")
    )
    contract_value = ct_val * multiplier
    payout_ccy = str(derivative_instrument.ct_val_ccy or "").strip().upper()
    if payout_ccy in {"USD", "USDT", "USDC"}:
        if reference_price is None or reference_price <= 0:
            return contract_value
        return contract_value / reference_price
    return contract_value


def spot_base_from_derivative_fill(
    *,
    derivative_filled_contracts: Decimal,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    base_per_contract = _derivative_base_per_contract(
        derivative_instrument=derivative_instrument,
        reference_price=reference_price,
    )
    return max(derivative_filled_contracts, Decimal("0")) * base_per_contract


def derivative_contracts_from_spot_base(
    *,
    spot_base_qty: Decimal,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    base_per_contract = _derivative_base_per_contract(
        derivative_instrument=derivative_instrument,
        reference_price=reference_price,
    )
    if base_per_contract <= 0:
        return Decimal("0")
    raw = max(spot_base_qty, Decimal("0")) / base_per_contract
    return snap_to_increment(raw, derivative_instrument.lot_size, "down")


def format_reconcile_message(label: str, reconciled: ReconciledFill) -> str:
    if reconciled.planned_size <= 0:
        return f"{label}：无计划数量。"
    if reconciled.fully_filled:
        return f"{label}：已按计划成交 {format_decimal(reconciled.filled_size)}。"
    return (
        f"{label}：计划 {format_decimal(reconciled.planned_size)}，"
        f"实际 {format_decimal(reconciled.filled_size)}，"
        f"偏差 {reconciled.deviation_ratio * Decimal('100'):.3f}%。"
    )


def estimate_cash_and_carry_pnl(
    *,
    spot_qty: Decimal,
    open_spot_price: Decimal | None,
    close_spot_price: Decimal | None,
    open_deriv_price: Decimal | None,
    close_deriv_price: Decimal | None,
    derivative_instrument: Instrument,
    derivative_qty: Decimal,
) -> Decimal | None:
    if (
        open_spot_price is None
        or close_spot_price is None
        or open_deriv_price is None
        or close_deriv_price is None
        or spot_qty <= 0
        or derivative_qty <= 0
    ):
        return None
    ct_val = derivative_instrument.ct_val or Decimal("1")
    base_exposure = derivative_qty * ct_val
    qty = min(spot_qty, base_exposure)
    spot_leg = (close_spot_price - open_spot_price) * qty
    deriv_leg = (open_deriv_price - close_deriv_price) * qty
    return spot_leg + deriv_leg
