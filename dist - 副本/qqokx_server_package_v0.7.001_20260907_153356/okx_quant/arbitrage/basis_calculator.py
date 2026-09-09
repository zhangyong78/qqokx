from __future__ import annotations

from decimal import Decimal


def mid_price(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / Decimal("2")


def compute_basis(spot_mid: Decimal, derivative_mid: Decimal) -> tuple[Decimal, Decimal]:
    basis_abs = derivative_mid - spot_mid
    if spot_mid <= 0:
        raise ValueError("spot_mid must be positive")
    basis_pct = basis_abs / spot_mid
    return basis_abs, basis_pct


def annualize_funding_rate(
    funding_rate: Decimal,
    *,
    funding_intervals_per_day: Decimal = Decimal("3"),
    days_per_year: Decimal = Decimal("365"),
) -> Decimal:
    """OKX 资金费率为每 8 小时一次，换算为年化（小数形式）。"""
    return funding_rate * funding_intervals_per_day * days_per_year


def annualize_basis_convergence(
    basis_pct: Decimal,
    *,
    hold_days: Decimal,
    days_per_year: Decimal = Decimal("365"),
) -> Decimal:
    if hold_days <= 0:
        return Decimal("0")
    return basis_pct * (days_per_year / hold_days)


def net_carry_annual_pct_cash_and_carry(
    *,
    basis_pct: Decimal,
    funding_annual: Decimal,
    fee_round_trip_pct: Decimal,
    slippage_pct: Decimal,
    hold_days: Decimal | None = None,
) -> Decimal:
    """
    正向套利：买现货 + 空衍生品。
    基差为正时，持有至收敛可获得 basis_pct；资金费为正时空头收取 funding。
    """
    if hold_days is not None and hold_days > 0:
        basis_component = annualize_basis_convergence(basis_pct, hold_days=Decimal(str(hold_days)))
    else:
        basis_component = basis_pct
    gross = basis_component + funding_annual
    costs = fee_round_trip_pct + slippage_pct
    return (gross - costs) * Decimal("100")
