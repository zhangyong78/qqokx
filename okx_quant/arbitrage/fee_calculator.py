from __future__ import annotations

from decimal import Decimal

from okx_quant.arbitrage.models import ArbitrageFeeProfile


def round_trip_fee_pct(
    *,
    fee_profile: ArbitrageFeeProfile,
    assume_taker: bool = True,
) -> Decimal:
    """开平双腿、现货+衍生品各一次，返回占名义价值的百分比（小数形式）。"""
    if assume_taker:
        spot_leg = fee_profile.spot_taker * Decimal("2")
        swap_leg = fee_profile.swap_taker * Decimal("2")
    else:
        spot_leg = fee_profile.spot_maker * Decimal("2")
        swap_leg = fee_profile.swap_maker * Decimal("2")
    return spot_leg + swap_leg


def round_trip_fee_pct_display(fee_pct: Decimal) -> Decimal:
    return fee_pct * Decimal("100")
