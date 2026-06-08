from __future__ import annotations

from decimal import Decimal
from typing import Literal


class InvalidProtectionPlanError(RuntimeError):
    """Raised when entry / stop / take-profit prices do not form a tradable setup."""


def validate_protection_prices(
    *,
    direction: Literal["long", "short"],
    entry_reference: Decimal,
    stop_loss: Decimal,
    take_profit: Decimal,
) -> None:
    if entry_reference <= 0:
        raise InvalidProtectionPlanError("开仓参考价必须大于 0")
    if stop_loss <= 0:
        raise InvalidProtectionPlanError("止损价必须大于 0")
    if take_profit <= 0:
        raise InvalidProtectionPlanError("止盈价必须大于 0")

    if direction == "long":
        if stop_loss >= entry_reference:
            raise InvalidProtectionPlanError("做多止损价必须低于开仓参考价")
        if take_profit <= entry_reference:
            raise InvalidProtectionPlanError("做多止盈价必须高于开仓参考价")
        return

    if stop_loss <= entry_reference:
        raise InvalidProtectionPlanError("做空止损价必须高于开仓参考价")
    if take_profit >= entry_reference:
        raise InvalidProtectionPlanError("做空止盈价必须低于开仓参考价")
