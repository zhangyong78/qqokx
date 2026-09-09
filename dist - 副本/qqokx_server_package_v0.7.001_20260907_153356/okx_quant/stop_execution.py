from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class StopExecutionAssessment:
    planned_risk_usdt: Decimal
    actual_price_loss_usdt: Decimal
    effective_stop_price: Decimal
    actual_exit_price: Decimal
    stop_slippage_price: Decimal
    stop_slippage_usdt: Decimal
    stop_overrun_usdt: Decimal
    stop_overrun_pct: Decimal
    status: str


@dataclass(frozen=True)
class StopExecutionThresholds:
    warning_usdt: Decimal = Decimal("1")
    warning_pct: Decimal = Decimal("10")
    critical_usdt: Decimal = Decimal("5")
    critical_loss_multiple: Decimal = Decimal("1.5")


def assess_stop_execution(
    *,
    direction: str,
    entry_price: Decimal,
    initial_stop_price: Decimal,
    effective_stop_price: Decimal,
    actual_exit_price: Decimal,
    size: Decimal,
    price_delta_multiplier: Decimal = Decimal("1"),
    actual_price_loss_usdt: Decimal | None = None,
    thresholds: StopExecutionThresholds | None = None,
) -> StopExecutionAssessment | None:
    if (
        entry_price <= 0
        or initial_stop_price <= 0
        or effective_stop_price <= 0
        or actual_exit_price <= 0
        or size <= 0
        or price_delta_multiplier <= 0
    ):
        return None
    normalized_direction = str(direction or "").strip().lower()
    if normalized_direction not in {"long", "short", "buy", "sell"}:
        return None
    is_long = normalized_direction in {"long", "buy"}
    planned_risk = abs(entry_price - initial_stop_price) * size * price_delta_multiplier
    if planned_risk <= 0:
        return None
    actual_loss = (
        abs(actual_price_loss_usdt)
        if actual_price_loss_usdt is not None
        else abs(entry_price - actual_exit_price) * size * price_delta_multiplier
    )
    adverse_slippage_price = (
        max(effective_stop_price - actual_exit_price, Decimal("0"))
        if is_long
        else max(actual_exit_price - effective_stop_price, Decimal("0"))
    )
    slippage_usdt = adverse_slippage_price * size * price_delta_multiplier
    overrun_usdt = max(actual_loss - planned_risk, Decimal("0"))
    overrun_pct = overrun_usdt / planned_risk * Decimal("100")
    resolved = thresholds or StopExecutionThresholds()
    if overrun_usdt >= resolved.critical_usdt or actual_loss >= planned_risk * resolved.critical_loss_multiple:
        status = "critical"
    elif overrun_usdt > resolved.warning_usdt and overrun_pct > resolved.warning_pct:
        status = "warning"
    else:
        status = "normal"
    return StopExecutionAssessment(
        planned_risk_usdt=planned_risk,
        actual_price_loss_usdt=actual_loss,
        effective_stop_price=effective_stop_price,
        actual_exit_price=actual_exit_price,
        stop_slippage_price=adverse_slippage_price,
        stop_slippage_usdt=slippage_usdt,
        stop_overrun_usdt=overrun_usdt,
        stop_overrun_pct=overrun_pct,
        status=status,
    )


def format_stop_execution_summary(assessment: StopExecutionAssessment) -> str:
    status_label = {"normal": "NORMAL", "warning": "WARNING", "critical": "CRITICAL"}.get(
        assessment.status,
        assessment.status.upper(),
    )
    return (
        f"止损执行归因 | 等级={status_label}"
        f" | 计划风险={assessment.planned_risk_usdt:.2f}U"
        f" | 实际价格亏损={assessment.actual_price_loss_usdt:.2f}U"
        f" | 有效止损={assessment.effective_stop_price}"
        f" | 实际平仓={assessment.actual_exit_price}"
        f" | 止损滑点={assessment.stop_slippage_price}"
        f" | 止损滑点金额={assessment.stop_slippage_usdt:.2f}U"
        f" | 风险超额={assessment.stop_overrun_usdt:.2f}U"
        f" | 超额比例={assessment.stop_overrun_pct:.1f}%"
    )
