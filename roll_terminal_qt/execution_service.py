from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.arbitrage_executor import ArbitrageRollRequest
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxRestClient

from roll_terminal_qt.account_service import FuturesPositionView


EXECUTION_MODE_BY_LABEL = {
    "双腿吃单": "dual_taker",
    "旧合约挂单/新合约吃单": "old_maker_new_taker",
    "新合约挂单/旧合约吃单": "new_maker_old_taker",
    "双方挂单/先成后市价": "both_maker_first_taker",
}


@dataclass(frozen=True)
class RollExecutionPlan:
    current: FuturesPositionView
    target_inst_id: str
    qty: Decimal
    execution_label: str
    execution_mode_value: str = ""
    max_slippage: Decimal = Decimal("0.0015")
    use_limit_orders: bool = False
    current_limit_price: Decimal | None = None
    target_limit_price: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3

    @property
    def execution_mode(self) -> str:
        if self.execution_mode_value:
            return self.execution_mode_value
        return EXECUTION_MODE_BY_LABEL.get(self.execution_label, "dual_taker")


@dataclass(frozen=True)
class RollDirectionSummary:
    current_order_side: str
    current_action_text: str
    target_order_side: str
    target_action_text: str

    @property
    def summary_text(self) -> str:
        return f"旧合约 {self.current_action_text} | 目标合约 {self.target_action_text}"


@dataclass(frozen=True)
class ExecutionStatus:
    phase: str
    current_inst_id: str
    target_inst_id: str
    current_filled: Decimal
    target_filled: Decimal
    message: str
    success: bool | None = None


class RollExecutionThread(QThread):
    log = Signal(str)
    status = Signal(object)
    finished_with_result = Signal(object)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, plan: RollExecutionPlan) -> None:
        super().__init__()
        self._runtime = runtime
        self._plan = plan

    def run(self) -> None:
        client = OkxRestClient()
        manager = ArbitrageManager(client, logger=self.log.emit)
        request = self._build_request()
        self.status.emit(
            ExecutionStatus(
                phase="提交",
                current_inst_id=str(request.current_derivative_inst_id or ""),
                target_inst_id=request.target_derivative_inst_id,
                current_filled=Decimal("0"),
                target_filled=Decimal("0"),
                message="订单请求已构造，准备提交",
            )
        )
        self.log.emit(
            f"提交移仓：{request.current_derivative_inst_id} -> {request.target_derivative_inst_id} "
            f"{self._plan.qty} 张 | {self._plan.execution_label} | {roll_direction_from_position(self._plan.current).summary_text}"
        )
        self.status.emit(
            ExecutionStatus(
                phase="执行中",
                current_inst_id=str(request.current_derivative_inst_id or ""),
                target_inst_id=request.target_derivative_inst_id,
                current_filled=Decimal("0"),
                target_filled=Decimal("0"),
                message="正在等待 OKX 委托/成交回报",
            )
        )
        result = manager.roll_now(request, runtime=self._runtime)
        self.status.emit(
            ExecutionStatus(
                phase="完成" if result.success else "失败",
                current_inst_id=str(request.current_derivative_inst_id or ""),
                target_inst_id=request.target_derivative_inst_id,
                current_filled=result.rolled_derivative_qty,
                target_filled=result.target_derivative_filled_qty,
                message=result.message,
                success=result.success,
            )
        )
        self.finished_with_result.emit(result)

    def _build_request(self) -> ArbitrageRollRequest:
        current = self._plan.current
        base_ccy = current.inst_id.split("-")[0].strip().upper()
        return ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id=self._plan.target_inst_id,
            max_slippage=self._plan.max_slippage,
            use_limit_orders=self._plan.use_limit_orders,
            roll_derivative_qty=self._plan.qty,
            current_derivative_limit_price=self._plan.current_limit_price,
            target_derivative_limit_price=self._plan.target_limit_price,
            batch_count=self._plan.batch_count,
            batch_contract_qty=self._plan.batch_contract_qty,
            execution_mode=self._plan.execution_mode,
            maker_wait_seconds=self._plan.maker_wait_seconds,
            chase_limit=self._plan.chase_limit,
            base_ccy=base_ccy,
            spot_inst_id=f"{base_ccy}-USDT",
            current_derivative_inst_id=current.inst_id,
            spot_qty=current.notional_base or Decimal("0"),
            current_derivative_qty=current.available,
            current_position_side=str(current.side or "").strip().lower() or None,
        )


def parse_roll_qty(text: str, *, max_qty: Decimal) -> Decimal:
    try:
        qty = Decimal(text.strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("移仓数量格式不正确") from exc
    if qty <= 0:
        raise ValueError("移仓数量必须大于 0")
    if qty > max_qty:
        raise ValueError(f"移仓数量不能超过可平 {max_qty} 张")
    return qty


def roll_direction_from_position(position: FuturesPositionView) -> RollDirectionSummary:
    side_raw = str(position.side or "").strip()
    side_lower = side_raw.lower()
    is_short = side_raw == "\u7a7a" or side_lower == "short"
    if is_short:
        return RollDirectionSummary(
            current_order_side="buy",
            current_action_text="\u4e70\u5165\u5e73\u7a7a",
            target_order_side="sell",
            target_action_text="\u5356\u51fa\u5f00\u7a7a",
        )
    return RollDirectionSummary(
        current_order_side="sell",
        current_action_text="\u5356\u51fa\u5e73\u591a",
        target_order_side="buy",
        target_action_text="\u4e70\u5165\u5f00\u591a",
    )


def parse_optional_decimal(text: str, *, field_name: str) -> Decimal | None:
    normalized = text.strip()
    if not normalized:
        return None
    try:
        value = Decimal(normalized)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name}格式不正确") from exc
    if value <= 0:
        raise ValueError(f"{field_name}必须大于 0")
    return value


def parse_slippage_percent(text: str) -> Decimal:
    try:
        value = Decimal(text.strip() or "0.15")
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("最大滑点格式不正确") from exc
    if value < 0:
        raise ValueError("最大滑点不能小于 0")
    return value / Decimal("100")


def parse_positive_int(text: str, *, field_name: str, default: int) -> int:
    raw = text.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name}格式不正确") from exc
    if value <= 0:
        raise ValueError(f"{field_name}必须大于 0")
    return value


def parse_nonnegative_int(text: str, *, field_name: str, default: int) -> int:
    raw = text.strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name}格式不正确") from exc
    if value < 0:
        raise ValueError(f"{field_name}不能小于 0")
    return value


def parse_positive_float(text: str, *, field_name: str, default: float) -> float:
    raw = text.strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name}格式不正确") from exc
    if value <= 0:
        raise ValueError(f"{field_name}必须大于 0")
    return value
