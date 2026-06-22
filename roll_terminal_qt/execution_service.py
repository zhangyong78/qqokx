from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.arbitrage_executor import ArbitrageOpenRequest, ArbitrageRollRequest
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxFillHistoryItem, OkxRestClient, OkxTradeOrderItem

from roll_terminal_qt.account_service import FuturesPositionView
from roll_terminal_qt.formatting import fmt_decimal


EXECUTION_MODE_BY_LABEL = {
    "双腿吃单": "dual_taker",
    "旧合约挂单/新合约吃单": "old_maker_new_taker",
    "新合约挂单/旧合约吃单": "new_maker_old_taker",
    "双方挂单/先成后市价": "both_maker_first_taker",
}

OPEN_EXECUTION_MODE_BY_LABEL = {
    "双腿吃单": "dual_taker",
    "现货挂单/合约吃单": "spot_maker_derivative_taker",
    "合约挂单/现货吃单": "derivative_maker_spot_taker",
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
    force_completion: bool = False

    @property
    def execution_mode(self) -> str:
        if self.execution_mode_value:
            return self.execution_mode_value
        return EXECUTION_MODE_BY_LABEL.get(self.execution_label, "dual_taker")


@dataclass(frozen=True)
class ProfessionalOpenExecutionPlan:
    left_inst_id: str
    right_inst_id: str
    spot_inst_id: str
    derivative_inst_id: str
    qty_contracts: Decimal
    execution_label: str
    execution_mode_value: str = ""
    max_slippage: Decimal = Decimal("0.0015")
    use_limit_orders: bool = False
    spot_limit_price: Decimal | None = None
    derivative_limit_price: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3

    @property
    def execution_mode(self) -> str:
        if self.execution_mode_value:
            return self.execution_mode_value
        return OPEN_EXECUTION_MODE_BY_LABEL.get(self.execution_label, "dual_taker")


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
        started_at_ms = int(time.time() * 1000)
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
        self._attach_completion_summary(
            client=client,
            request=request,
            result=result,
            started_at_ms=started_at_ms,
        )
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

    def _attach_completion_summary(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        result,
        started_at_ms: int,
    ) -> None:  # noqa: ANN001
        summary_lines = self._build_completion_summary_lines(
            client=client,
            request=request,
            result=result,
            started_at_ms=started_at_ms,
        )
        if not summary_lines:
            return
        result.message = f"{result.message}\n" + "\n".join(summary_lines)

    def _build_completion_summary_lines(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        result,
        started_at_ms: int,
    ) -> list[str]:  # noqa: ANN001
        lines = ["本次移仓结算："]
        direction = roll_direction_from_position(self._plan.current)
        current_avg_price = getattr(result, "current_derivative_avg_price", None)
        target_avg_price = getattr(result, "target_derivative_avg_price", None)
        if current_avg_price is not None:
            lines.append(f"旧合约{direction.current_action_text}均价：{fmt_decimal(current_avg_price, 2)}")
        if target_avg_price is not None:
            lines.append(f"目标合约{direction.target_action_text}均价：{fmt_decimal(target_avg_price, 2)}")
        if current_avg_price is not None and target_avg_price is not None and current_avg_price > 0:
            avg_spread = target_avg_price - current_avg_price
            spread_pct = (avg_spread / current_avg_price) * Decimal("100")
            lines.append(
                f"平均价差(目标-当前)：{fmt_decimal(avg_spread, 2)} | {fmt_decimal(spread_pct, 2)}%"
            )
        fee_line = self._build_fee_summary_line(
            client=client,
            request=request,
            started_at_ms=started_at_ms,
        )
        if fee_line:
            lines.append(fee_line)
        elif len(lines) == 1:
            return []
        return lines

    def _build_fee_summary_line(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        started_at_ms: int,
    ) -> str | None:
        matched_orders = self._load_recent_roll_orders(
            client=client,
            request=request,
            started_at_ms=started_at_ms,
        )
        matched_order_ids = {
            str(item.order_id or "").strip()
            for item in matched_orders
            if str(item.order_id or "").strip()
        }
        matched_fills = self._load_recent_roll_fills(
            client=client,
            request=request,
            started_at_ms=started_at_ms,
            matched_order_ids=matched_order_ids,
        )
        fee_totals = self._sum_fill_fees(matched_fills)
        if fee_totals:
            return f"本次移仓费用：{self._format_fee_totals(fee_totals)}"
        fee_totals = self._sum_order_fees(matched_orders)
        if fee_totals:
            return f"本次移仓费用：{self._format_fee_totals(fee_totals)}"
        return "本次移仓费用：暂未从 OKX 回报中取到，请稍后看订单历史。"

    def _load_recent_roll_orders(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        started_at_ms: int,
    ) -> list[OkxTradeOrderItem]:
        current_side = roll_direction_from_position(self._plan.current).current_order_side.lower()
        target_side = roll_direction_from_position(self._plan.current).target_order_side.lower()
        window_start = started_at_ms - 15000
        matched: list[OkxTradeOrderItem] = []
        for attempt in range(4):
            try:
                history = client.get_order_history(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_types=("FUTURES",),
                    limit=80,
                    include_algo=False,
                )
            except Exception:  # noqa: BLE001
                history = []
            matched = []
            for item in history:
                event_time = item.update_time or item.created_time or 0
                if event_time and event_time < window_start:
                    continue
                inst_id = str(item.inst_id or "").strip().upper()
                side = str(item.side or "").strip().lower()
                if inst_id == str(request.current_derivative_inst_id or "").strip().upper():
                    if side != current_side:
                        continue
                elif inst_id == request.target_derivative_inst_id.strip().upper():
                    if side != target_side:
                        continue
                else:
                    continue
                if (item.filled_size or Decimal("0")) <= 0 and item.fee is None:
                    continue
                matched.append(item)
            if matched or attempt == 3:
                return matched
            time.sleep(0.6)
        return matched

    def _load_recent_roll_fills(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        started_at_ms: int,
        matched_order_ids: set[str],
    ) -> list[OkxFillHistoryItem]:
        current_side = roll_direction_from_position(self._plan.current).current_order_side.lower()
        target_side = roll_direction_from_position(self._plan.current).target_order_side.lower()
        window_start = started_at_ms - 15000
        matched: list[OkxFillHistoryItem] = []
        for attempt in range(4):
            try:
                fills = client.get_fills_history(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_types=("FUTURES",),
                    limit=160,
                )
            except Exception:  # noqa: BLE001
                fills = []
            matched = []
            for item in fills:
                fill_time = item.fill_time or 0
                if fill_time and fill_time < window_start:
                    continue
                inst_id = str(item.inst_id or "").strip().upper()
                side = str(item.side or "").strip().lower()
                if inst_id == str(request.current_derivative_inst_id or "").strip().upper():
                    if side != current_side:
                        continue
                elif inst_id == request.target_derivative_inst_id.strip().upper():
                    if side != target_side:
                        continue
                else:
                    continue
                order_id = str(item.order_id or "").strip()
                if matched_order_ids and order_id and order_id not in matched_order_ids:
                    continue
                matched.append(item)
            if matched or attempt == 3:
                return matched
            time.sleep(0.6)
        return matched

    @staticmethod
    def _sum_fill_fees(items: list[OkxFillHistoryItem]) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for item in items:
            if item.fill_fee is None:
                continue
            currency = str(item.fee_currency or "").strip().upper() or "UNKNOWN"
            totals[currency] = totals.get(currency, Decimal("0")) + item.fill_fee
        return totals

    @staticmethod
    def _sum_order_fees(items: list[OkxTradeOrderItem]) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for item in items:
            if item.fee is None:
                continue
            currency = str(item.fee_currency or "").strip().upper() or "UNKNOWN"
            totals[currency] = totals.get(currency, Decimal("0")) + item.fee
        return totals

    @staticmethod
    def _format_fee_totals(fee_totals: dict[str, Decimal]) -> str:
        parts: list[str] = []
        for currency in sorted(fee_totals):
            parts.append(f"{fmt_decimal(fee_totals[currency], 2)} {currency}")
        return " | ".join(parts)

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
            force_execution_completion=self._plan.force_completion,
            base_ccy=base_ccy,
            spot_inst_id=f"{base_ccy}-USDT",
            current_derivative_inst_id=current.inst_id,
            spot_qty=current.notional_base or Decimal("0"),
            current_derivative_qty=current.available,
            current_position_side=str(current.side or "").strip().lower() or None,
        )


class ProfessionalOpenExecutionThread(QThread):
    log = Signal(str)
    status = Signal(object)
    finished_with_result = Signal(object)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, plan: ProfessionalOpenExecutionPlan) -> None:
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
                current_inst_id=self._plan.left_inst_id,
                target_inst_id=self._plan.right_inst_id,
                current_filled=Decimal("0"),
                target_filled=Decimal("0"),
                message="双腿开仓请求已构造，准备提交",
            )
        )
        self.log.emit(
            f"提交双腿开仓：{self._plan.left_inst_id} <-> {self._plan.right_inst_id} "
            f"| 合约张数 {self._plan.qty_contracts} | {self._plan.execution_label}"
        )
        self.status.emit(
            ExecutionStatus(
                phase="执行中",
                current_inst_id=self._plan.left_inst_id,
                target_inst_id=self._plan.right_inst_id,
                current_filled=Decimal("0"),
                target_filled=Decimal("0"),
                message="正在等待双腿委托/成交回报",
            )
        )
        result = manager.open_now(request, runtime=self._runtime)
        self._attach_completion_summary(result)
        self.status.emit(
            ExecutionStatus(
                phase="完成" if result.success else "失败",
                current_inst_id=self._plan.left_inst_id,
                target_inst_id=self._plan.right_inst_id,
                current_filled=result.spot_filled_qty,
                target_filled=result.derivative_filled_qty,
                message=result.message,
                success=result.success,
            )
        )
        self.finished_with_result.emit(result)

    def _build_request(self) -> ArbitrageOpenRequest:
        base_ccy = self._plan.spot_inst_id.split("-")[0].strip().upper()
        return ArbitrageOpenRequest(
            base_ccy=base_ccy,
            spot_inst_id=self._plan.spot_inst_id,
            derivative_inst_id=self._plan.derivative_inst_id,
            size=self._plan.qty_contracts,
            size_unit="contracts",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=None,
            spot_limit_price=self._plan.spot_limit_price,
            derivative_limit_price=self._plan.derivative_limit_price,
            use_limit_orders=self._plan.use_limit_orders,
            max_slippage=self._plan.max_slippage,
            batch_count=self._plan.batch_count,
            batch_contract_qty=self._plan.batch_contract_qty,
            execution_mode=self._plan.execution_mode,
            maker_wait_seconds=self._plan.maker_wait_seconds,
            chase_limit=self._plan.chase_limit,
        )

    def _attach_completion_summary(self, result) -> None:  # noqa: ANN001
        spot_avg = getattr(result, "spot_avg_price", None)
        derivative_avg = getattr(result, "derivative_avg_price", None)
        if spot_avg is None and derivative_avg is None:
            return
        lines = ["本次双腿开仓结算："]
        if spot_avg is not None:
            lines.append(f"现货买入均价：{fmt_decimal(spot_avg, 2)}")
        if derivative_avg is not None:
            lines.append(f"衍生品卖出均价：{fmt_decimal(derivative_avg, 2)}")
        if spot_avg is not None and derivative_avg is not None and spot_avg > 0:
            spread_abs = derivative_avg - spot_avg
            spread_pct = (spread_abs / spot_avg) * Decimal("100")
            lines.append(f"开仓价差(衍生品-现货)：{fmt_decimal(spread_abs, 2)} | {fmt_decimal(spread_pct, 2)}%")
        result.message = f"{result.message}\n" + "\n".join(lines)


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
