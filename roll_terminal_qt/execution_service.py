from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from typing import Callable

from PySide6.QtCore import QThread, Signal

from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.arbitrage.arbitrage_executor import (
    ArbitrageCloseRequest,
    ArbitrageOpenRequest,
    ArbitrageRollRequest,
    _build_strategy_config,
    _split_derivative_batches,
    _wait_order_fill,
)
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.arbitrage.position_ledger import find_ledger_entry, load_open_ledger_entries
from okx_quant.okx_client import OkxApiError, OkxFillHistoryItem, OkxRestClient, OkxTradeOrderItem
from okx_quant.pricing import snap_to_increment

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
    derivative_lot_size: Decimal = Decimal("1")
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
    size_value: Decimal
    size_unit: str
    execution_label: str
    execution_mode_value: str = ""
    max_slippage: Decimal = Decimal("0.0015")
    use_limit_orders: bool = False
    spot_limit_price: Decimal | None = None
    derivative_limit_price: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    derivative_lot_size: Decimal = Decimal("1")
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3

    @property
    def execution_mode(self) -> str:
        if self.execution_mode_value:
            return self.execution_mode_value
        return OPEN_EXECUTION_MODE_BY_LABEL.get(self.execution_label, "dual_taker")


@dataclass(frozen=True)
class ProfessionalCloseExecutionPlan:
    left_inst_id: str
    right_inst_id: str
    spot_inst_id: str
    derivative_inst_id: str
    entry_ids: tuple[str, ...]
    qty_contracts: Decimal
    execution_label: str
    execution_mode_value: str = ""
    max_slippage: Decimal = Decimal("0.0015")
    use_limit_orders: bool = False
    spot_limit_price: Decimal | None = None
    derivative_limit_price: Decimal | None = None
    batch_count: int = 1
    batch_contract_qty: Decimal | None = None
    derivative_lot_size: Decimal = Decimal("1")
    maker_wait_seconds: float = 6.0
    chase_limit: int = 3
    close_profit_spot: bool = False
    position_available_contracts: Decimal = Decimal("0")

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


def _load_spread_abs_from_public_market(
    client: OkxRestClient,
    *,
    left_inst_id: str,
    right_inst_id: str,
    environment: str,
) -> Decimal | None:
    client.ensure_public_ws_market_watch(left_inst_id, environment=environment)
    client.ensure_public_ws_market_watch(right_inst_id, environment=environment)
    left_payload = client.get_cached_public_ticker(left_inst_id, environment=environment)
    right_payload = client.get_cached_public_ticker(right_inst_id, environment=environment)
    left_ticker = left_payload[1] if left_payload is not None else client.get_ticker(left_inst_id)
    right_ticker = right_payload[1] if right_payload is not None else client.get_ticker(right_inst_id)
    left_mid = mid_price(left_ticker.bid, left_ticker.ask) or left_ticker.last
    right_mid = mid_price(right_ticker.bid, right_ticker.ask) or right_ticker.last
    if left_mid is None or left_mid <= 0 or right_mid is None:
        return None
    return right_mid - left_mid


def _wait_for_auto_spread_resume(
    *,
    client: OkxRestClient,
    environment: str,
    left_inst_id: str,
    right_inst_id: str,
    threshold: Decimal,
    should_stop: Callable[[], bool],
    logger: Callable[[str], None],
    trigger_when: str = "gte",
    wait_seconds: float = 0.8,
) -> bool:
    if trigger_when not in {"gte", "lte"}:
        raise ValueError(f"Unsupported auto trigger comparator: {trigger_when}")
    resume_operator = "<=" if trigger_when == "lte" else ">="
    wait_operator = ">" if trigger_when == "lte" else "<"
    waiting_logged = False
    while True:
        if should_stop():
            logger("已收到停止请求：当前批次完成后结束自动交易，不再继续下一批。")
            return False
        try:
            spread_abs = _load_spread_abs_from_public_market(
                client,
                left_inst_id=left_inst_id,
                right_inst_id=right_inst_id,
                environment=environment,
            )
        except Exception as exc:  # noqa: BLE001
            if not waiting_logged:
                logger(f"自动交易等待下一批：读取最新价差失败（{exc}），继续等待后重试。")
                waiting_logged = True
            time.sleep(wait_seconds)
            continue
        condition_met = spread_abs is not None and (
            spread_abs <= threshold if trigger_when == "lte" else spread_abs >= threshold
        )
        if condition_met:
            if waiting_logged:
                logger(
                    f"自动交易条件恢复：当前价差 {fmt_decimal(spread_abs)} {resume_operator} {fmt_decimal(threshold)}，继续下一批。"
                )
            return True
        if not waiting_logged:
            spread_text = fmt_decimal(spread_abs) if spread_abs is not None else "-"
            logger(
                f"自动交易等待下一批：当前价差 {spread_text} {wait_operator} {fmt_decimal(threshold)}，"
                "暂停后续批次，等待重新满足后继续。"
            )
            waiting_logged = True
        time.sleep(wait_seconds)


class RollExecutionThread(QThread):
    log = Signal(str)
    status = Signal(object)
    finished_with_result = Signal(object)

    def __init__(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        plan: RollExecutionPlan,
        auto_pause_threshold: Decimal | None = None,
    ) -> None:
        super().__init__()
        self._runtime = runtime
        self._plan = plan
        self._stop_after_batch_requested = False
        self._auto_pause_threshold = auto_pause_threshold

    @property
    def supports_stop_after_batch(self) -> bool:
        return True

    @property
    def stop_after_batch_requested(self) -> bool:
        return self._stop_after_batch_requested

    def request_stop_after_batch(self) -> bool:
        already_requested = self._stop_after_batch_requested
        self._stop_after_batch_requested = True
        return not already_requested

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
        result = manager.roll_now(
            request,
            runtime=self._runtime,
            should_stop_after_batch=lambda: self._stop_after_batch_requested,
            wait_before_next_batch=self._build_auto_wait_before_next_batch(client, request),
        )
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

    def _build_auto_wait_before_next_batch(self, client: OkxRestClient, request: ArbitrageRollRequest) -> Callable[[], bool] | None:
        threshold = self._auto_pause_threshold
        current_inst_id = str(request.current_derivative_inst_id or self._plan.current.inst_id or "").strip().upper()
        target_inst_id = str(request.target_derivative_inst_id or "").strip().upper()
        if threshold is None or not current_inst_id or not target_inst_id:
            return None
        return lambda: _wait_for_auto_spread_resume(
            client=client,
            environment=self._runtime.environment,
            left_inst_id=current_inst_id,
            right_inst_id=target_inst_id,
            threshold=threshold,
            should_stop=lambda: self._stop_after_batch_requested,
            logger=self.log.emit,
        )

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
        executed_contract_qty = self._resolve_executed_roll_contract_qty(result)
        history_limit = self._estimate_roll_fee_history_limit(executed_contract_qty=executed_contract_qty)
        fee_pending = self._should_defer_fee_summary_for_result(result)
        fee_line, fee_total_usdt = self._build_fee_summary_line(
            client=client,
            request=request,
            result=result,
            started_at_ms=started_at_ms,
            pending_only=fee_pending,
            history_limit=history_limit,
        )
        if fee_line:
            lines.append(fee_line)
            if fee_pending:
                return lines
            fee_per_coin_usdt = self._compute_fee_per_coin_usdt(
                total_fee_usdt=fee_total_usdt,
                current_avg_price=current_avg_price,
                target_avg_price=target_avg_price,
                executed_contract_qty=executed_contract_qty,
            )
            fee_per_coin_line = self._build_fee_per_coin_line(
                fee_per_coin_usdt=fee_per_coin_usdt,
                avg_spread=avg_spread if 'avg_spread' in locals() else None,
            )
            if fee_per_coin_line:
                lines.append(fee_per_coin_line)
                net_spread_line = self._build_net_spread_after_fee_line(
                    fee_per_coin_usdt=fee_per_coin_usdt,
                    avg_spread=avg_spread if 'avg_spread' in locals() else None,
                )
                if net_spread_line:
                    lines.append(net_spread_line)
        elif len(lines) == 1:
            return []
        return lines

    def _build_fee_summary_line(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        result,
        started_at_ms: int,
        pending_only: bool = False,
        history_limit: int = 200,
    ) -> tuple[str | None, Decimal | None]:
        explicit_order_ids = {
            str(order_id).strip()
            for order_id in getattr(result, "order_ids", ()) or ()
            if str(order_id).strip()
        }
        matched_orders = self._load_recent_roll_orders(
            client=client,
            request=request,
            started_at_ms=started_at_ms,
            explicit_order_ids=explicit_order_ids,
            history_limit=history_limit,
        )
        matched_order_ids_by_leg: dict[str, set[str]] = {"current": set(), "target": set()}
        for item in matched_orders:
            leg = self._classify_roll_leg(
                inst_id=item.inst_id,
                side=item.side,
                request=request,
            )
            order_id = str(item.order_id or "").strip()
            if leg is None or not order_id:
                continue
            matched_order_ids_by_leg[leg].add(order_id)
        matched_fills = self._load_recent_roll_fills(
            client=client,
            request=request,
            started_at_ms=started_at_ms,
            explicit_order_ids=explicit_order_ids,
            matched_order_ids_by_leg=matched_order_ids_by_leg,
            history_limit=max(history_limit * 2, history_limit),
        )
        fee_totals = self._sum_fill_fees(matched_fills)
        if fee_totals:
            fee_usdt_totals = self._estimate_fill_fee_usdt_totals(matched_fills)
            total_fee_usdt = self._sum_fee_usdt_total(fee_totals, fee_usdt_totals)
            if pending_only:
                return (
                    f"本次双腿手续费：当前仍有未终态委托，以下仅为已确认部分 {self._format_fee_totals(fee_totals, fee_usdt_totals)}；"
                    "最终手续费待订单终态后再确认。",
                    None,
                )
            return f"\u672c\u6b21\u53cc\u817f\u5408\u8ba1\u624b\u7eed\u8d39\uff1a{self._format_fee_totals(fee_totals, fee_usdt_totals)}", total_fee_usdt
        fee_totals = self._sum_order_fees(matched_orders)
        if fee_totals:
            fee_usdt_totals = self._estimate_order_fee_usdt_totals(matched_orders)
            total_fee_usdt = self._sum_fee_usdt_total(fee_totals, fee_usdt_totals)
            if pending_only:
                return (
                    f"本次双腿手续费：当前仍有未终态委托，以下仅为已确认部分 {self._format_fee_totals(fee_totals, fee_usdt_totals)}；"
                    "最终手续费待订单终态后再确认。",
                    None,
                )
            return f"\u672c\u6b21\u53cc\u817f\u5408\u8ba1\u624b\u7eed\u8d39\uff1a{self._format_fee_totals(fee_totals, fee_usdt_totals)}", total_fee_usdt
        if pending_only:
            return "本次双腿手续费：当前仍有未终态委托，手续费待订单终态后再确认。", None
        return "\u672c\u6b21\u53cc\u817f\u5408\u8ba1\u624b\u7eed\u8d39\uff1a\u6682\u672a\u4ece OKX \u56de\u62a5\u4e2d\u53d6\u5230\uff0c\u8bf7\u7a0d\u540e\u770b\u8ba2\u5355\u5386\u53f2\u3002", None

    @staticmethod
    def _should_defer_fee_summary_for_result(result) -> bool:  # noqa: ANN001
        message = str(getattr(result, "message", "") or "")
        if not message:
            return False
        return any(
            keyword in message
            for keyword in (
                "未进入终态",
                "当前状态：live",
                "最近状态：live",
                "仍有未终态委托",
                "恢复核对",
            )
        )

    def _load_recent_roll_orders(
        self,
        *,
        client: OkxRestClient,
        request: ArbitrageRollRequest,
        started_at_ms: int,
        explicit_order_ids: set[str] | None = None,
        history_limit: int = 200,
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
                    limit=max(100, history_limit),
                    include_algo=False,
                )
            except Exception:  # noqa: BLE001
                history = []
            matched = []
            for item in history:
                order_id = str(item.order_id or "").strip()
                if explicit_order_ids:
                    if order_id in explicit_order_ids:
                        matched.append(item)
                    continue
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
        explicit_order_ids: set[str] | None,
        matched_order_ids_by_leg: dict[str, set[str]],
        history_limit: int = 400,
    ) -> list[OkxFillHistoryItem]:
        window_start = started_at_ms - 15000
        matched: list[OkxFillHistoryItem] = []
        for attempt in range(4):
            try:
                fills = client.get_fills_history(
                    self._runtime.credentials,
                    environment=self._runtime.environment,
                    inst_types=("FUTURES",),
                    limit=max(200, history_limit),
                )
            except Exception:  # noqa: BLE001
                fills = []
            candidates_by_leg: dict[str, list[OkxFillHistoryItem]] = {"current": [], "target": []}
            exact_matches_by_leg: dict[str, list[OkxFillHistoryItem]] = {"current": [], "target": []}
            for item in fills:
                order_id = str(item.order_id or "").strip()
                if explicit_order_ids:
                    if order_id in explicit_order_ids:
                        matched.append(item)
                    continue
                fill_time = item.fill_time or 0
                if fill_time and fill_time < window_start:
                    continue
                leg = self._classify_roll_leg(
                    inst_id=item.inst_id,
                    side=item.side,
                    request=request,
                )
                if leg is None:
                    continue
                candidates_by_leg[leg].append(item)
                leg_order_ids = matched_order_ids_by_leg.get(leg, set())
                if order_id and order_id in leg_order_ids:
                    exact_matches_by_leg[leg].append(item)
            if explicit_order_ids:
                if matched or attempt == 3:
                    return matched
                time.sleep(0.6)
                continue
            matched = []
            for leg in ("current", "target"):
                leg_order_ids = matched_order_ids_by_leg.get(leg, set())
                leg_matches = exact_matches_by_leg[leg] if leg_order_ids else candidates_by_leg[leg]
                if leg_order_ids and not leg_matches:
                    # Order history may lag a fill; fall back to the time/inst/side-matched leg fills.
                    leg_matches = candidates_by_leg[leg]
                matched.extend(leg_matches)
            if matched or attempt == 3:
                return matched
            time.sleep(0.6)
        return matched

    def _classify_roll_leg(
        self,
        *,
        inst_id: str | None,
        side: str | None,
        request: ArbitrageRollRequest,
    ) -> str | None:
        direction = roll_direction_from_position(self._plan.current)
        normalized_inst_id = str(inst_id or "").strip().upper()
        normalized_side = str(side or "").strip().lower()
        current_inst_id = str(request.current_derivative_inst_id or "").strip().upper()
        target_inst_id = str(request.target_derivative_inst_id or "").strip().upper()
        if normalized_inst_id == current_inst_id:
            return "current" if normalized_side == direction.current_order_side.lower() else None
        if normalized_inst_id == target_inst_id:
            return "target" if normalized_side == direction.target_order_side.lower() else None
        return None

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
    def _estimate_fill_fee_usdt_totals(items: list[OkxFillHistoryItem]) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for item in items:
            fee_amount = item.fill_fee
            if fee_amount is None:
                continue
            currency = str(item.fee_currency or "").strip().upper() or "UNKNOWN"
            fee_usdt = RollExecutionThread._estimate_fee_usdt(
                fee_amount=fee_amount,
                fee_currency=currency,
                inst_id=item.inst_id,
                price=item.fill_price,
            )
            if fee_usdt is None:
                continue
            totals[currency] = totals.get(currency, Decimal("0")) + fee_usdt
        return totals

    @staticmethod
    def _estimate_order_fee_usdt_totals(items: list[OkxTradeOrderItem]) -> dict[str, Decimal]:
        totals: dict[str, Decimal] = {}
        for item in items:
            fee_amount = item.fee
            if fee_amount is None:
                continue
            currency = str(item.fee_currency or "").strip().upper() or "UNKNOWN"
            reference_price = item.avg_price or item.actual_price or item.price
            fee_usdt = RollExecutionThread._estimate_fee_usdt(
                fee_amount=fee_amount,
                fee_currency=currency,
                inst_id=item.inst_id,
                price=reference_price,
            )
            if fee_usdt is None:
                continue
            totals[currency] = totals.get(currency, Decimal("0")) + fee_usdt
        return totals

    @staticmethod
    def _estimate_fee_usdt(
        *,
        fee_amount: Decimal,
        fee_currency: str,
        inst_id: str,
        price: Decimal | None,
    ) -> Decimal | None:
        currency = str(fee_currency or "").strip().upper()
        if not currency:
            return None
        if currency in {"USDT", "USD", "USDC"}:
            return fee_amount
        base_currency = str(inst_id or "").strip().upper().split("-")[0]
        if currency != base_currency or price is None or price <= 0:
            return None
        return fee_amount * price

    @staticmethod
    def _format_usdt_amount(value: Decimal) -> str:
        text = fmt_decimal(value, 4)
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text

    @staticmethod
    def _sum_fee_usdt_total(
        fee_totals: dict[str, Decimal],
        fee_usdt_totals: dict[str, Decimal] | None = None,
    ) -> Decimal | None:
        total = Decimal("0")
        has_value = False
        for currency, amount in fee_totals.items():
            if currency in {"USDT", "USD", "USDC"}:
                total += amount
                has_value = True
                continue
            if fee_usdt_totals is None:
                continue
            approx_value = fee_usdt_totals.get(currency)
            if approx_value is None:
                continue
            total += approx_value
            has_value = True
        return total if has_value else None

    def _compute_fee_per_coin_usdt(
        self,
        *,
        total_fee_usdt: Decimal | None,
        current_avg_price: Decimal | None,
        target_avg_price: Decimal | None,
        executed_contract_qty: Decimal | None = None,
    ) -> Decimal | None:
        if total_fee_usdt is None:
            return None
        base_qty = self._estimate_roll_base_qty(
            current_avg_price=current_avg_price,
            target_avg_price=target_avg_price,
            executed_contract_qty=executed_contract_qty,
        )
        if base_qty is None or base_qty <= 0:
            return None
        return abs(total_fee_usdt) / base_qty

    def _build_fee_per_coin_line(
        self,
        *,
        fee_per_coin_usdt: Decimal | None,
        avg_spread: Decimal | None,
    ) -> str | None:
        if fee_per_coin_usdt is None:
            return None
        base_ccy = str(self._plan.current.inst_id or "").strip().upper().split("-")[0] or "BTC"
        line = f"\u6309 1 {base_ccy} \u6298\u7b97\u624b\u7eed\u8d39\uff1a{self._format_usdt_amount(fee_per_coin_usdt)} USDT"
        if avg_spread is not None and avg_spread != 0:
            spread_share_pct = (fee_per_coin_usdt / abs(avg_spread)) * Decimal("100")
            line += f" | \u7ea6\u5360\u672c\u6b21\u5e73\u5747\u4ef7\u5dee {fmt_decimal(spread_share_pct, 2)}%"
        return line

    def _build_net_spread_after_fee_line(
        self,
        *,
        fee_per_coin_usdt: Decimal | None,
        avg_spread: Decimal | None,
    ) -> str | None:
        if fee_per_coin_usdt is None or avg_spread is None:
            return None
        spread_direction = Decimal("1") if avg_spread >= 0 else Decimal("-1")
        net_spread = avg_spread - (spread_direction * fee_per_coin_usdt)
        return f"\u6263\u53cc\u817f\u624b\u7eed\u8d39\u540e\u51c0\u4ef7\u5dee\uff1a{self._format_usdt_amount(net_spread)} USDT/BTC"

    def _estimate_roll_base_qty(
        self,
        *,
        current_avg_price: Decimal | None,
        target_avg_price: Decimal | None,
        executed_contract_qty: Decimal | None = None,
    ) -> Decimal | None:
        current = self._plan.current
        contract_qty = executed_contract_qty if executed_contract_qty is not None and executed_contract_qty > 0 else self._plan.qty
        if contract_qty <= 0:
            return None
        contract_value = current.contract_value
        contract_value_ccy = str(current.contract_value_ccy or "").strip().upper()
        leg_base_qtys: list[Decimal] = []
        if contract_value is not None and contract_value > 0:
            notional_value = contract_qty * contract_value
            if contract_value_ccy in {"USD", "USDT", "USDC"}:
                for price in (current_avg_price, target_avg_price):
                    if price is not None and price > 0:
                        leg_base_qtys.append(notional_value / price)
            else:
                leg_base_qtys.extend((notional_value, notional_value))
        if leg_base_qtys:
            return sum(leg_base_qtys, Decimal("0")) / Decimal(str(len(leg_base_qtys)))
        if current.notional_base is not None and current.available > 0:
            return current.notional_base * (contract_qty / current.available)
        return None

    @staticmethod
    def _resolve_executed_roll_contract_qty(result) -> Decimal | None:  # noqa: ANN001
        current_filled = getattr(result, "rolled_derivative_qty", None)
        target_filled = getattr(result, "target_derivative_filled_qty", None)
        candidates: list[Decimal] = []
        for value in (current_filled, target_filled):
            if isinstance(value, Decimal) and value > 0:
                candidates.append(value)
        if not candidates:
            return None
        if len(candidates) == 2:
            return min(candidates)
        return candidates[0]

    def _estimate_roll_fee_history_limit(self, *, executed_contract_qty: Decimal | None) -> int:
        qty = executed_contract_qty if executed_contract_qty is not None and executed_contract_qty > 0 else self._plan.qty
        qty_count = int(qty)
        if qty != Decimal(qty_count):
            qty_count += 1
        qty_count = max(qty_count, 1)
        chase_attempts = max(int(self._plan.chase_limit), 0) + 1
        estimated = qty_count * max(chase_attempts * 2, 4)
        return min(max(estimated, 200), 1200)

    @staticmethod
    def _format_fee_totals(
        fee_totals: dict[str, Decimal],
        fee_usdt_totals: dict[str, Decimal] | None = None,
    ) -> str:
        parts: list[str] = []
        usdt_approx_total = Decimal("0")
        has_non_stable_conversion = False
        for currency in sorted(fee_totals):
            fee_text = fmt_decimal(fee_totals[currency], 8)
            if "." in fee_text:
                fee_text = fee_text.rstrip("0").rstrip(".")
            detail = f"{fee_text} {currency}"
            approx_value: Decimal | None = None
            if currency in {"USDT", "USD", "USDC"}:
                approx_value = fee_totals[currency]
            elif fee_usdt_totals is not None:
                approx_value = fee_usdt_totals.get(currency)
                has_non_stable_conversion = has_non_stable_conversion or approx_value is not None
                if approx_value is not None:
                    detail += f" (\u2248{RollExecutionThread._format_usdt_amount(approx_value)} USDT)"
            if approx_value is not None:
                usdt_approx_total += approx_value
            parts.append(detail)
        if fee_usdt_totals and (has_non_stable_conversion or len(fee_totals) > 1):
            parts.append(f"\u6298\u5408USDT\u5408\u8ba1 \u2248{RollExecutionThread._format_usdt_amount(usdt_approx_total)}")
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

    def __init__(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        plan: ProfessionalOpenExecutionPlan,
        auto_pause_threshold: Decimal | None = None,
    ) -> None:
        super().__init__()
        self._runtime = runtime
        self._plan = plan
        self._stop_after_batch_requested = False
        self._auto_pause_threshold = auto_pause_threshold

    @property
    def supports_stop_after_batch(self) -> bool:
        return True

    @property
    def stop_after_batch_requested(self) -> bool:
        return self._stop_after_batch_requested

    def request_stop_after_batch(self) -> bool:
        already_requested = self._stop_after_batch_requested
        self._stop_after_batch_requested = True
        return not already_requested

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
        size_suffix = {
            "coin": "币",
            "usdt": "U",
            "contracts": "张",
        }.get(self._plan.size_unit, self._plan.size_unit)
        self.log.emit(
            f"提交双腿开仓：{self._plan.left_inst_id} <-> {self._plan.right_inst_id} "
            f"| 输入数量 {fmt_decimal(self._plan.size_value)} {size_suffix} | {self._plan.execution_label}"
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
        result = manager.open_now(
            request,
            runtime=self._runtime,
            should_stop_after_batch=lambda: self._stop_after_batch_requested,
            wait_before_next_batch=self._build_auto_wait_before_next_batch(client),
        )
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

    def _build_auto_wait_before_next_batch(self, client: OkxRestClient) -> Callable[[], bool] | None:
        threshold = self._auto_pause_threshold
        left_inst_id = str(self._plan.left_inst_id or "").strip().upper()
        right_inst_id = str(self._plan.right_inst_id or "").strip().upper()
        if threshold is None or not left_inst_id or not right_inst_id:
            return None
        return lambda: _wait_for_auto_spread_resume(
            client=client,
            environment=self._runtime.environment,
            left_inst_id=left_inst_id,
            right_inst_id=right_inst_id,
            threshold=threshold,
            should_stop=lambda: self._stop_after_batch_requested,
            logger=self.log.emit,
        )

    def _build_request(self) -> ArbitrageOpenRequest:
        base_ccy = self._plan.spot_inst_id.split("-")[0].strip().upper()
        return ArbitrageOpenRequest(
            base_ccy=base_ccy,
            spot_inst_id=self._plan.spot_inst_id,
            derivative_inst_id=self._plan.derivative_inst_id,
            size=self._plan.size_value,
            size_unit=self._plan.size_unit,  # type: ignore[arg-type]
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


class ProfessionalCloseExecutionThread(QThread):
    log = Signal(str)
    status = Signal(object)
    finished_with_result = Signal(object)

    def __init__(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        plan: ProfessionalCloseExecutionPlan,
        auto_pause_threshold: Decimal | None = None,
    ) -> None:
        super().__init__()
        self._runtime = runtime
        self._plan = plan
        self._stop_after_batch_requested = False
        self._auto_pause_threshold = auto_pause_threshold

    @property
    def supports_stop_after_batch(self) -> bool:
        return True

    @property
    def stop_after_batch_requested(self) -> bool:
        return self._stop_after_batch_requested

    def request_stop_after_batch(self) -> bool:
        already_requested = self._stop_after_batch_requested
        self._stop_after_batch_requested = True
        return not already_requested

    def _base_ccy(self) -> str:
        return str(self._plan.spot_inst_id or "").strip().upper().split("-", 1)[0]

    def _planned_batch_summary(self, client: OkxRestClient, qty_contracts: Decimal) -> str:
        try:
            batches = self._planned_batches(client, qty_contracts)
        except Exception:
            return ""
        if len(batches) <= 1:
            return "预计 1 批"
        batch_text = " + ".join(fmt_decimal(item) for item in batches[:4])
        if len(batches) > 4:
            batch_text += " + ..."
        return f"预计 {len(batches)} 批（{batch_text}）"

    def _planned_batches(self, client: OkxRestClient, qty_contracts: Decimal) -> list[Decimal]:
        instrument = client.get_instrument(self._plan.derivative_inst_id, prefer_cached=True)
        return list(
            _split_derivative_batches(
                qty_contracts,
                instrument=instrument,
                batch_count=self._plan.batch_count,
                batch_contract_qty=self._plan.batch_contract_qty,
            )
        )

    def _is_coin_settled_derivative(self, client: OkxRestClient) -> bool:
        base_ccy = self._base_ccy()
        instrument = client.get_instrument(self._plan.derivative_inst_id, prefer_cached=True)
        settle_ccy = str(instrument.settle_ccy or "").strip().upper()
        return bool(base_ccy) and settle_ccy == base_ccy

    def _load_available_base_qty(self, client: OkxRestClient, *, base_ccy: str) -> Decimal:
        overview = client.get_account_overview(
            self._runtime.credentials,
            environment=self._runtime.environment,
            prefer_cache=False,
        )
        for asset in getattr(overview, "details", ()):
            ccy = str(getattr(asset, "ccy", "") or "").strip().upper()
            if ccy != base_ccy:
                continue
            available = getattr(asset, "available_balance", None)
            equity = getattr(asset, "equity", None)
            closeable = available if available not in {None, Decimal("0")} else equity
            if closeable is None or closeable <= 0:
                return Decimal("0")
            return closeable
        return Decimal("0")

    @staticmethod
    def _load_reserved_spot_qty(*, base_ccy: str) -> Decimal:
        return sum(
            (
                entry.spot_qty
                for entry in load_open_ledger_entries()
                if str(entry.base_ccy or "").strip().upper() == base_ccy and entry.close_mode == "open"
            ),
            start=Decimal("0"),
        )

    @staticmethod
    def _compute_profit_spot_sell_qty(
        *,
        available_base_qty: Decimal,
        reserved_spot_qty: Decimal,
        lot_size: Decimal,
    ) -> Decimal:
        releasable_qty = max((available_base_qty or Decimal("0")) - (reserved_spot_qty or Decimal("0")), Decimal("0"))
        increment = lot_size if lot_size is not None and lot_size > 0 else Decimal("0.00000001")
        snapped_qty = snap_to_increment(releasable_qty, increment, "down")
        return max(snapped_qty, Decimal("0"))

    def _close_profit_spot_balance(self, client: OkxRestClient) -> Decimal:
        base_ccy = self._base_ccy()
        if not base_ccy:
            return Decimal("0")
        if not self._is_coin_settled_derivative(client):
            self.log.emit(
                f"利润币清算已跳过：{self._plan.derivative_inst_id} 不是 {base_ccy} 币结算合约，本次平仓不会额外卖出现货利润币。"
            )
            return Decimal("0")
        spot_instrument = client.get_instrument(self._plan.spot_inst_id, prefer_cached=True)
        available_base_qty = self._load_available_base_qty(client, base_ccy=base_ccy)
        reserved_spot_qty = self._load_reserved_spot_qty(base_ccy=base_ccy)
        sell_qty = self._compute_profit_spot_sell_qty(
            available_base_qty=available_base_qty,
            reserved_spot_qty=reserved_spot_qty,
            lot_size=spot_instrument.lot_size,
        )
        self.log.emit(
            f"利润币清算测算：当前可用 {base_ccy} {fmt_decimal(available_base_qty)} - "
            f"账本仍占用 {fmt_decimal(reserved_spot_qty)} = 可额外卖出 {fmt_decimal(sell_qty)} {base_ccy}"
        )
        if sell_qty <= 0:
            return Decimal("0")
        config = _build_strategy_config(self._plan.spot_inst_id, self._runtime)
        order_result = client.place_simple_order(
            self._runtime.credentials,
            config,
            inst_id=self._plan.spot_inst_id,
            side="sell",
            size=sell_qty,
            ord_type="market",
            pos_side=None,
            reduce_only=False,
        )
        if not order_result.ord_id:
            raise OkxApiError("利润币现货卖出下单成功，但未返回订单号，无法确认成交。")
        filled_qty, avg_price = _wait_order_fill(
            client,
            credentials=self._runtime.credentials,
            config=config,
            inst_id=self._plan.spot_inst_id,
            ord_id=order_result.ord_id,
            expected_size=sell_qty,
            logger=self.log.emit,
            label="利润币现货卖出",
        )
        if filled_qty <= 0:
            raise OkxApiError("利润币现货卖出未成交，请手动检查账户剩余币数量。")
        avg_text = f" @ {fmt_decimal(avg_price)}" if avg_price is not None and avg_price > 0 else ""
        self.log.emit(f"利润币现货卖出完成：{fmt_decimal(filled_qty)} {base_ccy}{avg_text}")
        return filled_qty

    def run(self) -> None:
        client = OkxRestClient()
        manager = ArbitrageManager(client, logger=self.log.emit)
        plan_entry_ids = tuple(entry_id for entry_id in self._plan.entry_ids if str(entry_id).strip())
        open_entries = {
            item.entry_id: item
            for item in load_open_ledger_entries()
            if item.entry_id in set(plan_entry_ids)
        }
        remaining_qty = self._plan.qty_contracts
        total_pnl = Decimal("0")
        total_derivative_qty = Decimal("0")
        profit_spot_sold_qty = Decimal("0")
        closed_ids: list[str] = []
        errors: list[str] = []
        total_entries = len(plan_entry_ids)

        self.status.emit(
            ExecutionStatus(
                phase="提交",
                current_inst_id=self._plan.derivative_inst_id,
                target_inst_id=self._plan.spot_inst_id,
                current_filled=Decimal("0"),
                target_filled=Decimal("0"),
                message="双腿平仓请求已构造，准备提交",
            )
        )
        self.log.emit(
            f"提交套利平仓：{self._plan.left_inst_id} <-> {self._plan.right_inst_id}"
            f" | 合约张数 {self._plan.qty_contracts} | {self._plan.execution_label}"
        )

        if plan_entry_ids:
            for index, entry_id in enumerate(plan_entry_ids, start=1):
                if self._stop_after_batch_requested:
                    self.log.emit("已收到停止请求：当前已完成批次后停止后续平仓。")
                    break
                if remaining_qty <= 0:
                    break
                entry = find_ledger_entry(entry_id) or open_entries.get(entry_id)
                if entry is None or entry.close_mode != "open":
                    errors.append(f"{entry_id}: 未找到可继续处理的 open 持仓")
                    continue
                close_qty = min(remaining_qty, entry.derivative_qty)
                if close_qty <= 0:
                    continue
                self.status.emit(
                    ExecutionStatus(
                        phase="执行中",
                        current_inst_id=self._plan.derivative_inst_id,
                        target_inst_id=self._plan.spot_inst_id,
                        current_filled=total_derivative_qty,
                        target_filled=total_derivative_qty,
                        message=(
                            f"正在处理第 {index}/{total_entries} 笔套利持仓，目标 {fmt_decimal(close_qty)} 张"
                            + (f" | {self._planned_batch_summary(client, close_qty)}" if close_qty > 0 else "")
                        ),
                    )
                )
                result = manager.close_now(
                    ArbitrageCloseRequest(
                        entry_id=entry_id,
                        max_slippage=self._plan.max_slippage,
                        use_limit_orders=self._plan.use_limit_orders,
                        spot_limit_price=self._plan.spot_limit_price,
                        derivative_limit_price=self._plan.derivative_limit_price,
                        close_derivative_qty=close_qty,
                        batch_count=self._plan.batch_count,
                        batch_contract_qty=self._plan.batch_contract_qty,
                        execution_mode=self._plan.execution_mode,
                        maker_wait_seconds=self._plan.maker_wait_seconds,
                        chase_limit=self._plan.chase_limit,
                    ),
                    runtime=self._runtime,
                    should_stop_after_batch=lambda: self._stop_after_batch_requested,
                    wait_before_next_batch=self._build_auto_wait_before_next_batch(client),
                )
                if not result.success:
                    errors.append(f"{entry.base_ccy}: {result.message}")
                    continue
                total_derivative_qty += close_qty
                remaining_qty = max(remaining_qty - close_qty, Decimal("0"))
                total_pnl += result.total_pnl or Decimal("0")
                closed_ids.extend(result.entry_ids or (entry_id,))
        else:
            planned_batches = self._planned_batches(client, self._plan.qty_contracts)
            remaining_position_qty = (
                self._plan.position_available_contracts
                if self._plan.position_available_contracts > 0
                else self._plan.qty_contracts
            )
            for index, batch_qty in enumerate(planned_batches, start=1):
                if self._stop_after_batch_requested:
                    self.log.emit("已收到停止请求：当前已完成批次后停止后续平仓。")
                    break
                if batch_qty <= 0 or remaining_position_qty <= 0:
                    continue
                effective_qty = min(batch_qty, remaining_position_qty)
                self.status.emit(
                    ExecutionStatus(
                        phase="执行中",
                        current_inst_id=self._plan.derivative_inst_id,
                        target_inst_id=self._plan.spot_inst_id,
                        current_filled=total_derivative_qty,
                        target_filled=total_derivative_qty,
                        message=f"正在处理第 {index}/{len(planned_batches)} 批实时持仓，目标 {fmt_decimal(effective_qty)} 张",
                    )
                )
                result = manager.close_now(
                    ArbitrageCloseRequest(
                        entry_id=None,
                        max_slippage=self._plan.max_slippage,
                        use_limit_orders=self._plan.use_limit_orders,
                        spot_limit_price=self._plan.spot_limit_price,
                        derivative_limit_price=self._plan.derivative_limit_price,
                        close_derivative_qty=effective_qty,
                        batch_count=1,
                        batch_contract_qty=None,
                        execution_mode=self._plan.execution_mode,
                        maker_wait_seconds=self._plan.maker_wait_seconds,
                        chase_limit=self._plan.chase_limit,
                        base_ccy=self._base_ccy(),
                        spot_inst_id=self._plan.spot_inst_id,
                        derivative_inst_id=self._plan.derivative_inst_id,
                        current_derivative_qty=remaining_position_qty,
                    ),
                    runtime=self._runtime,
                    should_stop_after_batch=lambda: self._stop_after_batch_requested,
                    wait_before_next_batch=None,
                )
                if not result.success:
                    errors.append(f"{self._base_ccy()}: {result.message}")
                    break
                total_derivative_qty += effective_qty
                remaining_qty = max(remaining_qty - effective_qty, Decimal("0"))
                remaining_position_qty = max(remaining_position_qty - effective_qty, Decimal("0"))
                total_pnl += result.total_pnl or Decimal("0")
                if index < len(planned_batches):
                    wait_callback = self._build_auto_wait_before_next_batch(client)
                    if callable(wait_callback) and not wait_callback():
                        break

        success = total_derivative_qty > 0
        profit_spot_error: str | None = None
        if success and self._plan.close_profit_spot and plan_entry_ids:
            try:
                self.status.emit(
                    ExecutionStatus(
                        phase="执行中",
                        current_inst_id=self._plan.derivative_inst_id,
                        target_inst_id=self._plan.spot_inst_id,
                        current_filled=total_derivative_qty,
                        target_filled=total_derivative_qty,
                        message="主平仓已完成，正在检查是否需要额外卖出币本位利润币",
                    )
                )
                profit_spot_sold_qty = self._close_profit_spot_balance(client)
            except Exception as exc:  # noqa: BLE001
                profit_spot_error = str(exc)
                self.log.emit(f"利润币清算失败：{exc}")
        if success:
            if plan_entry_ids:
                message = f"已平仓 {len(closed_ids)} 笔，合约 {fmt_decimal(total_derivative_qty)} 张"
            else:
                message = f"已按实时持仓平仓，合约 {fmt_decimal(total_derivative_qty)} 张"
            if total_pnl != 0:
                message += f"，合计盈亏约 {fmt_decimal(total_pnl)} USDT"
            if profit_spot_sold_qty > 0:
                message += f"，额外卖出利润币 {fmt_decimal(profit_spot_sold_qty)} {self._base_ccy()}"
            if remaining_qty > 0:
                message += f"，仍有 {fmt_decimal(remaining_qty)} 张未处理"
            if errors:
                message += f"，{len(errors)} 笔失败"
            if profit_spot_error:
                message += f"，利润币清算失败：{profit_spot_error}"
            if not plan_entry_ids:
                message += "。本次来源为实时持仓，未更新本地套利账本。"
        else:
            message = "；".join(errors) or "套利平仓失败。"

        result = SimpleNamespace(
            success=success,
            message=message,
            closed_count=len(closed_ids),
            entry_ids=tuple(closed_ids),
            total_pnl=total_pnl if success else None,
            executed_derivative_qty=total_derivative_qty,
            profit_spot_sold_qty=profit_spot_sold_qty,
        )
        self.status.emit(
            ExecutionStatus(
                phase="完成" if success else "失败",
                current_inst_id=self._plan.derivative_inst_id,
                target_inst_id=self._plan.spot_inst_id,
                current_filled=total_derivative_qty,
                target_filled=total_derivative_qty,
                message=message,
                success=success,
            )
        )
        self.finished_with_result.emit(result)

    def _build_auto_wait_before_next_batch(self, client: OkxRestClient) -> Callable[[], bool] | None:
        threshold = self._auto_pause_threshold
        left_inst_id = str(self._plan.derivative_inst_id or "").strip().upper()
        right_inst_id = str(self._plan.spot_inst_id or "").strip().upper()
        if threshold is None or not left_inst_id or not right_inst_id:
            return None
        return lambda: _wait_for_auto_spread_resume(
            client=client,
            environment=self._runtime.environment,
            left_inst_id=left_inst_id,
            right_inst_id=right_inst_id,
            threshold=threshold,
            should_stop=lambda: self._stop_after_batch_requested,
            logger=self.log.emit,
            trigger_when="lte",
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
