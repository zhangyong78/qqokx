from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from okx_quant.models import Credentials, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxApiError, OkxPosition, OkxRestClient, OkxTicker
from okx_quant.pricing import format_decimal, snap_to_increment


Logger = Callable[[str], None]
ChangeCallback = Callable[[], None]
TriggerPriceType = Literal["mark", "last"]
ClosePriceMode = Literal["fixed_price", "mark_with_slippage"]
ReplayStatus = Literal["not_triggered", "filled", "error"]
ReplayTriggerReason = Literal["take_profit", "stop_loss"] | None

PRICE_GUARD_OPEN_AVG_MULTIPLIER = Decimal("8")
PRICE_GUARD_OPEN_BUFFER_MULTIPLIER = Decimal("3")
PRICE_GUARD_MARKET_MULTIPLIER = Decimal("2")
PRICE_GUARD_SELL_INTRINSIC_FLOOR_RATIO = Decimal("0.8")
PRICE_GUARD_SELL_BID_FLOOR_RATIO = Decimal("0.5")


class ProtectionPriceGuardError(RuntimeError):
    """Raised when a computed protection order price looks unsafe."""


class ProtectionCloseRetryError(RuntimeError):
    """Raised when a close attempt should be retried instead of stopping the task."""


_MOJIBAKE_REPLACEMENTS = {
    "准备�?": "准备中",
    "运行�?": "运行中",
    "已完�?": "已完成",
    "已停�?": "已停止",
    "停止�?": "停止中",
    "监控�?": "监控中",
    "盘口=不可�?": "盘口=不可用",
    "未返回订单状�?": "未返回订单状态",
    "ֹӯ触发市场快照": "触发市场快照",
    "ֹӯ平仓报单": "平仓报单",
    "ֹӯ平仓成交": "平仓成交",
    "ֹӯ": "止盈",
    "损�": "损",
}


def _repair_mojibake_text(text: str) -> str:
    if not text:
        return text
    suspicious_markers = (
        "鍑",
        "杩",
        "宸",
        "鍋",
        "寮",
        "鐩",
        "鏈",
        "缃",
        "鎸",
        "姝",
        "鏂",
        "褰",
        "瑙",
        "閹",
        "瓒",
        "鎻",
        "浠",
        "鍚",
        "鐨",
        "鍓",
    )
    if not any(marker in text for marker in suspicious_markers):
        return text
    repaired = text
    try:
        repaired = text.encode("gbk", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        repaired = text
    for bad, good in _MOJIBAKE_REPLACEMENTS.items():
        repaired = repaired.replace(bad, good)
    return repaired


@dataclass(frozen=True)
class OptionProtectionConfig:
    option_inst_id: str
    trigger_inst_id: str
    trigger_price_type: TriggerPriceType
    direction: Literal["long", "short"]
    pos_side: Literal["long", "short"] | None
    take_profit_trigger: Decimal | None
    stop_loss_trigger: Decimal | None
    take_profit_order_mode: ClosePriceMode
    take_profit_order_price: Decimal | None
    take_profit_slippage: Decimal
    stop_loss_order_mode: ClosePriceMode
    stop_loss_order_price: Decimal | None
    stop_loss_slippage: Decimal
    poll_seconds: float = 3.0
    trigger_label: str = ""


@dataclass(frozen=True)
class ProtectionSessionSnapshot:
    session_id: str
    option_inst_id: str
    trigger_inst_id: str
    trigger_label: str
    trigger_price_type: TriggerPriceType
    direction: str
    pos_side: str | None
    take_profit_trigger: Decimal | None
    take_profit_order_mode: ClosePriceMode
    take_profit_order_price: Decimal | None
    take_profit_slippage: Decimal
    stop_loss_trigger: Decimal | None
    stop_loss_order_mode: ClosePriceMode
    stop_loss_order_price: Decimal | None
    stop_loss_slippage: Decimal
    poll_seconds: float
    status: str
    started_at: datetime
    last_message: str


@dataclass(frozen=True)
class ProtectionReplayPoint:
    ts: int
    trigger_price: Decimal
    option_mark_price: Decimal


@dataclass(frozen=True)
class ProtectionReplayEvent:
    ts: int
    event_type: Literal["trigger", "fill", "error"]
    message: str
    trigger_price: Decimal
    option_mark_price: Decimal
    order_price: Decimal | None = None
    close_side: Literal["buy", "sell"] | None = None
    filled_size: Decimal | None = None
    remaining_position: Decimal | None = None


@dataclass(frozen=True)
class ProtectionReplayResult:
    status: ReplayStatus
    initial_position: Decimal
    final_position: Decimal
    close_side: Literal["buy", "sell"]
    trigger_reason: ReplayTriggerReason
    summary: str
    trigger_index: int | None
    trigger_ts: int | None
    trigger_price: Decimal | None
    close_order_price: Decimal | None
    fill_price: Decimal | None
    events: list[ProtectionReplayEvent]


@dataclass
class _ProtectionWorker:
    session_id: str
    credentials: Credentials
    config: StrategyConfig
    protection: OptionProtectionConfig
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    status: str = "准备中"
    started_at: datetime = field(default_factory=datetime.now)
    last_message: str = ""
    pending_close_reason: Literal["止盈", "止损"] | None = None
    transient_error_count: int = 0
    last_transient_error_at: datetime | None = None
    last_transient_notify_at: datetime | None = None
    last_transient_status_at: datetime | None = None
    close_retry_count: int = 0
    last_close_retry_notify_key: str | None = None
    trigger_notification_sent: bool = False
    close_order_sequence: int = 0
    active_close_cl_ord_id: str | None = None
    active_close_ord_id: str | None = None
    active_close_submitted_at: datetime | None = None
    last_trigger_price: Decimal | None = None


class PositionProtectionManager:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        notifier: EmailNotifier | None = None,
        on_change: ChangeCallback | None = None,
        transient_alert_interval_seconds: float = 180.0,
        transient_status_interval_seconds: float = 15.0,
    ) -> None:
        self._client = client
        self._logger = logger
        self._notifier = notifier
        self._on_change = on_change
        self._lock = threading.Lock()
        self._counter = 0
        self._workers: dict[str, _ProtectionWorker] = {}
        self._transient_alert_interval_seconds = max(transient_alert_interval_seconds, 0.0)
        self._transient_status_interval_seconds = max(transient_status_interval_seconds, 0.0)

    def set_notifier(self, notifier: EmailNotifier | None) -> None:
        self._notifier = notifier

    def list_sessions(self) -> list[ProtectionSessionSnapshot]:
        with self._lock:
            workers = list(self._workers.values())
        workers.sort(key=lambda item: item.started_at, reverse=True)
        return [
            ProtectionSessionSnapshot(
                session_id=item.session_id,
                option_inst_id=item.protection.option_inst_id,
                trigger_inst_id=item.protection.trigger_inst_id,
                trigger_label=item.protection.trigger_label or item.protection.trigger_inst_id,
                trigger_price_type=item.protection.trigger_price_type,
                direction=item.protection.direction,
                pos_side=item.protection.pos_side,
                take_profit_trigger=item.protection.take_profit_trigger,
                take_profit_order_mode=item.protection.take_profit_order_mode,
                take_profit_order_price=item.protection.take_profit_order_price,
                take_profit_slippage=item.protection.take_profit_slippage,
                stop_loss_trigger=item.protection.stop_loss_trigger,
                stop_loss_order_mode=item.protection.stop_loss_order_mode,
                stop_loss_order_price=item.protection.stop_loss_order_price,
                stop_loss_slippage=item.protection.stop_loss_slippage,
                poll_seconds=item.protection.poll_seconds,
                status=item.status,
                started_at=item.started_at,
                last_message=item.last_message,
            )
            for item in workers
        ]

    def clear_finished(self) -> int:
        return self._cleanup_dead_workers()

    def start(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        protection: OptionProtectionConfig,
    ) -> str:
        with self._lock:
            for existing in self._workers.values():
                if existing.protection.option_inst_id == protection.option_inst_id and existing.status in {"运行中", "准备中"}:
                    raise RuntimeError(f"{protection.option_inst_id} 已经有一个运行中的保护任务。")

            self._counter += 1
            session_id = f"P{self._counter:02d}"
            worker = _ProtectionWorker(
                session_id=session_id,
                credentials=credentials,
                config=config,
                protection=protection,
            )
            worker.thread = threading.Thread(
                target=self._run_worker,
                args=(worker,),
                daemon=True,
                name=f"qqokx-protection-{session_id}",
            )
            self._workers[session_id] = worker

        self._set_status(worker, "准备中", "期权持仓保护任务已创建。")
        assert worker.thread is not None
        worker.thread.start()
        return session_id

    def stop(self, session_id: str) -> None:
        with self._lock:
            worker = self._workers.get(session_id)
        if worker is None:
            raise RuntimeError("未找到对应的保护任务。")
        worker.stop_event.set()
        self._set_status(worker, "停止中", "已请求停止保护任务。")

    def stop_all(self) -> None:
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            worker.stop_event.set()
            self._set_status(worker, "停止中", "主程序关闭，正在停止保护任务。")

    def _run_worker(self, worker: _ProtectionWorker) -> None:
        protection = worker.protection
        self._set_status(
            worker,
            "运行中",
            (
                f"开始监控 {protection.option_inst_id} | 触发源={protection.trigger_label or protection.trigger_inst_id} | "
                f"止盈={_fmt_optional(protection.take_profit_trigger)} | 止损={_fmt_optional(protection.stop_loss_trigger)}"
            ),
        )
        while not worker.stop_event.is_set():
            try:
                if worker.pending_close_reason is not None:
                    self._close_position(worker, worker.pending_close_reason)
                    self._reset_transient_state(worker)
                    self._set_status(worker, "已完成", f"{worker.pending_close_reason}已完成平仓流程。")
                    return

                current_price = self._client.get_trigger_price(
                    protection.trigger_inst_id,
                    protection.trigger_price_type,
                )
                worker.last_trigger_price = current_price
                stop_hit, take_hit = evaluate_protection_trigger(
                    direction=protection.direction,
                    current_price=current_price,
                    stop_loss=protection.stop_loss_trigger,
                    take_profit=protection.take_profit_trigger,
                    option_inst_id=protection.option_inst_id,
                    uses_underlying_trigger=uses_underlying_price_trigger(protection),
                )
                self._reset_transient_state(worker)
                self._set_status(
                    worker,
                    "运行中",
                    f"监控中 | 触发价={format_decimal(current_price)} | 触发源={protection.trigger_label or protection.trigger_inst_id}",
                )
                if stop_hit or take_hit:
                    reason = "止损" if stop_hit else "止盈"
                    worker.pending_close_reason = reason
                    trigger_ticker = self._safe_get_ticker(protection.option_inst_id)
                    trigger_market_summary = self._format_ticker_snapshot(trigger_ticker)
                    self._logger(
                        f"[鎸佷粨淇濇姢 {worker.session_id}] {reason}瑙﹀彂甯傚満蹇収 | {protection.option_inst_id} | "
                        f"{trigger_market_summary}"
                    )
                    if not worker.trigger_notification_sent:
                        self._logger(
                            f"[持仓保护 {worker.session_id}] {reason}触发 | {protection.option_inst_id} | "
                            f"触发源={protection.trigger_label or protection.trigger_inst_id} | 当前价={format_decimal(current_price)}"
                        )
                        self._notify(
                            subject=f"[QQOKX] 持仓保护触发 | {reason} | {protection.option_inst_id}",
                            body="\n".join(
                                [
                                    f"任务：{worker.session_id}",
                                    f"期权合约：{protection.option_inst_id}",
                                    f"方向：{protection.direction}",
                                    f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                                    f"当前触发价：{format_decimal(current_price)}",
                                    f"止盈触发：{_fmt_optional(protection.take_profit_trigger)}",
                                    f"止损触发：{_fmt_optional(protection.stop_loss_trigger)}",
                                    f"触发原因：{reason}",
                                ]
                            ),
                        )
                        worker.trigger_notification_sent = True
                    self._set_status(
                        worker,
                        "运行中",
                        f"{reason}已触发，正在尝试平仓 | 触发价={format_decimal(current_price)}",
                    )
                    continue
                worker.stop_event.wait(protection.poll_seconds)
            except Exception as exc:
                if isinstance(exc, ProtectionPriceGuardError):
                    self._set_status(worker, "异常", f"价格保护拦截：{exc}")
                    self._notify(
                        subject=f"[QQOKX] 持仓保护价格拦截 | {protection.option_inst_id}",
                        body="\n".join(
                            [
                                f"任务：{worker.session_id}",
                                f"期权合约：{protection.option_inst_id}",
                                f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                                f"当前阶段：{worker.pending_close_reason or '监控触发价'}",
                                f"拦截原因：{exc}",
                                "处理结果：本次自动报单已停止，请人工确认后处理。",
                            ]
                        ),
                    )
                    return
                if self._is_transient_error(exc):
                    self._handle_transient_error(worker, exc)
                    worker.stop_event.wait(max(protection.poll_seconds, 0.05))
                    continue
                self._set_status(worker, "异常", f"保护任务异常：{exc}")
                self._notify(
                    subject=f"[QQOKX] 持仓保护异常 | {protection.option_inst_id}",
                    body="\n".join(
                        [
                            f"任务：{worker.session_id}",
                            f"期权合约：{protection.option_inst_id}",
                            f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                            f"异常：{exc}",
                        ]
                    ),
                )
                return

        self._set_status(worker, "已停止", "保护任务已停止。")

    def _close_position(self, worker: _ProtectionWorker, reason: str) -> None:
        trade_instrument = self._client.get_instrument(worker.protection.option_inst_id)
        remaining_position = self._find_matching_position(worker)
        if remaining_position is None:
            opposite = self._find_any_position(worker)
            if opposite is not None:
                raise RuntimeError(
                    f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
                )
            self._clear_active_close_order(worker)
            self._logger(f"[持仓保护 {worker.session_id}] {worker.protection.option_inst_id} 当前已经没有持仓。")
            return

        remaining = abs(remaining_position.position)
        close_side = derive_close_side(worker.protection.direction)
        pos_side = worker.protection.pos_side

        for _ in range(3):
            if remaining <= 0:
                break

            if worker.active_close_cl_ord_id is not None:
                state = self._reconcile_active_close_order(
                    worker,
                    reason=reason,
                    close_side=close_side,
                    expected_remaining=remaining,
                )
                if state == "waiting":
                    raise TimeoutError("等待平仓单状态确认中。")
                latest = self._find_matching_position(worker)
                if latest is None:
                    opposite = self._find_any_position(worker)
                    if opposite is not None:
                        raise RuntimeError(
                            f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
                        )
                    remaining = Decimal("0")
                    break
                remaining = abs(latest.position)
                if state == "progressed":
                    continue

            size = snap_to_increment(remaining, trade_instrument.lot_size, "down")
            if size < trade_instrument.min_size:
                raise RuntimeError(
                    f"剩余仓位 {format_decimal(remaining)} 小于最小下单量 {format_decimal(trade_instrument.min_size)}"
                )

            cl_ord_id = worker.active_close_cl_ord_id or self._next_close_cl_ord_id(worker)
            order_price = build_close_order_price(
                client=self._client,
                option_inst_id=worker.protection.option_inst_id,
                close_side=close_side,
                tick_size=trade_instrument.tick_size,
                mode=worker.protection.stop_loss_order_mode if reason == "止损" else worker.protection.take_profit_order_mode,
                fixed_price=worker.protection.stop_loss_order_price if reason == "止损" else worker.protection.take_profit_order_price,
                slippage=worker.protection.stop_loss_slippage if reason == "止损" else worker.protection.take_profit_slippage,
            )
            spot_price, option_ticker = validate_live_protection_order_price_guard(
                client=self._client,
                option_inst_id=worker.protection.option_inst_id,
                close_side=close_side,
                order_price=order_price,
                tick_size=trade_instrument.tick_size,
                open_avg_price=remaining_position.avg_price,
            )
            self._logger(
                f"[鎸佷粨淇濇姢 {worker.session_id}] {reason}骞充粨鎶ュ崟 | {worker.protection.option_inst_id} | "
                f"鏂瑰悜={close_side.upper()} | 委托价={format_decimal(order_price)} | 委托量={format_decimal(size)} | "
                f"clOrdId={cl_ord_id} | 触发价={_fmt_optional(worker.last_trigger_price)} | "
                f"现货={format_decimal(spot_price)} | 开仓均价={_fmt_optional(remaining_position.avg_price)} | "
                f"{self._format_ticker_snapshot(option_ticker)}"
            )
            worker.active_close_cl_ord_id = cl_ord_id
            worker.active_close_submitted_at = datetime.now()
            try:
                result = self._client.place_simple_order(
                    worker.credentials,
                    worker.config,
                    inst_id=worker.protection.option_inst_id,
                    side=close_side,
                    size=size,
                    ord_type="ioc",
                    pos_side=pos_side,
                    price=order_price,
                    cl_ord_id=cl_ord_id,
                )
                worker.active_close_ord_id = result.ord_id or None
                worker.active_close_cl_ord_id = result.cl_ord_id or cl_ord_id
                filled_size, filled_price = wait_order_fill(
                    client=self._client,
                    credentials=worker.credentials,
                    config=worker.config,
                    inst_id=worker.protection.option_inst_id,
                    ord_id=result.ord_id,
                    estimated_price=order_price,
                    wait_seconds=max(worker.protection.poll_seconds / 2, 0.5),
                    stop_event=worker.stop_event,
                )
            except Exception:
                raise

            latest = self._find_matching_position(worker)
            remaining = Decimal("0") if latest is None else abs(latest.position)
            opposite = self._find_any_position(worker)
            if latest is None and opposite is not None:
                raise RuntimeError(
                    f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
                )
            self._record_close_fill(
                worker,
                reason=reason,
                close_side=close_side,
                filled_size=filled_size,
                filled_price=filled_price,
                remaining=remaining,
                order_price=order_price,
                ticker=option_ticker,
            )
            self._clear_active_close_order(worker)
            latest = self._find_matching_position(worker)
            if latest is None:
                remaining = Decimal("0")
                break
            remaining = abs(latest.position)

        if remaining > 0:
            raise RuntimeError(f"{worker.protection.option_inst_id} 保护平仓后仍有剩余仓位 {format_decimal(remaining)}")

    def _find_matching_position(self, worker: _ProtectionWorker) -> OkxPosition | None:
        positions = self._client.get_positions(worker.credentials, environment=worker.config.environment)
        matches = [
            item
            for item in positions
            if item.inst_id == worker.protection.option_inst_id and derive_position_direction(item) == worker.protection.direction
        ]
        if worker.protection.pos_side:
            side_matches = [item for item in matches if item.pos_side == worker.protection.pos_side]
            if side_matches:
                matches = side_matches
        if not matches:
            return None
        matches.sort(key=lambda item: abs(item.position), reverse=True)
        return matches[0]

    def _find_any_position(self, worker: _ProtectionWorker) -> OkxPosition | None:
        positions = self._client.get_positions(worker.credentials, environment=worker.config.environment)
        matches = [item for item in positions if item.inst_id == worker.protection.option_inst_id]
        if worker.protection.pos_side:
            side_matches = [item for item in matches if item.pos_side == worker.protection.pos_side]
            if side_matches:
                matches = side_matches
        if not matches:
            return None
        matches.sort(key=lambda item: abs(item.position), reverse=True)
        return matches[0]

    def _next_close_cl_ord_id(self, worker: _ProtectionWorker) -> str:
        worker.close_order_sequence += 1
        return f"pp{worker.session_id.lower()}{worker.close_order_sequence:04d}"

    def _clear_active_close_order(self, worker: _ProtectionWorker) -> None:
        worker.active_close_cl_ord_id = None
        worker.active_close_ord_id = None
        worker.active_close_submitted_at = None

    def _reconcile_active_close_order(
        self,
        worker: _ProtectionWorker,
        *,
        reason: str,
        close_side: Literal["buy", "sell"],
        expected_remaining: Decimal,
    ) -> Literal["waiting", "progressed", "retry"]:
        if worker.active_close_cl_ord_id is None:
            return "retry"
        try:
            status = self._client.get_order(
                worker.credentials,
                worker.config,
                inst_id=worker.protection.option_inst_id,
                ord_id=worker.active_close_ord_id,
                cl_ord_id=worker.active_close_cl_ord_id,
            )
        except OkxApiError as exc:
            if not self._is_missing_order_error(exc):
                raise
            if worker.active_close_submitted_at is not None:
                elapsed = (datetime.now() - worker.active_close_submitted_at).total_seconds()
                if elapsed < max(worker.protection.poll_seconds * 2, 5.0):
                    return "waiting"
            self._clear_active_close_order(worker)
            return "retry"

        state = (status.state or "").lower()
        filled_size = status.filled_size or Decimal("0")
        if filled_size > 0:
            latest = self._find_matching_position(worker)
            remaining = Decimal("0") if latest is None else abs(latest.position)
            self._record_close_fill(
                worker,
                reason=reason,
                close_side=close_side,
                filled_size=filled_size,
                filled_price=status.avg_price or status.price or Decimal("0"),
                remaining=remaining,
                order_price=status.price,
                ticker=self._safe_get_ticker(worker.protection.option_inst_id),
            )

        if state in {"filled", "partially_filled", "canceled", "mmp_canceled"}:
            self._clear_active_close_order(worker)
            return "progressed"
        return "waiting"

    def _record_close_fill(
        self,
        worker: _ProtectionWorker,
        *,
        reason: str,
        close_side: Literal["buy", "sell"],
        filled_size: Decimal,
        filled_price: Decimal,
        remaining: Decimal,
        order_price: Decimal | None = None,
        ticker: OkxTicker | None = None,
    ) -> None:
        self._logger(
            f"[持仓保护 {worker.session_id}] {reason}平仓成交 | {worker.protection.option_inst_id} | "
            f"方向={close_side.upper()} | 成交价={format_decimal(filled_price)} | 成交量={format_decimal(filled_size)} | "
            f"剩余={format_decimal(max(remaining, Decimal('0')))}"
        )
        self._logger(
            f"[鎸佷粨淇濇姢 {worker.session_id}] {reason}骞充粨鎴愪氦蹇収 | {worker.protection.option_inst_id} | "
            f"委托价={_fmt_optional(order_price)} | {self._format_ticker_snapshot(ticker)}"
        )
        self._notify(
            subject=f"[QQOKX] 持仓保护成交 | {reason} | {worker.protection.option_inst_id}",
            body="\n".join(
                [
                    f"任务：{worker.session_id}",
                    f"期权合约：{worker.protection.option_inst_id}",
                    f"触发原因：{reason}",
                    f"平仓方向：{close_side}",
                    f"成交数量：{format_decimal(filled_size)}",
                    f"成交价格：{format_decimal(filled_price)}",
                    f"剩余仓位：{format_decimal(max(remaining, Decimal('0')))}",
                ]
            ),
        )

    def _is_missing_order_error(self, exc: Exception) -> bool:
        message = str(exc)
        return "未返回订单状态" in message or "order does not exist" in message.lower()

    def _set_status(self, worker: _ProtectionWorker, status: str, message: str) -> None:
        with self._lock:
            worker.status = status
            worker.last_message = message
        self._logger(f"[持仓保护 {worker.session_id}] {message}")
        self._emit_change()

    def _reset_transient_state(self, worker: _ProtectionWorker) -> None:
        worker.transient_error_count = 0
        worker.last_transient_error_at = None
        worker.last_transient_notify_at = None
        worker.last_transient_status_at = None
        worker.last_close_retry_notify_key = None

    def _is_transient_error(self, exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        if isinstance(exc, OkxApiError) and exc.status is not None and exc.status >= 500:
            return True

        message = str(exc).lower()
        transient_markers = (
            "网络错误",
            "timeout",
            "timed out",
            "handshake",
            "ssl",
            "connection reset",
            "connection aborted",
            "connection refused",
            "temporarily unavailable",
            "temporary failure",
            "remote disconnected",
            "read timed out",
            "connect timeout",
            "proxy error",
            "bad gateway",
            "service unavailable",
            "gateway timeout",
            "握手",
            "超时",
        )
        return any(marker in message for marker in transient_markers)

    def _handle_transient_error(self, worker: _ProtectionWorker, exc: Exception) -> None:
        now = datetime.now()
        worker.transient_error_count += 1
        worker.last_transient_error_at = now
        message = (
            f"网络异常，正在重试（第{worker.transient_error_count}次） | "
            f"{worker.protection.option_inst_id} | {exc}"
        )

        if (
            worker.last_transient_status_at is None
            or (now - worker.last_transient_status_at).total_seconds() >= self._transient_status_interval_seconds
        ):
            self._set_status(worker, "运行中", message)
            worker.last_transient_status_at = now

        if (
            worker.last_transient_notify_at is None
            or (now - worker.last_transient_notify_at).total_seconds() >= self._transient_alert_interval_seconds
        ):
            self._notify(
                subject=f"[QQOKX] 持仓保护网络重试 | {worker.protection.option_inst_id}",
                body="\n".join(
                    [
                        f"任务：{worker.session_id}",
                        f"期权合约：{worker.protection.option_inst_id}",
                        f"触发源：{worker.protection.trigger_label or worker.protection.trigger_inst_id}",
                        "当前状态：网络异常，正在重试，任务不会自动停止",
                        f"连续异常次数：{worker.transient_error_count}",
                        f"最新异常：{exc}",
                        (
                            f"当前平仓阶段：{worker.pending_close_reason}"
                            if worker.pending_close_reason is not None
                            else "当前阶段：监控触发价"
                        ),
                    ]
                ),
            )
            worker.last_transient_notify_at = now

    def _cleanup_dead_workers(self) -> int:
        with self._lock:
            finished_ids = [key for key, worker in self._workers.items() if worker.thread is not None and not worker.thread.is_alive()]
            for key in finished_ids:
                del self._workers[key]
        return len(finished_ids)

    def _emit_change(self) -> None:
        if self._on_change is not None:
            self._on_change()

    def _notify(self, *, subject: str, body: str) -> None:
        if self._notifier is not None and self._notifier.enabled:
            self._notifier.notify_async(subject, body)

    def _safe_get_ticker(self, inst_id: str) -> OkxTicker | None:
        try:
            return self._client.get_ticker(inst_id)
        except Exception:
            return None

    def _format_ticker_snapshot(self, ticker: OkxTicker | None) -> str:
        if ticker is None:
            return "盘口=不可用"
        return (
            "盘口 "
            f"bid/ask/last/mark={_fmt_optional(ticker.bid)}/{_fmt_optional(ticker.ask)}/"
            f"{_fmt_optional(ticker.last)}/{_fmt_optional(ticker.mark)}"
        )


def derive_position_direction(position: OkxPosition) -> Literal["long", "short"]:
    if position.pos_side and position.pos_side.lower() != "net":
        return "long" if position.pos_side.lower() == "long" else "short"
    return "long" if position.position >= 0 else "short"


def derive_close_side(direction: Literal["long", "short"]) -> Literal["buy", "sell"]:
    return "sell" if direction == "long" else "buy"


def infer_default_spot_inst_id(option_inst_id: str) -> str:
    normalized = option_inst_id.strip().upper()
    base = normalized.split("-")[0] if normalized else ""
    return f"{base}-USDT" if base else ""


def normalize_spot_inst_id(raw: str) -> str:
    cleaned = raw.strip().upper()
    if not cleaned:
        return ""
    if "-" in cleaned:
        return cleaned
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}-USDT"
    return cleaned


def infer_option_style(option_inst_id: str) -> Literal["call", "put"] | None:
    normalized = option_inst_id.strip().upper()
    if normalized.endswith("-C"):
        return "call"
    if normalized.endswith("-P"):
        return "put"
    return None


def infer_option_strike(option_inst_id: str) -> Decimal | None:
    parts = option_inst_id.strip().upper().split("-")
    if len(parts) < 2 or parts[-1] not in {"C", "P"}:
        return None
    try:
        return Decimal(parts[-2])
    except Exception:
        return None


def _positive_quote_candidates(*values: Decimal | None) -> list[Decimal]:
    return [value for value in values if value is not None and value > 0]


def compute_option_intrinsic_price(
    *,
    option_inst_id: str,
    spot_price: Decimal,
) -> Decimal:
    strike = infer_option_strike(option_inst_id)
    option_style = infer_option_style(option_inst_id)
    if strike is None or option_style is None or spot_price <= 0:
        return Decimal("0")
    if option_style == "call":
        return max(spot_price - strike, Decimal("0")) / spot_price
    return max(strike - spot_price, Decimal("0")) / spot_price


def compute_option_hard_price_cap(
    *,
    option_inst_id: str,
    spot_price: Decimal,
) -> Decimal | None:
    strike = infer_option_strike(option_inst_id)
    option_style = infer_option_style(option_inst_id)
    if strike is None or option_style is None or spot_price <= 0:
        return None
    if option_style == "call":
        return Decimal("1")
    return strike / spot_price


def validate_protection_order_price_guard(
    *,
    option_inst_id: str,
    close_side: Literal["buy", "sell"],
    order_price: Decimal,
    tick_size: Decimal,
    open_avg_price: Decimal | None,
    spot_price: Decimal,
    option_bid: Decimal | None,
    option_ask: Decimal | None,
    option_last: Decimal | None,
) -> None:
    if order_price <= 0:
        raise ProtectionPriceGuardError(f"{option_inst_id} 的报单价格 {format_decimal(order_price)} 无效。")
    if spot_price <= 0:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法取得有效现货价格，不能完成价格保护校验。")

    intrinsic_price = compute_option_intrinsic_price(option_inst_id=option_inst_id, spot_price=spot_price)
    hard_cap = compute_option_hard_price_cap(option_inst_id=option_inst_id, spot_price=spot_price)
    option_quotes = _positive_quote_candidates(option_bid, option_ask, option_last)
    market_anchor = max(option_quotes) * PRICE_GUARD_MARKET_MULTIPLIER if option_quotes else Decimal("0")
    avg_price = open_avg_price if open_avg_price is not None and open_avg_price > 0 else Decimal("0")

    strike = infer_option_strike(option_inst_id)
    if strike is None:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法解析行权价，不能完成价格保护校验。")

    moneyness_distance = abs(spot_price - strike) / spot_price
    delta_one_anchor = intrinsic_price + moneyness_distance + (avg_price * PRICE_GUARD_OPEN_BUFFER_MULTIPLIER)
    open_anchor = avg_price * PRICE_GUARD_OPEN_AVG_MULTIPLIER if avg_price > 0 else Decimal("0")

    if close_side == "buy":
        upper_candidates = [tick_size, delta_one_anchor, open_anchor, market_anchor]
        upper_bound = max(upper_candidates)
        if hard_cap is not None and hard_cap > 0:
            upper_bound = min(upper_bound, hard_cap)
        upper_bound = max(upper_bound, tick_size)
        if order_price > upper_bound:
            raise ProtectionPriceGuardError(
                (
                    f"{option_inst_id} 价格保护已拦截：买回报单价 {format_decimal(order_price)} 高于安全上限 "
                    f"{format_decimal(upper_bound)} | 现货价={format_decimal(spot_price)} | 行权价={format_decimal(strike)} | "
                    f"开仓均价={_fmt_optional(open_avg_price)} | bid/ask/last={_fmt_optional(option_bid)}/"
                    f"{_fmt_optional(option_ask)}/{_fmt_optional(option_last)}"
                )
            )
        return

    lower_candidates = [tick_size]
    if intrinsic_price > 0:
        lower_candidates.append(intrinsic_price * PRICE_GUARD_SELL_INTRINSIC_FLOOR_RATIO)
    if option_bid is not None and option_bid > 0:
        lower_candidates.append(option_bid * PRICE_GUARD_SELL_BID_FLOOR_RATIO)
    lower_bound = max(lower_candidates)
    if order_price < lower_bound:
        raise ProtectionPriceGuardError(
            (
                f"{option_inst_id} 价格保护已拦截：卖出报单价 {format_decimal(order_price)} 低于安全下限 "
                f"{format_decimal(lower_bound)} | 现货价={format_decimal(spot_price)} | 行权价={format_decimal(strike)} | "
                f"开仓均价={_fmt_optional(open_avg_price)} | bid/ask/last={_fmt_optional(option_bid)}/"
                f"{_fmt_optional(option_ask)}/{_fmt_optional(option_last)}"
            )
        )


def uses_underlying_price_trigger(protection: OptionProtectionConfig) -> bool:
    return not (
        protection.trigger_inst_id.strip().upper() == protection.option_inst_id.strip().upper()
        and protection.trigger_price_type == "mark"
    )


def infer_protection_profit_on_rise(
    *,
    option_inst_id: str,
    direction: Literal["long", "short"],
    trigger_inst_id: str,
    trigger_price_type: TriggerPriceType,
) -> bool:
    uses_underlying_trigger = not (
        option_inst_id.strip().upper() == trigger_inst_id.strip().upper() and trigger_price_type == "mark"
    )
    return is_profit_when_trigger_price_rises(
        option_inst_id=option_inst_id,
        direction=direction,
        uses_underlying_trigger=uses_underlying_trigger,
    )


def describe_protection_price_logic(
    *,
    option_inst_id: str,
    direction: Literal["long", "short"],
    trigger_inst_id: str,
    trigger_price_type: TriggerPriceType,
) -> str:
    uses_underlying_trigger = not (
        option_inst_id.strip().upper() == trigger_inst_id.strip().upper() and trigger_price_type == "mark"
    )
    option_style = infer_option_style(option_inst_id)
    direction_label = "买入" if direction == "long" else "卖出"
    style_label = "认购" if option_style == "call" else "认沽" if option_style == "put" else "期权"
    trigger_label = "现货触发" if uses_underlying_trigger else "期权标记价格触发"
    profit_on_rise = is_profit_when_trigger_price_rises(
        option_inst_id=option_inst_id,
        direction=direction,
        uses_underlying_trigger=uses_underlying_trigger,
    )
    if profit_on_rise:
        return (
            f"{direction_label}{style_label} + {trigger_label}：价格上涨偏向止盈，价格下跌偏向止损，"
            f"所以止盈触发价应大于止损触发价。"
        )
    return (
        f"{direction_label}{style_label} + {trigger_label}：价格下跌偏向止盈，价格上涨偏向止损，"
        f"所以止盈触发价应小于止损触发价。"
    )


def is_profit_when_trigger_price_rises(
    *,
    option_inst_id: str,
    direction: Literal["long", "short"],
    uses_underlying_trigger: bool,
) -> bool:
    if not uses_underlying_trigger:
        return direction == "long"

    option_style = infer_option_style(option_inst_id)
    if option_style == "call":
        return direction == "long"
    if option_style == "put":
        return direction == "short"
    return direction == "long"


def evaluate_protection_trigger(
    *,
    direction: Literal["long", "short"],
    current_price: Decimal,
    stop_loss: Decimal | None,
    take_profit: Decimal | None,
    option_inst_id: str | None = None,
    uses_underlying_trigger: bool = False,
) -> tuple[bool, bool]:
    stop_hit = False
    take_hit = False

    profit_on_rise = is_profit_when_trigger_price_rises(
        option_inst_id=option_inst_id or "",
        direction=direction,
        uses_underlying_trigger=uses_underlying_trigger,
    )
    if profit_on_rise:
        if stop_loss is not None and current_price <= stop_loss:
            stop_hit = True
        if take_profit is not None and current_price >= take_profit:
            take_hit = True
    else:
        if stop_loss is not None and current_price >= stop_loss:
            stop_hit = True
        if take_profit is not None and current_price <= take_profit:
            take_hit = True
    return stop_hit, take_hit


def build_close_order_price(
    *,
    client: OkxRestClient,
    option_inst_id: str,
    close_side: Literal["buy", "sell"],
    tick_size: Decimal,
    mode: ClosePriceMode,
    fixed_price: Decimal | None,
    slippage: Decimal,
) -> Decimal:
    mark_price = client.get_trigger_price(option_inst_id, "mark")
    return build_close_order_price_from_mark(
        mark_price=mark_price,
        close_side=close_side,
        tick_size=tick_size,
        mode=mode,
        fixed_price=fixed_price,
        slippage=slippage,
    )


def build_close_order_price_from_mark(
    *,
    mark_price: Decimal,
    close_side: Literal["buy", "sell"],
    tick_size: Decimal,
    mode: ClosePriceMode,
    fixed_price: Decimal | None,
    slippage: Decimal,
) -> Decimal:
    if mode == "fixed_price":
        if fixed_price is None or fixed_price <= 0:
            raise RuntimeError("固定报单价格必须大于 0。")
        return snap_to_increment(fixed_price, tick_size, "up" if close_side == "buy" else "down")

    if close_side == "buy":
        raw_price = mark_price + slippage
        return snap_to_increment(raw_price, tick_size, "up")
    raw_price = mark_price - slippage
    if raw_price <= 0:
        raw_price = tick_size
    return snap_to_increment(raw_price, tick_size, "down")


def validate_live_protection_order_price_guard(
    *,
    client: OkxRestClient,
    option_inst_id: str,
    close_side: Literal["buy", "sell"],
    order_price: Decimal,
    tick_size: Decimal,
    open_avg_price: Decimal | None,
    spot_price: Decimal | None = None,
    ticker: OkxTicker | None = None,
) -> tuple[Decimal, OkxTicker]:
    spot_inst_id = infer_default_spot_inst_id(option_inst_id)
    if not spot_inst_id:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法推导现货标的，不能完成价格保护校验。")
    resolved_spot_price = spot_price if spot_price is not None else client.get_trigger_price(spot_inst_id, "last")
    resolved_ticker = ticker if ticker is not None else client.get_ticker(option_inst_id)
    validate_protection_order_price_guard(
        option_inst_id=option_inst_id,
        close_side=close_side,
        order_price=order_price,
        tick_size=tick_size,
        open_avg_price=open_avg_price,
        spot_price=resolved_spot_price,
        option_bid=resolved_ticker.bid,
        option_ask=resolved_ticker.ask,
        option_last=resolved_ticker.last,
    )
    return resolved_spot_price, resolved_ticker


def replay_option_protection(
    *,
    protection: OptionProtectionConfig,
    initial_position: Decimal,
    tick_size: Decimal,
    lot_size: Decimal,
    min_size: Decimal,
    points: list[ProtectionReplayPoint],
) -> ProtectionReplayResult:
    normalized_position = abs(initial_position)
    close_side = derive_close_side(protection.direction)
    events: list[ProtectionReplayEvent] = []
    if normalized_position <= 0:
        return ProtectionReplayResult(
            status="error",
            initial_position=normalized_position,
            final_position=normalized_position,
            close_side=close_side,
            trigger_reason=None,
            summary="初始持仓数量必须大于 0。",
            trigger_index=None,
            trigger_ts=None,
            trigger_price=None,
            close_order_price=None,
            fill_price=None,
            events=[],
        )

    for index, point in enumerate(points):
        stop_hit, take_hit = evaluate_protection_trigger(
            direction=protection.direction,
            current_price=point.trigger_price,
            stop_loss=protection.stop_loss_trigger,
            take_profit=protection.take_profit_trigger,
            option_inst_id=protection.option_inst_id,
            uses_underlying_trigger=uses_underlying_price_trigger(protection),
        )
        if not (stop_hit or take_hit):
            continue

        trigger_reason: ReplayTriggerReason = "stop_loss" if stop_hit else "take_profit"
        reason_label = "止损" if trigger_reason == "stop_loss" else "止盈"
        events.append(
            ProtectionReplayEvent(
                ts=point.ts,
                event_type="trigger",
                message=f"{reason_label}触发",
                trigger_price=point.trigger_price,
                option_mark_price=point.option_mark_price,
                remaining_position=normalized_position,
            )
        )

        remaining = normalized_position
        latest_fill_price: Decimal | None = None
        latest_order_price: Decimal | None = None
        try:
            for _ in range(3):
                if remaining <= 0:
                    break
                size = snap_to_increment(remaining, lot_size, "down")
                if size < min_size:
                    raise RuntimeError(
                        f"剩余仓位 {format_decimal(remaining)} 小于最小下单量 {format_decimal(min_size)}"
                    )
                latest_order_price = build_close_order_price_from_mark(
                    mark_price=point.option_mark_price,
                    close_side=close_side,
                    tick_size=tick_size,
                    mode=protection.stop_loss_order_mode if trigger_reason == "stop_loss" else protection.take_profit_order_mode,
                    fixed_price=protection.stop_loss_order_price if trigger_reason == "stop_loss" else protection.take_profit_order_price,
                    slippage=protection.stop_loss_slippage if trigger_reason == "stop_loss" else protection.take_profit_slippage,
                )
                latest_fill_price = latest_order_price
                remaining -= size
                if remaining < 0:
                    remaining = Decimal("0")
                events.append(
                    ProtectionReplayEvent(
                        ts=point.ts,
                        event_type="fill",
                        message=f"{reason_label}平仓成交",
                        trigger_price=point.trigger_price,
                        option_mark_price=point.option_mark_price,
                        order_price=latest_order_price,
                        close_side=close_side,
                        filled_size=size,
                        remaining_position=remaining,
                    )
                )
            if remaining > 0:
                raise RuntimeError(f"回放结束后仍有剩余仓位 {format_decimal(remaining)}")
            return ProtectionReplayResult(
                status="filled",
                initial_position=normalized_position,
                final_position=Decimal("0"),
                close_side=close_side,
                trigger_reason=trigger_reason,
                summary=(
                    f"{reason_label}在第 {index + 1} 根回放K线上触发，"
                    f"按 {format_decimal(latest_order_price or Decimal('0'))} 完成平仓。"
                ),
                trigger_index=index,
                trigger_ts=point.ts,
                trigger_price=point.trigger_price,
                close_order_price=latest_order_price,
                fill_price=latest_fill_price,
                events=events,
            )
        except Exception as exc:
            events.append(
                ProtectionReplayEvent(
                    ts=point.ts,
                    event_type="error",
                    message=str(exc),
                    trigger_price=point.trigger_price,
                    option_mark_price=point.option_mark_price,
                    order_price=latest_order_price,
                    close_side=close_side,
                    remaining_position=remaining,
                )
            )
            return ProtectionReplayResult(
                status="error",
                initial_position=normalized_position,
                final_position=remaining,
                close_side=close_side,
                trigger_reason=trigger_reason,
                summary=f"{reason_label}已触发，但回放平仓失败：{exc}",
                trigger_index=index,
                trigger_ts=point.ts,
                trigger_price=point.trigger_price,
                close_order_price=latest_order_price,
                fill_price=latest_fill_price,
                events=events,
            )

    return ProtectionReplayResult(
        status="not_triggered",
        initial_position=normalized_position,
        final_position=normalized_position,
        close_side=close_side,
        trigger_reason=None,
        summary="在当前回放区间内，没有触发止盈或止损。",
        trigger_index=None,
        trigger_ts=None,
        trigger_price=None,
        close_order_price=None,
        fill_price=None,
        events=events,
    )


def wait_order_fill(
    *,
    client: OkxRestClient,
    credentials: Credentials,
    config: StrategyConfig,
    inst_id: str,
    ord_id: str | None,
    estimated_price: Decimal,
    wait_seconds: float,
    stop_event: threading.Event,
) -> tuple[Decimal, Decimal]:
    if not ord_id:
        raise RuntimeError("OKX 未返回 ordId，无法确认保护平仓结果。")

    latest_state = ""
    for _ in range(12):
        status = client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
        latest_state = status.state.lower()
        filled_size = status.filled_size or Decimal("0")
        if latest_state == "filled":
            return filled_size if filled_size > 0 else status.size or Decimal("0"), status.avg_price or status.price or estimated_price
        if latest_state == "partially_filled" and filled_size > 0:
            return filled_size, status.avg_price or status.price or estimated_price
        if latest_state in {"canceled", "order_failed"}:
            break
        stop_event.wait(wait_seconds)

    raise RuntimeError(f"保护平仓订单未成交，ordId={ord_id}，状态={latest_state or 'unknown'}")


def _fmt_optional(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value)


def _pp_list_sessions(self: PositionProtectionManager) -> list[ProtectionSessionSnapshot]:
    with self._lock:
        workers = list(self._workers.values())
    workers.sort(key=lambda item: item.started_at, reverse=True)
    return [
        ProtectionSessionSnapshot(
            session_id=item.session_id,
            option_inst_id=item.protection.option_inst_id,
            trigger_inst_id=item.protection.trigger_inst_id,
            trigger_label=item.protection.trigger_label or item.protection.trigger_inst_id,
            trigger_price_type=item.protection.trigger_price_type,
            direction=item.protection.direction,
            pos_side=item.protection.pos_side,
            take_profit_trigger=item.protection.take_profit_trigger,
            take_profit_order_mode=item.protection.take_profit_order_mode,
            take_profit_order_price=item.protection.take_profit_order_price,
            take_profit_slippage=item.protection.take_profit_slippage,
            stop_loss_trigger=item.protection.stop_loss_trigger,
            stop_loss_order_mode=item.protection.stop_loss_order_mode,
            stop_loss_order_price=item.protection.stop_loss_order_price,
            stop_loss_slippage=item.protection.stop_loss_slippage,
            poll_seconds=item.protection.poll_seconds,
            status=_repair_mojibake_text(item.status),
            started_at=item.started_at,
            last_message=_repair_mojibake_text(item.last_message),
        )
        for item in workers
    ]


def _pp_start(
    self: PositionProtectionManager,
    credentials: Credentials,
    config: StrategyConfig,
    protection: OptionProtectionConfig,
) -> str:
    with self._lock:
        for existing in self._workers.values():
            existing_status = _repair_mojibake_text(existing.status)
            if existing.protection.option_inst_id == protection.option_inst_id and existing_status in {
                "准备中",
                "运行中",
                "平仓重试中",
                "持续未成交待人工",
            }:
                raise RuntimeError(f"{protection.option_inst_id} 已经存在一个运行中的保护任务。")

        self._counter += 1
        session_id = f"P{self._counter:02d}"
        worker = _ProtectionWorker(
            session_id=session_id,
            credentials=credentials,
            config=config,
            protection=protection,
        )
        worker.status = "准备中"
        worker.thread = threading.Thread(
            target=self._run_worker,
            args=(worker,),
            daemon=True,
            name=f"qqokx-protection-{session_id}",
        )
        self._workers[session_id] = worker

    self._set_status(worker, "准备中", "期权持仓保护任务已创建。")
    assert worker.thread is not None
    worker.thread.start()
    return session_id


def _pp_stop(self: PositionProtectionManager, session_id: str) -> None:
    with self._lock:
        worker = self._workers.get(session_id)
    if worker is None:
        raise RuntimeError("未找到对应的保护任务。")
    worker.stop_event.set()
    self._set_status(worker, "停止中", "已请求停止保护任务。")


def _pp_stop_all(self: PositionProtectionManager) -> None:
    with self._lock:
        workers = list(self._workers.values())
    for worker in workers:
        worker.stop_event.set()
        self._set_status(worker, "停止中", "主程序关闭，正在停止保护任务。")


def _pp_handle_close_retry(
    self: PositionProtectionManager,
    worker: _ProtectionWorker,
    reason: str,
    exc: Exception,
) -> None:
    worker.close_retry_count += 1
    if worker.close_retry_count >= 3:
        status = "持续未成交待人工"
        message = (
            f"{reason}平仓持续未成交，待人工关注（第{worker.close_retry_count}次） | "
            f"{worker.protection.option_inst_id} | {exc}"
        )
    else:
        status = "平仓重试中"
        message = (
            f"{reason}平仓未成交，正在重试（第{worker.close_retry_count}次） | "
            f"{worker.protection.option_inst_id} | {exc}"
        )
    self._set_status(worker, status, message)
    notify_key = f"{worker.protection.option_inst_id}|{reason}|{status}"
    if worker.last_close_retry_notify_key != notify_key:
        self._notify(
            subject=f"[QQOKX] 持仓保护平仓重试 | {worker.protection.option_inst_id}",
            body="\n".join(
                [
                    f"任务：{worker.session_id}",
                    f"期权合约：{worker.protection.option_inst_id}",
                    f"当前阶段：{reason}已触发，仍在平仓重试",
                    f"重试次数：{worker.close_retry_count}",
                    f"最新原因：{exc}",
                ]
            ),
        )
        worker.last_close_retry_notify_key = notify_key


def _pp_run_worker(self: PositionProtectionManager, worker: _ProtectionWorker) -> None:
    protection = worker.protection
    self._set_status(
        worker,
        "运行中",
        (
            f"开始监控 {protection.option_inst_id} | 触发源={protection.trigger_label or protection.trigger_inst_id} | "
            f"止盈={_fmt_optional(protection.take_profit_trigger)} | 止损={_fmt_optional(protection.stop_loss_trigger)}"
        ),
    )
    while not worker.stop_event.is_set():
        try:
            if worker.pending_close_reason is not None:
                self._close_position(worker, worker.pending_close_reason)
                self._reset_transient_state(worker)
                worker.close_retry_count = 0
                worker.last_close_retry_notify_key = None
                self._set_status(worker, "已完成", f"{worker.pending_close_reason}已完成平仓流程。")
                return

            current_price = self._client.get_trigger_price(
                protection.trigger_inst_id,
                protection.trigger_price_type,
            )
            worker.last_trigger_price = current_price
            stop_hit, take_hit = evaluate_protection_trigger(
                direction=protection.direction,
                current_price=current_price,
                stop_loss=protection.stop_loss_trigger,
                take_profit=protection.take_profit_trigger,
                option_inst_id=protection.option_inst_id,
                uses_underlying_trigger=uses_underlying_price_trigger(protection),
            )
            self._reset_transient_state(worker)
            self._set_status(
                worker,
                "运行中",
                f"监控中 | 触发价={format_decimal(current_price)} | 触发源={protection.trigger_label or protection.trigger_inst_id}",
            )
            if stop_hit or take_hit:
                reason = "止损" if stop_hit else "止盈"
                worker.pending_close_reason = reason
                worker.close_retry_count = 0
                worker.last_close_retry_notify_key = None
                trigger_ticker = self._safe_get_ticker(protection.option_inst_id)
                trigger_market_summary = self._format_ticker_snapshot(trigger_ticker)
                self._logger(
                    _repair_mojibake_text(
                        f"[持仓保护 {worker.session_id}] {reason}触发市场快照 | {protection.option_inst_id} | {trigger_market_summary}"
                    )
                )
                if not worker.trigger_notification_sent:
                    self._logger(
                        _repair_mojibake_text(
                            f"[持仓保护 {worker.session_id}] {reason}触发 | {protection.option_inst_id} | "
                            f"触发源={protection.trigger_label or protection.trigger_inst_id} | 当前价={format_decimal(current_price)}"
                        )
                    )
                    self._notify(
                        subject=f"[QQOKX] 持仓保护触发 | {reason} | {protection.option_inst_id}",
                        body="\n".join(
                            [
                                f"任务：{worker.session_id}",
                                f"期权合约：{protection.option_inst_id}",
                                f"方向：{protection.direction}",
                                f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                                f"当前触发价：{format_decimal(current_price)}",
                                f"止盈触发：{_fmt_optional(protection.take_profit_trigger)}",
                                f"止损触发：{_fmt_optional(protection.stop_loss_trigger)}",
                                f"触发原因：{reason}",
                            ]
                        ),
                    )
                    worker.trigger_notification_sent = True
                self._set_status(worker, "运行中", f"{reason}已触发，正在尝试平仓 | 触发价={format_decimal(current_price)}")
                continue
            worker.stop_event.wait(protection.poll_seconds)
        except ProtectionCloseRetryError as exc:
            self._pp_handle_close_retry(worker, worker.pending_close_reason or "平仓", exc)
            worker.stop_event.wait(max(protection.poll_seconds, 0.2))
            continue
        except ProtectionPriceGuardError as exc:
            self._set_status(worker, "异常", f"价格保护拦截：{exc}")
            self._notify(
                subject=f"[QQOKX] 持仓保护价格拦截 | {protection.option_inst_id}",
                body="\n".join(
                    [
                        f"任务：{worker.session_id}",
                        f"期权合约：{protection.option_inst_id}",
                        f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                        f"当前阶段：{worker.pending_close_reason or '监控触发价'}",
                        f"拦截原因：{exc}",
                        "处理结果：本次自动报单已停止，请人工确认后处理。",
                    ]
                ),
            )
            return
        except Exception as exc:
            if self._is_transient_error(exc):
                self._handle_transient_error(worker, exc)
                worker.stop_event.wait(max(protection.poll_seconds, 0.05))
                continue
            self._set_status(worker, "异常", f"保护任务异常：{exc}")
            self._notify(
                subject=f"[QQOKX] 持仓保护异常 | {protection.option_inst_id}",
                body="\n".join(
                    [
                        f"任务：{worker.session_id}",
                        f"期权合约：{protection.option_inst_id}",
                        f"触发源：{protection.trigger_label or protection.trigger_inst_id}",
                        f"异常：{exc}",
                    ]
                ),
            )
            return

    self._set_status(worker, "已停止", "保护任务已停止。")


def _pp_close_position(self: PositionProtectionManager, worker: _ProtectionWorker, reason: str) -> None:
    trade_instrument = self._client.get_instrument(worker.protection.option_inst_id)
    remaining_position = self._find_matching_position(worker)
    if remaining_position is None:
        opposite = self._find_any_position(worker)
        if opposite is not None:
            raise RuntimeError(
                f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
            )
        self._clear_active_close_order(worker)
        self._logger(f"[持仓保护 {worker.session_id}] {worker.protection.option_inst_id} 当前已经没有持仓。")
        return

    remaining = abs(remaining_position.position)
    close_side = derive_close_side(worker.protection.direction)
    pos_side = worker.protection.pos_side

    for _ in range(3):
        if remaining <= 0:
            break

        if worker.active_close_cl_ord_id is not None:
            state = self._reconcile_active_close_order(
                worker,
                reason=reason,
                close_side=close_side,
                expected_remaining=remaining,
            )
            if state == "waiting":
                raise ProtectionCloseRetryError("平仓订单状态确认中，稍后重试。")
            latest = self._find_matching_position(worker)
            if latest is None:
                opposite = self._find_any_position(worker)
                if opposite is not None:
                    raise RuntimeError(
                        f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
                    )
                remaining = Decimal("0")
                break
            remaining = abs(latest.position)
            if state == "progressed":
                continue
            raise ProtectionCloseRetryError(f"{reason}平仓单未成交，准备重新挂单。")

        size = snap_to_increment(remaining, trade_instrument.lot_size, "down")
        if size < trade_instrument.min_size:
            raise RuntimeError(
                f"剩余仓位 {format_decimal(remaining)} 小于最小下单量 {format_decimal(trade_instrument.min_size)}"
            )

        cl_ord_id = worker.active_close_cl_ord_id or self._next_close_cl_ord_id(worker)
        order_price = build_close_order_price(
            client=self._client,
            option_inst_id=worker.protection.option_inst_id,
            close_side=close_side,
            tick_size=trade_instrument.tick_size,
            mode=worker.protection.stop_loss_order_mode if reason == "止损" else worker.protection.take_profit_order_mode,
            fixed_price=worker.protection.stop_loss_order_price if reason == "止损" else worker.protection.take_profit_order_price,
            slippage=worker.protection.stop_loss_slippage if reason == "止损" else worker.protection.take_profit_slippage,
        )
        spot_price, option_ticker = validate_live_protection_order_price_guard(
            client=self._client,
            option_inst_id=worker.protection.option_inst_id,
            close_side=close_side,
            order_price=order_price,
            tick_size=trade_instrument.tick_size,
            open_avg_price=remaining_position.avg_price,
        )
        self._logger(
            _repair_mojibake_text(
                f"[持仓保护 {worker.session_id}] {reason}平仓报单 | {worker.protection.option_inst_id} | "
                f"方向={close_side.upper()} | 委托价={format_decimal(order_price)} | 委托量={format_decimal(size)} | "
                f"clOrdId={cl_ord_id} | 触发价={_fmt_optional(worker.last_trigger_price)} | "
                f"现货={format_decimal(spot_price)} | 开仓均价={_fmt_optional(remaining_position.avg_price)} | "
                f"{self._format_ticker_snapshot(option_ticker)}"
            )
        )
        worker.active_close_cl_ord_id = cl_ord_id
        worker.active_close_submitted_at = datetime.now()
        result = self._client.place_simple_order(
            worker.credentials,
            worker.config,
            inst_id=worker.protection.option_inst_id,
            side=close_side,
            size=size,
            ord_type="ioc",
            pos_side=pos_side,
            price=order_price,
            cl_ord_id=cl_ord_id,
        )
        worker.active_close_ord_id = result.ord_id or None
        worker.active_close_cl_ord_id = result.cl_ord_id or cl_ord_id
        try:
            filled_size, filled_price = wait_order_fill(
                client=self._client,
                credentials=worker.credentials,
                config=worker.config,
                inst_id=worker.protection.option_inst_id,
                ord_id=result.ord_id,
                estimated_price=order_price,
                wait_seconds=max(worker.protection.poll_seconds / 2, 0.5),
                stop_event=worker.stop_event,
            )
        except ProtectionCloseRetryError:
            self._clear_active_close_order(worker)
            raise

        latest = self._find_matching_position(worker)
        remaining = Decimal("0") if latest is None else abs(latest.position)
        opposite = self._find_any_position(worker)
        if latest is None and opposite is not None:
            raise RuntimeError(
                f"{worker.protection.option_inst_id} 检测到反向仓位 {format_decimal(opposite.position)}，已停止自动保护，请立即人工检查。"
            )
        self._record_close_fill(
            worker,
            reason=reason,
            close_side=close_side,
            filled_size=filled_size,
            filled_price=filled_price,
            remaining=remaining,
            order_price=order_price,
            ticker=option_ticker,
        )
        self._clear_active_close_order(worker)
        latest = self._find_matching_position(worker)
        if latest is None:
            remaining = Decimal("0")
            break
        remaining = abs(latest.position)

    if remaining > 0:
        raise RuntimeError(f"{worker.protection.option_inst_id} 保护平仓后仍有剩余仓位 {format_decimal(remaining)}")


def _pp_reconcile_active_close_order(
    self: PositionProtectionManager,
    worker: _ProtectionWorker,
    *,
    reason: str,
    close_side: Literal["buy", "sell"],
    expected_remaining: Decimal,
) -> Literal["waiting", "progressed", "retry"]:
    if worker.active_close_cl_ord_id is None:
        return "retry"
    try:
        status = self._client.get_order(
            worker.credentials,
            worker.config,
            inst_id=worker.protection.option_inst_id,
            ord_id=worker.active_close_ord_id,
            cl_ord_id=worker.active_close_cl_ord_id,
        )
    except OkxApiError as exc:
        if not self._is_missing_order_error(exc):
            raise
        if worker.active_close_submitted_at is not None:
            elapsed = (datetime.now() - worker.active_close_submitted_at).total_seconds()
            if elapsed < max(worker.protection.poll_seconds * 2, 5.0):
                return "waiting"
        self._clear_active_close_order(worker)
        return "retry"

    state = (status.state or "").lower()
    filled_size = status.filled_size or Decimal("0")
    if filled_size > 0:
        latest = self._find_matching_position(worker)
        remaining = Decimal("0") if latest is None else abs(latest.position)
        self._record_close_fill(
            worker,
            reason=reason,
            close_side=close_side,
            filled_size=filled_size,
            filled_price=status.avg_price or status.price or Decimal("0"),
            remaining=remaining,
            order_price=status.price,
            ticker=self._safe_get_ticker(worker.protection.option_inst_id),
        )

    if state in {"filled", "partially_filled"}:
        self._clear_active_close_order(worker)
        return "progressed"
    if state in {"canceled", "mmp_canceled", "order_failed"}:
        self._clear_active_close_order(worker)
        return "progressed" if filled_size > 0 else "retry"
    return "waiting"


def _pp_record_close_fill(
    self: PositionProtectionManager,
    worker: _ProtectionWorker,
    *,
    reason: str,
    close_side: Literal["buy", "sell"],
    filled_size: Decimal,
    filled_price: Decimal,
    remaining: Decimal,
    order_price: Decimal | None = None,
    ticker: OkxTicker | None = None,
) -> None:
    self._logger(
        _repair_mojibake_text(
            f"[持仓保护 {worker.session_id}] {reason}平仓成交 | {worker.protection.option_inst_id} | "
            f"方向={close_side.upper()} | 成交价={format_decimal(filled_price)} | 成交量={format_decimal(filled_size)} | "
            f"剩余={format_decimal(max(remaining, Decimal('0')))}"
        )
    )
    self._logger(
        _repair_mojibake_text(
            f"[持仓保护 {worker.session_id}] {reason}成交快照 | {worker.protection.option_inst_id} | "
            f"委托价={_fmt_optional(order_price)} | {self._format_ticker_snapshot(ticker)}"
        )
    )
    self._notify(
        subject=f"[QQOKX] 持仓保护成交 | {reason} | {worker.protection.option_inst_id}",
        body="\n".join(
            [
                f"任务：{worker.session_id}",
                f"期权合约：{worker.protection.option_inst_id}",
                f"触发原因：{reason}",
                f"平仓方向：{close_side}",
                f"成交数量：{format_decimal(filled_size)}",
                f"成交价格：{format_decimal(filled_price)}",
                f"剩余仓位：{format_decimal(max(remaining, Decimal('0')))}",
            ]
        ),
    )


def _pp_is_missing_order_error(self: PositionProtectionManager, exc: Exception) -> bool:
    message = _repair_mojibake_text(str(exc)).lower()
    return "未返回订单状态" in message or "order does not exist" in message


def _pp_set_status(self: PositionProtectionManager, worker: _ProtectionWorker, status: str, message: str) -> None:
    repaired_status = _repair_mojibake_text(status)
    repaired_message = _repair_mojibake_text(message)
    with self._lock:
        worker.status = repaired_status
        worker.last_message = repaired_message
    self._logger(f"[持仓保护 {worker.session_id}] {repaired_message}")
    self._emit_change()


def _pp_is_transient_error(self: PositionProtectionManager, exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, OkxApiError) and exc.status is not None and exc.status >= 500:
        return True
    message = _repair_mojibake_text(str(exc)).lower()
    transient_markers = (
        "网络错误",
        "timeout",
        "timed out",
        "handshake",
        "ssl",
        "connection reset",
        "connection aborted",
        "connection refused",
        "temporarily unavailable",
        "temporary failure",
        "remote disconnected",
        "read timed out",
        "connect timeout",
        "proxy error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "握手",
        "超时",
    )
    return any(marker in message for marker in transient_markers)


def _pp_handle_transient_error(self: PositionProtectionManager, worker: _ProtectionWorker, exc: Exception) -> None:
    now = datetime.now()
    worker.transient_error_count += 1
    worker.last_transient_error_at = now
    message = (
        f"网络异常，正在重试（第{worker.transient_error_count}次） | "
        f"{worker.protection.option_inst_id} | {exc}"
    )
    if (
        worker.last_transient_status_at is None
        or (now - worker.last_transient_status_at).total_seconds() >= self._transient_status_interval_seconds
    ):
        self._set_status(worker, "运行中", message)
        worker.last_transient_status_at = now

    if (
        worker.last_transient_notify_at is None
        or (now - worker.last_transient_notify_at).total_seconds() >= self._transient_alert_interval_seconds
    ):
        self._notify(
            subject=f"[QQOKX] 持仓保护网络重试 | {worker.protection.option_inst_id}",
            body="\n".join(
                [
                    f"任务：{worker.session_id}",
                    f"期权合约：{worker.protection.option_inst_id}",
                    f"触发源：{worker.protection.trigger_label or worker.protection.trigger_inst_id}",
                    "当前状态：网络异常，正在重试，任务不会自动停止",
                    f"连续异常次数：{worker.transient_error_count}",
                    f"最新异常：{exc}",
                    (
                        f"当前平仓阶段：{worker.pending_close_reason}"
                        if worker.pending_close_reason is not None
                        else "当前阶段：监控触发价"
                    ),
                ]
            ),
        )
        worker.last_transient_notify_at = now


def _pp_notify(self: PositionProtectionManager, *, subject: str, body: str) -> None:
    if self._notifier is not None and self._notifier.enabled:
        self._notifier.notify_async(_repair_mojibake_text(subject), _repair_mojibake_text(body))


def _pp_format_ticker_snapshot(self: PositionProtectionManager, ticker: OkxTicker | None) -> str:
    if ticker is None:
        return "盘口=不可用"
    return (
        "盘口 "
        f"bid/ask/last/mark={_fmt_optional(ticker.bid)}/{_fmt_optional(ticker.ask)}/"
        f"{_fmt_optional(ticker.last)}/{_fmt_optional(ticker.mark)}"
    )


def validate_protection_order_price_guard(
    *,
    option_inst_id: str,
    close_side: Literal["buy", "sell"],
    order_price: Decimal,
    tick_size: Decimal,
    open_avg_price: Decimal | None,
    spot_price: Decimal,
    option_bid: Decimal | None,
    option_ask: Decimal | None,
    option_last: Decimal | None,
) -> None:
    if order_price <= 0:
        raise ProtectionPriceGuardError(f"{option_inst_id} 的报单价格 {format_decimal(order_price)} 无效。")
    if spot_price <= 0:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法取得有效现货价格，不能完成价格保护校验。")

    intrinsic_price = compute_option_intrinsic_price(option_inst_id=option_inst_id, spot_price=spot_price)
    hard_cap = compute_option_hard_price_cap(option_inst_id=option_inst_id, spot_price=spot_price)
    option_quotes = _positive_quote_candidates(option_bid, option_ask, option_last)
    market_anchor = max(option_quotes) * PRICE_GUARD_MARKET_MULTIPLIER if option_quotes else Decimal("0")
    avg_price = open_avg_price if open_avg_price is not None and open_avg_price > 0 else Decimal("0")

    strike = infer_option_strike(option_inst_id)
    if strike is None:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法解析行权价，不能完成价格保护校验。")

    moneyness_distance = abs(spot_price - strike) / spot_price
    delta_one_anchor = intrinsic_price + moneyness_distance + (avg_price * PRICE_GUARD_OPEN_BUFFER_MULTIPLIER)
    open_anchor = avg_price * PRICE_GUARD_OPEN_AVG_MULTIPLIER if avg_price > 0 else Decimal("0")

    if close_side == "buy":
        upper_candidates = [tick_size, delta_one_anchor, open_anchor, market_anchor]
        upper_bound = max(upper_candidates)
        if hard_cap is not None and hard_cap > 0:
            upper_bound = min(upper_bound, hard_cap)
        upper_bound = max(upper_bound, tick_size)
        if order_price > upper_bound:
            raise ProtectionPriceGuardError(
                (
                    f"{option_inst_id} 价格保护已拦截：买回报单价 {format_decimal(order_price)} 高于安全上限 "
                    f"{format_decimal(upper_bound)} | 现货价 {format_decimal(spot_price)} | 行权价 {format_decimal(strike)} | "
                    f"开仓均价 {_fmt_optional(open_avg_price)} | bid/ask/last={_fmt_optional(option_bid)}/"
                    f"{_fmt_optional(option_ask)}/{_fmt_optional(option_last)}"
                )
            )
        return

    lower_candidates = [tick_size]
    if intrinsic_price > 0:
        lower_candidates.append(intrinsic_price * PRICE_GUARD_SELL_INTRINSIC_FLOOR_RATIO)
    if option_bid is not None and option_bid > 0:
        lower_candidates.append(option_bid * PRICE_GUARD_SELL_BID_FLOOR_RATIO)
    lower_bound = max(lower_candidates)
    if order_price < lower_bound:
        raise ProtectionPriceGuardError(
            (
                f"{option_inst_id} 价格保护已拦截：卖出报单价 {format_decimal(order_price)} 低于安全下限 "
                f"{format_decimal(lower_bound)} | 现货价 {format_decimal(spot_price)} | 行权价 {format_decimal(strike)} | "
                f"开仓均价 {_fmt_optional(open_avg_price)} | bid/ask/last={_fmt_optional(option_bid)}/"
                f"{_fmt_optional(option_ask)}/{_fmt_optional(option_last)}"
            )
        )


def describe_protection_price_logic(
    *,
    option_inst_id: str,
    direction: Literal["long", "short"],
    trigger_inst_id: str,
    trigger_price_type: TriggerPriceType,
) -> str:
    uses_underlying_trigger = not (
        option_inst_id.strip().upper() == trigger_inst_id.strip().upper() and trigger_price_type == "mark"
    )
    option_style = infer_option_style(option_inst_id)
    direction_label = "买入" if direction == "long" else "卖出"
    style_label = "认购" if option_style == "call" else "认沽" if option_style == "put" else "期权"
    trigger_label = "现货触发" if uses_underlying_trigger else "期权标记价格触发"
    profit_on_rise = is_profit_when_trigger_price_rises(
        option_inst_id=option_inst_id,
        direction=direction,
        uses_underlying_trigger=uses_underlying_trigger,
    )
    if profit_on_rise:
        return (
            f"{direction_label}{style_label} + {trigger_label}：价格上涨偏向止盈，价格下跌偏向止损，"
            f"所以止盈触发价应大于止损触发价。"
        )
    return (
        f"{direction_label}{style_label} + {trigger_label}：价格下跌偏向止盈，价格上涨偏向止损，"
        f"所以止盈触发价应小于止损触发价。"
    )


def build_close_order_price_from_mark(
    *,
    mark_price: Decimal,
    close_side: Literal["buy", "sell"],
    tick_size: Decimal,
    mode: ClosePriceMode,
    fixed_price: Decimal | None,
    slippage: Decimal,
) -> Decimal:
    if mode == "fixed_price":
        if fixed_price is None or fixed_price <= 0:
            raise RuntimeError("固定报单价格必须大于 0。")
        return snap_to_increment(fixed_price, tick_size, "up" if close_side == "buy" else "down")

    if close_side == "buy":
        raw_price = mark_price + slippage
        return snap_to_increment(raw_price, tick_size, "up")
    raw_price = mark_price - slippage
    if raw_price <= 0:
        raw_price = tick_size
    return snap_to_increment(raw_price, tick_size, "down")


def validate_live_protection_order_price_guard(
    *,
    client: OkxRestClient,
    option_inst_id: str,
    close_side: Literal["buy", "sell"],
    order_price: Decimal,
    tick_size: Decimal,
    open_avg_price: Decimal | None,
    spot_price: Decimal | None = None,
    ticker: OkxTicker | None = None,
) -> tuple[Decimal, OkxTicker]:
    spot_inst_id = infer_default_spot_inst_id(option_inst_id)
    if not spot_inst_id:
        raise ProtectionPriceGuardError(f"{option_inst_id} 无法推导现货标的，不能完成价格保护校验。")
    resolved_spot_price = spot_price if spot_price is not None else client.get_trigger_price(spot_inst_id, "last")
    resolved_ticker = ticker if ticker is not None else client.get_ticker(option_inst_id)
    validate_protection_order_price_guard(
        option_inst_id=option_inst_id,
        close_side=close_side,
        order_price=order_price,
        tick_size=tick_size,
        open_avg_price=open_avg_price,
        spot_price=resolved_spot_price,
        option_bid=resolved_ticker.bid,
        option_ask=resolved_ticker.ask,
        option_last=resolved_ticker.last,
    )
    return resolved_spot_price, resolved_ticker


def wait_order_fill(
    *,
    client: OkxRestClient,
    credentials: Credentials,
    config: StrategyConfig,
    inst_id: str,
    ord_id: str | None,
    estimated_price: Decimal,
    wait_seconds: float,
    stop_event: threading.Event,
) -> tuple[Decimal, Decimal]:
    if not ord_id:
        raise RuntimeError("OKX 未返回 ordId，无法确认保护平仓结果。")

    latest_state = ""
    for _ in range(12):
        status = client.get_order(credentials, config, inst_id=inst_id, ord_id=ord_id)
        latest_state = (status.state or "").lower()
        filled_size = status.filled_size or Decimal("0")
        resolved_price = status.avg_price or status.price or estimated_price
        if latest_state == "filled":
            return filled_size if filled_size > 0 else status.size or Decimal("0"), resolved_price
        if latest_state == "partially_filled" and filled_size > 0:
            return filled_size, resolved_price
        if latest_state in {"canceled", "order_failed", "mmp_canceled"}:
            if filled_size > 0:
                return filled_size, resolved_price
            raise ProtectionCloseRetryError(
                f"保护平仓订单未成交，ordId={ord_id}，状态={latest_state or 'unknown'}"
            )
        stop_event.wait(wait_seconds)

    raise ProtectionCloseRetryError(f"保护平仓订单未成交，ordId={ord_id}，状态={latest_state or 'unknown'}")


PositionProtectionManager.list_sessions = _pp_list_sessions
PositionProtectionManager.start = _pp_start
PositionProtectionManager.stop = _pp_stop
PositionProtectionManager.stop_all = _pp_stop_all
PositionProtectionManager._run_worker = _pp_run_worker
PositionProtectionManager._close_position = _pp_close_position
PositionProtectionManager._reconcile_active_close_order = _pp_reconcile_active_close_order
PositionProtectionManager._record_close_fill = _pp_record_close_fill
PositionProtectionManager._is_missing_order_error = _pp_is_missing_order_error
PositionProtectionManager._set_status = _pp_set_status
PositionProtectionManager._is_transient_error = _pp_is_transient_error
PositionProtectionManager._handle_transient_error = _pp_handle_transient_error
PositionProtectionManager._notify = _pp_notify
PositionProtectionManager._format_ticker_snapshot = _pp_format_ticker_snapshot
PositionProtectionManager._pp_handle_close_retry = _pp_handle_close_retry
