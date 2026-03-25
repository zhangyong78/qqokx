from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from okx_quant.models import Credentials, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxPosition, OkxRestClient
from okx_quant.pricing import format_decimal, snap_to_increment


Logger = Callable[[str], None]
ChangeCallback = Callable[[], None]
TriggerPriceType = Literal["mark", "last"]
ClosePriceMode = Literal["fixed_price", "mark_with_slippage"]
ReplayStatus = Literal["not_triggered", "filled", "error"]
ReplayTriggerReason = Literal["take_profit", "stop_loss"] | None


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
    trigger_label: str
    direction: str
    take_profit_trigger: Decimal | None
    stop_loss_trigger: Decimal | None
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


class PositionProtectionManager:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        notifier: EmailNotifier | None = None,
        on_change: ChangeCallback | None = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._notifier = notifier
        self._on_change = on_change
        self._lock = threading.Lock()
        self._counter = 0
        self._workers: dict[str, _ProtectionWorker] = {}

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
                trigger_label=item.protection.trigger_label or item.protection.trigger_inst_id,
                direction=item.protection.direction,
                take_profit_trigger=item.protection.take_profit_trigger,
                stop_loss_trigger=item.protection.stop_loss_trigger,
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
        try:
            while not worker.stop_event.is_set():
                current_price = self._client.get_trigger_price(
                    protection.trigger_inst_id,
                    protection.trigger_price_type,
                )
                stop_hit, take_hit = evaluate_protection_trigger(
                    direction=protection.direction,
                    current_price=current_price,
                    stop_loss=protection.stop_loss_trigger,
                    take_profit=protection.take_profit_trigger,
                    option_inst_id=protection.option_inst_id,
                    uses_underlying_trigger=uses_underlying_price_trigger(protection),
                )
                self._set_status(
                    worker,
                    "运行中",
                    f"监控中 | 触发价={format_decimal(current_price)} | 触发源={protection.trigger_label or protection.trigger_inst_id}",
                )
                if stop_hit or take_hit:
                    reason = "止损" if stop_hit else "止盈"
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
                    self._close_position(worker, reason)
                    self._set_status(worker, "已完成", f"{reason}已完成平仓流程。")
                    return
                worker.stop_event.wait(protection.poll_seconds)

            self._set_status(worker, "已停止", "保护任务已停止。")
        except Exception as exc:
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

    def _close_position(self, worker: _ProtectionWorker, reason: str) -> None:
        trade_instrument = self._client.get_instrument(worker.protection.option_inst_id)
        remaining_position = self._find_matching_position(worker)
        if remaining_position is None:
            self._logger(f"[持仓保护 {worker.session_id}] {worker.protection.option_inst_id} 当前已经没有持仓。")
            return

        remaining = abs(remaining_position.position)
        close_side = derive_close_side(worker.protection.direction)
        pos_side = worker.protection.pos_side

        for _ in range(3):
            if remaining <= 0:
                break

            size = snap_to_increment(remaining, trade_instrument.lot_size, "down")
            if size < trade_instrument.min_size:
                raise RuntimeError(
                    f"剩余仓位 {format_decimal(remaining)} 小于最小下单量 {format_decimal(trade_instrument.min_size)}"
                )

            order_price = build_close_order_price(
                client=self._client,
                option_inst_id=worker.protection.option_inst_id,
                close_side=close_side,
                tick_size=trade_instrument.tick_size,
                mode=worker.protection.stop_loss_order_mode if reason == "止损" else worker.protection.take_profit_order_mode,
                fixed_price=worker.protection.stop_loss_order_price if reason == "止损" else worker.protection.take_profit_order_price,
                slippage=worker.protection.stop_loss_slippage if reason == "止损" else worker.protection.take_profit_slippage,
            )
            result = self._client.place_simple_order(
                worker.credentials,
                worker.config,
                inst_id=worker.protection.option_inst_id,
                side=close_side,
                size=size,
                ord_type="ioc",
                pos_side=pos_side,
                price=order_price,
            )
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
            remaining -= filled_size
            self._logger(
                f"[持仓保护 {worker.session_id}] {reason}平仓成交 | {worker.protection.option_inst_id} | "
                f"方向={close_side.upper()} | 成交价={format_decimal(filled_price)} | 成交量={format_decimal(filled_size)} | "
                f"剩余={format_decimal(max(remaining, Decimal('0')))}"
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

    def _set_status(self, worker: _ProtectionWorker, status: str, message: str) -> None:
        with self._lock:
            worker.status = status
            worker.last_message = message
        self._logger(f"[持仓保护 {worker.session_id}] {message}")
        self._emit_change()

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
