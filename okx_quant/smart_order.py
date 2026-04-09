from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Literal

from okx_quant.log_utils import ensure_log_timestamp
from okx_quant.models import Credentials, Instrument, PositionMode, StrategyConfig, TradeMode, TriggerPriceType
from okx_quant.okx_client import OkxOrderBook, OkxOrderStatus, OkxRestClient, OkxTicker, infer_inst_type
from okx_quant.persistence import load_smart_order_tasks_snapshot, save_smart_order_tasks_snapshot
from okx_quant.position_protection import evaluate_protection_trigger, validate_live_protection_order_price_guard
from okx_quant.pricing import format_decimal, format_decimal_by_increment, snap_to_increment


Logger = Callable[[str], None]
TaskType = Literal["grid", "condition", "tp_sl"]
TaskStatus = Literal["准备中", "等待触发", "等待成交", "等待止盈止损", "运行中", "已完成", "已停止", "异常", "待恢复"]
TriggerDirection = Literal["above", "below"]
ExecutionMode = Literal["limit", "aggressive_ioc"]
CycleMode = Literal["continuous", "counted"]

GRID_TICKER_REFRESH_SECONDS = 3.0
WORKER_POLL_SECONDS = 1.2
MAX_LOG_LINES = 400
POSITION_USAGE_REFRESH_SECONDS = 3.0

STATUS_READY = "\u51c6\u5907\u4e2d"
STATUS_WAIT_TRIGGER = "\u7b49\u5f85\u89e6\u53d1"
STATUS_WAIT_FILL = "\u7b49\u5f85\u6210\u4ea4"
STATUS_WAIT_TP_SL = "\u7b49\u5f85\u6b62\u76c8\u6b62\u635f"
STATUS_RUNNING = "\u8fd0\u884c\u4e2d"
STATUS_COMPLETED = "\u5df2\u5b8c\u6210"
STATUS_STOPPED = "\u5df2\u505c\u6b62"
STATUS_ERROR = "\u5f02\u5e38"
STATUS_RECOVERABLE = "\u5f85\u6062\u590d"
STATUS_POSITION_LIMIT = "\u8d85\u9650\u51bb\u7ed3"

TERMINAL_STATUSES = {STATUS_COMPLETED, STATUS_STOPPED, STATUS_ERROR}
REMOVABLE_STATUSES = TERMINAL_STATUSES | {STATUS_RECOVERABLE}
LOCKING_STATUSES = {
    STATUS_READY,
    STATUS_WAIT_TRIGGER,
    STATUS_WAIT_FILL,
    STATUS_WAIT_TP_SL,
    STATUS_RUNNING,
    STATUS_RECOVERABLE,
    STATUS_POSITION_LIMIT,
}


@dataclass(frozen=True)
class SmartOrderRuntimeConfig:
    credentials: Credentials
    environment: Literal["demo", "live"]
    trade_mode: TradeMode
    position_mode: PositionMode
    credential_profile_name: str = ""


@dataclass(frozen=True)
class SmartOrderTaskSnapshot:
    task_id: str
    task_type: TaskType
    inst_id: str
    trigger_label: str
    side: str
    status: str
    started_at: datetime
    active_order_price: Decimal | None
    active_order_size: Decimal | None
    completed_cycles: int
    cycle_limit_label: str
    last_message: str


@dataclass(frozen=True)
class SmartOrderLadderLevel:
    price: Decimal
    buy_working: Decimal | None
    sell_working: Decimal | None
    working_labels: tuple[str, ...]
    is_last_price: bool = False
    is_best_bid: bool = False
    is_best_ask: bool = False


@dataclass(frozen=True)
class SmartOrderPositionLimitState:
    enabled: bool
    long_limit: Decimal | None
    short_limit: Decimal | None
    actual_long: Decimal
    actual_short: Decimal
    reserved_long: Decimal
    reserved_short: Decimal

    @property
    def used_long(self) -> Decimal:
        return self.actual_long + self.reserved_long

    @property
    def used_short(self) -> Decimal:
        return self.actual_short + self.reserved_short

    @property
    def available_long(self) -> Decimal | None:
        if self.long_limit is None:
            return None
        available = self.long_limit - self.used_long
        return available if available > 0 else Decimal("0")

    @property
    def available_short(self) -> Decimal | None:
        if self.short_limit is None:
            return None
        available = self.short_limit - self.used_short
        return available if available > 0 else Decimal("0")


@dataclass
class _SmartOrderTask:
    task_id: str
    task_type: TaskType
    inst_id: str
    instrument: Instrument
    runtime: SmartOrderRuntimeConfig
    side: Literal["buy", "sell"]
    size: Decimal
    started_at: datetime = field(default_factory=datetime.now)
    status: TaskStatus = STATUS_READY
    last_message: str = ""
    active_order_id: str | None = None
    active_order_cl_ord_id: str | None = None
    active_order_price: Decimal | None = None
    active_order_size: Decimal | None = None
    active_order_side: Literal["buy", "sell"] | None = None
    waiting_for_fill: bool = False
    stop_requested: bool = False
    completed_cycles: int = 0
    cycle_mode: CycleMode = "continuous"
    cycle_limit: int | None = None
    initial_side: Literal["buy", "sell"] = "buy"
    long_step: Decimal | None = None
    short_step: Decimal | None = None
    trigger_inst_id: str | None = None
    trigger_price_type: TriggerPriceType = "last"
    trigger_direction: TriggerDirection = "above"
    trigger_price: Decimal | None = None
    exec_mode: ExecutionMode = "limit"
    exec_price: Decimal | None = None
    take_profit: Decimal | None = None
    stop_loss: Decimal | None = None
    protection_position_side: Literal["long", "short"] | None = None
    protection_reason: Literal["止盈", "止损"] | None = None
    protection_armed: bool = False
    last_trigger_price: Decimal | None = None
    transient_error_count: int = 0

    def cycle_limit_label(self) -> str:
        if self.cycle_mode == "continuous" or self.cycle_limit is None:
            return "连续"
        return str(self.cycle_limit)


def compute_next_grid_order_price(
    *,
    filled_side: Literal["buy", "sell"],
    fill_price: Decimal,
    long_step: Decimal,
    short_step: Decimal,
    tick_size: Decimal,
) -> tuple[Literal["buy", "sell"], Decimal]:
    if filled_side == "buy":
        return "sell", snap_to_increment(fill_price + long_step, tick_size, "up")
    next_price = fill_price - short_step
    if next_price <= 0:
        raise ValueError("空单参数导致下一跳买价小于等于 0。")
    return "buy", snap_to_increment(next_price, tick_size, "down")


def build_rule_ladder_prices(*, center_price: Decimal, tick_size: Decimal, levels_each_side: int) -> list[Decimal]:
    if levels_each_side <= 0:
        raise ValueError("levels_each_side must be positive")
    center = snap_to_increment(center_price, tick_size, "nearest")
    prices: list[Decimal] = []
    for offset in range(levels_each_side, 0, -1):
        candidate = center + (tick_size * offset)
        prices.append(snap_to_increment(candidate, tick_size, "up"))
    prices.append(center)
    for offset in range(1, levels_each_side + 1):
        candidate = center - (tick_size * offset)
        if candidate <= 0:
            break
        prices.append(snap_to_increment(candidate, tick_size, "down"))
    return prices


def _bucket_price(
    price: Decimal,
    increment: Decimal,
    *,
    side: Literal["buy", "sell", "neutral"],
) -> Decimal:
    mode = "nearest"
    if side == "buy":
        mode = "down"
    elif side == "sell":
        mode = "up"
    return snap_to_increment(price, increment, mode)


def _fmt_optional(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value)


def resolve_best_quote_price(
    *,
    side: Literal["buy", "sell"],
    ticker: OkxTicker,
    order_book: OkxOrderBook | None,
    tick_size: Decimal,
) -> Decimal:
    if side == "buy":
        raw_price = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
        if raw_price is None or raw_price <= 0:
            raise RuntimeError("当前缺少买一价格，无法按最优价挂单。")
        return snap_to_increment(raw_price, tick_size, "down")
    raw_price = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
    if raw_price is None or raw_price <= 0:
        raise RuntimeError("当前缺少卖一价格，无法按最优价挂单。")
    return snap_to_increment(raw_price, tick_size, "up")


def _serialize_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format_decimal(value)


def _deserialize_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    return Decimal(str(value))


def _serialize_instrument(instrument: Instrument | None) -> dict[str, object] | None:
    if instrument is None:
        return None
    return {
        "inst_id": instrument.inst_id,
        "inst_type": instrument.inst_type,
        "tick_size": format_decimal(instrument.tick_size),
        "lot_size": format_decimal(instrument.lot_size),
        "min_size": format_decimal(instrument.min_size),
        "state": instrument.state,
        "settle_ccy": instrument.settle_ccy,
        "ct_val": _serialize_decimal(instrument.ct_val),
        "ct_mult": _serialize_decimal(instrument.ct_mult),
        "ct_val_ccy": instrument.ct_val_ccy,
        "uly": instrument.uly,
        "inst_family": instrument.inst_family,
    }


def _deserialize_instrument(payload: object) -> Instrument | None:
    if not isinstance(payload, dict):
        return None
    try:
        return Instrument(
            inst_id=str(payload.get("inst_id", "")),
            inst_type=str(payload.get("inst_type", "")),
            tick_size=Decimal(str(payload.get("tick_size", "0.00000001"))),
            lot_size=Decimal(str(payload.get("lot_size", "1"))),
            min_size=Decimal(str(payload.get("min_size", "1"))),
            state=str(payload.get("state", "")),
            settle_ccy=(str(payload["settle_ccy"]) if payload.get("settle_ccy") is not None else None),
            ct_val=_deserialize_decimal(payload.get("ct_val")),
            ct_mult=_deserialize_decimal(payload.get("ct_mult")),
            ct_val_ccy=(str(payload["ct_val_ccy"]) if payload.get("ct_val_ccy") is not None else None),
            uly=(str(payload["uly"]) if payload.get("uly") is not None else None),
            inst_family=(str(payload["inst_family"]) if payload.get("inst_family") is not None else None),
        )
    except Exception:
        return None


class SmartOrderManager:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger | None = None,
        *,
        storage_path: Path | None = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._storage_path = storage_path
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._tasks: dict[str, _SmartOrderTask] = {}
        self._task_counter = 0
        self._logs: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self._locked_inst_id: str | None = None
        self._locked_instrument: Instrument | None = None
        self._ladder_center_price: Decimal | None = None
        self._latest_ticker: OkxTicker | None = None
        self._latest_order_book: OkxOrderBook | None = None
        self._last_ticker_refresh_at: float = 0.0
        self._position_limit_enabled = False
        self._long_position_limit: Decimal | None = None
        self._short_position_limit: Decimal | None = None
        self._actual_usage_cache_key: tuple[str, str, str, str] | None = None
        self._actual_usage_cache_at: float = 0.0
        self._actual_usage_cache_value: tuple[Decimal, Decimal] | None = None
        self._last_persisted_payload = ""
        self._load_persisted_state()
        self._worker = threading.Thread(target=self._run_loop, daemon=True, name="qqokx-smart-order")
        self._worker.start()

    @property
    def locked_inst_id(self) -> str | None:
        with self._lock:
            self._reconcile_lock_state_locked()
            return self._locked_inst_id

    @property
    def locked_instrument(self) -> Instrument | None:
        with self._lock:
            self._reconcile_lock_state_locked()
            return self._locked_instrument

    def get_position_limit_config(self) -> tuple[bool, Decimal | None, Decimal | None]:
        with self._lock:
            return self._position_limit_enabled, self._long_position_limit, self._short_position_limit

    def set_position_limits(
        self,
        *,
        enabled: bool,
        long_limit: Decimal | None,
        short_limit: Decimal | None,
    ) -> None:
        with self._lock:
            self._position_limit_enabled = enabled
            self._long_position_limit = long_limit if enabled else None
            self._short_position_limit = short_limit if enabled else None
            self._invalidate_actual_usage_cache_locked()
        self._save_state(force=True)

    def list_logs(self) -> list[str]:
        with self._lock:
            return list(self._logs)

    def list_tasks(self) -> list[SmartOrderTaskSnapshot]:
        with self._lock:
            tasks = list(self._tasks.values())
        tasks.sort(key=lambda item: item.started_at, reverse=True)
        return [
            SmartOrderTaskSnapshot(
                task_id=item.task_id,
                task_type=item.task_type,
                inst_id=item.inst_id,
                trigger_label=self._build_trigger_label(item),
                side=item.side,
                status=item.status,
                started_at=item.started_at,
                active_order_price=item.active_order_price,
                active_order_size=item.active_order_size,
                completed_cycles=item.completed_cycles,
                cycle_limit_label=item.cycle_limit_label(),
                last_message=item.last_message,
            )
            for item in tasks
        ]

    def get_position_limit_state(
        self,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        *,
        force: bool = False,
    ) -> SmartOrderPositionLimitState:
        actual_long, actual_short = self._get_actual_position_usage(instrument, runtime, force=force)
        reserved_long, reserved_short = self._get_reserved_open_usage(instrument.inst_id)
        enabled, long_limit, short_limit = self.get_position_limit_config()
        return SmartOrderPositionLimitState(
            enabled=enabled,
            long_limit=long_limit,
            short_limit=short_limit,
            actual_long=actual_long,
            actual_short=actual_short,
            reserved_long=reserved_long,
            reserved_short=reserved_short,
        )

    def validate_opening_capacity(
        self,
        *,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        side: Literal["buy", "sell"],
        size: Decimal,
        ignore_task_id: str | None = None,
    ) -> SmartOrderPositionLimitState:
        actual_long, actual_short = self._get_actual_position_usage(instrument, runtime)
        reserved_long, reserved_short = self._get_reserved_open_usage(instrument.inst_id, ignore_task_id=ignore_task_id)
        state = SmartOrderPositionLimitState(
            enabled=self._position_limit_enabled,
            long_limit=self._long_position_limit if self._position_limit_enabled else None,
            short_limit=self._short_position_limit if self._position_limit_enabled else None,
            actual_long=actual_long,
            actual_short=actual_short,
            reserved_long=reserved_long,
            reserved_short=reserved_short,
        )
        if not state.enabled:
            return state
        additional_long, additional_short = self._estimate_opening_increment(
            instrument=instrument,
            runtime=runtime,
            side=side,
            size=size,
            actual_long=actual_long,
            actual_short=actual_short,
        )
        projected_long = state.used_long + additional_long
        projected_short = state.used_short + additional_short
        if state.long_limit is not None and projected_long > state.long_limit:
            raise RuntimeError(
                f"多头总仓位限制触发：当前占用 {format_decimal(state.used_long)}，"
                f"本次新增 {format_decimal(additional_long)}，上限 {format_decimal(state.long_limit)}。"
            )
        if state.short_limit is not None and projected_short > state.short_limit:
            raise RuntimeError(
                f"空头总仓位限制触发：当前占用 {format_decimal(state.used_short)}，"
                f"本次新增 {format_decimal(additional_short)}，上限 {format_decimal(state.short_limit)}。"
            )
        return state

    def _load_persisted_state(self) -> None:
        snapshot = load_smart_order_tasks_snapshot(self._storage_path)
        self._task_counter = int(snapshot.get("task_counter", 0))
        self._locked_inst_id = str(snapshot.get("locked_inst_id") or "").strip().upper() or None
        self._locked_instrument = _deserialize_instrument(snapshot.get("locked_instrument"))
        self._position_limit_enabled = bool(snapshot.get("position_limit_enabled", False))
        self._long_position_limit = _deserialize_decimal(snapshot.get("long_position_limit"))
        self._short_position_limit = _deserialize_decimal(snapshot.get("short_position_limit"))
        restored_tasks: dict[str, _SmartOrderTask] = {}
        for raw_task in snapshot.get("tasks", []):
            task = self._deserialize_task(raw_task)
            if task is None:
                continue
            if task.status not in {STATUS_COMPLETED, STATUS_STOPPED, STATUS_ERROR}:
                task.status = STATUS_RECOVERABLE
                task.stop_requested = False
                task.waiting_for_fill = False
                task.last_message = "软件异常关闭后恢复，请确认旧委托状态后再重新启动。"
            restored_tasks[task.task_id] = task
        if restored_tasks and self._locked_instrument is None:
            newest_task = sorted(restored_tasks.values(), key=lambda item: item.started_at, reverse=True)[0]
            self._locked_inst_id = newest_task.inst_id
            self._locked_instrument = newest_task.instrument
        if not restored_tasks:
            self._locked_inst_id = None
            self._locked_instrument = None
        self._tasks = restored_tasks
        self._reconcile_lock_state(clear_market_snapshot=True)
        self._last_persisted_payload = ""
        self._save_state(force=True)

    def _save_state(self, *, force: bool = False) -> None:
        with self._lock:
            self._reconcile_lock_state_locked(clear_market_snapshot=True)
            tasks = [self._serialize_task(task) for task in sorted(self._tasks.values(), key=lambda item: item.started_at)]
            payload = {
                "task_counter": self._task_counter,
                "locked_inst_id": self._locked_inst_id,
                "locked_instrument": _serialize_instrument(self._locked_instrument),
                "position_limit_enabled": self._position_limit_enabled,
                "long_position_limit": _serialize_decimal(self._long_position_limit),
                "short_position_limit": _serialize_decimal(self._short_position_limit),
                "tasks": tasks,
            }
        payload_key = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if not force and payload_key == self._last_persisted_payload:
            return
        save_smart_order_tasks_snapshot(
            task_counter=int(payload["task_counter"]),
            locked_inst_id=payload["locked_inst_id"],
            locked_instrument=payload["locked_instrument"],
            position_limit_enabled=bool(payload["position_limit_enabled"]),
            long_position_limit=payload["long_position_limit"],
            short_position_limit=payload["short_position_limit"],
            tasks=tasks,
            path=self._storage_path,
        )
        self._last_persisted_payload = payload_key

    def _reconcile_lock_state(self, *, clear_market_snapshot: bool = False) -> None:
        with self._lock:
            self._reconcile_lock_state_locked(clear_market_snapshot=clear_market_snapshot)

    def _reconcile_lock_state_locked(self, *, clear_market_snapshot: bool = False) -> None:
        locking_tasks = [task for task in self._tasks.values() if task.status in LOCKING_STATUSES]
        if locking_tasks:
            newest_task = sorted(locking_tasks, key=lambda item: item.started_at, reverse=True)[0]
            self._locked_inst_id = newest_task.inst_id
            self._locked_instrument = newest_task.instrument
            return

        self._locked_inst_id = None
        self._locked_instrument = None
        if clear_market_snapshot:
            self._ladder_center_price = None
            self._latest_ticker = None
            self._latest_order_book = None
            self._last_ticker_refresh_at = 0.0

    def _serialize_task(self, task: _SmartOrderTask) -> dict[str, object]:
        return {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "inst_id": task.inst_id,
            "instrument": _serialize_instrument(task.instrument),
            "runtime": {
                "environment": task.runtime.environment,
                "trade_mode": task.runtime.trade_mode,
                "position_mode": task.runtime.position_mode,
                "credential_profile_name": task.runtime.credential_profile_name,
            },
            "side": task.side,
            "size": format_decimal(task.size),
            "started_at": task.started_at.isoformat(timespec="seconds"),
            "status": task.status,
            "last_message": task.last_message,
            "active_order_id": task.active_order_id,
            "active_order_cl_ord_id": task.active_order_cl_ord_id,
            "active_order_price": _serialize_decimal(task.active_order_price),
            "active_order_size": _serialize_decimal(task.active_order_size),
            "active_order_side": task.active_order_side,
            "waiting_for_fill": task.waiting_for_fill,
            "stop_requested": task.stop_requested,
            "completed_cycles": task.completed_cycles,
            "cycle_mode": task.cycle_mode,
            "cycle_limit": task.cycle_limit,
            "initial_side": task.initial_side,
            "long_step": _serialize_decimal(task.long_step),
            "short_step": _serialize_decimal(task.short_step),
            "trigger_inst_id": task.trigger_inst_id,
            "trigger_price_type": task.trigger_price_type,
            "trigger_direction": task.trigger_direction,
            "trigger_price": _serialize_decimal(task.trigger_price),
            "exec_mode": task.exec_mode,
            "exec_price": _serialize_decimal(task.exec_price),
            "take_profit": _serialize_decimal(task.take_profit),
            "stop_loss": _serialize_decimal(task.stop_loss),
            "protection_position_side": task.protection_position_side,
            "protection_reason": task.protection_reason,
            "protection_armed": task.protection_armed,
            "last_trigger_price": _serialize_decimal(task.last_trigger_price),
            "transient_error_count": task.transient_error_count,
        }

    def _deserialize_task(self, payload: object) -> _SmartOrderTask | None:
        if not isinstance(payload, dict):
            return None
        instrument = _deserialize_instrument(payload.get("instrument"))
        if instrument is None:
            return None
        runtime_payload = payload.get("runtime")
        if not isinstance(runtime_payload, dict):
            runtime_payload = {}
        started_at_raw = payload.get("started_at")
        try:
            started_at = datetime.fromisoformat(str(started_at_raw)) if started_at_raw else datetime.now()
        except Exception:
            started_at = datetime.now()
        return _SmartOrderTask(
            task_id=str(payload.get("task_id", "")),
            task_type=str(payload.get("task_type", "condition")),  # type: ignore[arg-type]
            inst_id=str(payload.get("inst_id", instrument.inst_id)),
            instrument=instrument,
            runtime=SmartOrderRuntimeConfig(
                credentials=Credentials(api_key="", secret_key="", passphrase=""),
                environment=str(runtime_payload.get("environment", "demo")),  # type: ignore[arg-type]
                trade_mode=str(runtime_payload.get("trade_mode", "cross")),  # type: ignore[arg-type]
                position_mode=str(runtime_payload.get("position_mode", "net")),  # type: ignore[arg-type]
                credential_profile_name=str(runtime_payload.get("credential_profile_name", "")),
            ),
            side=str(payload.get("side", "buy")),  # type: ignore[arg-type]
            size=Decimal(str(payload.get("size", "0"))),
            started_at=started_at,
            status=str(payload.get("status", STATUS_RECOVERABLE)),  # type: ignore[arg-type]
            last_message=str(payload.get("last_message", "")),
            active_order_id=(str(payload["active_order_id"]) if payload.get("active_order_id") else None),
            active_order_cl_ord_id=(str(payload["active_order_cl_ord_id"]) if payload.get("active_order_cl_ord_id") else None),
            active_order_price=_deserialize_decimal(payload.get("active_order_price")),
            active_order_size=_deserialize_decimal(payload.get("active_order_size")),
            active_order_side=(str(payload["active_order_side"]) if payload.get("active_order_side") else None),  # type: ignore[arg-type]
            waiting_for_fill=bool(payload.get("waiting_for_fill", False)),
            stop_requested=bool(payload.get("stop_requested", False)),
            completed_cycles=int(payload.get("completed_cycles", 0)),
            cycle_mode=str(payload.get("cycle_mode", "continuous")),  # type: ignore[arg-type]
            cycle_limit=(int(payload["cycle_limit"]) if payload.get("cycle_limit") is not None else None),
            initial_side=str(payload.get("initial_side", payload.get("side", "buy"))),  # type: ignore[arg-type]
            long_step=_deserialize_decimal(payload.get("long_step")),
            short_step=_deserialize_decimal(payload.get("short_step")),
            trigger_inst_id=(str(payload["trigger_inst_id"]) if payload.get("trigger_inst_id") else None),
            trigger_price_type=str(payload.get("trigger_price_type", "last")),  # type: ignore[arg-type]
            trigger_direction=str(payload.get("trigger_direction", "above")),  # type: ignore[arg-type]
            trigger_price=_deserialize_decimal(payload.get("trigger_price")),
            exec_mode=str(payload.get("exec_mode", "limit")),  # type: ignore[arg-type]
            exec_price=_deserialize_decimal(payload.get("exec_price")),
            take_profit=_deserialize_decimal(payload.get("take_profit")),
            stop_loss=_deserialize_decimal(payload.get("stop_loss")),
            protection_position_side=(str(payload["protection_position_side"]) if payload.get("protection_position_side") else None),  # type: ignore[arg-type]
            protection_reason=(str(payload["protection_reason"]) if payload.get("protection_reason") else None),  # type: ignore[arg-type]
            protection_armed=bool(payload.get("protection_armed", False)),
            last_trigger_price=_deserialize_decimal(payload.get("last_trigger_price")),
            transient_error_count=int(payload.get("transient_error_count", 0)),
        )

    def _invalidate_actual_usage_cache(self) -> None:
        with self._lock:
            self._invalidate_actual_usage_cache_locked()

    def _invalidate_actual_usage_cache_locked(self) -> None:
        self._actual_usage_cache_key = None
        self._actual_usage_cache_at = 0.0
        self._actual_usage_cache_value = None

    def _get_actual_position_usage(
        self,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        *,
        force: bool = False,
    ) -> tuple[Decimal, Decimal]:
        cache_key = (
            instrument.inst_id,
            runtime.environment,
            runtime.position_mode,
            runtime.credential_profile_name,
        )
        with self._lock:
            if (
                not force
                and self._actual_usage_cache_key == cache_key
                and self._actual_usage_cache_value is not None
                and (time.time() - self._actual_usage_cache_at) < POSITION_USAGE_REFRESH_SECONDS
            ):
                return self._actual_usage_cache_value

        if instrument.inst_type == "SPOT":
            base_ccy = instrument.inst_id.split("-")[0].upper()
            overview = self._client.get_account_overview(runtime.credentials, environment=runtime.environment)
            asset = next((item for item in overview.details if item.ccy.upper() == base_ccy), None)
            actual_long = abs(asset.equity or asset.cash_balance or Decimal("0")) if asset is not None else Decimal("0")
            actual_short = Decimal("0")
        else:
            positions = self._client.get_positions(
                runtime.credentials,
                environment=runtime.environment,
                inst_type=infer_inst_type(instrument.inst_id),
            )
            relevant = [item for item in positions if item.inst_id == instrument.inst_id]
            if runtime.position_mode == "long_short":
                actual_long = sum(
                    (abs(item.position) for item in relevant if (item.pos_side or "").lower() == "long"),
                    Decimal("0"),
                )
                actual_short = sum(
                    (abs(item.position) for item in relevant if (item.pos_side or "").lower() == "short"),
                    Decimal("0"),
                )
            else:
                net_position = sum((item.position for item in relevant), Decimal("0"))
                actual_long = net_position if net_position > 0 else Decimal("0")
                actual_short = abs(net_position) if net_position < 0 else Decimal("0")

        with self._lock:
            self._actual_usage_cache_key = cache_key
            self._actual_usage_cache_at = time.time()
            self._actual_usage_cache_value = (actual_long, actual_short)
        return actual_long, actual_short

    def _get_reserved_open_usage(
        self,
        inst_id: str,
        *,
        ignore_task_id: str | None = None,
    ) -> tuple[Decimal, Decimal]:
        long_reserved = Decimal("0")
        short_reserved = Decimal("0")
        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if task.inst_id != inst_id:
                continue
            if ignore_task_id and task.task_id == ignore_task_id:
                continue
            long_part, short_part = self._reserved_open_usage_for_task(task)
            long_reserved += long_part
            short_reserved += short_part
        return long_reserved, short_reserved

    def _reserved_open_usage_for_task(self, task: _SmartOrderTask) -> tuple[Decimal, Decimal]:
        if task.task_type == "tp_sl":
            return Decimal("0"), Decimal("0")
        if task.status in TERMINAL_STATUSES | {STATUS_STOPPED, STATUS_RECOVERABLE, STATUS_POSITION_LIMIT}:
            return Decimal("0"), Decimal("0")
        opening_side = self._task_opening_side(task)
        if opening_side is None:
            return Decimal("0"), Decimal("0")
        if task.waiting_for_fill:
            if task.active_order_side != opening_side or task.active_order_size is None:
                return Decimal("0"), Decimal("0")
            return self._reserve_to_bucket(task, opening_side, task.active_order_size)
        if task.task_type == "condition" and task.status in {STATUS_READY, STATUS_WAIT_TRIGGER, STATUS_RUNNING}:
            return self._reserve_to_bucket(task, opening_side, task.size)
        return Decimal("0"), Decimal("0")

    def _reserve_to_bucket(
        self,
        task: _SmartOrderTask,
        side: Literal["buy", "sell"],
        size: Decimal,
    ) -> tuple[Decimal, Decimal]:
        if task.instrument.inst_type == "SPOT":
            return (size, Decimal("0")) if side == "buy" else (Decimal("0"), Decimal("0"))
        if task.runtime.position_mode == "long_short":
            return (size, Decimal("0")) if side == "buy" else (Decimal("0"), size)
        return (size, Decimal("0")) if side == "buy" else (Decimal("0"), size)

    def _estimate_opening_increment(
        self,
        *,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        side: Literal["buy", "sell"],
        size: Decimal,
        actual_long: Decimal,
        actual_short: Decimal,
    ) -> tuple[Decimal, Decimal]:
        if instrument.inst_type == "SPOT":
            return (size, Decimal("0")) if side == "buy" else (Decimal("0"), Decimal("0"))
        if runtime.position_mode == "long_short":
            return (size, Decimal("0")) if side == "buy" else (Decimal("0"), size)
        if side == "buy":
            reduction = min(size, actual_short)
            return size - reduction, Decimal("0")
        reduction = min(size, actual_long)
        return Decimal("0"), size - reduction

    def _task_opening_side(self, task: _SmartOrderTask) -> Literal["buy", "sell"] | None:
        if task.task_type == "tp_sl":
            return None
        if task.task_type == "grid":
            return task.initial_side
        return task.side

    def set_contract(self, instrument: Instrument) -> None:
        normalized = instrument.inst_id.strip().upper()
        with self._lock:
            self._reconcile_lock_state_locked()
            if self._locked_inst_id and self._locked_inst_id != normalized:
                raise RuntimeError(f"当前窗口已锁定 {self._locked_inst_id}，请先停止并撤掉所有任务。")
            self._locked_inst_id = normalized
            self._locked_instrument = instrument
            if self._ladder_center_price is None:
                self._ladder_center_price = instrument.tick_size
        self._save_state()

    def unlock_contract_if_idle(self) -> None:
        with self._lock:
            if any(task.status in LOCKING_STATUSES for task in self._tasks.values()):
                raise RuntimeError("仍有活动任务，不能解锁合约。")
            self._locked_inst_id = None
            self._locked_instrument = None
            self._ladder_center_price = None
            self._latest_ticker = None
            self._latest_order_book = None
            self._last_ticker_refresh_at = 0.0
        self._save_state()

    def ensure_ticker(self, instrument: Instrument, *, force: bool = False) -> OkxTicker:
        ticker, _ = self.ensure_market_snapshot(instrument, force=force)
        return ticker

    def get_cached_market_snapshot(self, instrument: Instrument) -> tuple[OkxTicker | None, OkxOrderBook | None]:
        with self._lock:
            if self._latest_ticker is None or self._latest_ticker.inst_id != instrument.inst_id:
                return None, None
            return self._latest_ticker, self._latest_order_book

    def ensure_market_snapshot(
        self,
        instrument: Instrument,
        *,
        force: bool = False,
    ) -> tuple[OkxTicker, OkxOrderBook | None]:
        with self._lock:
            current = self._latest_ticker
            current_book = self._latest_order_book
            last_refresh = self._last_ticker_refresh_at
        if not force and current is not None and current.inst_id == instrument.inst_id and (time.time() - last_refresh) < GRID_TICKER_REFRESH_SECONDS:
            return current, current_book
        ticker = self._client.get_ticker(instrument.inst_id)
        try:
            order_book = self._client.get_order_book(instrument.inst_id, depth=50)
        except Exception:
            order_book = None
        center = ticker.last or ticker.bid or ticker.ask or ticker.mark or ticker.index or instrument.tick_size
        with self._lock:
            self._latest_ticker = ticker
            self._latest_order_book = order_book
            self._ladder_center_price = center
            self._last_ticker_refresh_at = time.time()
        return ticker, order_book

    def build_ladder(
        self,
        instrument: Instrument,
        *,
        levels_each_side: int = 18,
        price_increment: Decimal | None = None,
    ) -> list[SmartOrderLadderLevel]:
        ticker, order_book = self.get_cached_market_snapshot(instrument)
        if ticker is None:
            ticker = OkxTicker(
                inst_id=instrument.inst_id,
                last=None,
                bid=None,
                ask=None,
                mark=None,
                index=None,
                raw={},
            )
            order_book = None
        display_increment = price_increment if price_increment is not None and price_increment > 0 else instrument.tick_size
        if display_increment < instrument.tick_size:
            display_increment = instrument.tick_size
        center = ticker.last or ticker.bid or ticker.ask or ticker.mark or ticker.index or instrument.tick_size
        with self._lock:
            self._ladder_center_price = center
            tasks = list(self._tasks.values())
        level_prices = build_rule_ladder_prices(
            center_price=center,
            tick_size=display_increment,
            levels_each_side=levels_each_side,
        )
        buy_map: dict[Decimal, Decimal] = {}
        sell_map: dict[Decimal, Decimal] = {}
        label_map: dict[Decimal, list[str]] = {}
        if order_book is not None:
            for price, size in order_book.bids:
                bucket = _bucket_price(price, display_increment, side="buy")
                buy_map[bucket] = buy_map.get(bucket, Decimal("0")) + size
            for price, size in order_book.asks:
                bucket = _bucket_price(price, display_increment, side="sell")
                sell_map[bucket] = sell_map.get(bucket, Decimal("0")) + size
        for task in tasks:
            if task.inst_id != instrument.inst_id:
                continue
            if not task.waiting_for_fill or task.active_order_price is None or task.active_order_size is None or task.active_order_side is None:
                continue
            price = _bucket_price(
                task.active_order_price,
                display_increment,
                side="buy" if task.active_order_side == "buy" else "sell",
            )
            label_map.setdefault(price, []).append(self._build_ladder_task_label(task))
        best_bid = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
        best_ask = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        last_price = ticker.last
        if best_bid is not None:
            best_bid = _bucket_price(best_bid, display_increment, side="buy")
        if best_ask is not None:
            best_ask = _bucket_price(best_ask, display_increment, side="sell")
        if last_price is not None:
            last_price = _bucket_price(last_price, display_increment, side="neutral")
        return [
            SmartOrderLadderLevel(
                price=price,
                buy_working=buy_map.get(price),
                sell_working=sell_map.get(price),
                working_labels=tuple(label_map.get(price, ())),
                is_last_price=(last_price == price if last_price is not None else False),
                is_best_bid=(best_bid == price if best_bid is not None else False),
                is_best_ask=(best_ask == price if best_ask is not None else False),
            )
            for price in level_prices
        ]

    def _build_ladder_task_label(self, task: _SmartOrderTask) -> str:
        side_label = "\u4e70" if task.active_order_side == "buy" else "\u5356"
        size_label = format_decimal(task.active_order_size) if task.active_order_size is not None else "-"
        return f"{task.task_id}:{side_label}{size_label}"

    def has_active_or_pending_tasks(self) -> bool:
        with self._lock:
            return any(task.status not in TERMINAL_STATUSES for task in self._tasks.values())

    def stop_task(self, task_id: str, runtime: SmartOrderRuntimeConfig | None = None) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            raise RuntimeError("未找到对应任务。")
        if runtime is not None:
            task.runtime = runtime
        task.stop_requested = True
        self._log(task, "已请求停止任务。")
        self._save_state()

    def stop_all(self, runtime: SmartOrderRuntimeConfig | None = None) -> None:
        with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            if runtime is not None:
                task.runtime = runtime
            task.stop_requested = True
            self._log(task, "已请求停止任务。")
        self._save_state()

    def remove_task(self, task_id: str) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise RuntimeError("未找到对应任务。")
            if task.status not in REMOVABLE_STATUSES:
                raise RuntimeError("当前任务仍在运行或等待中，不能直接删除，请先停止任务。")
            if task.status == STATUS_RECOVERABLE and (task.active_order_id or task.active_order_cl_ord_id):
                raise RuntimeError("待恢复任务仍带有旧委托信息，请先重新启动或人工确认旧委托后再删除。")
            del self._tasks[task_id]
            self._reconcile_lock_state_locked(clear_market_snapshot=True)
        self._save_state(force=True)

    def restart_task(self, task_id: str, runtime: SmartOrderRuntimeConfig) -> None:
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            raise RuntimeError("未找到对应任务。")
        if task.status not in {STATUS_RECOVERABLE, STATUS_STOPPED, STATUS_ERROR, STATUS_COMPLETED}:
            raise RuntimeError("当前任务仍在运行或等待中，无需重新启动。")

        self.set_contract(task.instrument)
        self._cleanup_recovery_order(task, runtime)

        task.runtime = runtime
        task.stop_requested = False
        task.transient_error_count = 0
        task.last_message = "任务已重新启动。"

        if task.task_type == "grid":
            restart_side = task.active_order_side or task.initial_side or task.side
            restart_price = task.active_order_price
            restart_size = task.active_order_size or task.size
            if restart_price is None:
                raise RuntimeError("旧网格任务缺少待挂价格，无法重新启动。")
            self._submit_limit_order(task, side=restart_side, price=restart_price, size=restart_size, message_prefix="网格恢复委托")
            return

        task.active_order_id = None
        task.active_order_cl_ord_id = None
        task.active_order_price = None
        task.active_order_size = None
        task.active_order_side = None
        task.waiting_for_fill = False

        if task.task_type == "condition":
            task.status = STATUS_WAIT_TRIGGER
            task.last_message = "条件单已恢复，等待触发。"
            self._log(task, task.last_message)
            self._save_state()
            return

        task.status = STATUS_WAIT_TP_SL
        task.last_message = "止盈止损任务已恢复，等待触发。"
        self._log(task, task.last_message)
        self._save_state()

    def close_all_and_unlock(self, runtime: SmartOrderRuntimeConfig | None = None) -> None:
        self.stop_all(runtime)
        deadline = time.time() + 15.0
        while time.time() < deadline:
            if not self.has_active_or_pending_tasks():
                self.unlock_contract_if_idle()
                return
            time.sleep(0.35)
        with self._lock:
            unresolved = [task.task_id for task in self._tasks.values() if task.status not in TERMINAL_STATUSES]
        raise RuntimeError(f"仍有任务/委托未完全结束：{', '.join(unresolved)}")

    def destroy(self) -> None:
        self._stop_event.set()
        self._save_state(force=True)
        if self._worker.is_alive():
            self._worker.join(timeout=2.0)

    def _cleanup_recovery_order(self, task: _SmartOrderTask, runtime: SmartOrderRuntimeConfig) -> None:
        if not task.active_order_id and not task.active_order_cl_ord_id:
            return
        status = self._client.get_order(
            runtime.credentials,
            self._build_config(task.inst_id, runtime),
            inst_id=task.inst_id,
            ord_id=task.active_order_id,
            cl_ord_id=task.active_order_cl_ord_id,
        )
        if status.state == "filled":
            raise RuntimeError("旧委托已成交，请先检查仓位和委托后再人工处理。")
        if status.state in {"live", "partially_filled"} and task.active_order_id:
            self._client.cancel_order(
                runtime.credentials,
                self._build_config(task.inst_id, runtime),
                inst_id=task.inst_id,
                ord_id=task.active_order_id,
            )
        task.active_order_id = None
        task.active_order_cl_ord_id = None
        task.waiting_for_fill = False

    def start_grid_task(
        self,
        *,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        side: Literal["buy", "sell"],
        entry_price: Decimal,
        size: Decimal,
        long_step: Decimal,
        short_step: Decimal,
        cycle_mode: CycleMode,
        cycle_limit: int | None,
    ) -> str:
        self.set_contract(instrument)
        order_price = snap_to_increment(entry_price, instrument.tick_size, "up" if side == "buy" else "down")
        order_size = snap_to_increment(size, instrument.lot_size, "down")
        self.validate_opening_capacity(
            instrument=instrument,
            runtime=runtime,
            side=side,
            size=order_size,
        )
        if order_size < instrument.min_size:
            raise RuntimeError(f"下单数量 {format_decimal(order_size)} 小于最小下单量 {format_decimal(instrument.min_size)}。")
        with self._lock:
            for existing in self._tasks.values():
                if (
                    existing.task_type == "grid"
                    and existing.inst_id == instrument.inst_id
                    and existing.side == side
                    and existing.active_order_price == order_price
                    and existing.active_order_size == order_size
                    and existing.status in {"准备中", "等待成交", "运行中", "等待止盈止损"}
                ):
                    raise RuntimeError(f"{format_decimal_by_increment(order_price, instrument.tick_size)} 的同方向同数量网格任务已存在。")
            self._task_counter += 1
            task_id = f"G{self._task_counter:03d}"
            task = _SmartOrderTask(
                task_id=task_id,
                task_type="grid",
                inst_id=instrument.inst_id,
                instrument=instrument,
                runtime=runtime,
                side=side,
                size=order_size,
                initial_side=side,
                long_step=long_step,
                short_step=short_step,
                cycle_mode=cycle_mode,
                cycle_limit=cycle_limit,
            )
            self._tasks[task_id] = task
        self._save_state()
        self._submit_limit_order(task, side=side, price=order_price, size=order_size, message_prefix="网格起始委托")
        return task_id

    def start_condition_task(
        self,
        *,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        side: Literal["buy", "sell"],
        size: Decimal,
        trigger_inst_id: str,
        trigger_price_type: TriggerPriceType,
        trigger_direction: TriggerDirection,
        trigger_price: Decimal,
        exec_mode: ExecutionMode,
        exec_price: Decimal | None,
        take_profit: Decimal | None,
        stop_loss: Decimal | None,
    ) -> str:
        self.set_contract(instrument)
        order_size = snap_to_increment(size, instrument.lot_size, "down")
        self.validate_opening_capacity(
            instrument=instrument,
            runtime=runtime,
            side=side,
            size=order_size,
        )
        if order_size < instrument.min_size:
            raise RuntimeError(f"下单数量 {format_decimal(order_size)} 小于最小下单量 {format_decimal(instrument.min_size)}。")
        if instrument.inst_type == "SPOT" and side == "sell" and (take_profit is not None or stop_loss is not None):
            raise RuntimeError("现货卖出条件单第一版不支持附带止盈止损，请先只做条件卖出。")
        with self._lock:
            self._task_counter += 1
            task_id = f"C{self._task_counter:03d}"
            task = _SmartOrderTask(
                task_id=task_id,
                task_type="condition",
                inst_id=instrument.inst_id,
                instrument=instrument,
                runtime=runtime,
                side=side,
                size=order_size,
                status="等待触发",
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
                trigger_direction=trigger_direction,
                trigger_price=trigger_price,
                exec_mode=exec_mode,
                exec_price=exec_price,
                take_profit=take_profit,
                stop_loss=stop_loss,
            )
            self._tasks[task_id] = task
        self._log(task, f"条件单已创建 | 触发源={trigger_inst_id}/{trigger_price_type} | 方向={trigger_direction} | 触发价={format_decimal(trigger_price)}")
        self._save_state()
        return task_id

    def start_tp_sl_task(
        self,
        *,
        instrument: Instrument,
        runtime: SmartOrderRuntimeConfig,
        position_side: Literal["long", "short"],
        size: Decimal,
        trigger_inst_id: str,
        trigger_price_type: TriggerPriceType,
        take_profit: Decimal | None,
        stop_loss: Decimal | None,
    ) -> str:
        self.set_contract(instrument)
        if take_profit is None and stop_loss is None:
            raise RuntimeError("止盈和止损至少要填写一个。")
        if instrument.inst_type == "SPOT" and position_side == "short":
            raise RuntimeError("现货不支持空头止盈止损。")
        order_size = snap_to_increment(size, instrument.lot_size, "down")
        if order_size < instrument.min_size:
            raise RuntimeError(f"保护数量 {format_decimal(order_size)} 小于最小下单量 {format_decimal(instrument.min_size)}。")
        with self._lock:
            self._task_counter += 1
            task_id = f"T{self._task_counter:03d}"
            task = _SmartOrderTask(
                task_id=task_id,
                task_type="tp_sl",
                inst_id=instrument.inst_id,
                instrument=instrument,
                runtime=runtime,
                side="sell" if position_side == "long" else "buy",
                size=order_size,
                status="等待止盈止损",
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
                take_profit=take_profit,
                stop_loss=stop_loss,
                protection_position_side=position_side,
                protection_armed=True,
            )
            self._tasks[task_id] = task
        self._log(task, f"止盈止损任务已创建 | 触发源={trigger_inst_id}/{trigger_price_type} | 止盈={_fmt_optional(take_profit)} | 止损={_fmt_optional(stop_loss)}")
        self._save_state()
        return task_id

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._refresh_locked_market_snapshot_if_due()
            with self._lock:
                tasks = list(self._tasks.values())
            for task in tasks:
                try:
                    self._tick_task(task)
                except Exception as exc:  # noqa: BLE001
                    if self._is_transient_error(exc):
                        task.transient_error_count += 1
                        task.status = STATUS_RUNNING
                        task.last_message = f"网络重试中：{exc}"
                        self._log(task, task.last_message)
                        self._save_state()
                        continue
                    task.status = STATUS_ERROR
                    task.last_message = f"任务异常：{exc}"
                    self._log(task, task.last_message)
                    self._save_state()
            time.sleep(WORKER_POLL_SECONDS)

    def _refresh_locked_market_snapshot_if_due(self) -> None:
        with self._lock:
            instrument = self._locked_instrument
            last_refresh = self._last_ticker_refresh_at
            has_snapshot = self._latest_ticker is not None and instrument is not None and self._latest_ticker.inst_id == instrument.inst_id
        if instrument is None:
            return
        if has_snapshot and (time.time() - last_refresh) < GRID_TICKER_REFRESH_SECONDS:
            return
        try:
            self.ensure_market_snapshot(instrument, force=not has_snapshot)
        except Exception:
            return

    def _tick_task(self, task: _SmartOrderTask) -> None:
        if task.status in TERMINAL_STATUSES:
            return
        if task.status == STATUS_RECOVERABLE:
            if task.stop_requested:
                self._handle_stop(task)
            return
        if task.status == STATUS_POSITION_LIMIT:
            self._try_resume_position_limited_task(task)
            return
        if task.stop_requested:
            self._handle_stop(task)
            return
        self._enforce_position_limit(task)
        if task.status == STATUS_POSITION_LIMIT:
            return
        if task.waiting_for_fill and task.active_order_id:
            self._poll_active_order(task)
            return
        if task.task_type == "condition":
            self._tick_condition_task(task)
            return
        if task.task_type == "tp_sl":
            self._tick_protection_task(task)
            return

    def _enforce_position_limit(self, task: _SmartOrderTask) -> None:
        if not self._position_limit_enabled or task.task_type == "tp_sl":
            return
        opening_side = self._task_opening_side(task)
        if opening_side is None:
            return
        if task.waiting_for_fill and task.active_order_side == opening_side and task.active_order_size is not None:
            try:
                self.validate_opening_capacity(
                    instrument=task.instrument,
                    runtime=task.runtime,
                    side=opening_side,
                    size=task.active_order_size,
                    ignore_task_id=task.task_id,
                )
            except RuntimeError as exc:
                self._cancel_opening_order_for_position_limit(task, str(exc))
            return
        if task.task_type == "condition" and task.status in {STATUS_READY, STATUS_WAIT_TRIGGER, STATUS_RUNNING}:
            try:
                self.validate_opening_capacity(
                    instrument=task.instrument,
                    runtime=task.runtime,
                    side=opening_side,
                    size=task.size,
                    ignore_task_id=task.task_id,
                )
            except RuntimeError as exc:
                self._freeze_task_for_position_limit(task, str(exc))

    def _cancel_opening_order_for_position_limit(self, task: _SmartOrderTask, reason: str) -> None:
        status = self._client.get_order(
            task.runtime.credentials,
            self._build_config(task.inst_id, task.runtime),
            inst_id=task.inst_id,
            ord_id=task.active_order_id,
            cl_ord_id=task.active_order_cl_ord_id,
        )
        ord_id = status.ord_id or task.active_order_id
        if not ord_id:
            raise RuntimeError("仓位限制触发，但无法确认活动开仓单编号。")
        self._client.cancel_order(
            task.runtime.credentials,
            self._build_config(task.inst_id, task.runtime),
            inst_id=task.inst_id,
            ord_id=ord_id,
        )
        order_size = status.size or task.active_order_size or task.size
        filled_size = status.filled_size or Decimal("0")
        remaining_size = order_size - filled_size
        task.active_order_size = remaining_size if remaining_size > 0 else Decimal("0")
        task.active_order_id = None
        task.active_order_cl_ord_id = None
        task.waiting_for_fill = False
        self._freeze_task_for_position_limit(task, reason)

    def _try_resume_position_limited_task(self, task: _SmartOrderTask) -> None:
        if not self._position_limit_enabled:
            if task.task_type == "condition":
                task.status = STATUS_WAIT_TRIGGER
                task.last_message = "仓位限制已关闭，条件单恢复等待触发。"
                return
            if task.task_type == "grid":
                self._resume_frozen_grid_task(task)
                return
            return
        opening_side = self._task_opening_side(task)
        if opening_side is None:
            return
        size = task.active_order_size if task.active_order_size is not None else task.size
        try:
            self.validate_opening_capacity(
                instrument=task.instrument,
                runtime=task.runtime,
                side=opening_side,
                size=size,
                ignore_task_id=task.task_id,
            )
        except RuntimeError:
            return
        if task.task_type == "condition":
            task.status = STATUS_WAIT_TRIGGER
            task.last_message = "仓位限制解除，条件单恢复等待触发。"
            return
        if task.task_type == "grid":
            self._resume_frozen_grid_task(task)

    def _resume_frozen_grid_task(self, task: _SmartOrderTask) -> None:
        restart_side = task.active_order_side or task.initial_side
        restart_price = task.active_order_price
        restart_size = task.active_order_size if task.active_order_size is not None else task.size
        if restart_size <= 0:
            task.status = STATUS_COMPLETED
            task.last_message = "超限冻结的开仓单已无剩余数量，任务已结束。"
            self._log(task, task.last_message)
            self._save_state()
            return
        if restart_price is None:
            task.status = STATUS_ERROR
            task.last_message = "超限冻结后的网格任务缺少恢复价格，已转为异常。"
            self._log(task, task.last_message)
            self._save_state()
            return
        self._submit_limit_order(
            task,
            side=restart_side,
            price=restart_price,
            size=restart_size,
            message_prefix="超限解除后恢复网格委托",
        )

    def _freeze_task_for_position_limit(self, task: _SmartOrderTask, reason: str) -> None:
        task.status = STATUS_POSITION_LIMIT
        task.last_message = f"仓位限制触发，任务已冻结：{reason}"
        self._log(task, task.last_message)
        self._save_state()

    def _tick_condition_task(self, task: _SmartOrderTask) -> None:
        current_price = self._client.get_trigger_price(task.trigger_inst_id or task.inst_id, task.trigger_price_type)
        task.last_trigger_price = current_price
        if not self._is_trigger_hit(current_price, task.trigger_direction, task.trigger_price):
            task.status = STATUS_WAIT_TRIGGER
            task.last_message = f"等待触发 | 当前价={format_decimal(current_price)}"
            return
        task.status = STATUS_RUNNING
        task.last_message = f"条件单已触发 | 当前价={format_decimal(current_price)}"
        self._log(task, task.last_message)
        self._submit_execution_order(task)

    def _tick_protection_task(self, task: _SmartOrderTask) -> None:
        current_price = self._client.get_trigger_price(task.trigger_inst_id or task.inst_id, task.trigger_price_type)
        task.last_trigger_price = current_price
        uses_underlying_trigger = not (
            (task.trigger_inst_id or task.inst_id).strip().upper() == task.inst_id.strip().upper()
            and task.trigger_price_type == "mark"
        )
        stop_hit, take_hit = evaluate_protection_trigger(
            direction=task.protection_position_side or "long",
            current_price=current_price,
            stop_loss=task.stop_loss,
            take_profit=task.take_profit,
            option_inst_id=task.inst_id if task.instrument.inst_type == "OPTION" else None,
            uses_underlying_trigger=uses_underlying_trigger if task.instrument.inst_type == "OPTION" else False,
        )
        if not stop_hit and not take_hit:
            task.status = STATUS_WAIT_TP_SL
            task.last_message = f"保护监控中 | 当前价={format_decimal(current_price)}"
            return
        task.protection_reason = "止损" if stop_hit else "止盈"
        self._log(task, f"{task.protection_reason}已触发 | 当前价={format_decimal(current_price)}")
        self._submit_protection_close_order(task)

    def _poll_active_order(self, task: _SmartOrderTask) -> None:
        status = self._client.get_order(
            task.runtime.credentials,
            self._build_config(task.inst_id, task.runtime),
            inst_id=task.inst_id,
            ord_id=task.active_order_id,
            cl_ord_id=task.active_order_cl_ord_id,
        )
        task.last_message = self._summarize_order_status(status)
        if status.state in {"live", "partially_filled"}:
            task.status = STATUS_WAIT_FILL
            return
        if status.state == "canceled":
            if task.stop_requested:
                task.status = STATUS_STOPPED
                task.waiting_for_fill = False
                task.active_order_id = None
                task.active_order_cl_ord_id = None
                task.active_order_price = None
                task.active_order_size = None
                task.active_order_side = None
                task.last_message = "任务已停止并撤单。"
                self._log(task, task.last_message)
                self._save_state()
                return
            raise RuntimeError(f"活动委托被取消：{task.active_order_id}")
        if status.state != "filled":
            raise RuntimeError(f"订单状态异常：{status.state}")
        self._handle_order_filled(task, status)

    def _handle_order_filled(self, task: _SmartOrderTask, status: OkxOrderStatus) -> None:
        fill_price = status.avg_price or status.price or task.active_order_price
        fill_size = status.filled_size or status.size or task.active_order_size
        filled_side = task.active_order_side or task.side
        self._invalidate_actual_usage_cache()
        task.waiting_for_fill = False
        task.active_order_id = None
        task.active_order_cl_ord_id = None
        task.active_order_price = None
        task.active_order_size = None
        task.active_order_side = None
        if fill_price is None or fill_size is None:
            raise RuntimeError("订单成交后拿不到价格或数量。")
        self._log(task, f"委托成交 | 方向={filled_side} | 成交价={format_decimal_by_increment(fill_price, task.instrument.tick_size)} | 成交量={format_decimal(fill_size)}")
        if task.task_type == "grid":
            if task.initial_side == "buy" and filled_side == "sell":
                task.completed_cycles += 1
            elif task.initial_side == "sell" and filled_side == "buy":
                task.completed_cycles += 1
            if task.cycle_mode == "counted" and task.cycle_limit is not None and task.completed_cycles >= task.cycle_limit:
                task.status = STATUS_COMPLETED
                task.last_message = f"网格循环完成，共 {task.completed_cycles} 轮。"
                self._log(task, task.last_message)
                self._save_state()
                return
            next_side, next_price = compute_next_grid_order_price(
                filled_side=filled_side,
                fill_price=fill_price,
                long_step=task.long_step or Decimal("0"),
                short_step=task.short_step or Decimal("0"),
                tick_size=task.instrument.tick_size,
            )
            self._submit_limit_order(task, side=next_side, price=next_price, size=task.size, message_prefix="网格反向委托")
            return
        if task.task_type == "condition":
            if task.take_profit is None and task.stop_loss is None:
                task.status = STATUS_COMPLETED
                task.last_message = "条件单已成交完成。"
                self._log(task, task.last_message)
                self._save_state()
                return
            if task.instrument.inst_type == "SPOT" and filled_side == "sell":
                task.status = STATUS_COMPLETED
                task.last_message = "现货卖出条件单已成交，未附带止盈止损。"
                self._log(task, task.last_message)
                self._save_state()
                return
            task.status = STATUS_WAIT_TP_SL
            task.protection_armed = True
            task.protection_position_side = "long" if filled_side == "buy" else "short"
            task.last_message = "条件单已成交，转入止盈止损监控。"
            self._log(task, task.last_message)
            self._save_state()
            return
        if task.task_type == "tp_sl":
            task.status = STATUS_COMPLETED
            reason = task.protection_reason or "止盈止损"
            task.last_message = f"{reason}平仓已完成。"
            self._log(task, task.last_message)
            self._save_state()

    def _submit_execution_order(self, task: _SmartOrderTask) -> None:
        if task.exec_mode == "limit":
            if task.exec_price is None or task.exec_price <= 0:
                raise RuntimeError("限价触发单必须填写下单价格。")
            self._submit_limit_order(task, side=task.side, price=task.exec_price, size=task.size, message_prefix="条件单触发下单", pos_side=self._open_pos_side(task))
            return
        self._submit_aggressive_order(task, side=task.side, size=task.size, message_prefix="条件单触发下单", pos_side=self._open_pos_side(task))

    def _submit_protection_close_order(self, task: _SmartOrderTask) -> None:
        close_size, close_side, pos_side = self._resolve_close_order(task)
        if close_size <= 0:
            task.status = STATUS_COMPLETED
            task.last_message = "未发现剩余仓位，保护任务结束。"
            self._log(task, task.last_message)
            self._save_state()
            return
        if task.instrument.inst_type == "OPTION":
            ticker = self._client.get_ticker(task.inst_id)
            aggressive_price = self._pick_aggressive_ioc_price(task.instrument, ticker, close_side)
            validate_live_protection_order_price_guard(
                client=self._client,
                option_inst_id=task.inst_id,
                close_side=close_side,
                order_price=aggressive_price,
                tick_size=task.instrument.tick_size,
                open_avg_price=None,
                ticker=ticker,
            )
        self._submit_aggressive_order(task, side=close_side, size=close_size, pos_side=pos_side, message_prefix=f"{task.protection_reason or '止盈止损'}平仓")

    def _submit_limit_order(
        self,
        task: _SmartOrderTask,
        *,
        side: Literal["buy", "sell"],
        price: Decimal,
        size: Decimal,
        message_prefix: str,
        pos_side: Literal["long", "short"] | None = None,
    ) -> None:
        opening_side = self._task_opening_side(task)
        if side == opening_side:
            self.validate_opening_capacity(
                instrument=task.instrument,
                runtime=task.runtime,
                side=side,
                size=size,
                ignore_task_id=task.task_id,
            )
        cl_ord_id = f"so{task.task_id.lower()}{int(time.time() * 1000) % 1000000:06d}"
        result = self._client.place_simple_order(
            task.runtime.credentials,
            self._build_config(task.inst_id, task.runtime),
            inst_id=task.inst_id,
            side=side,
            size=size,
            ord_type="limit",
            pos_side=pos_side,
            price=price,
            cl_ord_id=cl_ord_id,
        )
        task.active_order_id = result.ord_id
        task.active_order_cl_ord_id = result.cl_ord_id or cl_ord_id
        task.active_order_price = price
        task.active_order_size = size
        task.active_order_side = side
        task.waiting_for_fill = True
        task.status = STATUS_WAIT_FILL
        task.last_message = f"{message_prefix}已提交 | {side} {format_decimal_by_increment(price, task.instrument.tick_size)}"
        self._log(task, f"{message_prefix} | 方向={side} | 价格={format_decimal_by_increment(price, task.instrument.tick_size)} | 数量={format_decimal(size)} | ordId={result.ord_id}")
        self._save_state()

    def _submit_aggressive_order(
        self,
        task: _SmartOrderTask,
        *,
        side: Literal["buy", "sell"],
        size: Decimal,
        message_prefix: str,
        pos_side: Literal["long", "short"] | None = None,
    ) -> None:
        opening_side = self._task_opening_side(task)
        if side == opening_side:
            self.validate_opening_capacity(
                instrument=task.instrument,
                runtime=task.runtime,
                side=side,
                size=size,
                ignore_task_id=task.task_id,
            )
        result = self._client.place_aggressive_limit_order(
            task.runtime.credentials,
            self._build_config(task.inst_id, task.runtime),
            task.instrument,
            side=side,
            size=size,
            pos_side=pos_side,
        )
        task.active_order_id = result.ord_id
        task.active_order_cl_ord_id = result.cl_ord_id
        task.active_order_size = size
        task.active_order_side = side
        task.waiting_for_fill = True
        task.status = STATUS_WAIT_FILL
        task.last_message = f"{message_prefix}已提交 | {side} | 数量={format_decimal(size)}"
        self._log(task, f"{message_prefix} | 方向={side} | 数量={format_decimal(size)} | ordId={result.ord_id}")
        self._save_state()

    def _handle_stop(self, task: _SmartOrderTask) -> None:
        if task.active_order_id or task.active_order_cl_ord_id:
            try:
                ord_id = task.active_order_id
                if not ord_id and task.active_order_cl_ord_id:
                    status = self._client.get_order(
                        task.runtime.credentials,
                        self._build_config(task.inst_id, task.runtime),
                        inst_id=task.inst_id,
                        cl_ord_id=task.active_order_cl_ord_id,
                    )
                    ord_id = status.ord_id
                if not ord_id:
                    raise RuntimeError("无法确认旧委托编号，不能安全撤单。")
                self._client.cancel_order(
                    task.runtime.credentials,
                    self._build_config(task.inst_id, task.runtime),
                    inst_id=task.inst_id,
                    ord_id=ord_id,
                )
            except Exception as exc:  # noqa: BLE001
                if self._is_transient_error(exc):
                    task.last_message = f"撤单重试中：{exc}"
                    return
                raise
            task.waiting_for_fill = False
            task.active_order_id = None
            task.active_order_cl_ord_id = None
            task.active_order_price = None
            task.active_order_size = None
            task.active_order_side = None
        task.status = STATUS_STOPPED
        task.last_message = "任务已停止。"
        self._save_state()

    def _resolve_close_order(self, task: _SmartOrderTask) -> tuple[Decimal, Literal["buy", "sell"], Literal["long", "short"] | None]:
        position_side = task.protection_position_side or ("long" if task.side == "buy" else "short")
        close_side: Literal["buy", "sell"] = "sell" if position_side == "long" else "buy"
        pos_side: Literal["long", "short"] | None = None
        if task.instrument.inst_type == "SPOT":
            base_ccy = task.inst_id.split("-")[0]
            overview = self._client.get_account_overview(task.runtime.credentials, environment=task.runtime.environment)
            asset = next((item for item in overview.details if item.ccy.upper() == base_ccy.upper()), None)
            available = asset.available_balance or asset.equity if asset is not None else Decimal("0")
            resolved = snap_to_increment(min(task.size, available), task.instrument.lot_size, "down")
            return resolved, close_side, None
        positions = self._client.get_positions(
            task.runtime.credentials,
            environment=task.runtime.environment,
            inst_type=infer_inst_type(task.inst_id),
        )
        relevant = [item for item in positions if item.inst_id == task.inst_id]
        available = Decimal("0")
        if task.runtime.position_mode == "long_short":
            pos_side = position_side
            for item in relevant:
                if item.pos_side.lower() == position_side:
                    available = max(available, abs(item.avail_position or item.position))
        else:
            net_position = sum((item.position for item in relevant), Decimal("0"))
            if position_side == "long" and net_position > 0:
                available = net_position
            elif position_side == "short" and net_position < 0:
                available = abs(net_position)
        resolved = snap_to_increment(min(task.size, available), task.instrument.lot_size, "down")
        return resolved, close_side, pos_side

    def _pick_aggressive_ioc_price(self, instrument: Instrument, ticker: OkxTicker, side: Literal["buy", "sell"]) -> Decimal:
        base_price = ticker.ask if side == "buy" else ticker.bid
        base_price = base_price or ticker.last or ticker.mark or ticker.index
        if base_price is None or base_price <= 0:
            raise RuntimeError(f"{instrument.inst_id} 当前拿不到有效盘口/成交价。")
        if side == "buy":
            return snap_to_increment(base_price + (instrument.tick_size * 2), instrument.tick_size, "up")
        candidate = base_price - (instrument.tick_size * 2)
        if candidate <= 0:
            candidate = instrument.tick_size
        return snap_to_increment(candidate, instrument.tick_size, "down")

    def _build_trigger_label(self, task: _SmartOrderTask) -> str:
        if task.task_type == "grid":
            return "规则盘口点击网格"
        if task.task_type == "condition":
            return f"{task.trigger_inst_id or task.inst_id} {task.trigger_price_type}"
        return f"{task.trigger_inst_id or task.inst_id} 止盈止损"

    def _is_trigger_hit(self, current_price: Decimal, trigger_direction: TriggerDirection, trigger_price: Decimal | None) -> bool:
        if trigger_price is None:
            return False
        return current_price >= trigger_price if trigger_direction == "above" else current_price <= trigger_price

    def _open_pos_side(self, task: _SmartOrderTask) -> Literal["long", "short"] | None:
        if task.instrument.inst_type == "SPOT":
            return None
        if task.runtime.position_mode != "long_short":
            return None
        return "long" if task.side == "buy" else "short"

    def _build_config(self, inst_id: str, runtime: SmartOrderRuntimeConfig) -> StrategyConfig:
        return StrategyConfig(
            inst_id=inst_id,
            bar="1m",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=Decimal("1"),
            trade_mode=runtime.trade_mode,
            signal_mode="long_only",
            position_mode=runtime.position_mode,
            environment=runtime.environment,
            tp_sl_trigger_type="last",
            strategy_id="smart_order_runtime",
        )

    def _summarize_order_status(self, status: OkxOrderStatus) -> str:
        return f"订单监控 | 状态={status.state} | 价格={_fmt_optional(status.price)} | 成交均价={_fmt_optional(status.avg_price)} | 已成交={_fmt_optional(status.filled_size)}"

    def _log(self, task: _SmartOrderTask, message: str) -> None:
        line = ensure_log_timestamp(f"[无限下单 {task.task_id}] {message}")
        with self._lock:
            self._logs.append(line)
        if self._logger is not None:
            self._logger(line)

    def _is_transient_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        return any(
            token in text
            for token in (
                "timed out",
                "handshake",
                "ssl",
                "connection reset",
                "connection aborted",
                "connection refused",
                "read timed out",
                "temporarily unavailable",
                "502",
                "503",
                "504",
            )
        )
