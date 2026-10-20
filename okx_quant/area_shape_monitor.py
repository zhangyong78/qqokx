"""Independent public-market monitor for local area-shape signals and paper trades."""

from __future__ import annotations

import queue
import threading
import time
from typing import Any, Callable

from PySide6.QtCore import QObject, Signal

from okx_quant.area_shape_store import load_area_shape_plans, save_area_shape_plans
from okx_quant.area_shape_trade import AreaShapePlan
from okx_quant.models import Candle
from okx_quant.okx_candle_ws import CandleStreamKey
from okx_quant.okx_client import OkxRestClient


_ACTIVE_STATUSES = {"monitoring", "touched", "paper_pending", "paper_position", "paper_break_even", "paper_locked"}


class AreaShapeMonitor(QObject):
    """Owns 1H public candle subscriptions independently of Kline page visibility."""

    plan_updated = Signal(object)
    status_changed = Signal(str)

    def __init__(
        self, parent: QObject | None = None, *, client_factory: Callable[[], Any] = OkxRestClient,
        store_load: Callable[[], list[dict[str, object]]] = load_area_shape_plans,
        store_save: Callable[[list[dict[str, object]]], Any] = save_area_shape_plans,
    ) -> None:
        super().__init__(parent)
        self._lock = threading.RLock()
        self._plans: dict[str, AreaShapePlan] = {}
        self._load_errors: list[str] = []
        self._store_write_blocked = False
        try:
            records = store_load()
        except Exception as exc:
            records = []
            self._load_errors.append(f"计划文件读取失败：{exc}")
            self._store_write_blocked = True
        for payload in records:
            try:
                plan = AreaShapePlan.from_dict(payload)
            except (TypeError, ValueError, KeyError) as exc:
                plan_id = payload.get("plan_id", "未知计划") if isinstance(payload, dict) else "未知计划"
                self._load_errors.append(f"{plan_id}: {exc}")
                self._store_write_blocked = True
                continue
            self._plans[plan.plan_id] = plan
        if self._store_write_blocked:
            for plan in self._plans.values():
                plan.status = "manual_review"
                plan._event("store_manual_review", "区域计划文件含损坏或未知记录，已停止所有自动恢复与写入；修复/核对存储文件后再启动")
        self._client_factory = client_factory
        self._store_save = store_save
        self._clients: dict[str, Any] = {}
        self._subscriptions: dict[tuple[str, str], Callable[[], None]] = {}
        self._candles: dict[tuple[str, str], list[Candle]] = {}
        self._last_live_prices: dict[tuple[str, str, str], Any] = {}
        self._catchup_required: set[tuple[str, str]] = set()
        self._last_fallback_poll: dict[tuple[str, str], float] = {}
        self._last_reconcile: dict[tuple[str, str], float] = {}
        self._queue: queue.Queue[tuple[str, Candle] | tuple[str, None]] = queue.Queue(maxsize=3000)
        self._stop_event = threading.Event()
        self._worker: threading.Thread | None = None

    def start(self) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop_event.clear()
            self._worker = threading.Thread(target=self._run, daemon=True, name="qqokx-area-shape-monitor")
            self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            self._queue.put_nowait(("__stop__", None))
        except queue.Full:
            pass
        for unsubscribe in list(self._subscriptions.values()):
            try:
                unsubscribe()
            except Exception:
                pass
        self._subscriptions.clear()
        if self._worker is not None:
            self._worker.join(timeout=3.0)
        for client in self._clients.values():
            try:
                client.close()
            except Exception:
                pass
        self._clients.clear()

    def plans(self) -> list[AreaShapePlan]:
        with self._lock:
            return [AreaShapePlan.from_dict(item.to_dict()) for item in self._plans.values()]

    def get_plan(self, plan_id: str) -> AreaShapePlan | None:
        with self._lock:
            plan = self._plans.get(plan_id)
            return AreaShapePlan.from_dict(plan.to_dict()) if plan is not None else None

    def save_draft(self, plan: AreaShapePlan) -> None:
        if plan.status != "draft":
            raise ValueError("only draft plans can be edited")
        with self._lock:
            self._require_writable_store()
            existing = self._plans.get(plan.plan_id)
            if existing is not None and existing.status != "draft":
                raise ValueError("a stale draft cannot overwrite an active or finished plan")
            if plan.execution_enabled:
                raise ValueError("area plan exchange execution is disabled")
            self._plans[plan.plan_id] = AreaShapePlan.from_dict(plan.to_dict())
            try:
                self._persist_locked()
            except Exception:
                if existing is None:
                    self._plans.pop(plan.plan_id, None)
                else:
                    self._plans[plan.plan_id] = existing
                raise

    def start_plan(
        self, plan_id: str, *, last_closed_candle_ts: int, profile_name: str, environment: str,
        execution_mode: str = "paper",
    ) -> AreaShapePlan:
        if execution_mode not in {"paper", "demo_exchange"}:
            raise ValueError("unsupported area execution mode")
        with self._lock:
            self._require_writable_store()
            plan = self._plans[plan_id]
            plan.activate(last_closed_candle_ts=last_closed_candle_ts, profile_name=profile_name, environment=environment)
            # Persist the routing mode atomically with activation.  A candle
            # already queued for the paper worker must observe demo_exchange
            # and skip paper mutations while asynchronous demo preflight runs.
            plan.execution_mode = execution_mode
            self._persist_locked()
            snapshot = AreaShapePlan.from_dict(plan.to_dict())
        self.plan_updated.emit(snapshot.to_dict())
        try:
            self._queue.put_nowait(("__reload__", None))
        except queue.Full:
            self.status_changed.emit("区域形态行情队列已满，监控稍后重建")
        return snapshot

    def mark_demo_execution(self, plan_id: str) -> AreaShapePlan:
        """Route an already monitoring plan away from paper state mutation."""
        with self._lock:
            self._require_writable_store()
            plan = self._plans[plan_id]
            if plan.execution_mode == "demo_exchange" and plan.status in {"monitoring", "touched"} and plan.paper_position is None:
                return AreaShapePlan.from_dict(plan.to_dict())
            if plan.status not in {"monitoring", "touched"} or plan.paper_position is not None:
                raise ValueError("只有无纸面持仓/待入场的监控计划能切换到独立demo执行")
            plan.execution_mode = "demo_exchange"
            self._persist_locked()
            snapshot = AreaShapePlan.from_dict(plan.to_dict())
        self.plan_updated.emit(snapshot.to_dict())
        return snapshot

    def pause_plan(self, plan_id: str) -> None:
        with self._lock:
            self._require_writable_store()
            plan = self._plans[plan_id]
            plan.pause()
            self._persist_locked()
            snapshot = AreaShapePlan.from_dict(plan.to_dict())
        self.plan_updated.emit(snapshot.to_dict())
        try:
            self._queue.put_nowait(("__reload__", None))
        except queue.Full:
            pass

    def cancel_plan(self, plan_id: str) -> None:
        with self._lock:
            self._require_writable_store()
            plan = self._plans[plan_id]
            plan.cancel()
            self._persist_locked()
            snapshot = AreaShapePlan.from_dict(plan.to_dict())
        self.plan_updated.emit(snapshot.to_dict())
        try:
            self._queue.put_nowait(("__reload__", None))
        except queue.Full:
            pass

    def _persist_locked(self) -> None:
        self._require_writable_store()
        self._store_save([plan.to_dict() for plan in self._plans.values()])

    def _require_writable_store(self) -> None:
        if self._store_write_blocked:
            raise RuntimeError("区域计划存储包含损坏或未知记录；为保护原文件，当前禁止新增、修改与后台状态写入")

    def _run(self) -> None:
        self.status_changed.emit("区域形态后台监控启动")
        if self._load_errors:
            self.status_changed.emit(f"有 {len(self._load_errors)} 条区域计划需人工核对，未恢复运行")
        try:
            self._rebuild_connections()
        except Exception as exc:
            self.status_changed.emit(f"区域形态监控连接失败，稍后重试：{exc}")
        retry_at = time.monotonic() + 10.0
        while not self._stop_event.wait(0.25):
            for _ in range(128):
                try:
                    key, candle = self._queue.get_nowait()
                except queue.Empty:
                    break
                if key == "__stop__":
                    break
                if key == "__reload__":
                    try:
                        self._rebuild_connections()
                    except Exception as exc:
                        self.status_changed.emit(f"区域形态订阅重建失败，稍后重试：{exc}")
                elif candle is not None:
                    try:
                        self._handle_candle(key, candle)
                    except Exception as exc:
                        self.status_changed.emit(f"区域形态K线处理失败，已标记补查：{exc}")
                        parts = key.split("|", 1)
                        if len(parts) == 2:
                            self._catchup_required.add((parts[0], parts[1]))
            if time.monotonic() >= retry_at:
                retry_at = time.monotonic() + 10.0
                try:
                    self._retry_missing_subscriptions()
                except Exception as exc:
                    self.status_changed.emit(f"区域形态补查失败，保留待重试：{exc}")
        self.status_changed.emit("区域形态后台监控已停止")

    def _rebuild_connections(self) -> None:
        for unsubscribe in self._subscriptions.values():
            try:
                unsubscribe()
            except Exception:
                pass
        self._subscriptions.clear()
        with self._lock:
            active = [plan for plan in self._plans.values() if plan.status in _ACTIVE_STATUSES]
        wanted: set[tuple[str, str]] = {(plan.symbol, plan.environment) for plan in active}
        for environment, client in list(self._clients.items()):
            if environment not in {env for _, env in wanted}:
                try:
                    client.close()
                except Exception:
                    pass
                self._clients.pop(environment, None)
        count = 0
        for symbol, environment in sorted(wanted):
            client = self._clients.get(environment)
            if client is None:
                client = self._client_factory()
                self._clients[environment] = client
            key = (symbol, environment)
            if key not in self._candles:
                try:
                    self._candles[key] = list(client.get_candles_history(symbol, "1H", limit=240))[-240:]
                    self._process_history_updates(key, self._candles[key])
                except Exception as exc:
                    self._catchup_required.add(key)
                    self.status_changed.emit(f"{symbol} 1H 启动补查失败，10秒后重试：{exc}")
            unsubscribe = client.watch_candle(CandleStreamKey(symbol, "1H", environment), lambda candle, _confirmed, text=f"{symbol}|{environment}": self._enqueue(text, candle))
            if unsubscribe is not None:
                self._subscriptions[key] = unsubscribe
            else:
                self._catchup_required.add(key)
            self._last_reconcile[key] = time.monotonic()
            count += 1
        self.status_changed.emit(f"区域形态后台监控 {count} 路（包含本地纸面任务）")

    def _retry_missing_subscriptions(self) -> None:
        with self._lock:
            active_keys = {(plan.symbol, plan.environment) for plan in self._plans.values() if plan.status in _ACTIVE_STATUSES}
        if not active_keys or self._stop_event.is_set():
            return
        for key in sorted(active_keys):
            if key not in self._subscriptions:
                symbol, environment = key
                client = self._clients.get(environment)
                if client is None:
                    client = self._client_factory()
                    self._clients[environment] = client
                unsubscribe = client.watch_candle(CandleStreamKey(symbol, "1H", environment), lambda candle, _confirmed, text=f"{symbol}|{environment}": self._enqueue(text, candle))
                if unsubscribe is not None:
                    self._subscriptions[key] = unsubscribe
            if key in self._catchup_required or key not in self._subscriptions:
                self._catch_up_key(key)
            elif time.monotonic() - self._last_reconcile.get(key, 0.0) >= 60.0:
                self._catch_up_key(key)

    def _catch_up_key(self, key: tuple[str, str]) -> None:
        symbol, environment = key
        if key not in self._catchup_required and key not in self._subscriptions:
            if time.monotonic() - self._last_fallback_poll.get(key, 0.0) < 30.0:
                return
        client = self._clients.get(environment)
        if client is None:
            client = self._client_factory()
            self._clients[environment] = client
        candles = list(client.get_candles_history(symbol, "1H", limit=240))[-240:]
        confirmed = sorted((item for item in candles if item.confirmed), key=lambda item: item.ts)
        self._last_reconcile[key] = time.monotonic()
        if confirmed:
            self._process_history_updates(key, confirmed)
            self._candles[key] = confirmed[-240:]
            self._catchup_required.discard(key)
        if key not in self._subscriptions:
            # REST polling is the supported fallback when the public WS is unavailable.
            now = time.monotonic()
            self._last_fallback_poll[key] = now

    def _process_history_updates(self, key: tuple[str, str], candles: list[Candle]) -> None:
        symbol, environment = key
        if not candles:
            return
        with self._lock:
            changed: list[AreaShapePlan] = []
            for plan in self._plans.values():
                if plan.symbol != symbol or plan.environment != environment or plan.status not in _ACTIVE_STATUSES:
                    continue
                if plan.execution_mode == "demo_exchange":
                    continue
                baseline = self._plan_cursor(plan)
                if self._history_has_gap(candles, baseline):
                    plan.status = "manual_review"
                    plan._event("history_gap", "1H补查区间不连续或超出可用历史页，已停止自动纸面恢复并需人工核对")
                    changed.append(plan)
                    continue
                try:
                    for candle in candles:
                        if candle.ts > baseline:
                            self._process_plan_candle(plan, candles, candle)
                except Exception as exc:
                    plan.status = "manual_review"
                    plan._event("catchup_failed", f"历史补查处理失败，计划暂停并需人工核对：{exc}")
                changed.append(plan)
            if changed:
                self._persist_locked()
        for plan in changed:
            self.plan_updated.emit(plan.to_dict())

    @staticmethod
    def _plan_cursor(plan: AreaShapePlan) -> int:
        if plan.paper_position and plan.status in {"paper_position", "paper_break_even", "paper_locked"}:
            return int(plan.paper_position.get("last_checked_ts", plan.last_candle_ts) or plan.last_candle_ts)
        return plan.last_candle_ts

    @staticmethod
    def _history_has_gap(candles: list[Candle], baseline_ts: int) -> bool:
        ordered = sorted((item for item in candles if item.confirmed), key=lambda item: item.ts)
        if not ordered:
            return False
        if baseline_ts and ordered[-1].ts > baseline_ts and ordered[0].ts > baseline_ts + 3_600_000:
            return True
        return any(
            right.ts - left.ts > 3_600_000 and right.ts > baseline_ts
            for left, right in zip(ordered, ordered[1:])
        )

    def _enqueue(self, key: str, candle: Candle) -> None:
        try:
            self._queue.put_nowait((key, candle))
        except queue.Full:
            parts = key.split("|", 1)
            if len(parts) == 2:
                self._catchup_required.add((parts[0], parts[1]))
            self.status_changed.emit("区域形态行情队列已满，正在等待下次历史补查")

    def _handle_candle(self, key_text: str, candle: Candle) -> None:
        symbol, environment = key_text.split("|", 1)
        key = (symbol, environment)
        candles = self._candles.setdefault(key, [])
        existing = next((item for item in candles if item.ts == candle.ts), None)
        if existing is not None and existing.confirmed and not candle.confirmed:
            return
        candles[:] = [item for item in candles if item.ts != candle.ts]
        candles.append(candle)
        candles.sort(key=lambda item: item.ts)
        del candles[:-240]
        if not candle.confirmed:
            self._process_unconfirmed_price(key, candle)
            return
        updated: list[AreaShapePlan] = []
        with self._lock:
            for plan in self._plans.values():
                if plan.symbol == symbol and plan.environment == environment and plan.status in _ACTIVE_STATUSES:
                    if plan.execution_mode == "demo_exchange":
                        continue
                    cursor = self._plan_cursor(plan)
                    if candle.ts > cursor + 3_600_000:
                        # Do not let a later live candle advance signal/stop state
                        # across a missing confirmed 1H bar. REST catch-up must
                        # replay the contiguous prefix first.
                        self._catchup_required.add(key)
                        continue
                    if self._process_plan_candle(plan, candles, candle):
                        updated.append(plan)
            if updated:
                self._persist_locked()
        for plan in updated:
            self.plan_updated.emit(plan.to_dict())

    def _process_unconfirmed_price(self, key: tuple[str, str], candle: Candle) -> None:
        """Use the current 1H candle's last price for direct paper entries and exits."""
        price = candle.close
        if price <= 0:
            return
        symbol, environment = key
        timestamp_ms = int(time.time() * 1000)
        confirmed = [item for item in self._candles.get(key, []) if item.confirmed]
        updated: list[AreaShapePlan] = []
        with self._lock:
            for plan in self._plans.values():
                if plan.symbol != symbol or plan.environment != environment or plan.execution_mode == "demo_exchange":
                    continue
                price_key = (symbol, environment, plan.plan_id)
                previous_price = self._last_live_prices.get(price_key)
                self._last_live_prices[price_key] = price
                result = None
                if (plan.status in {"monitoring", "touched"}
                        and plan.config.entry_trigger_mode == "direct_touch"):
                    entry_line = plan.support_price if plan.config.direction == "long" else plan.pressure_price
                    crossed = (
                        (price <= entry_line and (previous_price is None or previous_price > entry_line))
                        if plan.config.direction == "long"
                        else (price >= entry_line and (previous_price is None or previous_price < entry_line))
                    )
                    if crossed:
                        result = plan.observe_direct_touch_price(
                            price, timestamp_ms, confirmed, source="live_last_price",
                        )
                        if (result is not None and plan.paper_position is not None
                                and plan.status in {"paper_position", "paper_break_even", "paper_locked"}):
                            result = plan.observe_paper_live_price(price, timestamp_ms) or result
                elif plan.status in {"paper_position", "paper_break_even", "paper_locked"}:
                    result = plan.observe_paper_live_price(price, timestamp_ms)
                if result is not None:
                    updated.append(plan)
            if updated:
                self._persist_locked()
        for plan in updated:
            self.plan_updated.emit(plan.to_dict())

    def _process_plan_candle(self, plan: AreaShapePlan, candles: list[Candle], candle: Candle) -> bool:
        previous_ts = plan.last_candle_ts
        if plan.status in {"paper_position", "paper_break_even", "paper_locked"}:
            result = plan.observe_paper_candle(candle)
        else:
            event = plan.observe(candles)
            result = event
        return result is not None or plan.last_candle_ts != previous_ts

