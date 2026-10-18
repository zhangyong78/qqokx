from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from decimal import Decimal

from PySide6.QtCore import QObject, QThread, QTimer, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxRestClient, OkxTradeOrderItem

from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView, load_current_order_views, order_view_to_trade_order_item
from roll_terminal_qt.history_service import load_cached_order_history, merge_order_history_cache
from okx_quant.arbitrage.arbitrage_executor import _build_strategy_config


@dataclass(frozen=True)
class SharedOrderSnapshot:
    current_order_views: tuple[OrderStatusView, ...] = ()
    current_order_items: tuple[OkxTradeOrderItem, ...] = ()
    history_orders: tuple[OkxTradeOrderItem, ...] = ()
    history_order_usdt_prices: dict[str, Decimal] = field(default_factory=dict)


class SharedOrderRefreshThread(QThread):
    current_orders_ready = Signal(str, str, object)
    completed = Signal(str, str, object)
    cached_history_ready = Signal(str, str, object)
    failed = Signal(str, str, str)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, profile_name: str, limit: int = 200) -> None:
        super().__init__()
        self._runtime = runtime
        self._profile_name = str(profile_name or "").strip()
        self._limit = max(20, int(limit))

    def run(self) -> None:
        environment = str(getattr(self._runtime, "environment", "") or "").strip()
        cached_history = load_cached_order_history(self._profile_name, environment, self._limit)
        if cached_history:
            self.cached_history_ready.emit(self._profile_name, environment, cached_history)
        client = OkxRestClient()
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                current_future = executor.submit(
                    load_current_order_views,
                    self._runtime,
                    client=client,
                    limit=min(self._limit, 100),
                )
                history_future = executor.submit(self._load_history_orders, client)
                _status_text, current_views = current_future.result()
                self.current_orders_ready.emit(self._profile_name, environment, current_views)
                history_orders, history_prices = history_future.result()
            snapshot = SharedOrderSnapshot(
                current_order_views=tuple(current_views),
                current_order_items=tuple(order_view_to_trade_order_item(item) for item in current_views),
                history_orders=tuple(history_orders),
                history_order_usdt_prices=dict(history_prices),
            )
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._profile_name, environment, str(exc))
            return
        finally:
            client.close()
        self.completed.emit(self._profile_name, environment, snapshot)

    def _load_history_orders(
        self,
        client: OkxRestClient | None = None,
    ) -> tuple[list[OkxTradeOrderItem], dict[str, Decimal]]:
        resolved_client = client or OkxRestClient()
        owns_client = client is None
        try:
            remote_items = resolved_client.get_order_history(
                self._runtime.credentials,
                environment=self._runtime.environment,
                limit=self._limit,
                include_algo=True,
            )
            items = merge_order_history_cache(
                profile_name=self._profile_name,
                environment=str(getattr(self._runtime, "environment", "") or "").strip(),
                remote_items=remote_items,
                limit=self._limit,
            )
            currencies = {
                str(item.fee_currency).strip().upper()
                for item in items
                if item.fee is not None and str(item.fee_currency or "").strip()
            }
            prices: dict[str, Decimal] = {}
            for currency in currencies:
                if currency == "USDT":
                    prices[currency] = Decimal("1")
                    continue
                try:
                    prices[currency] = resolved_client.get_ccy_to_usdt_price(currency, prefer_cached=True)
                except Exception:
                    continue
            return items, prices
        finally:
            if owns_client:
                resolved_client.close()


class SubmittedOrderConfirmThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def __init__(self, *, runtime: ArbitrageTradeRuntime, order: OrderStatusView) -> None:
        super().__init__()
        self._runtime = runtime
        self._order = order

    def run(self) -> None:
        client = OkxRestClient()
        try:
            status = client.get_order(
                self._runtime.credentials,
                _build_strategy_config(self._order.inst_id, self._runtime),
                inst_id=self._order.inst_id,
                ord_id=self._order.ord_id or None,
                cl_ord_id=self._order.client_order_id or None,
                request_timeout=5,
            )
            view = OrderFeedThread(None)._to_view(status)
            self.completed.emit(replace(view, raw={**view.raw, "_feed_source": "rest_order"}))
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(str(exc))
        finally:
            client.close()


_TERMINAL_ORDER_STATES = {"filled", "canceled", "cancelled", "mmp_canceled", "order_failed"}


def _order_key(order: OrderStatusView) -> tuple[str, str, str]:
    return (order.inst_id, str(order.raw.get("_source_kind", "normal")), order.ord_id or order.client_order_id)


def _prefer_order(candidate: OrderStatusView, previous: OrderStatusView) -> bool:
    if candidate.raw.get("_feed_source") == "submit_ack":
        return previous.raw.get("_feed_source") == "submit_ack"
    if previous.raw.get("_feed_source") == "submit_ack":
        return True
    if previous.state in _TERMINAL_ORDER_STATES and candidate.state not in _TERMINAL_ORDER_STATES:
        return False
    if candidate.filled_size is not None and previous.filled_size is not None and candidate.filled_size < previous.filled_size:
        return False
    if candidate.update_time is not None and previous.update_time is not None:
        return candidate.update_time >= previous.update_time
    return True


class SharedOrderStore(QObject):
    snapshot_changed = Signal(str, str, object)
    refresh_failed = Signal(str, str, str)
    refresh_finished = Signal(str, str)

    def __init__(self) -> None:
        super().__init__()
        self._snapshots: dict[tuple[str, str], SharedOrderSnapshot] = {}
        self._refresh_threads: dict[tuple[str, str], SharedOrderRefreshThread] = {}
        self._pending_refreshes: dict[tuple[str, str], ArbitrageTradeRuntime] = {}
        self._order_revisions: dict[tuple[str, str], int] = {}
        self._order_updates: dict[tuple[str, str], dict[tuple[str, str, str], tuple[int, OrderStatusView]]] = {}
        self._observed_orders: dict[tuple[str, str], dict[tuple[str, str, str], OrderStatusView]] = {}
        self._confirmation_threads: dict[tuple[str, str, tuple[str, str, str]], SubmittedOrderConfirmThread] = {}

    def snapshot_for(self, *, profile_name: str, environment: str) -> SharedOrderSnapshot:
        return self._snapshots.get(_snapshot_key(profile_name, environment), SharedOrderSnapshot())

    def publish_current_orders(
        self, *, profile_name: str, environment: str, orders: list[OrderStatusView],
        started_revision: int | None = None,
    ) -> None:
        key = _snapshot_key(profile_name, environment)
        previous = self._snapshots.get(key, SharedOrderSnapshot())
        rows = {_order_key(order): order for order in orders}
        overrides = self._order_updates.get(key, {})
        for identity, (revision, update) in list(overrides.items()):
            candidate = rows.get(identity)
            if candidate is not None and _prefer_order(candidate, update):
                overrides[identity] = (revision, candidate)
            elif (
                candidate is None and started_revision is not None and revision <= started_revision
                and update.raw.get("_feed_source") != "submit_ack"
                and update.state not in _TERMINAL_ORDER_STATES
            ):
                del overrides[identity]
            else:
                rows[identity] = update
        orders = sorted(
            (order for order in rows.values() if order.state not in _TERMINAL_ORDER_STATES),
            key=lambda order: order.update_time or order.created_time or 0, reverse=True,
        )
        snapshot = SharedOrderSnapshot(
            current_order_views=tuple(orders),
            current_order_items=tuple(order_view_to_trade_order_item(item) for item in orders),
            history_orders=previous.history_orders,
            history_order_usdt_prices=dict(previous.history_order_usdt_prices),
        )
        if snapshot == previous:
            return
        self._snapshots[key] = snapshot
        self.snapshot_changed.emit(key[0], key[1], snapshot)

    def publish_order_updates(self, *, profile_name: str, environment: str, orders: list[OrderStatusView]) -> None:
        key = _snapshot_key(profile_name, environment)
        overrides = self._order_updates.setdefault(key, {})
        observed = self._observed_orders.setdefault(key, {})
        existing = {_order_key(order): order for order in self.snapshot_for(profile_name=key[0], environment=key[1]).current_order_views}
        for order in orders:
            identity = _order_key(order)
            previous = overrides.get(identity, (0, observed.get(identity, existing.get(identity))))[1]
            if previous is not None and (
                replace(order, raw={}) == replace(previous, raw={}) or not _prefer_order(order, previous)
            ):
                continue
            revision = self._order_revisions.get(key, 0) + 1
            self._order_revisions[key] = revision
            overrides[identity] = (revision, order)
            observed[identity] = order
        self.publish_current_orders(profile_name=key[0], environment=key[1], orders=list(existing.values()))

    def confirm_submitted_order(
        self, *, runtime: ArbitrageTradeRuntime, profile_name: str, order: OrderStatusView, attempt: int = 0,
    ) -> None:
        key = (*_snapshot_key(profile_name, runtime.environment), _order_key(order))
        if key in self._confirmation_threads:
            return
        current = self._order_updates.get(key[:2], {}).get(key[2])
        if current is None or current[1].raw.get("_feed_source") != "submit_ack":
            return
        thread = SubmittedOrderConfirmThread(runtime=runtime, order=order)
        self._confirmation_threads[key] = thread
        thread.completed.connect(lambda view: self.publish_order_updates(profile_name=key[0], environment=key[1], orders=[view]))

        def failed(message: str) -> None:
            self.refresh_failed.emit(key[0], key[1], f"订单 {order.ord_id} 待确认：{message}")
            if attempt < 2:
                QTimer.singleShot(1000 * (attempt + 1), self, lambda: self.confirm_submitted_order(
                    runtime=runtime, profile_name=profile_name, order=order, attempt=attempt + 1,
                ))

        def finished() -> None:
            self._confirmation_threads.pop(key, None)
            thread.deleteLater()

        thread.failed.connect(failed)
        thread.finished.connect(finished)
        thread.start()

    def publish_history_orders(
        self,
        *,
        profile_name: str,
        environment: str,
        orders: list[OkxTradeOrderItem],
        usdt_prices: dict[str, Decimal],
    ) -> None:
        key = _snapshot_key(profile_name, environment)
        previous = self._snapshots.get(key, SharedOrderSnapshot())
        snapshot = SharedOrderSnapshot(
            current_order_views=previous.current_order_views,
            current_order_items=previous.current_order_items,
            history_orders=tuple(orders),
            history_order_usdt_prices=dict(usdt_prices),
        )
        if snapshot == previous:
            return
        self._snapshots[key] = snapshot
        self.snapshot_changed.emit(key[0], key[1], snapshot)

    def request_refresh(self, *, runtime: ArbitrageTradeRuntime | None, profile_name: str) -> None:
        if runtime is None:
            return
        key = _snapshot_key(profile_name, getattr(runtime, "environment", ""))
        if not key[0]:
            return
        thread = self._refresh_threads.get(key)
        if thread is not None and thread.isRunning():
            self._pending_refreshes[key] = runtime
            return
        thread = SharedOrderRefreshThread(runtime=runtime, profile_name=key[0])
        started_revision = self._order_revisions.get(key, 0)
        thread.current_orders_ready.connect(lambda profile, environment, orders: self.publish_current_orders(
            profile_name=profile, environment=environment, orders=orders, started_revision=started_revision,
        ))
        thread.completed.connect(self._apply_refreshed_snapshot)
        thread.cached_history_ready.connect(self._apply_cached_history)
        thread.failed.connect(self._apply_refresh_error)
        thread.finished.connect(lambda key=key, thread=thread: self._clear_refresh_thread(key, thread))
        self._refresh_threads[key] = thread
        thread.start()

    def is_refreshing(self, *, profile_name: str, environment: str) -> bool:
        thread = self._refresh_threads.get(_snapshot_key(profile_name, environment))
        return bool(thread is not None and thread.isRunning())

    def _apply_cached_history(self, profile_name: str, environment: str, orders: object) -> None:
        if not isinstance(orders, list):
            return
        key = _snapshot_key(profile_name, environment)
        previous = self._snapshots.get(key, SharedOrderSnapshot())
        self.publish_history_orders(
            profile_name=key[0],
            environment=key[1],
            orders=list(orders),
            usdt_prices=dict(previous.history_order_usdt_prices),
        )

    def _apply_refreshed_snapshot(self, profile_name: str, environment: str, snapshot: object) -> None:
        if not isinstance(snapshot, SharedOrderSnapshot):
            return
        self.publish_history_orders(
            profile_name=profile_name, environment=environment,
            orders=list(snapshot.history_orders), usdt_prices=snapshot.history_order_usdt_prices,
        )

    def _apply_refresh_error(self, profile_name: str, environment: str, message: str) -> None:
        key = _snapshot_key(profile_name, environment)
        self.refresh_failed.emit(key[0], key[1], message)

    def _clear_refresh_thread(
        self, key: tuple[str, str], finished_thread: SharedOrderRefreshThread | None = None,
    ) -> None:
        thread = self._refresh_threads.get(key)
        if finished_thread is None or thread is finished_thread:
            self._refresh_threads.pop(key, None)
            if thread is not None:
                thread.deleteLater()
        self.refresh_finished.emit(key[0], key[1])
        pending_runtime = self._pending_refreshes.pop(key, None)
        if pending_runtime is not None:
            QTimer.singleShot(0, self, lambda: self.request_refresh(runtime=pending_runtime, profile_name=key[0]))


_SHARED_ORDER_STORE: SharedOrderStore | None = None


def get_shared_order_store() -> SharedOrderStore:
    global _SHARED_ORDER_STORE
    if _SHARED_ORDER_STORE is None:
        _SHARED_ORDER_STORE = SharedOrderStore()
    return _SHARED_ORDER_STORE


def _snapshot_key(profile_name: str, environment: str) -> tuple[str, str]:
    return (str(profile_name or "").strip(), str(environment or "").strip())
