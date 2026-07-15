from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal

from PySide6.QtCore import QObject, QThread, Signal

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxRestClient, OkxTradeOrderItem

from roll_terminal_qt.order_service import OrderStatusView, load_current_order_views, order_view_to_trade_order_item
from roll_terminal_qt.history_service import load_cached_order_history, merge_order_history_cache


@dataclass(frozen=True)
class SharedOrderSnapshot:
    current_order_views: tuple[OrderStatusView, ...] = ()
    current_order_items: tuple[OkxTradeOrderItem, ...] = ()
    history_orders: tuple[OkxTradeOrderItem, ...] = ()
    history_order_usdt_prices: dict[str, Decimal] = field(default_factory=dict)


class SharedOrderRefreshThread(QThread):
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
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                current_future = executor.submit(
                    load_current_order_views,
                    self._runtime,
                    limit=min(self._limit, 100),
                )
                history_future = executor.submit(self._load_history_orders)
                _status_text, current_views = current_future.result()
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
        self.completed.emit(self._profile_name, environment, snapshot)

    def _load_history_orders(self) -> tuple[list[OkxTradeOrderItem], dict[str, Decimal]]:
        client = OkxRestClient()
        remote_items = client.get_order_history(
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
                prices[currency] = client.get_ccy_to_usdt_price(currency, prefer_cached=True)
            except Exception:
                continue
        return items, prices


class SharedOrderStore(QObject):
    snapshot_changed = Signal(str, str, object)
    refresh_failed = Signal(str, str, str)

    def __init__(self) -> None:
        super().__init__()
        self._snapshots: dict[tuple[str, str], SharedOrderSnapshot] = {}
        self._refresh_threads: dict[tuple[str, str], SharedOrderRefreshThread] = {}

    def snapshot_for(self, *, profile_name: str, environment: str) -> SharedOrderSnapshot:
        return self._snapshots.get(_snapshot_key(profile_name, environment), SharedOrderSnapshot())

    def publish_current_orders(self, *, profile_name: str, environment: str, orders: list[OrderStatusView]) -> None:
        key = _snapshot_key(profile_name, environment)
        previous = self._snapshots.get(key, SharedOrderSnapshot())
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
            return
        thread = SharedOrderRefreshThread(runtime=runtime, profile_name=key[0])
        thread.completed.connect(self._apply_refreshed_snapshot)
        thread.cached_history_ready.connect(self._apply_cached_history)
        thread.failed.connect(self._apply_refresh_error)
        thread.finished.connect(lambda key=key: self._clear_refresh_thread(key))
        self._refresh_threads[key] = thread
        thread.start()

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
        key = _snapshot_key(profile_name, environment)
        if snapshot == self._snapshots.get(key):
            return
        self._snapshots[key] = snapshot
        self.snapshot_changed.emit(key[0], key[1], snapshot)

    def _apply_refresh_error(self, profile_name: str, environment: str, message: str) -> None:
        key = _snapshot_key(profile_name, environment)
        self.refresh_failed.emit(key[0], key[1], message)

    def _clear_refresh_thread(self, key: tuple[str, str]) -> None:
        thread = self._refresh_threads.pop(key, None)
        if thread is not None:
            thread.deleteLater()


_SHARED_ORDER_STORE: SharedOrderStore | None = None


def get_shared_order_store() -> SharedOrderStore:
    global _SHARED_ORDER_STORE
    if _SHARED_ORDER_STORE is None:
        _SHARED_ORDER_STORE = SharedOrderStore()
    return _SHARED_ORDER_STORE


def _snapshot_key(profile_name: str, environment: str) -> tuple[str, str]:
    return (str(profile_name or "").strip(), str(environment or "").strip())
