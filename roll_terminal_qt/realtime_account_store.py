from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from typing import Callable

from PySide6.QtCore import QObject, QTimer, Signal, Slot

from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.okx_client import OkxAccountOverview, OkxOrderStatus, OkxPosition, OkxRestClient, OkxTradeOrderItem
from okx_quant.ui_shell import _build_position_instrument_map, _build_position_ticker_map, _build_upl_usdt_price_map
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView


@dataclass(frozen=True)
class AccountRealtimeSnapshot:
    profile_name: str
    environment: str
    positions: tuple[OkxPosition, ...]
    orders: tuple[OrderStatusView, ...]
    account: OkxAccountOverview | object | None
    generation: int
    source: str
    position_instruments: dict[str, object] = field(default_factory=dict)
    position_tickers: dict[str, object] = field(default_factory=dict)
    upl_usdt_prices: dict[str, object] = field(default_factory=dict)


class RealtimeAccountStore(QObject):
    """Qt-owned account state with WS-first updates and bounded REST reconciliation."""

    snapshot_ready = Signal(object)
    status_changed = Signal(str)
    _reconcile_completed = Signal(int, str, object)
    _cache_completed = Signal(int, object)
    _event_received = Signal(int, str, int)

    def __init__(
        self,
        *,
        client: OkxRestClient | object | None = None,
        coalesce_ms: int = 100,
        reconcile_seconds: int = 60,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self._client = client or OkxRestClient()
        self._coalesce_ms = max(0, int(coalesce_ms))
        self._generation = 0
        self._runtime: ArbitrageTradeRuntime | object | None = None
        self._profile_name = ""
        self._environment = ""
        self._positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._account: OkxAccountOverview | object | None = None
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, object] = {}
        self._source = "startup"
        self._unsubscribers: list[Callable[[], None]] = []
        self._reconcile_in_flight = False
        self._cache_in_flight = False
        self._pending_cache_refresh = False
        self._emit_timer = QTimer(self)
        self._emit_timer.setSingleShot(True)
        self._emit_timer.timeout.connect(self._emit_snapshot)
        self._reconcile_timer = QTimer(self)
        self._reconcile_timer.setInterval(max(1, int(reconcile_seconds)) * 1000)
        self._reconcile_timer.timeout.connect(lambda: self.request_reconcile("safety"))
        self._reconcile_completed.connect(self._apply_reconcile_result)
        self._cache_completed.connect(self._apply_cache_result)
        self._event_received.connect(self._queue_cache_refresh)

    def start(self, runtime: ArbitrageTradeRuntime | object) -> None:
        self.stop()
        self._generation += 1
        self._runtime = runtime
        self._profile_name = str(getattr(getattr(runtime, "credentials", None), "profile_name", "") or "").strip()
        self._environment = str(getattr(runtime, "environment", "") or "").strip()
        self._positions = []
        self._orders = []
        self._account = None
        self._position_instruments = {}
        self._position_tickers = {}
        self._upl_usdt_prices = {}
        self._source = "startup"
        self._subscribe_ws_updates(self._generation)
        self._reconcile_timer.start()
        self.request_reconcile("startup")

    def stop(self) -> None:
        self._generation += 1
        self._reconcile_timer.stop()
        self._emit_timer.stop()
        self._reconcile_in_flight = False
        self._cache_in_flight = False
        self._pending_cache_refresh = False
        for unsubscribe in self._unsubscribers:
            try:
                unsubscribe()
            except Exception:
                pass
        self._unsubscribers.clear()

    def request_reconcile(self, reason: str) -> None:
        if self._runtime is None or self._reconcile_in_flight:
            return
        self._reconcile_in_flight = True
        generation = self._generation
        runtime = self._runtime
        thread = threading.Thread(
            target=self._run_reconcile_worker,
            args=(generation, str(reason or "manual"), runtime),
            daemon=True,
            name="qt-account-reconcile",
        )
        thread.start()

    def _subscribe_ws_updates(self, generation: int) -> None:
        runtime = self._runtime
        if runtime is None:
            return
        credentials = getattr(runtime, "credentials", None)
        environment = str(getattr(runtime, "environment", "") or "")
        if credentials is None:
            return

        def _listener(channel: str, version: int) -> None:
            self._event_received.emit(generation, str(channel), int(version))

        for subscribe in (
            getattr(self._client, "add_private_update_listener", None),
            getattr(self._client, "add_algo_order_update_listener", None),
        ):
            if not callable(subscribe):
                continue
            try:
                unsubscribe = subscribe(credentials, environment=environment, listener=_listener)
            except Exception:
                continue
            if callable(unsubscribe):
                self._unsubscribers.append(unsubscribe)

    def _run_reconcile_worker(self, generation: int, reason: str, runtime: object) -> None:
        try:
            credentials = getattr(runtime, "credentials")
            environment = str(getattr(runtime, "environment", "") or "")
            positions: list[OkxPosition] = []
            for inst_type in ("FUTURES", "SWAP", "OPTION"):
                positions.extend(
                    self._client.get_positions(
                        credentials,
                        environment=environment,
                        inst_type=inst_type,
                        prefer_cache=False,
                    )
                )
            account = self._client.get_account_overview(credentials, environment=environment, prefer_cache=False)
            pending_orders = self._client.get_pending_orders(
                credentials,
                environment=environment,
                limit=80,
                include_algo=True,
            )
            try:
                upl_usdt_prices = _build_upl_usdt_price_map(self._client, positions)
            except Exception:
                upl_usdt_prices = {}
            try:
                position_instruments = _build_position_instrument_map(self._client, positions)
            except Exception:
                position_instruments = {}
            try:
                position_tickers = _build_position_ticker_map(self._client, positions)
            except Exception:
                position_tickers = {}
            self._reconcile_completed.emit(
                generation,
                reason,
                {
                    "positions": positions,
                    "account": account,
                    "pending_orders": pending_orders,
                    "upl_usdt_prices": upl_usdt_prices,
                    "position_instruments": position_instruments,
                    "position_tickers": position_tickers,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._reconcile_completed.emit(generation, reason, exc)

    @Slot(int, str, object)
    def _apply_reconcile_result(self, generation: int, reason: str, result: object) -> None:
        if generation != self._generation:
            return
        self._reconcile_in_flight = False
        if isinstance(result, Exception):
            self.status_changed.emit(f"账户 REST 校验失败：{result}")
            return
        if not isinstance(result, dict):
            return
        self._positions = list(result.get("positions") or [])
        self._account = result.get("account")
        self._upl_usdt_prices = dict(result.get("upl_usdt_prices") or {})
        self._position_instruments = dict(result.get("position_instruments") or {})
        self._position_tickers = dict(result.get("position_tickers") or {})
        self._orders = self._views_from_pending_orders(list(result.get("pending_orders") or []))
        self._source = "rest"
        self.status_changed.emit(f"账户已通过 REST {reason} 校验")
        self._schedule_emit()

    @Slot(int, str, int)
    def _queue_cache_refresh(self, generation: int, _channel: str, _version: int) -> None:
        if generation != self._generation or self._runtime is None:
            return
        self._pending_cache_refresh = True
        if self._cache_in_flight:
            return
        self._start_cache_worker()

    def _start_cache_worker(self) -> None:
        if self._runtime is None or self._cache_in_flight:
            return
        self._cache_in_flight = True
        self._pending_cache_refresh = False
        generation = self._generation
        runtime = self._runtime
        threading.Thread(
            target=self._run_cache_worker,
            args=(generation, runtime),
            daemon=True,
            name="qt-account-ws-cache",
        ).start()

    def _run_cache_worker(self, generation: int, runtime: object) -> None:
        try:
            credentials = getattr(runtime, "credentials")
            environment = str(getattr(runtime, "environment", "") or "")
            self._cache_completed.emit(
                generation,
                {
                    "positions": self._client.get_cached_private_positions(credentials, environment=environment),
                    "account": self._client.get_cached_private_account_overview(credentials, environment=environment),
                    "orders": self._client.get_cached_private_order_statuses(credentials, environment=environment, limit=80),
                    "algo_orders": self._client.get_cached_algo_order_statuses(credentials, environment=environment, limit=80),
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._cache_completed.emit(generation, exc)

    @Slot(int, object)
    def _apply_cache_result(self, generation: int, result: object) -> None:
        if generation != self._generation:
            return
        self._cache_in_flight = False
        if isinstance(result, Exception):
            self.status_changed.emit(f"账户 WS 缓存读取失败：{result}")
            return
        if not isinstance(result, dict):
            return
        positions_payload = result.get("positions")
        if positions_payload is not None:
            self._positions = list(positions_payload[1])
        account_payload = result.get("account")
        if account_payload is not None:
            self._account = account_payload[1]
        updates: list[OrderStatusView] = []
        orders_payload = result.get("orders")
        if orders_payload is not None:
            updates.extend(self._views_from_order_statuses(list(orders_payload[1])))
        algo_payload = result.get("algo_orders")
        if algo_payload is not None:
            updates.extend(self._views_from_algo_orders(list(algo_payload[1])))
        if updates:
            self._orders = self._merge_order_updates(self._orders, updates)
        self._source = "ws"
        self.status_changed.emit("账户由 WS 实时更新")
        self._schedule_emit()
        if self._pending_cache_refresh:
            self._start_cache_worker()

    def _views_from_pending_orders(self, orders: list[OkxTradeOrderItem]) -> list[OrderStatusView]:
        feed = OrderFeedThread(None)
        return feed._merge_order_views([], orders)

    def _views_from_order_statuses(self, orders: list[OkxOrderStatus]) -> list[OrderStatusView]:
        feed = OrderFeedThread(None)
        return [feed._to_view(item) for item in orders]

    def _views_from_algo_orders(self, orders: list[OkxTradeOrderItem]) -> list[OrderStatusView]:
        feed = OrderFeedThread(None)
        views: list[OrderStatusView] = []
        for order in orders:
            view = feed._trade_order_to_view(order)
            raw = dict(view.raw)
            raw["_feed_source"] = "ws"
            views.append(replace(view, raw=raw))
        return views

    @staticmethod
    def _merge_order_updates(existing: list[OrderStatusView], updates: list[OrderStatusView]) -> list[OrderStatusView]:
        rows = {OrderFeedThread._view_identity(item): item for item in existing}
        for item in updates:
            key = OrderFeedThread._view_identity(item)
            if key is None:
                continue
            rows[key] = item
        return sorted(rows.values(), key=lambda item: item.update_time or item.created_time or 0, reverse=True)

    def _schedule_emit(self) -> None:
        if self._coalesce_ms == 0:
            self._emit_snapshot()
            return
        self._emit_timer.start(self._coalesce_ms)

    @Slot()
    def _emit_snapshot(self) -> None:
        if self._runtime is None:
            return
        self.snapshot_ready.emit(
            AccountRealtimeSnapshot(
                profile_name=self._profile_name,
                environment=self._environment,
                positions=tuple(self._positions),
                orders=tuple(self._orders),
                account=self._account,
                generation=self._generation,
                source=self._source,
                position_instruments=dict(self._position_instruments),
                position_tickers=dict(self._position_tickers),
                upl_usdt_prices=dict(self._upl_usdt_prices),
            )
        )
