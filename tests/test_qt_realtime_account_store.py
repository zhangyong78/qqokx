from __future__ import annotations

import time
from types import SimpleNamespace

from PySide6.QtWidgets import QApplication

from okx_quant.models import Credentials
from okx_quant.okx_client import OkxOrderStatus
from roll_terminal_qt.realtime_account_store import RealtimeAccountStore


def _drain_qt_events(*, timeout: float = 1.0) -> None:
    app = QApplication.instance() or QApplication([])
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.processEvents()
        time.sleep(0.01)


def _runtime(profile_name: str = "moni", environment: str = "demo") -> object:
    return SimpleNamespace(
        credentials=Credentials(api_key="k", secret_key="s", passphrase="p", profile_name=profile_name),
        environment=environment,
    )


class _FakeRealtimeClient:
    def __init__(self) -> None:
        self.pending_order_rest_calls = 0
        self._private_listeners: list[object] = []
        self._algo_listeners: list[object] = []
        self._order_version = 0
        self._orders: list[OkxOrderStatus] = []

    def get_positions(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []

    def get_account_overview(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return SimpleNamespace(details=())

    def get_pending_orders(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.pending_order_rest_calls += 1
        return []

    def get_cached_private_positions(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1, []

    def get_cached_private_account_overview(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return 1, SimpleNamespace(details=())

    def get_cached_private_order_statuses(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return self._order_version, list(self._orders)

    def get_cached_algo_order_statuses(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return None

    def add_private_update_listener(self, *args, listener, **kwargs):  # noqa: ANN002, ANN003
        self._private_listeners.append(listener)

        def _unsubscribe() -> None:
            if listener in self._private_listeners:
                self._private_listeners.remove(listener)

        return _unsubscribe

    def add_algo_order_update_listener(self, *args, listener, **kwargs):  # noqa: ANN002, ANN003
        self._algo_listeners.append(listener)

        def _unsubscribe() -> None:
            if listener in self._algo_listeners:
                self._algo_listeners.remove(listener)

        return _unsubscribe

    def publish_order(self, item: dict[str, object]) -> None:
        self._order_version += 1
        self._orders = [
            OkxOrderStatus(
                ord_id=str(item["ordId"]),
                state=str(item["state"]),
                side=str(item.get("side") or "buy"),
                ord_type=str(item.get("ordType") or "limit"),
                price=None,
                avg_price=None,
                size=None,
                filled_size=None,
                raw=item,
            )
        ]
        for listener in list(self._private_listeners):
            listener("orders", self._order_version)


def test_store_loads_rest_once_then_uses_ws_events() -> None:
    client = _FakeRealtimeClient()
    store = RealtimeAccountStore(client=client, coalesce_ms=0, reconcile_seconds=60)
    snapshots = []
    store.snapshot_ready.connect(snapshots.append)

    store.start(_runtime())
    _drain_qt_events()
    client.publish_order({"ordId": "1", "state": "filled", "uTime": "20"})
    _drain_qt_events()

    assert client.pending_order_rest_calls == 1
    assert snapshots[-1].orders[0].state == "filled"
    assert snapshots[-1].source == "ws"
    store.stop()


def test_store_stop_unsubscribes_and_keeps_profiles_isolated() -> None:
    client = _FakeRealtimeClient()
    store = RealtimeAccountStore(client=client, coalesce_ms=0, reconcile_seconds=60)

    store.start(_runtime("one", "demo"))
    _drain_qt_events()
    store.start(_runtime("two", "live"))
    _drain_qt_events()
    store.stop()

    assert client._private_listeners == []
    assert client._algo_listeners == []
