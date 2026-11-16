from __future__ import annotations

import time
import threading
from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtCore import QTimer
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


def test_start_if_needed_preserves_warm_snapshot_for_same_profile() -> None:
    client = _FakeRealtimeClient()
    runtime = _runtime("selected", "live")
    store = RealtimeAccountStore(client=client, coalesce_ms=0, reconcile_seconds=60)
    try:
        store.start(runtime)
        _drain_qt_events()
        generation = store._generation

        store.start_if_needed(runtime)

        snapshot = store.snapshot_for(profile_name="selected", environment="live")
        assert store._generation == generation
        assert snapshot is not None
        assert snapshot.source == "rest"
        assert len(client._private_listeners) == 1
    finally:
        store.stop()


def test_reconcile_interval_can_be_changed_without_restarting_store() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), reconcile_seconds=60)
    try:
        store.start(_runtime())
        generation = store._generation
        store.set_reconcile_interval_seconds(300)

        assert store._reconcile_timer.interval() == 300_000
        assert store._reconcile_timer.isActive()
        assert store._generation == generation
    finally:
        store.stop()


def test_account_recovery_retry_preserves_snapshot_and_resets_after_success() -> None:
    app = QApplication.instance() or QApplication([])
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), reconcile_seconds=300)
    store._runtime = _runtime()
    previous_account = object()
    store._account = previous_account
    try:
        for expected_delay in (5000, 10000, 20000, 30000, 30000):
            store._apply_reconcile_result(store._generation, "safety", OSError("proxy offline"))
            assert store._reconcile_retry_timer.isActive()
            assert store._reconcile_retry_timer.interval() == expected_delay
            assert store._account is previous_account
        restored_account = object()
        store._apply_reconcile_result(store._generation, "recovery", {"account": restored_account})
        assert not store._reconcile_retry_timer.isActive()
        assert store._reconcile_retry_delay == 5
        assert store._account is restored_account
    finally:
        store.stop()
        app.processEvents()


def test_account_ws_reconnect_requests_full_reconcile_only_for_current_generation() -> None:
    app = QApplication.instance() or QApplication([])
    store = RealtimeAccountStore(client=_FakeRealtimeClient())
    store._runtime = _runtime()
    try:
        with patch.object(store, "request_reconcile") as reconcile:
            store._queue_cache_refresh(store._generation - 1, "reconnected", 0)
            reconcile.assert_not_called()
            store._queue_cache_refresh(store._generation, "reconnected", 0)
            reconcile.assert_called_once_with("ws-reconnected")
        assert not store._cache_in_flight
    finally:
        store.stop()
        app.processEvents()


def test_profile_switch_detaches_slow_websockets_without_blocking_ui_thread() -> None:
    stopped = threading.Event()
    release = threading.Event()

    class SlowConnection:
        def stop(self) -> None:
            stopped.set()
            release.wait(timeout=1.0)

    class SlowDisconnectClient(_FakeRealtimeClient):
        def detach_profile_websockets(self, credentials, *, environment):  # noqa: ANN001
            if credentials.profile_name == "one" and environment == "demo":
                return (SlowConnection(),)
            return ()

    client = SlowDisconnectClient()
    store = RealtimeAccountStore(client=client, coalesce_ms=0, reconcile_seconds=60)
    store.start(_runtime("one", "demo"))
    _drain_qt_events()

    started_at = time.monotonic()
    store.start(_runtime("two", "live"))
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.2
    assert stopped.wait(timeout=0.5)
    release.set()
    store.stop()


def test_continuous_updates_do_not_starve_coalesced_snapshots() -> None:
    app = QApplication.instance() or QApplication([])
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=50)
    store._runtime = _runtime()
    snapshots = []
    store.snapshot_ready.connect(snapshots.append)
    producer = QTimer()
    producer.setInterval(5)
    producer.timeout.connect(store._schedule_emit)
    try:
        producer.start()
        _drain_qt_events(timeout=0.25)
        assert snapshots, "Continuous events must not postpone UI updates indefinitely"
        assert producer.isActive()
    finally:
        producer.stop()
        store.stop()
        app.processEvents()


def test_ws_burst_is_read_once_and_stop_cancels_scheduled_cache_read() -> None:
    app = QApplication.instance() or QApplication([])
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=50)
    store._runtime = _runtime()
    generation = store._generation
    try:
        with patch.object(store, "_run_cache_worker", wraps=store._run_cache_worker) as worker:
            for version in range(100):
                store._queue_cache_refresh(generation, "orders", version)
            assert worker.call_count == 0
            _drain_qt_events(timeout=0.2)
            assert worker.call_count == 1
            store._queue_cache_refresh(generation, "orders", 101)
            store.stop()
            _drain_qt_events(timeout=0.1)
            assert worker.call_count == 1
    finally:
        store.stop()
        app.processEvents()


def test_events_during_failed_cache_read_are_retried_without_parallel_worker() -> None:
    app = QApplication.instance() or QApplication([])
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=50)
    store._runtime = _runtime()
    generation = store._generation
    store._cache_in_flight = True
    try:
        with patch.object(store, "_run_cache_worker", wraps=store._run_cache_worker) as worker:
            for version in range(10):
                store._queue_cache_refresh(generation, "orders", version)
            _drain_qt_events(timeout=0.1)
            assert worker.call_count == 0
            store._apply_cache_result(generation, RuntimeError("cache unavailable"))
            _drain_qt_events(timeout=0.2)
            assert worker.call_count == 1
            assert not store._cache_in_flight
    finally:
        store.stop()
        app.processEvents()


def test_stale_rest_reconcile_does_not_overwrite_newer_ws_snapshot() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=0)
    store._runtime = _runtime()
    generation = store._generation
    store._positions = ["newer-ws-position"]
    store._source = "ws"
    store._ws_revision = 2
    statuses: list[str] = []
    store.status_changed.connect(statuses.append)

    store._reconcile_in_flight = True
    store._apply_reconcile_result(
        generation,
        "safety",
        {
            "started_ws_revision": 1,
            "positions": ["stale-rest-position"],
            "pending_orders": [],
        },
    )

    assert store._reconcile_in_flight is False
    assert store._positions == ["newer-ws-position"]
    assert store._source == "ws"
    assert any("已过期" in message for message in statuses)
    store.stop()


def test_stale_account_result_keeps_public_quotes_for_unchanged_instruments() -> None:
    from types import SimpleNamespace

    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=0)
    store._runtime = _runtime()
    current = SimpleNamespace(inst_id="BTC-USD-261225-95000-C")
    store._positions = [current]
    store._ws_revision = 2
    store._apply_reconcile_result(store._generation, "safety", {
        "started_ws_revision": 1,
        "positions": [SimpleNamespace(inst_id=current.inst_id)],
        "upl_usdt_prices": {"BTC": 80000},
        "position_tickers": {current.inst_id: "quote"},
    })
    assert store._positions == [current]
    assert store._upl_usdt_prices == {"BTC": 80000}
    assert store._position_tickers == {current.inst_id: "quote"}
    store.stop()


def test_public_metadata_fetch_finishes_while_pending_orders_are_blocked() -> None:
    from types import SimpleNamespace

    pending_started = threading.Event()
    release_pending = threading.Event()
    public_finished = threading.Event()

    class SlowOrdersClient(_FakeRealtimeClient):
        def get_positions(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return [SimpleNamespace(inst_id="BTC-USDT-SWAP", inst_type="SWAP")]

        def get_account_overview(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return SimpleNamespace(details=())

        def get_pending_orders(self, *args, **kwargs):  # noqa: ANN002, ANN003
            pending_started.set()
            assert release_pending.wait(timeout=2)
            return []

    store = RealtimeAccountStore(client=SlowOrdersClient())
    try:
        with patch("roll_terminal_qt.realtime_account_store._build_upl_usdt_price_map", return_value={"BTC": 1}), \
             patch("roll_terminal_qt.realtime_account_store._build_position_instrument_map", return_value={}), \
             patch("roll_terminal_qt.realtime_account_store._build_position_ticker_map", side_effect=lambda *_: (public_finished.set() or {})):
            worker = threading.Thread(target=store._run_reconcile_worker, args=(0, "test", _runtime()), daemon=True)
            worker.start()
            assert pending_started.wait(timeout=1)
            assert public_finished.wait(timeout=1), "Public data must finish independently of the blocked order request"
            release_pending.set()
            worker.join(timeout=1)
            assert not worker.is_alive()
    finally:
        release_pending.set()
        store.stop()


def test_reconcile_remains_gated_until_both_private_and_public_workers_finish() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=0)
    store._runtime = _runtime()
    store._reconcile_in_flight = True
    store._public_data_in_flight = True
    store._reconcile_private_done = False
    store._positions = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
    store._apply_public_data_result(store._generation, store._active_reconcile_id, {
        "started_ws_revision": store._ws_revision,
        "positions": list(store._positions),
        "position_tickers": {"BTC-USDT-SWAP": "quote"},
        "done": True,
    })
    assert store._position_tickers == {"BTC-USDT-SWAP": "quote"}
    assert store._reconcile_in_flight is True
    store._reconcile_private_done = True
    store._finish_reconcile_if_ready()
    assert store._reconcile_in_flight is False
    store.stop()


def test_public_results_are_emitted_per_category_before_later_fetches_finish() -> None:
    from types import SimpleNamespace

    instrument_started = threading.Event()
    release_instruments = threading.Event()
    emitted: list[object] = []
    store = RealtimeAccountStore(client=_FakeRealtimeClient())
    store._public_data_completed.connect(lambda *_args: emitted.append(_args[2]))

    def slow_instruments(*_args):
        instrument_started.set()
        assert release_instruments.wait(timeout=2)
        return {"BTC-USDT-SWAP": "instrument"}

    try:
        with patch("roll_terminal_qt.realtime_account_store._build_upl_usdt_price_map", return_value={"BTC": 1}), \
             patch("roll_terminal_qt.realtime_account_store._build_position_instrument_map", side_effect=slow_instruments), \
             patch("roll_terminal_qt.realtime_account_store._build_position_ticker_map", return_value={"BTC-USDT-SWAP": "ticker"}):
            worker = threading.Thread(
                target=store._run_public_data_worker,
                args=(store._generation, store._active_reconcile_id, [SimpleNamespace(inst_id="BTC-USDT-SWAP")]),
                daemon=True,
            )
            worker.start()
            assert instrument_started.wait(timeout=1)
            _drain_qt_events(timeout=0.1)
            assert emitted and emitted[0].get("upl_usdt_prices") == {"BTC": 1}
            assert "position_instruments" not in emitted[0]
            release_instruments.set()
            worker.join(timeout=1)
            _drain_qt_events(timeout=0.1)
            assert any(item.get("position_instruments") for item in emitted)
    finally:
        release_instruments.set()
        store.stop()


def test_private_failure_keeps_started_public_worker_gated_until_completion() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient())
    store._runtime = _runtime()
    store._reconcile_in_flight = True
    store._public_data_in_flight = True
    store._reconcile_private_done = False
    store._public_started_event.set()
    statuses = []
    store.status_changed.connect(statuses.append)
    try:
        store._apply_reconcile_result(store._generation, "test", {
            "reconcile_id": store._active_reconcile_id,
            "error": OSError("private request failed"),
        })
        assert store._public_data_in_flight is True
        assert store._reconcile_in_flight is True
        store._apply_public_data_result(store._generation, store._active_reconcile_id, {"done": True})
        assert store._public_data_in_flight is False
        assert store._reconcile_in_flight is False
        assert statuses
    finally:
        store.stop()


def test_public_result_allows_ws_revision_change_when_position_ids_match() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=0)
    current = SimpleNamespace(inst_id="BTC-USDT-SWAP")
    store._positions = [current]
    store._ws_revision = 4
    store._public_data_in_flight = True
    store._reconcile_in_flight = True
    store._reconcile_private_done = True
    store._apply_public_data_result(store._generation, store._active_reconcile_id, {
        "positions": [SimpleNamespace(inst_id=current.inst_id)],
        "position_tickers": {current.inst_id: "fresh quote"},
        "done": True,
    })
    assert store._position_tickers == {current.inst_id: "fresh quote"}
    assert store._reconcile_in_flight is False
    store.stop()


def test_private_success_before_public_first_signal_keeps_reconcile_gated() -> None:
    store = RealtimeAccountStore(client=_FakeRealtimeClient(), coalesce_ms=0)
    store._runtime = _runtime()
    store._reconcile_in_flight = True
    store._public_data_in_flight = True
    store._reconcile_private_done = False
    store._positions = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
    store._apply_reconcile_result(store._generation, "test", {
        "reconcile_id": store._active_reconcile_id,
        "started_ws_revision": store._ws_revision,
        "positions": list(store._positions),
        "pending_orders": [],
    })
    assert store._reconcile_private_done is True
    assert store._reconcile_in_flight is True
    store._apply_public_data_result(store._generation, store._active_reconcile_id, {
        "positions": list(store._positions), "done": True,
    })
    assert store._reconcile_in_flight is False
    store.stop()


def test_retry_that_expires_during_public_worker_runs_after_public_completion() -> None:
    from unittest.mock import patch

    store = RealtimeAccountStore(client=_FakeRealtimeClient())
    store._runtime = _runtime()
    store._reconcile_in_flight = True
    store._public_data_in_flight = True
    store._public_started_event.set()
    try:
        store._apply_reconcile_result(store._generation, "test", {
            "reconcile_id": store._active_reconcile_id,
            "error": OSError("private request failed"),
        })
        store._retry_reconcile_when_ready()
        assert store._reconcile_retry_pending is True
        with patch.object(store, "request_reconcile") as request:
            store._apply_public_data_result(store._generation, store._active_reconcile_id, {"done": True})
            assert store._reconcile_retry_timer.isActive()
            _drain_qt_events(timeout=0.1)
            request.assert_called_once_with("recovery")
    finally:
        store.stop()
