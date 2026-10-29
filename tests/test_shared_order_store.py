from __future__ import annotations

from decimal import Decimal
from dataclasses import replace
import threading
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.okx_client import OkxTradeOrderItem
from roll_terminal_qt.shared_order_store import SharedOrderRefreshThread, SharedOrderSnapshot, SharedOrderStore, SubmittedOrderConfirmThread
from roll_terminal_qt.order_service import OrderFeedThread


def _history_order() -> OkxTradeOrderItem:
    values = {
        "source_kind": "normal", "source_label": "普通委托", "created_time": 1, "update_time": 2,
        "inst_id": "BTC-USDT-SWAP", "inst_type": "SWAP", "side": "buy", "pos_side": "long",
        "td_mode": "cross", "ord_type": "limit", "state": "canceled", "price": Decimal("60000"),
        "size": Decimal("1"), "filled_size": Decimal("0"), "avg_price": None, "order_id": "order-1",
        "algo_id": None, "client_order_id": None, "algo_client_order_id": None, "pnl": None, "fee": None,
        "fee_currency": None, "reduce_only": False, "trigger_price": None, "trigger_price_type": None,
        "order_price": None, "actual_price": None, "actual_size": None, "actual_side": None,
        "take_profit_trigger_price": None, "take_profit_order_price": None,
        "take_profit_trigger_price_type": None, "stop_loss_trigger_price": None,
        "stop_loss_order_price": None, "stop_loss_trigger_price_type": None, "raw": {},
    }
    return OkxTradeOrderItem(**values)


def _view(*, state="live", timestamp=10, filled="0", source="ws"):
    view = OrderFeedThread(None)._trade_order_to_view(_history_order())
    return replace(view, state=state, update_time=timestamp, filled_size=Decimal(filled), raw={"_source_kind": "normal", "_feed_source": source})


class SharedOrderStoreTest(TestCase):
    def test_ack_survives_missing_snapshot_and_is_replaced_by_exchange_confirmation(self) -> None:
        store = SharedOrderStore()
        ack = _view(state="pending_confirmation", timestamp=None, source="submit_ack")
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[ack])
        store.publish_current_orders(profile_name="moni", environment="demo", orders=[], started_revision=0)
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (ack,))
        confirmed = _view()
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[confirmed])
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[ack])
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (confirmed,))
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="live").current_order_views, ())
        self.assertEqual(store.snapshot_for(profile_name="other", environment="demo").current_order_views, ())

    def test_old_snapshot_cannot_regress_partial_fill_or_resurrect_terminal_order(self) -> None:
        store = SharedOrderStore()
        partial = _view(state="partially_filled", timestamp=20, filled="0.5")
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[partial])
        store.publish_current_orders(profile_name="moni", environment="demo", orders=[_view()], started_revision=0)
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (partial,))
        terminal = _view(state="filled", timestamp=30, filled="1")
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[terminal])
        store.publish_current_orders(profile_name="moni", environment="demo", orders=[_view()], started_revision=0)
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, ())
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[partial])
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, ())

    def test_empty_realtime_update_does_not_erase_rest_orders_and_terminal_update_removes_them(self) -> None:
        from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget

        store = SharedOrderStore()
        order = _view(source="rest_pending")
        store.publish_current_orders(profile_name="moni", environment="demo", orders=[order], started_revision=0)
        home = SimpleNamespace(
            _shared_order_store=store, _runtime=SimpleNamespace(environment="demo"),
            _last_profile_name="moni", _orders=[], _refresh_current_orders_table=MagicMock(),
        )
        AccountPositionsHomeWidget._apply_orders(home, [])
        self.assertEqual(home._orders, [order])
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (order,))

        terminal = _view(state="filled", timestamp=20, filled="1")
        AccountPositionsHomeWidget._apply_orders(home, [terminal])
        self.assertEqual(home._orders, [])
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, ())

    def test_fresh_full_snapshot_can_remove_missing_live_order_but_history_cannot_replace_current(self) -> None:
        store = SharedOrderStore()
        order = _view()
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[order])
        store._apply_refreshed_snapshot("moni", "demo", SharedOrderSnapshot(history_orders=(_history_order(),)))
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (order,))
        store.publish_current_orders(profile_name="moni", environment="demo", orders=[], started_revision=1)
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, ())
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[order])
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, ())

    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    @patch("roll_terminal_qt.shared_order_store.load_cached_order_history", return_value=[])
    @patch("roll_terminal_qt.shared_order_store.load_current_order_views")
    def test_current_orders_are_published_before_slow_history_finishes(self, load_current, _cache, _client) -> None:
        order = _view()
        load_current.return_value = ("ok", [order])
        thread = SharedOrderRefreshThread(runtime=SimpleNamespace(environment="demo", credentials=object()), profile_name="moni")
        released = threading.Event()
        events = []

        def history(_client):
            self.assertTrue(released.wait(1), "current orders were blocked by history")
            return [], {}

        def current_ready(*_args):
            events.append("current")
            released.set()

        thread.current_orders_ready.connect(current_ready)
        thread.completed.connect(lambda *_args: events.append("history"))
        with patch.object(thread, "_load_history_orders", side_effect=history):
            thread.run()
        self.assertEqual(events, ["current", "history"])

    def test_confirmation_retries_reads_only_and_stops_after_three_attempts(self) -> None:
        store = SharedOrderStore()
        ack = _view(state="pending_confirmation", timestamp=None, source="submit_ack")
        store.publish_order_updates(profile_name="moni", environment="demo", orders=[ack])
        runtime = SimpleNamespace(environment="demo")
        with (
            patch("roll_terminal_qt.shared_order_store.SubmittedOrderConfirmThread.start"),
            patch("roll_terminal_qt.shared_order_store.QTimer.singleShot") as timer,
        ):
            store.confirm_submitted_order(runtime=runtime, profile_name="moni", order=ack)
            for attempt in range(3):
                thread = next(iter(store._confirmation_threads.values()))
                thread.failed.emit("timeout")
                thread.finished.emit()
                if attempt < 2:
                    callback = timer.call_args.args[2]
                    callback()
            self.assertEqual(timer.call_count, 2)
            self.assertEqual(store._confirmation_threads, {})
        self.assertEqual(store.snapshot_for(profile_name="moni", environment="demo").current_order_views, (ack,))

    @patch("roll_terminal_qt.shared_order_store._build_strategy_config", return_value=object())
    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    def test_confirmation_only_reads_the_submitted_order(self, client_type, _config) -> None:
        client = client_type.return_value
        client.get_order.return_value = SimpleNamespace(
            ord_id="order-1", state="live", side="buy", ord_type="limit", price=Decimal("60000"),
            avg_price=None, size=Decimal("1"), filled_size=Decimal("0"),
            raw={"instId": "BTC-USDT-SWAP", "instType": "SWAP", "uTime": "10"},
        )
        thread = SubmittedOrderConfirmThread(
            runtime=SimpleNamespace(credentials=object()),
            order=_view(state="pending_confirmation", source="submit_ack"),
        )
        results = []
        thread.completed.connect(results.append)
        thread.run()
        self.assertEqual(results[0].state, "live")
        self.assertEqual(results[0].raw["_feed_source"], "rest_order")
        self.assertEqual(client.get_order.call_args.kwargs["ord_id"], "order-1")
        client.place_limit_order.assert_not_called()
        client.place_market_order.assert_not_called()
        client.close.assert_called_once()

    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    @patch("roll_terminal_qt.shared_order_store.load_cached_order_history", return_value=[])
    @patch("roll_terminal_qt.shared_order_store.load_current_order_views", return_value=("ok", [_view()]))
    def test_history_failure_does_not_discard_current_orders(self, _current, _cache, _client) -> None:
        thread = SharedOrderRefreshThread(runtime=SimpleNamespace(environment="demo", credentials=object()), profile_name="moni")
        current = []
        failures = []
        thread.current_orders_ready.connect(lambda *_args: current.extend(_args[2]))
        thread.failed.connect(lambda *_args: failures.append(_args[2]))
        with patch.object(thread, "_load_history_orders", side_effect=RuntimeError("slow history failed")):
            thread.run()
        self.assertEqual(len(current), 1)
        self.assertEqual(failures, ["slow history failed"])

    @patch("roll_terminal_qt.shared_order_store.load_current_order_views", return_value=("ok", [_view()]))
    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    def test_normal_orders_arrive_before_slow_algo_and_history(self, client_type, _current) -> None:
        client = client_type.return_value
        client.get_pending_orders.return_value = [replace(_history_order(), state="live")]
        normal_emitted = threading.Event()
        completed_orders = threading.Event()

        def slow_algo(**_kwargs):
            self.assertTrue(normal_emitted.wait(2), "normal orders were blocked by algorithm orders")
            return []

        client._fetch_algo_orders.side_effect = slow_algo
        thread = SharedOrderRefreshThread(
            runtime=SimpleNamespace(environment="demo", credentials=object()), profile_name="moni",
        )
        partial = []
        thread.current_order_updates_ready.connect(lambda _p, _e, items: (partial.extend(items), normal_emitted.set()))
        thread.current_orders_ready.connect(lambda *_args: completed_orders.set())
        with (
            patch("roll_terminal_qt.shared_order_store.load_cached_order_history", return_value=[]),
            patch.object(thread, "_load_history_orders", side_effect=lambda _client: (completed_orders.wait(2) and [], {})),
        ):
            thread.run()
        self.assertEqual([item.ord_id for item in partial], ["order-1"])
        self.assertTrue(completed_orders.is_set())

    @patch("roll_terminal_qt.shared_order_store.load_current_order_views")
    @patch("roll_terminal_qt.shared_order_store.load_cached_order_history")
    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    def test_current_only_refresh_skips_history_and_does_not_publish_incomplete_snapshot(
        self, client_type, cached_history, load_current,
    ) -> None:
        client = client_type.return_value
        client.get_pending_orders.return_value = [replace(_history_order(), state="live")]

        def failed_algo(**kwargs):
            kwargs["errors"].append(("oco", "temporary error"))
            return []

        client._fetch_algo_orders.side_effect = failed_algo
        thread = SharedOrderRefreshThread(
            runtime=SimpleNamespace(environment="demo", credentials=object()),
            profile_name="moni", include_history=False,
        )
        partial, full, failures = [], [], []
        thread.current_order_updates_ready.connect(lambda _p, _e, items: partial.extend(items))
        thread.current_orders_ready.connect(lambda *_args: full.append(True))
        thread.failed.connect(lambda _p, _e, message: failures.append(message))
        with patch.object(thread, "_load_history_orders") as history:
            thread.run()
        self.assertEqual([item.ord_id for item in partial], ["order-1"])
        self.assertEqual(full, [])
        self.assertTrue(any("oco" in item and "temporary error" in item for item in failures))
        history.assert_not_called()
        cached_history.assert_not_called()
        load_current.assert_not_called()

    def test_algo_query_reports_failed_types_to_callers_requesting_completeness(self) -> None:
        from okx_quant.okx_client import OkxRestClient

        client = OkxRestClient()
        failures = []

        def request(_method, _path, **kwargs):
            if kwargs["params"]["ordType"] == "oco":
                raise RuntimeError("temporary error")
            return {"data": []}

        with patch.object(client, "_request", side_effect=request):
            result = client._fetch_algo_orders(
                credentials=object(), environment="demo", limit=20, history=False, errors=failures,
            )
        self.assertEqual(result, [])
        self.assertEqual(failures, [("oco", "temporary error")])

    def test_queued_current_refresh_does_not_drop_a_history_refresh_request(self) -> None:
        store = SharedOrderStore()
        runtime = SimpleNamespace(environment="demo", credentials=object())
        running = MagicMock()
        running.isRunning.return_value = True
        store._refresh_threads[("moni", "demo")] = running
        store.request_refresh(runtime=runtime, profile_name="moni", include_history=True)
        store.request_refresh(runtime=runtime, profile_name="moni", include_history=False)
        self.assertEqual(store._pending_refreshes[("moni", "demo")], (runtime, True))

    def test_publish_identical_history_snapshot_emits_only_once(self) -> None:
        store = SharedOrderStore()
        callback = MagicMock()
        store.snapshot_changed.connect(callback)
        order = MagicMock()

        store.publish_history_orders(
            profile_name="moni",
            environment="demo",
            orders=[order],
            usdt_prices={"USDT": Decimal("1")},
        )
        store.publish_history_orders(
            profile_name="moni",
            environment="demo",
            orders=[order],
            usdt_prices={"USDT": Decimal("1")},
        )

        self.assertEqual(callback.call_count, 1)

    @patch("roll_terminal_qt.history_service.save_history_cache_records")
    @patch("roll_terminal_qt.history_service.load_history_cache_records", return_value=[])
    @patch("roll_terminal_qt.shared_order_store.OkxRestClient")
    def test_shared_refresh_merges_remote_history_into_disk_cache(
        self,
        client_type: MagicMock,
        _load_cache: MagicMock,
        save_cache: MagicMock,
    ) -> None:
        remote = _history_order()
        client_type.return_value.get_order_history.return_value = [remote]
        runtime = SimpleNamespace(environment="demo", credentials=SimpleNamespace())
        thread = SharedOrderRefreshThread(runtime=runtime, profile_name="moni", limit=200)

        items, _prices = thread._load_history_orders()

        self.assertEqual([item.order_id for item in items], ["order-1"])
        save_cache.assert_called_once()

    def test_apply_cached_history_preserves_current_orders_and_publishes_history(self) -> None:
        store = SharedOrderStore()
        current = MagicMock()
        cached = _history_order()
        store._snapshots[("moni", "demo")] = SharedOrderSnapshot(current_order_views=(current,))
        callback = MagicMock()
        store.snapshot_changed.connect(callback)
        method = getattr(store, "_apply_cached_history", None)

        self.assertIsNotNone(method)
        method("moni", "demo", [cached])

        snapshot = store.snapshot_for(profile_name="moni", environment="demo")
        self.assertEqual(snapshot.current_order_views, (current,))
        self.assertEqual([item.order_id for item in snapshot.history_orders], ["order-1"])
        callback.assert_called_once()
