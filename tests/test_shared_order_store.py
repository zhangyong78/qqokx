from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.okx_client import OkxTradeOrderItem
from roll_terminal_qt.shared_order_store import SharedOrderRefreshThread, SharedOrderSnapshot, SharedOrderStore


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


class SharedOrderStoreTest(TestCase):
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
