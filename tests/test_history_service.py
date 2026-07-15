from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.okx_client import OkxFillHistoryItem, OkxPositionHistoryItem, OkxTradeOrderItem
from roll_terminal_qt.history_service import FillHistoryFeedThread, OrderHistoryFeedThread, PositionHistoryFeedThread


def _order(order_id: str, *, update_time: int) -> OkxTradeOrderItem:
    return OkxTradeOrderItem(
        source_kind="normal",
        source_label="普通委托",
        created_time=update_time - 1,
        update_time=update_time,
        inst_id="BTC-USDT-SWAP",
        inst_type="SWAP",
        side="buy",
        pos_side="long",
        td_mode="cross",
        ord_type="limit",
        state="canceled",
        price=Decimal("60000"),
        size=Decimal("1"),
        filled_size=Decimal("0"),
        avg_price=None,
        order_id=order_id,
        algo_id=None,
        client_order_id=None,
        algo_client_order_id=None,
        pnl=None,
        fee=None,
        fee_currency=None,
        reduce_only=False,
        trigger_price=None,
        trigger_price_type=None,
        order_price=None,
        actual_price=None,
        actual_size=None,
        actual_side=None,
        take_profit_trigger_price=None,
        take_profit_order_price=None,
        take_profit_trigger_price_type=None,
        stop_loss_trigger_price=None,
        stop_loss_order_price=None,
        stop_loss_trigger_price_type=None,
        raw={},
    )


def _record(item: OkxTradeOrderItem) -> dict[str, object]:
    return {
        "source_kind": item.source_kind,
        "source_label": item.source_label,
        "created_time": item.created_time,
        "update_time": item.update_time,
        "inst_id": item.inst_id,
        "inst_type": item.inst_type,
        "side": item.side,
        "pos_side": item.pos_side,
        "td_mode": item.td_mode,
        "ord_type": item.ord_type,
        "state": item.state,
        "price": str(item.price),
        "size": str(item.size),
        "filled_size": str(item.filled_size),
        "order_id": item.order_id,
        "reduce_only": item.reduce_only,
        "raw": {},
    }


class OrderHistoryFeedThreadTest(TestCase):
    def _runtime(self) -> SimpleNamespace:
        return SimpleNamespace(
            credential_profile_name="moni",
            environment="demo",
            credentials=SimpleNamespace(),
        )

    @patch("roll_terminal_qt.history_service.save_history_cache_records")
    @patch("roll_terminal_qt.history_service.load_history_cache_records")
    @patch("roll_terminal_qt.history_service.OkxRestClient")
    def test_run_emits_cached_orders_before_remote_sync(
        self,
        client_type: MagicMock,
        load_cache: MagicMock,
        _save_cache: MagicMock,
    ) -> None:
        cached = _order("cached-1", update_time=10)
        remote = _order("remote-1", update_time=20)
        load_cache.return_value = [_record(cached)]
        client_type.return_value.get_order_history.return_value = [remote]
        payloads: list[dict[str, object]] = []
        thread = OrderHistoryFeedThread(self._runtime(), limit=200)
        thread.data_ready.connect(payloads.append)

        thread.run()

        self.assertEqual([item.order_id for item in payloads[0]["items"]], ["cached-1"])
        self.assertEqual([item.order_id for item in payloads[-1]["items"]], ["remote-1", "cached-1"])

    @patch("roll_terminal_qt.history_service.save_history_cache_records")
    @patch("roll_terminal_qt.history_service.load_history_cache_records")
    @patch("roll_terminal_qt.history_service.OkxRestClient")
    def test_run_merges_remote_orders_into_local_cache(
        self,
        client_type: MagicMock,
        load_cache: MagicMock,
        save_cache: MagicMock,
    ) -> None:
        cached = _order("cached-1", update_time=10)
        remote = _order("remote-1", update_time=20)
        load_cache.return_value = [_record(cached)]
        client = client_type.return_value
        client.get_order_history.return_value = [remote]
        thread = OrderHistoryFeedThread(self._runtime(), limit=200)

        thread.run()

        client.get_order_history.assert_called_once_with(
            self._runtime().credentials,
            environment="demo",
            limit=200,
            include_algo=True,
        )
        saved_records = save_cache.call_args.args[3]
        self.assertEqual({record["order_id"] for record in saved_records}, {"cached-1", "remote-1"})

    @patch("roll_terminal_qt.history_service.save_history_cache_records")
    @patch("roll_terminal_qt.history_service.load_history_cache_records")
    @patch("roll_terminal_qt.history_service.OkxRestClient")
    def test_run_keeps_cached_orders_when_remote_sync_fails(
        self,
        client_type: MagicMock,
        load_cache: MagicMock,
        save_cache: MagicMock,
    ) -> None:
        cached = _order("cached-1", update_time=10)
        load_cache.return_value = [_record(cached)]
        client_type.return_value.get_order_history.side_effect = RuntimeError("network down")
        payloads: list[dict[str, object]] = []
        statuses: list[str] = []
        thread = OrderHistoryFeedThread(self._runtime(), limit=200)
        thread.data_ready.connect(payloads.append)
        thread.status_changed.connect(statuses.append)

        thread.run()

        self.assertEqual([item.order_id for item in payloads[-1]["items"]], ["cached-1"])
        self.assertTrue(any("本地缓存" in status for status in statuses))
        save_cache.assert_not_called()


class OtherHistoryFeedThreadTest(TestCase):
    def _runtime(self) -> SimpleNamespace:
        return SimpleNamespace(
            credential_profile_name="moni",
            environment="demo",
            credentials=SimpleNamespace(),
        )

    @patch("roll_terminal_qt.history_service.save_history_cache_records")
    @patch("roll_terminal_qt.history_service.load_history_cache_records")
    @patch("roll_terminal_qt.history_service.OkxRestClient")
    def test_fill_history_emits_cache_before_remote_sync(
        self,
        client_type: MagicMock,
        load_cache: MagicMock,
        _save_cache: MagicMock,
    ) -> None:
        cached = OkxFillHistoryItem(
            fill_time=10, inst_id="BTC-USDT-SWAP", inst_type="SWAP", side="buy", pos_side="long",
            fill_price=Decimal("60000"), fill_size=Decimal("1"), fill_fee=None, fee_currency=None,
            pnl=None, order_id="cached-order", trade_id="cached-trade", exec_type="T", raw={},
        )
        remote = OkxFillHistoryItem(
            fill_time=20, inst_id="BTC-USDT-SWAP", inst_type="SWAP", side="sell", pos_side="long",
            fill_price=Decimal("61000"), fill_size=Decimal("1"), fill_fee=None, fee_currency=None,
            pnl=None, order_id="remote-order", trade_id="remote-trade", exec_type="T", raw={},
        )
        load_cache.return_value = [
            {
                "fill_time": cached.fill_time, "inst_id": cached.inst_id, "inst_type": cached.inst_type,
                "side": cached.side, "pos_side": cached.pos_side, "fill_price": str(cached.fill_price),
                "fill_size": str(cached.fill_size), "order_id": cached.order_id, "trade_id": cached.trade_id,
                "exec_type": cached.exec_type, "raw": {},
            }
        ]
        client_type.return_value.get_fills_history.return_value = [remote]
        payloads: list[dict[str, object]] = []
        thread = FillHistoryFeedThread(self._runtime(), limit=100)
        thread.data_ready.connect(payloads.append)

        thread.run()

        self.assertEqual([item.trade_id for item in payloads[0]["items"]], ["cached-trade"])
        self.assertEqual([item.trade_id for item in payloads[-1]["items"]], ["remote-trade", "cached-trade"])

    @patch.object(PositionHistoryFeedThread, "_build_usdt_prices", return_value={})
    @patch.object(PositionHistoryFeedThread, "_build_instrument_map", return_value={})
    @patch.object(PositionHistoryFeedThread, "_merge_position_history_cache")
    @patch.object(PositionHistoryFeedThread, "_load_local_position_history")
    @patch("roll_terminal_qt.history_service.OkxRestClient")
    def test_position_history_emits_cache_before_remote_sync(
        self,
        client_type: MagicMock,
        load_local: MagicMock,
        merge_cache: MagicMock,
        _build_instruments: MagicMock,
        _build_prices: MagicMock,
    ) -> None:
        cached = OkxPositionHistoryItem(
            update_time=10, inst_id="BTC-USDT-SWAP", inst_type="SWAP", mgn_mode="cross",
            pos_side="long", direction="long", open_avg_price=Decimal("60000"),
            close_avg_price=Decimal("61000"), close_size=Decimal("1"), pnl=Decimal("1000"),
            realized_pnl=None, settle_pnl=None, raw={},
        )
        remote = OkxPositionHistoryItem(
            update_time=20, inst_id="ETH-USDT-SWAP", inst_type="SWAP", mgn_mode="cross",
            pos_side="long", direction="long", open_avg_price=Decimal("3000"),
            close_avg_price=Decimal("3100"), close_size=Decimal("1"), pnl=Decimal("100"),
            realized_pnl=None, settle_pnl=None, raw={},
        )
        load_local.return_value = [cached]
        merge_cache.return_value = [remote, cached]
        client_type.return_value.get_positions_history.return_value = [remote]
        payloads: list[dict[str, object]] = []
        thread = PositionHistoryFeedThread(self._runtime(), limit=120)
        thread.data_ready.connect(payloads.append)

        thread.run()

        self.assertEqual([item.inst_id for item in payloads[0]["items"]], ["BTC-USDT-SWAP"])
        self.assertEqual([item.inst_id for item in payloads[-1]["items"]], ["ETH-USDT-SWAP", "BTC-USDT-SWAP"])
