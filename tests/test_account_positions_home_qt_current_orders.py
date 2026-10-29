from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch

from roll_terminal_qt import account_positions_home as account_positions_home_module
from roll_terminal_qt.account_positions_home import (
    AccountPositionsHomeWidget,
    _current_order_state_is_terminal,
    _current_order_view_cancel_reference,
    _current_order_cancel_result_error_message,
    _current_order_cancel_result_failed,
    _current_order_view_owner_display_label,
    _current_order_view_program_owner_label,
    _current_order_view_to_trade_order_item,
)
from okx_quant.okx_client import OkxOrderResult
from roll_terminal_qt.order_service import OrderStatusView
from PySide6.QtCore import QTimer
from PySide6.QtTest import QTest
from tests.qt_test_case import QtWidgetTestCase


class AccountPositionsHomeQtCurrentOrderHelpersTest(TestCase):
    def test_missing_instrument_never_triggers_network_during_table_render(self) -> None:
        client = MagicMock()
        client._get_cached_instrument.return_value = None
        timer = MagicMock()
        timer.isActive.return_value = False
        home = SimpleNamespace(_position_instruments={}, _shared_client=client,
            _order_instrument_attempted=set(), _order_instrument_pending=set(), _order_instrument_timer=timer)
        home._cached_order_instrument = lambda inst_id: AccountPositionsHomeWidget._cached_order_instrument(home, inst_id)
        order = SimpleNamespace(inst_id="BTC-USDT-SWAP", inst_type="SWAP", size=Decimal("0.82"),
                                filled_size=Decimal("0"), price=Decimal("80000"), avg_price=None)
        AccountPositionsHomeWidget._format_current_order_size_with_coin(home, order, filled=False)
        AccountPositionsHomeWidget._format_current_order_size_with_coin(home, order, filled=True)
        AccountPositionsHomeWidget._format_order_history_size_with_coin(home, order, filled=False)
        client.get_instrument.assert_not_called()
        self.assertEqual(home._order_instrument_pending, {"BTC-USDT-SWAP"})
        timer.start.assert_called_once_with(0)

    def test_background_instrument_result_refreshes_both_order_tables(self) -> None:
        home = SimpleNamespace(_shutdown_in_progress=False, _current_order_instruments={},
            _refresh_current_orders_table=MagicMock(), _refresh_order_history_table=MagicMock())
        instrument = object()
        AccountPositionsHomeWidget._apply_order_instruments(home, {"BTC-USDT-SWAP": instrument})
        self.assertIs(home._current_order_instruments["BTC-USDT-SWAP"], instrument)
        home._refresh_current_orders_table.assert_called_once()
        home._refresh_order_history_table.assert_called_once()


class AccountInstrumentLoadingResponsivenessTest(QtWidgetTestCase):
    def test_slow_metadata_query_runs_in_background_and_updates_the_cache(self):
        started, release = threading.Event(), threading.Event()
        client = MagicMock()
        client._get_cached_instrument.return_value = None
        instrument = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        def slow_lookup(*_args, **_kwargs):
            started.set()
            if not release.wait(3):
                raise TimeoutError("test timed out")
            return instrument
        client.get_instrument.side_effect = slow_lookup
        home = None
        with (patch("roll_terminal_qt.account_positions_home.load_runtime", return_value=None),
              patch("roll_terminal_qt.account_positions_home.OkxRestClient", return_value=client)):
            try:
                home = AccountPositionsHomeWidget()
                order = SimpleNamespace(inst_id="BTC-USDT-SWAP", inst_type="SWAP", size=Decimal("1"),
                                        filled_size=Decimal("0"), price=Decimal("80000"), avg_price=None)
                home._format_current_order_size_with_coin(order, filled=False)
                responsive = []
                QTimer.singleShot(50, lambda: responsive.append(True))
                QTest.qWait(150)
                self.assertTrue(started.is_set())
                self.assertEqual(responsive, [True])
                self.assertFalse(release.is_set())
                release.set()
                for _ in range(100):
                    QTest.qWait(10)
                    if home._order_instrument_thread is None:
                        break
                self.assertIsNone(home._order_instrument_thread)
                self.assertIs(home._current_order_instruments[order.inst_id], instrument)
            finally:
                release.set()
                if home is not None:
                    home.shutdown()
                    self.dispose_widget(home)


class AccountPositionsHomeQtCurrentOrderRemainingTest(TestCase):
    def test_net_position_direction_uses_signed_holding_not_protection_order_side(self) -> None:
        order = SimpleNamespace(inst_id="BTC-USDT-SWAP", pos_side="net", side="sell", raw={})
        item = SimpleNamespace(inst_id=order.inst_id, pos_side="net", side="sell", inst_type="SWAP", price=None, size=Decimal("1"))
        position = SimpleNamespace(inst_id=order.inst_id, pos_side="net", position=Decimal("1"), avg_price=Decimal("80225"), raw={})
        home = SimpleNamespace(_raw_positions=[position], _position_instruments={}, _current_order_instruments={}, _position_tickers={})
        context = AccountPositionsHomeWidget._current_order_tp_sl_preview_context(home, order, item)
        self.assertEqual(context["position_direction"], "long")
        position.position = Decimal("-1")
        context = AccountPositionsHomeWidget._current_order_tp_sl_preview_context(home, order, item)
        self.assertEqual(context["position_direction"], "short")
        home._raw_positions = []
        context = AccountPositionsHomeWidget._current_order_tp_sl_preview_context(home, order, item)
        self.assertEqual(context["position_direction"], "")

    def test_estimated_pnl_requires_known_direction_and_is_mirrored_for_short(self) -> None:
        dialog_type = account_positions_home_module._CurrentOrderTpSlDialog
        context = {"entry_price": Decimal("80225"), "quantity": Decimal("0.82"),
                   "contract_value": Decimal("0.01"), "contract_value_currency": "BTC",
                   "base_currency": "BTC", "instrument_type": "SWAP", "pnl_currency": "USDT"}
        dialog = SimpleNamespace(_preview_context=context, _decimal_value=dialog_type._decimal_value,
                                 _format_amount=dialog_type._format_amount)
        self.assertIn("无法确定持仓方向", dialog_type._estimated_pnl_text(dialog, "止盈", Decimal("82966.1")))
        context["position_direction"] = "long"
        self.assertIn("+22.48", dialog_type._estimated_pnl_text(dialog, "止盈", Decimal("82966.1")))
        context["position_direction"] = "short"
        self.assertIn("-22.48", dialog_type._estimated_pnl_text(dialog, "止盈", Decimal("82966.1")))
    def test_current_order_view_to_trade_order_item_preserves_algo_fields(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=1,
            update_time=2,
            client_order_id="abcdexi123456789012345",
            reduce_only=True,
            raw={
                "_source_kind": "algo",
                "algoId": "987654321",
                "algoClOrdId": "abcdexi123456789012345",
            },
        )

        item = _current_order_view_to_trade_order_item(order)

        self.assertEqual(item.source_kind, "algo")
        self.assertEqual(item.algo_id, "987654321")
        self.assertEqual(item.algo_client_order_id, "abcdexi123456789012345")
        self.assertEqual(item.client_order_id, "abcdexi123456789012345")

    def test_current_order_view_to_trade_order_item_preserves_trigger_and_tp_sl_fields(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="algo-123",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="oco",
            state="effective",
            price=Decimal("65000"),
            avg_price=None,
            size=Decimal("2.6"),
            filled_size=Decimal("0"),
            created_time=1,
            update_time=2,
            client_order_id="algo-client-123",
            reduce_only=True,
            raw={
                "_source_kind": "algo",
                "algoId": "algo-123",
                "algoClOrdId": "algo-client-123",
                "triggerPx": "64999.5",
                "triggerPxType": "mark",
                "orderPx": "64998",
                "actualPx": "64997.5",
                "actualSz": "1.2",
                "actualSide": "sell",
                "tpTriggerPx": "63754",
                "tpOrdPx": "-1",
                "tpTriggerPxType": "mark",
                "slTriggerPx": "64921.4",
                "slOrdPx": "-1",
                "slTriggerPxType": "mark",
            },
        )

        item = _current_order_view_to_trade_order_item(order)

        self.assertEqual(item.trigger_price, Decimal("64999.5"))
        self.assertEqual(item.trigger_price_type, "mark")
        self.assertEqual(item.order_price, Decimal("64998"))
        self.assertEqual(item.actual_price, Decimal("64997.5"))
        self.assertEqual(item.actual_size, Decimal("1.2"))
        self.assertEqual(item.actual_side, "sell")
        self.assertEqual(item.take_profit_trigger_price, Decimal("63754"))
        self.assertEqual(item.take_profit_order_price, Decimal("-1"))
        self.assertEqual(item.take_profit_trigger_price_type, "mark")
        self.assertEqual(item.stop_loss_trigger_price, Decimal("64921.4"))
        self.assertEqual(item.stop_loss_order_price, Decimal("-1"))
        self.assertEqual(item.stop_loss_trigger_price_type, "mark")

    def test_current_order_state_is_terminal_matches_current_tab_expectation(self) -> None:
        self.assertTrue(_current_order_state_is_terminal("canceled"))
        self.assertTrue(_current_order_state_is_terminal("filled"))
        self.assertTrue(_current_order_state_is_terminal("order_failed"))
        self.assertFalse(_current_order_state_is_terminal("live"))
        self.assertFalse(_current_order_state_is_terminal("effective"))

    def test_current_order_view_cancel_reference_prefers_algo_id_for_algo_orders(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="ordinary-ord-id",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="oco",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="abcdent123456789012345",
            reduce_only=None,
            raw={
                "_source_kind": "algo",
                "algoId": "algo-123",
                "algoClOrdId": "abcdent123456789012345",
            },
        )

        self.assertEqual(_current_order_view_cancel_reference(order), "algo-123")

    def test_current_order_view_program_owner_label_uses_existing_trade_order_patterns(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="123",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="limit",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="abcdent123456789012345",
            reduce_only=None,
            raw={"_source_kind": "normal"},
        )

        self.assertEqual(_current_order_view_program_owner_label(order), "策略引擎")

    def test_cancel_selected_current_order_request_uses_algo_endpoint_for_algo_orders(self) -> None:
        shared_client = SimpleNamespace(
            cancel_algo_order=MagicMock(return_value="algo-result"),
            cancel_order_by_id=MagicMock(),
        )
        app = SimpleNamespace(_shared_client=shared_client)
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="",
            side="sell",
            pos_side="long",
            td_mode="cross",
            ord_type="conditional",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="abcdexi123456789012345",
            reduce_only=None,
            raw={"_source_kind": "algo", "algoId": "algo-123", "algoClOrdId": "abcdexi123456789012345"},
        )

        result = AccountPositionsHomeWidget._cancel_selected_current_order_request(
            app,
            credentials=SimpleNamespace(),
            environment="live",
            order=order,
        )

        self.assertEqual(result, "algo-result")
        shared_client.cancel_algo_order.assert_called_once()
        shared_client.cancel_order_by_id.assert_not_called()

    def test_cancel_selected_current_order_request_uses_normal_endpoint_for_normal_orders(self) -> None:
        shared_client = SimpleNamespace(
            cancel_algo_order=MagicMock(),
            cancel_order_by_id=MagicMock(return_value="normal-result"),
        )
        app = SimpleNamespace(_shared_client=shared_client)
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="ord-123",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="limit",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="abcdent123456789012345",
            reduce_only=None,
            raw={"_source_kind": "normal"},
        )

        result = AccountPositionsHomeWidget._cancel_selected_current_order_request(
            app,
            credentials=SimpleNamespace(),
            environment="demo",
            order=order,
        )

        self.assertEqual(result, "normal-result")
        shared_client.cancel_order_by_id.assert_called_once()
        shared_client.cancel_algo_order.assert_not_called()

    def test_current_order_cancel_result_failed_uses_s_code(self) -> None:
        self.assertFalse(_current_order_cancel_result_failed(OkxOrderResult(ord_id="1", cl_ord_id="a", s_code="0", s_msg="", raw={})))
        self.assertTrue(
            _current_order_cancel_result_failed(
                OkxOrderResult(ord_id="", cl_ord_id=None, s_code="50120", s_msg="permission denied", raw={})
            )
        )

    def test_current_order_cancel_result_error_message_contains_okx_error(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="123",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="limit",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="abcdent123456789012345",
            reduce_only=None,
            raw={"_source_kind": "normal"},
        )
        result = OkxOrderResult(ord_id="", cl_ord_id=None, s_code="50120", s_msg="You are using a read-only API key", raw={})

        message = _current_order_cancel_result_error_message(order, result)

        self.assertIn("BTC-USDT-SWAP", message)
        self.assertIn("sCode=50120", message)
        self.assertIn("read-only API key", message)

    def test_current_order_view_owner_display_label_falls_back_for_unrecognized_order(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="ord-123",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="limit",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="manual-order-001",
            reduce_only=None,
            raw={"_source_kind": "normal"},
        )

        self.assertEqual(_current_order_view_owner_display_label(order), "未识别来源")

    def test_cancel_selected_current_order_allows_unrecognized_owner_when_order_id_exists(self) -> None:
        order = OrderStatusView(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            ord_id="ord-123",
            side="buy",
            pos_side="long",
            td_mode="cross",
            ord_type="limit",
            state="live",
            price=None,
            avg_price=None,
            size=None,
            filled_size=None,
            created_time=None,
            update_time=None,
            client_order_id="manual-order-001",
            reduce_only=None,
            raw={"_source_kind": "normal"},
        )
        app = SimpleNamespace(
            _current_order_canceling=False,
            _ensure_runtime_ready=lambda force_unlock=False: True,
            _selected_current_order=lambda: order,
            _runtime=SimpleNamespace(credentials=SimpleNamespace()),
            _note_environment=lambda: "live",
            _orders_summary_label=SimpleNamespace(setText=MagicMock()),
            _cancel_selected_current_order_worker=MagicMock(),
        )

        with (
            patch("roll_terminal_qt.account_positions_home.QMessageBox.information") as info,
            patch("roll_terminal_qt.account_positions_home.QMessageBox.question") as question,
            patch("roll_terminal_qt.account_positions_home.threading.Thread") as thread_cls,
        ):
            question.return_value = account_positions_home_module.QMessageBox.StandardButton.Yes
            thread = MagicMock()
            thread_cls.return_value = thread

            AccountPositionsHomeWidget._cancel_selected_current_order(app)

        info.assert_not_called()
        self.assertTrue(app._current_order_canceling)
        app._orders_summary_label.setText.assert_called_once()
        thread_cls.assert_called_once()
        self.assertEqual(thread_cls.call_args.kwargs["args"][3], "未识别来源")
        thread.start.assert_called_once()
