from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
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


class AccountPositionsHomeQtCurrentOrderHelpersTest(TestCase):
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
