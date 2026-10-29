from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from PySide6.QtWidgets import QDialog
from PySide6.QtTest import QTest
from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget, _CurrentOrderTpSlDialog
from roll_terminal_qt.kline_account_drawer import KlineAccountDrawer
from roll_terminal_qt.order_actions import OrderActions


class OrderActionsTests(QtWidgetTestCase):
    def test_missing_metadata_updates_open_dialog_after_background_fetch(self):
        drawer = KlineAccountDrawer()
        try:
            item = SimpleNamespace(inst_id="BTC-USDT-SWAP", take_profit_trigger_price=Decimal("120"),
                                   stop_loss_trigger_price=Decimal("90"), side="sell", price=None)
            source = SimpleNamespace(_position_instruments={}, _current_order_instruments={})
            context = {"position_direction": "long", "quantity_text": "0.82 张", "holding_text": "0.82 张"}
            enriched = {"position_direction": "long", "quantity_text": "0.0082 BTC", "holding_text": "0.0082 BTC",
                        "cached_break_even": (Decimal("100.3"), "long", Decimal("0.001"))}
            def dialog_exec(dialog):
                dialog.show()
                for _ in range(100):
                    QTest.qWait(10)
                    if dialog._quantity_value.text() == "0.0082 BTC":
                        break
                self.assertEqual(dialog._quantity_value.text(), "0.0082 BTC")
                self.assertEqual(dialog._holding_value.text(), "0.0082 BTC")
                self.assertIn("预估保本价 100.3", dialog._break_even_note.text())
                dialog.hide()
                return QDialog.DialogCode.Rejected
            with (patch("roll_terminal_qt.order_actions.OkxRestClient") as client,
                  patch.object(drawer._order_actions, "_preview_context", return_value=enriched),
                  patch.object(_CurrentOrderTpSlDialog, "exec", dialog_exec)):
                client.return_value.get_instrument.return_value = object()
                drawer._order_actions._dialog(object(), item, context, source=source, order_view=object())
                client.return_value.get_instrument.assert_called_once_with(item.inst_id, prefer_cached=True)
        finally:
            self.dispose_widget(drawer)

    def test_kline_popup_uses_cached_contract_metadata_and_account_fee(self):
        from dataclasses import replace
        from tests.test_shared_order_store import _history_order
        drawer = KlineAccountDrawer()
        try:
            item = replace(_history_order(), source_kind="algo", ord_type="oco", side="sell", pos_side="net",
                           algo_id="a", order_id=None, size=Decimal("0.82"), stop_loss_order_price=Decimal("-1"),
                           raw={"algoId": "a"})
            position = SimpleNamespace(inst_id=item.inst_id, pos_side="net", position=Decimal("0.82"),
                                       avg_price=Decimal("80225"), raw={})
            instrument = SimpleNamespace(inst_type="SWAP", ct_val=Decimal("0.01"), ct_mult=Decimal("1"),
                                         ct_val_ccy="BTC", tick_size=Decimal("0.1"), settle_ccy="USDT")
            drawer._snapshot = SimpleNamespace(positions=(position,), position_instruments={}, position_tickers={})
            drawer.profile_snapshots_provider = lambda: {"moni": {"futures_taker_fee_rate": "0.036"}}
            runtime = SimpleNamespace(environment="demo", credentials=SimpleNamespace(profile_name="moni", api_key="test-key"))
            controller = drawer._order_actions
            controller.runtime_provider = lambda: runtime
            controller.selection_provider = lambda: item
            with patch("roll_terminal_qt.order_actions.OkxRestClient") as client, patch.object(controller, "_dialog") as dialog:
                client.return_value._get_cached_instrument.return_value = instrument
                controller.edit()
                context = dialog.call_args.args[2]
                self.assertEqual(context["quantity_text"], "0.0082 BTC")
                self.assertEqual(context["contract_value"], Decimal("0.01"))
                self.assertEqual(context["cached_break_even"][0], Decimal("80282.8"))
        finally:
            self.dispose_widget(drawer)
    def test_cached_break_even_fills_immediately_without_network(self):
        drawer = KlineAccountDrawer()
        try:
            item = SimpleNamespace(take_profit_trigger_price=Decimal("120"), stop_loss_trigger_price=Decimal("90"), side="sell", price=None)
            def dialog_exec(dialog):
                self.assertIn("预估保本价 100.3", dialog._break_even_note.text())
                dialog._break_even_button.click()
                self.assertEqual(dialog._stop_loss_edit.text(), "100.3")
                self.assertTrue(dialog._break_even_requested)
                return QDialog.DialogCode.Rejected
            with patch.object(drawer._order_actions, "_run") as run, patch.object(_CurrentOrderTpSlDialog, "exec", dialog_exec):
                drawer._order_actions._dialog(object(), item, {"position_direction": "long", "cached_break_even": (Decimal("100.3"), "long", Decimal("0.001"))})
                run.assert_not_called()
        finally:
            self.dispose_widget(drawer)

    def test_cached_break_even_does_not_weaken_existing_stop(self):
        drawer = KlineAccountDrawer()
        try:
            item = SimpleNamespace(take_profit_trigger_price=Decimal("120"), stop_loss_trigger_price=Decimal("101"), side="sell", price=None)
            def dialog_exec(dialog):
                dialog._break_even_button.click()
                self.assertEqual(dialog._stop_loss_edit.text(), "101")
                self.assertFalse(dialog._break_even_requested)
                return QDialog.DialogCode.Rejected
            with patch.object(_CurrentOrderTpSlDialog, "exec", dialog_exec):
                drawer._order_actions._dialog(object(), item, {"position_direction": "long", "cached_break_even": (Decimal("100.3"), "long", Decimal("0.001"))})
        finally:
            self.dispose_widget(drawer)

    def test_open_dialog_uses_cached_data_without_waiting_for_network(self):
        from tests.test_shared_order_store import _history_order
        drawer = KlineAccountDrawer()
        try:
            controller = drawer._order_actions
            runtime = SimpleNamespace(environment="demo", credentials=object())
            item = _history_order()
            controller.runtime_provider = lambda: runtime
            controller.selection_provider = lambda: item
            with patch.object(controller, "_run") as worker, patch.object(controller, "_dialog") as dialog:
                controller.edit()
                worker.assert_not_called()
                self.assertIs(dialog.call_args.args[1], item)
                self.assertIs(dialog.call_args.args[0], runtime)
        finally:
            self.dispose_widget(drawer)

    def test_both_views_route_edit_to_the_shared_controller(self):
        home = SimpleNamespace(_order_actions=Mock())
        AccountPositionsHomeWidget._amend_selected_current_order(home)
        home._order_actions.edit.assert_called_once()
        drawer = KlineAccountDrawer()
        try:
            self.assertIsInstance(drawer._order_actions, OrderActions)
            with patch.object(drawer._order_actions, "edit") as edit:
                drawer._amend_button.click()
                edit.assert_called_once()
        finally:
            self.dispose_widget(drawer)

    def test_break_even_button_prepares_price_and_submit_uses_guarded_execution(self):
        drawer = KlineAccountDrawer()
        try:
            controller = drawer._order_actions
            item = SimpleNamespace(take_profit_trigger_price=Decimal("120"), stop_loss_trigger_price=Decimal("90"), side="sell", price=None)
            runtime = object()
            def execute(action, completed, failed=None):
                completed(action(Mock()))
            def dialog_exec(dialog):
                dialog._break_even_button.click()
                self.assertEqual(dialog._stop_loss_edit.text(), "100.3")
                self.assertTrue(dialog._break_even_requested)
                return QDialog.DialogCode.Accepted
            with (
                patch.object(controller, "_run", side_effect=execute),
                patch.object(controller, "_amended") as amended,
                patch.object(_CurrentOrderTpSlDialog, "exec", dialog_exec),
                patch("roll_terminal_qt.order_actions.prepare_break_even", return_value=(item, Decimal("100.3"), "long", Decimal("0.001"))),
                patch("roll_terminal_qt.order_actions.amend_protection", return_value=item) as submit,
            ):
                controller._dialog(runtime, item, {"position_direction": "long"})
                self.assertTrue(submit.call_args.kwargs["break_even"])
                self.assertIsNone(submit.call_args.args[3])
                self.assertEqual(submit.call_args.args[4], Decimal("100.3"))
                amended.assert_called_once_with(runtime, item)
        finally:
            self.dispose_widget(drawer)
