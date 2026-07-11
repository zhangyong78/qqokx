from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch
from unittest import TestCase

from PySide6.QtWidgets import QAbstractItemView, QMessageBox
from PySide6.QtCore import Qt

from roll_terminal_qt.kline_account_drawer import (
    AccountDrawerCancelThread,
    AccountDrawerLoadThread,
    AccountDrawerSnapshot,
    KlineAccountDrawer,
    filter_account_items,
    order_cancel_reference,
    order_source_kind,
)
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from roll_terminal_qt.shared_order_store import SharedOrderSnapshot
from tests.qt_test_case import QtWidgetTestCase


class KlineAccountDrawerHelperTests(TestCase):
    def test_filter_account_items_uses_normalized_symbol_and_all_scope(self) -> None:
        btc = SimpleNamespace(inst_id="BTC-USDT-SWAP")
        eth = SimpleNamespace(inst_id="ETH-USDT-SWAP")

        self.assertEqual(
            filter_account_items([btc, eth], scope="symbol", symbol="btc-usdt-swap"),
            [btc],
        )
        self.assertEqual(
            filter_account_items([btc, eth], scope="all", symbol="btc-usdt-swap"),
            [btc, eth],
        )

    def test_order_source_and_cancel_reference_support_normal_and_algo_orders(self) -> None:
        normal = SimpleNamespace(
            source_kind="normal",
            order_id="ord-1",
            client_order_id="",
            algo_id="",
            algo_client_order_id="",
        )
        algo = SimpleNamespace(
            source_kind="algo",
            order_id="",
            client_order_id="",
            algo_id="algo-1",
            algo_client_order_id="",
        )

        self.assertEqual(order_source_kind(normal), "normal")
        self.assertEqual(order_cancel_reference(normal), "ord-1")
        self.assertEqual(order_source_kind(algo), "algo")
        self.assertEqual(order_cancel_reference(algo), "algo-1")


class KlineAccountDrawerWidgetTests(QtWidgetTestCase):
    def test_drawer_defaults_to_symbol_scope_and_read_only_positions(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            self.assertEqual(drawer._scope_combo.currentData(), "symbol")
            self.assertEqual(drawer._tabs.tabText(0), "当前委托")
            self.assertEqual(drawer._tabs.tabText(1), "当前持仓")
            self.assertEqual(drawer._tabs.tabText(2), "历史委托")
            self.assertEqual(
                drawer._positions_table.editTriggers(),
                QAbstractItemView.EditTrigger.NoEditTriggers,
            )
            self.assertFalse(hasattr(drawer, "_flatten_button"))
        finally:
            self.dispose_widget(drawer)

    def test_stale_snapshot_and_load_error_keep_current_data(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            current = AccountDrawerSnapshot(
                positions=(
                    SimpleNamespace(
                        inst_id="ETH-USDT-SWAP",
                        pos_side="long",
                        position=Decimal("1"),
                        avail_position=Decimal("1"),
                        avg_price=Decimal("100"),
                        mark_price=Decimal("101"),
                        unrealized_pnl=Decimal("1"),
                        mgn_mode="cross",
                        raw={},
                    ),
                ),
            )
            drawer._request_generation = 2
            drawer._snapshot = current
            drawer._apply_snapshot(
                1,
                AccountDrawerSnapshot(
                    positions=(
                        SimpleNamespace(
                            inst_id="BTC-USDT-SWAP",
                            pos_side="long",
                            position=Decimal("1"),
                            avail_position=Decimal("1"),
                            avg_price=Decimal("100"),
                            mark_price=Decimal("101"),
                            unrealized_pnl=Decimal("1"),
                            mgn_mode="cross",
                            raw={},
                        ),
                    ),
                ),
            )
            drawer._apply_load_error(2, "network error")

            self.assertIs(drawer._snapshot, current)
            self.assertIn("network error", drawer._status_label.text())
        finally:
            self.dispose_widget(drawer)

    def test_refresh_data_defers_new_request_until_running_load_finishes(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            drawer._runtime = SimpleNamespace(credentials=object(), environment="demo")
            running_thread = Mock()
            running_thread.isRunning.return_value = True
            running_thread.deleteLater = Mock()
            drawer._load_thread = running_thread
            drawer._request_generation = 4

            drawer.refresh_data()

            self.assertEqual(drawer._request_generation, 5)
            self.assertTrue(drawer._refresh_pending)
            with patch.object(drawer, "_start_load") as start_load:
                drawer._clear_load_thread()
            start_load.assert_called_once_with(5)
        finally:
            self.dispose_widget(drawer)

    @patch("roll_terminal_qt.kline_account_drawer.get_shared_order_store")
    def test_refresh_data_requests_shared_order_store_refresh(self, get_shared_order_store: Mock) -> None:
        shared_order_store = Mock()
        get_shared_order_store.return_value = shared_order_store
        drawer = KlineAccountDrawer()
        try:
            drawer._runtime = SimpleNamespace(credentials=object(), environment="demo")
            drawer._profile_name = "moni"

            with patch.object(drawer, "_start_load") as start_load:
                drawer.refresh_data()

            shared_order_store.request_refresh.assert_called_once_with(
                runtime=drawer._runtime,
                profile_name="moni",
            )
            start_load.assert_called_once_with(1)
        finally:
            self.dispose_widget(drawer)

    def test_orders_table_shows_algo_tp_sl_and_client_identifiers(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            algo_order = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                source_kind="algo",
                source_label="算法委托",
                side="sell",
                pos_side="long",
                ord_type="oco",
                trigger_price=None,
                order_price=None,
                price=None,
                size=Decimal("2.6"),
                filled_size=Decimal("0"),
                state="live",
                update_time=1783692698837,
                created_time=1783692698837,
                algo_id="3730927321386143744",
                client_order_id="rrsto77e9a3508b0457d3688e9631",
                algo_client_order_id="rrsto77e9a3508b0457d3688e9631",
                take_profit_trigger_price=Decimal("65000"),
                take_profit_order_price=Decimal("64950"),
                stop_loss_trigger_price=Decimal("59000"),
                stop_loss_order_price=Decimal("58950"),
                inst_type="SWAP",
            )
            drawer._symbol = "BTC-USDT-SWAP"
            drawer._snapshot = AccountDrawerSnapshot(orders=(algo_order,))
            drawer._refresh_tables()

            headers = [
                drawer._orders_table.horizontalHeaderItem(index).text()
                for index in range(drawer._orders_table.columnCount())
            ]
            self.assertIn("TP/SL", headers)
            self.assertIn("clOrdId", headers)
            tp_sl_col = headers.index("TP/SL")
            order_id_col = headers.index("订单ID")
            cl_ord_col = headers.index("clOrdId")
            direction_col = headers.index("方向")

            self.assertIn("TP 65000", drawer._orders_table.item(0, tp_sl_col).text())
            self.assertIn("SL 59000", drawer._orders_table.item(0, tp_sl_col).text())
            self.assertEqual(drawer._orders_table.item(0, order_id_col).text(), "3730927321386143744")
            self.assertEqual(drawer._orders_table.item(0, cl_ord_col).text(), "rrsto77e9a3508b0457d3688e9631")
            self.assertEqual(drawer._orders_table.item(0, direction_col).text(), "卖出")
        finally:
            self.dispose_widget(drawer)

    def test_positions_table_shows_chinese_direction_labels(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            position = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                pos_side="long",
                position=Decimal("1"),
                avail_position=Decimal("1"),
                avg_price=Decimal("100"),
                mark_price=Decimal("101"),
                unrealized_pnl=Decimal("1"),
                mgn_mode="cross",
                raw={},
            )
            drawer._symbol = "BTC-USDT-SWAP"
            drawer._snapshot = AccountDrawerSnapshot(positions=(position,))
            drawer._refresh_tables()

            self.assertEqual(drawer._positions_table.item(0, 1).text(), "买入")
        finally:
            self.dispose_widget(drawer)

    def test_history_orders_tab_shows_canceled_orders(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            history_order = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                source_kind="normal",
                source_label="普通委托",
                side="sell",
                pos_side="long",
                ord_type="limit",
                trigger_price=None,
                order_price=None,
                price=Decimal("64616.1"),
                size=Decimal("9.87"),
                filled_size=Decimal("0"),
                state="canceled",
                update_time=1783764489406,
                created_time=1783764489406,
                order_id="3733336213178388480",
                client_order_id="rrentf22d15aec205d83537734ffe",
                algo_id="",
                algo_client_order_id="",
                take_profit_trigger_price=Decimal("62084.2"),
                take_profit_order_price=Decimal("-1"),
                stop_loss_trigger_price=Decimal("65628.9"),
                stop_loss_order_price=Decimal("-1"),
                inst_type="SWAP",
            )
            drawer._symbol = "BTC-USDT-SWAP"
            drawer._snapshot = AccountDrawerSnapshot(order_history=(history_order,))
            drawer._refresh_tables()

            headers = [
                drawer._history_orders_table.horizontalHeaderItem(index).text()
                for index in range(drawer._history_orders_table.columnCount())
            ]
            state_col = headers.index("状态")
            tp_sl_col = headers.index("TP/SL")

            self.assertEqual(drawer._history_orders_table.rowCount(), 1)
            self.assertEqual(drawer._history_orders_table.item(0, state_col).text(), "canceled")
            self.assertIn("TP 62084.2", drawer._history_orders_table.item(0, tp_sl_col).text())
            self.assertIn("SL 65628.9", drawer._history_orders_table.item(0, tp_sl_col).text())
        finally:
            self.dispose_widget(drawer)

    def test_drawer_applies_shared_order_snapshot_for_matching_context(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            current_order = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                source_kind="normal",
                source_label="普通委托",
                side="sell",
                pos_side="long",
                ord_type="limit",
                trigger_price=None,
                order_price=None,
                price=Decimal("64616.1"),
                size=Decimal("9.87"),
                filled_size=Decimal("0"),
                state="live",
                update_time=1783764489406,
                created_time=1783764489406,
                order_id="3733336213178388480",
                client_order_id="rrentf22d15aec205d83537734ffe",
                algo_id="",
                algo_client_order_id="",
                take_profit_trigger_price=Decimal("62084.2"),
                take_profit_order_price=Decimal("-1"),
                stop_loss_trigger_price=Decimal("65628.9"),
                stop_loss_order_price=Decimal("-1"),
                inst_type="SWAP",
            )
            history_order = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                source_kind="algo",
                source_label="算法委托",
                side="buy",
                pos_side="short",
                ord_type="oco",
                trigger_price=None,
                order_price=None,
                price=None,
                size=Decimal("2.6"),
                filled_size=Decimal("0"),
                state="canceled",
                update_time=1783764489407,
                created_time=1783764489407,
                order_id="",
                client_order_id="algo-client-1",
                algo_id="algo-1",
                algo_client_order_id="algo-client-1",
                take_profit_trigger_price=Decimal("65000"),
                take_profit_order_price=Decimal("64950"),
                stop_loss_trigger_price=Decimal("59000"),
                stop_loss_order_price=Decimal("58950"),
                inst_type="SWAP",
            )
            drawer._profile_name = "moni"
            drawer._environment = "demo"
            drawer._symbol = "BTC-USDT-SWAP"

            drawer._apply_shared_order_snapshot(
                "moni",
                "demo",
                SharedOrderSnapshot(
                    current_order_items=(current_order,),
                    history_orders=(history_order,),
                ),
            )

            self.assertEqual(drawer._orders_table.rowCount(), 1)
            self.assertEqual(drawer._history_orders_table.rowCount(), 1)
            self.assertEqual(drawer._orders_table.item(0, 0).text(), "BTC-USDT-SWAP")
            self.assertEqual(drawer._history_orders_table.item(0, 0).text(), "BTC-USDT-SWAP")
        finally:
            self.dispose_widget(drawer)


class KlineAccountDrawerThreadTests(TestCase):
    def test_load_thread_reads_positions_only(self) -> None:
        runtime = SimpleNamespace(credentials=object(), environment="demo")
        client = Mock()
        client.get_positions.return_value = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
        thread = AccountDrawerLoadThread(request_generation=3, runtime=runtime, client=client)
        completed: list[tuple[int, AccountDrawerSnapshot]] = []

        thread.completed.connect(lambda generation, snapshot: completed.append((generation, snapshot)))
        thread.run()

        self.assertEqual(completed[0][0], 3)
        client.get_positions.assert_called_once_with(
            runtime.credentials,
            environment="demo",
        )
        client.get_pending_orders.assert_not_called()
        client.get_order_history.assert_not_called()

    def test_cancel_thread_routes_normal_and_algo_orders(self) -> None:
        runtime = SimpleNamespace(credentials=object(), environment="demo")

        normal_client = Mock()
        normal = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            source_kind="normal",
            order_id="ord-1",
            client_order_id="",
            algo_id="",
            algo_client_order_id="",
        )
        AccountDrawerCancelThread(runtime=runtime, order=normal, client=normal_client).run()
        normal_client.cancel_order_by_id.assert_called_once_with(
            runtime.credentials,
            environment="demo",
            inst_id="BTC-USDT-SWAP",
            ord_id="ord-1",
            cl_ord_id=None,
        )

        algo_client = Mock()
        algo = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            source_kind="algo",
            order_id="",
            client_order_id="",
            algo_id="algo-1",
            algo_client_order_id="",
        )
        AccountDrawerCancelThread(runtime=runtime, order=algo, client=algo_client).run()
        algo_client.cancel_algo_order.assert_called_once_with(
            runtime.credentials,
            environment="demo",
            inst_id="BTC-USDT-SWAP",
            algo_id="algo-1",
            algo_cl_ord_id=None,
        )


class KlineAccountDrawerCancelTests(QtWidgetTestCase):
    def test_cancel_requires_selection_confirmation_and_single_inflight_action(self) -> None:
        drawer = KlineAccountDrawer()
        try:
            drawer._runtime = SimpleNamespace(credentials=object(), environment="demo")
            order = SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                source_kind="normal",
                side="buy",
                ord_type="limit",
                order_id="ord-1",
                client_order_id="",
                algo_id="",
                algo_client_order_id="",
            )
            drawer._snapshot = AccountDrawerSnapshot(orders=(order,))
            drawer._refresh_tables()
            drawer._orders_table.selectRow(0)

            with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
                drawer._cancel_selected_order()
            self.assertIsNone(drawer._cancel_thread)

            drawer._cancel_in_flight = True
            with patch.object(QMessageBox, "question") as question:
                drawer._cancel_selected_order()
            question.assert_not_called()
            self.assertIsNone(drawer._cancel_thread)
        finally:
            self.dispose_widget(drawer)


class KlineAnalysisWindowAccountDrawerTests(QtWidgetTestCase):
    def test_kline_window_has_collapsed_account_drawer_and_two_entry_buttons(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}),
        ):
            window = KlineAnalysisWindow()
        try:
            self.assertEqual(window._chart_account_splitter.orientation(), Qt.Orientation.Vertical)
            self.assertTrue(window._account_drawer.isHidden())
            self.assertEqual(window._orders_drawer_button.text(), "委托")
            self.assertEqual(window._positions_drawer_button.text(), "持仓")
        finally:
            self.dispose_widget(window)

    def test_visible_drawer_receives_normalized_symbol_change(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}),
        ):
            window = KlineAnalysisWindow()
        try:
            window._account_drawer.show()
            with patch.object(window._account_drawer, "set_context") as set_context, patch.object(window, "_load_data"):
                window._symbol_input.setText("eth-usdt-swap")
                window._on_symbol_confirmed()
            self.assertEqual(set_context.call_args.kwargs["symbol"], "ETH-USDT-SWAP")
        finally:
            self.dispose_widget(window)
