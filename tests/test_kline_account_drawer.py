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


class KlineAccountDrawerThreadTests(TestCase):
    def test_load_thread_reads_positions_and_all_pending_order_kinds(self) -> None:
        runtime = SimpleNamespace(credentials=object(), environment="demo")
        client = Mock()
        client.get_positions.return_value = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
        client.get_pending_orders.return_value = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
        thread = AccountDrawerLoadThread(request_generation=3, runtime=runtime, client=client)
        completed: list[tuple[int, AccountDrawerSnapshot]] = []

        thread.completed.connect(lambda generation, snapshot: completed.append((generation, snapshot)))
        thread.run()

        self.assertEqual(completed[0][0], 3)
        client.get_pending_orders.assert_called_once_with(
            runtime.credentials,
            environment="demo",
            limit=100,
            include_algo=True,
        )

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
