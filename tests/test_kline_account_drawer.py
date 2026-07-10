from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock
from unittest import TestCase

from PySide6.QtWidgets import QAbstractItemView

from roll_terminal_qt.kline_account_drawer import (
    AccountDrawerLoadThread,
    AccountDrawerSnapshot,
    KlineAccountDrawer,
    filter_account_items,
    order_cancel_reference,
    order_source_kind,
)
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
