from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase

from roll_terminal_qt.kline_account_drawer import (
    filter_account_items,
    order_cancel_reference,
    order_source_kind,
)


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
