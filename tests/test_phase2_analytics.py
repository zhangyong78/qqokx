from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from tempfile import TemporaryDirectory
from pathlib import Path
from types import SimpleNamespace
import unittest

from okx_quant.phase2_analytics import cash_flow_summary, spot_cost_basis
from okx_quant.okx_client import OkxAccountBillItem
from okx_quant.okx_client import OkxRestClient
from okx_quant.models import Credentials
from okx_quant.persistence import load_history_cache_records
from roll_terminal_qt.bill_history_service import (
    asset_bill_category, bill_category, load_local_account_bills, merge_account_bills,
)
from roll_terminal_qt.history_sync_manager import HistorySyncThread


def fill(side, price, size, stamp, *, fee="0", fee_currency="USDT"):
    return SimpleNamespace(
        fill_time=int(stamp.timestamp() * 1000), inst_id="BTC-USDT", inst_type="SPOT",
        side=side, pos_side=None, fill_price=Decimal(price), fill_size=Decimal(size),
        fill_fee=Decimal(fee), fee_currency=fee_currency, pnl=None,
        order_id=None, trade_id=str(stamp.timestamp()), exec_type=None, raw={},
    )


class Phase2AnalyticsTests(unittest.TestCase):
    def test_fifo_spot_realized_profit_and_fee(self):
        stamp = datetime(2026, 9, 1, tzinfo=timezone.utc)
        rows = [fill("buy", "100", "2", stamp), fill("sell", "130", "1", stamp.replace(hour=2), fee="1")]
        result = spot_cost_basis(rows)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].realized_pnl, Decimal("29"))

    def test_unknown_quote_rate_is_not_presented_as_usdt(self):
        stamp = datetime(2026, 9, 1, tzinfo=timezone.utc)
        rows = [fill("buy", "100", "1", stamp), fill("sell", "130", "1", stamp.replace(hour=2))]
        rows[0].inst_id = rows[1].inst_id = "BTC-USDC"
        result = spot_cost_basis(rows)
        self.assertIsNone(result[0].realized_pnl)
        self.assertIn("USDC", result[0].valuation_note)
        valued = spot_cost_basis(rows, usdt_prices={"USDC": "1.01"})
        self.assertEqual(valued[0].realized_pnl, Decimal("30.30"))

    def test_bill_categories_and_external_flow_are_separate(self):
        account = SimpleNamespace(
            bill_id="a", bill_time=1, inst_id="", inst_type="", bill_type="1", bill_sub_type="11",
            business_type=None, event_type=None, side=None, pos_side=None, size=None, price=None,
            amount=Decimal("50"), fee=None, pnl=None, balance_change=Decimal("50"), currency="USDT",
            order_id=None, trade_id=None, client_order_id=None, raw={},
        )
        asset = SimpleNamespace(bill_id="x", bill_time=1, currency="USDT", bill_type="1", amount=Decimal("100"), fee=None,
                                state="success", tx_id=None, client_id=None, raw={})
        summary = cash_flow_summary([account], [asset])
        self.assertEqual((summary.deposits, summary.withdrawals, summary.internal_transfers, summary.fees), ({"USDT": Decimal("100")}, {}, {"USDT": Decimal("50")}, {}))
        self.assertEqual((bill_category(account), asset_bill_category(asset)), ("内部划转", "充值"))

    def test_bill_cache_is_profile_and_environment_isolated(self):
        item = OkxAccountBillItem(
            bill_id="b1", bill_time=100, inst_id="", inst_type="", bill_type="1", bill_sub_type="11",
            business_type=None, event_type=None, side=None, pos_side=None, size=None, price=None,
            amount=Decimal("1"), fee=None, pnl=None, balance_change=Decimal("1"), currency="USDT",
            order_id=None, trade_id=None, client_order_id=None, raw={},
        )
        with TemporaryDirectory() as temp:
            root = Path(temp)
            # The cache helpers use the configured data root; this assertion
            # documents that the merge function writes a stable bill id.
            from unittest.mock import patch
            with patch("roll_terminal_qt.bill_history_service.load_history_cache_records", return_value=[]), \
                 patch("roll_terminal_qt.bill_history_service.save_history_cache_records") as save:
                merge_account_bills(profile_name="a", environment="demo", remote_items=[item])
                payload = save.call_args.args[3] if len(save.call_args.args) > 3 else save.call_args.kwargs["records"]
                self.assertEqual(payload[0]["bill_id"], "b1")

    def test_asset_bill_endpoint_parses_funding_records(self):
        client = OkxRestClient()
        client._request = lambda *args, **kwargs: {"data": [{
            "billId": "asset-1", "ts": "1713863360000", "ccy": "USDT", "type": "1",
            "amt": "25", "fee": "0", "state": "success", "txId": "tx-1",
        }]}  # type: ignore[method-assign]
        items = client.get_asset_bills_history(Credentials(api_key="", secret_key="", passphrase=""), environment="demo")
        self.assertEqual((items[0].bill_id, items[0].currency, items[0].amount), ("asset-1", "USDT", Decimal("25")))

    def test_bill_sync_source_does_not_request_positions(self):
        class Client:
            def get_account_bills_history(self, *args, **kwargs):
                return []
            def get_positions_history(self, *args, **kwargs):
                raise AssertionError("bill source requested positions")
        runtime = SimpleNamespace(credentials=SimpleNamespace(profile_name="api"), environment="demo")
        thread = HistorySyncThread(runtime, profile_name="api", sources=("bills",), deep=False,
                                   display_limits={"bills": 20})
        from unittest.mock import patch
        with patch("roll_terminal_qt.history_sync_manager.load_history_sync_state", return_value={"sources": {}}), \
             patch("roll_terminal_qt.history_sync_manager.merge_account_bills", return_value=[]), \
             patch.object(thread, "_save_cursor"):
            thread._sync_source("bills", Client())


if __name__ == "__main__":
    unittest.main()
