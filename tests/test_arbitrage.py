from __future__ import annotations

import unittest
from decimal import Decimal

from okx_quant.arbitrage.arbitrage_auto_open import ArbitrageAutoOpenService
from okx_quant.arbitrage.arbitrage_executor import ArbitrageOpenRequest
from okx_quant.arbitrage.basis_calculator import (
    annualize_funding_rate,
    compute_basis,
    mid_price,
    net_carry_annual_pct_cash_and_carry,
)
from okx_quant.arbitrage.fee_calculator import round_trip_fee_pct
from okx_quant.arbitrage.fill_reconciler import (
    derivative_contracts_from_spot_base,
    estimate_cash_and_carry_pnl,
    reconcile_fill,
)
from okx_quant.arbitrage.models import ArbitrageFeeProfile, ArbitrageTradeRuntime
from okx_quant.arbitrage.order_book_analyzer import estimated_slippage_pct, vwap_for_base_size
from okx_quant.arbitrage.size_converter import preview_arbitrage_size
from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import OkxOrderBook, OkxTicker


class ArbitrageCalculatorTest(unittest.TestCase):
    def test_mid_and_basis(self) -> None:
        spot_mid = mid_price(Decimal("100"), Decimal("102"))
        self.assertEqual(spot_mid, Decimal("101"))
        _, basis_pct = compute_basis(Decimal("100"), Decimal("101"))
        self.assertEqual(basis_pct, Decimal("0.01"))

    def test_funding_annualization(self) -> None:
        annual = annualize_funding_rate(Decimal("0.0001"))
        self.assertEqual(annual, Decimal("0.0001") * Decimal("3") * Decimal("365"))

    def test_round_trip_fee_vip2(self) -> None:
        fee = round_trip_fee_pct(fee_profile=ArbitrageFeeProfile(), assume_taker=True)
        expected = Decimal("0.00036") * 2 + Decimal("0.00070") * 2
        self.assertEqual(fee, expected)

    def test_net_carry(self) -> None:
        net = net_carry_annual_pct_cash_and_carry(
            basis_pct=Decimal("0.01"),
            funding_annual=Decimal("0.05"),
            fee_round_trip_pct=Decimal("0.002"),
            slippage_pct=Decimal("0.001"),
        )
        self.assertEqual(net, (Decimal("0.01") + Decimal("0.05") - Decimal("0.003")) * Decimal("100"))

    def test_vwap_and_slippage(self) -> None:
        book = OkxOrderBook(
            inst_id="BTC-USDT",
            bids=((Decimal("100"), Decimal("1")), (Decimal("99"), Decimal("2"))),
            asks=((Decimal("101"), Decimal("1")), (Decimal("102"), Decimal("2"))),
            raw={},
        )
        vwap, filled = vwap_for_base_size(book, side="buy", base_size=Decimal("1.5"))
        self.assertEqual(filled, Decimal("1.5"))
        assert vwap is not None
        slip = estimated_slippage_pct(Decimal("100.5"), vwap, side="buy")
        self.assertGreaterEqual(slip, Decimal("0"))

    def test_size_preview(self) -> None:
        spot = Instrument(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.0001"),
            min_size=Decimal("0.0001"),
            state="live",
        )
        swap = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        preview = preview_arbitrage_size(
            size=Decimal("1000"),
            unit="usdt",
            spot_mid=Decimal("50000"),
            spot_instrument=spot,
            swap_instrument=swap,
        )
        self.assertEqual(preview.spot_base_qty, Decimal("0.0200"))
        self.assertEqual(preview.swap_contracts, Decimal("2"))


class _FakeTickerClient:
    def __init__(self, spot: OkxTicker, deriv: OkxTicker) -> None:
        self._spot = spot
        self._deriv = deriv

    def get_ticker(self, inst_id: str) -> OkxTicker:
        if inst_id.endswith("-SWAP"):
            return self._deriv
        return self._spot


class ArbitrageAutoOpenTest(unittest.TestCase):
    def test_spread_trigger(self) -> None:
        client = _FakeTickerClient(
            OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
            OkxTicker("BTC-USDT-SWAP", Decimal("100.2"), Decimal("100.1"), Decimal("100.3"), None, None, raw={}),
        )
        service = ArbitrageAutoOpenService(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("1000"),
            size_unit="usdt",
            trigger_mode="spread",
            open_spread_pct_max=Decimal("0.5"),
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=Decimal("0.0015"),
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        session = type("S", (), {"request": request, "runtime": runtime})()
        self.assertTrue(service._should_trigger(session))  # noqa: SLF001

    def test_limit_trigger(self) -> None:
        client = _FakeTickerClient(
            OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("100"), None, None, raw={}),
            OkxTicker("BTC-USDT-SWAP", Decimal("101"), Decimal("101"), Decimal("102"), None, None, raw={}),
        )
        service = ArbitrageAutoOpenService(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("1000"),
            size_unit="usdt",
            trigger_mode="limit_price",
            open_spread_pct_max=None,
            spot_limit_price=Decimal("100.5"),
            derivative_limit_price=Decimal("100.5"),
            use_limit_orders=True,
            max_slippage=Decimal("0.0015"),
        )
        session = type("S", (), {"request": request})()
        self.assertTrue(service._should_trigger(session))  # noqa: SLF001


class FillReconcilerTest(unittest.TestCase):
    def test_reconcile_and_pnl(self) -> None:
        reconciled = reconcile_fill(planned_size=Decimal("2"), filled_size=Decimal("1.9"), avg_price=Decimal("100"))
        self.assertFalse(reconciled.fully_filled)
        swap = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
        )
        contracts = derivative_contracts_from_spot_base(spot_base_qty=Decimal("0.02"), derivative_instrument=swap)
        self.assertEqual(contracts, Decimal("2"))
        pnl = estimate_cash_and_carry_pnl(
            spot_qty=Decimal("0.02"),
            open_spot_price=Decimal("100"),
            close_spot_price=Decimal("101"),
            open_deriv_price=Decimal("101"),
            close_deriv_price=Decimal("100"),
            derivative_instrument=swap,
            derivative_qty=Decimal("2"),
        )
        assert pnl is not None
        self.assertEqual(pnl, Decimal("0.04"))


if __name__ == "__main__":
    unittest.main()
