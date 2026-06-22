from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from okx_quant.arbitrage.arbitrage_auto_open import ArbitrageAutoOpenService
from okx_quant.arbitrage.arbitrage_auto_close import ArbitrageAutoCloseService, ArbitrageAutoCloseSession
from okx_quant.arbitrage import arbitrage_executor as arbitrage_executor_module
from okx_quant.arbitrage.arbitrage_scanner import ArbitrageScanner, _describe_futures_series
from okx_quant.arbitrage.arbitrage_executor import (
    ArbitrageCloseRequest,
    ArbitrageExecutor,
    ArbitrageOpenRequest,
    ArbitrageRollRequest,
)
from okx_quant.arbitrage_ui import (
    ArbitrageWindow,
    _actionable_spread_abs,
    _arbitrage_fee_profile_from_snapshot,
    _build_spread_candles,
    _estimated_dual_leg_fee_pct,
    _estimated_one_coin_taker_fee_usdt,
    _format_fee_amount_usdt_rounded,
    _pair_derivative_base_qty_from_contracts,
    _pair_derivative_qty_from_spot_qty,
    _roll_target_future_candidates,
    _build_runtime_for_profile,
    _build_spot_positions_from_account,
    _market_depth_rows,
    _credential_profile_names_from_snapshot,
    _pair_max_derivative_close_qty,
    _pair_position_label,
    _pair_spot_qty_from_derivative_qty,
    _split_pair_close_batches,
)
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
from okx_quant.arbitrage.models import ArbitrageFeeProfile, ArbitrageLedgerEntry, ArbitrageTradeRuntime
from okx_quant.arbitrage.order_book_analyzer import estimated_slippage_pct, vwap_for_base_size
from okx_quant.arbitrage.size_converter import preview_arbitrage_size
from okx_quant.models import Credentials, Instrument
from okx_quant.models import Candle
from okx_quant.okx_client import OkxAccountAssetItem, OkxAccountOverview, OkxOrderBook, OkxOrderStatus, OkxPosition, OkxTicker, infer_inst_type


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
        expected = Decimal("0.000700") * 2 + Decimal("0.000350") * 2
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
            open_spread_abs_max=None,
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


class _WsFirstOrderClient:
    def __init__(self) -> None:
        self.rest_calls = 0
        self.ws_calls = 0

    def wait_private_order_update(self, credentials, **kwargs):  # noqa: ANN001,ANN003
        self.ws_calls += 1
        return (
            1,
            OkxOrderStatus(
                ord_id="ord-1",
                state="filled",
                side="buy",
                ord_type="limit",
                price=Decimal("100"),
                avg_price=Decimal("100.1"),
                size=Decimal("2"),
                filled_size=Decimal("2"),
                raw={},
            ),
        )

    def get_order(self, credentials, config, *, inst_id: str, ord_id: str | None = None, cl_ord_id: str | None = None):  # noqa: ANN001
        self.rest_calls += 1
        return OkxOrderStatus(
            ord_id=ord_id or "ord-1",
            state="live",
            side="buy",
            ord_type="limit",
            price=Decimal("100"),
            avg_price=None,
            size=Decimal("2"),
            filled_size=Decimal("0"),
            raw={},
        )


class ArbitrageWaitOrderFillTest(unittest.TestCase):
    def test_wait_order_fill_prefers_private_ws_update(self) -> None:
        client = _WsFirstOrderClient()
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        config = arbitrage_executor_module._build_strategy_config("BTC-USDT-SWAP", runtime)  # noqa: SLF001
        with patch("okx_quant.arbitrage.arbitrage_executor.FillWaitSeconds", 0.2), patch(
            "okx_quant.arbitrage.arbitrage_executor.PollSeconds",
            0.05,
        ):
            filled, avg_price = arbitrage_executor_module._wait_order_fill(
                client,
                credentials=runtime.credentials,
                config=config,
                inst_id="BTC-USDT-SWAP",
                ord_id="ord-1",
                expected_size=Decimal("2"),
                logger=lambda _message: None,
                label="测试订单",
            )
        self.assertEqual(filled, Decimal("2"))
        self.assertEqual(avg_price, Decimal("100.1"))
        self.assertEqual(client.rest_calls, 0)
        self.assertGreaterEqual(client.ws_calls, 1)


class ArbitrageAutoOpenMoreTest(unittest.TestCase):
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
            open_spread_abs_max=None,
            spot_limit_price=Decimal("100.5"),
            derivative_limit_price=Decimal("100.5"),
            use_limit_orders=True,
            max_slippage=Decimal("0.0015"),
        )
        session = type("S", (), {"request": request})()
        self.assertTrue(service._should_trigger(session))  # noqa: SLF001

    def test_absolute_spread_trigger(self) -> None:
        client = _FakeTickerClient(
            OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
            OkxTicker("BTC-USDT-SWAP", Decimal("100.6"), Decimal("100.5"), Decimal("100.7"), None, None, raw={}),
        )
        service = ArbitrageAutoOpenService(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("1000"),
            size_unit="usdt",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=Decimal("0.50"),
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

    def test_auto_open_prefers_public_ws_cached_ticker(self) -> None:
        class _WsTickerClient:
            def ensure_public_ws_market_watch(self, inst_id: str, *, environment: str) -> None:
                return None

            def get_cached_public_ticker(self, inst_id: str, *, environment: str):
                mapping = {
                    "BTC-USDT": OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
                    "BTC-USDT-SWAP": OkxTicker("BTC-USDT-SWAP", Decimal("100.6"), Decimal("100.5"), Decimal("100.7"), None, None, raw={}),
                }
                return 1, mapping[inst_id]

            def get_ticker(self, inst_id: str) -> OkxTicker:
                raise AssertionError("should use public ws cache first")

        client = _WsTickerClient()
        service = ArbitrageAutoOpenService(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("1000"),
            size_unit="usdt",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=Decimal("0.50"),
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


class _FakeScannerClient:
    def get_spot_instruments(self) -> list[Instrument]:
        return [
            Instrument(
                inst_id="BTC-USDT",
                inst_type="SPOT",
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.0001"),
                min_size=Decimal("0.0001"),
                state="live",
            )
        ]

    def get_swap_instruments(self) -> list[Instrument]:
        return [
            Instrument(
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_val_ccy="BTC",
            )
        ]

    def get_instruments(self, inst_type: str) -> list[Instrument]:
        if inst_type != "FUTURES":
            return []
        return [
            Instrument(
                inst_id="BTC-USDT-260626",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="USDT",
                ct_val=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
            Instrument(
                inst_id="BTC-USD-260626",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="BTC",
                ct_val=Decimal("100"),
                ct_val_ccy="USD",
            ),
            Instrument(
                inst_id="BTC-USD-260925",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="BTC",
                ct_val=Decimal("100"),
                ct_val_ccy="USD",
            ),
            Instrument(
                inst_id="BTC-USD-261225",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="BTC",
                ct_val=Decimal("100"),
                ct_val_ccy="USD",
            ),
            Instrument(
                inst_id="BTC-USDT-260925",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="USDT",
                ct_val=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
        ]

    def get_tickers(self, inst_type: str) -> list[OkxTicker]:
        mapping = {
            "SPOT": [OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={})],
            "SWAP": [OkxTicker("BTC-USDT-SWAP", Decimal("101"), Decimal("100.5"), Decimal("101.5"), None, None, raw={})],
            "FUTURES": [
                OkxTicker("BTC-USDT-260626", Decimal("102"), Decimal("101.5"), Decimal("102.5"), None, None, raw={}),
                OkxTicker("BTC-USDT-260925", Decimal("103"), Decimal("102.5"), Decimal("103.5"), None, None, raw={}),
                OkxTicker("BTC-USD-260626", Decimal("102"), Decimal("101.5"), Decimal("102.5"), None, None, raw={}),
                OkxTicker("BTC-USD-260925", Decimal("103"), Decimal("102.5"), Decimal("103.5"), None, None, raw={}),
                OkxTicker("BTC-USD-261225", Decimal("104"), Decimal("103.5"), Decimal("104.5"), None, None, raw={}),
            ],
        }
        return mapping[inst_type]


class ArbitrageAutoCloseTest(unittest.TestCase):
    def test_refresh_spread_supports_absolute_mode(self) -> None:
        client = _FakeTickerClient(
            OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
            OkxTicker("BTC-USDT-SWAP", Decimal("100.8"), Decimal("100.7"), Decimal("100.9"), None, None, raw={}),
        )
        service = ArbitrageAutoCloseService(client)
        session = ArbitrageAutoCloseSession(
            request=ArbitrageCloseRequest(
                entry_id="entry-1",
                max_slippage=Decimal("0.0015"),
                use_limit_orders=False,
            ),
            runtime=ArbitrageTradeRuntime(
                credentials=Credentials("k", "s", "p"),
                environment="demo",
                trade_mode="cross",
                position_mode="net",
            ),
            close_trigger_mode="spread_abs",
            close_spread_pct_min=None,
            close_spread_abs_min=Decimal("0.5"),
            entry_id="entry-1",
        )

        with patch.object(
            service,
            "_resolve_target_entry",
            return_value=SimpleNamespace(spot_inst_id="BTC-USDT", derivative_inst_id="BTC-USDT-SWAP"),
        ):
            spread_pct, spread_abs = service._refresh_spread(session)  # noqa: SLF001

        self.assertIsNotNone(spread_pct)
        self.assertEqual(spread_abs, Decimal("0.8"))

    def test_close_trigger_uses_less_than_or_equal_for_absolute_spread(self) -> None:
        client = _FakeTickerClient(
            OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
            OkxTicker("BTC-USDT-SWAP", Decimal("100.4"), Decimal("100.3"), Decimal("100.5"), None, None, raw={}),
        )
        service = ArbitrageAutoCloseService(client)
        session = ArbitrageAutoCloseSession(
            request=ArbitrageCloseRequest(
                entry_id="entry-1",
                max_slippage=Decimal("0.0015"),
                use_limit_orders=False,
            ),
            runtime=ArbitrageTradeRuntime(
                credentials=Credentials("k", "s", "p"),
                environment="demo",
                trade_mode="cross",
                position_mode="net",
            ),
            close_trigger_mode="spread_abs",
            close_spread_pct_min=None,
            close_spread_abs_min=Decimal("0.5"),
            entry_id="entry-1",
        )

        with patch.object(
            service,
            "_resolve_target_entry",
            return_value=SimpleNamespace(spot_inst_id="BTC-USDT", derivative_inst_id="BTC-USDT-SWAP"),
        ):
            spread_pct, spread_abs = service._refresh_spread(session)  # noqa: SLF001

        self.assertEqual(spread_abs, Decimal("0.4"))
        self.assertIsNotNone(spread_pct)
        self.assertLessEqual(spread_abs, session.close_spread_abs_min)

    def test_auto_close_prefers_public_ws_cached_ticker(self) -> None:
        class _WsTickerClient:
            def ensure_public_ws_market_watch(self, inst_id: str, *, environment: str) -> None:
                return None

            def get_cached_public_ticker(self, inst_id: str, *, environment: str):
                mapping = {
                    "BTC-USDT": OkxTicker("BTC-USDT", Decimal("100"), Decimal("99"), Decimal("101"), None, None, raw={}),
                    "BTC-USDT-SWAP": OkxTicker("BTC-USDT-SWAP", Decimal("100.4"), Decimal("100.3"), Decimal("100.5"), None, None, raw={}),
                }
                return 1, mapping[inst_id]

            def get_ticker(self, inst_id: str) -> OkxTicker:
                raise AssertionError("should use public ws cache first")

        client = _WsTickerClient()
        service = ArbitrageAutoCloseService(client)
        session = ArbitrageAutoCloseSession(
            request=ArbitrageCloseRequest(
                entry_id="entry-1",
                max_slippage=Decimal("0.0015"),
                use_limit_orders=False,
            ),
            runtime=ArbitrageTradeRuntime(
                credentials=Credentials("k", "s", "p"),
                environment="demo",
                trade_mode="cross",
                position_mode="net",
            ),
            close_trigger_mode="spread_abs",
            close_spread_pct_min=None,
            close_spread_abs_min=Decimal("0.5"),
            entry_id="entry-1",
        )
        with patch.object(
            service,
            "_resolve_target_entry",
            return_value=SimpleNamespace(spot_inst_id="BTC-USDT", derivative_inst_id="BTC-USDT-SWAP"),
        ):
            spread_pct, spread_abs = service._refresh_spread(session)  # noqa: SLF001
        self.assertIsNotNone(spread_pct)
        self.assertEqual(spread_abs, Decimal("0.4"))


class ArbitrageScannerFilterTest(unittest.TestCase):
    def test_scan_can_filter_only_swap(self) -> None:
        scanner = ArbitrageScanner(_FakeScannerClient())

        rows = scanner.scan(include_swap=True, include_futures=False)

        self.assertEqual(len(rows), 1)
        self.assertTrue(all(item.pair_kind == "spot_swap" for item in rows))

    def test_scan_can_filter_only_futures(self) -> None:
        scanner = ArbitrageScanner(_FakeScannerClient())

        rows = scanner.scan(include_swap=False, include_futures=True)

        self.assertEqual(len(rows), 5)
        self.assertTrue(all(item.pair_kind in {"spot_quarter", "spot_next_quarter", "spot_future"} for item in rows))
        self.assertIn("BTC-USDT-260626", {item.derivative_inst_id for item in rows})
        self.assertIn("BTC-USD-260626", {item.derivative_inst_id for item in rows})
        self.assertIn("BTC-USD-261225", {item.derivative_inst_id for item in rows})
        self.assertIn("现货+当季(USD)", {item.pair_kind_label for item in rows})
        self.assertIn("现货+季交割(USD)", {item.pair_kind_label for item in rows})


class ArbitrageChartHelperTest(unittest.TestCase):
    def test_infer_inst_type_recognizes_futures(self) -> None:
        self.assertEqual(infer_inst_type("BTC-USD-260626"), "FUTURES")
        self.assertEqual(infer_inst_type("BTC-USDT-260626"), "FUTURES")

    def test_credential_profile_names_are_sorted(self) -> None:
        names = _credential_profile_names_from_snapshot(
            {
                "selected_profile": "api2",
                "profiles": {
                    "api3": {},
                    "api1": {},
                    "api2": {},
                },
            }
        )

        self.assertEqual(names, ["api1", "api2", "api3"])

    def test_build_runtime_for_profile_reuses_trade_settings_from_fallback(self) -> None:
        fallback_runtime = ArbitrageTradeRuntime(
            credentials=Credentials("fallback-key", "fallback-secret", "fallback-pass", profile_name="api1"),
            environment="demo",
            trade_mode="isolated",
            position_mode="long_short",
            credential_profile_name="api1",
        )

        runtime = _build_runtime_for_profile(
            "api2",
            profile_snapshot={
                "api_key": "key-2",
                "secret_key": "secret-2",
                "passphrase": "pass-2",
                "environment": "live",
            },
            fallback_runtime=fallback_runtime,
        )

        assert runtime is not None
        self.assertEqual(runtime.credentials.profile_name, "api2")
        self.assertEqual(runtime.credentials.api_key, "key-2")
        self.assertEqual(runtime.environment, "live")
        self.assertEqual(runtime.trade_mode, "isolated")
        self.assertEqual(runtime.position_mode, "long_short")

    def test_describe_futures_series_uses_week_month_quarter_labels(self) -> None:
        rows = _describe_futures_series(
            [
                Instrument(
                    inst_id="BTC-USD-260605",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                Instrument(
                    inst_id="BTC-USD-260612",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                Instrument(
                    inst_id="BTC-USD-260626",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                Instrument(
                    inst_id="BTC-USD-260731",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                Instrument(
                    inst_id="BTC-USD-260925",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                Instrument(
                    inst_id="BTC-USD-261225",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
            ],
            settle_suffix="USD",
        )

        labels = {instrument.inst_id: label for _, label, instrument in rows}
        self.assertEqual(labels["BTC-USD-260605"], "现货+近周(USD)")
        self.assertEqual(labels["BTC-USD-260612"], "现货+次周(USD)")
        self.assertEqual(labels["BTC-USD-260626"], "现货+当季(USD)")
        self.assertEqual(labels["BTC-USD-260731"], "现货+当月(USD)")
        self.assertEqual(labels["BTC-USD-260925"], "现货+次季(USD)")
        self.assertEqual(labels["BTC-USD-261225"], "现货+季交割(USD)")

    def test_build_spread_candles_uses_aligned_timestamps(self) -> None:
        spot_candles = [
            Candle(
                ts=1000,
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("95"),
                close=Decimal("105"),
                volume=Decimal("1"),
                confirmed=True,
            ),
            Candle(
                ts=2000,
                open=Decimal("200"),
                high=Decimal("210"),
                low=Decimal("190"),
                close=Decimal("205"),
                volume=Decimal("1"),
                confirmed=True,
            ),
        ]
        derivative_candles = [
            Candle(
                ts=1000,
                open=Decimal("101"),
                high=Decimal("112"),
                low=Decimal("96"),
                close=Decimal("106"),
                volume=Decimal("1"),
                confirmed=True,
            ),
            Candle(
                ts=3000,
                open=Decimal("300"),
                high=Decimal("310"),
                low=Decimal("290"),
                close=Decimal("305"),
                volume=Decimal("1"),
                confirmed=True,
            ),
        ]

        spread = _build_spread_candles(spot_candles, derivative_candles)

        self.assertEqual(len(spread), 1)
        self.assertEqual(spread[0].ts, 1000)
        self.assertEqual(spread[0].open, Decimal("1"))
        self.assertEqual(spread[0].high, Decimal("2"))
        self.assertEqual(spread[0].low, Decimal("1"))
        self.assertEqual(spread[0].close, Decimal("1"))

    def test_pair_close_helpers_limit_by_spot_and_convert_back(self) -> None:
        spot_position = OkxPosition(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            mgn_mode="cash",
            position=Decimal("0.019"),
            avail_position=Decimal("0.019"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy=None,
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        derivative_position = OkxPosition(
            inst_id="BTC-USDT-260626",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("-3"),
            avail_position=Decimal("3"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy=None,
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        spot_instrument = Instrument(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.0001"),
            min_size=Decimal("0.0001"),
            state="live",
        )
        derivative_instrument = Instrument(
            inst_id="BTC-USDT-260626",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )

        max_derivative = _pair_max_derivative_close_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
        )
        spot_qty = _pair_spot_qty_from_derivative_qty(
            max_derivative,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
        )

        self.assertEqual(max_derivative, Decimal("1"))
        self.assertEqual(spot_qty, Decimal("0.0100"))

    def test_pair_close_helpers_support_inverse_futures(self) -> None:
        spot_position = OkxPosition(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            pos_side="net",
            mgn_mode="cash",
            position=Decimal("0.03"),
            avail_position=Decimal("0.03"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy=None,
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        derivative_position = OkxPosition(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("-20"),
            avail_position=Decimal("20"),
            avg_price=None,
            mark_price=Decimal("50000"),
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy="BTC",
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        spot_instrument = Instrument(
            inst_id="BTC-USDT",
            inst_type="SPOT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.0001"),
            min_size=Decimal("0.0001"),
            state="live",
        )
        derivative_instrument = Instrument(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )

        base_per_contract = _pair_derivative_base_qty_from_contracts(
            Decimal("1"),
            instrument=derivative_instrument,
            reference_price=Decimal("50000"),
        )
        max_derivative = _pair_max_derivative_close_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=Decimal("50000"),
        )
        spot_qty = _pair_spot_qty_from_derivative_qty(
            Decimal("10"),
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=Decimal("50000"),
        )

        self.assertEqual(base_per_contract, Decimal("0.00200000"))
        self.assertEqual(max_derivative, Decimal("15"))
        self.assertEqual(spot_qty, Decimal("0.0200"))

    def test_pair_close_helper_converts_spot_back_to_derivative_qty(self) -> None:
        derivative_instrument = Instrument(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )

        derivative_qty = _pair_derivative_qty_from_spot_qty(
            Decimal("0.0200"),
            derivative_instrument=derivative_instrument,
            reference_price=Decimal("50000"),
        )

        self.assertEqual(derivative_qty, Decimal("10"))

    def test_split_pair_close_batches_by_count(self) -> None:
        derivative_instrument = Instrument(
            inst_id="BTC-USDT-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )

        batches = _split_pair_close_batches(
            Decimal("1000"),
            derivative_instrument=derivative_instrument,
            batch_count=10,
        )

        self.assertEqual(len(batches), 10)
        self.assertTrue(all(item == Decimal("100") for item in batches))

    def test_split_pair_close_batches_by_batch_qty(self) -> None:
        derivative_instrument = Instrument(
            inst_id="BTC-USDT-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )

        batches = _split_pair_close_batches(
            Decimal("105"),
            derivative_instrument=derivative_instrument,
            batch_qty=Decimal("20"),
        )

        self.assertEqual(batches, [Decimal("20"), Decimal("20"), Decimal("20"), Decimal("20"), Decimal("20"), Decimal("5")])

    def test_build_spot_positions_from_account_uses_available_balance(self) -> None:
        overview = OkxAccountOverview(
            total_equity=None,
            adjusted_equity=None,
            isolated_equity=None,
            available_equity=None,
            unrealized_pnl=None,
            initial_margin=None,
            maintenance_margin=None,
            order_frozen=None,
            notional_usd=None,
            details=(
                OkxAccountAssetItem(
                    ccy="BTC",
                    equity=Decimal("0.12"),
                    equity_usd=Decimal("9600"),
                    cash_balance=Decimal("0.15"),
                    available_balance=Decimal("0.10"),
                    available_equity=None,
                    frozen_balance=None,
                    unrealized_pnl=None,
                    discount_equity=None,
                    liability=None,
                    cross_liability=None,
                    interest=None,
                    raw={},
                ),
            ),
            raw={},
        )

        class _Client:
            @staticmethod
            def get_instrument(inst_id: str) -> Instrument:
                return Instrument(
                    inst_id=inst_id,
                    inst_type="SPOT",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("0.0001"),
                    min_size=Decimal("0.0001"),
                    state="live",
                )

        positions = _build_spot_positions_from_account(overview, _Client())

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].inst_id, "BTC-USDT")
        self.assertEqual(positions[0].avail_position, Decimal("0.10"))

    def test_pair_position_label_includes_base_exposure_for_derivative(self) -> None:
        position = OkxPosition(
            inst_id="BTC-USDT-260626",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("-1112"),
            avail_position=Decimal("1112"),
            avg_price=None,
            mark_price=None,
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy=None,
            last_price=None,
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        instrument = Instrument(
            inst_id="BTC-USDT-260626",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )

        label = _pair_position_label(position, instrument)

        self.assertIn("可平 1112", label)
        self.assertIn("折合 11.12 BTC", label)


    def test_roll_target_future_candidates_only_keep_same_family_and_farther_expiry(self) -> None:
        instruments = [
            Instrument(
                inst_id="BTC-USD-260626",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
            Instrument(
                inst_id="BTC-USD-260925",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
            Instrument(
                inst_id="BTC-USD-261225",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
            Instrument(
                inst_id="BTC-USDT-260925",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
        ]

        self.assertEqual(
            _roll_target_future_candidates("BTC-USD-260626", instruments),
            ["BTC-USD-260925", "BTC-USD-261225"],
        )

    def test_estimated_dual_leg_fee_pct_supports_roll_and_maker_taker_modes(self) -> None:
        self.assertEqual(
            _estimated_dual_leg_fee_pct(panel_key="trade", execution_mode="dual_taker"),
            Decimal("0.1050"),
        )
        self.assertEqual(
            _estimated_dual_leg_fee_pct(panel_key="pair_close", execution_mode="spot_maker_derivative_taker"),
            Decimal("0.1025"),
        )
        self.assertEqual(
            _estimated_dual_leg_fee_pct(panel_key="roll", execution_mode="dual_taker"),
            Decimal("0.0700"),
        )

    def test_arbitrage_fee_profile_from_snapshot_uses_api_profile_overrides(self) -> None:
        profile = _arbitrage_fee_profile_from_snapshot(
            {
                "spot_maker_fee_rate": "0.0600",
                "spot_taker_fee_rate": "0.0700",
                "futures_maker_fee_rate": "0.0150",
                "futures_taker_fee_rate": "0.0360",
            }
        )
        self.assertEqual(profile.spot_maker, Decimal("0.000600"))
        self.assertEqual(profile.spot_taker, Decimal("0.000700"))
        self.assertEqual(profile.swap_maker, Decimal("0.000150"))
        self.assertEqual(profile.swap_taker, Decimal("0.000360"))

    def test_estimated_one_coin_taker_fee_usdt_uses_spot_and_derivative_taker_rates(self) -> None:
        self.assertEqual(
            _estimated_one_coin_taker_fee_usdt(
                instrument=Instrument(
                    inst_id="BTC-USDT",
                    inst_type="SPOT",
                    tick_size=Decimal("0.01"),
                    lot_size=Decimal("0.0001"),
                    min_size=Decimal("0.0001"),
                    state="live",
                ),
                reference_price=Decimal("20000"),
            ),
            Decimal("14.00000"),
        )
        self.assertEqual(
            _estimated_one_coin_taker_fee_usdt(
                instrument=Instrument(
                    inst_id="BTC-USD-260626",
                    inst_type="FUTURES",
                    tick_size=Decimal("0.1"),
                    lot_size=Decimal("1"),
                    min_size=Decimal("1"),
                    state="live",
                ),
                reference_price=Decimal("20000"),
            ),
            Decimal("7.00000"),
        )

    def test_format_fee_amount_usdt_rounded_keeps_integer_display(self) -> None:
        self.assertEqual(_format_fee_amount_usdt_rounded(Decimal("24.963246")), "25")
        self.assertEqual(_format_fee_amount_usdt_rounded(Decimal("48.520745")), "49")

    def test_estimated_one_coin_taker_fee_spot_is_higher_than_futures_at_same_price(self) -> None:
        reference_price = Decimal("70000")
        spot_fee = _estimated_one_coin_taker_fee_usdt(
            instrument=Instrument(
                inst_id="BTC-USDT",
                inst_type="SPOT",
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.0001"),
                min_size=Decimal("0.0001"),
                state="live",
            ),
            reference_price=reference_price,
        )
        futures_fee = _estimated_one_coin_taker_fee_usdt(
            instrument=Instrument(
                inst_id="BTC-USD-260626",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
            ),
            reference_price=reference_price,
        )

        assert spot_fee is not None
        assert futures_fee is not None
        self.assertGreater(spot_fee, futures_fee)


class ArbitrageMarketPanelHelperTest(unittest.TestCase):
    def test_actionable_spread_abs_uses_side_specific_best_prices(self) -> None:
        spot_ticker = OkxTicker(
            inst_id="BTC-USDT",
            last=Decimal("100"),
            bid=Decimal("99.8"),
            ask=Decimal("100.2"),
            mark=None,
            index=None,
            raw={},
        )
        derivative_ticker = OkxTicker(
            inst_id="BTC-USDT-SWAP",
            last=Decimal("100.5"),
            bid=Decimal("100.8"),
            ask=Decimal("101.1"),
            mark=None,
            index=None,
            raw={},
        )
        spot_book = OkxOrderBook(
            inst_id="BTC-USDT",
            bids=((Decimal("99.8"), Decimal("1")),),
            asks=((Decimal("100.2"), Decimal("1")),),
            raw={},
        )
        derivative_book = OkxOrderBook(
            inst_id="BTC-USDT-SWAP",
            bids=((Decimal("100.8"), Decimal("2")),),
            asks=((Decimal("101.1"), Decimal("2")),),
            raw={},
        )

        open_spread = _actionable_spread_abs(
            spot_ticker=spot_ticker,
            spot_order_book=spot_book,
            derivative_ticker=derivative_ticker,
            derivative_order_book=derivative_book,
            spot_side="buy",
            derivative_side="sell",
        )
        close_spread = _actionable_spread_abs(
            spot_ticker=spot_ticker,
            spot_order_book=spot_book,
            derivative_ticker=derivative_ticker,
            derivative_order_book=derivative_book,
            spot_side="sell",
            derivative_side="buy",
        )

        self.assertEqual(open_spread, Decimal("0.6"))
        self.assertEqual(close_spread, Decimal("1.3"))

    def test_market_depth_rows_convert_inverse_contract_size_to_base_coin(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )
        book = OkxOrderBook(
            inst_id="BTC-USD-260626",
            bids=((Decimal("49990"), Decimal("1")), (Decimal("49980"), Decimal("2"))),
            asks=((Decimal("50000"), Decimal("1")), (Decimal("50010"), Decimal("2"))),
            raw={},
        )

        rows = _market_depth_rows(book, instrument=instrument, depth=2)

        self.assertEqual(rows[0], ("ask", "50010", "0.0039992"))
        self.assertEqual(rows[1], ("ask", "50000", "0.002"))
        self.assertEqual(rows[2], ("bid", "49990", "0.0020004"))
        self.assertEqual(rows[3], ("bid", "49980", "0.0040016"))


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


class _FakeArbitrageTradeClient:
    def __init__(self) -> None:
        self.orders: list[dict[str, object]] = []
        self.cancels: list[tuple[str, str]] = []
        self._instruments = {
            "BTC-USDT": Instrument(
                inst_id="BTC-USDT",
                inst_type="SPOT",
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.0001"),
                min_size=Decimal("0.0001"),
                state="live",
            ),
            "BTC-USDT-SWAP": Instrument(
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
            "BTC-USDT-260926": Instrument(
                inst_id="BTC-USDT-260926",
                inst_type="FUTURES",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
        }
        self._tickers = {
            "BTC-USDT": OkxTicker("BTC-USDT", Decimal("101"), Decimal("100.9"), Decimal("101"), None, None, raw={}),
            "BTC-USDT-SWAP": OkxTicker(
                "BTC-USDT-SWAP",
                Decimal("100"),
                Decimal("99.9"),
                Decimal("100"),
                None,
                None,
                raw={},
            ),
            "BTC-USDT-260926": OkxTicker(
                "BTC-USDT-260926",
                Decimal("102"),
                Decimal("101.8"),
                Decimal("102"),
                None,
                None,
                raw={},
            ),
        }

    def get_instrument(self, inst_id: str) -> Instrument:
        return self._instruments[inst_id]

    def get_ticker(self, inst_id: str) -> OkxTicker:
        return self._tickers[inst_id]

    def place_simple_order(self, credentials, config, **kwargs):  # noqa: ANN001
        self.orders.append(kwargs)
        return SimpleNamespace(ord_id=f"ord-{len(self.orders)}")

    def cancel_order(self, credentials, config, *, inst_id: str, ord_id: str):  # noqa: ANN001
        self.cancels.append((inst_id, ord_id))


class ArbitrageExecutorCloseTest(unittest.TestCase):
    def test_open_supports_spot_maker_derivative_taker_mode(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("2"),
            size_unit="contracts",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=Decimal("0.05"),
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=Decimal("0.0015"),
            execution_mode="spot_maker_derivative_taker",
            maker_wait_seconds=0.1,
            chase_limit=1,
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry"),
            patch.object(executor, "_wait_order_fill_until", return_value=(Decimal("0.0200"), Decimal("100"), True)),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                return_value=(Decimal("2"), Decimal("101")),
            ),
        ):
            result = executor.open_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["ord_type"], "post_only")
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[1]["ord_type"], "market")
        self.assertEqual(client.orders[1]["side"], "sell")

    def test_open_supports_batch_execution_by_count(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageOpenRequest(
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            size=Decimal("4"),
            size_unit="contracts",
            trigger_mode="spread_abs",
            open_spread_pct_max=None,
            open_spread_abs_max=Decimal("0.05"),
            spot_limit_price=None,
            derivative_limit_price=None,
            use_limit_orders=False,
            max_slippage=Decimal("0.0015"),
            batch_count=2,
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        upserts: list[ArbitrageLedgerEntry] = []
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry", side_effect=upserts.append),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("0.0200"), Decimal("101")),
                    (Decimal("2"), Decimal("100")),
                    (Decimal("0.0200"), Decimal("101")),
                    (Decimal("2"), Decimal("100")),
                ],
            ),
        ):
            result = executor.open_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertIn("分批开仓完成", result.message)
        self.assertEqual(len(client.orders), 4)
        self.assertEqual(client.orders[0]["size"], Decimal("0.0200"))
        self.assertEqual(client.orders[1]["size"], Decimal("2"))
        self.assertEqual(client.orders[2]["size"], Decimal("0.0200"))
        self.assertEqual(client.orders[3]["size"], Decimal("2"))
        self.assertEqual(len(upserts), 2)

    def test_partial_close_updates_open_entry_and_creates_closed_record(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_swap",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="测试持仓",
        )
        request = ArbitrageCloseRequest(
            entry_id="entry-open",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            close_derivative_qty=Decimal("1"),
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        upserts: list[ArbitrageLedgerEntry] = []
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.load_open_ledger_entries", return_value=[entry]),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry", side_effect=upserts.append),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("1"), Decimal("100")),
                    (Decimal("0.0100"), Decimal("101")),
                ],
            ),
        ):
            result = executor.close_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(result.closed_count, 1)
        self.assertEqual(result.total_pnl, Decimal("0.0200"))
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["size"], Decimal("1"))
        self.assertEqual(client.orders[1]["size"], Decimal("0.0100"))
        self.assertEqual(len(upserts), 2)

        remaining_entry = next(item for item in upserts if item.close_mode == "open")
        partial_entry = next(item for item in upserts if item.close_mode == "partial")
        self.assertEqual(remaining_entry.entry_id, "entry-open")
        self.assertEqual(remaining_entry.derivative_qty, Decimal("1"))
        self.assertEqual(remaining_entry.spot_qty, Decimal("0.0100"))
        self.assertEqual(partial_entry.derivative_qty, Decimal("1"))
        self.assertEqual(partial_entry.spot_qty, Decimal("0.0100"))
        self.assertEqual(partial_entry.realized_pnl, Decimal("0.0200"))

    def test_partial_close_requires_specific_entry(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        request = ArbitrageCloseRequest(
            entry_id=None,
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            close_derivative_qty=Decimal("1"),
        )

        result = executor.close_cash_and_carry(request, runtime=runtime)

        self.assertFalse(result.success)
        self.assertIn("选择一条具体的套利持仓", result.message)

    def test_close_supports_spot_maker_derivative_taker_mode(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_swap",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="测试持仓",
        )
        request = ArbitrageCloseRequest(
            entry_id="entry-open",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            close_derivative_qty=Decimal("1"),
            execution_mode="spot_maker_derivative_taker",
            maker_wait_seconds=0.1,
            chase_limit=1,
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        upserts: list[ArbitrageLedgerEntry] = []
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.load_open_ledger_entries", return_value=[entry]),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry", side_effect=upserts.append),
            patch.object(executor, "_wait_order_fill_until", return_value=(Decimal("0.0100"), Decimal("100"), True)),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                return_value=(Decimal("1"), Decimal("101")),
            ),
        ):
            result = executor.close_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["ord_type"], "post_only")
        self.assertEqual(client.orders[0]["side"], "sell")
        self.assertEqual(client.orders[1]["ord_type"], "market")
        self.assertEqual(client.orders[1]["side"], "buy")

    def test_close_supports_batch_execution_by_count(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_swap",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="测试持仓",
        )
        request = ArbitrageCloseRequest(
            entry_id="entry-open",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            close_derivative_qty=Decimal("2"),
            batch_count=2,
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        entry_map = {entry.entry_id: entry}
        upserts: list[ArbitrageLedgerEntry] = []

        def _store_entry(item: ArbitrageLedgerEntry) -> None:
            upserts.append(item)
            entry_map[item.entry_id] = item

        with (
            patch("okx_quant.arbitrage.arbitrage_executor.load_open_ledger_entries", return_value=[entry]),
            patch("okx_quant.arbitrage.arbitrage_executor.find_ledger_entry", side_effect=lambda entry_id: entry_map.get(entry_id)),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry", side_effect=_store_entry),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("1"), Decimal("100")),
                    (Decimal("0.0100"), Decimal("101")),
                    (Decimal("1"), Decimal("100")),
                    (Decimal("0.0100"), Decimal("101")),
                ],
            ),
        ):
            result = executor.close_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 4)
        self.assertEqual(client.orders[0]["size"], Decimal("1"))
        self.assertEqual(client.orders[1]["size"], Decimal("0.0100"))
        self.assertEqual(client.orders[2]["size"], Decimal("1"))
        self.assertEqual(client.orders[3]["size"], Decimal("0.0100"))
        self.assertEqual(result.total_pnl, Decimal("0.0400"))

    def test_roll_supports_partial_delivery_roll(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_quarter",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="测试持仓",
        )
        entry_map = {entry.entry_id: entry}
        upserts: list[ArbitrageLedgerEntry] = []

        def _store_entry(item: ArbitrageLedgerEntry) -> None:
            upserts.append(item)
            entry_map[item.entry_id] = item

        request = ArbitrageRollRequest(
            entry_id="entry-open",
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.find_ledger_entry", side_effect=lambda entry_id: entry_map.get(entry_id)),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry", side_effect=_store_entry),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("1"), Decimal("100")),
                    (Decimal("1"), Decimal("102")),
                ],
            ),
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[1]["inst_id"], "BTC-USDT-260926")
        self.assertEqual(client.orders[1]["side"], "sell")
        self.assertEqual(len(upserts), 2)
        remaining_entry = next(item for item in upserts if item.entry_id == "entry-open")
        rolled_entry = next(item for item in upserts if item.entry_id != "entry-open")
        self.assertEqual(remaining_entry.derivative_qty, Decimal("1"))
        self.assertEqual(rolled_entry.derivative_inst_id, "BTC-USDT-260926")
        self.assertEqual(rolled_entry.derivative_qty, Decimal("1"))

    def test_roll_supports_live_positions_without_ledger_entry(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            current_derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0100"),
            current_derivative_qty=Decimal("1"),
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("1"), Decimal("100")),
                    (Decimal("1"), Decimal("102")),
                ],
            ),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry") as upsert_mock,
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertIsNone(result.entry_id)
        self.assertEqual(len(client.orders), 2)
        self.assertEqual(client.orders[0]["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(client.orders[1]["inst_id"], "BTC-USDT-260926")
        upsert_mock.assert_not_called()

    def test_roll_supports_both_maker_first_fill_then_market_hedge(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_quarter",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="test",
        )
        request = ArbitrageRollRequest(
            entry_id="entry-open",
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            execution_mode="both_maker_first_taker",
            maker_wait_seconds=0.1,
            chase_limit=0,
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.find_ledger_entry", return_value=entry),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry"),
            patch.object(
                executor,
                "_wait_two_maker_orders_until",
                return_value=(Decimal("1"), Decimal("100"), Decimal("0"), None, True),
            ),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                return_value=(Decimal("1"), Decimal("102")),
            ),
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 3)
        self.assertEqual(client.orders[0]["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(client.orders[0]["ord_type"], "post_only")
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[1]["inst_id"], "BTC-USDT-260926")
        self.assertEqual(client.orders[1]["ord_type"], "post_only")
        self.assertEqual(client.orders[1]["side"], "sell")
        self.assertEqual(client.orders[2]["inst_id"], "BTC-USDT-260926")
        self.assertEqual(client.orders[2]["ord_type"], "market")
        self.assertEqual(client.orders[2]["side"], "sell")
        self.assertEqual(client.cancels, [("BTC-USDT-SWAP", "ord-1"), ("BTC-USDT-260926", "ord-2")])

    def test_roll_auto_force_completion_falls_back_to_dual_taker_when_both_makers_miss(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        entry = ArbitrageLedgerEntry(
            entry_id="entry-open",
            base_ccy="BTC",
            pair_kind="spot_quarter",
            spot_inst_id="BTC-USDT",
            derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0200"),
            derivative_qty=Decimal("2"),
            open_spot_price=Decimal("100"),
            open_derivative_price=Decimal("101"),
            close_spot_price=None,
            close_derivative_price=None,
            basis_at_open_pct=Decimal("1"),
            fee_total=Decimal("0"),
            funding_total=Decimal("0"),
            realized_pnl=None,
            close_mode="open",
            opened_at="2026-06-01T00:00:00Z",
            closed_at=None,
            notes="test",
        )
        request = ArbitrageRollRequest(
            entry_id="entry-open",
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            execution_mode="both_maker_first_taker",
            maker_wait_seconds=0.1,
            chase_limit=0,
            force_execution_completion=True,
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch("okx_quant.arbitrage.arbitrage_executor.find_ledger_entry", return_value=entry),
            patch("okx_quant.arbitrage.arbitrage_executor.upsert_ledger_entry"),
            patch.object(
                executor,
                "_wait_two_maker_orders_until",
                return_value=(Decimal("0"), None, Decimal("0"), None, False),
            ),
            patch(
                "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
                side_effect=[
                    (Decimal("1"), Decimal("100")),
                    (Decimal("1"), Decimal("102")),
                ],
            ),
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(len(client.orders), 4)
        self.assertEqual(client.orders[0]["ord_type"], "post_only")
        self.assertEqual(client.orders[1]["ord_type"], "post_only")
        self.assertEqual(client.orders[2]["inst_id"], "BTC-USDT-SWAP")
        self.assertEqual(client.orders[2]["ord_type"], "market")
        self.assertEqual(client.orders[2]["side"], "buy")
        self.assertEqual(client.orders[3]["inst_id"], "BTC-USDT-260926")
        self.assertEqual(client.orders[3]["ord_type"], "market")
        self.assertEqual(client.orders[3]["side"], "sell")
        self.assertEqual(client.cancels, [("BTC-USDT-SWAP", "ord-1"), ("BTC-USDT-260926", "ord-2")])

    def test_roll_short_position_in_long_short_mode_uses_buy_close_and_sell_open(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            current_derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0100"),
            current_derivative_qty=Decimal("1"),
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="long_short",
        )
        with patch(
            "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
            side_effect=[
                (Decimal("1"), Decimal("100")),
                (Decimal("1"), Decimal("102")),
            ],
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[0]["pos_side"], "short")
        self.assertTrue(client.orders[0]["reduce_only"])
        self.assertEqual(client.orders[1]["side"], "sell")
        self.assertEqual(client.orders[1]["pos_side"], "short")
        self.assertFalse(client.orders[1]["reduce_only"])

    def test_roll_long_position_in_long_short_mode_uses_sell_close_and_buy_open(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            current_derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0100"),
            current_derivative_qty=Decimal("1"),
            current_position_side="long",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="long_short",
        )
        with patch(
            "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
            side_effect=[
                (Decimal("1"), Decimal("100")),
                (Decimal("1"), Decimal("102")),
            ],
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(client.orders[0]["side"], "sell")
        self.assertEqual(client.orders[0]["pos_side"], "long")
        self.assertTrue(client.orders[0]["reduce_only"])
        self.assertEqual(client.orders[1]["side"], "buy")
        self.assertEqual(client.orders[1]["pos_side"], "long")
        self.assertFalse(client.orders[1]["reduce_only"])

    def test_roll_blocks_submit_when_current_position_side_missing(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            current_derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0100"),
            current_derivative_qty=Decimal("1"),
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="long_short",
        )

        result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertFalse(result.success)
        self.assertIn("缺少当前持仓方向", result.message)
        self.assertEqual(client.orders, [])

    def test_roll_short_position_keeps_short_pos_side_even_when_runtime_declares_net(self) -> None:
        client = _FakeArbitrageTradeClient()
        executor = ArbitrageExecutor(client)
        request = ArbitrageRollRequest(
            entry_id=None,
            target_derivative_inst_id="BTC-USDT-260926",
            max_slippage=Decimal("0.0015"),
            use_limit_orders=False,
            roll_derivative_qty=Decimal("1"),
            base_ccy="BTC",
            spot_inst_id="BTC-USDT",
            current_derivative_inst_id="BTC-USDT-SWAP",
            spot_qty=Decimal("0.0100"),
            current_derivative_qty=Decimal("1"),
            current_position_side="short",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=Credentials("k", "s", "p"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with patch(
            "okx_quant.arbitrage.arbitrage_executor._wait_order_fill",
            side_effect=[
                (Decimal("1"), Decimal("100")),
                (Decimal("1"), Decimal("102")),
            ],
        ):
            result = executor.roll_cash_and_carry(request, runtime=runtime)

        self.assertTrue(result.success)
        self.assertEqual(client.orders[0]["side"], "buy")
        self.assertEqual(client.orders[0]["pos_side"], "short")
        self.assertTrue(client.orders[0]["reduce_only"])
        self.assertEqual(client.orders[1]["side"], "sell")
        self.assertEqual(client.orders[1]["pos_side"], "short")
        self.assertFalse(client.orders[1]["reduce_only"])

    def test_roll_ui_builds_request_from_delivery_position_without_spot_match(self) -> None:
        class _Value:
            def __init__(self, value: str | bool) -> None:
                self._value = value

            def get(self):
                return self._value

        class _Manager:
            def get_instrument(self, inst_id: str) -> Instrument:
                if inst_id == "BTC-USDT":
                    return Instrument(
                        inst_id="BTC-USDT",
                        inst_type="SPOT",
                        tick_size=Decimal("0.01"),
                        lot_size=Decimal("0.0001"),
                        min_size=Decimal("0.0001"),
                        state="live",
                    )
                raise AssertionError(f"unexpected instrument lookup: {inst_id}")

        current_future = OkxPosition(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("-890"),
            avail_position=Decimal("890"),
            avg_price=Decimal("64000"),
            mark_price=Decimal("64000"),
            unrealized_pnl=None,
            unrealized_pnl_ratio=None,
            liquidation_price=None,
            leverage=None,
            margin_ccy=None,
            last_price=Decimal("64000"),
            realized_pnl=None,
            margin_ratio=None,
            initial_margin=None,
            maintenance_margin=None,
            delta=None,
            gamma=None,
            vega=None,
            theta=None,
            raw={},
        )
        current_instrument = Instrument(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        window = object.__new__(ArbitrageWindow)
        window._roll_entry_label = _Value("current")
        window._roll_position_by_key = {"current": current_future}
        window._roll_instruments = {"BTC-USD-260626": current_instrument}
        window._roll_reference_prices = {}
        window._roll_spot_by_base = {}
        window._roll_source_entry_id = None
        window._ledger_entry_by_id = {}
        window.manager = _Manager()
        window.roll_target_derivative_inst_id = _Value("BTC-USD-260925")
        window.max_slippage_percent = _Value("0.15")
        window.use_limit_orders = _Value(False)
        window.roll_current_limit_price = _Value("")
        window.roll_target_limit_price = _Value("")
        window.roll_batch_count = _Value("10")
        window.roll_batch_qty = _Value("1")
        window.roll_execution_mode_label = _Value("dual_taker")
        window.roll_maker_wait_seconds = _Value("6")
        window.roll_chase_limit = _Value("3")

        entry = window._selected_roll_entry()
        assert entry is not None
        request = window._build_roll_request(entry=entry, roll_derivative_qty=Decimal("100"))

        self.assertIsNone(request.entry_id)
        self.assertEqual(request.spot_inst_id, "BTC-USDT")
        self.assertEqual(request.current_derivative_inst_id, "BTC-USD-260626")
        self.assertEqual(request.target_derivative_inst_id, "BTC-USD-260925")
        self.assertEqual(request.roll_derivative_qty, Decimal("100"))
        self.assertEqual(request.current_derivative_qty, Decimal("890"))
        self.assertEqual(request.current_position_side, "short")
        self.assertGreater(request.spot_qty or Decimal("0"), Decimal("0"))


if __name__ == "__main__":
    unittest.main()
