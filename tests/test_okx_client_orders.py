import http.client
import json
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from okx_quant.models import Credentials, OrderPlan, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderStatus, OkxRestClient, _okx_trade_order_request_log_fragment


class OkxClientOrderRequestTest(TestCase):
    def test_get_instrument_prefers_local_metadata_cache_when_requested(self) -> None:
        client = OkxRestClient()
        payload = {
            "version": 1,
            "items": [
                {
                    "inst_id": "BTC-USDT-SWAP",
                    "inst_type": "SWAP",
                    "tick_size": "0.1",
                    "lot_size": "0.01",
                    "min_size": "0.01",
                    "state": "live",
                    "settle_ccy": "USDT",
                    "ct_val": None,
                    "ct_mult": None,
                    "ct_val_ccy": None,
                    "uly": "BTC-USDT",
                    "inst_family": "BTC-USDT",
                }
            ],
        }

        with TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "instrument_metadata_cache.json"
            cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            with patch("okx_quant.okx_client.instrument_metadata_cache_file_path", return_value=cache_path):
                client._request = lambda *args, **kwargs: self.fail("should not call remote instruments")  # type: ignore[method-assign]
                instrument = client.get_instrument("BTC-USDT-SWAP", prefer_cached=True)

        self.assertEqual(instrument.inst_id, "BTC-USDT-SWAP")
        self.assertEqual(instrument.tick_size, Decimal("0.1"))
        self.assertEqual(instrument.min_size, Decimal("0.01"))

    def test_get_instruments_fall_back_to_local_metadata_cache_on_network_error(self) -> None:
        client = OkxRestClient()
        payload = {
            "version": 1,
            "items": [
                {
                    "inst_id": "ETH-USDT-SWAP",
                    "inst_type": "SWAP",
                    "tick_size": "0.01",
                    "lot_size": "0.1",
                    "min_size": "0.1",
                    "state": "live",
                    "settle_ccy": "USDT",
                    "ct_val": None,
                    "ct_mult": None,
                    "ct_val_ccy": None,
                    "uly": "ETH-USDT",
                    "inst_family": "ETH-USDT",
                }
            ],
        }

        with TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "instrument_metadata_cache.json"
            cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            with patch("okx_quant.okx_client.instrument_metadata_cache_file_path", return_value=cache_path):
                client._request = lambda *args, **kwargs: (_ for _ in ()).throw(OkxApiError("网络错误：timeout"))  # type: ignore[method-assign]
                instruments = client.get_instruments("SWAP")

        self.assertEqual([item.inst_id for item in instruments], ["ETH-USDT-SWAP"])

    def test_get_positions_prefers_ws_snapshot_when_available(self) -> None:
        client = OkxRestClient()
        client.get_cached_private_positions = lambda credentials, environment: (  # type: ignore[method-assign]
            3,
            [
                type("P", (), {"inst_type": "SWAP", "inst_id": "BTC-USDT-SWAP", "pos_side": "net"})(),
                type("P", (), {"inst_type": "OPTION", "inst_id": "BTC-USD-260626-80000-C", "pos_side": "long"})(),
            ],
        )
        client._request = lambda *args, **kwargs: self.fail("should not call REST positions")  # type: ignore[method-assign]

        positions = client.get_positions(
            Credentials(api_key="k", secret_key="s", passphrase="p"),
            environment="live",
            inst_type="OPTION",
        )

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].inst_id, "BTC-USD-260626-80000-C")

    def test_get_account_overview_prefers_ws_snapshot_when_available(self) -> None:
        client = OkxRestClient()
        overview = object()
        client.get_cached_private_account_overview = lambda credentials, environment: (1, overview)  # type: ignore[method-assign]
        client._request = lambda *args, **kwargs: self.fail("should not call REST account overview")  # type: ignore[method-assign]

        result = client.get_account_overview(
            Credentials(api_key="k", secret_key="s", passphrase="p"),
            environment="demo",
        )

        self.assertIs(result, overview)

    def test_wait_private_order_update_uses_ws_snapshot_when_available(self) -> None:
        client = OkxRestClient()

        class _StubWs:
            def wait_for_order_update(self, **kwargs):  # noqa: ANN003
                self.kwargs = kwargs
                return (
                    7,
                    {
                        "ordId": "123",
                        "instId": "BTC-USDT-SWAP",
                        "state": "filled",
                        "side": "sell",
                        "ordType": "limit",
                        "px": "70000",
                        "avgPx": "70001",
                        "sz": "2",
                        "accFillSz": "2",
                    },
                )

        stub = _StubWs()
        client._private_ws_connection_for = lambda *args, **kwargs: stub  # type: ignore[method-assign]

        result = client.wait_private_order_update(
            Credentials(api_key="k", secret_key="s", passphrase="p"),
            environment="demo",
            inst_id="BTC-USDT-SWAP",
            ord_id="123",
            timeout=0.1,
        )

        assert result is not None
        version, status = result
        self.assertEqual(version, 7)
        self.assertIsInstance(status, OkxOrderStatus)
        self.assertEqual(status.ord_id, "123")
        self.assertEqual(status.state, "filled")
        self.assertEqual(status.filled_size, Decimal("2"))
        self.assertEqual(stub.kwargs["ord_id"], "123")

    def test_wait_private_order_update_returns_none_when_ws_disabled(self) -> None:
        client = OkxRestClient()
        with patch.dict("os.environ", {"QQOKX_PRIVATE_WS_ENABLED": "0"}):
            result = client.wait_private_order_update(
                Credentials(api_key="k", secret_key="s", passphrase="p"),
                environment="live",
                inst_id="BTC-USDT-SWAP",
                ord_id="123",
                timeout=0.1,
            )
        self.assertIsNone(result)

    def test_get_private_ws_debug_status_reports_disabled_mode(self) -> None:
        client = OkxRestClient()

        with patch.dict("os.environ", {"QQOKX_PRIVATE_WS_ENABLED": "0"}):
            status = client.get_private_ws_debug_status(
                Credentials(api_key="k", secret_key="s", passphrase="p"),
                environment="live",
            )

        self.assertFalse(status["enabled"])
        self.assertEqual(status["reason"], "disabled")

    def test_get_private_ws_debug_status_uses_connection_snapshot(self) -> None:
        client = OkxRestClient()

        class _StubWs:
            @staticmethod
            def debug_status():
                return {
                    "connected": True,
                    "last_error": "",
                    "version": 9,
                    "positions_version": 7,
                    "positions_received_at": 1700000000.0,
                    "account_version": 8,
                    "account_received_at": 1700000001.0,
                    "environment": "demo",
                }

        client._private_ws_connection_for = lambda *args, **kwargs: _StubWs()  # type: ignore[method-assign]

        status = client.get_private_ws_debug_status(
            Credentials(api_key="k", secret_key="s", passphrase="p"),
            environment="demo",
        )

        self.assertTrue(status["enabled"])
        self.assertTrue(status["available"])
        self.assertTrue(status["connected"])
        self.assertEqual(status["positions_version"], 7)

    def test_get_cached_public_market_snapshots_use_ws_payloads(self) -> None:
        client = OkxRestClient()

        class _StubPublicWs:
            def get_latest_ticker(self, inst_id: str):
                return 5, {"instId": inst_id, "last": "70000", "bidPx": "69999", "askPx": "70001"}

            def get_latest_order_book(self, inst_id: str):
                return 6, {"bids": [["69999", "1.2"]], "asks": [["70001", "0.8"]]}

        client._public_ws_connection_for = lambda **kwargs: _StubPublicWs()  # type: ignore[method-assign]

        ticker_payload = client.get_cached_public_ticker("BTC-USDT-SWAP", environment="demo")
        book_payload = client.get_cached_public_order_book("BTC-USDT-SWAP", environment="demo")

        assert ticker_payload is not None
        assert book_payload is not None
        ticker_version, ticker = ticker_payload
        book_version, book = book_payload
        self.assertEqual(ticker_version, 5)
        self.assertEqual(book_version, 6)
        self.assertEqual(ticker.last, Decimal("70000"))
        self.assertEqual(book.bids[0][0], Decimal("69999"))
        self.assertEqual(book.asks[0][1], Decimal("0.8"))

    def test_get_trigger_price_prefers_cached_public_ticker_when_available(self) -> None:
        client = OkxRestClient()
        watched: list[tuple[str, str]] = []

        client.ensure_public_ws_market_watch = lambda inst_id, environment: watched.append((inst_id, environment))  # type: ignore[method-assign]
        client.get_cached_public_ticker = lambda inst_id, environment: (  # type: ignore[method-assign]
            5,
            type(
                "Ticker",
                (),
                {
                    "inst_id": inst_id,
                    "last": Decimal("70000"),
                    "bid": Decimal("69999"),
                    "ask": Decimal("70001"),
                    "mark": Decimal("69998"),
                    "index": Decimal("69997"),
                    "raw": {},
                },
            )(),
        )
        client.get_ticker = lambda inst_id: self.fail(f"should not call REST ticker for {inst_id}")  # type: ignore[method-assign]

        price = client.get_trigger_price("BTC-USDT-SWAP", "last", environment="demo")

        self.assertEqual(price, Decimal("70000"))
        self.assertEqual(watched, [("BTC-USDT-SWAP", "demo")])

    def test_get_trigger_price_falls_back_to_rest_when_cached_mark_is_missing(self) -> None:
        client = OkxRestClient()
        client.ensure_public_ws_market_watch = lambda inst_id, environment: None  # type: ignore[method-assign]
        client.get_cached_public_ticker = lambda inst_id, environment: (  # type: ignore[method-assign]
            5,
            type(
                "Ticker",
                (),
                {
                    "inst_id": inst_id,
                    "last": Decimal("70000"),
                    "bid": Decimal("69999"),
                    "ask": Decimal("70001"),
                    "mark": None,
                    "index": Decimal("69997"),
                    "raw": {},
                },
            )(),
        )
        client.get_ticker = lambda inst_id: type(  # type: ignore[method-assign]
            "Ticker",
            (),
            {
                "inst_id": inst_id,
                "last": Decimal("70000"),
                "bid": Decimal("69999"),
                "ask": Decimal("70001"),
                "mark": None,
                "index": Decimal("69997"),
                "raw": {},
            },
        )()
        client.get_mark_price = lambda inst_id: Decimal("69995")  # type: ignore[method-assign]

        price = client.get_trigger_price("BTC-USDT-SWAP", "mark", environment="live")

        self.assertEqual(price, Decimal("69995"))

    def test_get_public_ws_debug_status_reports_disabled_mode(self) -> None:
        client = OkxRestClient()

        with patch.dict("os.environ", {"QQOKX_PUBLIC_WS_ENABLED": "0"}):
            status = client.get_public_ws_debug_status(environment="live")

        self.assertFalse(status["enabled"])
        self.assertEqual(status["reason"], "disabled")

    def test_parse_order_result_merges_attach_algo_errors(self) -> None:
        client = OkxRestClient()
        payload = {
            "data": [
                {
                    "ordId": "",
                    "clOrdId": "x",
                    "sCode": "1",
                    "sMsg": "操作全部失败",
                    "attachAlgoOrds": [{"sCode": "59999", "sMsg": "子单说明"}],
                }
            ]
        }
        with self.assertRaises(OkxApiError) as ctx:
            client._parse_order_result(payload, empty_message="empty")
        msg = str(ctx.exception)
        self.assertIn("附带TP/SL", msg)
        self.assertIn("59999", msg)
        self.assertIn("子单说明", msg)
        self.assertIn("常见原因", msg)

    def test_parse_order_result_bulk_fail_appends_raw_data(self) -> None:
        client = OkxRestClient()
        payload = {
            "data": [{"ordId": "", "clOrdId": "c1", "sCode": "1", "sMsg": "操作全部失败"}]
        }
        with self.assertRaises(OkxApiError) as ctx:
            client._parse_order_result(payload, empty_message="e")
        msg = str(ctx.exception)
        self.assertIn("常见原因", msg)
        self.assertIn("data[0]=", msg)
        self.assertIn("c1", msg)

    def test_parse_order_result_accepts_integer_s_code(self) -> None:
        client = OkxRestClient()
        payload = {"data": [{"sCode": 1, "sMsg": "操作全部失败", "ordId": ""}]}
        with self.assertRaises(OkxApiError) as ctx:
            client._parse_order_result(payload, empty_message="e")
        self.assertEqual(ctx.exception.code, "1")

    def test_trade_order_request_log_fragment_includes_attach_tp_sl(self) -> None:
        frag = _okx_trade_order_request_log_fragment(
            {
                "instId": "BTC-USDT-SWAP",
                "tdMode": "cross",
                "side": "buy",
                "ordType": "limit",
                "px": "76000",
                "sz": "0.01",
                "attachAlgoOrds": [{"tpTriggerPx": "77000", "slTriggerPx": "75000", "slTriggerPxType": "last"}],
            }
        )
        self.assertIn("attach_tp_sl", frag)
        self.assertIn("77000", frag)

    def test_get_order_book_empty_payload_uses_readable_error_message(self) -> None:
        client = OkxRestClient()
        client._request = lambda *args, **kwargs: {"data": []}  # type: ignore[method-assign]

        with self.assertRaises(OkxApiError) as context:
            client.get_order_book("BTC-USDT-SWAP")

        self.assertEqual(str(context.exception), "OKX 未返回盘口：BTC-USDT-SWAP")

    def test_parse_algo_order_item_uses_readable_source_label(self) -> None:
        client = OkxRestClient()

        parsed = client._parse_algo_order_item(
            {
                "instId": "BTC-USDT-SWAP",
                "instType": "SWAP",
                "side": "buy",
                "posSide": "long",
                "ordType": "conditional",
                "state": "live",
                "algoId": "123",
            },
            default_inst_type="SWAP",
        )

        self.assertEqual(parsed.source_kind, "algo")
        self.assertEqual(parsed.source_label, "算法委托")

    def test_amend_algo_order_posts_single_object_payload(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            captured["simulated"] = kwargs.get("simulated")
            return {
                "data": [
                    {
                        "algoId": "123",
                        "algoClOrdId": "algo-1",
                        "sCode": "0",
                        "sMsg": "",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        result = client.amend_algo_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="demo",
            inst_id="BTC-USD-SWAP",
            algo_id="123",
            algo_cl_ord_id="algo-1",
            req_id="req-1",
            new_stop_loss_trigger_price=Decimal("72000.5"),
            new_stop_loss_trigger_price_type="last",
        )

        self.assertEqual(result.ord_id, "123")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/amend-algos")
        self.assertTrue(captured["simulated"])
        self.assertEqual(
            captured["body"],
            {
                "instId": "BTC-USD-SWAP",
                "algoId": "123",
                "algoClOrdId": "algo-1",
                "reqId": "req-1",
                "newSlTriggerPx": "72000.5",
                "newSlOrdPx": "-1",
                "newSlTriggerPxType": "last",
            },
        )

    def test_place_stop_loss_algo_order_posts_conditional_reduce_only_order(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-SWAP",
                            "instType": "SWAP",
                            "tickSz": "0.1",
                            "lotSz": "0.1",
                            "minSz": "0.1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "long_short_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            captured["simulated"] = kwargs.get("simulated")
            return {
                "data": [
                    {
                        "algoId": "algo-1",
                        "algoClOrdId": "slg-1",
                        "sCode": "0",
                        "sMsg": "",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-SWAP",
            trade_inst_id="BTC-USD-SWAP",
            local_tp_sl_inst_id="BTC-USD-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )

        result = client.place_stop_loss_algo_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            inst_id="BTC-USD-SWAP",
            side="sell",
            size=Decimal("0.1"),
            pos_side="long",
            stop_loss_trigger_price=Decimal("71000"),
            algo_cl_ord_id="slg-1",
        )

        self.assertEqual(result.ord_id, "algo-1")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/order-algo")
        self.assertTrue(captured["simulated"])
        self.assertEqual(
            captured["body"],
            {
                "instId": "BTC-USD-SWAP",
                "tdMode": "cross",
                "side": "sell",
                "ordType": "conditional",
                "sz": "0.1",
                "slTriggerPx": "71000",
                "slOrdPx": "-1",
                "slTriggerPxType": "last",
                "reduceOnly": True,
                "cxlOnClosePos": True,
                "posSide": "long",
                "algoClOrdId": "slg-1",
            },
        )

    def test_place_market_order_can_attach_stop_without_take_profit(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-SWAP",
                            "instType": "SWAP",
                            "tickSz": "0.1",
                            "lotSz": "0.1",
                            "minSz": "0.1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "long_short_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {
                "data": [
                    {
                        "ordId": "456",
                        "clOrdId": "entry-1",
                        "sCode": "0",
                        "sMsg": "",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-SWAP",
            trade_inst_id="BTC-USD-SWAP",
            local_tp_sl_inst_id="BTC-USD-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        plan = OrderPlan(
            inst_id="BTC-USD-SWAP",
            side="buy",
            pos_side="long",
            size=Decimal("0.1"),
            take_profit=Decimal("82000"),
            stop_loss=Decimal("71000"),
            entry_reference=Decimal("75000"),
            atr_value=Decimal("1000"),
            signal="long",
            candle_ts=1710000000000,
            tp_sl_inst_id="BTC-USD-SWAP",
            tp_sl_mode="exchange",
        )

        result = client.place_market_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            plan,
            cl_ord_id="entry-1",
            include_take_profit=False,
            stop_loss_algo_cl_ord_id="stop-1",
        )

        self.assertEqual(result.ord_id, "456")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/order")
        self.assertEqual(
            captured["body"],
            {
                "instId": "BTC-USD-SWAP",
                "tdMode": "cross",
                "side": "buy",
                "ordType": "market",
                "sz": "0.1",
                "attachAlgoOrds": [
                    {
                        "slTriggerPx": "71000",
                        "slOrdPx": "-1",
                        "slTriggerPxType": "last",
                        "attachAlgoClOrdId": "stop-1",
                    }
                ],
                "posSide": "long",
                "clOrdId": "entry-1",
            },
        )

    def test_place_limit_order_can_skip_attached_protection(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-SWAP",
                            "instType": "SWAP",
                            "tickSz": "0.1",
                            "lotSz": "0.1",
                            "minSz": "0.1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "long_short_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {
                "data": [
                    {
                        "ordId": "789",
                        "clOrdId": "entry-2",
                        "sCode": "0",
                        "sMsg": "",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-SWAP",
            trade_inst_id="BTC-USD-SWAP",
            local_tp_sl_inst_id="BTC-USD-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        plan = OrderPlan(
            inst_id="BTC-USD-SWAP",
            side="buy",
            pos_side="long",
            size=Decimal("0.1"),
            take_profit=Decimal("82000"),
            stop_loss=Decimal("71000"),
            entry_reference=Decimal("75000"),
            atr_value=Decimal("1000"),
            signal="long",
            candle_ts=1710000000000,
            tp_sl_inst_id="BTC-USD-SWAP",
            tp_sl_mode="exchange",
        )

        result = client.place_limit_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            plan,
            cl_ord_id="entry-2",
            include_attached_protection=False,
        )

        self.assertEqual(result.ord_id, "789")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/order")
        self.assertEqual(
            captured["body"],
            {
                "instId": "BTC-USD-SWAP",
                "tdMode": "cross",
                "side": "buy",
                "ordType": "limit",
                "px": "75000",
                "sz": "0.1",
                "posSide": "long",
                "clOrdId": "entry-2",
            },
        )

    def test_place_trigger_limit_algo_order_posts_order_algo_with_trigger_above_entry_for_buy(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-SWAP",
                            "instType": "SWAP",
                            "tickSz": "0.1",
                            "lotSz": "0.1",
                            "minSz": "0.1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "long_short_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {
                "data": [
                    {
                        "algoId": "algo-99",
                        "algoClOrdId": "a-1",
                        "sCode": "0",
                        "sMsg": "",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-SWAP",
            trade_inst_id="BTC-USD-SWAP",
            local_tp_sl_inst_id="BTC-USD-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        plan = OrderPlan(
            inst_id="BTC-USD-SWAP",
            side="buy",
            pos_side="long",
            size=Decimal("0.1"),
            take_profit=Decimal("82000"),
            stop_loss=Decimal("71000"),
            entry_reference=Decimal("75000"),
            atr_value=Decimal("1000"),
            signal="long",
            candle_ts=1710000000000,
            tp_sl_inst_id="BTC-USD-SWAP",
            tp_sl_mode="exchange",
        )

        result = client.place_trigger_limit_algo_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            plan,
            algo_cl_ord_id="a-1",
        )

        self.assertEqual(result.ord_id, "algo-99")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["path"], "/api/v5/trade/order-algo")
        body = captured["body"]
        assert isinstance(body, dict)
        self.assertEqual(body["ordType"], "trigger")
        self.assertEqual(body["orderPx"], "75000")
        self.assertEqual(body["triggerPx"], "75000.1")
        self.assertEqual(body["triggerPxType"], "mark")
        self.assertEqual(len(body["attachAlgoOrds"]), 1)

    def test_place_limit_net_mode_skips_pos_side_despite_launcher_long_short(self) -> None:
        """OKX net_mode 下发 posSide 会拒单；须以账户 /account/config 为准。"""
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-SWAP",
                            "instType": "SWAP",
                            "tickSz": "0.1",
                            "lotSz": "0.1",
                            "minSz": "0.1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "net_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {"data": [{"ordId": "n1", "sCode": "0", "sMsg": ""}]}

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-SWAP",
            trade_inst_id="BTC-USD-SWAP",
            local_tp_sl_inst_id="BTC-USD-SWAP",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.1"),
            trade_mode="isolated",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )
        plan = OrderPlan(
            inst_id="BTC-USD-SWAP",
            side="buy",
            pos_side="long",
            size=Decimal("0.1"),
            take_profit=Decimal("82000"),
            stop_loss=Decimal("71000"),
            entry_reference=Decimal("75000"),
            atr_value=Decimal("1000"),
            signal="long",
            candle_ts=1710000000000,
            tp_sl_inst_id="BTC-USD-SWAP",
            tp_sl_mode="exchange",
        )
        client.place_limit_order(Credentials(api_key="", secret_key="", passphrase=""), config, plan, include_attached_protection=False)
        body = captured["body"]
        assert isinstance(body, dict)
        self.assertNotIn("posSide", body)
        self.assertEqual(body["ccy"], "BTC")

    def test_place_simple_option_order_does_not_send_pos_side(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {"data": [{"ordId": "o1", "sCode": "0", "sMsg": ""}]}

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-260515-81000-C",
            trade_inst_id="BTC-USD-260515-81000-C",
            local_tp_sl_inst_id="BTC-USD-260515-81000-C",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )

        client.place_simple_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            inst_id="BTC-USD-260515-81000-C",
            side="buy",
            size=Decimal("1"),
            ord_type="limit",
            pos_side=None,
            price=Decimal("0.01"),
            cl_ord_id="test-option",
        )

        body = captured["body"]
        assert isinstance(body, dict)
        self.assertEqual("POST", captured["method"])
        self.assertEqual("/api/v5/trade/order", captured["path"])
        self.assertNotIn("posSide", body)

    def test_place_simple_reduce_only_keeps_short_pos_side_when_account_is_long_short(self) -> None:
        client = OkxRestClient()
        captured: dict[str, object] = {}

        def _stub_request(method: str, path: str, params=None, body=None, **kwargs):
            if path == "/api/v5/public/instruments":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-260626",
                            "instType": "FUTURES",
                            "tickSz": "0.1",
                            "lotSz": "1",
                            "minSz": "1",
                            "state": "live",
                            "settleCcy": "BTC",
                            "ctVal": "100",
                            "ctMult": "1",
                            "ctValCcy": "USD",
                            "uly": "BTC-USD",
                            "instFamily": "BTC-USD",
                        }
                    ]
                }
            if path == "/api/v5/account/config":
                return {"data": [{"posMode": "long_short_mode"}]}
            captured["method"] = method
            captured["path"] = path
            captured["body"] = body
            return {"data": [{"ordId": "r1", "sCode": "0", "sMsg": ""}]}

        client._request = _stub_request  # type: ignore[method-assign]
        config = StrategyConfig(
            inst_id="BTC-USD-260626",
            trade_inst_id="BTC-USD-260626",
            local_tp_sl_inst_id="BTC-USD-260626",
            bar="1H",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="last",
            tp_sl_mode="exchange",
            take_profit_mode="dynamic",
            risk_amount=Decimal("10"),
        )

        client.place_simple_order(
            Credentials(api_key="", secret_key="", passphrase=""),
            config,
            inst_id="BTC-USD-260626",
            side="buy",
            size=Decimal("1"),
            ord_type="market",
            pos_side="short",
            reduce_only=True,
        )

        body = captured["body"]
        assert isinstance(body, dict)
        self.assertEqual(body["side"], "buy")
        self.assertEqual(body["posSide"], "short")
        self.assertTrue(body["reduceOnly"])

    def test_request_uses_code_when_okx_error_message_is_empty(self) -> None:
        client = OkxRestClient()

        class _StubResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            @staticmethod
            def read() -> bytes:
                return b'{"code":"51000","msg":"","data":[]}'

        with patch("okx_quant.okx_client.request.urlopen", return_value=_StubResponse()):
            with self.assertRaises(OkxApiError) as context:
                client._request(
                    "GET",
                    "/api/v5/account/config",
                    auth=True,
                    credentials=Credentials(api_key="key", secret_key="secret", passphrase="pass"),
                    simulated=True,
                )

        self.assertEqual(context.exception.code, "51000")
        self.assertEqual(str(context.exception), "OKX API 错误 code=51000")

    def test_request_wraps_remote_disconnected_as_okx_api_error(self) -> None:
        client = OkxRestClient()

        with patch(
            "okx_quant.okx_client.request.urlopen",
            side_effect=http.client.RemoteDisconnected("Remote end closed connection without response"),
        ):
            with self.assertRaises(OkxApiError) as context:
                client._request("GET", "/api/v5/public/instruments")

        self.assertEqual(
            str(context.exception),
            "\u7f51\u7edc\u9519\u8bef\uff1aRemote end closed connection without response",
        )

    def test_get_account_bills_history_parses_funding_fee_bill(self) -> None:
        client = OkxRestClient()
        client._fetch_account_bill_history = lambda **kwargs: [  # type: ignore[method-assign]
            {
                "billId": "9001",
                "ts": "1713863360000",
                "instId": "ETH-USDT-SWAP",
                "instType": "SWAP",
                "type": "8",
                "subType": "173",
                "pnl": "-0.01",
                "balChg": "-0.01",
                "ccy": "USDT",
                "ordId": "2001",
                "tradeId": "3001",
            }
        ]

        items = client.get_account_bills_history(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="demo",
            inst_types=("SWAP",),
            limit=20,
        )

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].bill_id, "9001")
        self.assertEqual(items[0].bill_sub_type, "173")
        self.assertEqual(items[0].amount, Decimal("-0.01"))
        self.assertEqual(items[0].currency, "USDT")
