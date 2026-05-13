import http.client
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from okx_quant.models import Credentials, OrderPlan, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxRestClient, _okx_trade_order_request_log_fragment


class OkxClientOrderRequestTest(TestCase):
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
