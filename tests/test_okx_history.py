from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Credentials
from okx_quant.okx_client import (
    OkxAccountAssetItem,
    OkxAccountConfig,
    OkxAccountOverview,
    OkxFillHistoryItem,
    OkxPositionHistoryItem,
    OkxRestClient,
)
from okx_quant.ui import (
    _build_account_asset_detail_text,
    _build_account_config_detail_text,
    _build_fill_history_detail_text,
    _build_position_history_detail_text,
    _build_position_history_usdt_price_map,
    _filter_position_history_items,
    _format_fill_history_pnl,
    _format_position_history_filter_stats,
    _format_position_history_pnl,
    _format_position_history_price,
    _position_history_realized_pnl_usdt,
)


class OkxHistoryParsingTest(TestCase):
    def test_get_account_overview_parses_summary_and_details(self) -> None:
        client = OkxRestClient()

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(path, "/api/v5/account/balance")
            return {
                "data": [
                    {
                        "totalEq": "12500.5",
                        "adjEq": "12000.1",
                        "availEq": "8600.2",
                        "upl": "-120.3",
                        "imr": "3400",
                        "mmr": "2100",
                        "ordFroz": "88.6",
                        "notionalUsd": "52000",
                        "details": [
                            {
                                "ccy": "USDT",
                                "eq": "5000",
                                "eqUsd": "5000",
                                "cashBal": "5200",
                                "availBal": "4800",
                                "availEq": "4700",
                                "upl": "-5",
                                "frozenBal": "20",
                                "liab": "0",
                            },
                            {
                                "ccy": "BTC",
                                "eq": "0.12",
                                "eqUsd": "9600",
                                "cashBal": "0.15",
                                "availBal": "0.1",
                                "availEq": "0.09",
                                "upl": "0.002",
                                "disEq": "0.11",
                                "crossLiab": "0.01",
                                "interest": "0.0001",
                            },
                        ],
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        overview = client.get_account_overview(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="live",
        )

        self.assertEqual(overview.total_equity, Decimal("12500.5"))
        self.assertEqual(overview.details[0].ccy, "BTC")
        self.assertEqual(overview.details[0].equity_usd, Decimal("9600"))
        self.assertEqual(overview.details[1].ccy, "USDT")

    def test_get_account_config_parses_key_fields(self) -> None:
        client = OkxRestClient()

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(path, "/api/v5/account/config")
            return {
                "data": [
                    {
                        "acctLv": "4",
                        "posMode": "net",
                        "autoLoan": "true",
                        "greeksType": "PA",
                        "level": "Lv1",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        config = client.get_account_config(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="live",
        )

        self.assertEqual(config.account_level, "4")
        self.assertEqual(config.position_mode, "net")
        self.assertTrue(config.auto_loan)
        self.assertEqual(config.greeks_type, "PA")

    def test_get_fills_history_merges_and_sorts_items(self) -> None:
        client = OkxRestClient()

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(path, "/api/v5/trade/fills-history")
            inst_type = params["instType"]
            if inst_type == "SWAP":
                return {
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "instType": "SWAP",
                            "side": "buy",
                            "posSide": "long",
                            "fillPx": "71210.5",
                            "fillSz": "3",
                            "fillFee": "-0.5",
                            "fillFeeCcy": "USDT",
                            "fillPnl": "12.3",
                            "ordId": "1",
                            "tradeId": "11",
                            "execType": "T",
                            "fillTime": "1710000000200",
                        }
                    ]
                }
            return {
                "data": [
                    {
                        "instId": "BTC-USD-260626-100000-C",
                        "instType": "OPTION",
                        "side": "sell",
                        "posSide": "short",
                        "fillPx": "0.015",
                        "fillSz": "20",
                        "fillFee": "-0.0001",
                        "fillFeeCcy": "BTC",
                        "fillPnl": "-0.0005",
                        "ordId": "2",
                        "tradeId": "22",
                        "execType": "M",
                        "fillTime": "1710000000100",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        items = client.get_fills_history(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="live",
            inst_types=("OPTION", "SWAP"),
            limit=10,
        )

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].inst_id, "BTC-USDT-SWAP")
        self.assertEqual(items[0].fill_price, Decimal("71210.5"))
        self.assertEqual(items[1].fee_currency, "BTC")

    def test_get_positions_history_merges_and_sorts_items(self) -> None:
        client = OkxRestClient()

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(path, "/api/v5/account/positions-history")
            inst_type = params["instType"]
            if inst_type == "FUTURES":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-260626",
                            "instType": "FUTURES",
                            "mgnMode": "cross",
                            "posSide": "short",
                            "direction": "net",
                            "openAvgPx": "70000",
                            "closeAvgPx": "69000",
                            "closeTotalPos": "200",
                            "pnl": "0.12",
                            "realizedPnl": "0.08",
                            "settledPnl": "0.01",
                            "uTime": "1710000000300",
                        }
                    ]
                }
            return {
                "data": [
                    {
                        "instId": "BTC-USD-260626-100000-C",
                        "instType": "OPTION",
                        "mgnMode": "isolated",
                        "posSide": "long",
                        "openAvgPx": "0.02",
                        "closeAvgPx": "0.03",
                        "closeSz": "10",
                        "pnl": "0.001",
                        "realizedPnl": "0.0005",
                        "settledPnl": "0",
                        "uTime": "1710000000200",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        items = client.get_positions_history(
            Credentials(api_key="", secret_key="", passphrase=""),
            environment="live",
            inst_types=("OPTION", "FUTURES"),
            limit=10,
        )

        self.assertEqual(len(items), 2)
        self.assertEqual(items[0].inst_id, "BTC-USD-260626")
        self.assertEqual(items[0].close_size, Decimal("200"))
        self.assertEqual(items[1].mgn_mode, "isolated")

    def test_get_tickers_parses_market_rows(self) -> None:
        client = OkxRestClient()

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(path, "/api/v5/market/tickers")
            self.assertEqual(params["instType"], "OPTION")
            self.assertEqual(params["instFamily"], "BTC-USD")
            return {
                "data": [
                    {
                        "instId": "BTC-USD-260626-100000-C",
                        "last": "0.012",
                        "bidPx": "0.0115",
                        "askPx": "0.0125",
                        "markPx": "0.0121",
                        "idxPx": "98500",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        items = client.get_tickers("OPTION", inst_family="BTC-USD")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].inst_id, "BTC-USD-260626-100000-C")
        self.assertEqual(items[0].mark, Decimal("0.0121"))
        self.assertEqual(items[0].index, Decimal("98500"))

    def test_get_mark_price_candles_pages_when_limit_exceeds_public_cap(self) -> None:
        client = OkxRestClient()
        requests: list[dict[str, str]] = []

        def _stub_request(method: str, path: str, params=None, **kwargs):
            self.assertEqual(method, "GET")
            self.assertEqual(path, "/api/v5/market/mark-price-candles")
            requests.append(dict(params or {}))
            after = (params or {}).get("after")
            if after is None:
                return {
                    "data": [
                        ["3000", "1.3", "1.4", "1.2", "1.35", "1"],
                        ["2000", "1.2", "1.3", "1.1", "1.25", "1"],
                        ["1000", "1.1", "1.2", "1.0", "1.15", "1"],
                    ]
                }
            self.assertEqual(after, "1000")
            return {
                "data": [
                    ["0", "1.0", "1.1", "0.9", "1.05", "1"],
                    ["-1000", "0.9", "1.0", "0.8", "0.95", "1"],
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]
        candles = client.get_mark_price_candles("BTC-USD-260626-100000-C", "1H", limit=5)

        self.assertEqual(len(candles), 5)
        self.assertEqual([item.ts for item in candles], [-1000, 0, 1000, 2000, 3000])
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0]["limit"], "5")
        self.assertEqual(requests[1]["limit"], "2")
        self.assertEqual(requests[1]["after"], "1000")

    def test_position_history_realized_pnl_usdt_converts_coin_margin(self) -> None:
        item = OkxPositionHistoryItem(
            update_time=1710000000200,
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            mgn_mode="isolated",
            pos_side="long",
            direction=None,
            open_avg_price=Decimal("0.02"),
            close_avg_price=Decimal("0.03"),
            close_size=Decimal("10"),
            pnl=Decimal("0.001"),
            realized_pnl=Decimal("0.0005"),
            settle_pnl=Decimal("0"),
            raw={},
        )
        self.assertEqual(_position_history_realized_pnl_usdt(item, {"BTC": Decimal("80000")}), Decimal("40"))

    def test_position_history_realized_pnl_usdt_keeps_usdt_value(self) -> None:
        item = OkxPositionHistoryItem(
            update_time=1710000000300,
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            mgn_mode="cross",
            pos_side="long",
            direction=None,
            open_avg_price=Decimal("70000"),
            close_avg_price=Decimal("70500"),
            close_size=Decimal("3"),
            pnl=Decimal("12.3"),
            realized_pnl=Decimal("8.5"),
            settle_pnl=Decimal("0"),
            raw={},
        )
        self.assertIsNone(_position_history_realized_pnl_usdt(item, {"BTC": Decimal("80000")}))

    def test_build_position_history_usdt_price_map_uses_current_spot_snapshot(self) -> None:
        client = OkxRestClient()

        def _stub_ticker(inst_id: str):
            if inst_id == "BTC-USDT":
                class _Ticker:
                    last = Decimal("90000")
                    bid = None
                    ask = None
                    mark = None
                    index = None

                return _Ticker()
            raise AssertionError(inst_id)

        client.get_ticker = _stub_ticker  # type: ignore[method-assign]
        items = [
            OkxPositionHistoryItem(
                update_time=1710000000200,
                inst_id="BTC-USD-260626-100000-C",
                inst_type="OPTION",
                mgn_mode="isolated",
                pos_side="long",
                direction=None,
                open_avg_price=Decimal("0.02"),
                close_avg_price=Decimal("0.03"),
                close_size=Decimal("10"),
                pnl=Decimal("0.001"),
                realized_pnl=Decimal("0.0005"),
                settle_pnl=Decimal("0"),
                raw={},
            ),
            OkxPositionHistoryItem(
                update_time=1710000000300,
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                mgn_mode="cross",
                pos_side="long",
                direction=None,
                open_avg_price=Decimal("70000"),
                close_avg_price=Decimal("70500"),
                close_size=Decimal("3"),
                pnl=Decimal("12.3"),
                realized_pnl=Decimal("8.5"),
                settle_pnl=Decimal("0"),
                raw={},
            ),
        ]

        price_map = _build_position_history_usdt_price_map(client, items)

        self.assertEqual(price_map["BTC"], Decimal("90000"))
        self.assertEqual(price_map["USDT"], Decimal("1"))

    def test_fill_history_detail_formats_realized_pnl_with_five_decimals(self) -> None:
        detail = _build_fill_history_detail_text(
            OkxFillHistoryItem(
                fill_time=1710000000200,
                inst_id="BTC-USD-260626-100000-C",
                inst_type="OPTION",
                side="sell",
                pos_side="short",
                fill_price=Decimal("0.015"),
                fill_size=Decimal("20"),
                fill_fee=Decimal("-0.0001"),
                fee_currency="BTC",
                pnl=Decimal("-0.0005"),
                order_id="2",
                trade_id="22",
                exec_type="M",
                raw={},
            )
        )
        self.assertIn("已实现盈亏：-0.0005", detail)

    def test_fill_history_pnl_formats_usdt_with_two_decimals(self) -> None:
        text = _format_fill_history_pnl(
            OkxFillHistoryItem(
                fill_time=1710000000200,
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                side="buy",
                pos_side="long",
                fill_price=Decimal("71210.5"),
                fill_size=Decimal("3"),
                fill_fee=Decimal("-0.5"),
                fee_currency="USDT",
                pnl=Decimal("12.3"),
                order_id="1",
                trade_id="11",
                exec_type="T",
                raw={},
            )
        )
        self.assertEqual(text, "+12.30")

    def test_position_history_detail_formats_option_values_with_capped_decimals(self) -> None:
        detail = _build_position_history_detail_text(
            OkxPositionHistoryItem(
                update_time=1710000000200,
                inst_id="BTC-USD-260626-100000-C",
                inst_type="OPTION",
                mgn_mode="isolated",
                pos_side="long",
                direction=None,
                open_avg_price=Decimal("0.02"),
                close_avg_price=Decimal("0.03"),
                close_size=Decimal("10"),
                pnl=Decimal("0.001"),
                realized_pnl=Decimal("0.0005"),
                settle_pnl=Decimal("0"),
                raw={},
            ),
            {"BTC": Decimal("90000")},
        )
        self.assertIn("开仓均价：0.02", detail)
        self.assertIn("平仓均价：0.03", detail)
        self.assertIn("盈亏：0.001", detail)
        self.assertIn("已实现盈亏：+0.0005", detail)

    def test_position_history_formats_usdt_values_with_two_decimals(self) -> None:
        item = OkxPositionHistoryItem(
            update_time=1710000000300,
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            mgn_mode="cross",
            pos_side="long",
            direction=None,
            open_avg_price=Decimal("70000"),
            close_avg_price=Decimal("70500"),
            close_size=Decimal("3"),
            pnl=Decimal("12.3"),
            realized_pnl=Decimal("8.5"),
            settle_pnl=Decimal("0"),
            raw={},
        )
        self.assertEqual(_format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type), "70000.00")
        self.assertEqual(_format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type), "70500.00")
        self.assertEqual(_format_position_history_pnl(item.pnl, item), "12.30")
        self.assertEqual(_format_position_history_pnl(item.realized_pnl, item, with_sign=True), "+8.50")

    def test_filter_position_history_items_supports_type_margin_and_keyword(self) -> None:
        items = [
            OkxPositionHistoryItem(
                update_time=1710000000300,
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                mgn_mode="cross",
                pos_side="long",
                direction=None,
                open_avg_price=Decimal("70000"),
                close_avg_price=Decimal("70500"),
                close_size=Decimal("3"),
                pnl=Decimal("12.3"),
                realized_pnl=Decimal("8.5"),
                settle_pnl=Decimal("0"),
                raw={},
            ),
            OkxPositionHistoryItem(
                update_time=1710000000200,
                inst_id="BTC-USD-260626-100000-C",
                inst_type="OPTION",
                mgn_mode="isolated",
                pos_side="short",
                direction=None,
                open_avg_price=Decimal("0.02"),
                close_avg_price=Decimal("0.03"),
                close_size=Decimal("10"),
                pnl=Decimal("0.001"),
                realized_pnl=Decimal("0.0005"),
                settle_pnl=Decimal("0"),
                raw={},
            ),
        ]

        filtered_by_type = _filter_position_history_items(items, inst_type="OPTION")
        filtered_by_margin = _filter_position_history_items(items, margin_mode="cross")
        filtered_by_keyword = _filter_position_history_items(items, keyword="100000-c")

        self.assertEqual([index for index, _ in filtered_by_type], [1])
        self.assertEqual([index for index, _ in filtered_by_margin], [0])
        self.assertEqual([index for index, _ in filtered_by_keyword], [1])

    def test_format_position_history_filter_stats_sums_option_totals(self) -> None:
        filtered_items = [
            (
                0,
                OkxPositionHistoryItem(
                    update_time=1710000000200,
                    inst_id="BTC-USD-260626-100000-C",
                    inst_type="OPTION",
                    mgn_mode="isolated",
                    pos_side="long",
                    direction=None,
                    open_avg_price=Decimal("0.02"),
                    close_avg_price=Decimal("0.03"),
                    close_size=Decimal("10"),
                    pnl=Decimal("0.001"),
                    realized_pnl=Decimal("0.0005"),
                    settle_pnl=Decimal("0"),
                    raw={},
                ),
            ),
            (
                1,
                OkxPositionHistoryItem(
                    update_time=1710000000300,
                    inst_id="BTC-USD-260626-90000-P",
                    inst_type="OPTION",
                    mgn_mode="cross",
                    pos_side="short",
                    direction=None,
                    open_avg_price=Decimal("0.01"),
                    close_avg_price=Decimal("0.02"),
                    close_size=Decimal("5"),
                    pnl=Decimal("-0.0002"),
                    realized_pnl=Decimal("-0.0001"),
                    settle_pnl=Decimal("0"),
                    raw={},
                ),
            ),
        ]

        summary = _format_position_history_filter_stats(filtered_items, {"BTC": Decimal("90000")})

        self.assertIn("盈亏合计 BTC +0.0008", summary)
        self.assertIn("已实现合计 BTC +0.0004", summary)
        self.assertIn("折合USDT合计 +36", summary)

    def test_format_position_history_filter_stats_groups_mixed_currencies(self) -> None:
        filtered_items = [
            (
                0,
                OkxPositionHistoryItem(
                    update_time=1710000000300,
                    inst_id="BTC-USDT-SWAP",
                    inst_type="SWAP",
                    mgn_mode="cross",
                    pos_side="long",
                    direction=None,
                    open_avg_price=Decimal("70000"),
                    close_avg_price=Decimal("70500"),
                    close_size=Decimal("3"),
                    pnl=Decimal("12.3"),
                    realized_pnl=Decimal("8.5"),
                    settle_pnl=Decimal("0"),
                    raw={},
                ),
            ),
            (
                1,
                OkxPositionHistoryItem(
                    update_time=1710000000200,
                    inst_id="BTC-USD-260626-100000-C",
                    inst_type="OPTION",
                    mgn_mode="isolated",
                    pos_side="long",
                    direction=None,
                    open_avg_price=Decimal("0.02"),
                    close_avg_price=Decimal("0.03"),
                    close_size=Decimal("10"),
                    pnl=Decimal("0.001"),
                    realized_pnl=Decimal("0.0005"),
                    settle_pnl=Decimal("0"),
                    raw={},
                ),
            ),
        ]

        summary = _format_position_history_filter_stats(filtered_items, {"BTC": Decimal("80000"), "USDT": Decimal("1")})

        self.assertIn("盈亏合计 USDT +12.30 / BTC +0.001", summary)
        self.assertIn("已实现合计 USDT +8.50 / BTC +0.0005", summary)
        self.assertIn("折合USDT合计 +40", summary)

    def test_build_account_config_detail_text_contains_translated_labels(self) -> None:
        text = _build_account_config_detail_text(
            OkxAccountConfig(account_level="4", position_mode="net", auto_loan=True, greeks_type="PA", level="Lv1", raw={}),
            OkxAccountOverview(
                total_equity=Decimal("10000"),
                adjusted_equity=Decimal("9800"),
                isolated_equity=None,
                available_equity=Decimal("7600"),
                unrealized_pnl=Decimal("50"),
                initial_margin=Decimal("2000"),
                maintenance_margin=Decimal("900"),
                order_frozen=Decimal("88"),
                notional_usd=Decimal("42000"),
                details=(),
                raw={},
            ),
            profile_name="主账户",
            environment="live",
        )

        self.assertIn("账户模式：组合保证金", text)
        self.assertIn("持仓模式：净持仓 net", text)
        self.assertIn("Greeks类型：PA", text)

    def test_build_account_asset_detail_text_contains_asset_fields(self) -> None:
        text = _build_account_asset_detail_text(
            OkxAccountAssetItem(
                ccy="BTC",
                equity=Decimal("0.12"),
                equity_usd=Decimal("9600"),
                cash_balance=Decimal("0.15"),
                available_balance=Decimal("0.1"),
                available_equity=Decimal("0.09"),
                frozen_balance=Decimal("0.01"),
                unrealized_pnl=Decimal("0.002"),
                discount_equity=Decimal("0.11"),
                liability=Decimal("0.003"),
                cross_liability=Decimal("0.001"),
                interest=Decimal("0.0001"),
                raw={},
            )
        )

        self.assertIn("币种：BTC", text)
        self.assertIn("折合USD：9600.00", text)
        self.assertIn("全仓负债：0.001", text)
