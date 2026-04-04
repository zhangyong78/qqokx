from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Credentials, Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient, OkxTicker
from okx_quant.ui import (
    _aggregate_position_metrics,
    _build_upl_usdt_price_map,
    _build_group_row_values,
    _format_margin_mode,
    _format_mark_price,
    _format_option_trade_side,
    _format_optional_decimal_fixed,
    _format_optional_integer,
    _format_optional_usdt_precise,
    _format_optional_usdt,
    _format_group_option_trade_side,
    _format_group_position_size,
    _format_position_avg_price,
    _format_position_avg_price_usdt,
    _format_position_mark_price_usdt,
    _format_position_market_value,
    _format_position_size,
    _format_position_unrealized_pnl,
    _group_positions_for_tree,
    _group_pnl_places,
    _margin_mode_tag,
    _option_search_shortcuts,
    _position_theta_usdt,
    _position_mark_price_usdt,
    _position_unrealized_pnl_usdt,
)


class _StubClient:
    def get_ticker(self, inst_id: str) -> OkxTicker:
        prices = {
            "BTC-USDT": Decimal("80000"),
            "ETH-USDT": Decimal("4000"),
        }
        return OkxTicker(
            inst_id=inst_id,
            last=prices.get(inst_id),
            bid=None,
            ask=None,
            mark=None,
            index=None,
            raw={},
        )


def _make_position(*, inst_id: str, upl: str, margin_ccy: str | None) -> OkxPosition:
    return OkxPosition(
        inst_id=inst_id,
        inst_type="OPTION",
        pos_side="long",
        mgn_mode="cross",
        position=Decimal("1"),
        avail_position=None,
        avg_price=None,
        mark_price=None,
        unrealized_pnl=Decimal(upl),
        unrealized_pnl_ratio=None,
        liquidation_price=None,
        leverage=None,
        margin_ccy=margin_ccy,
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


class PositionUplConversionTest(TestCase):
    def test_build_upl_usdt_price_map_uses_spot_prices(self) -> None:
        prices = _build_upl_usdt_price_map(
            _StubClient(),
            [
                _make_position(inst_id="BTC-USD-260626-100000-C", upl="0.0012", margin_ccy="BTC"),
                _make_position(inst_id="ETH-USD-260626-4000-C", upl="0.5", margin_ccy="USDT"),
            ],
        )

        self.assertEqual(prices["BTC"], Decimal("80000"))
        self.assertEqual(prices["USDT"], Decimal("1"))

    def test_position_unrealized_pnl_usdt_converts_from_margin_currency(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0.0015", margin_ccy="BTC")
        converted = _position_unrealized_pnl_usdt(position, {"BTC": Decimal("80000")})
        self.assertEqual(converted, Decimal("120"))

    def test_position_theta_usdt_converts_from_margin_currency(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "theta": Decimal("-0.00034")})
        converted = _position_theta_usdt(position, {"BTC": Decimal("80000")})
        self.assertEqual(converted, Decimal("-27.20000"))

    def test_format_position_size_uses_ct_val_and_ct_mult(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0.0015", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "position": Decimal("20"), "pos_side": "net"})
        instrument = Instrument(
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        text = _format_position_size(position, {instrument.inst_id: instrument})
        self.assertEqual(text, "0.2 BTC (long)")

    def test_format_position_size_marks_short_with_negative_amount(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0.0015", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "position": Decimal("20"), "pos_side": "short"})
        instrument = Instrument(
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        text = _format_position_size(position, {instrument.inst_id: instrument})
        self.assertEqual(text, "-0.2 BTC (short)")

    def test_format_position_size_returns_dash_for_zero_position(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "position": Decimal("0"), "pos_side": "net"})
        instrument = Instrument(
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        self.assertEqual(_format_position_size(position, {instrument.inst_id: instrument}), "-")

    def test_format_option_trade_side_maps_all_four_option_directions(self) -> None:
        call_long = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        call_short = OkxPosition(**{**call_long.__dict__, "position": Decimal("1"), "pos_side": "short"})
        put_long = OkxPosition(**{**call_long.__dict__, "inst_id": "BTC-USD-260626-50000-P"})
        put_short = OkxPosition(**{**put_long.__dict__, "position": Decimal("1"), "pos_side": "short"})

        self.assertEqual(_format_option_trade_side(call_long), "买购")
        self.assertEqual(_format_option_trade_side(call_short), "卖购")
        self.assertEqual(_format_option_trade_side(put_long), "买沽")
        self.assertEqual(_format_option_trade_side(put_short), "卖沽")

    def test_format_group_option_trade_side_summarizes_by_option_direction(self) -> None:
        positions = [
            OkxPosition(**{**_make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC").__dict__, "position": Decimal("20"), "pos_side": "long"}),
            OkxPosition(**{**_make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC").__dict__, "position": Decimal("10"), "pos_side": "short"}),
            OkxPosition(**{**_make_position(inst_id="BTC-USD-260626-50000-P", upl="0", margin_ccy="BTC").__dict__, "position": Decimal("30"), "pos_side": "short"}),
        ]
        instruments = {
            "BTC-USD-260626-100000-C": Instrument(
                inst_id="BTC-USD-260626-100000-C",
                inst_type="OPTION",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("1"),
                ct_mult=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
            "BTC-USD-260626-50000-P": Instrument(
                inst_id="BTC-USD-260626-50000-P",
                inst_type="OPTION",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("1"),
                ct_mult=Decimal("0.01"),
                ct_val_ccy="BTC",
            ),
        }
        self.assertEqual(
            _format_group_option_trade_side(positions, instruments),
            "买购0.20BTC / 卖购0.10BTC / 卖沽0.30BTC",
        )

    def test_format_position_size_converts_usd_futures_to_base_coin(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC")
        position = OkxPosition(
            **{
                **position.__dict__,
                "inst_type": "FUTURES",
                "position": Decimal("200"),
                "pos_side": "short",
                "avg_price": Decimal("50000"),
            }
        )
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
        self.assertEqual(_format_position_size(position, {instrument.inst_id: instrument}), "-0.4000 BTC (short)")

    def test_aggregate_position_metrics_uses_coin_delta_for_usd_futures(self) -> None:
        futures = _make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC")
        futures = OkxPosition(
            **{
                **futures.__dict__,
                "inst_type": "FUTURES",
                "position": Decimal("200"),
                "pos_side": "short",
                "mark_price": Decimal("50000"),
                "delta": Decimal("251886"),
            }
        )
        option_position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        option_position = OkxPosition(**{**option_position.__dict__, "delta": Decimal("0.25000")})
        futures_instrument = Instrument(
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
        option_instrument = Instrument(
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        metrics = _aggregate_position_metrics(
            [futures, option_position],
            {"BTC": Decimal("80000")},
            {
                futures_instrument.inst_id: futures_instrument,
                option_instrument.inst_id: option_instrument,
            },
        )
        self.assertEqual(metrics["delta"], Decimal("-0.15000"))

    def test_format_position_avg_price_usdt_for_option(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "avg_price": Decimal("0.015")})
        self.assertEqual(_format_position_avg_price_usdt(position, {"BTC": Decimal("80000")}), "1200")

    def test_position_mark_price_usdt_for_option(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "mark_price": Decimal("0.0524")})
        self.assertEqual(_position_mark_price_usdt(position, {"BTC": Decimal("80000")}), Decimal("4192.0000"))

    def test_format_position_mark_price_usdt_for_option(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "mark_price": Decimal("0.0524")})
        self.assertEqual(_format_position_mark_price_usdt(position, {"BTC": Decimal("80000")}), "4192")

    def test_format_position_avg_price_uses_b_prefix_for_btc_option(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "avg_price": Decimal("0.015")})
        self.assertEqual(_format_position_avg_price(position, {}), "B 0.0150")

    def test_format_position_avg_price_option_follows_tick_size(self) -> None:
        position = _make_position(inst_id="BTC-USD-260529-80000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "avg_price": Decimal("0.0410714285714286")})
        instrument = Instrument(
            inst_id="BTC-USD-260529-80000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        self.assertEqual(_format_position_avg_price(position, {instrument.inst_id: instrument}), "B 0.0411")

    def test_format_mark_price_uses_dollar_prefix_for_usd_family(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC")
        position = OkxPosition(
            **{
                **position.__dict__,
                "inst_type": "FUTURES",
                "mark_price": Decimal("71210.9"),
            }
        )
        self.assertEqual(_format_mark_price(position), "$ 71210.9")

    def test_format_mark_price_uses_b_prefix_for_btc_option(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-80000-P", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "mark_price": Decimal("0.0524")})
        self.assertEqual(_format_mark_price(position), "B 0.0524")

    def test_format_mark_price_uses_e_prefix_for_eth_option(self) -> None:
        position = _make_position(inst_id="ETH-USD-260626-2200-C", upl="0", margin_ccy="ETH")
        position = OkxPosition(**{**position.__dict__, "mark_price": Decimal("0.0125")})
        self.assertEqual(_format_mark_price(position), "E 0.0125")

    def test_get_positions_prefers_pa_greeks_for_options(self) -> None:
        client = OkxRestClient()

        def _stub_request(*args, **kwargs):
            return {
                "data": [
                    {
                        "instId": "BTC-USD-260626-100000-C",
                        "instType": "OPTION",
                        "posSide": "net",
                        "mgnMode": "cross",
                        "pos": "20",
                        "deltaPA": "0.12345",
                        "gammaPA": "0.54321",
                        "vegaPA": "0.00012",
                        "thetaPA": "-0.00034",
                        "deltaBS": "9.9",
                        "gammaBS": "8.8",
                        "vegaBS": "7.7",
                        "thetaBS": "6.6",
                        "imr": "123.45",
                        "mmr": "67.89",
                    }
                ]
            }

        client._request = _stub_request  # type: ignore[method-assign]

        positions = client.get_positions(
            credentials=Credentials(api_key="", secret_key="", passphrase=""),
            environment="live",
        )

        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].delta, Decimal("0.12345"))
        self.assertEqual(positions[0].gamma, Decimal("0.54321"))
        self.assertEqual(positions[0].vega, Decimal("0.00012"))
        self.assertEqual(positions[0].theta, Decimal("-0.00034"))

    def test_get_trigger_price_mark_falls_back_to_public_mark_price_for_option(self) -> None:
        client = OkxRestClient()
        requests: list[tuple[str, dict[str, str] | None]] = []

        def _stub_request(method: str, path: str, params=None, **kwargs):
            requests.append((path, params))
            if path == "/api/v5/market/ticker":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-260626-100000-C",
                            "last": "0.0115",
                            "bidPx": "0.011",
                            "askPx": "0.0115",
                        }
                    ]
                }
            if path == "/api/v5/public/mark-price":
                return {
                    "data": [
                        {
                            "instId": "BTC-USD-260626-100000-C",
                            "instType": "OPTION",
                            "markPx": "0.0116421317242157",
                        }
                    ]
                }
            raise AssertionError(path)

        client._request = _stub_request  # type: ignore[method-assign]

        price = client.get_trigger_price("BTC-USD-260626-100000-C", "mark")

        self.assertEqual(price, Decimal("0.0116421317242157"))
        self.assertEqual(requests[0][0], "/api/v5/market/ticker")
        self.assertEqual(requests[1][0], "/api/v5/public/mark-price")

    def test_position_display_formatting_rules(self) -> None:
        self.assertEqual(
            _format_optional_decimal_fixed(Decimal("0.123456789"), places=8, with_sign=True),
            "+0.12345679",
        )
        self.assertEqual(
            _format_optional_decimal_fixed(Decimal("0.7399188161835572"), places=5, with_sign=True),
            "+0.73992",
        )
        self.assertEqual(_format_optional_usdt(Decimal("123.6")), "+124")
        self.assertEqual(_format_optional_usdt_precise(Decimal("-27.2"), places=2), "-27.20")
        self.assertEqual(_format_optional_integer(Decimal("41735.8515")), "41736")
        self.assertEqual(_group_pnl_places("BTC"), 5)
        self.assertEqual(_group_pnl_places("USDT"), 2)

    def test_position_unrealized_pnl_formats_with_currency_and_ratio(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="-0.0045", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "unrealized_pnl_ratio": Decimal("-0.4975")})
        self.assertEqual(_format_position_unrealized_pnl(position), "-0.00450000 BTC（-49.75%）")

    def test_swap_and_futures_unrealized_pnl_use_two_decimals(self) -> None:
        swap = _make_position(inst_id="OKB-USDT-SWAP", upl="-2733.7537", margin_ccy="USDT")
        swap = OkxPosition(
            **{
                **swap.__dict__,
                "inst_type": "SWAP",
                "unrealized_pnl_ratio": Decimal("-0.3897"),
            }
        )
        futures = _make_position(inst_id="BTC-USD-260626", upl="0.10909674", margin_ccy="BTC")
        futures = OkxPosition(
            **{
                **futures.__dict__,
                "inst_type": "FUTURES",
                "unrealized_pnl_ratio": Decimal("0.1366"),
            }
        )
        self.assertEqual(_format_position_unrealized_pnl(swap), "-2733.75 USDT（-38.97%）")
        self.assertEqual(_format_position_unrealized_pnl(futures), "+0.11 BTC（13.66%）")

    def test_swap_and_futures_avg_price_follow_tick_size(self) -> None:
        swap = _make_position(inst_id="OKB-USDT-SWAP", upl="0", margin_ccy="USDT")
        swap = OkxPosition(**{**swap.__dict__, "inst_type": "SWAP", "avg_price": Decimal("105.2287685")})
        swap_instrument = Instrument(
            inst_id="OKB-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="OKB",
        )
        futures = _make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC")
        futures = OkxPosition(**{**futures.__dict__, "inst_type": "FUTURES", "avg_price": Decimal("74315.80134")})
        futures_instrument = Instrument(
            inst_id="BTC-USD-260626",
            inst_type="FUTURES",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.1"),
            min_size=Decimal("0.1"),
            state="live",
            ct_val=Decimal("100"),
            ct_mult=Decimal("1"),
            ct_val_ccy="USD",
        )
        instruments = {
            swap_instrument.inst_id: swap_instrument,
            futures_instrument.inst_id: futures_instrument,
        }
        self.assertEqual(_format_position_avg_price(swap, instruments), "$ 105.23")
        self.assertEqual(_format_position_avg_price(futures, instruments), "$ 74315.8")

    def test_option_market_value_formats_with_native_and_usdt(self) -> None:
        position = _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC")
        position = OkxPosition(**{**position.__dict__, "position": Decimal("20"), "mark_price": Decimal("0.00355")})
        instrument = Instrument(
            inst_id="BTC-USD-260626-100000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.01"),
            ct_val_ccy="BTC",
        )
        text = _format_position_market_value(position, {instrument.inst_id: instrument}, {"BTC": Decimal("70000")})
        self.assertEqual(text, "0.00071 BTC (≈50 USDT)")

    def test_swap_market_value_formats_in_quote_currency(self) -> None:
        position = _make_position(inst_id="DOGE-USDT-SWAP", upl="0", margin_ccy="USDT")
        position = OkxPosition(
            **{
                **position.__dict__,
                "inst_type": "SWAP",
                "position": Decimal("250"),
                "mark_price": Decimal("0.09477"),
            }
        )
        instrument = Instrument(
            inst_id="DOGE-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.00001"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
            ct_val=Decimal("1000"),
            ct_mult=Decimal("1"),
            ct_val_ccy="DOGE",
            settle_ccy="USDT",
        )
        text = _format_position_market_value(position, {instrument.inst_id: instrument}, {"DOGE": Decimal("0.09477")})
        self.assertEqual(text, "23692.50000 USDT")

    def test_format_margin_mode_returns_cross_or_isolated(self) -> None:
        self.assertEqual(_format_margin_mode("isolated"), "逐仓 isolated")
        self.assertEqual(_format_margin_mode("cross"), "全仓 cross")
        self.assertEqual(_format_margin_mode(""), "-")
        self.assertEqual(_margin_mode_tag("isolated"), "isolated_mode")
        self.assertEqual(_margin_mode_tag("cross"), "cross_mode")

    def test_group_row_values_use_five_decimals_for_coin_pnl(self) -> None:
        values = _build_group_row_values(
            "缁勫悎",
            {
                "count": 3,
                "upl": Decimal("-0.06947484"),
                "upl_usdt": Decimal("-4903"),
                "market_value_usdt": Decimal("12345"),
                "realized": Decimal("0.7399188161835572"),
                "pnl_currency": "BTC",
                "imr": None,
                "mmr": None,
                "delta": Decimal("1.23456"),
                "gamma": None,
                "vega": None,
                "theta": None,
                "theta_usdt": Decimal("-27.2"),
            },
        )
        self.assertEqual(values[8], "-0.06947")
        self.assertEqual(values[10], "+0.73992")
        self.assertEqual(values[16], "1.23456")
        self.assertEqual(values[20], "-27.20")

    def test_group_row_values_use_two_decimals_for_usdt_pnl(self) -> None:
        values = _build_group_row_values(
            "缁勫悎",
            {
                "count": 1,
                "size_display": "250000 DOGE",
                "upl": Decimal("-2733.7537"),
                "upl_usdt": Decimal("-2734"),
                "market_value_usdt": Decimal("360"),
                "realized": Decimal("-31.358025335611146"),
                "pnl_currency": "USDT",
                "imr": None,
                "mmr": None,
                "delta": Decimal("250000"),
                "gamma": None,
                "vega": None,
                "theta": None,
                "theta_usdt": Decimal("123.456"),
            },
        )
        self.assertEqual(values[6], "1 个持仓 | 250000 DOGE")
        self.assertEqual(values[8], "-2733.75")
        self.assertEqual(values[10], "-31.36")
        self.assertEqual(values[16], "250000.00000")
        self.assertEqual(values[20], "+123.46")

    def test_format_group_position_size_accumulates_coin_quantity(self) -> None:
        positions = [
            OkxPosition(
                **{
                    **_make_position(inst_id="BTC-USD-260626-50000-P", upl="0", margin_ccy="BTC").__dict__,
                    "position": Decimal("-20"),
                    "pos_side": "short",
                }
            ),
            OkxPosition(
                **{
                    **_make_position(inst_id="BTC-USD-260626-60000-P", upl="0", margin_ccy="BTC").__dict__,
                    "position": Decimal("-30"),
                    "pos_side": "short",
                }
            ),
        ]
        instruments = {
            "BTC-USD-260626-50000-P": Instrument(
                inst_id="BTC-USD-260626-50000-P",
                inst_type="OPTION",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_mult=Decimal("1"),
                ct_val_ccy="BTC",
                settle_ccy="BTC",
            ),
            "BTC-USD-260626-60000-P": Instrument(
                inst_id="BTC-USD-260626-60000-P",
                inst_type="OPTION",
                tick_size=Decimal("0.0001"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                ct_val=Decimal("0.01"),
                ct_mult=Decimal("1"),
                ct_val_ccy="BTC",
                settle_ccy="BTC",
            ),
        }

        self.assertEqual(_format_group_position_size(positions, instruments), "-0.50 BTC")

    def test_group_positions_for_tree_orders_buckets_by_nearest_date(self) -> None:
        positions = [
            OkxPosition(**{**_make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC").__dict__, "inst_type": "FUTURES"}),
            _make_position(inst_id="BTC-USD-260529-90000-C", upl="0", margin_ccy="BTC"),
            _make_position(inst_id="BTC-USD-260327-70000-C", upl="0", margin_ccy="BTC"),
            OkxPosition(**{**_make_position(inst_id="BTC-USDT-SWAP", upl="0", margin_ccy="USDT").__dict__, "inst_type": "SWAP"}),
        ]
        grouped = _group_positions_for_tree(positions)
        self.assertEqual(list(grouped["BTC"].keys()), ["260327", "260529", "260626", "__DIRECT__"])

    def test_group_positions_for_tree_sorts_option_strikes_and_puts_futures_last(self) -> None:
        positions = [
            _make_position(inst_id="BTC-USD-260626-100000-C", upl="0", margin_ccy="BTC"),
            _make_position(inst_id="BTC-USD-260626-80000-P", upl="0", margin_ccy="BTC"),
            _make_position(inst_id="BTC-USD-260626-80000-C", upl="0", margin_ccy="BTC"),
            _make_position(inst_id="BTC-USD-260626-50000-P", upl="0", margin_ccy="BTC"),
            OkxPosition(**{**_make_position(inst_id="BTC-USD-260626", upl="0", margin_ccy="BTC").__dict__, "inst_type": "FUTURES"}),
        ]

        grouped = _group_positions_for_tree(positions)
        bucket_items = grouped["BTC"]["260626"]

        self.assertEqual(
            [item.inst_id for item in bucket_items],
            [
                "BTC-USD-260626-50000-P",
                "BTC-USD-260626-80000-C",
                "BTC-USD-260626-80000-P",
                "BTC-USD-260626-100000-C",
                "BTC-USD-260626",
            ],
        )

    def test_option_search_shortcuts_returns_contract_and_expiry_prefix(self) -> None:
        contract, expiry_prefix = _option_search_shortcuts("BTC-USD-260327-55000-C")
        self.assertEqual(contract, "BTC-USD-260327-55000-C")
        self.assertEqual(expiry_prefix, "BTC-USD-260327-")

    def test_option_search_shortcuts_ignores_non_option_inst_id(self) -> None:
        contract, expiry_prefix = _option_search_shortcuts("BTC-USDT-SWAP")
        self.assertEqual(contract, "")
        self.assertEqual(expiry_prefix, "")
