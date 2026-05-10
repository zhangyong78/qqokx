from datetime import datetime
from decimal import Decimal
from unittest import TestCase

from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle, Instrument
from okx_quant.okx_client import OkxTicker
from okx_quant.option_strategy_ui import (
    OptionStrategyCalculatorWindow,
    _align_overlay_candles,
    _align_overlay_three_series,
    _filter_option_instruments_by_family,
    _filter_option_tickers_by_family,
    _aggregate_deribit_option_chart_candles,
    _annualization_factor_for_bar,
    _build_deribit_option_chart_candles,
    _build_volatility_candles_from_reference,
    _normalized_kline_view,
    _pan_kline_view,
    _zoom_kline_view,
)
from okx_quant.option_strategy import (
    OptionQuote,
    StrategyLegDefinition,
    StrategyPayoffPoint,
    StrategyPayoffSnapshot,
    build_composite_candles,
    build_default_formula,
    build_option_chain_rows,
    build_payoff_snapshot,
    build_simulated_payoff_snapshot,
    convert_candles_by_reference,
    convert_payoff_snapshot_to_usdt,
    evaluate_linear_formula,
    estimate_strategy_greeks,
    infer_implied_volatility_for_leg,
    infer_inverse_implied_volatility,
    inverse_black_scholes_price,
    option_intrinsic_value_at_expiry,
    option_contract_value,
    option_time_to_expiry_years,
    parse_linear_formula,
    parse_option_contract,
    parse_option_expiry_datetime,
    resolve_strategy_leg,
    shift_candles,
    simulated_option_value,
)


def _make_instrument(inst_id: str) -> Instrument:
    return Instrument(
        inst_id=inst_id,
        inst_type="OPTION",
        tick_size=Decimal("0.0001"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
        ct_val=Decimal("1"),
        ct_mult=Decimal("0.1"),
        ct_val_ccy="BTC",
        inst_family="BTC-USD",
    )


class OptionStrategyTest(TestCase):
    def test_refresh_leg_quotes_keeps_holding_price_and_updates_mark(self) -> None:
        window = OptionStrategyCalculatorWindow.__new__(OptionStrategyCalculatorWindow)
        leg = StrategyLegDefinition(
            alias="L1",
            inst_id="BTC-USD-260626-90000-C",
            side="buy",
            quantity=Decimal("2"),
            premium=Decimal("0.0123"),
        )
        instrument = _make_instrument("BTC-USD-260626-90000-C")
        quote = OptionQuote(
            instrument=instrument,
            mark_price=Decimal("0.0345"),
            bid_price=Decimal("0.0340"),
            ask_price=Decimal("0.0350"),
            index_price=Decimal("65000"),
        )

        class _Status:
            def __init__(self) -> None:
                self.value = ""

            def set(self, value: str) -> None:
                self.value = value

        window._instrument_map = {}
        window._quotes_by_inst_id = {}
        window._legs = [leg]
        window._current_underlying_price = None
        window._render_legs = lambda: None
        window._refresh_strategy_summary = lambda: None
        window.status_text = _Status()

        window._apply_refreshed_leg_quotes([(leg.inst_id, instrument, quote)])

        self.assertEqual(window._legs[0].premium, Decimal("0.0123"))
        self.assertEqual(window._leg_mark_price(leg.inst_id), Decimal("0.0345"))
        self.assertEqual(window._current_underlying_price, Decimal("65000"))
        self.assertEqual(window.status_text.value, "策略腿报价已刷新。")

    def test_refresh_leg_greeks_populates_values_when_quote_and_underlying_exist(self) -> None:
        window = OptionStrategyCalculatorWindow.__new__(OptionStrategyCalculatorWindow)
        leg = StrategyLegDefinition(
            alias="L1",
            inst_id="BTC-USD-260626-90000-C",
            side="buy",
            quantity=Decimal("2"),
            premium=Decimal("0.0123"),
        )
        instrument = _make_instrument("BTC-USD-260626-90000-C")
        quote = OptionQuote(
            instrument=instrument,
            mark_price=Decimal("0.0345"),
            bid_price=Decimal("0.0340"),
            ask_price=Decimal("0.0350"),
        )
        window._legs = [leg]
        window._instrument_map = {leg.inst_id: instrument}
        window._quotes_by_inst_id = {leg.inst_id: quote}
        window._current_underlying_price = Decimal("65000")

        window._refresh_leg_greeks()

        self.assertIsNotNone(window._legs[0].delta)
        self.assertIsNotNone(window._legs[0].gamma)
        self.assertIsNotNone(window._legs[0].theta)
        self.assertIsNotNone(window._legs[0].vega)

    def test_build_volatility_candles_from_reference_generates_kline_series(self) -> None:
        candles: list[Candle] = []
        base_price = Decimal("60000")
        for index in range(25):
            close = base_price + Decimal(index * 100)
            candles.append(
                Candle(
                    ts=1_700_000_000_000 + (index * 3_600_000),
                    open=close - Decimal("50"),
                    high=close + Decimal("80"),
                    low=close - Decimal("120"),
                    close=close,
                    volume=Decimal("0"),
                    confirmed=True,
                )
            )

        volatility_candles = _build_volatility_candles_from_reference(candles, bar="1H", lookback=20)

        self.assertEqual(len(volatility_candles), 5)
        self.assertTrue(all(item.close >= 0 for item in volatility_candles))
        self.assertEqual(volatility_candles[-1].ts, candles[-1].ts)

    def test_annualization_factor_for_4h_is_positive(self) -> None:
        factor = _annualization_factor_for_bar("4H")
        self.assertGreater(factor, 0.0)

    def test_build_deribit_option_chart_candles_uses_hourly_for_subhour_bars(self) -> None:
        hourly = [
            DeribitVolatilityCandle(
                ts=1_700_000_000_000,
                open=Decimal("40.0"),
                high=Decimal("41.0"),
                low=Decimal("39.0"),
                close=Decimal("40.5"),
            ),
            DeribitVolatilityCandle(
                ts=1_700_003_600_000,
                open=Decimal("40.5"),
                high=Decimal("42.0"),
                low=Decimal("40.0"),
                close=Decimal("41.2"),
            ),
        ]

        candles, resolution_label, resolution_note = _build_deribit_option_chart_candles(
            hourly,
            bar="15m",
            requested_limit=100,
        )

        self.assertEqual(len(candles), 2)
        self.assertEqual(resolution_label, "1小时")
        self.assertIn("最小周期为1小时", resolution_note)
        self.assertEqual(candles[-1].close, Decimal("41.2"))

    def test_aggregate_deribit_option_chart_candles_builds_4h_bar(self) -> None:
        hourly = [
            DeribitVolatilityCandle(
                ts=0,
                open=Decimal("40.0"),
                high=Decimal("41.0"),
                low=Decimal("39.0"),
                close=Decimal("40.5"),
            ),
            DeribitVolatilityCandle(
                ts=3_600_000,
                open=Decimal("40.5"),
                high=Decimal("42.0"),
                low=Decimal("40.0"),
                close=Decimal("41.2"),
            ),
            DeribitVolatilityCandle(
                ts=7_200_000,
                open=Decimal("41.2"),
                high=Decimal("43.0"),
                low=Decimal("41.0"),
                close=Decimal("42.0"),
            ),
            DeribitVolatilityCandle(
                ts=10_800_000,
                open=Decimal("42.0"),
                high=Decimal("44.0"),
                low=Decimal("41.5"),
                close=Decimal("43.1"),
            ),
        ]

        aggregated = _aggregate_deribit_option_chart_candles(hourly, 14_400_000)

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0].open, Decimal("40.0"))
        self.assertEqual(aggregated[0].close, Decimal("43.1"))
        self.assertEqual(aggregated[0].high, Decimal("44.0"))
        self.assertEqual(aggregated[0].low, Decimal("39.0"))

    def test_align_overlay_candles_uses_common_timestamps(self) -> None:
        combo_candles = [
            Candle(ts=1000, open=Decimal("1"), high=Decimal("2"), low=Decimal("0.5"), close=Decimal("1.5"), volume=Decimal("0"), confirmed=True),
            Candle(ts=2000, open=Decimal("1.5"), high=Decimal("2.2"), low=Decimal("1.2"), close=Decimal("2.0"), volume=Decimal("0"), confirmed=True),
            Candle(ts=3000, open=Decimal("2.0"), high=Decimal("2.4"), low=Decimal("1.8"), close=Decimal("2.1"), volume=Decimal("0"), confirmed=True),
        ]
        volatility_candles = [
            Candle(ts=2000, open=Decimal("45"), high=Decimal("46"), low=Decimal("44"), close=Decimal("45.5"), volume=Decimal("0"), confirmed=True),
            Candle(ts=3000, open=Decimal("45.5"), high=Decimal("47"), low=Decimal("45"), close=Decimal("46.2"), volume=Decimal("0"), confirmed=True),
            Candle(ts=4000, open=Decimal("46.2"), high=Decimal("48"), low=Decimal("46"), close=Decimal("47.8"), volume=Decimal("0"), confirmed=True),
        ]

        aligned = _align_overlay_candles(combo_candles, volatility_candles)

        self.assertEqual(len(aligned), 2)
        self.assertEqual([item[0].ts for item in aligned], [2000, 3000])
        self.assertEqual([item[1].ts for item in aligned], [2000, 3000])

    def test_align_overlay_three_series_requires_all_timestamps(self) -> None:
        combo_candles = [
            Candle(ts=1000, open=Decimal("1"), high=Decimal("2"), low=Decimal("0.5"), close=Decimal("1.5"), volume=Decimal("0"), confirmed=True),
            Candle(ts=2000, open=Decimal("1.5"), high=Decimal("2.2"), low=Decimal("1.2"), close=Decimal("2.0"), volume=Decimal("0"), confirmed=True),
        ]
        volatility_candles = [
            Candle(ts=2000, open=Decimal("45"), high=Decimal("46"), low=Decimal("44"), close=Decimal("45.5"), volume=Decimal("0"), confirmed=True),
        ]
        spot_candles = [
            Candle(ts=2000, open=Decimal("60000"), high=Decimal("61000"), low=Decimal("59000"), close=Decimal("60500"), volume=Decimal("0"), confirmed=True),
        ]
        aligned = _align_overlay_three_series(combo_candles, volatility_candles, spot_candles)
        self.assertEqual(len(aligned), 1)
        self.assertEqual(aligned[0][0].ts, 2000)
        self.assertEqual(aligned[0][1].ts, 2000)
        self.assertEqual(aligned[0][2].ts, 2000)

    def test_build_deribit_option_chart_candles_daily_aggregate(self) -> None:
        hourly = []
        base_ts = 86_400_000
        for hour in range(26):
            hourly.append(
                DeribitVolatilityCandle(
                    ts=base_ts + hour * 3_600_000,
                    open=Decimal("40") + Decimal(hour),
                    high=Decimal("41") + Decimal(hour),
                    low=Decimal("39") + Decimal(hour),
                    close=Decimal("40.5") + Decimal(hour),
                )
            )
        candles, resolution_label, resolution_note = _build_deribit_option_chart_candles(
            hourly,
            bar="1D",
            requested_limit=100,
        )
        self.assertEqual(resolution_label, "日线")
        self.assertIn("小时", resolution_note)
        self.assertEqual(len(candles), 2)

    def test_normalized_kline_view_returns_full_when_auto_full(self) -> None:
        start, visible = _normalized_kline_view(200, 20, 50, auto_full=True)
        self.assertEqual((start, visible), (0, 200))

    def test_zoom_kline_view_keeps_focus_inside_bounds(self) -> None:
        start, visible = _zoom_kline_view(2000, 0, 2000, focus_ratio=0.5, zoom_in=True)
        self.assertLess(visible, 2000)
        self.assertGreaterEqual(start, 0)
        self.assertLessEqual(start + visible, 2000)

    def test_pan_kline_view_clamps_to_available_range(self) -> None:
        start, visible = _pan_kline_view(500, 100, 120, delta_items=600)
        self.assertEqual(visible, 120)
        self.assertEqual(start, 380)

    def test_parse_option_contract_extracts_fields(self) -> None:
        parsed = parse_option_contract("BTC-USD-260626-90000-C")
        self.assertEqual(parsed.inst_family, "BTC-USD")
        self.assertEqual(parsed.expiry_code, "260626")
        self.assertEqual(parsed.strike, Decimal("90000"))
        self.assertEqual(parsed.option_type, "C")
        self.assertEqual(parsed.expiry_label, "2026-06-26")

    def test_build_option_chain_rows_groups_call_and_put(self) -> None:
        call = OptionQuote(
            instrument=_make_instrument("BTC-USD-260626-90000-C"),
            mark_price=Decimal("0.015"),
        )
        put = OptionQuote(
            instrument=_make_instrument("BTC-USD-260626-90000-P"),
            mark_price=Decimal("0.012"),
        )
        rows = build_option_chain_rows([put, call])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].call_quote.instrument.inst_id, "BTC-USD-260626-90000-C")
        self.assertEqual(rows[0].put_quote.instrument.inst_id, "BTC-USD-260626-90000-P")

    def test_build_default_formula_uses_side_and_quantity(self) -> None:
        formula = build_default_formula(
            [
                StrategyLegDefinition(alias="L1", inst_id="BTC-USD-260626-90000-C", side="buy", quantity=Decimal("1")),
                StrategyLegDefinition(alias="L2", inst_id="BTC-USD-260626-95000-C", side="sell", quantity=Decimal("2")),
            ]
        )
        self.assertEqual(formula, "L1 - 2*L2")

    def test_parse_linear_formula_and_evaluate(self) -> None:
        formula = parse_linear_formula("L1 - 2*L2 + 0.5", allowed_names={"L1", "L2"})
        self.assertEqual(formula.coefficients["L1"], Decimal("1"))
        self.assertEqual(formula.coefficients["L2"], Decimal("-2"))
        self.assertEqual(formula.constant, Decimal("0.5"))
        value = evaluate_linear_formula(formula, {"L1": Decimal("3"), "L2": Decimal("1.25")})
        self.assertEqual(value, Decimal("1.0"))

    def test_parse_linear_formula_rejects_non_linear_expression(self) -> None:
        with self.assertRaises(ValueError):
            parse_linear_formula("L1 * L2", allowed_names={"L1", "L2"})

    def test_build_composite_candles_respects_negative_coefficients(self) -> None:
        candles = build_composite_candles(
            "L1 - L2",
            {
                "L1": [
                    Candle(
                        ts=1,
                        open=Decimal("10"),
                        high=Decimal("12"),
                        low=Decimal("9"),
                        close=Decimal("11"),
                        volume=Decimal("0"),
                        confirmed=True,
                    )
                ],
                "L2": [
                    Candle(
                        ts=1,
                        open=Decimal("4"),
                        high=Decimal("6"),
                        low=Decimal("3"),
                        close=Decimal("5"),
                        volume=Decimal("0"),
                        confirmed=True,
                    )
                ],
            },
            allowed_names={"L1", "L2"},
        )
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].open, Decimal("6"))
        self.assertEqual(candles[0].close, Decimal("6"))
        self.assertEqual(candles[0].high, Decimal("9"))
        self.assertEqual(candles[0].low, Decimal("3"))

    def test_option_contract_value_uses_ct_val_and_ct_mult(self) -> None:
        value = option_contract_value(_make_instrument("BTC-USD-260626-90000-C"))
        self.assertEqual(value, Decimal("0.1"))

    def test_option_intrinsic_value_at_expiry_uses_coin_settled_formula(self) -> None:
        intrinsic = option_intrinsic_value_at_expiry(
            settlement_price=Decimal("120000"),
            strike=Decimal("100000"),
            option_type="C",
            contract_value=Decimal("0.1"),
        )
        self.assertEqual(intrinsic, Decimal("0.01666666666666666666666666667"))

    def test_build_payoff_snapshot_tracks_net_premium(self) -> None:
        long_call = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=Decimal("0.10"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )
        short_call = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L2",
                inst_id="BTC-USD-260626-120000-C",
                side="sell",
                quantity=Decimal("1"),
                premium=Decimal("0.03"),
            ),
            _make_instrument("BTC-USD-260626-120000-C"),
        )
        snapshot = build_payoff_snapshot(
            [long_call, short_call],
            current_underlying_price=Decimal("110000"),
            sample_count=61,
        )
        self.assertEqual(snapshot.net_premium, Decimal("-0.007"))
        self.assertGreater(len(snapshot.points), 10)
        self.assertTrue(snapshot.price_lower < snapshot.price_upper)

    def test_convert_payoff_snapshot_to_usdt_uses_underlying_price_per_point(self) -> None:
        snapshot = StrategyPayoffSnapshot(
            points=(
                StrategyPayoffPoint(underlying_price=Decimal("50000"), pnl=Decimal("0.01")),
                StrategyPayoffPoint(underlying_price=Decimal("60000"), pnl=Decimal("0.02")),
            ),
            break_even_prices=(Decimal("55000"),),
            net_premium=Decimal("-0.005"),
            price_lower=Decimal("50000"),
            price_upper=Decimal("60000"),
            current_underlying_price=Decimal("58000"),
        )

        converted = convert_payoff_snapshot_to_usdt(snapshot)

        self.assertEqual(converted.points[0].pnl, Decimal("500.00"))
        self.assertEqual(converted.points[1].pnl, Decimal("1200.00"))
        self.assertEqual(converted.net_premium, Decimal("-290.000"))
        self.assertEqual(converted.break_even_prices, snapshot.break_even_prices)

    def test_convert_candles_by_reference_multiplies_ohlc_by_aligned_factor(self) -> None:
        candles = [
            Candle(
                ts=1,
                open=Decimal("0.01"),
                high=Decimal("0.02"),
                low=Decimal("0.008"),
                close=Decimal("0.015"),
                volume=Decimal("0"),
                confirmed=True,
            ),
            Candle(
                ts=2,
                open=Decimal("0.02"),
                high=Decimal("0.03"),
                low=Decimal("0.01"),
                close=Decimal("0.025"),
                volume=Decimal("0"),
                confirmed=True,
            ),
        ]
        reference = [
            Candle(
                ts=1,
                open=Decimal("50000"),
                high=Decimal("51000"),
                low=Decimal("49000"),
                close=Decimal("50500"),
                volume=Decimal("0"),
                confirmed=True,
            )
        ]

        converted = convert_candles_by_reference(candles, reference)

        self.assertEqual(len(converted), 1)
        self.assertEqual(converted[0].open, Decimal("500.00"))
        self.assertEqual(converted[0].high, Decimal("1020.00"))
        self.assertEqual(converted[0].low, Decimal("392.000"))
        self.assertEqual(converted[0].close, Decimal("757.500"))

    def test_shift_candles_offsets_each_ohlc_value(self) -> None:
        candles = [
            Candle(
                ts=1,
                open=Decimal("0.01"),
                high=Decimal("0.015"),
                low=Decimal("0.008"),
                close=Decimal("0.012"),
                volume=Decimal("0"),
                confirmed=True,
            )
        ]

        shifted = shift_candles(candles, offset=Decimal("-0.005"))

        self.assertEqual(shifted[0].open, Decimal("0.005"))
        self.assertEqual(shifted[0].high, Decimal("0.010"))
        self.assertEqual(shifted[0].low, Decimal("0.003"))
        self.assertEqual(shifted[0].close, Decimal("0.007"))

    def test_inverse_black_scholes_price_can_round_trip_implied_volatility(self) -> None:
        option_value = inverse_black_scholes_price(
            settlement_price=Decimal("100000"),
            strike=Decimal("100000"),
            option_type="C",
            contract_value=Decimal("0.1"),
            time_to_expiry_years=Decimal("0.5"),
            volatility=Decimal("0.6"),
        )

        implied = infer_inverse_implied_volatility(
            option_value=option_value,
            settlement_price=Decimal("100000"),
            strike=Decimal("100000"),
            option_type="C",
            contract_value=Decimal("0.1"),
            time_to_expiry_years=Decimal("0.5"),
        )

        self.assertIsNotNone(implied)
        self.assertAlmostEqual(float(implied), 0.6, places=4)

    def test_parse_option_expiry_datetime_uses_next_day_midnight_boundary(self) -> None:
        expiry = parse_option_expiry_datetime("260626")
        self.assertEqual(expiry, datetime(2026, 6, 27, 0, 0, 0))

    def test_option_time_to_expiry_years_returns_zero_after_expiry(self) -> None:
        value = option_time_to_expiry_years("260101", valuation_time=datetime(2026, 1, 2, 0, 0, 0))
        self.assertEqual(value, Decimal("0"))

    def test_simulated_option_value_matches_intrinsic_after_expiry(self) -> None:
        leg = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=Decimal("0.05"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )

        value = simulated_option_value(
            settlement_price=Decimal("120000"),
            leg=leg,
            valuation_time=datetime(2026, 6, 28, 0, 0, 0),
            base_implied_volatility=Decimal("0.7"),
        )

        self.assertEqual(
            value,
            option_intrinsic_value_at_expiry(
                settlement_price=Decimal("120000"),
                strike=Decimal("100000"),
                option_type="C",
                contract_value=Decimal("0.1"),
            ),
        )

    def test_infer_implied_volatility_for_leg_returns_positive_value(self) -> None:
        target_volatility = Decimal("0.72")
        premium_value = inverse_black_scholes_price(
            settlement_price=Decimal("100000"),
            strike=Decimal("100000"),
            option_type="C",
            contract_value=Decimal("0.1"),
            time_to_expiry_years=option_time_to_expiry_years("260626", valuation_time=datetime(2026, 1, 1, 0, 0, 0)),
            volatility=target_volatility,
        )
        leg = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=premium_value / Decimal("0.1"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )

        implied = infer_implied_volatility_for_leg(
            leg,
            settlement_price=Decimal("100000"),
            valuation_time=datetime(2026, 1, 1, 0, 0, 0),
        )

        self.assertIsNotNone(implied)
        self.assertAlmostEqual(float(implied), float(target_volatility), places=4)

    def test_infer_implied_volatility_for_leg_can_use_market_price_separate_from_entry_cost(self) -> None:
        target_volatility = Decimal("0.55")
        market_value = inverse_black_scholes_price(
            settlement_price=Decimal("100000"),
            strike=Decimal("100000"),
            option_type="C",
            contract_value=Decimal("0.1"),
            time_to_expiry_years=option_time_to_expiry_years("260626", valuation_time=datetime(2026, 1, 1, 0, 0, 0)),
            volatility=target_volatility,
        )
        leg = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=Decimal("0.01"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )

        implied = infer_implied_volatility_for_leg(
            leg,
            settlement_price=Decimal("100000"),
            valuation_time=datetime(2026, 1, 1, 0, 0, 0),
            option_price=market_value / Decimal("0.1"),
        )

        self.assertIsNotNone(implied)
        self.assertAlmostEqual(float(implied), float(target_volatility), places=4)

    def test_build_simulated_payoff_snapshot_reduces_to_expiry_value_when_time_is_over(self) -> None:
        leg = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=Decimal("0.05"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )

        snapshot = build_simulated_payoff_snapshot(
            [leg],
            implied_volatility_by_alias={"L1": Decimal("0.8")},
            valuation_time=datetime(2026, 6, 28, 0, 0, 0),
            current_underlying_price=Decimal("100000"),
            sample_count=11,
        )

        target = next(point for point in snapshot.points if point.underlying_price == Decimal("100000"))
        self.assertEqual(target.pnl, Decimal("-0.005"))

    def test_estimate_strategy_greeks_returns_expected_keys(self) -> None:
        leg = resolve_strategy_leg(
            StrategyLegDefinition(
                alias="L1",
                inst_id="BTC-USD-260626-100000-C",
                side="buy",
                quantity=Decimal("1"),
                premium=Decimal("0.05"),
            ),
            _make_instrument("BTC-USD-260626-100000-C"),
        )

        greeks = estimate_strategy_greeks(
            [leg],
            implied_volatility_by_alias={"L1": Decimal("0.7")},
            valuation_time=datetime(2026, 1, 1, 0, 0, 0),
            settlement_price=Decimal("100000"),
        )

        self.assertIn("delta", greeks)
        self.assertIn("gamma", greeks)
        self.assertIn("theta", greeks)
        self.assertIn("vega", greeks)
        self.assertGreater(greeks["delta"], Decimal("0"))

    def test_filter_option_instruments_by_family_drops_usdt_when_usd_selected(self) -> None:
        coin = _make_instrument("BTC-USD-260626-90000-C")
        usdt_m = Instrument(
            inst_id="BTC-USDT-260626-90000-C",
            inst_type="OPTION",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            ct_val=Decimal("1"),
            ct_mult=Decimal("0.1"),
            ct_val_ccy="USDT",
            inst_family="BTC-USDT",
        )
        mixed = [coin, usdt_m]
        usd_only = _filter_option_instruments_by_family("BTC-USD", mixed)
        self.assertEqual([i.inst_id for i in usd_only], ["BTC-USD-260626-90000-C"])
        usdt_only = _filter_option_instruments_by_family("BTC-USDT", mixed)
        self.assertEqual([i.inst_id for i in usdt_only], ["BTC-USDT-260626-90000-C"])

    def test_filter_option_tickers_by_family_drops_other_settlement_line(self) -> None:
        tickers = [
            OkxTicker(
                inst_id="BTC-USD-260626-90000-C",
                last=None,
                bid=None,
                ask=None,
                mark=Decimal("0.01"),
                index=None,
                raw={},
            ),
            OkxTicker(
                inst_id="BTC-USDT-260626-90000-C",
                last=None,
                bid=None,
                ask=None,
                mark=Decimal("0.02"),
                index=None,
                raw={},
            ),
        ]
        self.assertEqual(
            [t.inst_id for t in _filter_option_tickers_by_family("BTC-USD", tickers)],
            ["BTC-USD-260626-90000-C"],
        )
