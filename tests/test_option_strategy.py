from datetime import datetime
from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle, Instrument
from okx_quant.okx_client import OkxPosition
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
from okx_quant.option_strategy_ui import _filter_option_positions, _position_side_and_quantity


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
    def test_filter_option_positions_supports_family_and_expiry(self) -> None:
        positions = [
            OkxPosition(
                inst_id="BTC-USD-260410-68000-C",
                inst_type="OPTION",
                pos_side="net",
                mgn_mode="cross",
                position=Decimal("2"),
                avail_position=None,
                avg_price=Decimal("0.01"),
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
            ),
            OkxPosition(
                inst_id="BTC-USD-260417-68000-P",
                inst_type="OPTION",
                pos_side="short",
                mgn_mode="cross",
                position=Decimal("-3"),
                avail_position=None,
                avg_price=Decimal("0.02"),
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
            ),
            OkxPosition(
                inst_id="ETH-USD-260410-3000-C",
                inst_type="OPTION",
                pos_side="long",
                mgn_mode="cross",
                position=Decimal("1"),
                avail_position=None,
                avg_price=Decimal("0.03"),
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
            ),
        ]

        family_rows = _filter_option_positions(positions, family="BTC-USD")
        expiry_rows = _filter_option_positions(positions, family="BTC-USD", expiry_code="260410")

        self.assertEqual([item.inst_id for item in family_rows], ["BTC-USD-260410-68000-C", "BTC-USD-260417-68000-P"])
        self.assertEqual([item.inst_id for item in expiry_rows], ["BTC-USD-260410-68000-C"])

    def test_position_side_and_quantity_prefers_pos_side(self) -> None:
        short_position = OkxPosition(
            inst_id="BTC-USD-260410-68000-P",
            inst_type="OPTION",
            pos_side="short",
            mgn_mode="cross",
            position=Decimal("5"),
            avail_position=None,
            avg_price=Decimal("0.02"),
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
        net_short = OkxPosition(
            inst_id="BTC-USD-260410-68000-C",
            inst_type="OPTION",
            pos_side="net",
            mgn_mode="cross",
            position=Decimal("-2"),
            avail_position=None,
            avg_price=Decimal("0.01"),
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

        self.assertEqual(_position_side_and_quantity(short_position), ("sell", Decimal("5")))
        self.assertEqual(_position_side_and_quantity(net_short), ("sell", Decimal("2")))

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
