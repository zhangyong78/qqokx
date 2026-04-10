from datetime import datetime
from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Instrument
from okx_quant.okx_client import OkxPosition
from okx_quant.option_roll import (
    _directional_strike_steps,
    _preferred_strike_bonus,
    build_option_roll_suggestions,
    build_option_roll_transfer_payload,
)
from okx_quant.option_strategy import OptionQuote


def _make_instrument(inst_id: str) -> Instrument:
    return Instrument(
        inst_id=inst_id,
        inst_type="OPTION",
        tick_size=Decimal("0.0001"),
        lot_size=Decimal("1"),
        min_size=Decimal("1"),
        state="live",
        settle_ccy="BTC",
        ct_val=Decimal("1"),
        ct_mult=Decimal("0.1"),
        ct_val_ccy="BTC",
        uly="BTC-USD",
        inst_family="BTC-USD",
    )


def _make_position(inst_id: str) -> OkxPosition:
    return OkxPosition(
        inst_id=inst_id,
        inst_type="OPTION",
        pos_side="short",
        mgn_mode="cross",
        position=Decimal("-20"),
        avail_position=Decimal("20"),
        avg_price=Decimal("0.0105"),
        mark_price=Decimal("0.0426"),
        unrealized_pnl=Decimal("-0.006"),
        unrealized_pnl_ratio=Decimal("-0.3"),
        liquidation_price=None,
        leverage=None,
        margin_ccy="BTC",
        last_price=Decimal("0.0425"),
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


class OptionRollTest(TestCase):
    def test_directional_strike_steps_for_call_and_put(self) -> None:
        levels = [Decimal("68000"), Decimal("69000"), Decimal("70000"), Decimal("71000")]
        self.assertEqual(
            _directional_strike_steps(
                current_strike=Decimal("69000"),
                candidate_strike=Decimal("70000"),
                option_type="C",
                levels=levels,
            ),
            1,
        )
        self.assertEqual(
            _directional_strike_steps(
                current_strike=Decimal("69000"),
                candidate_strike=Decimal("68000"),
                option_type="P",
                levels=levels,
            ),
            1,
        )

    def test_preferred_strike_bonus_only_rewards_safer_nearby_levels(self) -> None:
        strike_levels = {("260417", "C"): [Decimal("69000"), Decimal("70000"), Decimal("71000"), Decimal("73000")]}
        current = type("Parsed", (), {"strike": Decimal("69000"), "option_type": "C", "expiry_code": "260410"})()
        near_candidate = type("Parsed", (), {"strike": Decimal("70000"), "option_type": "C", "expiry_code": "260417"})()
        far_candidate = type("Parsed", (), {"strike": Decimal("73000"), "option_type": "C", "expiry_code": "260417"})()
        self.assertGreater(
            _preferred_strike_bonus(
                parsed_current=current,
                parsed_candidate=near_candidate,
                strike_levels_by_expiry=strike_levels,
                preferred_strike_levels=2,
            ),
            Decimal("0"),
        )
        self.assertEqual(
            _preferred_strike_bonus(
                parsed_current=current,
                parsed_candidate=far_candidate,
                strike_levels_by_expiry=strike_levels,
                preferred_strike_levels=2,
            ),
            Decimal("0"),
        )

    def test_build_option_roll_suggestions_filters_to_later_expiry(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        same_expiry = _make_instrument("BTC-USD-260410-70000-C")
        later_expiry = _make_instrument("BTC-USD-260417-70000-C")
        earlier_expiry = _make_instrument("BTC-USD-260403-70000-C")
        quote_map = {
            same_expiry.inst_id: OptionQuote(same_expiry, bid_price=Decimal("0.0300"), ask_price=Decimal("0.0310"), mark_price=Decimal("0.0305"), last_price=Decimal("0.0304"), index_price=Decimal("70000")),
            later_expiry.inst_id: OptionQuote(later_expiry, bid_price=Decimal("0.0390"), ask_price=Decimal("0.0405"), mark_price=Decimal("0.0398"), last_price=Decimal("0.0400"), index_price=Decimal("70000")),
            earlier_expiry.inst_id: OptionQuote(earlier_expiry, bid_price=Decimal("0.0490"), ask_price=Decimal("0.0500"), mark_price=Decimal("0.0495"), last_price=Decimal("0.0494"), index_price=Decimal("70000")),
        }

        suggestions = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[same_expiry, later_expiry, earlier_expiry],
            candidate_quotes_by_inst_id=quote_map,
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="credit",
            strike_scope="safer_preferred",
            preferred_strike_levels=None,
            max_results=10,
        )

        self.assertEqual([item.new_inst_id for item in suggestions], ["BTC-USD-260417-70000-C"])

    def test_build_option_roll_suggestions_prefers_nearer_expiry_when_requested(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        near_expiry = _make_instrument("BTC-USD-260417-70000-C")
        far_expiry = _make_instrument("BTC-USD-260424-70000-C")
        quote_map = {
            near_expiry.inst_id: OptionQuote(
                near_expiry,
                bid_price=Decimal("0.0390"),
                ask_price=Decimal("0.0405"),
                mark_price=Decimal("0.0398"),
                last_price=Decimal("0.0400"),
                index_price=Decimal("70000"),
            ),
            far_expiry.inst_id: OptionQuote(
                far_expiry,
                bid_price=Decimal("0.0390"),
                ask_price=Decimal("0.0405"),
                mark_price=Decimal("0.0398"),
                last_price=Decimal("0.0400"),
                index_price=Decimal("70000"),
            ),
        }

        suggestions = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[far_expiry, near_expiry],
            candidate_quotes_by_inst_id=quote_map,
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="near_expiry",
            strike_scope="safer_preferred",
            preferred_strike_levels=None,
            max_results=10,
        )

        self.assertEqual(suggestions[0].new_inst_id, near_expiry.inst_id)

    def test_build_option_roll_suggestions_same_and_safer_scope_for_short_call(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        same_strike = _make_instrument("BTC-USD-260417-69000-C")
        higher_strike = _make_instrument("BTC-USD-260417-70000-C")
        lower_strike = _make_instrument("BTC-USD-260417-68000-C")
        quote_map = {
            same_strike.inst_id: OptionQuote(same_strike, bid_price=Decimal("0.0460"), ask_price=Decimal("0.0495"), mark_price=Decimal("0.0470"), last_price=Decimal("0.0475"), index_price=Decimal("70000")),
            higher_strike.inst_id: OptionQuote(higher_strike, bid_price=Decimal("0.0390"), ask_price=Decimal("0.0405"), mark_price=Decimal("0.0398"), last_price=Decimal("0.0400"), index_price=Decimal("70000")),
            lower_strike.inst_id: OptionQuote(lower_strike, bid_price=Decimal("0.0530"), ask_price=Decimal("0.0545"), mark_price=Decimal("0.0538"), last_price=Decimal("0.0540"), index_price=Decimal("70000")),
        }

        suggestions = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[same_strike, higher_strike, lower_strike],
            candidate_quotes_by_inst_id=quote_map,
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="near_expiry",
            strike_scope="same_and_safer",
            preferred_strike_levels=3,
            max_results=10,
        )

        suggested_ids = [item.new_inst_id for item in suggestions]
        self.assertIn(same_strike.inst_id, suggested_ids)
        self.assertIn(higher_strike.inst_id, suggested_ids)
        self.assertNotIn(lower_strike.inst_id, suggested_ids)

    def test_build_option_roll_suggestions_sets_price_gap_from_candidate_bid_minus_current_mark(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        candidate_instrument = _make_instrument("BTC-USD-260417-70000-C")
        candidate_quote = OptionQuote(
            instrument=candidate_instrument,
            mark_price=Decimal("0.0398"),
            bid_price=Decimal("0.0390"),
            ask_price=Decimal("0.0405"),
            last_price=Decimal("0.0400"),
            index_price=Decimal("70000"),
        )

        suggestions = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[candidate_instrument],
            candidate_quotes_by_inst_id={candidate_instrument.inst_id: candidate_quote},
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="credit",
            strike_scope="all",
            preferred_strike_levels=None,
            max_results=10,
        )

        self.assertEqual(suggestions[0].price_gap, Decimal("-0.0036"))

    def test_build_option_roll_suggestions_excludes_candidates_without_bid_price(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        candidate_instrument = _make_instrument("BTC-USD-260417-70000-C")
        candidate_quote = OptionQuote(
            instrument=candidate_instrument,
            mark_price=Decimal("0.0398"),
            bid_price=None,
            ask_price=Decimal("0.0405"),
            last_price=Decimal("0.0400"),
            index_price=Decimal("70000"),
        )

        suggestions = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[candidate_instrument],
            candidate_quotes_by_inst_id={candidate_instrument.inst_id: candidate_quote},
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="credit",
            strike_scope="all",
            preferred_strike_levels=None,
            max_results=10,
        )

        self.assertEqual(suggestions, [])

    def test_build_transfer_payload_creates_buy_close_and_sell_open_legs(self) -> None:
        current_position = _make_position("BTC-USD-260410-69000-C")
        current_instrument = _make_instrument("BTC-USD-260410-69000-C")
        current_quote = OptionQuote(
            instrument=current_instrument,
            mark_price=Decimal("0.0426"),
            bid_price=Decimal("0.0415"),
            ask_price=Decimal("0.0440"),
            last_price=Decimal("0.0425"),
            index_price=Decimal("70000"),
        )
        candidate_instrument = _make_instrument("BTC-USD-260417-70000-C")
        candidate_quote = OptionQuote(
            instrument=candidate_instrument,
            mark_price=Decimal("0.0398"),
            bid_price=Decimal("0.0390"),
            ask_price=Decimal("0.0405"),
            last_price=Decimal("0.0400"),
            index_price=Decimal("70000"),
        )
        suggestion = build_option_roll_suggestions(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            candidate_instruments=[candidate_instrument],
            candidate_quotes_by_inst_id={candidate_instrument.inst_id: candidate_quote},
            settlement_price=Decimal("70000"),
            valuation_time=datetime(2026, 4, 10, 8, 0, 0),
            preference="credit",
            strike_scope="all",
            preferred_strike_levels=None,
            max_results=10,
        )[0]

        payload = build_option_roll_transfer_payload(
            current_position=current_position,
            current_instrument=current_instrument,
            current_quote=current_quote,
            suggestion=suggestion,
            candidate_instrument=candidate_instrument,
            candidate_quote=candidate_quote,
        )

        self.assertEqual(payload.option_family, "BTC-USD")
        self.assertEqual(payload.expiry_code, "260417")
        self.assertEqual(len(payload.legs), 2)
        self.assertEqual(payload.legs[0].inst_id, "BTC-USD-260410-69000-C")
        self.assertEqual(payload.legs[0].side, "buy")
        self.assertEqual(payload.legs[1].inst_id, "BTC-USD-260417-70000-C")
        self.assertEqual(payload.legs[1].side, "sell")
