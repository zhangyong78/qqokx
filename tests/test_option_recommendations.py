from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle, Instrument
from okx_quant.option_strategy import OptionQuote
from okx_quant.option_recommendations import (
    DAY_MS, HOUR_MS, PERIOD_MS, RecommendationRules, StrategySuggestion,
    _daily_dvol_closes, _match_contracts, build_option_recommendations, option_days_to_expiry,
    build_recommendation_analysis_payload,
)


NOW = int(datetime(2026, 10, 5, 4, tzinfo=timezone.utc).timestamp() * 1000)


def prices(direction: int = 1) -> dict[str, list[Candle]]:
    result = {}
    for period, step in PERIOD_MS.items():
        offset = -8 * HOUR_MS if period == "1D" else 0
        last = (NOW - offset) // step * step + offset - step
        candles = []
        for index in range(180):
            close = Decimal("100") + Decimal(direction) * Decimal(index) / 10
            candles.append(Candle(last - (179-index)*step, close, close+1, close-1, close,
                                  Decimal("10"), True))
        result[period] = candles
    return result


def volatility(*, high: bool = False, rising: bool = False) -> list[Candle]:
    last = NOW // HOUR_MS * HOUR_MS - HOUR_MS
    total = 24 * 93
    result = []
    for i in range(total):
        close = Decimal("20") + Decimal(i) / 30 if high else Decimal("100") - Decimal(i) / 30
        if i >= total-25:
            close += Decimal("2") * Decimal(i-(total-25)) / 24 if rising else Decimal("-2") * Decimal(i-(total-25)) / 24
        result.append(Candle(last-(total-1-i)*HOUR_MS, close, close+1, close-1, close,
                             Decimal("0"), True))
    return result


def quote(expiry: str, strike: str, option_type="C", *, spread: bool = False, face="0.01") -> OptionQuote:
    instrument = Instrument(f"BTC-USD-{expiry}-{strike}-{option_type}", "OPTION", Decimal("0.0001"),
                            Decimal("1"), Decimal("1"), "live", settle_ccy="BTC", ct_val=Decimal(face),
                            ct_mult=Decimal("1"), ct_val_ccy="BTC", inst_family="BTC-USD")
    return OptionQuote(instrument, bid_price=Decimal("0.02"), ask_price=Decimal("0.2") if spread else Decimal("0.021"))


class OptionRecommendationTest(TestCase):
    def analyze(self, **kwargs):
        return build_option_recommendations(family="BTC-USD", now_ms=NOW,
                                            price_candles=kwargs.pop("price_candles", prices()),
                                            dvol_hourly=kwargs.pop("dvol_hourly", volatility()), **kwargs)

    def test_analysis_payload_keeps_entry_quotes_face_and_scales_each_ratio(self):
        quotes = [quote("261120", "120"), quote("261120", "125")]
        quotes[0] = replace(quotes[0], mark_price=Decimal("0.019"))
        snap = self.analyze(quotes=quotes)
        original = snap.suggestions[0]
        suggestion = replace(original, legs=(original.legs[0], replace(original.legs[1], ratio=2)))
        snap = replace(snap, suggestions=(suggestion,))
        payload = build_recommendation_analysis_payload(snap, suggestion, base_quantity=Decimal(5), now_ms=NOW)
        self.assertEqual([leg.quantity for leg in payload.legs], [Decimal(5), Decimal(10)])
        self.assertEqual([leg.side for leg in payload.legs], ["buy", "sell"])
        self.assertEqual([leg.premium for leg in payload.legs], [Decimal("0.021"), Decimal("0.02")])
        self.assertEqual(payload.quotes[0].mark_price, Decimal("0.019"))
        self.assertEqual(payload.instruments[0].ct_val * payload.legs[0].quantity, Decimal("0.05"))
        self.assertEqual(payload.quotes[0].index_price, snap.trends[1].close)
        self.assertEqual([leg.alias for leg in payload.legs], ["L1", "L2"])
        self.assertEqual(payload.expiry_code, "261120")

    def test_analysis_payload_rejects_stale_incomplete_invalid_or_expired_recommendation(self):
        snap = self.analyze(quotes=[quote("261120", "120"), quote("261120", "125")])
        suggestion = snap.suggestions[0]
        invalid_snapshots = (replace(snap, analyzed_at_ms=NOW-300_001), replace(snap, blockers=("缺口",)),
                             replace(snap, analyzed_at_ms=NOW+60_001))
        for item in invalid_snapshots:
            with self.assertRaises(ValueError):
                build_recommendation_analysis_payload(item, suggestion, base_quantity=Decimal(1), now_ms=NOW)
        for value in (Decimal(0), Decimal(-1), Decimal("NaN"), Decimal("Infinity")):
            with self.assertRaises(ValueError):
                build_recommendation_analysis_payload(snap, suggestion, base_quantity=value, now_ms=NOW)
        broken = (replace(suggestion, legs=()), replace(suggestion, expiry="261001"),
                  replace(suggestion, legs=(replace(suggestion.legs[0], quote=None), suggestion.legs[1])))
        for item in broken:
            with self.assertRaises(ValueError):
                build_recommendation_analysis_payload(replace(snap, suggestions=(item,)), item,
                                                      base_quantity=Decimal(1), now_ms=NOW)

    def test_bullish_and_bearish_price_spreads_follow_all_three_trend_periods(self):
        bull = self.analyze()
        self.assertFalse(bull.blockers)
        self.assertIn("bull_call", [s.key for s in bull.suggestions])
        bear = self.analyze(price_candles=prices(-1))
        self.assertIn("bear_put", [s.key for s in bear.suggestions])
        self.assertNotIn("bull_call", [s.key for s in bear.suggestions])
        self.assertIn("尚未确认", bull.suggestions[0].reason)

    def test_conflicting_daily_direction_does_not_recommend_directional_spread(self):
        mixed = prices()
        mixed["1D"] = prices(-1)["1D"]
        snapshot = self.analyze(price_candles=mixed)
        self.assertNotIn("bull_call", [s.key for s in snapshot.suggestions])
        self.assertNotIn("bear_put", [s.key for s in snapshot.suggestions])

    def test_stale_gap_missing_or_unconfirmed_data_blocks_recommendations(self):
        for kind in ("missing", "gap", "stale", "unconfirmed"):
            with self.subTest(kind=kind):
                candles = prices()
                if kind == "missing":
                    candles["1H"] = candles["1H"][-20:]
                elif kind == "gap":
                    candles["1H"].pop(-10)
                elif kind == "stale":
                    candles["1H"] = [Candle(c.ts-10*HOUR_MS, c.open, c.high, c.low, c.close, c.volume, True)
                                      for c in candles["1H"]]
                else:
                    candles["1H"] = [Candle(c.ts, c.open, c.high, c.low, c.close, c.volume, False)
                                      for c in candles["1H"]]
                snapshot = self.analyze(price_candles=candles)
                self.assertTrue(snapshot.blockers)
                self.assertFalse(snapshot.suggestions)

    def test_future_candles_are_not_treated_as_current_market(self):
        candles = prices()
        c = candles["1H"][-1]
        candles["1H"].append(Candle(NOW+HOUR_MS, c.open*10, c.high*10, c.low*10, c.close*10, c.volume, True))
        snapshot = self.analyze(price_candles=candles)
        self.assertEqual(next(t.close for t in snapshot.trends if t.period == "1H"), c.close)

    def test_missing_dvol_history_does_not_guess_low_volatility(self):
        snapshot = self.analyze(dvol_hourly=volatility()[-48:])
        self.assertIsNone(snapshot.dvol_percentile)
        self.assertTrue(snapshot.blockers)
        self.assertFalse(snapshot.suggestions)

    def test_recent_dvol_gap_and_old_dvol_cache_are_blocked(self):
        series = volatility()
        series.pop(-5)
        self.assertTrue(self.analyze(dvol_hourly=series).blockers)
        self.assertTrue(self.analyze(dvol_hourly=volatility()[:-10]).blockers)

    def test_dvol_utc8_day_requires_all_24_hours(self):
        start = int(datetime(2026, 10, 1, 16, tzinfo=timezone.utc).timestamp()*1000)
        candles = [Candle(start+i*HOUR_MS, Decimal(i+1), Decimal(i+1), Decimal(i+1), Decimal(i+1), Decimal(0), True)
                   for i in range(24)]
        self.assertEqual(_daily_dvol_closes(candles), [Decimal(24)])
        self.assertEqual(_daily_dvol_closes(candles[:-1]), [])
        self.assertEqual(_daily_dvol_closes(candles[:10]+candles[11:]), [])

    def test_expiry_uses_exchange_08utc_instead_of_next_local_midnight(self):
        at_expiry = int(datetime(2026, 10, 5, 8, tzinfo=timezone.utc).timestamp()*1000)
        self.assertEqual(option_days_to_expiry("261005", at_expiry), 0)
        self.assertLess(option_days_to_expiry("261005", at_expiry+1), 0)

    def test_contracts_require_matching_expiry_quotes_face_and_bid_ask(self):
        suggestion = StrategySuggestion("bull_call", "牛市价差", "买1卖1", 30, 60, "", "", "", "")
        quotes = [quote("261005", "120"), quote("261005", "125"),
                  quote("261120", "120"), quote("261120", "125")]
        matched = _match_contracts(suggestion, "BTC-USD", Decimal(120), quotes, NOW, RecommendationRules())
        self.assertEqual(matched.expiry, "261120")
        self.assertEqual([leg.side for leg in matched.legs], ["buy", "sell"])
        self.assertEqual(matched.legs[0].reference_price, Decimal("0.021"))
        self.assertEqual(matched.legs[1].reference_price, Decimal("0.02"))
        for invalid in ([quote("261120", "120"), quote("261120", "125", spread=True)],
                        [quote("261120", "120"), quote("261120", "125", face="0.1")],
                        [quote("261120", "120"), quote("261121", "125")]):
            self.assertFalse(_match_contracts(suggestion,"BTC-USD",Decimal(120),invalid,NOW,RecommendationRules()).legs)

    def test_low_and_rising_dvol_suggests_buy_volatility_but_no_calendar_guess(self):
        snapshot = self.analyze(dvol_hourly=volatility(rising=True))
        self.assertIn("long_straddle", [s.key for s in snapshot.suggestions])
        self.assertFalse(any("calendar" in s.key for s in snapshot.suggestions))
        self.assertTrue(any("日历" in note for note in snapshot.notes))

    def test_high_volatility_alone_is_not_enough_for_seller_strategy(self):
        snapshot = self.analyze(dvol_hourly=volatility(high=True, rising=True))
        self.assertFalse(any(s.high_risk for s in snapshot.suggestions))

    def test_unsupported_option_family_returns_no_candidates(self):
        snapshot = build_option_recommendations(family="SOL-USD", now_ms=NOW,
                                                price_candles=prices(), dvol_hourly=volatility())
        self.assertTrue(snapshot.blockers)
        self.assertFalse(snapshot.suggestions)

    def test_utc_zero_daily_and_misaligned_intraday_bars_are_not_utc8_data(self):
        for period, shift in (("1D", 8 * HOUR_MS), ("1H", 60_000)):
            candles = prices()
            candles[period] = [replace(c, ts=c.ts+shift) for c in candles[period]]
            snapshot = self.analyze(price_candles=candles)
            self.assertTrue(snapshot.blockers)
            self.assertFalse(snapshot.suggestions)

    def test_invalid_contract_dates_strikes_and_zero_multipliers_are_skipped(self):
        suggestion = StrategySuggestion("bull_call", "牛市价差", "买1卖1", 30, 60, "", "", "", "")
        valid = [quote("261120", "120"), quote("261120", "125")]
        bad = [quote("269999", "120"), quote("261120", "NaN"), quote("261120", "-1")]
        self.assertTrue(_match_contracts(suggestion, "BTC-USD", Decimal(120), bad+valid, NOW, RecommendationRules()).legs)
        for mult in ("0", "-1", "NaN"):
            quotes = [valid[0], replace(valid[1], instrument=replace(valid[1].instrument, ct_mult=Decimal(mult)))]
            self.assertFalse(_match_contracts(suggestion, "BTC-USD", Decimal(120), quotes, NOW, RecommendationRules()).legs)

    def test_structure_directions_and_ratios_match_real_contracts(self):
        quotes = [quote("261105", str(strike), kind) for strike in (110, 120, 130) for kind in ("C", "P")]
        expected = {
            "bull_call": [("buy", 1, 120), ("sell", 1, 130)],
            "bear_put": [("buy", 1, 120), ("sell", 1, 110)],
            "call_ratio": [("buy", 1, 120), ("sell", 2, 130)],
            "put_ratio": [("buy", 1, 120), ("sell", 2, 110)],
            "call_backspread": [("sell", 1, 120), ("buy", 2, 130)],
            "put_backspread": [("sell", 1, 120), ("buy", 2, 110)],
            "long_straddle": [("buy", 1, 120), ("buy", 1, 120)],
            "short_strangle": [("sell", 1, 110), ("sell", 1, 130)],
        }
        for key, specs in expected.items():
            with self.subTest(key=key):
                suggestion = StrategySuggestion(key, key, "", 20, 45, "", "", "", "")
                match = _match_contracts(suggestion, "BTC-USD", Decimal(120), quotes, NOW, RecommendationRules())
                self.assertEqual([(leg.side, leg.ratio, leg.strike) for leg in match.legs], specs)
