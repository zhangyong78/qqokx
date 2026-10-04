from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from okx_quant.contract_recommendations import DAY_MS, HOUR_MS, PERIOD_MS, build_contract_recommendation
from okx_quant.models import Candle


NOW = int(datetime(2026, 10, 5, 4, tzinfo=timezone.utc).timestamp() * 1000)


def prices(direction: int = 1) -> dict[str, list[Candle]]:
    result = {}
    for period, step in PERIOD_MS.items():
        offset = -8 * HOUR_MS if period == "1D" else (-(3 * DAY_MS + 8 * HOUR_MS) if period == "1W" else 0)
        last = (NOW - offset) // step * step + offset - step
        candles = []
        for index in range(100):
            close = Decimal("100") + direction * Decimal(index) / 10
            candles.append(Candle(last - (99-index) * step, close, close + 1, close - 1, close, Decimal("10"), True))
        result[period] = candles
    return result


class ContractRecommendationTest(TestCase):
    def analyze(self, candles=None, **kwargs):
        return build_contract_recommendation(price_candles=prices() if candles is None else candles,
                                             now_ms=kwargs.pop("now_ms", NOW), **kwargs)

    def test_bullish_and_bearish_use_overview_ema15_ma50(self):
        for sign, bias, side in ((1, "偏多", "做多"), (-1, "偏空", "做空")):
            with self.subTest(sign=sign):
                snapshot = self.analyze(prices(sign))
                self.assertFalse(snapshot.blockers)
                self.assertEqual(snapshot.direction, bias)
                self.assertEqual(snapshot.strategy, "顺势回踩" + side)
                hourly = next(t for t in snapshot.trends if t.period == "1H")
                expected = sum((c.close for c in prices(sign)["1H"][-50:]), Decimal(0)) / 50
                self.assertEqual(hourly.ma50, expected)
                self.assertIn("1R", snapshot.management)

    def test_daily_4h_conflict_or_flat_market_waits(self):
        mixed = prices()
        mixed["4H"] = prices(-1)["4H"]
        for candles in (mixed, prices(0)):
            snapshot = self.analyze(candles)
            self.assertEqual(snapshot.direction, "观望")
            self.assertIn("观望", snapshot.strategy)
            self.assertIn("分歧", snapshot.status)

    def test_hourly_countertrend_waits_for_recovery(self):
        candles = prices()
        candles["1H"] = prices(-1)["1H"]
        snapshot = self.analyze(candles)
        self.assertEqual(snapshot.direction, "偏多")
        self.assertEqual(snapshot.strategy, "等待1H恢复后做多")
        self.assertIn("尚未同向", snapshot.status)
        self.assertNotIn("止损参考", snapshot.invalidation)

    def test_weekly_opposition_is_context_and_missing_week_does_not_block(self):
        candles = prices()
        candles["1W"] = prices(-1)["1W"]
        snapshot = self.analyze(candles)
        self.assertFalse(snapshot.blockers)
        self.assertIn("逆大周期", snapshot.reason)
        del candles["1W"]
        self.assertFalse(self.analyze(candles).blockers)
        self.assertIn("周线背景不可用", self.analyze(candles).reason)

    def test_incomplete_stale_gap_misaligned_or_invalid_price_blocks(self):
        for kind in ("missing", "stale", "gap", "unconfirmed", "daily_boundary", "invalid"):
            with self.subTest(kind=kind):
                candles = prices()
                if kind == "missing":
                    candles["1H"] = candles["1H"][-20:]
                elif kind == "stale":
                    candles["1H"] = [replace(c, ts=c.ts - 10 * HOUR_MS) for c in candles["1H"]]
                elif kind == "gap":
                    candles["1H"].pop(-5)
                elif kind == "unconfirmed":
                    candles["1H"] = [replace(c, confirmed=False) for c in candles["1H"]]
                elif kind == "daily_boundary":
                    candles["1D"] = [replace(c, ts=c.ts + 8 * HOUR_MS) for c in candles["1D"]]
                else:
                    candles["1H"][-5] = replace(candles["1H"][-5], close=Decimal("NaN"))
                snapshot = self.analyze(candles)
                self.assertTrue(snapshot.blockers)
                self.assertEqual(snapshot.strategy, "等待数据更新")
                self.assertNotIn("止损参考", snapshot.invalidation)

    def test_future_open_and_unconfirmed_candles_never_change_recommendation(self):
        baseline = self.analyze()
        for ts, confirmed in ((NOW, True), (NOW + HOUR_MS, True), (NOW - HOUR_MS, False)):
            candles = prices()
            candles["1H"].append(Candle(ts, Decimal(1), Decimal(2), Decimal(1), Decimal(1), Decimal(0), confirmed))
            self.assertEqual(self.analyze(candles), baseline)

    def test_breakout_excludes_current_bar_from_prior_twenty(self):
        for sign, name in ((1, "做多"), (-1, "做空")):
            with self.subTest(sign=sign):
                candles = prices(sign)
                last = candles["1H"][-1]
                prior = candles["1H"][-21:-1]
                boundary = max(c.high for c in prior) if sign > 0 else min(c.low for c in prior)
                close = boundary + sign * Decimal(1)
                candles["1H"][-1] = replace(last, close=close, high=max(last.high, close + 1), low=min(last.low, close - 1))
                snapshot = self.analyze(candles)
                self.assertEqual(snapshot.strategy, "突破回测" + name)
                self.assertIn(f"{boundary:,.2f}", snapshot.entry_condition)
                self.assertIn("等待回测", snapshot.status)

    def test_confirmed_pullback_shows_trigger_extreme_and_buffered_stop(self):
        for sign in (1, -1):
            candles = prices(sign)
            last = candles["1H"][-1]
            if sign > 0:
                candles["1H"][-1] = replace(last, low=last.low - 1)
            else:
                candles["1H"][-1] = replace(last, high=last.high + 1)
            snapshot = self.analyze(candles)
            self.assertIn("回踩已确认", snapshot.status)
            self.assertIn(f"{(last.high if sign > 0 else last.low):,.2f}", snapshot.entry_condition)
            hourly = next(t for t in snapshot.trends if t.period == "1H")
            extreme = candles["1H"][-1].low if sign > 0 else candles["1H"][-1].high
            stop = extreme - sign * hourly.atr14 * Decimal("0.2")
            self.assertIn(f"{stop:,.2f}", snapshot.invalidation)

    def test_dvol_is_optional_and_never_sets_price_direction(self):
        for sign in (1, -1):
            dvol = prices(sign)["1H"]
            baseline = self.analyze()
            snapshot = self.analyze(dvol_hourly=dvol)
            self.assertEqual(snapshot.direction, baseline.direction)
            self.assertEqual(snapshot.strategy, baseline.strategy)
            self.assertIn("DVOL 不决定多空", snapshot.volatility_note)
        self.assertIn("背景未知", self.analyze().volatility_note)
        self.assertIn("ETH 判断", self.analyze(asset_label="ETH").volatility_note)

    def test_dvol_gap_staleness_and_unclosed_values_are_reported(self):
        dvol = prices()["1H"]
        baseline = self.analyze(dvol_hourly=dvol)
        dvol.append(Candle(NOW, Decimal(900), Decimal(900), Decimal(900), Decimal(900), Decimal(0), True))
        self.assertEqual(self.analyze(dvol_hourly=dvol).volatility_note, baseline.volatility_note)
        dvol.pop(-5)
        self.assertIn("缺口", self.analyze(dvol_hourly=dvol).volatility_note)
        self.assertIn("过期", self.analyze(dvol_hourly=dvol[:-10]).volatility_note)

    def test_age_recheck_removes_previously_valid_direction(self):
        self.assertFalse(self.analyze().blockers)
        snapshot = self.analyze(now_ms=NOW + 4 * HOUR_MS)
        self.assertTrue(snapshot.blockers)
        self.assertEqual(snapshot.direction, "观望")
