from dataclasses import replace
from decimal import Decimal
from unittest import TestCase

from okx_quant.kline_trade_recommendations import DAY_MS, HOUR_MS, STEPS, build_kline_trade_idea
from okx_quant.models import Candle

NOW = 1_791_158_400_000  # fixed UTC timestamp, not the machine clock


def candles(period="1H", slope="0.1", count=100, spread="2"):
    step = STEPS[period.upper()]
    offset = -8 * HOUR_MS if period == "1D" else 4 * DAY_MS - 8 * HOUR_MS if period == "1W" else 0
    end = (NOW - offset) // step * step + offset
    rows = []
    for i in range(count):
        close = Decimal(100) + Decimal(slope) * i
        rows.append(Candle(end - (count-i)*step, close, close+Decimal(spread),
                           close-Decimal(spread), close, Decimal(1), True))
    return rows


def idea(symbol="ETH-USDT-SWAP", period="1H", rows=None, now=NOW):
    return build_kline_trade_idea(symbol=symbol, period=period,
                                 candles=candles(period) if rows is None else rows, now_ms=now)


class TradeIdeaTest(TestCase):
    def test_long_perpetual_has_conditional_entry_and_ordered_levels(self):
        result = idea()
        self.assertEqual(result.action, "条件做多")
        self.assertLess(result.stop, result.entry_low)
        self.assertLess(result.entry_low, result.entry_high)
        self.assertLess(result.entry_high, result.target_2r)
        self.assertLess(result.target_2r, result.target_3r)
        mid = (result.entry_low + result.entry_high) / 2
        self.assertEqual(result.target_2r-mid, 2*(mid-result.stop))
        self.assertLess(result.worst_rr, 2)
        self.assertIn("等待", result.trigger)

    def test_short_perpetual_has_ordered_levels(self):
        result = idea(rows=candles(slope="-0.1"))
        self.assertEqual(result.action, "条件做空")
        self.assertGreater(result.stop, result.entry_high)
        self.assertGreater(result.entry_low, result.target_2r)
        self.assertGreater(result.target_2r, result.target_3r)
        mid = (result.entry_low + result.entry_high) / 2
        self.assertLess(abs((mid-result.target_3r)-3*(result.stop-mid)), Decimal("1e-20"))

    def test_spot_buys_but_never_opens_a_short(self):
        for symbol in ("ETH-USDT", "SOL-USDT", "ETH-BTC"):
            with self.subTest(symbol=symbol):
                self.assertEqual(idea(symbol=symbol).action, "条件买入")
                result = idea(symbol=symbol, rows=candles(slope="-0.1"))
                self.assertEqual(result.action, "减仓/观望")
                self.assertEqual(result.direction, "wait")
                self.assertIsNone(result.entry_low)
                self.assertIn("已有持仓", result.status)

    def test_flat_or_far_from_ema_does_not_create_entry(self):
        for rows in (candles(slope="0"), candles(slope="2", spread="0.1")):
            with self.subTest(close=rows[-1].close):
                self.assertIsNone(idea(rows=rows).entry_low)

    def test_volatile_market_waits(self):
        result = idea(rows=candles(spread="20"))
        self.assertGreater(result.atr_percent, 10)
        self.assertIsNone(result.stop)

    def test_unsupported_instruments_and_period_wait(self):
        for symbol in ("BTC-DVOL", "BTC-USD-261127-86000-C", "BTC-USDT-261127", ""):
            with self.subTest(symbol=symbol):
                self.assertEqual(idea(symbol=symbol).direction, "wait")
        self.assertEqual(idea(period="5m", rows=[]).direction, "wait")

    def test_less_than_60_confirmed_bars_wait(self):
        rows = candles(count=60)
        rows[-1] = replace(rows[-1], confirmed=False)
        self.assertIn("不足60根", idea(rows=rows).reasons[0])

    def test_gap_and_stale_cache_wait(self):
        rows = candles()
        result = idea(rows=rows[:70]+rows[71:])
        self.assertIn("缺口", result.reasons[0])
        result = idea(rows=rows, now=NOW+4*HOUR_MS)
        self.assertIn("过期", result.reasons[0])
        self.assertIsNone(result.stop)

    def test_future_unfinished_and_invalid_bars_cannot_change_result(self):
        rows = candles()
        base = idea(rows=rows)
        bad = [replace(rows[-1], ts=NOW, close=Decimal(500)),
               replace(rows[-1], confirmed=False, close=Decimal(300)),
               replace(rows[-1], high=Decimal("NaN")),
               replace(rows[-1], low=Decimal(-1))]
        self.assertEqual(idea(rows=rows+bad), base)

    def test_unsorted_and_duplicate_bars_are_normalized(self):
        rows = candles()
        self.assertEqual(idea(rows=rows), idea(rows=list(reversed(rows))+rows[:5]))

    def test_all_supported_periods_respect_boundaries(self):
        for period in ("15m", "1H", "4H", "1D", "1Dutc", "1W"):
            with self.subTest(period=period):
                rows = candles(period)
                result = idea(period=period, rows=rows)
                self.assertEqual(result.direction, "long")
                self.assertEqual(result.candle_ts, rows[-1].ts)
                if period == "1D":
                    self.assertEqual((result.candle_ts+8*HOUR_MS) % DAY_MS, 0)
                if period == "1W":
                    self.assertEqual((result.candle_ts-(4*DAY_MS-8*HOUR_MS)) % (7*DAY_MS), 0)

    def test_utc_daily_cannot_be_used_as_utc8_daily(self):
        result = idea(period="1D", rows=candles("1Dutc"))
        self.assertEqual(result.direction, "wait")
        self.assertIsNone(result.entry_low)
