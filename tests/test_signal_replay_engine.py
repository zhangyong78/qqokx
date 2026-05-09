from __future__ import annotations

import unittest
from decimal import Decimal

from okx_quant.models import Candle
from okx_quant.signal_replay_engine import SignalReplayConfig, build_signal_replay_dataset


def _candle(index: int, close: Decimal) -> Candle:
    return Candle(
        ts=1_700_000_000_000 + (index * 3_600_000),
        open=close - Decimal("1"),
        high=close + Decimal("2"),
        low=close - Decimal("2"),
        close=close,
        volume=Decimal("100") + Decimal(index % 10),
        confirmed=True,
    )


def _custom_candle(index: int, open_: str, high: str, low: str, close: str) -> Candle:
    return Candle(
        ts=1_700_000_000_000 + (index * 3_600_000),
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("100"),
        confirmed=True,
    )


class SignalReplayEngineTest(unittest.TestCase):
    def test_builds_long_signals_from_uptrend(self) -> None:
        candles = [_candle(index, Decimal("100") + Decimal(index)) for index in range(120)]
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_short=False,
            ),
        )

        self.assertGreater(dataset.summary.long_count, 0)
        self.assertEqual(dataset.summary.short_count, 0)
        self.assertGreater(dataset.summary.completed_24h, 0)
        self.assertTrue(all(signal.direction == "long" for signal in dataset.signals))

    def test_builds_short_signals_from_downtrend(self) -> None:
        candles = [_candle(index, Decimal("220") - Decimal(index)) for index in range(120)]
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_long=False,
            ),
        )

        self.assertGreater(dataset.summary.short_count, 0)
        self.assertEqual(dataset.summary.long_count, 0)
        self.assertGreater(dataset.summary.completed_24h, 0)
        self.assertTrue(all(signal.direction == "short" for signal in dataset.signals))

    def test_near_ema_filter_reduces_signals(self) -> None:
        candles = [_candle(index, Decimal("100") + Decimal(index)) for index in range(120)]
        loose = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_short=False,
            ),
        )
        strict = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                enable_near_ema_filter=True,
                near_ema_max_pct=Decimal("0.4"),
                include_short=False,
            ),
        )

        self.assertGreater(len(loose.signals), 0)
        self.assertLess(len(strict.signals), len(loose.signals))
        for signal in strict.signals:
            near_pct = strict.near_ema_pct[signal.index]
            self.assertIsNotNone(near_pct)
            self.assertLessEqual(near_pct, Decimal("0.4"))

    def test_detects_pattern_signals_with_large_move_gate(self) -> None:
        candles = [_custom_candle(index, "100", "100.3", "99.8", "100.1") for index in range(30)]
        candles.extend(
            [
                _custom_candle(30, "100", "113", "99", "112"),
                _custom_candle(31, "112", "112.5", "111.5", "112.1"),
                _custom_candle(32, "112", "114", "98", "113"),
                _custom_candle(33, "113", "116", "112", "112.5"),
                _custom_candle(34, "112", "113", "111", "111.2"),
            ]
        )
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_trend_filter=False,
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                enable_near_ema_filter=False,
                enable_atr_filter=False,
                include_long=False,
                include_short=False,
                enable_large_move_atr=False,
                enable_large_move_mean=True,
                mean_body_period=20,
                mean_body_multiplier=Decimal("1.8"),
            ),
        )
        pattern_ids = {signal.pattern_id for signal in dataset.signals}

        self.assertIn("big_bullish", pattern_ids)
        self.assertIn("inside_bar", pattern_ids)
        self.assertTrue(any(signal.large_move_rules for signal in dataset.signals if signal.pattern_id == "big_bullish"))

    def test_fractals_require_prior_trend(self) -> None:
        candles = [_custom_candle(index, "100", "100.3", "99.8", "100.1") for index in range(30)]
        candles.extend(
            [
                _custom_candle(30, "100", "102", "99", "101"),
                _custom_candle(31, "101", "103", "100", "102"),
                _custom_candle(32, "102", "104", "101", "103"),
                _custom_candle(33, "103", "110", "102", "108"),
                _custom_candle(34, "108", "112", "107", "108.2"),
                _custom_candle(35, "108", "109", "99", "100"),
                _custom_candle(36, "100", "101", "95", "97"),
                _custom_candle(37, "97", "98", "92", "94"),
                _custom_candle(38, "94", "95", "90", "92"),
                _custom_candle(39, "92", "93", "84", "86"),
                _custom_candle(40, "86", "87", "80", "85.8"),
                _custom_candle(41, "86", "96", "85", "95"),
            ]
        )
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_trend_filter=False,
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_long=False,
                include_short=False,
                enable_large_move_gate=False,
                fractal_trend_lookback=5,
                fractal_trend_min_bars=3,
            ),
        )
        top = [signal for signal in dataset.signals if signal.pattern_id == "top_fractal"]
        bottom = [signal for signal in dataset.signals if signal.pattern_id == "bottom_fractal"]

        self.assertTrue(any(signal.index == 35 for signal in top))
        self.assertTrue(any(signal.index == 41 for signal in bottom))

    def test_false_breaks_require_prior_trend(self) -> None:
        candles = [_custom_candle(index, "100", "100.3", "99.8", "100.1") for index in range(30)]
        candles.extend(
            [
                _custom_candle(30, "100", "101", "97", "98"),
                _custom_candle(31, "98", "99", "95", "96"),
                _custom_candle(32, "96", "97", "93", "94"),
                _custom_candle(33, "94", "96", "90", "95"),
                _custom_candle(34, "95", "99", "94", "98"),
                _custom_candle(35, "98", "102", "97", "101"),
                _custom_candle(36, "101", "105", "100", "104"),
                _custom_candle(37, "104", "108", "103", "107"),
                _custom_candle(38, "107", "110", "106", "106.5"),
            ]
        )
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_trend_filter=False,
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_long=False,
                include_short=False,
                enable_large_move_gate=False,
                fractal_trend_lookback=5,
                fractal_trend_min_bars=3,
            ),
        )
        breakdown = [signal for signal in dataset.signals if signal.pattern_id == "false_breakdown"]
        breakout = [signal for signal in dataset.signals if signal.pattern_id == "false_breakout"]

        self.assertTrue(any(signal.index == 33 for signal in breakdown))
        self.assertTrue(any(signal.index == 38 for signal in breakout))

    def test_false_breaks_require_structure_break_distance_and_reclaim(self) -> None:
        candles = [_custom_candle(index, "100", "100.3", "99.8", "100.1") for index in range(30)]
        candles.extend(
            [
                _custom_candle(30, "100", "101", "97", "98"),
                _custom_candle(31, "98", "99", "95", "96"),
                _custom_candle(32, "96", "97", "93", "94"),
                _custom_candle(33, "94", "95", "92.96", "94.2"),
                _custom_candle(34, "94", "95", "92.4", "94.3"),
            ]
        )
        dataset = build_signal_replay_dataset(
            candles,
            config=SignalReplayConfig(
                enable_trend_filter=False,
                enable_pullback_trigger=False,
                enable_macd_filter=False,
                enable_volume_filter=False,
                enable_bias_filter=False,
                include_long=False,
                include_short=False,
                enable_large_move_gate=False,
                enable_large_move_atr=False,
                false_break_reference_lookback=6,
                false_break_min_pct=Decimal("0.05"),
                false_break_atr_multiplier=Decimal("0"),
                false_break_reclaim_position=Decimal("0.6"),
                fractal_trend_lookback=5,
                fractal_trend_min_bars=3,
            ),
        )
        breakdown = [signal for signal in dataset.signals if signal.pattern_id == "false_breakdown"]

        self.assertFalse(any(signal.index == 33 for signal in breakdown))
        self.assertTrue(any(signal.index == 34 for signal in breakdown))


if __name__ == "__main__":
    unittest.main()
