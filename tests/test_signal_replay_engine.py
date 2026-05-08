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


if __name__ == "__main__":
    unittest.main()
