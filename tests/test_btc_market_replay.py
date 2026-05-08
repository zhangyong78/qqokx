from decimal import Decimal
from unittest import TestCase

from okx_quant.btc_market_replay import build_replay_validation, replay_validation_payload
from okx_quant.models import Candle


class BtcMarketReplayTest(TestCase):
    def test_build_replay_validation_marks_long_signal_effective(self) -> None:
        candles = [
            Candle(
                ts=1_000 + (index * 3_600_000),
                open=Decimal("100") + Decimal(index),
                high=Decimal("101") + Decimal(index),
                low=Decimal("99.5") + Decimal(index),
                close=Decimal("100.8") + Decimal(index),
                volume=Decimal("1"),
                confirmed=True,
            )
            for index in range(24)
        ]

        validation = build_replay_validation(
            direction="long",
            timeframe="1H",
            entry_price=Decimal("100"),
            analysis_candle_ts=0,
            future_candles=candles,
            timeframe_ms=3_600_000,
        )

        payload = replay_validation_payload(validation)
        self.assertEqual(validation.verdict, "effective")
        self.assertEqual(validation.status, "completed")
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["review_windows"], ["4H", "12H", "24H"])

    def test_build_replay_validation_pending_without_future_candles(self) -> None:
        validation = build_replay_validation(
            direction="long",
            timeframe="1H",
            entry_price=Decimal("100"),
            analysis_candle_ts=0,
            future_candles=[],
            timeframe_ms=3_600_000,
        )

        self.assertEqual(validation.status, "pending")
        self.assertEqual(validation.verdict, "pending")
