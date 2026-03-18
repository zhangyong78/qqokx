from decimal import Decimal
from unittest import TestCase

from okx_quant.engine import fetch_hourly_ema_debug, format_hourly_debug, recommended_indicator_lookback
from okx_quant.models import Candle


class DummyClient:
    def __init__(self, candles: list[Candle]) -> None:
        self._candles = candles
        self.calls: list[tuple[str, str, int]] = []

    def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
        self.calls.append((inst_id, bar, limit))
        return self._candles


class DebugSnapshotTest(TestCase):
    def test_recommended_lookback_uses_at_least_four_times_period(self) -> None:
        self.assertEqual(recommended_indicator_lookback(21), 120)
        self.assertEqual(recommended_indicator_lookback(80), 300)

    def test_hourly_debug_uses_requested_ema_period_and_outputs_atr10(self) -> None:
        candles = []
        for index in range(1, 130):
            candles.append(
                Candle(
                    ts=index,
                    open=Decimal(index),
                    high=Decimal(index + 1),
                    low=Decimal(index - 1),
                    close=Decimal(index),
                    volume=Decimal("1"),
                    confirmed=True,
                )
            )
        client = DummyClient(candles)
        snapshot = fetch_hourly_ema_debug(client, "BTC-USDT-SWAP", ema_period=21)
        debug_text = format_hourly_debug("BTC-USDT-SWAP", snapshot)
        self.assertEqual(snapshot.ema_period, 21)
        self.assertEqual(snapshot.atr_period, 10)
        self.assertEqual(client.calls[0], ("BTC-USDT-SWAP", "1H", 120))
        self.assertIn("上一根EMA21", debug_text)
        self.assertIn("上一根ATR10", debug_text)
