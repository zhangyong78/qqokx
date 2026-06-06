from decimal import Decimal
from unittest import TestCase

from okx_quant.market_data_hub import MarketDataHub
from okx_quant.models import Candle


class MarketDataHubTest(TestCase):
    def test_reuses_shared_feed_for_same_symbol_and_bar(self) -> None:
        calls: list[tuple[str, str, int]] = []

        class _StubClient:
            def get_candles(self, inst_id: str, bar: str, limit: int = 200) -> list[Candle]:
                calls.append((inst_id, bar, limit))
                return [
                    Candle(
                        ts=index,
                        open=Decimal("1"),
                        high=Decimal("2"),
                        low=Decimal("0.5"),
                        close=Decimal("1.5"),
                        volume=Decimal("10"),
                        confirmed=True,
                    )
                    for index in range(1, limit + 1)
                ]

        hub = MarketDataHub(_StubClient())
        try:
            first = hub.get_candles("BTC-USDT-SWAP", "1H", limit=120)
            second = hub.get_candles("BTC-USDT-SWAP", "1H", limit=120)
        finally:
            hub.stop()

        self.assertEqual(len(first), 120)
        self.assertEqual(len(second), 120)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], ("BTC-USDT-SWAP", "1H", 120))
