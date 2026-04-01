from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle, StrategyConfig
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy


class EmaCrossEmaStopStrategyTest(TestCase):
    def _make_candles(self, closes: list[str]) -> list[Candle]:
        candles: list[Candle] = []
        previous_close = Decimal(closes[0])
        for index, raw_close in enumerate(closes, start=1):
            close = Decimal(raw_close)
            open_price = previous_close
            high = max(open_price, close) + Decimal("1")
            low = min(open_price, close) - Decimal("1")
            candles.append(Candle(index, open_price, high, low, close, Decimal("1"), True))
            previous_close = close
        return candles

    def _build_config(self, *, signal_mode: str = "both") -> StrategyConfig:
        return StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="4H",
            ema_period=2,
            trend_ema_period=3,
            atr_period=10,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode=signal_mode,
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
            strategy_id="ema5_ema8_cross_stop",
        )

    def test_detects_golden_cross_long(self) -> None:
        candles = self._make_candles(["100", "99", "98", "97", "102"])

        decision = EmaCrossEmaStopStrategy().evaluate(candles, self._build_config())

        self.assertEqual(decision.signal, "long")
        self.assertEqual(decision.entry_reference, Decimal("102"))
        self.assertIsNotNone(decision.ema_value)

    def test_detects_death_cross_short(self) -> None:
        candles = self._make_candles(["100", "101", "102", "103", "98"])

        decision = EmaCrossEmaStopStrategy().evaluate(candles, self._build_config())

        self.assertEqual(decision.signal, "short")
        self.assertEqual(decision.entry_reference, Decimal("98"))
        self.assertIsNotNone(decision.ema_value)

    def test_stop_triggered_when_long_close_falls_below_slow_ema(self) -> None:
        candles = self._make_candles(["100", "99", "98", "97", "102", "100", "98"])

        stop_hit, current_candle, stop_line = EmaCrossEmaStopStrategy().stop_triggered(
            candles,
            self._build_config(),
            "long",
        )

        self.assertTrue(stop_hit)
        self.assertEqual(current_candle.close, Decimal("98"))
        self.assertGreater(stop_line, current_candle.close)
