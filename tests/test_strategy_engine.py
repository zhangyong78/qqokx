from decimal import Decimal
from unittest import TestCase

from okx_quant.engine import build_order_plan
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID, STRATEGY_DYNAMIC_ID


class StrategyEngineTest(TestCase):
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

    def test_long_signal_is_detected(self) -> None:
        candles = [
            Candle(1, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2, Decimal("100"), Decimal("101"), Decimal("98"), Decimal("99"), Decimal("1"), True),
            Candle(3, Decimal("99"), Decimal("100"), Decimal("97"), Decimal("98"), Decimal("1"), True),
            Candle(4, Decimal("98"), Decimal("99"), Decimal("95"), Decimal("96"), Decimal("1"), True),
            Candle(5, Decimal("96"), Decimal("106"), Decimal("95"), Decimal("104"), Decimal("1"), True),
        ]
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
        )
        decision = EmaAtrStrategy().evaluate(candles, config)
        self.assertEqual(decision.signal, "long")
        self.assertIsNotNone(decision.atr_value)

    def test_cross_long_signal_is_blocked_when_below_ema55(self) -> None:
        candles = self._make_candles(["100"] * 60 + ["50", "80"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
        )

        decision = EmaAtrStrategy().evaluate(candles, config)

        self.assertIsNone(decision.signal)
        self.assertIn("EMA55", decision.reason)

    def test_order_plan_builds_tp_and_sl(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_ID,
        )
        plan = build_order_plan(
            instrument=instrument,
            config=config,
            order_size=Decimal("2"),
            signal="long",
            entry_reference=Decimal("2500"),
            atr_value=Decimal("10"),
            candle_ts=1,
        )
        self.assertEqual(plan.side, "buy")
        self.assertEqual(plan.pos_side, "long")
        self.assertEqual(plan.stop_loss, Decimal("2480"))
        self.assertEqual(plan.take_profit, Decimal("2540"))

    def test_cross_strategy_stop_loss_uses_signal_candle_low_minus_one_atr(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
        )
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=21,
            atr_period=14,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="both",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_CROSS_ID,
        )
        plan = build_order_plan(
            instrument=instrument,
            config=config,
            order_size=Decimal("2"),
            signal="long",
            entry_reference=Decimal("2500"),
            atr_value=Decimal("10"),
            candle_ts=1,
            signal_candle_low=Decimal("2485.2"),
        )
        self.assertEqual(plan.stop_loss, Decimal("2475.2"))
        self.assertEqual(plan.take_profit, Decimal("2540"))
