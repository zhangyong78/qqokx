from decimal import Decimal
from unittest import TestCase

from okx_quant.engine import build_order_plan
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy


class DynamicStrategyTest(TestCase):
    def test_long_mode_uses_latest_ema_as_entry_reference(self) -> None:
        candles = [
            Candle(1, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True),
            Candle(2, Decimal("101"), Decimal("103"), Decimal("100"), Decimal("102"), Decimal("1"), True),
            Candle(3, Decimal("102"), Decimal("105"), Decimal("101"), Decimal("104"), Decimal("1"), True),
            Candle(4, Decimal("104"), Decimal("106"), Decimal("103"), Decimal("105"), Decimal("1"), True),
        ]
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
        )
        decision = EmaDynamicOrderStrategy().evaluate(candles, config)
        self.assertEqual(decision.signal, "long")
        self.assertEqual(decision.entry_reference, decision.ema_value)
        self.assertIsNotNone(decision.atr_value)

    def test_long_mode_skips_when_close_is_not_above_ema(self) -> None:
        candles = [
            Candle(1, Decimal("100"), Decimal("102"), Decimal("99"), Decimal("101"), Decimal("1"), True),
            Candle(2, Decimal("101"), Decimal("103"), Decimal("100"), Decimal("102"), Decimal("1"), True),
            Candle(3, Decimal("102"), Decimal("103"), Decimal("97"), Decimal("98"), Decimal("1"), True),
            Candle(4, Decimal("98"), Decimal("99"), Decimal("94"), Decimal("95"), Decimal("1"), True),
        ]
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
        )
        decision = EmaDynamicOrderStrategy().evaluate(candles, config)
        self.assertIsNone(decision.signal)
        self.assertIsNone(decision.entry_reference)

    def test_risk_amount_controls_order_size(self) -> None:
        instrument = Instrument(
            inst_id="BTC-USDT-SWAP",
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
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
        )
        plan = build_order_plan(
            instrument=instrument,
            config=config,
            order_size=None,
            signal="long",
            entry_reference=Decimal("2500"),
            atr_value=Decimal("10"),
            candle_ts=1,
        )
        self.assertEqual(plan.size, Decimal("5"))
        self.assertEqual(plan.stop_loss, Decimal("2480"))
        self.assertEqual(plan.take_profit, Decimal("2540"))
