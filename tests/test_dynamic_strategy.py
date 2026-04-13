from decimal import Decimal
from unittest import TestCase

from okx_quant.engine import build_order_plan
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy


class DynamicStrategyTest(TestCase):
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
            trend_ema_period=3,
            big_ema_period=4,
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

    def test_long_mode_keeps_pullback_order_when_close_dips_below_fast_ema(self) -> None:
        candles = self._make_candles(["100", "100", "100", "112", "107"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=3,
            big_ema_period=4,
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

    def test_long_mode_is_blocked_when_price_is_below_medium_trend_ema(self) -> None:
        candles = self._make_candles(["100"] * 60 + ["50", "80"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=5,
            big_ema_period=6,
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
        self.assertIn("EMA5", decision.reason)

    def test_long_mode_ignores_big_ema_when_fast_and_trend_conditions_hold(self) -> None:
        candles = self._make_candles(["100", "100", "100", "100", "100", "100", "60", "60", "65", "75"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=3,
            big_ema_period=6,
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
        self.assertIn("EMA2", decision.reason)
        self.assertIn("EMA3", decision.reason)

    def test_long_mode_is_blocked_when_fast_ema_is_below_trend_ema(self) -> None:
        candles = self._make_candles(["100", "90", "80", "70", "60", "66"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=5,
            big_ema_period=6,
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
        self.assertIn("EMA2", decision.reason)
        self.assertIn("EMA5", decision.reason)

    def test_short_mode_is_blocked_when_fast_ema_is_above_trend_ema(self) -> None:
        candles = self._make_candles(["60", "70", "80", "90", "100", "94"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=5,
            big_ema_period=6,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
        )

        decision = EmaDynamicOrderStrategy().evaluate(candles, config)

        self.assertIsNone(decision.signal)
        self.assertIn("EMA2", decision.reason)
        self.assertIn("EMA5", decision.reason)

    def test_short_mode_keeps_rebound_order_when_close_bounces_above_fast_ema(self) -> None:
        candles = self._make_candles(["100", "100", "100", "88", "93"])
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="15m",
            ema_period=2,
            trend_ema_period=3,
            big_ema_period=4,
            atr_period=2,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            risk_amount=Decimal("100"),
        )

        decision = EmaDynamicOrderStrategy().evaluate(candles, config)

        self.assertEqual(decision.signal, "short")
        self.assertEqual(decision.entry_reference, decision.ema_value)

    def test_risk_amount_controls_order_size(self) -> None:
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
            trend_ema_period=55,
            big_ema_period=233,
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
