from decimal import Decimal
from unittest import TestCase

from okx_quant.engine import _advance_dynamic_stop_live, build_order_plan, can_use_exchange_managed_orders
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
)


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
            trend_ema_period=2,
            big_ema_period=3,
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
            trend_ema_period=5,
            big_ema_period=6,
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
        self.assertIn("EMA6", decision.reason)

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
            trend_ema_period=55,
            big_ema_period=233,
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

    def test_dynamic_long_strategy_can_use_okx托管止盈止损(self) -> None:
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
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        )

        self.assertTrue(can_use_exchange_managed_orders(config, instrument, instrument))

    def test_dynamic_short_strategy_can_use_okx托管止盈止损(self) -> None:
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
            bar="4H",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("1"),
            trade_mode="cross",
            signal_mode="short_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        )

        self.assertTrue(can_use_exchange_managed_orders(config, instrument, instrument))

    def test_dynamic_live_stop_locks_1r_at_2r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("110.1"))
        self.assertEqual(next_take_profit, Decimal("130.1"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_stop_locks_2r_at_3r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("130.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("110.1"),
            next_trigger_r=3,
            tick_size=Decimal("0.1"),
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("120.1"))
        self.assertEqual(next_take_profit, Decimal("140.1"))
        self.assertEqual(next_trigger_r, 4)

    def test_dynamic_live_stop_can_move_to_break_even_plus_two_taker_fees_at_2r(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120.1"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("100.1"))
        self.assertEqual(next_take_profit, Decimal("130.1"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_break_even_mode_is_mirrored_for_short(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="short",
            current_price=Decimal("79.9"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("110"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("99.9"))
        self.assertEqual(next_take_profit, Decimal("69.9"))
        self.assertEqual(next_trigger_r, 3)

    def test_dynamic_live_break_even_can_disable_fee_offset(self) -> None:
        stop_loss, next_take_profit, next_trigger_r, moved = _advance_dynamic_stop_live(
            direction="long",
            current_price=Decimal("120"),
            entry_price=Decimal("100"),
            risk_per_unit=Decimal("10"),
            current_stop_loss=Decimal("90"),
            next_trigger_r=2,
            tick_size=Decimal("0.1"),
            two_r_break_even=True,
            dynamic_fee_offset_enabled=False,
        )

        self.assertTrue(moved)
        self.assertEqual(stop_loss, Decimal("100"))
        self.assertEqual(next_take_profit, Decimal("130"))
        self.assertEqual(next_trigger_r, 3)

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
            trend_ema_period=55,
            big_ema_period=233,
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
