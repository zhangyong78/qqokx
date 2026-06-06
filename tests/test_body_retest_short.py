from __future__ import annotations

from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.strategies.body_retest_short import (
    build_body_retest_short_protection_plan,
    evaluate_body_retest_short_signal,
)


def _candle(ts: int, open_: str, high: str, low: str, close: str) -> Candle:
    return Candle(
        ts=ts,
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("1"),
        confirmed=True,
    )


def _body_retest_config() -> StrategyConfig:
    return StrategyConfig(
        inst_id="BNB-USDT-SWAP",
        bar="1H",
        ema_period=20,
        ema_type="ma",
        trend_ema_period=20,
        trend_ema_type="ma",
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id="body_retest_short",
        risk_amount=Decimal("10"),
        atr_percentile_filter_max=Decimal("1"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        body_retest_breakdown_atr_multiplier=Decimal("0.2"),
        body_retest_retest_atr_multiplier=Decimal("0.3"),
        body_retest_stop_buffer_atr_multiplier=Decimal("0.3"),
        body_retest_body_atr_limit=Decimal("1.0"),
        body_retest_watch_bars=6,
    )


class BodyRetestShortStrategyTest(TestCase):
    def test_evaluate_body_retest_short_signal_triggers_after_breakdown_retest(self) -> None:
        candles = [
            _candle(index, "100", "101", "99", "100")
            for index in range(105)
        ]
        candles[103] = _candle(103, "99.8", "100.0", "98.8", "99.0")
        candles[104] = _candle(104, "99.5", "99.5", "99.0", "99.3")

        line_values = [Decimal("100")] * 105
        line_values[103] = Decimal("99.9")
        line_values[104] = Decimal("99.8")
        atr_values = [Decimal("2")] * 105
        bias = ["short"] * 105

        with (
            patch("okx_quant.strategies.body_retest_short.moving_average", return_value=line_values),
            patch("okx_quant.strategies.body_retest_short.atr", return_value=atr_values),
        ):
            decision = evaluate_body_retest_short_signal(
                candles,
                _body_retest_config(),
                direction_filter_bias=bias,
                price_increment=Decimal("0.1"),
            )

        self.assertEqual(decision.signal, "short")
        self.assertEqual(decision.candle_ts, 104)
        self.assertEqual(decision.entry_reference, Decimal("99.3"))
        self.assertEqual(decision.signal_candle_high, Decimal("99.5"))
        self.assertEqual(decision.signal_candle_low, Decimal("99.0"))
        self.assertIn("body_retest_short_triggered", decision.reason)

    def test_build_body_retest_short_protection_plan_uses_breakdown_risk_distance(self) -> None:
        instrument = Instrument(
            inst_id="BNB-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        protection = build_body_retest_short_protection_plan(
            instrument=instrument,
            config=_body_retest_config(),
            entry_reference=Decimal("100"),
            signal_candle_high=Decimal("101"),
            signal_candle_close=Decimal("100"),
            atr_value=Decimal("2"),
            candle_ts=123456,
            trigger_inst_id="BNB-USDT-SWAP",
        )

        self.assertEqual(protection.direction, "short")
        self.assertEqual(protection.entry_reference, Decimal("100.0"))
        self.assertEqual(protection.stop_loss, Decimal("101.6"))
        self.assertEqual(protection.take_profit, Decimal("96.8"))
        self.assertEqual(protection.atr_value, Decimal("2"))
        self.assertEqual(protection.candle_ts, 123456)
