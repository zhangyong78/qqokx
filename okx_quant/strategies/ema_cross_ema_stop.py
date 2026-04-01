from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig


class EmaCrossEmaStopStrategy:
    name = "ema5_ema8_cross_stop"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(config.ema_period, config.trend_ema_period) + 1
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"已收盘 K 线不足，至少需要 {minimum} 根",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
                signal_candle_high=None,
                signal_candle_low=None,
            )

        current_candle, previous_fast, previous_slow, current_fast, current_slow = self._latest_snapshot(candles, config)
        long_cross = previous_fast <= previous_slow and current_fast > current_slow
        short_cross = previous_fast >= previous_slow and current_fast < current_slow

        if long_cross and config.signal_mode != "short_only":
            return SignalDecision(
                signal="long",
                reason=(
                    f"EMA{config.ema_period} 金叉 EMA{config.trend_ema_period}，"
                    f"按最新收盘价开多，EMA{config.trend_ema_period} 作为动态止损线"
                ),
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=None,
                ema_value=current_slow,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if short_cross and config.signal_mode != "long_only":
            return SignalDecision(
                signal="short",
                reason=(
                    f"EMA{config.ema_period} 死叉 EMA{config.trend_ema_period}，"
                    f"按最新收盘价开空，EMA{config.trend_ema_period} 作为动态止损线"
                ),
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=None,
                ema_value=current_slow,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        return SignalDecision(
            signal=None,
            reason=(
                f"当前未出现 EMA{config.ema_period}/EMA{config.trend_ema_period} 金叉死叉，"
                f"上一根快慢线={previous_fast}/{previous_slow}，当前快慢线={current_fast}/{current_slow}"
            ),
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=current_slow,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    def latest_stop_line(self, candles: list[Candle], config: StrategyConfig) -> tuple[Candle, Decimal]:
        current_candle, _, _, _, current_slow = self._latest_snapshot(candles, config)
        return current_candle, current_slow

    def stop_triggered(self, candles: list[Candle], config: StrategyConfig, signal: str) -> tuple[bool, Candle, Decimal]:
        current_candle, stop_line = self.latest_stop_line(candles, config)
        if signal == "long":
            return current_candle.close < stop_line, current_candle, stop_line
        return current_candle.close > stop_line, current_candle, stop_line

    def _latest_snapshot(
        self,
        candles: list[Candle],
        config: StrategyConfig,
    ) -> tuple[Candle, Decimal, Decimal, Decimal, Decimal]:
        closes = [candle.close for candle in candles]
        fast_values = ema(closes, config.ema_period)
        slow_values = ema(closes, config.trend_ema_period)
        return candles[-1], fast_values[-2], slow_values[-2], fast_values[-1], slow_values[-1]
