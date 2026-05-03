from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig


class EmaAtrStrategy:
    name = "ema_atr"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        reference_ema_period = config.resolved_entry_reference_ema_period()
        minimum = max(
            reference_ema_period + 2,
            config.atr_period + 2,
        )
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"Not enough candles yet (need at least {minimum})",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
                signal_candle_high=None,
                signal_candle_low=None,
            )

        closes = [candle.close for candle in candles]
        ema_values = ema(closes, config.ema_period)
        reference_ema_values = ema(closes, reference_ema_period)
        atr_values = atr(candles, config.atr_period)

        previous_candle = candles[-2]
        current_candle = candles[-1]
        previous_reference_ema = reference_ema_values[-2]
        current_reference_ema = reference_ema_values[-1]
        current_atr = atr_values[-1]

        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="ATR is not ready on the latest candle",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        long_cross = previous_candle.close <= previous_reference_ema and current_candle.close > current_reference_ema
        short_cross = previous_candle.close >= previous_reference_ema and current_candle.close < current_reference_ema

        if long_cross and config.signal_mode != "short_only":
            return SignalDecision(
                signal="long",
                reason=(
                    f"Price crossed above reference EMA{reference_ema_period} on latest closed candle"
                ),
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if short_cross and config.signal_mode != "long_only":
            return SignalDecision(
                signal="short",
                reason=(
                    f"Price crossed below reference EMA{reference_ema_period} on latest closed candle"
                ),
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal=None,
            reason=(
                f"close={current_candle.close} reference_ema={current_reference_ema} and no fresh breakout signal"
            ),
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_reference_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
