from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig


class EmaAtrStrategy:
    name = "ema_atr"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(config.ema_period + 2, config.atr_period + 2)
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"Not enough candles yet (need at least {minimum})",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
            )

        closes = [candle.close for candle in candles]
        ema_values = ema(closes, config.ema_period)
        atr_values = atr(candles, config.atr_period)

        previous_candle = candles[-2]
        current_candle = candles[-1]
        previous_ema = ema_values[-2]
        current_ema = ema_values[-1]
        current_atr = atr_values[-1]

        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="ATR is not ready on the latest candle",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_ema,
            )

        long_cross = previous_candle.close <= previous_ema and current_candle.close > current_ema
        short_cross = previous_candle.close >= previous_ema and current_candle.close < current_ema

        if long_cross and config.signal_mode != "short_only":
            return SignalDecision(
                signal="long",
                reason="Price crossed above EMA on the latest closed candle",
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_ema,
            )

        if short_cross and config.signal_mode != "long_only":
            return SignalDecision(
                signal="short",
                reason="Price crossed below EMA on the latest closed candle",
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_ema,
            )

        return SignalDecision(
            signal=None,
            reason=(
                f"close={current_candle.close} ema={current_ema} "
                "and no fresh cross signal was detected"
            ),
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_ema,
        )
