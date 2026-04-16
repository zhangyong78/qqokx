from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.strategy_catalog import STRATEGY_SLOT_LONG_ID, STRATEGY_SLOT_SHORT_ID


class EmaSlotHandoffStrategy:
    name = "ema_slot_handoff"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(
            config.ema_period + 2,
            config.trend_ema_period + 1,
            config.atr_period + 1,
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
        trend_ema_values = ema(closes, config.trend_ema_period)
        atr_values = atr(candles, config.atr_period)

        previous_candle = candles[-2]
        current_candle = candles[-1]
        previous_ema = ema_values[-2]
        current_ema = ema_values[-1]
        trend_ema = trend_ema_values[-1]
        current_atr = atr_values[-1]

        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="ATR is not ready on the latest candle",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        long_cross = previous_candle.close <= previous_ema and current_candle.close > current_ema
        short_cross = previous_candle.close >= previous_ema and current_candle.close < current_ema

        if config.strategy_id == STRATEGY_SLOT_LONG_ID:
            if long_cross and current_candle.close > trend_ema:
                return SignalDecision(
                    signal="long",
                    reason=(
                        f"Price crossed above EMA{config.ema_period} and closed above "
                        f"EMA{config.trend_ema_period}"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=current_candle.close,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal=None,
                reason=(
                    f"close={current_candle.close} fast={current_ema} trend={trend_ema} "
                    "and no fresh long cross was detected"
                ),
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if config.strategy_id == STRATEGY_SLOT_SHORT_ID:
            if short_cross and current_candle.close < trend_ema:
                return SignalDecision(
                    signal="short",
                    reason=(
                        f"Price crossed below EMA{config.ema_period} and closed below "
                        f"EMA{config.trend_ema_period}"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=current_candle.close,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal=None,
                reason=(
                    f"close={current_candle.close} fast={current_ema} trend={trend_ema} "
                    "and no fresh short cross was detected"
                ),
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        return SignalDecision(
            signal=None,
            reason=f"Unsupported slot handoff strategy id: {config.strategy_id}",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    def signal_invalidated(self, candles: list[Candle], config: StrategyConfig, signal: str) -> tuple[bool, Candle, str]:
        minimum = max(config.ema_period, 1)
        if len(candles) < minimum:
            raise ValueError("Not enough candles to evaluate invalidation")
        closes = [candle.close for candle in candles]
        ema_values = ema(closes, config.ema_period)
        current_candle = candles[-1]
        current_ema = ema_values[-1]
        if signal == "long":
            invalidated = current_candle.close < current_ema
            reason = f"close fell back below EMA{config.ema_period}"
        else:
            invalidated = current_candle.close > current_ema
            reason = f"close climbed back above EMA{config.ema_period}"
        return invalidated, current_candle, reason
