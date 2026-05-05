from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price


class EmaAtrStrategy:
    name = "ema_atr"

    def evaluate(
        self,
        candles: list[Candle],
        config: StrategyConfig,
        *,
        price_increment: Decimal | None = None,
    ) -> SignalDecision:
        reference_ema_period = config.resolved_entry_reference_ema_period()
        minimum = max(
            reference_ema_period + 2,
            config.atr_period + 2,
            config.ema_period + 2,
            config.trend_ema_period + 2,
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
        ema_small_values = ema(closes, config.ema_period)
        ema_medium_values = ema(closes, config.trend_ema_period)
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

        ema_small = ema_small_values[-1]
        ema_medium = ema_medium_values[-1]
        if config.ema_period == config.trend_ema_period:
            ema_bias_allows_long = True
            ema_bias_allows_short = True
        else:
            ema_bias_allows_long = ema_small > ema_medium
            ema_bias_allows_short = ema_small < ema_medium

        long_breakout = previous_candle.close <= previous_reference_ema and current_candle.close > current_reference_ema
        short_breakdown = (
            previous_candle.close >= previous_reference_ema and current_candle.close < current_reference_ema
        )

        def px(value: Decimal) -> str:
            return format_strategy_reason_price(value, price_increment)

        if long_breakout and config.signal_mode != "short_only":
            if not ema_bias_allows_long:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价向上突破参考EMA{reference_ema_period}，但 EMA{config.ema_period}({px(ema_small)}) "
                        f"未在 EMA{config.trend_ema_period}({px(ema_medium)}) 上方，不满足做多条件"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_reference_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal="long",
                reason=(
                    f"收盘价向上突破参考EMA{reference_ema_period}，且 EMA{config.ema_period} 在 "
                    f"EMA{config.trend_ema_period} 上方，按突破开多"
                ),
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_reference_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if short_breakdown and config.signal_mode != "long_only":
            if not ema_bias_allows_short:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价向下跌破参考EMA{reference_ema_period}，但 EMA{config.ema_period}({px(ema_small)}) "
                        f"未在 EMA{config.trend_ema_period}({px(ema_medium)}) 下方，不满足做空条件"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_reference_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal="short",
                reason=(
                    f"收盘价向下跌破参考EMA{reference_ema_period}，且 EMA{config.ema_period} 在 "
                    f"EMA{config.trend_ema_period} 下方，按跌破开空"
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
                f"close={px(current_candle.close)} reference_ema={px(current_reference_ema)}，无新的突破/跌破信号"
            ),
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_reference_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
