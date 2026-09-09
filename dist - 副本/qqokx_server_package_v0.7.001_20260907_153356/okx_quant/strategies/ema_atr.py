from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, moving_average
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
        reference_period = config.resolved_entry_reference_ema_period()
        minimum = max(
            reference_period + 2,
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
        fast_values = moving_average(closes, config.ema_period, config.resolved_ema_type())
        trend_values = moving_average(closes, config.trend_ema_period, config.resolved_trend_ema_type())
        reference_values = (
            fast_values
            if (
                reference_period == config.ema_period
                and config.resolved_entry_reference_ema_type() == config.resolved_ema_type()
            )
            else moving_average(closes, reference_period, config.resolved_entry_reference_ema_type())
        )
        atr_values = atr(candles, config.atr_period)

        previous_candle = candles[-2]
        current_candle = candles[-1]
        previous_reference = reference_values[-2]
        current_reference = reference_values[-1]
        current_atr = atr_values[-1]
        current_fast = fast_values[-1]
        current_trend = trend_values[-1]
        if (
            previous_reference is None
            or current_reference is None
            or current_fast is None
            or current_trend is None
        ):
            return SignalDecision(
                signal=None,
                reason="Moving average values are not ready on the latest candle",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=None,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="ATR is not ready on the latest candle",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_reference,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if (
            config.ema_period == config.trend_ema_period
            and config.resolved_ema_type() == config.resolved_trend_ema_type()
        ):
            bias_allows_long = True
            bias_allows_short = True
        else:
            bias_allows_long = current_fast > current_trend
            bias_allows_short = current_fast < current_trend

        long_breakout = previous_candle.close <= previous_reference and current_candle.close > current_reference
        short_breakdown = previous_candle.close >= previous_reference and current_candle.close < current_reference

        fast_label = config.ema_label()
        trend_label = config.trend_ema_label()
        reference_label = config.entry_reference_line_label()

        def px(value: Decimal) -> str:
            return format_strategy_reason_price(value, price_increment)

        if long_breakout and config.signal_mode != "short_only":
            if not bias_allows_long:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价向上突破参考线 {reference_label}，但 {fast_label}({px(current_fast)}) "
                        f"未在 {trend_label}({px(current_trend)}) 上方，不满足做多条件"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_reference,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal="long",
                reason=f"收盘价向上突破参考线 {reference_label}，且 {fast_label} 在 {trend_label} 上方，按突破开多",
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_reference,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if short_breakdown and config.signal_mode != "long_only":
            if not bias_allows_short:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价向下跌破参考线 {reference_label}，但 {fast_label}({px(current_fast)}) "
                        f"未在 {trend_label}({px(current_trend)}) 下方，不满足做空条件"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_reference,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            return SignalDecision(
                signal="short",
                reason=f"收盘价向下跌破参考线 {reference_label}，且 {fast_label} 在 {trend_label} 下方，按跌破开空",
                candle_ts=current_candle.ts,
                entry_reference=current_candle.close,
                atr_value=current_atr,
                ema_value=current_reference,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal=None,
            reason=f"close={px(current_candle.close)} reference={px(current_reference)}，无新的突破/跌破信号",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_reference,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
