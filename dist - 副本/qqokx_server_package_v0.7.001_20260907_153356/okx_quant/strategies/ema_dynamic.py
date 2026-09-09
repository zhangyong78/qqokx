from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, linear_regression_slope, moving_average
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price
from okx_quant.strategy_catalog import resolve_dynamic_signal_mode


class EmaDynamicOrderStrategy:
    name = "ema_dynamic"

    def evaluate(
        self,
        candles: list[Candle],
        config: StrategyConfig,
        *,
        price_increment: Decimal | None = None,
    ) -> SignalDecision:
        entry_reference_period = config.resolved_entry_reference_ema_period()
        effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
        trend_slope_filter_enabled = bool(config.trend_ema_slope_filter_enabled) and effective_signal_mode == "long_only"
        if effective_signal_mode == "short_only":
            trend_slope_filter_enabled = bool(config.trend_ema_slope_filter_enabled)
        trend_slope_lookback = max(2, int(config.trend_ema_slope_filter_lookback_bars))
        trend_slope_min_ratio = Decimal(str(config.trend_ema_slope_filter_min_ratio))
        minimum = max(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_period,
        )
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"已收盘K线不足，至少需要 {minimum} 根。",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
                signal_candle_high=None,
                signal_candle_low=None,
            )

        closes = [candle.close for candle in candles]
        fast_values = moving_average(closes, config.ema_period, config.resolved_ema_type())
        entry_reference_values = (
            fast_values
            if (
                entry_reference_period == config.ema_period
                and config.resolved_entry_reference_ema_type() == config.resolved_ema_type()
            )
            else moving_average(closes, entry_reference_period, config.resolved_entry_reference_ema_type())
        )
        trend_values = moving_average(closes, config.trend_ema_period, config.resolved_trend_ema_type())
        atr_values = atr(candles, config.atr_period)

        current_candle = candles[-1]
        current_fast = fast_values[-1]
        current_entry_reference = entry_reference_values[-1]
        current_trend = trend_values[-1]
        trend_window = trend_values[-trend_slope_lookback:] if trend_slope_filter_enabled else []
        trend_window_ready = bool(trend_window) and all(value is not None for value in trend_window)
        trend_slope = (
            linear_regression_slope([value for value in trend_window if value is not None])
            if trend_window_ready
            else None
        )
        trend_slope_ratio = (
            trend_slope / current_trend
            if trend_slope is not None and current_trend is not None and current_trend != 0
            else None
        )
        current_atr = atr_values[-1]
        if (
            current_fast is None
            or current_entry_reference is None
            or current_trend is None
        ):
            return SignalDecision(
                signal=None,
                reason="均线数据尚未准备好，请等待更多已收盘K线。",
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
                reason="最新一根已收盘K线的ATR尚未准备好。",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_fast,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        fast_label = config.ema_label()
        trend_label = config.trend_ema_label()
        reference_label = config.entry_reference_line_label()

        def px(value: Decimal) -> str:
            return format_strategy_reason_price(value, price_increment)

        if effective_signal_mode == "long_only":
            if current_fast <= current_trend:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"{fast_label} 仍在 {trend_label} 下方，当前不是有效多头趋势。"
                        f"（快线={px(current_fast)} 慢线={px(current_trend)} 收盘={px(current_candle.close)}）"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close <= current_trend:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价仍在 {trend_label} 下方，当前不是有效多头趋势。"
                        f"（收盘={px(current_candle.close)} 慢线={px(current_trend)}）"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if (
                trend_slope_filter_enabled
                and trend_slope_ratio is not None
                and trend_slope_ratio < trend_slope_min_ratio
            ):
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"{trend_label} regression slope filter blocks the long entry "
                        f"(lookback={trend_slope_lookback} slope_ratio={trend_slope_ratio:.6f} threshold={trend_slope_min_ratio:.6f})."
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            signal = "long"
            reason = (
                f"多头趋势成立，以下一根的回调委托参考 {reference_label} 作为挂单价"
                f"（委托价≈{px(current_entry_reference)}）。"
            )
        elif effective_signal_mode == "short_only":
            if current_fast >= current_trend:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"{fast_label} 仍在 {trend_label} 上方，当前不是有效空头趋势。"
                        f"（快线={px(current_fast)} 慢线={px(current_trend)} 收盘={px(current_candle.close)}）"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close >= current_trend:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价仍在 {trend_label} 上方，当前不是有效空头趋势。"
                        f"（收盘={px(current_candle.close)} 慢线={px(current_trend)}）"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if (
                trend_slope_filter_enabled
                and trend_slope_ratio is not None
                and trend_slope_ratio > abs(trend_slope_min_ratio)
            ):
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"{trend_label} regression slope filter blocks the short entry "
                        f"(lookback={trend_slope_lookback} slope_ratio={trend_slope_ratio:.6f} threshold={abs(trend_slope_min_ratio):.6f})."
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_fast,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            signal = "short"
            reason = (
                f"空头趋势成立，以下一根的反弹委托参考 {reference_label} 作为挂单价"
                f"（委托价≈{px(current_entry_reference)}）。"
            )
        else:
            return SignalDecision(
                signal=None,
                reason="动态委托只支持单向运行，请选择只做多或只做空。",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_fast,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        return SignalDecision(
            signal=signal,
            reason=reason,
            candle_ts=current_candle.ts,
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_fast,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
