from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig


class EmaDynamicOrderStrategy:
    name = "ema_dynamic"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(
            config.ema_period,
            config.trend_ema_period,
            config.big_ema_period,
            config.atr_period,
        )
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"已收盘 K 线不足，至少需要 {minimum} 根。",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
                signal_candle_high=None,
                signal_candle_low=None,
            )

        closes = [candle.close for candle in candles]
        ema_values = ema(closes, config.ema_period)
        medium_ema_values = ema(closes, config.trend_ema_period)
        big_ema_values = ema(closes, config.big_ema_period)
        atr_values = atr(candles, config.atr_period)

        current_candle = candles[-1]
        current_ema = ema_values[-1]
        medium_ema = medium_ema_values[-1]
        big_ema = big_ema_values[-1]
        current_atr = atr_values[-1]
        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="最新一根已收盘 K 线的 ATR 尚未准备好。",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        if config.signal_mode == "long_only":
            if current_candle.close <= current_ema:
                return SignalDecision(
                    signal=None,
                    reason="最近一根已收盘 K 线没有站上 EMA 小周期，暂不挂做多回调单。",
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_ema <= medium_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 当前仍在 EMA{config.trend_ema_period} 下方，"
                        "中周期趋势过滤不允许做多。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close <= medium_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"最近一根已收盘 K 线未站上 EMA{config.trend_ema_period}，"
                        "中周期趋势过滤不允许做多。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close <= big_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"最近一根已收盘 K 线未站上 EMA{config.big_ema_period}，"
                        "大周期趋势过滤不允许做多。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            signal = "long"
            reason = (
                f"以上一根已收盘 EMA{config.ema_period} 作为做多回调挂单价，"
                f"且 EMA{config.ema_period} > EMA{config.trend_ema_period}，"
                f"收盘价位于 EMA{config.big_ema_period} 上方。"
            )
        elif config.signal_mode == "short_only":
            if current_candle.close >= current_ema:
                return SignalDecision(
                    signal=None,
                    reason="最近一根已收盘 K 线没有跌破 EMA 小周期，暂不挂做空反弹单。",
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_ema >= medium_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 当前仍在 EMA{config.trend_ema_period} 上方，"
                        "中周期趋势过滤不允许做空。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close >= medium_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"最近一根已收盘 K 线未跌破 EMA{config.trend_ema_period}，"
                        "中周期趋势过滤不允许做空。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close >= big_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"最近一根已收盘 K 线未跌破 EMA{config.big_ema_period}，"
                        "大周期趋势过滤不允许做空。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            signal = "short"
            reason = (
                f"以上一根已收盘 EMA{config.ema_period} 作为做空反弹挂单价，"
                f"且 EMA{config.ema_period} < EMA{config.trend_ema_period}，"
                f"收盘价位于 EMA{config.big_ema_period} 下方。"
            )
        else:
            return SignalDecision(
                signal=None,
                reason="EMA 动态委托策略只支持只做多或只做空。",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )

        return SignalDecision(
            signal=signal,
            reason=reason,
            candle_ts=current_candle.ts,
            entry_reference=current_ema,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
