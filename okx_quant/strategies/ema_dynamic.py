from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.strategy_catalog import resolve_dynamic_signal_mode


class EmaDynamicOrderStrategy:
    name = "ema_dynamic"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(config.ema_period, config.trend_ema_period, config.atr_period)
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
        trend_ema_values = ema(closes, config.trend_ema_period)
        atr_values = atr(candles, config.atr_period)

        current_candle = candles[-1]
        current_ema = ema_values[-1]
        trend_ema = trend_ema_values[-1]
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

        effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)

        if effective_signal_mode == "long_only":
            if current_ema <= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 仍在 EMA{config.trend_ema_period} 下方，"
                        "当前不是有效多头趋势。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close <= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价仍在 EMA{config.trend_ema_period} 下方，"
                        "当前不是有效多头趋势。"
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
                f"多头趋势成立，以 EMA{config.ema_period} 作为下一根回调委托价。"
            )
        elif effective_signal_mode == "short_only":
            if current_ema >= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 仍在 EMA{config.trend_ema_period} 上方，"
                        "当前不是有效空头趋势。"
                    ),
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                    signal_candle_high=current_candle.high,
                    signal_candle_low=current_candle.low,
                )
            if current_candle.close >= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"收盘价仍在 EMA{config.trend_ema_period} 上方，"
                        "当前不是有效空头趋势。"
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
                f"空头趋势成立，以 EMA{config.ema_period} 作为下一根反弹委托价。"
            )
        else:
            return SignalDecision(
                signal=None,
                reason="EMA 动态委托只支持单向运行，请选择只做多或只做空。",
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
