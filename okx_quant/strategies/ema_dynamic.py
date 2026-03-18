from __future__ import annotations

from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig


class EmaDynamicOrderStrategy:
    name = "ema_dynamic"

    def evaluate(self, candles: list[Candle], config: StrategyConfig) -> SignalDecision:
        minimum = max(config.ema_period, config.atr_period)
        if len(candles) < minimum:
            return SignalDecision(
                signal=None,
                reason=f"已收盘 K 线不足，至少需要 {minimum} 根",
                candle_ts=None,
                entry_reference=None,
                atr_value=None,
                ema_value=None,
            )

        closes = [candle.close for candle in candles]
        ema_values = ema(closes, config.ema_period)
        atr_values = atr(candles, config.atr_period)

        current_candle = candles[-1]
        current_ema = ema_values[-1]
        current_atr = atr_values[-1]
        if current_atr is None:
            return SignalDecision(
                signal=None,
                reason="最新一根已收盘 K 线的 ATR 尚未准备好",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=None,
                ema_value=current_ema,
            )

        if config.signal_mode == "long_only":
            if current_candle.close <= current_ema:
                return SignalDecision(
                    signal=None,
                    reason="最近一根已收盘 K 线没有站上 EMA，暂不挂做多回调单",
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                )
            signal = "long"
            reason = "以上一根已收盘 EMA 作为做多回调挂单价格"
        elif config.signal_mode == "short_only":
            if current_candle.close >= current_ema:
                return SignalDecision(
                    signal=None,
                    reason="最近一根已收盘 K 线没有跌破 EMA，暂不挂做空反弹单",
                    candle_ts=current_candle.ts,
                    entry_reference=None,
                    atr_value=current_atr,
                    ema_value=current_ema,
                )
            signal = "short"
            reason = "以上一根已收盘 EMA 作为做空反弹挂单价格"
        else:
            return SignalDecision(
                signal=None,
                reason="EMA 动态委托策略只支持只做多或只做空",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
            )

        return SignalDecision(
            signal=signal,
            reason=reason,
            candle_ts=current_candle.ts,
            entry_reference=current_ema,
            atr_value=current_atr,
            ema_value=current_ema,
        )
