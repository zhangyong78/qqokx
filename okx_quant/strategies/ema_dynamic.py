from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, ema
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
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        minimum = max(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
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
        entry_reference_ema_values = (
            ema_values if entry_reference_ema_period == config.ema_period else ema(closes, entry_reference_ema_period)
        )
        trend_ema_values = ema(closes, config.trend_ema_period)
        atr_values = atr(candles, config.atr_period)

        current_candle = candles[-1]
        current_ema = ema_values[-1]
        current_entry_reference = entry_reference_ema_values[-1]
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

        def px(value: Decimal) -> str:
            return format_strategy_reason_price(value, price_increment)

        if effective_signal_mode == "long_only":
            if current_ema <= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 仍在 EMA{config.trend_ema_period} 下方，"
                        "当前不是有效多头趋势。"
                        f"（快线={px(current_ema)} 慢线={px(trend_ema)} 收盘={px(current_candle.close)}）"
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
                        f"（收盘={px(current_candle.close)} 慢线={px(trend_ema)}）"
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
                f"多头趋势成立，以下一根的回调委托参考 EMA{entry_reference_ema_period} 作为挂单价"
                f"（委托价≈{px(current_entry_reference)}）。"
            )
        elif effective_signal_mode == "short_only":
            if current_ema >= trend_ema:
                return SignalDecision(
                    signal=None,
                    reason=(
                        f"EMA{config.ema_period} 仍在 EMA{config.trend_ema_period} 上方，"
                        "当前不是有效空头趋势。"
                        f"（快线={px(current_ema)} 慢线={px(trend_ema)} 收盘={px(current_candle.close)}）"
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
                        f"（收盘={px(current_candle.close)} 慢线={px(trend_ema)}）"
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
                f"空头趋势成立，以下一根的反弹委托参考 EMA{entry_reference_ema_period} 作为挂单价"
                f"（委托价≈{px(current_entry_reference)}）。"
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
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )
