from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from okx_quant.indicators import ema
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import resolve_dynamic_signal_mode

MtfFilterBias = Literal["long", "short", "neutral"]


@dataclass(frozen=True)
class TimeframeContext:
    entry_inst_id: str
    entry_bar: str
    filter_inst_id: str
    filter_bar: str
    entry_candles: list[Candle]
    filter_candles: list[Candle]


def evaluate_filter_bias(filter_candles: list[Candle], config: StrategyConfig) -> MtfFilterBias:
    confirmed = [candle for candle in filter_candles if candle.confirmed]
    fast_period = int(config.mtf_filter_fast_ema_period)
    slow_period = int(config.mtf_filter_slow_ema_period)
    minimum = max(fast_period, slow_period)
    if fast_period <= 0 or slow_period <= 0 or len(confirmed) < minimum:
        return "neutral"

    closes = [candle.close for candle in confirmed]
    fast = ema(closes, fast_period)[-1]
    slow = ema(closes, slow_period)[-1]
    if fast > slow:
        return "long"
    if fast < slow:
        return "short"
    return "neutral"


def filter_bias_allows_signal(bias: MtfFilterBias, signal: str | None) -> bool:
    if signal == "long":
        return bias == "long"
    if signal == "short":
        return bias == "short"
    return False


class EmaDynamicMultiTimeframeStrategy:
    name = "ema_dynamic_multi_timeframe"

    def __init__(self, entry_strategy: EmaDynamicOrderStrategy | None = None) -> None:
        self._entry_strategy = entry_strategy or EmaDynamicOrderStrategy()

    def evaluate(
        self,
        entry_candles: list[Candle],
        filter_candles: list[Candle],
        config: StrategyConfig,
        *,
        price_increment: Decimal | None = None,
    ) -> SignalDecision:
        effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
        entry_decision = self._entry_strategy.evaluate(
            entry_candles,
            config,
            price_increment=price_increment,
        )
        if entry_decision.signal is None:
            return entry_decision

        bias = evaluate_filter_bias(filter_candles, config)
        if filter_bias_allows_signal(bias, entry_decision.signal):
            return SignalDecision(
                signal=entry_decision.signal,
                reason=(
                    f"{entry_decision.reason} 高周期过滤通过："
                    f"{config.resolved_mtf_filter_bar()} EMA{config.mtf_filter_fast_ema_period}/"
                    f"EMA{config.mtf_filter_slow_ema_period}={_format_bias_label(bias)}。"
                ),
                candle_ts=entry_decision.candle_ts,
                entry_reference=entry_decision.entry_reference,
                atr_value=entry_decision.atr_value,
                ema_value=entry_decision.ema_value,
                signal_candle_high=entry_decision.signal_candle_high,
                signal_candle_low=entry_decision.signal_candle_low,
            )

        blocked_direction = "做多" if effective_signal_mode == "long_only" else "做空"
        return SignalDecision(
            signal=None,
            reason=(
                f"低周期出现{blocked_direction}动态委托条件，但高周期过滤未放行："
                f"{config.resolved_mtf_filter_bar()} EMA{config.mtf_filter_fast_ema_period}/"
                f"EMA{config.mtf_filter_slow_ema_period}={_format_bias_label(bias)}。"
            ),
            candle_ts=entry_decision.candle_ts,
            entry_reference=None,
            atr_value=entry_decision.atr_value,
            ema_value=entry_decision.ema_value,
            signal_candle_high=entry_decision.signal_candle_high,
            signal_candle_low=entry_decision.signal_candle_low,
        )


def _format_bias_label(bias: MtfFilterBias) -> str:
    if bias == "long":
        return "多头"
    if bias == "short":
        return "空头"
    return "中性"
