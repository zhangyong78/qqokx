from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price
from okx_quant.strategy_catalog import is_btc_ema55_slope_short_strategy


def _ema55_slope_negative_entry_bars(config: StrategyConfig) -> int:
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return max(int(config.ema55_slope_negative_entry_bars), 1)
    return 1


def _ema55_slope_ratio_from_series(ema_values: list[Decimal | None], index: int) -> Decimal | None:
    if index <= 0 or index >= len(ema_values):
        return None
    current_ema = ema_values[index]
    previous_ema = ema_values[index - 1]
    if current_ema is None or previous_ema is None or current_ema == 0:
        return None
    return (current_ema - previous_ema) / current_ema


def evaluate_ema55_slope_short_signal(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    price_increment: Decimal | None = None,
) -> SignalDecision:
    line_label = config.ema_label()
    negative_entry_bars = _ema55_slope_negative_entry_bars(config)
    minimum = max(int(config.ema_period), int(config.atr_period), int(config.trend_ema_period)) + 1
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
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    atr_values = atr(candles, int(config.atr_period))

    current_candle = candles[-1]
    current_ema = ema_values[-1]
    current_atr = atr_values[-1]
    threshold = Decimal(str(config.trend_ema_slope_filter_min_ratio))

    if current_ema is None:
        return SignalDecision(
            signal=None,
            reason=f"{line_label} 尚未准备完成。",
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
            reason="ATR 尚未准备完成。",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    current_index = len(ema_values) - 1
    slope_ratio = _ema55_slope_ratio_from_series(ema_values, current_index)

    def px(value: Decimal) -> str:
        return format_strategy_reason_price(value, price_increment)

    if slope_ratio is None:
        return SignalDecision(
            signal=None,
            reason=f"{line_label} 当前值为 0，无法计算斜率比例。",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if not is_btc_ema55_slope_short_strategy(config.strategy_id):
        if slope_ratio <= threshold:
            return SignalDecision(
                signal="short",
                reason=(
                    f"{line_label} 斜率比例={slope_ratio:.6f}，达到开空阈值 {threshold:.6f}，"
                    f"按收盘价 {px(current_candle.close)} 做空。"
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
            reason=f"{line_label} 斜率比例={slope_ratio:.6f}，尚未达到开空阈值 {threshold:.6f}。",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    recent_slope_ratios = [
        _ema55_slope_ratio_from_series(ema_values, index)
        for index in range(current_index - negative_entry_bars + 1, current_index + 1)
    ]
    entry_ready = bool(recent_slope_ratios) and all(item is not None and item <= threshold for item in recent_slope_ratios)

    if entry_ready:
        return SignalDecision(
            signal="short",
            reason=(
                f"{line_label} 最近连续 {negative_entry_bars} 根负斜率满足阈值，"
                f"且当前斜率比例 {slope_ratio:.6f}，达到开空阈值 {threshold:.6f}，"
                f"按收盘价 {px(current_candle.close)} 做空。"
            ),
            candle_ts=current_candle.ts,
            entry_reference=current_candle.close,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    consecutive_negative_bars = 0
    for item in reversed(recent_slope_ratios):
        if item is not None and item <= threshold:
            consecutive_negative_bars += 1
        else:
            break

    return SignalDecision(
        signal=None,
        reason=(
            f"{line_label} 当前仅连续 {consecutive_negative_bars} 根负斜率，"
            f"需连续 {negative_entry_bars} 根且每根都不高于阈值 {threshold:.6f} 才开空。"
        ),
        candle_ts=current_candle.ts,
        entry_reference=None,
        atr_value=current_atr,
        ema_value=current_ema,
        signal_candle_high=current_candle.high,
        signal_candle_low=current_candle.low,
    )


def ema55_slope_short_exit_ready(
    candles: list[Candle],
    config: StrategyConfig,
) -> tuple[bool, Candle | None, Decimal | None, Decimal | None]:
    if not bool(config.ema55_slope_exit_enabled):
        return False, candles[-1] if candles else None, None, None

    minimum = max(int(config.ema_period), int(config.trend_ema_period)) + 1
    if len(candles) < minimum:
        return False, None, None, None

    closes = [candle.close for candle in candles]
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    current_candle = candles[-1]
    current_ema = ema_values[-1]
    slope_ratio = _ema55_slope_ratio_from_series(ema_values, len(ema_values) - 1)
    if current_ema is None:
        return False, current_candle, current_ema, slope_ratio

    previous_ema = ema_values[-2]
    if previous_ema is None:
        return False, current_candle, current_ema, slope_ratio

    slope = current_ema - previous_ema
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return slope >= 0, current_candle, current_ema, slope_ratio
    return slope > 0, current_candle, current_ema, slope_ratio
