from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price
from okx_quant.strategy_catalog import is_btc_ema55_slope_short_strategy


def evaluate_ema55_slope_short_signal(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    price_increment: Decimal | None = None,
) -> SignalDecision:
    line_label = config.ema_label()
    minimum = max(int(config.ema_period), int(config.atr_period), int(config.trend_ema_period)) + (
        2 if is_btc_ema55_slope_short_strategy(config.strategy_id) else 1
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
    ema_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    atr_values = atr(candles, int(config.atr_period))

    current_candle = candles[-1]
    current_ema = ema_values[-1]
    previous_ema = ema_values[-2]
    pre_previous_ema = ema_values[-3] if len(ema_values) >= 3 else None
    current_atr = atr_values[-1]
    threshold = Decimal(str(config.trend_ema_slope_filter_min_ratio))

    if current_ema is None or previous_ema is None:
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

    slope = current_ema - previous_ema
    slope_ratio = slope / current_ema if current_ema != 0 else None
    previous_slope_ratio = None
    if pre_previous_ema is not None and previous_ema != 0:
        previous_slope_ratio = (previous_ema - pre_previous_ema) / previous_ema

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

    entry_ready = slope_ratio <= threshold
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        entry_ready = entry_ready and previous_slope_ratio is not None and previous_slope_ratio >= 0

    if entry_ready:
        reason = (
            f"{line_label} 此前处于上升/走平，当前斜率比例由非负转为 {slope_ratio:.6f}，"
            f"已跌破开空阈值 {threshold:.6f}，按收盘价 {px(current_candle.close)} 做空。"
            if is_btc_ema55_slope_short_strategy(config.strategy_id)
            else (
                f"{line_label} 斜率比例={slope_ratio:.6f}，达到开空阈值 {threshold:.6f}，"
                f"按收盘价 {px(current_candle.close)} 做空。"
            )
        )
        return SignalDecision(
            signal="short",
            reason=reason,
            candle_ts=current_candle.ts,
            entry_reference=current_candle.close,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if is_btc_ema55_slope_short_strategy(config.strategy_id) and slope_ratio <= threshold:
        return SignalDecision(
            signal=None,
            reason=(
                f"{line_label} 当前斜率比例={slope_ratio:.6f} 已低于阈值 {threshold:.6f}，"
                "但上一根均线斜率已为负，当前不属于首次转负开空。"
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
        reason=f"{line_label} 斜率比例={slope_ratio:.6f}，尚未达到开空阈值 {threshold:.6f}。",
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
    current_ema = ema_values[-1]
    previous_ema = ema_values[-2]
    current_candle = candles[-1]
    if current_ema is None or previous_ema is None:
        return False, current_candle, current_ema, None

    slope = current_ema - previous_ema
    slope_ratio = slope / current_ema if current_ema != 0 else None
    if is_btc_ema55_slope_short_strategy(config.strategy_id):
        return slope >= 0, current_candle, current_ema, slope_ratio
    return slope > 0, current_candle, current_ema, slope_ratio
