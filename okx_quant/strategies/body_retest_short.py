from __future__ import annotations

import math
from decimal import Decimal

from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, Instrument, ProtectionPlan, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price, snap_to_increment
from okx_quant.protection_validation import validate_protection_prices


BODY_RETEST_ATR_PERCENTILE_LOOKBACK = 100
BODY_RETEST_MIN_RISK_ATR_FLOOR = Decimal("0.5")
BODY_RETEST_RECLAIM_RATIO = Decimal("0.5")


def body_retest_short_minimum_candles(config: StrategyConfig) -> int:
    return max(
        int(config.ema_period),
        int(config.trend_ema_period),
        int(config.atr_period),
        BODY_RETEST_ATR_PERCENTILE_LOOKBACK,
        max(int(config.body_retest_watch_bars), 1),
    ) + 2


def rolling_body_retest_percentile(
    values: list[Decimal | None],
    lookback: int = BODY_RETEST_ATR_PERCENTILE_LOOKBACK,
) -> list[Decimal | None]:
    out: list[Decimal | None] = [None] * len(values)
    normalized_lookback = max(int(lookback), 1)
    for index in range(len(values)):
        if index + 1 < normalized_lookback:
            continue
        window = values[index + 1 - normalized_lookback : index + 1]
        if any(item is None for item in window):
            continue
        current = window[-1]
        if current is None:
            continue
        rank = sum(1 for item in window if item is not None and item <= current)
        out[index] = Decimal(rank) / Decimal(normalized_lookback)
    return out


def body_retest_short_bias_allows_short(direction_filter_bias: list[str] | None, index: int) -> bool:
    if direction_filter_bias is None or index >= len(direction_filter_bias):
        return True
    bias = str(direction_filter_bias[index] or "neutral").strip().lower()
    return bias in {"short", "both"}


def evaluate_body_retest_short_signal(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    direction_filter_bias: list[str] | None = None,
    price_increment: Decimal | None = None,
) -> SignalDecision:
    minimum = body_retest_short_minimum_candles(config)
    if len(candles) < minimum:
        return SignalDecision(
            signal=None,
            reason=f"need_at_least_{minimum}_confirmed_candles",
            candle_ts=None,
            entry_reference=None,
            atr_value=None,
            ema_value=None,
            signal_candle_high=None,
            signal_candle_low=None,
        )

    closes = [candle.close for candle in candles]
    line_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    atr_values = atr(candles, int(config.atr_period))
    atr_percentiles = rolling_body_retest_percentile(atr_values)
    slope_threshold = Decimal(str(config.trend_ema_slope_filter_min_ratio))
    breakdown_mult = Decimal(str(config.body_retest_breakdown_atr_multiplier))
    retest_mult = Decimal(str(config.body_retest_retest_atr_multiplier))
    body_atr_limit = Decimal(str(config.body_retest_body_atr_limit))
    watch_bars = max(int(config.body_retest_watch_bars), 1)
    atr_percentile_limit = Decimal(str(config.atr_percentile_filter_max))

    pending_index: int | None = None
    pending_reclaim_close: Decimal | None = None
    latest_index = len(candles) - 1
    latest_candle = candles[latest_index]

    def px(value: Decimal) -> str:
        return format_strategy_reason_price(value, price_increment)

    start_index = max(BODY_RETEST_ATR_PERCENTILE_LOOKBACK, 60)
    for index in range(start_index, len(candles)):
        candle = candles[index]
        line_value = line_values[index]
        prev_line = line_values[index - 1] if index > 0 else None
        atr_value = atr_values[index] if index < len(atr_values) else None
        atr_pct = atr_percentiles[index] if index < len(atr_percentiles) else None
        if line_value is None or prev_line is None or atr_value is None or atr_value <= 0 or atr_pct is None:
            continue
        slope_ratio = (line_value - prev_line) / line_value if line_value != 0 else None
        if slope_ratio is None or not math.isfinite(float(slope_ratio)):
            continue

        if pending_index is not None and pending_reclaim_close is not None:
            age = index - pending_index
            if age > watch_bars:
                pending_index = None
                pending_reclaim_close = None
            else:
                bearish_close = candle.close < candle.open
                near_line = candle.high >= (line_value - retest_mult * atr_value)
                still_below = candle.close < line_value
                reclaim_ok = candle.close <= pending_reclaim_close
                bias_ok = body_retest_short_bias_allows_short(direction_filter_bias, index)
                if near_line and still_below and bearish_close and reclaim_ok and bias_ok:
                    if index == latest_index:
                        return SignalDecision(
                            signal="short",
                            reason=(
                                f"body_retest_short_triggered: close={px(candle.close)} "
                                f"line={px(line_value)} slope={slope_ratio:.6f} "
                                f"threshold={slope_threshold:.6f} atr_pct={atr_pct:.2%}"
                            ),
                            candle_ts=candle.ts,
                            entry_reference=candle.close,
                            atr_value=atr_value,
                            ema_value=line_value,
                            signal_candle_high=candle.high,
                            signal_candle_low=candle.low,
                        )
                    pending_index = None
                    pending_reclaim_close = None
                    continue

        if pending_index is not None:
            continue
        if slope_ratio > slope_threshold or atr_pct > atr_percentile_limit:
            continue
        if candle.close >= line_value - breakdown_mult * atr_value or candle.close >= candle.open:
            continue
        if not body_retest_short_bias_allows_short(direction_filter_bias, index):
            continue
        body_size = abs(candle.open - candle.close)
        if (body_size / atr_value) > body_atr_limit:
            continue
        pending_index = index
        pending_reclaim_close = candle.close + (candle.open - candle.close) * BODY_RETEST_RECLAIM_RATIO

    latest_line = line_values[-1]
    latest_atr = atr_values[-1] if atr_values else None
    if pending_index is not None:
        pending_age = latest_index - pending_index
        return SignalDecision(
            signal=None,
            reason=f"pending_body_retest_setup_remaining_bars={max(watch_bars - pending_age, 0)}",
            candle_ts=latest_candle.ts,
            entry_reference=None,
            atr_value=latest_atr,
            ema_value=latest_line,
            signal_candle_high=latest_candle.high,
            signal_candle_low=latest_candle.low,
        )
    return SignalDecision(
        signal=None,
        reason="body_retest_short_not_ready",
        candle_ts=latest_candle.ts,
        entry_reference=None,
        atr_value=latest_atr,
        ema_value=latest_line,
        signal_candle_high=latest_candle.high,
        signal_candle_low=latest_candle.low,
    )


def body_retest_short_initial_stop(
    *,
    candle_high: Decimal,
    candle_close: Decimal,
    atr_value: Decimal,
    config: StrategyConfig,
) -> Decimal:
    risk_distance = max(
        (candle_high + Decimal(str(config.body_retest_stop_buffer_atr_multiplier)) * atr_value) - candle_close,
        atr_value * BODY_RETEST_MIN_RISK_ATR_FLOOR,
    )
    return candle_close + risk_distance


def build_body_retest_short_protection_plan(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    entry_reference: Decimal,
    signal_candle_high: Decimal,
    signal_candle_close: Decimal,
    atr_value: Decimal,
    candle_ts: int,
    trigger_inst_id: str,
) -> ProtectionPlan:
    if atr_value <= 0:
        raise RuntimeError("ATR must be greater than 0 for body retest short protection planning")
    reference_price = snap_to_increment(entry_reference, instrument.tick_size, "nearest")
    stop_raw = body_retest_short_initial_stop(
        candle_high=signal_candle_high,
        candle_close=signal_candle_close,
        atr_value=atr_value,
        config=config,
    )
    risk_distance = max(stop_raw - signal_candle_close, atr_value * BODY_RETEST_MIN_RISK_ATR_FLOOR)
    stop_loss = snap_to_increment(reference_price + risk_distance, instrument.tick_size, "down")
    reward_multiple = (
        Decimal(str(config.atr_take_multiplier)) / Decimal(str(config.atr_stop_multiplier))
        if config.atr_stop_multiplier > 0
        else Decimal("2")
    )
    take_profit = snap_to_increment(reference_price - (risk_distance * reward_multiple), instrument.tick_size, "up")
    validate_protection_prices(
        direction="short",
        entry_reference=reference_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )
    return ProtectionPlan(
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=config.tp_sl_trigger_type,
        take_profit=take_profit,
        stop_loss=stop_loss,
        entry_reference=reference_price,
        atr_value=atr_value,
        direction="short",
        candle_ts=candle_ts,
    )
