from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.indicators import atr, moving_average
from okx_quant.models import Candle, SignalDecision, StrategyConfig
from okx_quant.pricing import format_strategy_reason_price


TRACKED_PULLBACK_COUNT = 3


@dataclass(frozen=True)
class PullbackCandidate:
    cross_index: int
    signal_index: int
    cross_ts: int
    signal_ts: int
    pullback_index: int
    bars_after_cross: int
    ema15_at_signal: Decimal
    ma50_at_signal: Decimal
    atr_at_signal: Decimal
    pullback_depth_pct: Decimal
    ema15_slope_5: Decimal
    ema15_slope_10: Decimal
    ma50_slope_10: Decimal
    daily_filter_pass: bool


def btc_ema15_ma50_pullback_long_minimum_candles(config: StrategyConfig) -> int:
    return max(
        int(config.ema_period),
        int(config.trend_ema_period),
        int(config.atr_period),
        int(config.resolved_cross_window_bars()),
        10,
    ) + 2


def btc_ema15_ma50_pullback_long_bias_allows_long(
    direction_filter_bias: list[str] | None,
    index: int,
) -> bool:
    if direction_filter_bias is None or index >= len(direction_filter_bias):
        return True
    bias = str(direction_filter_bias[index] or "neutral").strip().lower()
    return bias in {"long", "both"}


def is_cross_up(
    fast_values: list[Decimal | None],
    slow_values: list[Decimal | None],
    index: int,
) -> bool:
    if index <= 0 or index >= len(fast_values) or index >= len(slow_values):
        return False
    fast_prev = fast_values[index - 1]
    slow_prev = slow_values[index - 1]
    fast_current = fast_values[index]
    slow_current = slow_values[index]
    if fast_prev is None or slow_prev is None or fast_current is None or slow_current is None:
        return False
    return fast_prev <= slow_prev and fast_current > slow_current


def _slope_ratio(values: list[Decimal | None], index: int, lookback: int) -> Decimal:
    if lookback <= 0 or index - lookback < 0:
        return Decimal("0")
    current = values[index]
    previous = values[index - lookback]
    if current is None or previous is None or current == 0:
        return Decimal("0")
    return (current - previous) / current


def scan_btc_ema15_ma50_pullback_long_candidates(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    direction_filter_bias: list[str] | None = None,
) -> list[PullbackCandidate]:
    minimum = btc_ema15_ma50_pullback_long_minimum_candles(config)
    if len(candles) < minimum:
        return []

    closes = [candle.close for candle in candles]
    fast_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
    slow_values = moving_average(closes, int(config.trend_ema_period), config.resolved_trend_ema_type())
    atr_values = atr(candles, int(config.atr_period))
    watch_bars = config.resolved_cross_window_bars()

    candidates: list[PullbackCandidate] = []
    active_cross_index: int | None = None
    active_cross_ts: int | None = None
    active_pullback_count = 0

    for index in range(1, len(candles)):
        fast_value = fast_values[index]
        slow_value = slow_values[index]
        atr_value = atr_values[index] if index < len(atr_values) else None
        if fast_value is None or slow_value is None or atr_value is None or atr_value <= 0:
            continue

        if is_cross_up(fast_values, slow_values, index):
            active_cross_index = index
            active_cross_ts = candles[index].ts
            active_pullback_count = 0
            continue

        if active_cross_index is None or active_cross_ts is None:
            continue

        if fast_value <= slow_value:
            active_cross_index = None
            active_cross_ts = None
            active_pullback_count = 0
            continue

        bars_after_cross = index - active_cross_index
        if bars_after_cross <= 0:
            continue
        if bars_after_cross > watch_bars:
            active_cross_index = None
            active_cross_ts = None
            active_pullback_count = 0
            continue

        candle = candles[index]
        if candle.low > fast_value or candle.close <= fast_value:
            continue

        active_pullback_count += 1
        daily_filter_pass = btc_ema15_ma50_pullback_long_bias_allows_long(direction_filter_bias, index)
        pullback_depth_pct = (
            ((fast_value - candle.low) / fast_value) * Decimal("100")
            if candle.low < fast_value and fast_value > 0
            else Decimal("0")
        )
        candidates.append(
            PullbackCandidate(
                cross_index=active_cross_index,
                signal_index=index,
                cross_ts=active_cross_ts,
                signal_ts=candle.ts,
                pullback_index=active_pullback_count,
                bars_after_cross=bars_after_cross,
                ema15_at_signal=fast_value,
                ma50_at_signal=slow_value,
                atr_at_signal=atr_value,
                pullback_depth_pct=pullback_depth_pct,
                ema15_slope_5=_slope_ratio(fast_values, index, 5),
                ema15_slope_10=_slope_ratio(fast_values, index, 10),
                ma50_slope_10=_slope_ratio(slow_values, index, 10),
                daily_filter_pass=daily_filter_pass,
            )
        )

    return candidates


def evaluate_btc_ema15_ma50_pullback_long_signal(
    candles: list[Candle],
    config: StrategyConfig,
    *,
    direction_filter_bias: list[str] | None = None,
    price_increment: Decimal | None = None,
) -> SignalDecision:
    minimum = btc_ema15_ma50_pullback_long_minimum_candles(config)
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

    latest_candle = candles[-1]
    candidates = scan_btc_ema15_ma50_pullback_long_candidates(
        candles,
        config,
        direction_filter_bias=direction_filter_bias,
    )
    latest_candidate = next((item for item in reversed(candidates) if item.signal_index == len(candles) - 1), None)

    def px(value: Decimal) -> str:
        return format_strategy_reason_price(value, price_increment)

    if latest_candidate is None:
        closes = [candle.close for candle in candles]
        fast_values = moving_average(closes, int(config.ema_period), config.resolved_ema_type())
        latest_fast = fast_values[-1]
        return SignalDecision(
            signal=None,
            reason="btc_ema15_ma50_pullback_long_not_ready",
            candle_ts=latest_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=latest_fast,
            signal_candle_high=latest_candle.high,
            signal_candle_low=latest_candle.low,
        )

    if latest_candidate.pullback_index > config.resolved_max_pullback_index():
        return SignalDecision(
            signal=None,
            reason=f"pullback_index_exceeds_limit_{latest_candidate.pullback_index}",
            candle_ts=latest_candidate.signal_ts,
            entry_reference=None,
            atr_value=latest_candidate.atr_at_signal,
            ema_value=latest_candidate.ema15_at_signal,
            signal_candle_high=latest_candle.high,
            signal_candle_low=latest_candle.low,
        )

    if not latest_candidate.daily_filter_pass:
        return SignalDecision(
            signal=None,
            reason="daily_filter_blocks_long",
            candle_ts=latest_candidate.signal_ts,
            entry_reference=None,
            atr_value=latest_candidate.atr_at_signal,
            ema_value=latest_candidate.ema15_at_signal,
            signal_candle_high=latest_candle.high,
            signal_candle_low=latest_candle.low,
        )

    return SignalDecision(
        signal="long",
        reason=(
            f"crossup_pullback_long_ready: close={px(latest_candle.close)} "
            f"ema15={px(latest_candidate.ema15_at_signal)} ma50={px(latest_candidate.ma50_at_signal)} "
            f"pullback_index={latest_candidate.pullback_index} bars_after_cross={latest_candidate.bars_after_cross}"
        ),
        candle_ts=latest_candidate.signal_ts,
        entry_reference=latest_candle.close,
        atr_value=latest_candidate.atr_at_signal,
        ema_value=latest_candidate.ema15_at_signal,
        signal_candle_high=latest_candle.high,
        signal_candle_low=latest_candle.low,
    )
