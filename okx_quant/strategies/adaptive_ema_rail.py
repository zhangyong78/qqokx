from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.models import Candle, SignalDecision, StrategyConfig


ADAPTIVE_RAIL_STATE_SEARCHING = "SEARCHING"
ADAPTIVE_RAIL_STATE_CONFIRMED = "CONFIRMED"
ADAPTIVE_RAIL_STATE_BROKEN = "BROKEN"

ADAPTIVE_RAIL_MIN_SCORE = Decimal("55")
ADAPTIVE_RAIL_ENTRY_MIN_BOUNCES = 2
ADAPTIVE_RAIL_SLOPE_LOOKBACK = 10
ADAPTIVE_RAIL_TREND_STRUCTURE_WINDOW = 8
ADAPTIVE_RAIL_SCORE_LOOKBACK_FALLBACK = 60


@dataclass(frozen=True)
class AdaptiveRailMetrics:
    period: int
    score: Decimal
    touch_count: int
    bounce_count: int
    fake_break_count: int
    hard_break_count: int
    slope_strength: Decimal
    last_touch_age: int | None
    latest_hard_break: bool


@dataclass(frozen=True)
class AdaptiveRailSnapshot:
    state: str
    decision: SignalDecision
    dominant_period: int | None
    dominant_score: Decimal
    metrics: AdaptiveRailMetrics | None = None


def adaptive_rail_candidate_periods(config: StrategyConfig) -> tuple[int, ...]:
    periods = tuple(int(period) for period in config.rail_candidate_ema_periods if int(period) > 0)
    return periods or (5, 8, 13, 21, 34, 55, 89, 144, 233)


def adaptive_rail_minimum_candles(config: StrategyConfig) -> int:
    candidates = adaptive_rail_candidate_periods(config)
    return max(
        max(candidates),
        200,
        config.atr_period,
    ) + max(int(config.rail_score_lookback_bars), ADAPTIVE_RAIL_SCORE_LOOKBACK_FALLBACK)


def evaluate_adaptive_rail_signal(
    candles: list[Candle],
    index: int,
    *,
    ema_by_period: dict[int, list[Decimal]],
    ema200_values: list[Decimal],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
    current_period: int | None = None,
) -> AdaptiveRailSnapshot:
    candle = candles[index]
    current_atr = atr_values[index]
    if current_atr is None or current_atr <= 0:
        return _snapshot_without_signal(
            state=ADAPTIVE_RAIL_STATE_SEARCHING,
            reason="atr_not_ready",
            candle=candle,
        )

    if not _trend_up(candles, index, ema200_values):
        return _snapshot_without_signal(
            state=ADAPTIVE_RAIL_STATE_SEARCHING,
            reason="trend_filter_not_met",
            candle=candle,
            atr_value=current_atr,
        )

    metrics_by_period = [
        _score_period(
            candles,
            index,
            period=period,
            ema_values=ema_by_period[period],
            atr_values=atr_values,
            config=config,
        )
        for period in adaptive_rail_candidate_periods(config)
        if period in ema_by_period
    ]
    qualifying = [item for item in metrics_by_period if _metrics_confirmed(item, config)]
    if not qualifying:
        best = max(metrics_by_period, key=lambda item: item.score, default=None)
        return _snapshot_without_signal(
            state=ADAPTIVE_RAIL_STATE_SEARCHING,
            reason="rail_not_confirmed",
            candle=candle,
            atr_value=current_atr,
            ema_value=ema_by_period[best.period][index] if best is not None else None,
            dominant_period=best.period if best is not None else None,
            dominant_score=best.score if best is not None else Decimal("0"),
            metrics=best,
        )

    selected = max(qualifying, key=lambda item: item.score)
    if current_period is not None:
        current_metrics = next((item for item in qualifying if item.period == current_period), None)
        if current_metrics is not None:
            switch_delta = Decimal(config.rail_switch_min_score_delta)
            if selected.period != current_period and selected.score < current_metrics.score + switch_delta:
                selected = current_metrics

    selected_ema = ema_by_period[selected.period][index]
    if selected.latest_hard_break:
        return _snapshot_without_signal(
            state=ADAPTIVE_RAIL_STATE_BROKEN,
            reason="rail_hard_break",
            candle=candle,
            atr_value=current_atr,
            ema_value=selected_ema,
            dominant_period=selected.period,
            dominant_score=selected.score,
            metrics=selected,
        )

    if selected.bounce_count < ADAPTIVE_RAIL_ENTRY_MIN_BOUNCES:
        return _snapshot_without_signal(
            state=ADAPTIVE_RAIL_STATE_CONFIRMED,
            reason="waiting_for_third_touch",
            candle=candle,
            atr_value=current_atr,
            ema_value=selected_ema,
            dominant_period=selected.period,
            dominant_score=selected.score,
            metrics=selected,
        )

    return AdaptiveRailSnapshot(
        state=ADAPTIVE_RAIL_STATE_CONFIRMED,
        dominant_period=selected.period,
        dominant_score=selected.score,
        metrics=selected,
        decision=SignalDecision(
            signal="long",
            reason=f"adaptive_rail_long_ema{selected.period}_score_{selected.score}",
            candle_ts=candle.ts,
            entry_reference=selected_ema,
            atr_value=current_atr,
            ema_value=selected_ema,
            signal_candle_high=candle.high,
            signal_candle_low=candle.low,
        ),
    )


def is_adaptive_rail_hard_break(
    candle: Candle,
    *,
    ema_value: Decimal,
    atr_value: Decimal | None,
    config: StrategyConfig,
) -> bool:
    if atr_value is None or atr_value <= 0:
        return False
    return candle.close < ema_value - (Decimal(config.rail_break_atr_ratio) * atr_value)


def _snapshot_without_signal(
    *,
    state: str,
    reason: str,
    candle: Candle,
    atr_value: Decimal | None = None,
    ema_value: Decimal | None = None,
    dominant_period: int | None = None,
    dominant_score: Decimal = Decimal("0"),
    metrics: AdaptiveRailMetrics | None = None,
) -> AdaptiveRailSnapshot:
    return AdaptiveRailSnapshot(
        state=state,
        dominant_period=dominant_period,
        dominant_score=dominant_score,
        metrics=metrics,
        decision=SignalDecision(
            signal=None,
            reason=reason,
            candle_ts=candle.ts,
            entry_reference=None,
            atr_value=atr_value,
            ema_value=ema_value,
            signal_candle_high=candle.high,
            signal_candle_low=candle.low,
        ),
    )


def _trend_up(candles: list[Candle], index: int, ema200_values: list[Decimal]) -> bool:
    if index < 200 + ADAPTIVE_RAIL_SLOPE_LOOKBACK:
        return False
    candle = candles[index]
    if candle.close <= ema200_values[index]:
        return False
    if ema200_values[index] <= ema200_values[index - ADAPTIVE_RAIL_SLOPE_LOOKBACK]:
        return False

    window = ADAPTIVE_RAIL_TREND_STRUCTURE_WINDOW
    if index < window * 2:
        return False
    recent = candles[index - window + 1 : index + 1]
    previous = candles[index - (window * 2) + 1 : index - window + 1]
    if not recent or not previous:
        return False
    return max(item.high for item in recent) > max(item.high for item in previous) and min(
        item.low for item in recent
    ) > min(item.low for item in previous)


def _score_period(
    candles: list[Candle],
    index: int,
    *,
    period: int,
    ema_values: list[Decimal],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> AdaptiveRailMetrics:
    lookback = max(int(config.rail_score_lookback_bars), ADAPTIVE_RAIL_SCORE_LOOKBACK_FALLBACK)
    start = max(0, index - lookback + 1)
    touch_count = 0
    bounce_count = 0
    fake_break_count = 0
    hard_break_count = 0
    last_touch_age: int | None = None

    for bar_index in range(start, index + 1):
        atr_value = atr_values[bar_index]
        if atr_value is None or atr_value <= 0:
            continue
        candle = candles[bar_index]
        ema_value = ema_values[bar_index]
        zone = Decimal(config.rail_touch_atr_ratio) * atr_value

        if _is_touch(candle, ema_value, zone):
            touch_count += 1
            last_touch_age = index - bar_index
            if _touch_bounced(candles, bar_index, index, ema_values, atr_values, config):
                bounce_count += 1

        if _is_fake_break(candles, bar_index, index, ema_values, atr_values, config):
            fake_break_count += 1
        if is_adaptive_rail_hard_break(candle, ema_value=ema_value, atr_value=atr_value, config=config):
            hard_break_count += 1

    slope_past_index = max(start, index - ADAPTIVE_RAIL_SLOPE_LOOKBACK)
    current_atr = atr_values[index] or Decimal("0")
    if current_atr > 0:
        slope_strength = (ema_values[index] - ema_values[slope_past_index]) / current_atr
    else:
        slope_strength = Decimal("0")

    score = _respect_score(
        touch_count=touch_count,
        bounce_count=bounce_count,
        fake_break_count=fake_break_count,
        hard_break_count=hard_break_count,
        slope_strength=slope_strength,
        last_touch_age=last_touch_age,
    )
    latest_atr = atr_values[index]
    latest_hard_break = is_adaptive_rail_hard_break(
        candles[index],
        ema_value=ema_values[index],
        atr_value=latest_atr,
        config=config,
    )
    return AdaptiveRailMetrics(
        period=period,
        score=score,
        touch_count=touch_count,
        bounce_count=bounce_count,
        fake_break_count=fake_break_count,
        hard_break_count=hard_break_count,
        slope_strength=slope_strength,
        last_touch_age=last_touch_age,
        latest_hard_break=latest_hard_break,
    )


def _metrics_confirmed(metrics: AdaptiveRailMetrics, config: StrategyConfig) -> bool:
    return (
        metrics.score >= ADAPTIVE_RAIL_MIN_SCORE
        and metrics.touch_count >= int(config.rail_min_touches)
        and metrics.bounce_count >= int(config.rail_min_bounces)
        and metrics.slope_strength > 0
    )


def _respect_score(
    *,
    touch_count: int,
    bounce_count: int,
    fake_break_count: int,
    hard_break_count: int,
    slope_strength: Decimal,
    last_touch_age: int | None,
) -> Decimal:
    touch_score = Decimal(min(touch_count, 5) * 8)
    bounce_score = Decimal(min(bounce_count, 4) * 12)
    slope_score = _clamp(slope_strength, Decimal("0"), Decimal("2")) * Decimal("10")
    recency_score = Decimal("0") if last_touch_age is None else Decimal(max(0, 10 - last_touch_age))
    fake_break_penalty = Decimal(fake_break_count * 6)
    hard_break_penalty = Decimal(hard_break_count * 12)
    score = touch_score + bounce_score + slope_score + recency_score - fake_break_penalty - hard_break_penalty
    return max(score, Decimal("0"))


def _clamp(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    return max(lower, min(upper, value))


def _is_touch(candle: Candle, ema_value: Decimal, zone: Decimal) -> bool:
    return candle.low <= ema_value + zone and candle.close >= ema_value - zone


def _touch_bounced(
    candles: list[Candle],
    touch_index: int,
    current_index: int,
    ema_values: list[Decimal],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> bool:
    confirm_bars = max(int(config.rail_bounce_confirm_bars), 1)
    end_index = touch_index + confirm_bars
    if end_index > current_index:
        return False
    touch_atr = atr_values[touch_index]
    if touch_atr is None or touch_atr <= 0:
        return False
    target = ema_values[touch_index] + (Decimal(config.rail_bounce_atr_ratio) * touch_atr)
    for bar_index in range(touch_index, end_index + 1):
        atr_value = atr_values[bar_index]
        if is_adaptive_rail_hard_break(
            candles[bar_index],
            ema_value=ema_values[bar_index],
            atr_value=atr_value,
            config=config,
        ):
            return False
    return max(candles[bar_index].high for bar_index in range(touch_index, end_index + 1)) >= target


def _is_fake_break(
    candles: list[Candle],
    bar_index: int,
    current_index: int,
    ema_values: list[Decimal],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> bool:
    atr_value = atr_values[bar_index]
    if atr_value is None or atr_value <= 0:
        return False
    candle = candles[bar_index]
    ema_value = ema_values[bar_index]
    zone = Decimal(config.rail_touch_atr_ratio) * atr_value
    if candle.low >= ema_value - zone:
        return False
    if candle.close >= ema_value:
        return True
    next_index = bar_index + 1
    return next_index <= current_index and candles[next_index].close >= ema_values[next_index]
