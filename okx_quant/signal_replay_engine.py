from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.indicators import atr, ema, macd, sma
from okx_quant.models import Candle


ZERO = Decimal("0")
ONE_HOUR_MS = 3_600_000


@dataclass(frozen=True)
class SignalReplayConfig:
    ema_fast_period: int = 21
    ema_slow_period: int = 55
    volume_ma_period: int = 20
    atr_period: int = 14
    macd_fast_period: int = 12
    macd_slow_period: int = 26
    macd_signal_period: int = 9
    bias_min_pct: Decimal = Decimal("-1.5")
    bias_max_pct: Decimal = Decimal("2.8")
    near_ema_max_pct: Decimal = Decimal("0.6")
    volume_multiplier: Decimal = Decimal("1.2")
    atr_min_pct: Decimal = Decimal("0.8")
    enable_trend_filter: bool = True
    enable_pullback_trigger: bool = True
    enable_macd_filter: bool = True
    enable_volume_filter: bool = True
    enable_bias_filter: bool = True
    enable_near_ema_filter: bool = False
    enable_atr_filter: bool = False
    include_long: bool = True
    include_short: bool = True
    confirmed_only: bool = True
    enable_pattern_signals: bool = True
    enable_big_bullish: bool = True
    enable_big_bearish: bool = True
    enable_long_upper_shadow: bool = True
    enable_long_lower_shadow: bool = True
    enable_false_breakdown: bool = True
    enable_false_breakout: bool = True
    enable_inside_bar: bool = True
    enable_top_fractal: bool = True
    enable_bottom_fractal: bool = True
    enable_large_move_gate: bool = True
    large_move_mode: str = "any"
    mean_body_period: int = 20
    mean_body_multiplier: Decimal = Decimal("1.8")
    large_move_atr_period: int = 14
    large_move_atr_multiplier: Decimal = Decimal("1.2")
    body_ratio_threshold: Decimal = Decimal("0.6")
    fixed_body_threshold: Decimal = Decimal("0")
    enable_large_move_mean: bool = True
    enable_large_move_atr: bool = True
    enable_large_move_body_ratio: bool = False
    enable_large_move_fixed: bool = False
    shadow_body_multiplier: Decimal = Decimal("2")
    small_body_ratio: Decimal = Decimal("0.6")
    double_large_move_mode: str = "any"
    triple_large_move_mode: str = "any"
    fractal_trend_lookback: int = 5
    fractal_trend_min_bars: int = 3
    false_break_reference_lookback: int = 6
    false_break_min_pct: Decimal = Decimal("0.05")
    false_break_atr_multiplier: Decimal = Decimal("0.1")
    false_break_reclaim_position: Decimal = Decimal("0.6")


@dataclass(frozen=True)
class SignalValidation:
    return_4h_pct: Decimal | None
    return_12h_pct: Decimal | None
    return_24h_pct: Decimal | None
    max_favorable_excursion_pct: Decimal | None
    max_adverse_excursion_pct: Decimal | None


@dataclass(frozen=True)
class SignalReplayPoint:
    index: int
    ts: int
    direction: str
    score: int
    setup: str
    reason: str
    validation: SignalValidation
    pattern_id: str = "indicator_combo"
    pattern_name: str = "指标组合"
    candle_count: int = 1
    large_move_rules: tuple[str, ...] = ()


@dataclass(frozen=True)
class SignalReplaySummary:
    total: int
    long_count: int
    short_count: int
    completed_24h: int
    hit_rate_24h: Decimal | None
    avg_return_24h_pct: Decimal | None


@dataclass(frozen=True)
class SignalReplayDataset:
    candles: tuple[Candle, ...]
    ema_fast: tuple[Decimal, ...]
    ema_slow: tuple[Decimal, ...]
    macd_line: tuple[Decimal, ...]
    macd_signal: tuple[Decimal, ...]
    macd_histogram: tuple[Decimal, ...]
    volume_ma: tuple[Decimal | None, ...]
    bias_pct: tuple[Decimal | None, ...]
    atr_pct: tuple[Decimal | None, ...]
    near_ema_pct: tuple[Decimal | None, ...]
    body_ma: tuple[Decimal | None, ...]
    large_move_hits: tuple[tuple[str, ...], ...]
    signals: tuple[SignalReplayPoint, ...]
    summary: SignalReplaySummary


def build_signal_replay_dataset(
    candles: list[Candle],
    *,
    config: SignalReplayConfig | None = None,
) -> SignalReplayDataset:
    config = config or SignalReplayConfig()
    ordered = sorted(candles, key=lambda item: item.ts)
    if config.confirmed_only:
        ordered = [item for item in ordered if item.confirmed]
    closes = [item.close for item in ordered]
    volumes = [item.volume for item in ordered]
    bodies = [_body(item) for item in ordered]
    if not ordered:
        return SignalReplayDataset((), (), (), (), (), (), (), (), (), (), (), (), (), _build_summary(()))

    ema_fast = ema(closes, config.ema_fast_period)
    ema_slow = ema(closes, config.ema_slow_period)
    macd_line, macd_signal, macd_histogram = macd(
        closes,
        fast_period=config.macd_fast_period,
        slow_period=config.macd_slow_period,
        signal_period=config.macd_signal_period,
    )
    volume_ma = sma(volumes, config.volume_ma_period)
    atr_values = atr(ordered, config.atr_period)
    bias_pct = _build_bias_pct(closes, ema_fast)
    atr_pct = _build_atr_pct(closes, atr_values)
    near_ema_pct = _build_near_ema_pct(closes, ema_fast, ema_slow)
    body_ma = sma(bodies, config.mean_body_period)
    large_move_atr_values = atr(ordered, config.large_move_atr_period)
    large_move_hits = tuple(
        _large_move_hit_rules(ordered, index, body_ma, large_move_atr_values, config)
        for index in range(len(ordered))
    )
    indicator_signals = tuple(
        _iter_signals(
            ordered,
            ema_fast,
            ema_slow,
            macd_line,
            macd_signal,
            macd_histogram,
            volume_ma,
            bias_pct,
            atr_pct,
            near_ema_pct,
            config,
        )
    )
    pattern_signals = tuple(
        _iter_pattern_signals(
            ordered,
            body_ma,
            large_move_hits,
            large_move_atr_values,
            config,
        )
    )
    signals = tuple(sorted((*indicator_signals, *pattern_signals), key=lambda item: (item.index, item.candle_count, item.pattern_id)))
    return SignalReplayDataset(
        candles=tuple(ordered),
        ema_fast=tuple(ema_fast),
        ema_slow=tuple(ema_slow),
        macd_line=tuple(macd_line),
        macd_signal=tuple(macd_signal),
        macd_histogram=tuple(macd_histogram),
        volume_ma=tuple(volume_ma),
        bias_pct=tuple(bias_pct),
        atr_pct=tuple(atr_pct),
        near_ema_pct=tuple(near_ema_pct),
        body_ma=tuple(body_ma),
        large_move_hits=large_move_hits,
        signals=signals,
        summary=_build_summary(signals),
    )


def _iter_signals(
    candles: list[Candle],
    ema_fast: list[Decimal],
    ema_slow: list[Decimal],
    macd_line: list[Decimal],
    macd_signal: list[Decimal],
    macd_histogram: list[Decimal],
    volume_ma: list[Decimal | None],
    bias_pct: list[Decimal | None],
    atr_pct: list[Decimal | None],
    near_ema_pct: list[Decimal | None],
    config: SignalReplayConfig,
) -> list[SignalReplayPoint]:
    start = max(
        config.ema_slow_period,
        config.volume_ma_period,
        config.atr_period,
        config.macd_slow_period + config.macd_signal_period,
        2,
    )
    signals: list[SignalReplayPoint] = []
    for index in range(start, len(candles)):
        if config.include_long:
            point = _build_direction_signal(
                "long",
                index,
                candles,
                ema_fast,
                ema_slow,
                macd_line,
                macd_signal,
                macd_histogram,
                volume_ma,
                bias_pct,
                atr_pct,
                near_ema_pct,
                config,
            )
            if point is not None:
                signals.append(point)
        if config.include_short:
            point = _build_direction_signal(
                "short",
                index,
                candles,
                ema_fast,
                ema_slow,
                macd_line,
                macd_signal,
                macd_histogram,
                volume_ma,
                bias_pct,
                atr_pct,
                near_ema_pct,
                config,
            )
            if point is not None:
                signals.append(point)
    return signals


def _build_direction_signal(
    direction: str,
    index: int,
    candles: list[Candle],
    ema_fast: list[Decimal],
    ema_slow: list[Decimal],
    macd_line: list[Decimal],
    macd_signal: list[Decimal],
    macd_histogram: list[Decimal],
    volume_ma: list[Decimal | None],
    bias_pct: list[Decimal | None],
    atr_pct: list[Decimal | None],
    near_ema_pct: list[Decimal | None],
    config: SignalReplayConfig,
) -> SignalReplayPoint | None:
    candle = candles[index]
    previous = candles[index - 1]
    fast = ema_fast[index]
    slow = ema_slow[index]
    previous_fast = ema_fast[index - 1]
    previous_slow = ema_slow[index - 1]
    fast_slope = fast - previous_fast

    if direction == "long":
        trend_ok = candle.close > fast > slow and fast_slope > ZERO and fast > previous_slow
        pullback_ok = previous.close <= previous_fast and candle.low <= fast and candle.close > fast
        macd_ok = macd_line[index] > macd_signal[index] and (
            macd_line[index - 1] <= macd_signal[index - 1] or macd_histogram[index] > ZERO
        )
        bias_ok = _between(bias_pct[index], config.bias_min_pct, config.bias_max_pct)
    else:
        trend_ok = candle.close < fast < slow and fast_slope < ZERO and fast < previous_slow
        pullback_ok = previous.close >= previous_fast and candle.high >= fast and candle.close < fast
        macd_ok = macd_line[index] < macd_signal[index] and (
            macd_line[index - 1] >= macd_signal[index - 1] or macd_histogram[index] < ZERO
        )
        bias_ok = _between(bias_pct[index], -config.bias_max_pct, abs(config.bias_min_pct))

    vol_ma = volume_ma[index]
    volume_ok = vol_ma is not None and vol_ma > ZERO and candle.volume >= (vol_ma * config.volume_multiplier)
    atr_ok = atr_pct[index] is not None and atr_pct[index] >= config.atr_min_pct
    near_ema_ok = near_ema_pct[index] is not None and near_ema_pct[index] <= config.near_ema_max_pct

    required = (
        (not config.enable_trend_filter or trend_ok)
        and (not config.enable_pullback_trigger or pullback_ok)
        and (not config.enable_macd_filter or macd_ok)
        and (not config.enable_volume_filter or volume_ok)
        and (not config.enable_bias_filter or bias_ok)
        and (not config.enable_near_ema_filter or near_ema_ok)
        and (not config.enable_atr_filter or atr_ok)
    )
    if not required:
        return None

    score = 35
    reasons: list[str] = []
    setup_parts: list[str] = []
    if trend_ok:
        score += 18
        setup_parts.append("趋势")
        reasons.append("EMA21/55 同向排列")
    if pullback_ok:
        score += 17
        setup_parts.append("回踩")
        reasons.append("价格回踩后重新确认方向")
    if macd_ok:
        score += 12
        setup_parts.append("MACD")
        reasons.append("MACD 动能同向")
    if volume_ok:
        score += 10
        setup_parts.append("量能")
        reasons.append(f"成交量达到均量 {config.volume_multiplier}x")
    if bias_ok:
        score += 5
        setup_parts.append("乖离")
        reasons.append("乖离率仍在可接受区间")
    if near_ema_ok:
        score += 6
        setup_parts.append("均线附近")
        reasons.append(f"价格距离均线不超过 {config.near_ema_max_pct}%")
    if atr_ok:
        score += 3
        setup_parts.append("波动")
        reasons.append("ATR 波动满足过滤")
    score = min(score, 100)
    return SignalReplayPoint(
        index=index,
        ts=candle.ts,
        direction=direction,
        score=score,
        setup=" + ".join(setup_parts) if setup_parts else "条件组合",
        reason="；".join(reasons) if reasons else "满足当前组合条件",
        validation=_validate_signal(candles, index, direction),
    )


def _iter_pattern_signals(
    candles: list[Candle],
    body_ma: list[Decimal | None],
    large_move_hits: tuple[tuple[str, ...], ...],
    atr_values: list[Decimal | None],
    config: SignalReplayConfig,
) -> list[SignalReplayPoint]:
    if not config.enable_pattern_signals:
        return []
    signals: list[SignalReplayPoint] = []
    for index, candle in enumerate(candles):
        if config.enable_big_bullish and _is_bullish(candle) and _large_move_gate(large_move_hits, (index,), "single", config):
            signals.append(
                _pattern_signal(
                    candles,
                    index,
                    "big_bullish",
                    "大阳线",
                    1,
                    "long",
                    "单根大阳线",
                    large_move_hits[index],
                )
            )
        if config.enable_big_bearish and _is_bearish(candle) and _large_move_gate(large_move_hits, (index,), "single", config):
            signals.append(
                _pattern_signal(
                    candles,
                    index,
                    "big_bearish",
                    "大阴线",
                    1,
                    "short",
                    "单根大阴线",
                    large_move_hits[index],
                )
            )
        if (
            config.enable_long_upper_shadow
            and _upper_shadow(candle) >= (_body(candle) * config.shadow_body_multiplier)
            and _upper_shadow(candle) >= _lower_shadow(candle)
            and _large_move_gate(large_move_hits, (index,), "single", config)
        ):
            signals.append(
                _pattern_signal(
                    candles,
                    index,
                    "long_upper_shadow",
                    "长上影线",
                    1,
                    "short",
                    "上影线明显长于实体，冲高回落",
                    large_move_hits[index],
                )
            )
        if (
            config.enable_long_lower_shadow
            and _lower_shadow(candle) >= (_body(candle) * config.shadow_body_multiplier)
            and _lower_shadow(candle) >= _upper_shadow(candle)
            and _large_move_gate(large_move_hits, (index,), "single", config)
        ):
            signals.append(
                _pattern_signal(
                    candles,
                    index,
                    "long_lower_shadow",
                    "长下影线",
                    1,
                    "long",
                    "下影线明显长于实体，探底回收",
                    large_move_hits[index],
                )
            )
        if index >= 1:
            previous = candles[index - 1]
            span = (index - 1, index)
            span_rules = _merge_hit_rules(large_move_hits, span)
            breakdown_reference = _prior_low(candles, index, config.false_break_reference_lookback)
            breakout_reference = _prior_high(candles, index, config.false_break_reference_lookback)
            if (
                config.enable_false_breakdown
                and breakdown_reference is not None
                and _breaks_below(candle, breakdown_reference, atr_values[index], config)
                and candle.close >= breakdown_reference
                and candle.close > candle.open
                and _has_prior_drop(candles, index - 1, config.fractal_trend_lookback, config.fractal_trend_min_bars)
                and _close_position(candle) >= config.false_break_reclaim_position
                and _large_move_gate(large_move_hits, span, config.double_large_move_mode, config)
            ):
                signals.append(
                    _pattern_signal(candles, index, "false_breakdown", "假跌破", 2, "long", "前面有一段下跌后，跌破结构低点再强收回", span_rules)
                )
            if (
                config.enable_false_breakout
                and breakout_reference is not None
                and _breaks_above(candle, breakout_reference, atr_values[index], config)
                and candle.close <= breakout_reference
                and candle.close < candle.open
                and _has_prior_rise(candles, index - 1, config.fractal_trend_lookback, config.fractal_trend_min_bars)
                and _close_position(candle) <= (Decimal("1") - config.false_break_reclaim_position)
                and _large_move_gate(large_move_hits, span, config.double_large_move_mode, config)
            ):
                signals.append(
                    _pattern_signal(candles, index, "false_breakout", "假突破", 2, "short", "前面有一段上涨后，突破结构高点再弱收回", span_rules)
                )
            if (
                config.enable_inside_bar
                and previous.high >= candle.high
                and previous.low <= candle.low
                and _body(candle) <= (_body(previous) * config.small_body_ratio)
                and _large_move_gate(large_move_hits, (index - 1,), "single", config)
            ):
                direction = "long" if previous.close > previous.open else ("short" if previous.close < previous.open else "neutral")
                signals.append(
                    _pattern_signal(candles, index, "inside_bar", "孕育线", 2, direction, "大K线后出现完全包含的小K线", large_move_hits[index - 1])
                )
        if index >= 2:
            first = candles[index - 2]
            middle = candles[index - 1]
            third = candles[index]
            span = (index - 2, index - 1, index)
            span_rules = _merge_hit_rules(large_move_hits, span)
            middle_is_small = _is_small_body(middle, body_ma[index - 1], config)
            if (
                config.enable_top_fractal
                and _is_bullish(first)
                and middle_is_small
                and _is_bearish(third)
                and middle.high > first.high
                and middle.high > third.high
                and _has_prior_rise(candles, index - 2, config.fractal_trend_lookback, config.fractal_trend_min_bars)
                and _large_move_gate(large_move_hits, span, config.triple_large_move_mode, config)
            ):
                signals.append(
                    _pattern_signal(candles, index, "top_fractal", "顶分型", 3, "short", "前面有一段上涨后，阳线-小K-阴线形成局部高点", span_rules)
                )
            if (
                config.enable_bottom_fractal
                and _is_bearish(first)
                and middle_is_small
                and _is_bullish(third)
                and middle.low < first.low
                and middle.low < third.low
                and _has_prior_drop(candles, index - 2, config.fractal_trend_lookback, config.fractal_trend_min_bars)
                and _large_move_gate(large_move_hits, span, config.triple_large_move_mode, config)
            ):
                signals.append(
                    _pattern_signal(candles, index, "bottom_fractal", "底分型", 3, "long", "前面有一段下跌后，阴线-小K-阳线形成局部低点", span_rules)
                )
    return signals


def _pattern_signal(
    candles: list[Candle],
    index: int,
    pattern_id: str,
    pattern_name: str,
    candle_count: int,
    direction: str,
    reason: str,
    large_move_rules: tuple[str, ...],
) -> SignalReplayPoint:
    score = min(100, 50 + (candle_count * 6) + (len(large_move_rules) * 5))
    return SignalReplayPoint(
        index=index,
        ts=candles[index].ts,
        direction=direction,
        score=score,
        setup=pattern_name,
        reason=reason,
        validation=_validate_signal(candles, index, direction),
        pattern_id=pattern_id,
        pattern_name=pattern_name,
        candle_count=candle_count,
        large_move_rules=large_move_rules,
    )


def _validate_signal(candles: list[Candle], index: int, direction: str) -> SignalValidation:
    entry = candles[index].close
    if entry <= ZERO:
        return SignalValidation(None, None, None, None, None)
    if direction not in {"long", "short"}:
        return SignalValidation(None, None, None, None, None)
    future = candles[index + 1 : index + 25]
    if not future:
        return SignalValidation(None, None, None, None, None)

    returns = {
        4: _window_return(candles, index, 4, direction),
        12: _window_return(candles, index, 12, direction),
        24: _window_return(candles, index, 24, direction),
    }
    if direction == "short":
        mfe = max(((entry - item.low) / entry) * Decimal("100") for item in future)
        mae = min(((entry - item.high) / entry) * Decimal("100") for item in future)
    else:
        mfe = max(((item.high - entry) / entry) * Decimal("100") for item in future)
        mae = min(((item.low - entry) / entry) * Decimal("100") for item in future)
    return SignalValidation(
        return_4h_pct=returns[4],
        return_12h_pct=returns[12],
        return_24h_pct=returns[24],
        max_favorable_excursion_pct=mfe,
        max_adverse_excursion_pct=mae,
    )


def _window_return(candles: list[Candle], index: int, hours: int, direction: str) -> Decimal | None:
    target = index + hours
    if target >= len(candles):
        return None
    entry = candles[index].close
    close = candles[target].close
    if entry <= ZERO:
        return None
    if direction == "short":
        return ((entry - close) / entry) * Decimal("100")
    return ((close - entry) / entry) * Decimal("100")


def _build_summary(signals: tuple[SignalReplayPoint, ...]) -> SignalReplaySummary:
    long_count = sum(1 for item in signals if item.direction == "long")
    short_count = sum(1 for item in signals if item.direction == "short")
    returns = [item.validation.return_24h_pct for item in signals if item.validation.return_24h_pct is not None]
    if not returns:
        return SignalReplaySummary(len(signals), long_count, short_count, 0, None, None)
    hit_count = sum(1 for item in returns if item > ZERO)
    hit_rate = (Decimal(hit_count) / Decimal(len(returns))) * Decimal("100")
    avg_return = sum(returns, ZERO) / Decimal(len(returns))
    return SignalReplaySummary(len(signals), long_count, short_count, len(returns), hit_rate, avg_return)


def _large_move_hit_rules(
    candles: list[Candle],
    index: int,
    body_ma: list[Decimal | None],
    atr_values: list[Decimal | None],
    config: SignalReplayConfig,
) -> tuple[str, ...]:
    candle = candles[index]
    body = _body(candle)
    rules: list[str] = []
    if config.enable_large_move_mean:
        mean_body = body_ma[index]
        if mean_body is not None and mean_body > ZERO and body >= (mean_body * config.mean_body_multiplier):
            rules.append("均值实体")
    if config.enable_large_move_atr:
        atr_value = atr_values[index]
        if atr_value is not None and atr_value > ZERO and body >= (atr_value * config.large_move_atr_multiplier):
            rules.append("ATR")
    if config.enable_large_move_body_ratio:
        ratio = _body_ratio(candle)
        if ratio is not None and ratio >= config.body_ratio_threshold:
            rules.append("实体占比")
    if config.enable_large_move_fixed and config.fixed_body_threshold > ZERO and body >= config.fixed_body_threshold:
        rules.append("固定阈值")
    return tuple(rules)


def _prior_low(candles: list[Candle], index: int, lookback: int) -> Decimal | None:
    if lookback <= 0 or index <= 0:
        return None
    start = max(0, index - lookback)
    prior = candles[start:index]
    if not prior:
        return None
    return min(item.low for item in prior)


def _prior_high(candles: list[Candle], index: int, lookback: int) -> Decimal | None:
    if lookback <= 0 or index <= 0:
        return None
    start = max(0, index - lookback)
    prior = candles[start:index]
    if not prior:
        return None
    return max(item.high for item in prior)


def _false_break_min_distance(candle: Candle, atr_value: Decimal | None, config: SignalReplayConfig) -> Decimal:
    pct_distance = candle.close * config.false_break_min_pct / Decimal("100")
    atr_distance = Decimal("0")
    if atr_value is not None and atr_value > ZERO:
        atr_distance = atr_value * config.false_break_atr_multiplier
    return max(pct_distance, atr_distance)


def _breaks_below(
    candle: Candle,
    reference: Decimal,
    atr_value: Decimal | None,
    config: SignalReplayConfig,
) -> bool:
    return candle.low <= reference - _false_break_min_distance(candle, atr_value, config)


def _breaks_above(
    candle: Candle,
    reference: Decimal,
    atr_value: Decimal | None,
    config: SignalReplayConfig,
) -> bool:
    return candle.high >= reference + _false_break_min_distance(candle, atr_value, config)


def _close_position(candle: Candle) -> Decimal:
    total_range = candle.high - candle.low
    if total_range <= ZERO:
        return Decimal("0.5")
    return (candle.close - candle.low) / total_range


def _large_move_gate(
    large_move_hits: tuple[tuple[str, ...], ...],
    indices: tuple[int, ...],
    mode: str,
    config: SignalReplayConfig,
) -> bool:
    if not config.enable_large_move_gate:
        return True
    if not indices:
        return False
    normalized = str(mode or "any").strip().lower()
    if normalized == "all":
        return all(0 <= index < len(large_move_hits) and bool(large_move_hits[index]) for index in indices)
    if normalized == "first":
        first = indices[0]
        return 0 <= first < len(large_move_hits) and bool(large_move_hits[first])
    if normalized == "last":
        last = indices[-1]
        return 0 <= last < len(large_move_hits) and bool(large_move_hits[last])
    if normalized == "edge":
        first = indices[0]
        last = indices[-1]
        return (
            (0 <= first < len(large_move_hits) and bool(large_move_hits[first]))
            or (0 <= last < len(large_move_hits) and bool(large_move_hits[last]))
        )
    return any(0 <= index < len(large_move_hits) and bool(large_move_hits[index]) for index in indices)


def _merge_hit_rules(
    large_move_hits: tuple[tuple[str, ...], ...],
    indices: tuple[int, ...],
) -> tuple[str, ...]:
    rules: list[str] = []
    for index in indices:
        if not (0 <= index < len(large_move_hits)):
            continue
        for rule in large_move_hits[index]:
            if rule not in rules:
                rules.append(rule)
    return tuple(rules)


def _is_bullish(candle: Candle) -> bool:
    return candle.close > candle.open


def _is_bearish(candle: Candle) -> bool:
    return candle.close < candle.open


def _body(candle: Candle) -> Decimal:
    return abs(candle.close - candle.open)


def _upper_shadow(candle: Candle) -> Decimal:
    return candle.high - max(candle.open, candle.close)


def _lower_shadow(candle: Candle) -> Decimal:
    return min(candle.open, candle.close) - candle.low


def _body_ratio(candle: Candle) -> Decimal | None:
    total_range = candle.high - candle.low
    if total_range <= ZERO:
        return None
    return _body(candle) / total_range


def _is_small_body(candle: Candle, mean_body: Decimal | None, config: SignalReplayConfig) -> bool:
    body = _body(candle)
    if mean_body is not None and mean_body > ZERO:
        return body <= (mean_body * config.small_body_ratio)
    total_range = candle.high - candle.low
    if total_range <= ZERO:
        return False
    return (body / total_range) <= config.small_body_ratio


def _has_prior_rise(candles: list[Candle], end_index: int, lookback: int, min_bars: int) -> bool:
    if lookback <= 0 or min_bars <= 0 or end_index <= 0:
        return False
    start = max(1, end_index - lookback + 1)
    up_count = 0
    for index in range(start, end_index + 1):
        if candles[index].close > candles[index - 1].close:
            up_count += 1
    return up_count >= min_bars


def _has_prior_drop(candles: list[Candle], end_index: int, lookback: int, min_bars: int) -> bool:
    if lookback <= 0 or min_bars <= 0 or end_index <= 0:
        return False
    start = max(1, end_index - lookback + 1)
    down_count = 0
    for index in range(start, end_index + 1):
        if candles[index].close < candles[index - 1].close:
            down_count += 1
    return down_count >= min_bars


def _build_bias_pct(closes: list[Decimal], ema_values: list[Decimal]) -> list[Decimal | None]:
    result: list[Decimal | None] = []
    for close, ema_value in zip(closes, ema_values):
        if ema_value <= ZERO:
            result.append(None)
            continue
        result.append(((close - ema_value) / ema_value) * Decimal("100"))
    return result


def _build_atr_pct(closes: list[Decimal], atr_values: list[Decimal | None]) -> list[Decimal | None]:
    result: list[Decimal | None] = []
    for close, atr_value in zip(closes, atr_values):
        if atr_value is None or close <= ZERO:
            result.append(None)
            continue
        result.append((atr_value / close) * Decimal("100"))
    return result


def _build_near_ema_pct(
    closes: list[Decimal],
    ema_fast: list[Decimal],
    ema_slow: list[Decimal],
) -> list[Decimal | None]:
    result: list[Decimal | None] = []
    for close, fast, slow in zip(closes, ema_fast, ema_slow):
        if close <= ZERO:
            result.append(None)
            continue
        nearest_gap = min(abs(close - fast), abs(close - slow))
        result.append((nearest_gap / close) * Decimal("100"))
    return result


def _between(value: Decimal | None, lower: Decimal, upper: Decimal) -> bool:
    if value is None:
        return False
    return lower <= value <= upper
