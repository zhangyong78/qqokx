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
    if not ordered:
        return SignalReplayDataset((), (), (), (), (), (), (), (), (), (), (), _build_summary(()))

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
    signals = tuple(
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


def _validate_signal(candles: list[Candle], index: int, direction: str) -> SignalValidation:
    entry = candles[index].close
    if entry <= ZERO:
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
