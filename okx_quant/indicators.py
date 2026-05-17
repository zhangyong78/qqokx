from __future__ import annotations

from decimal import Decimal

from okx_quant.models import Candle, normalize_moving_average_type


def ema(values: list[Decimal], period: int) -> list[Decimal]:
    if period <= 0:
        raise ValueError("period must be positive")
    if not values:
        return []

    multiplier = Decimal("2") / Decimal(period + 1)
    result = [values[0]]
    for value in values[1:]:
        current = (value - result[-1]) * multiplier + result[-1]
        result.append(current)
    return result


def sma(values: list[Decimal], period: int) -> list[Decimal | None]:
    if period <= 0:
        raise ValueError("period must be positive")
    if not values:
        return []

    result: list[Decimal | None] = [None] * len(values)
    running_sum = Decimal("0")
    for index, value in enumerate(values):
        running_sum += value
        if index >= period:
            running_sum -= values[index - period]
        if index >= period - 1:
            result[index] = running_sum / Decimal(period)
    return result


def moving_average(values: list[Decimal], period: int, ma_type: str = "ema") -> list[Decimal | None]:
    normalized_type = normalize_moving_average_type(ma_type)
    if normalized_type == "ma":
        return sma(values, period)
    return list(ema(values, period))


def macd(
    values: list[Decimal],
    *,
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> tuple[list[Decimal], list[Decimal], list[Decimal]]:
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        raise ValueError("period must be positive")
    if not values:
        return [], [], []
    if fast_period >= slow_period:
        raise ValueError("fast_period must be less than slow_period")

    fast_values = ema(values, fast_period)
    slow_values = ema(values, slow_period)
    macd_line = [fast - slow for fast, slow in zip(fast_values, slow_values)]
    signal_line = ema(macd_line, signal_period)
    histogram = [line - signal for line, signal in zip(macd_line, signal_line)]
    return macd_line, signal_line, histogram


def bollinger_bands(
    values: list[Decimal],
    *,
    period: int = 20,
    std_dev_multiplier: Decimal = Decimal("2"),
) -> tuple[list[Decimal | None], list[Decimal | None], list[Decimal | None]]:
    if period <= 0:
        raise ValueError("period must be positive")
    if not values:
        return [], [], []

    middle = sma(values, period)
    upper: list[Decimal | None] = [None] * len(values)
    lower: list[Decimal | None] = [None] * len(values)
    for index in range(period - 1, len(values)):
        window = values[index - period + 1 : index + 1]
        mean = middle[index]
        if mean is None:
            continue
        variance = sum((item - mean) * (item - mean) for item in window) / Decimal(period)
        std_dev = variance.sqrt()
        upper[index] = mean + (std_dev * std_dev_multiplier)
        lower[index] = mean - (std_dev * std_dev_multiplier)
    return middle, upper, lower


def true_ranges(candles: list[Candle]) -> list[Decimal]:
    if not candles:
        return []

    ranges: list[Decimal] = []
    previous_close: Decimal | None = None
    for candle in candles:
        if previous_close is None:
            tr = candle.high - candle.low
        else:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
        ranges.append(tr)
        previous_close = candle.close
    return ranges


def atr(candles: list[Candle], period: int) -> list[Decimal | None]:
    if period <= 0:
        raise ValueError("period must be positive")
    if not candles:
        return []

    trs = true_ranges(candles)
    result: list[Decimal | None] = [None] * len(candles)
    if len(trs) < period:
        return result

    initial = sum(trs[:period], Decimal("0")) / Decimal(period)
    result[period - 1] = initial
    previous_atr = initial
    for index in range(period, len(trs)):
        current = ((previous_atr * Decimal(period - 1)) + trs[index]) / Decimal(period)
        result[index] = current
        previous_atr = current
    return result
