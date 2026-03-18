from __future__ import annotations

from decimal import Decimal

from okx_quant.models import Candle


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
