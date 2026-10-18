"""Causal support/resistance zones built from confirmed swing pivots."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.indicators import atr
from okx_quant.models import Candle


@dataclass(frozen=True)
class AutoZone:
    side: str
    price: Decimal
    touches: int
    first_ts: int
    last_ts: int


@dataclass(frozen=True)
class AutoZonePair:
    support: AutoZone
    resistance: AutoZone
    atr_value: Decimal
    candle_ts: int
    lookback_bars: int


def find_auto_zone_pair(
    candles: list[Candle], *, lookback_bars: int = 240, left_bars: int = 3,
    right_bars: int = 3, atr_period: int = 14,
    min_reversal_atr: Decimal = Decimal("0.75"),
    cluster_tolerance_atr: Decimal = Decimal("0.50"), min_touches: int = 2,
) -> AutoZonePair:
    """Find nearest support below and resistance above the last close.

    A pivot is used only after ``right_bars`` closed candles confirm it. A
    pivot must also reverse by ``min_reversal_atr`` from its extreme. Similar
    pivots are clustered within half an ATR by default.
    """
    confirmed = sorted((item for item in candles if item.confirmed), key=lambda item: item.ts)
    if lookback_bars < 2 * right_bars + atr_period + 2:
        raise ValueError("回看范围太短，无法确认摆动区域")
    confirmed = confirmed[-lookback_bars:]
    if len(confirmed) < max(atr_period + 1, left_bars + right_bars + 2):
        raise ValueError("已收盘1H K线不足，无法自动识别区域")

    atr_values = atr(confirmed, atr_period)
    current_atr = atr_values[-1]
    if current_atr is None or current_atr <= 0:
        raise ValueError("最新 ATR 不可用，无法确定区域宽度")

    pivots: dict[str, list[tuple[Decimal, int]]] = {"support": [], "resistance": []}
    last_index = len(confirmed) - right_bars
    for index in range(left_bars, last_index):
        pivot_atr = atr_values[index]
        if pivot_atr is None or pivot_atr <= 0:
            continue
        before = confirmed[index - left_bars:index]
        after = confirmed[index + 1:index + right_bars + 1]
        candle = confirmed[index]
        if (candle.high >= max(item.high for item in before)
                and candle.high > max(item.high for item in after)
                and candle.high - min(item.low for item in after) >= pivot_atr * min_reversal_atr):
            pivots["resistance"].append((candle.high, candle.ts))
        if (candle.low <= min(item.low for item in before)
                and candle.low < min(item.low for item in after)
                and max(item.high for item in after) - candle.low >= pivot_atr * min_reversal_atr):
            pivots["support"].append((candle.low, candle.ts))

    last_close = confirmed[-1].close
    tolerance = current_atr * cluster_tolerance_atr
    support_zones = _cluster("support", pivots["support"], tolerance, min_touches)
    resistance_zones = _cluster("resistance", pivots["resistance"], tolerance, min_touches)
    supports_below = [zone for zone in support_zones if zone.price <= last_close]
    resistances_above = [zone for zone in resistance_zones if zone.price >= last_close]
    support = max(supports_below, key=lambda item: item.price, default=None)
    resistance = min(resistances_above, key=lambda item: item.price, default=None)
    if support is None or resistance is None or support.price >= resistance.price:
        raise ValueError("最新收盘价上下方未同时找到有效支撑/压力簇（每侧至少2个确认摆动点）")
    return AutoZonePair(support, resistance, current_atr, confirmed[-1].ts, len(confirmed))


def _cluster(
    side: str, points: list[tuple[Decimal, int]], tolerance: Decimal, min_touches: int,
) -> list[AutoZone]:
    ordered = sorted(points, key=lambda item: item[0])
    zones: list[AutoZone] = []
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][0] - ordered[start][0] <= tolerance:
            end += 1
        group = ordered[start:end]
        if len(group) >= min_touches:
            levels = sorted(price for price, _ in group)
            midpoint = levels[len(levels) // 2] if len(levels) % 2 else (levels[len(levels) // 2 - 1] + levels[len(levels) // 2]) / 2
            times = [ts for _, ts in group]
            zones.append(AutoZone(side, midpoint, len(group), min(times), max(times)))
            start = end
        else:
            start += 1
    return zones
