from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.indicators import atr
from okx_quant.models import Candle
from okx_quant.analysis.structure_models import PivotPoint


@dataclass(frozen=True)
class PivotDetectionConfig:
    left_bars: int = 3
    right_bars: int = 3
    atr_period: int = 14
    atr_multiplier: Decimal = Decimal("0.5")
    min_price_move: Decimal = Decimal("0")
    min_index_distance: int = 2
    confirmed_only: bool = True


def detect_pivots(candles: list[Candle] | tuple[Candle, ...], config: PivotDetectionConfig | None = None) -> list[PivotPoint]:
    cfg = config or PivotDetectionConfig()
    _validate_config(cfg)
    items = tuple(candles)
    if len(items) < cfg.left_bars + cfg.right_bars + 1:
        return []

    atr_values = atr(list(items), cfg.atr_period)
    raw: list[PivotPoint] = []
    for index in range(cfg.left_bars, len(items) - cfg.right_bars):
        candle = items[index]
        if cfg.confirmed_only and not candle.confirmed:
            continue
        left = items[index - cfg.left_bars : index]
        right = items[index + 1 : index + cfg.right_bars + 1]
        neighbors = left + right
        if candle.high > max(item.high for item in neighbors):
            strength = _pivot_strength(candle.high, [item.high for item in neighbors], atr_values[index])
            raw.append(PivotPoint(index=index, ts=candle.ts, price=candle.high, kind="high", strength=strength))
        if candle.low < min(item.low for item in neighbors):
            strength = _pivot_strength(candle.low, [item.low for item in neighbors], atr_values[index])
            raw.append(PivotPoint(index=index, ts=candle.ts, price=candle.low, kind="low", strength=strength))

    return _filter_significant_pivots(raw, cfg, atr_values)


def _validate_config(config: PivotDetectionConfig) -> None:
    if config.left_bars <= 0 or config.right_bars <= 0:
        raise ValueError("left_bars and right_bars must be positive")
    if config.atr_period <= 0:
        raise ValueError("atr_period must be positive")
    if config.atr_multiplier < 0:
        raise ValueError("atr_multiplier cannot be negative")
    if config.min_index_distance < 0:
        raise ValueError("min_index_distance cannot be negative")


def _pivot_strength(price: Decimal, neighbor_prices: list[Decimal], atr_value: Decimal | None) -> Decimal:
    if not neighbor_prices:
        return Decimal("0")
    distance = min(abs(price - item) for item in neighbor_prices)
    if atr_value is None or atr_value <= 0:
        return distance
    return distance / atr_value


def _filter_significant_pivots(
    pivots: list[PivotPoint],
    config: PivotDetectionConfig,
    atr_values: list[Decimal | None],
) -> list[PivotPoint]:
    accepted: list[PivotPoint] = []
    last_by_kind: dict[str, PivotPoint] = {}
    for pivot in pivots:
        atr_value = atr_values[pivot.index] if pivot.index < len(atr_values) else None
        min_move = config.min_price_move
        if atr_value is not None and atr_value > 0:
            min_move = max(min_move, atr_value * config.atr_multiplier)

        previous = last_by_kind.get(pivot.kind)
        if previous is not None:
            if pivot.index - previous.index < config.min_index_distance:
                if pivot.strength <= previous.strength:
                    continue
                accepted = [item for item in accepted if item is not previous]
            elif abs(pivot.price - previous.price) < min_move:
                continue

        accepted.append(pivot)
        last_by_kind[pivot.kind] = pivot
    return accepted
