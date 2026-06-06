from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from okx_quant.indicators import moving_average
from okx_quant.models import Candle, DailyFilterBoundary, MovingAverageType
from okx_quant.timeframe import latest_closed_candle_index


DAY_MS = 86_400_000
HOUR_MS = 3_600_000
_BOUNDARY_OFFSET_MS: dict[DailyFilterBoundary, int] = {
    "exchange": 0,
    "bjt_08": 0,
    "bjt_00": 16 * HOUR_MS,
}


@dataclass(frozen=True)
class AggregatedDailyAudit:
    bucket_open_ts: int
    hours_in_bucket: int
    first_hour_ts: int
    last_hour_ts: int
    is_full_24h: bool


def daily_boundary_anchor_offset_ms(boundary: DailyFilterBoundary) -> int:
    return _BOUNDARY_OFFSET_MS.get(boundary, 0)


def aggregate_candles_to_daily_boundary(
    candles: list[Candle],
    *,
    boundary: DailyFilterBoundary = "exchange",
) -> tuple[list[Candle], list[AggregatedDailyAudit]]:
    if not candles:
        return [], []
    anchor_offset_ms = daily_boundary_anchor_offset_ms(boundary)
    buckets: dict[int, list[Candle]] = {}
    for candle in candles:
        bucket_ts = ((int(candle.ts) - anchor_offset_ms) // DAY_MS) * DAY_MS + anchor_offset_ms
        buckets.setdefault(bucket_ts, []).append(candle)

    aggregated: list[Candle] = []
    audits: list[AggregatedDailyAudit] = []
    for bucket_ts in sorted(buckets):
        group = sorted(buckets[bucket_ts], key=lambda item: int(item.ts))
        aggregated.append(
            Candle(
                ts=int(bucket_ts),
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum((item.volume for item in group), Decimal("0")),
                confirmed=all(item.confirmed for item in group),
            )
        )
        audits.append(
            AggregatedDailyAudit(
                bucket_open_ts=int(bucket_ts),
                hours_in_bucket=len(group),
                first_hour_ts=int(group[0].ts),
                last_hour_ts=int(group[-1].ts),
                is_full_24h=len(group) == 24,
            )
        )
    return aggregated, audits


def build_daily_close_vs_ma_bias(
    entry_candles: list[Candle],
    daily_candles: list[Candle],
    *,
    ma_type: MovingAverageType | str,
    period: int,
) -> list[str]:
    if not entry_candles or not daily_candles or period <= 0:
        return ["neutral"] * len(entry_candles)
    closes = [candle.close for candle in daily_candles]
    line_values = moving_average(closes, int(period), ma_type)
    bias: list[str] = []
    for entry_candle in entry_candles:
        index = latest_closed_candle_index(daily_candles, int(entry_candle.ts))
        if index < 0 or index >= len(line_values):
            bias.append("neutral")
            continue
        line_value = line_values[index]
        if line_value is None:
            bias.append("neutral")
            continue
        daily_close = daily_candles[index].close
        if daily_close > line_value:
            bias.append("long")
        elif daily_close < line_value:
            bias.append("short")
        else:
            bias.append("neutral")
    return bias


def build_daily_weak_day_flags(entry_candles: list[Candle], daily_candles: list[Candle]) -> list[bool]:
    if not entry_candles or not daily_candles:
        return [False] * len(entry_candles)
    flags: list[bool] = []
    for entry_candle in entry_candles:
        index = latest_closed_candle_index(daily_candles, int(entry_candle.ts))
        if index < 0 or index >= len(daily_candles):
            flags.append(False)
            continue
        daily_candle = daily_candles[index]
        flags.append(bool(daily_candle.close < daily_candle.open))
    return flags
