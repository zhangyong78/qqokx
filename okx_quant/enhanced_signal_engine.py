from __future__ import annotations

from decimal import Decimal

from okx_quant.indicators import ema
from okx_quant.enhanced_models import ChildSignalConfig, SignalEvent, SignalRuleMatch
from okx_quant.models import Candle


class EnhancedSignalEngine:
    def aggregate_candles(
        self,
        candles: list[Candle],
        *,
        base_bar: str,
        target_bar: str,
    ) -> list[Candle]:
        if target_bar == base_bar:
            return list(candles)
        base_minutes = bar_to_minutes(base_bar)
        target_minutes = bar_to_minutes(target_bar)
        if target_minutes < base_minutes:
            raise ValueError(f"target bar {target_bar} cannot be smaller than base bar {base_bar}")
        if target_minutes % base_minutes != 0:
            raise ValueError(f"target bar {target_bar} must be an integer multiple of base bar {base_bar}")
        step = target_minutes // base_minutes
        if step <= 1:
            return list(candles)

        aggregated: list[Candle] = []
        confirmed_candles = [item for item in candles if item.confirmed]
        for start in range(0, len(confirmed_candles), step):
            bucket = confirmed_candles[start : start + step]
            if len(bucket) < step:
                break
            high = max(item.high for item in bucket)
            low = min(item.low for item in bucket)
            volume = sum((item.volume for item in bucket), Decimal("0"))
            aggregated.append(
                Candle(
                    ts=bucket[-1].ts,
                    open=bucket[0].open,
                    high=high,
                    low=low,
                    close=bucket[-1].close,
                    volume=volume,
                    confirmed=all(item.confirmed for item in bucket),
                )
            )
        return aggregated

    def evaluate_signal(
        self,
        config: ChildSignalConfig,
        candles: list[Candle],
        *,
        trigger_rule,
        invalidation_rule=None,
    ) -> list[SignalEvent]:
        events: list[SignalEvent] = []
        for index in range(len(candles)):
            match = trigger_rule(candles, index)
            if match is None or not match.triggered:
                continue
            if invalidation_rule is not None:
                invalidation_match = invalidation_rule(candles, index)
                if invalidation_match is not None and invalidation_match.triggered:
                    continue
            signal_price = match.signal_price if match.signal_price is not None else candles[index].close
            events.append(
                SignalEvent(
                    signal_id=config.signal_id,
                    signal_name=config.signal_name,
                    underlying_family=config.underlying_family,
                    source_market=config.source.market,
                    source_inst_id=config.source.inst_id,
                    source_bar=config.source.bar,
                    candle_index=index,
                    candle_ts=candles[index].ts,
                    signal_price=signal_price,
                    bar_step=_infer_bar_step(candles, index),
                    direction_bias=config.direction_bias,
                    reason=match.reason,
                )
            )
        return events


def bar_to_minutes(bar: str) -> int:
    normalized = bar.strip().lower()
    if normalized.endswith("m"):
        return int(normalized[:-1])
    if normalized.endswith("h"):
        return int(normalized[:-1]) * 60
    if normalized.endswith("d"):
        return int(normalized[:-1]) * 24 * 60
    raise ValueError(f"unsupported bar: {bar}")


def close_crosses_above_lookback_high(candles: list[Candle], index: int, lookback: int) -> SignalRuleMatch | None:
    if lookback <= 0 or index <= 0 or index < lookback:
        return None
    previous_high = max(item.high for item in candles[index - lookback : index])
    previous_close = candles[index - 1].close
    current_close = candles[index].close
    if previous_close <= previous_high and current_close > previous_high:
        return SignalRuleMatch(
            triggered=True,
            reason=f"close crossed above {lookback}-bar high {previous_high}",
            signal_price=current_close,
        )
    return None


def close_crosses_below_lookback_low(candles: list[Candle], index: int, lookback: int) -> SignalRuleMatch | None:
    if lookback <= 0 or index <= 0 or index < lookback:
        return None
    previous_low = min(item.low for item in candles[index - lookback : index])
    previous_close = candles[index - 1].close
    current_close = candles[index].close
    if previous_close >= previous_low and current_close < previous_low:
        return SignalRuleMatch(
            triggered=True,
            reason=f"close crossed below {lookback}-bar low {previous_low}",
            signal_price=current_close,
        )
    return None


def close_crosses_above_ema(candles: list[Candle], index: int, period: int) -> SignalRuleMatch | None:
    if period <= 0 or index <= 0 or len(candles) < period:
        return None
    closes = [item.close for item in candles]
    ema_values = ema(closes, period)
    previous_close = candles[index - 1].close
    current_close = candles[index].close
    previous_ema = ema_values[index - 1]
    current_ema = ema_values[index]
    if previous_close <= previous_ema and current_close > current_ema:
        return SignalRuleMatch(
            triggered=True,
            reason=f"close crossed above EMA{period} {current_ema}",
            signal_price=current_close,
        )
    return None


def close_crosses_below_ema(candles: list[Candle], index: int, period: int) -> SignalRuleMatch | None:
    if period <= 0 or index <= 0 or len(candles) < period:
        return None
    closes = [item.close for item in candles]
    ema_values = ema(closes, period)
    previous_close = candles[index - 1].close
    current_close = candles[index].close
    previous_ema = ema_values[index - 1]
    current_ema = ema_values[index]
    if previous_close >= previous_ema and current_close < current_ema:
        return SignalRuleMatch(
            triggered=True,
            reason=f"close crossed below EMA{period} {current_ema}",
            signal_price=current_close,
        )
    return None


def pullback_reclaim_above_ema_after_run(
    candles: list[Candle],
    index: int,
    *,
    ema_period: int,
    run_bars: int,
    minimum_up_closes: int,
) -> SignalRuleMatch | None:
    if ema_period <= 0 or run_bars <= 0 or index <= run_bars:
        return None
    closes = [item.close for item in candles]
    ema_values = ema(closes, ema_period)
    current = candles[index]
    current_ema = ema_values[index]
    if current.low > current_ema or current.close <= current_ema:
        return None
    up_close_count = 0
    for offset in range(index - run_bars + 1, index):
        if offset <= 0:
            continue
        if candles[offset].close > candles[offset - 1].close:
            up_close_count += 1
    if up_close_count < minimum_up_closes:
        return None
    return SignalRuleMatch(
        triggered=True,
        reason=(
            f"pullback reclaimed EMA{ema_period} after {up_close_count} rising closes "
            f"in last {run_bars} bars"
        ),
        signal_price=current.close,
    )


def _infer_bar_step(candles: list[Candle], index: int) -> int:
    if len(candles) <= 1:
        return 1
    if index > 0:
        step = candles[index].ts - candles[index - 1].ts
        if step > 0:
            return step
    if index + 1 < len(candles):
        step = candles[index + 1].ts - candles[index].ts
        if step > 0:
            return step
    return 1
