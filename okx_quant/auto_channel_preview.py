from __future__ import annotations

from decimal import Decimal

from okx_quant.analysis import BoxDetectionConfig, ChannelDetectionConfig, PivotDetectionConfig
from okx_quant.models import Candle
from okx_quant.strategy_live_chart import StrategyLiveChartSnapshot, build_auto_channel_live_chart_snapshot


def sample_auto_channel_candles(sample: str = "channel") -> list[Candle]:
    if sample == "box":
        return _sample_box_candles()
    return _sample_channel_candles()


def build_auto_channel_preview_snapshot(sample: str = "channel") -> StrategyLiveChartSnapshot:
    normalized = "box" if sample == "box" else "channel"
    candles = sample_auto_channel_candles(normalized)
    return build_auto_channel_live_chart_snapshot(
        session_id=f"auto-{normalized}",
        candles=candles,
        channel_config=ChannelDetectionConfig(
            pivot=PivotDetectionConfig(
                left_bars=1,
                right_bars=1,
                atr_period=3,
                atr_multiplier=Decimal("0"),
                min_index_distance=1,
            ),
            min_anchor_distance=8,
            min_channel_bars=18,
            max_violations=8,
        ),
        box_config=BoxDetectionConfig(
            pivot=PivotDetectionConfig(
                left_bars=1,
                right_bars=1,
                atr_period=3,
                atr_multiplier=Decimal("0"),
                min_index_distance=1,
            ),
            min_box_bars=18,
            min_touches_per_side=2,
            max_violations=8,
        ),
        right_pad_bars=50,
        channel_extend_bars=50,
        latest_price=candles[-1].close if candles else None,
        note="自动通道预览",
    )


def _sample_channel_candles() -> list[Candle]:
    candles: list[Candle] = []
    ts0 = 1714330800000
    for index in range(64):
        wave = Decimal((index % 8) - 4) * Decimal("0.18")
        base = Decimal("100") + Decimal(index) * Decimal("0.45")
        low = base + Decimal("0.8") + wave
        high = base + Decimal("4.2") + wave
        if index in {5, 18, 32, 46}:
            low = base - Decimal("0.2")
        if index in {11, 25, 39, 54}:
            high = base + Decimal("6.0")
        close = (low + high) / Decimal("2")
        candles.append(
            Candle(
                ts=ts0 + index * 60_000,
                open=close - Decimal("0.25"),
                high=high,
                low=low,
                close=close,
                volume=Decimal("1"),
                confirmed=True,
            )
        )
    return candles


def _sample_box_candles() -> list[Candle]:
    candles: list[Candle] = []
    ts0 = 1714330800000
    for index in range(56):
        low = Decimal("99.2")
        high = Decimal("105.8")
        if index % 8 in {2, 6}:
            high = Decimal("108")
        if index % 8 in {0, 4}:
            low = Decimal("96")
        close = (low + high) / Decimal("2")
        candles.append(
            Candle(
                ts=ts0 + index * 60_000,
                open=close,
                high=high,
                low=low,
                close=close,
                volume=Decimal("1"),
                confirmed=True,
            )
        )
    return candles
