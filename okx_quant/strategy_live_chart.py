from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from tkinter import Canvas

from okx_quant.analysis import (
    BoxDetectionConfig,
    ChannelDetectionConfig,
    TrendlineDetectionConfig,
    TriangleDetectionConfig,
    detect_boxes,
    detect_channels,
    detect_pivots,
    detect_trendlines,
    detect_triangles,
)
from okx_quant.analysis.structure_models import (
    BoxCandidate,
    ChannelCandidate,
    PivotPoint,
    PriceLine,
    TrendlineCandidate,
    TriangleCandidate,
)
from okx_quant.indicators import ema
from okx_quant.models import Candle
from okx_quant.pricing import format_decimal, format_decimal_by_increment

DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT = 240
DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS = 5000
# 划线交易台轮询略慢于策略图，减轻 OKX 与主线程压力。
LINE_TRADING_DESK_POLL_MS = 7500
# 划线交易台单次拉取根数（曾分 INITIAL+补足 两阶段，易与画线锚点不同步；现与 ui_shell 一致为一次拉满）。
LINE_TRADING_DESK_CANDLE_INITIAL = 280  # 历史/脚本兼容参考，当前划线台 worker 不再使用。
LINE_TRADING_DESK_CANDLE_TARGET = 500
LINE_TRADING_DESK_MAX_RIGHT_PAD_BARS = 240
# 已映射画布的最小像素：略低于 200 以兼容分屏/窄窗；仍须与内区 inset 检查一起通过才绘制 K 线。
CHART_CANVAS_MIN_MAPPED_W = 100
CHART_CANVAS_MIN_MAPPED_H = 100

_CHART_INSET_LEFT = 76
_CHART_INSET_RIGHT_PAD = 156
_CHART_INSET_TOP = 40
_CHART_INSET_BOTTOM = 56

_GRID_COLOR = "#e9edf3"
_TEXT_COLOR = "#2f3944"
_MUTED_TEXT_COLOR = "#667281"
_PRICE_LABEL_BG = "#ffffff"
_CANDLE_RISE = "#137333"
_CANDLE_FALL = "#cf222e"
_CANDLE_PENDING = "#b08800"
_MARKER_LABEL_HALF_HEIGHT = 10.0
_MARKER_LABEL_MIN_GAP = 24.0
_MARKER_LABEL_CANVAS_MARGIN = 6.0


@dataclass(frozen=True)
class StrategyLiveChartOverlaySeries:
    key: str
    label: str
    color: str
    values: tuple[Decimal, ...]


@dataclass(frozen=True)
class StrategyLiveChartMarker:
    key: str
    label: str
    price: Decimal
    color: str
    dash: tuple[int, ...] = ()
    width: int = 1


@dataclass(frozen=True)
class StrategyLiveChartTimeMarker:
    key: str
    label: str
    at: datetime
    color: str
    dash: tuple[int, ...] = ()
    width: int = 1


@dataclass(frozen=True)
class StrategyLiveChartLineOverlay:
    key: str
    label: str
    line: PriceLine
    color: str
    dash: tuple[int, ...] = ()
    width: int = 1


@dataclass(frozen=True)
class StrategyLiveChartBandOverlay:
    key: str
    label: str
    start_index: int
    end_index: int
    upper_line: PriceLine
    lower_line: PriceLine
    outline: str
    fill: str
    stipple: str = "gray25"


@dataclass(frozen=True)
class StrategyLiveChartBoxOverlay:
    key: str
    label: str
    start_index: int
    end_index: int
    upper: Decimal
    lower: Decimal
    outline: str
    fill: str
    stipple: str = "gray25"


@dataclass(frozen=True)
class StrategyLiveChartPointOverlay:
    key: str
    label: str
    index: int
    price: Decimal
    color: str
    radius: float = 4.0


@dataclass(frozen=True)
class StrategyLiveChartSnapshot:
    session_id: str
    candles: tuple[Candle, ...]
    series: tuple[StrategyLiveChartOverlaySeries, ...] = ()
    markers: tuple[StrategyLiveChartMarker, ...] = ()
    time_markers: tuple[StrategyLiveChartTimeMarker, ...] = ()
    line_overlays: tuple[StrategyLiveChartLineOverlay, ...] = ()
    band_overlays: tuple[StrategyLiveChartBandOverlay, ...] = ()
    box_overlays: tuple[StrategyLiveChartBoxOverlay, ...] = ()
    point_overlays: tuple[StrategyLiveChartPointOverlay, ...] = ()
    latest_price: Decimal | None = None
    latest_candle_time: datetime | None = None
    latest_candle_confirmed: bool = True
    note: str = ""
    #: 若设置：均线等 overlay 只绘制到该索引之前（不含），用于右侧占位 K 不画均线。
    series_plot_end_index: int | None = None
    #: 若设置：纵轴价签、右侧标记价等按该最小变动单位格式化（与合约 tick 一致）。
    price_display_tick: Decimal | None = None
    right_pad_bars: int = 0


@dataclass(frozen=True)
class _ChartBounds:
    left: float
    top: float
    right: float
    bottom: float

    @property
    def width(self) -> float:
        return max(self.right - self.left, 1.0)

    @property
    def height(self) -> float:
        return max(self.bottom - self.top, 1.0)


def build_strategy_live_chart_snapshot(
    *,
    session_id: str,
    candles: list[Candle] | tuple[Candle, ...],
    ema_period: int | None = None,
    trend_ema_period: int | None = None,
    reference_ema_period: int | None = None,
    pending_entry_prices: tuple[Decimal, ...] = (),
    entry_price: Decimal | None = None,
    entry_time: datetime | None = None,
    time_markers: tuple[StrategyLiveChartTimeMarker, ...] = (),
    position_avg_price: Decimal | None = None,
    stop_price: Decimal | None = None,
    latest_price: Decimal | None = None,
    note: str = "",
) -> StrategyLiveChartSnapshot:
    candle_items = tuple(candles)
    series: list[StrategyLiveChartOverlaySeries] = []
    used_periods: set[int] = set()
    close_values = [item.close for item in candle_items]
    for key, period, color in (
        ("ema_fast", ema_period, "#f59f00"),
        ("ema_trend", trend_ema_period, "#0b7285"),
        ("ema_reference", reference_ema_period, "#7c3aed"),
    ):
        if not candle_items:
            break
        normalized_period = _normalize_period(period)
        if normalized_period is None or normalized_period in used_periods:
            continue
        values = tuple(ema(close_values, normalized_period))
        if len(values) != len(candle_items):
            continue
        series.append(
            StrategyLiveChartOverlaySeries(
                key=key,
                label=f"EMA{normalized_period}",
                color=color,
                values=values,
            )
        )
        used_periods.add(normalized_period)

    markers: list[StrategyLiveChartMarker] = []
    seen_marker_keys: set[tuple[str, Decimal]] = set()
    unique_pending_entry_prices: list[Decimal] = []
    seen_pending_prices: set[Decimal] = set()
    for price in pending_entry_prices:
        if price in seen_pending_prices:
            continue
        seen_pending_prices.add(price)
        unique_pending_entry_prices.append(price)
    for index, price in enumerate(unique_pending_entry_prices, start=1):
        _append_marker(
            markers,
            seen_marker_keys,
            key=f"pending_{index}",
            label="挂单" if index == 1 else f"挂单{index}",
            price=price,
            color="#1d4ed8",
            dash=(4, 3),
        )
    _append_marker(
        markers,
        seen_marker_keys,
        key="entry",
        label="开仓均价",
        price=entry_price,
        color="#6f42c1",
    )
    if position_avg_price is not None and position_avg_price != entry_price:
        _append_marker(
            markers,
            seen_marker_keys,
            key="position_avg",
            label="持仓均价",
            price=position_avg_price,
            color="#8b5cf6",
        )
    _append_marker(
        markers,
        seen_marker_keys,
        key="stop",
        label="当前止损",
        price=stop_price,
        color="#cf222e",
        dash=(6, 3),
        width=2,
    )
    _append_marker(
        markers,
        seen_marker_keys,
        key="last",
        label="最新价",
        price=latest_price,
        color="#bf8700",
        dash=(10, 12),
    )
    resolved_time_markers: list[StrategyLiveChartTimeMarker] = list(time_markers)
    if entry_time is not None:
        existing_entry = any(marker.key == "entry_time" for marker in resolved_time_markers)
        if not existing_entry:
            resolved_time_markers.append(
                StrategyLiveChartTimeMarker(
                    key="entry_time",
                    label=f"开仓 {entry_time.strftime('%m-%d %H:%M')}",
                    at=entry_time,
                    color="#6f42c1",
                    dash=(4, 3),
                    width=2,
                )
            )

    latest_candle_time = None
    latest_candle_confirmed = True
    if candle_items:
        latest_candle_time = datetime.fromtimestamp(candle_items[-1].ts / 1000)
        latest_candle_confirmed = candle_items[-1].confirmed
        if latest_price is None:
            latest_price = candle_items[-1].close

    return StrategyLiveChartSnapshot(
        session_id=session_id,
        candles=candle_items,
        series=tuple(series),
        markers=tuple(markers),
        time_markers=tuple(resolved_time_markers),
        latest_price=latest_price,
        latest_candle_time=latest_candle_time,
        latest_candle_confirmed=latest_candle_confirmed,
        note=note,
    )


def build_auto_channel_live_chart_snapshot(
    *,
    session_id: str,
    candles: list[Candle] | tuple[Candle, ...],
    channel_config: ChannelDetectionConfig | None = None,
    box_config: BoxDetectionConfig | None = None,
    trendline_config: TrendlineDetectionConfig | None = None,
    triangle_config: TriangleDetectionConfig | None = None,
    max_channels: int = 1,
    max_boxes: int = 1,
    max_trendlines: int = 2,
    max_triangles: int = 1,
    show_pivots: bool = True,
    right_pad_bars: int = 0,
    channel_extend_bars: int = 0,
    latest_price: Decimal | None = None,
    note: str = "",
) -> StrategyLiveChartSnapshot:
    candle_items = tuple(candles)
    base = build_strategy_live_chart_snapshot(
        session_id=session_id,
        candles=candle_items,
        ema_period=None,
        trend_ema_period=None,
        reference_ema_period=None,
        latest_price=latest_price,
        note=note,
    )
    if not candle_items:
        return base

    resolved_trendline_config = trendline_config
    if resolved_trendline_config is None and channel_config is not None:
        resolved_trendline_config = TrendlineDetectionConfig(pivot=channel_config.pivot)
    resolved_triangle_config = triangle_config
    if resolved_triangle_config is None and channel_config is not None:
        resolved_triangle_config = TriangleDetectionConfig(pivot=channel_config.pivot)

    channels = tuple(detect_channels(candle_items, channel_config))[: max(0, max_channels)]
    boxes = tuple(detect_boxes(candle_items, box_config))[: max(0, max_boxes)]
    trendlines = tuple(detect_trendlines(candle_items, resolved_trendline_config))[: max(0, max_trendlines)]
    triangles = tuple(detect_triangles(candle_items, resolved_triangle_config))[: max(0, max_triangles)]
    pivot_config = channel_config.pivot if channel_config is not None else None
    pivots = tuple(detect_pivots(candle_items, pivot_config)) if show_pivots else ()

    pad_bars = max(0, int(right_pad_bars))
    extend_bars = max(0, int(channel_extend_bars))
    band_overlays = tuple(
        _channel_to_band_overlay(index, channel, extend_bars=extend_bars)
        for index, channel in enumerate(channels, start=1)
    )
    line_overlays = list(
        StrategyLiveChartLineOverlay(
            key=f"auto_channel_{index}_mid",
            label="通道中轴",
            line=_extend_price_line(channel.mid_line, end_index=channel.end_index + extend_bars),
            color="#2563eb",
            dash=(5, 4),
            width=1,
        )
        for index, channel in enumerate(channels, start=1)
    )
    line_overlays.extend(
        _trendline_to_overlay(index, trendline, extend_bars=extend_bars)
        for index, trendline in enumerate(trendlines, start=1)
    )
    line_overlays.extend(
        _triangle_upper_overlay(index, triangle, extend_bars=extend_bars)
        for index, triangle in enumerate(triangles, start=1)
    )
    line_overlays.extend(
        _triangle_lower_overlay(index, triangle, extend_bars=extend_bars)
        for index, triangle in enumerate(triangles, start=1)
    )
    box_overlays = tuple(_box_to_overlay(index, box) for index, box in enumerate(boxes, start=1))
    point_overlays = tuple(_pivot_to_point_overlay(index, pivot) for index, pivot in enumerate(pivots, start=1))
    summary = _auto_channel_snapshot_note(channels, boxes, trendlines, triangles, note)

    return StrategyLiveChartSnapshot(
        session_id=base.session_id,
        candles=base.candles,
        series=base.series,
        markers=base.markers,
        time_markers=base.time_markers,
        line_overlays=tuple(line_overlays),
        band_overlays=band_overlays,
        box_overlays=box_overlays,
        point_overlays=point_overlays,
        latest_price=base.latest_price,
        latest_candle_time=base.latest_candle_time,
        latest_candle_confirmed=base.latest_candle_confirmed,
        note=summary,
        series_plot_end_index=base.series_plot_end_index,
        price_display_tick=base.price_display_tick,
        right_pad_bars=pad_bars,
    )


def _extend_price_line(line: PriceLine, *, end_index: int) -> PriceLine:
    if end_index <= line.end_index:
        return line
    return PriceLine(
        start_index=line.start_index,
        start_price=line.start_price,
        end_index=end_index,
        end_price=line.value_at(end_index),
    )


def _channel_to_band_overlay(index: int, channel: ChannelCandidate, *, extend_bars: int = 0) -> StrategyLiveChartBandOverlay:
    upper_line, lower_line = _channel_upper_lower_lines(channel)
    extended_end_index = channel.end_index + max(0, int(extend_bars))
    upper_line = _extend_price_line(upper_line, end_index=extended_end_index)
    lower_line = _extend_price_line(lower_line, end_index=extended_end_index)
    return StrategyLiveChartBandOverlay(
        key=f"auto_channel_{index}",
        label="自动通道",
        start_index=channel.start_index,
        end_index=extended_end_index,
        upper_line=upper_line,
        lower_line=lower_line,
        outline="#2563eb",
        fill="#dbeafe",
    )


def _box_to_overlay(index: int, box: BoxCandidate) -> StrategyLiveChartBoxOverlay:
    return StrategyLiveChartBoxOverlay(
        key=f"auto_box_{index}",
        label="自动箱体",
        start_index=box.start_index,
        end_index=box.end_index,
        upper=box.upper,
        lower=box.lower,
        outline="#a855f7",
        fill="#f3e8ff",
    )


def _trendline_to_overlay(index: int, trendline: TrendlineCandidate, *, extend_bars: int = 0) -> StrategyLiveChartLineOverlay:
    color = "#f59e0b" if trendline.kind == "resistance" else "#10b981"
    label = "趋势压力线" if trendline.kind == "resistance" else "趋势支撑线"
    return StrategyLiveChartLineOverlay(
        key=f"trendline_{index}",
        label=label,
        line=_extend_price_line(trendline.line, end_index=trendline.end_index + max(0, int(extend_bars))),
        color=color,
        dash=(8, 4),
        width=2,
    )


def _triangle_upper_overlay(index: int, triangle: TriangleCandidate, *, extend_bars: int = 0) -> StrategyLiveChartLineOverlay:
    return StrategyLiveChartLineOverlay(
        key=f"triangle_{index}_upper",
        label="三角形上沿",
        line=_extend_price_line(triangle.upper_line, end_index=triangle.end_index + max(0, int(extend_bars))),
        color="#ec4899",
        dash=(3, 3),
        width=2,
    )


def _triangle_lower_overlay(index: int, triangle: TriangleCandidate, *, extend_bars: int = 0) -> StrategyLiveChartLineOverlay:
    return StrategyLiveChartLineOverlay(
        key=f"triangle_{index}_lower",
        label="三角形下沿",
        line=_extend_price_line(triangle.lower_line, end_index=triangle.end_index + max(0, int(extend_bars))),
        color="#ec4899",
        dash=(3, 3),
        width=2,
    )


def _pivot_to_point_overlay(index: int, pivot: PivotPoint) -> StrategyLiveChartPointOverlay:
    return StrategyLiveChartPointOverlay(
        key=f"pivot_{index}",
        label="高点" if pivot.kind == "high" else "低点",
        index=pivot.index,
        price=pivot.price,
        color="#dc2626" if pivot.kind == "high" else "#16a34a",
        radius=3.5,
    )


def _channel_upper_lower_lines(channel: ChannelCandidate) -> tuple[PriceLine, PriceLine]:
    base_at_start = channel.base_line.value_at(channel.start_index)
    parallel_at_start = channel.parallel_line.value_at(channel.start_index)
    if base_at_start >= parallel_at_start:
        return channel.base_line, channel.parallel_line
    return channel.parallel_line, channel.base_line


def _auto_channel_snapshot_note(
    channels: tuple[ChannelCandidate, ...],
    boxes: tuple[BoxCandidate, ...],
    trendlines: tuple[TrendlineCandidate, ...],
    triangles: tuple[TriangleCandidate, ...],
    note: str,
) -> str:
    parts: list[str] = []
    if note:
        parts.append(note)
    if channels:
        main = channels[0]
        label = "上升通道" if main.kind == "ascending" else "下降通道"
        parts.append(f"{label} | 触碰={main.touches} | 穿越={main.violations} | 评分={format_decimal(main.score)}")
    if boxes:
        main_box = boxes[0]
        parts.append(
            f"箱体 | 上沿触碰={main_box.upper_touches} | 下沿触碰={main_box.lower_touches} | 评分={format_decimal(main_box.score)}"
        )
    if trendlines:
        main_line = trendlines[0]
        label = "趋势压力线" if main_line.kind == "resistance" else "趋势支撑线"
        parts.append(f"{label} | 触碰={main_line.touches} | 评分={format_decimal(main_line.score)}")
    if triangles:
        main_triangle = triangles[0]
        triangle_label = {
            "symmetrical": "对称三角形",
            "ascending": "上升三角形",
            "descending": "下降三角形",
        }.get(main_triangle.kind, "三角形")
        parts.append(f"{triangle_label} | 触碰={main_triangle.touches} | 评分={format_decimal(main_triangle.score)}")
    if not channels and not boxes and not trendlines and not triangles:
        parts.append("暂无有效通道/箱体/三角形/趋势线")
    return " | ".join(parts)


def _format_snapshot_axis_price(snapshot: StrategyLiveChartSnapshot, price: Decimal) -> str:
    return format_decimal_by_increment(price, snapshot.price_display_tick)


def strategy_live_chart_price_bounds(
    snapshot: StrategyLiveChartSnapshot,
    *,
    bounds_policy: str = "full",
    desk_anchor_prices: tuple[Decimal, ...] | None = None,
) -> tuple[Decimal, Decimal]:
    """纵轴价格范围。full：K+均线+全部标记；desk：以 K 线 OHLC 为核心，均线离群点剔除，标记仅靠近核心区域时纳入。
    desk_anchor_prices：划线交易台用户画线/盈亏比锚点价，强制纳入 desk 纵轴，避免价在布局外被钳到图顶与 K 线脱节。"""
    ohlc: list[Decimal] = []
    for candle in snapshot.candles:
        ohlc.append(candle.high)
        ohlc.append(candle.low)
    if not ohlc:
        return Decimal("0"), Decimal("1")
    core_low = min(ohlc)
    core_high = max(ohlc)
    span0 = core_high - core_low
    if span0 <= 0:
        span0 = abs(core_low) * Decimal("0.0001") if core_low != 0 else Decimal("1")

    price_values: list[Decimal] = list(ohlc)
    plot_count = len(snapshot.candles) + max(0, int(snapshot.right_pad_bars))
    for series in snapshot.series:
        for raw in series.values:
            if not isinstance(raw, Decimal):
                continue
            if bounds_policy == "desk":
                lo_b = core_low - span0 * Decimal("2")
                hi_b = core_high + span0 * Decimal("2")
                if raw < lo_b or raw > hi_b:
                    continue
            price_values.append(raw)
    for line_overlay in snapshot.line_overlays:
        price_values.extend(_line_overlay_prices(line_overlay.line, plot_count))
    for band_overlay in snapshot.band_overlays:
        price_values.extend(_line_overlay_prices(band_overlay.upper_line, plot_count))
        price_values.extend(_line_overlay_prices(band_overlay.lower_line, plot_count))
    for box_overlay in snapshot.box_overlays:
        price_values.append(box_overlay.upper)
        price_values.append(box_overlay.lower)
    for point_overlay in snapshot.point_overlays:
        price_values.append(point_overlay.price)

    if not price_values:
        return Decimal("0"), Decimal("1")
    core_low = min(price_values)
    core_high = max(price_values)
    if core_low == core_high:
        padding = abs(core_low) * Decimal("0.02") if core_low != 0 else Decimal("1")
        c_lo, c_hi = core_low - padding, core_high + padding
    else:
        pad = (core_high - core_low) * Decimal("0.08")
        c_lo, c_hi = core_low - pad, core_high + pad
    if bounds_policy != "desk":
        merged = list(price_values)
        for marker in snapshot.markers:
            merged.append(marker.price)
        lower = min(merged)
        upper = max(merged)
        if lower == upper:
            padding = abs(lower) * Decimal("0.02") if lower != 0 else Decimal("1")
            return lower - padding, upper + padding
        padding = (upper - lower) * Decimal("0.08")
        lo, hi = lower - padding, upper + padding
        return (lo, hi) if lo <= hi else (hi, lo)
    span = c_hi - c_lo
    if span <= 0:
        span = abs(c_lo) * Decimal("0.001") if c_lo != 0 else Decimal("1")
    slack = span * Decimal("0.42")
    extras: list[Decimal] = []
    for marker in snapshot.markers:
        if c_lo - slack <= marker.price <= c_hi + slack:
            extras.append(marker.price)
    merged = [c_lo, c_hi] + extras
    for p in desk_anchor_prices or ():
        merged.append(p)
    lower = min(merged)
    upper = max(merged)
    if lower == upper:
        padding = abs(lower) * Decimal("0.02") if lower != 0 else Decimal("1")
        lo, hi = lower - padding, upper + padding
        return (lo, hi) if lo <= hi else (hi, lo)
    padding = (upper - lower) * Decimal("0.04")
    lo, hi = lower - padding, upper + padding
    return (lo, hi) if lo <= hi else (hi, lo)


def strategy_live_chart_canvas_layout(canvas: Canvas) -> tuple[int, int, bool]:
    """返回 (宽, 高, 是否可用于 K 线布局)。

    必须用 Tk 已映射的 ``winfo`` 尺寸。open+pack 后 ``winfo`` 常仍为 1，若用 ``cget`` 的默认
    width/height（如 980×620）冒充，则按「假画布」算 candle_step 与纵轴，首帧会与真实窗口错位、闪动。
    """
    win_w = max(0, int(canvas.winfo_width()))
    win_h = max(0, int(canvas.winfo_height()))
    if win_w >= CHART_CANVAS_MIN_MAPPED_W and win_h >= CHART_CANVAS_MIN_MAPPED_H:
        return win_w, win_h, True
    return win_w or 1, win_h or 1, False


def render_strategy_live_chart(
    canvas: Canvas,
    snapshot: StrategyLiveChartSnapshot,
    *,
    bounds_policy: str = "full",
    desk_anchor_prices: tuple[Decimal, ...] | None = None,
) -> bool:
    width, height, layout_ready = strategy_live_chart_canvas_layout(canvas)
    left = float(_CHART_INSET_LEFT)
    top = float(_CHART_INSET_TOP)
    right = float(width - _CHART_INSET_RIGHT_PAD)
    bottom = float(height - _CHART_INSET_BOTTOM)
    inner_w = right - left
    inner_h = bottom - top

    if snapshot.candles and (
        not layout_ready
        or width < CHART_CANVAS_MIN_MAPPED_W
        or height < CHART_CANVAS_MIN_MAPPED_H
        or inner_w <= 24
        or inner_h <= 24
    ):
        # 窗口尚未完成布局时跳过：避免清空画布后纵轴/网格在错误尺寸下计算，出现错乱与「过一会才好」。
        return False

    if not snapshot.candles:
        width = max(width, 320)
        height = max(height, 240)

    canvas.delete("all")
    canvas.configure(background="#ffffff")

    if not snapshot.candles:
        canvas.create_text(
            width / 2,
            height / 2 - 12,
            text="暂无可绘制的K线数据",
            fill=_TEXT_COLOR,
            font=("Microsoft YaHei UI", 12, "bold"),
        )
        if snapshot.note:
            canvas.create_text(
                width / 2,
                height / 2 + 16,
                text=snapshot.note,
                fill=_MUTED_TEXT_COLOR,
                font=("Microsoft YaHei UI", 10),
            )
        return True

    bounds = _ChartBounds(left=left, top=top, right=right, bottom=bottom)

    lower, upper = strategy_live_chart_price_bounds(
        snapshot, bounds_policy=bounds_policy, desk_anchor_prices=desk_anchor_prices
    )
    plot_count = len(snapshot.candles) + max(0, int(snapshot.right_pad_bars))
    candle_step = bounds.width / max(plot_count, 1)
    candle_body_half_width = max(min(candle_step * 0.28, 12.0), 2.5)

    for line_index in range(6):
        ratio = line_index / 5 if 5 else 0
        y = bounds.top + bounds.height * ratio
        canvas.create_line(bounds.left, y, bounds.right, y, fill=_GRID_COLOR, dash=(2, 4))
        price = upper - (upper - lower) * Decimal(str(ratio))
        canvas.create_text(
            bounds.left - 8,
            y,
            text=_format_snapshot_axis_price(snapshot, price),
            fill=_MUTED_TEXT_COLOR,
            anchor="e",
            font=("Microsoft YaHei UI", 9),
        )

    for index in _axis_marker_indexes(len(snapshot.candles), target_count=6):
        x = bounds.left + (index + 0.5) * candle_step
        canvas.create_line(x, bounds.top, x, bounds.bottom, fill="#f3f4f6", dash=(2, 4))
        canvas.create_text(
            x,
            bounds.bottom + 18,
            text=_format_chart_timestamp(snapshot.candles[index].ts),
            fill=_MUTED_TEXT_COLOR,
            anchor="n",
            font=("Microsoft YaHei UI", 9),
        )

    for time_marker in snapshot.time_markers:
        x = _time_marker_x(time_marker.at, snapshot, bounds, candle_step)
        line_kwargs = {
            "fill": time_marker.color,
            "width": time_marker.width,
        }
        if time_marker.dash:
            line_kwargs["dash"] = time_marker.dash
        canvas.create_line(x, bounds.top, x, bounds.bottom, **line_kwargs)
        label_text = time_marker.label
        text_width = max(len(label_text) * 7 + 14, 88)
        x1 = min(max(bounds.left, x - text_width / 2), max(bounds.left, bounds.right - text_width))
        x2 = x1 + text_width
        y1 = max(6, bounds.top - 28)
        y2 = y1 + 18
        canvas.create_rectangle(x1, y1, x2, y2, outline=time_marker.color, fill=_PRICE_LABEL_BG)
        canvas.create_text(
            x1 + 6,
            y1 + 9,
            text=label_text,
            fill=time_marker.color,
            anchor="w",
            font=("Microsoft YaHei UI", 9),
        )

    if not snapshot.latest_candle_confirmed:
        latest_left = bounds.left + max(len(snapshot.candles) - 1, 0) * candle_step
        latest_right = min(bounds.right, latest_left + candle_step)
        canvas.create_rectangle(
            latest_left,
            bounds.top,
            latest_right,
            bounds.bottom,
            outline="",
            fill="#fff7d6",
        )

    for box_overlay in snapshot.box_overlays:
        _draw_box_overlay(canvas, box_overlay, lower, upper, bounds, candle_step)
    for band_overlay in snapshot.band_overlays:
        _draw_band_overlay(canvas, band_overlay, lower, upper, bounds, candle_step)

    for index, candle in enumerate(snapshot.candles):
        x = bounds.left + (index + 0.5) * candle_step
        high_y = _price_to_y(candle.high, lower, upper, bounds)
        low_y = _price_to_y(candle.low, lower, upper, bounds)
        open_y = _price_to_y(candle.open, lower, upper, bounds)
        close_y = _price_to_y(candle.close, lower, upper, bounds)
        color = _candle_color(candle)
        canvas.create_line(x, high_y, x, low_y, fill=color, width=1)
        body_top = min(open_y, close_y)
        body_bottom = max(open_y, close_y)
        if abs(body_bottom - body_top) < 1.2:
            canvas.create_line(x - candle_body_half_width, body_top, x + candle_body_half_width, body_bottom, fill=color, width=2)
        else:
            fill = color if candle.confirmed else "#ffffff"
            canvas.create_rectangle(
                x - candle_body_half_width,
                body_top,
                x + candle_body_half_width,
                body_bottom,
                outline=color,
                fill=fill,
                width=1,
            )

    legend_x = bounds.left
    plot_end = len(snapshot.candles)
    if snapshot.series_plot_end_index is not None:
        plot_end = max(0, min(int(snapshot.series_plot_end_index), len(snapshot.candles)))
    for series in snapshot.series:
        points: list[float] = []
        n_pts = min(len(series.values), plot_end)
        for index in range(n_pts):
            value = series.values[index]
            x = bounds.left + (index + 0.5) * candle_step
            y = _price_to_y(value, lower, upper, bounds)
            points.extend((x, y))
        if len(points) >= 4:
            canvas.create_line(*points, fill=series.color, width=2, smooth=True)
        canvas.create_line(legend_x, 18, legend_x + 18, 18, fill=series.color, width=3)
        canvas.create_text(
            legend_x + 24,
            18,
            text=series.label,
            fill=_TEXT_COLOR,
            anchor="w",
            font=("Microsoft YaHei UI", 9),
        )
        legend_x += 92

    for band_overlay in snapshot.band_overlays:
        _draw_line_overlay(canvas, StrategyLiveChartLineOverlay(
            key=f"{band_overlay.key}_upper",
            label=f"{band_overlay.label} upper",
            line=band_overlay.upper_line,
            color=band_overlay.outline,
            width=2,
        ), lower, upper, bounds, candle_step)
        _draw_line_overlay(canvas, StrategyLiveChartLineOverlay(
            key=f"{band_overlay.key}_lower",
            label=f"{band_overlay.label} lower",
            line=band_overlay.lower_line,
            color=band_overlay.outline,
            width=2,
        ), lower, upper, bounds, candle_step)
    for line_overlay in snapshot.line_overlays:
        _draw_line_overlay(canvas, line_overlay, lower, upper, bounds, candle_step)

    for point_overlay in snapshot.point_overlays:
        _draw_point_overlay(canvas, point_overlay, lower, upper, bounds, candle_step)

    marker_layouts = _layout_marker_label_positions(
        snapshot.markers,
        lower,
        upper,
        bounds,
        height=height,
        bounds_policy=bounds_policy,
    )
    for marker, y, label_y in marker_layouts:
        line_kwargs = {
            "fill": marker.color,
            "width": marker.width,
        }
        if marker.dash:
            line_kwargs["dash"] = marker.dash
        canvas.create_line(bounds.left, y, bounds.right, y, **line_kwargs)
        label_text = f"{marker.label} {_format_snapshot_axis_price(snapshot, marker.price)}"
        if bounds_policy == "desk" and (marker.price < lower or marker.price > upper):
            label_text = f"{label_text}·界外"
        text_width = max(len(label_text) * 7 + 14, 82)
        x1 = bounds.right + 10
        x2 = min(width - 8, x1 + text_width)
        y1 = max(_MARKER_LABEL_CANVAS_MARGIN, label_y - _MARKER_LABEL_HALF_HEIGHT)
        y2 = min(height - _MARKER_LABEL_CANVAS_MARGIN, label_y + _MARKER_LABEL_HALF_HEIGHT)
        if abs(label_y - y) > 0.5:
            canvas.create_line(bounds.right, y, x1, label_y, fill=marker.color, width=1)
        canvas.create_rectangle(x1, y1, x2, y2, outline=marker.color, fill=_PRICE_LABEL_BG)
        canvas.create_text(
            x1 + 6,
            label_y,
            text=label_text,
            fill=marker.color,
            anchor="w",
            font=("Microsoft YaHei UI", 9),
        )

    footer_parts = [f"K线 {len(snapshot.candles)} 根"]
    if snapshot.latest_candle_time is not None:
        candle_state = "已收盘" if snapshot.latest_candle_confirmed else "未收盘"
        footer_parts.append(f"最新 {snapshot.latest_candle_time.strftime('%m-%d %H:%M')} ({candle_state})")
    if snapshot.note:
        footer_parts.append(snapshot.note)
    canvas.create_text(
        bounds.left,
        height - 18,
        text=" | ".join(footer_parts),
        fill=_MUTED_TEXT_COLOR,
        anchor="w",
        font=("Microsoft YaHei UI", 9),
    )
    return True


def _line_overlay_prices(line: PriceLine, candle_count: int) -> list[Decimal]:
    if candle_count <= 0:
        return []
    start_index = max(0, min(line.start_index, candle_count - 1))
    end_index = max(0, min(line.end_index, candle_count - 1))
    if start_index > end_index:
        start_index, end_index = end_index, start_index
    return [line.value_at(start_index), line.value_at(end_index)]


def _index_to_x(index: int, bounds: _ChartBounds, candle_step: float) -> float:
    return bounds.left + (index + 0.5) * candle_step


def _draw_line_overlay(
    canvas: Canvas,
    overlay: StrategyLiveChartLineOverlay,
    lower: Decimal,
    upper: Decimal,
    bounds: _ChartBounds,
    candle_step: float,
) -> None:
    candle_count = max(round(bounds.width / max(candle_step, 1e-9)), 0)
    if candle_count <= 0:
        return
    start_index = max(0, min(overlay.line.start_index, candle_count - 1))
    end_index = max(0, min(overlay.line.end_index, candle_count - 1))
    if start_index == end_index:
        return
    line_kwargs = {"fill": overlay.color, "width": overlay.width}
    if overlay.dash:
        line_kwargs["dash"] = overlay.dash
    canvas.create_line(
        _index_to_x(start_index, bounds, candle_step),
        _price_to_y(overlay.line.value_at(start_index), lower, upper, bounds),
        _index_to_x(end_index, bounds, candle_step),
        _price_to_y(overlay.line.value_at(end_index), lower, upper, bounds),
        **line_kwargs,
    )


def _draw_band_overlay(
    canvas: Canvas,
    overlay: StrategyLiveChartBandOverlay,
    lower: Decimal,
    upper: Decimal,
    bounds: _ChartBounds,
    candle_step: float,
) -> None:
    start_index = max(0, int(overlay.start_index))
    end_index = max(start_index, int(overlay.end_index))
    x1 = _index_to_x(start_index, bounds, candle_step)
    x2 = _index_to_x(end_index, bounds, candle_step)
    upper_start = _price_to_y(overlay.upper_line.value_at(start_index), lower, upper, bounds)
    upper_end = _price_to_y(overlay.upper_line.value_at(end_index), lower, upper, bounds)
    lower_end = _price_to_y(overlay.lower_line.value_at(end_index), lower, upper, bounds)
    lower_start = _price_to_y(overlay.lower_line.value_at(start_index), lower, upper, bounds)
    canvas.create_polygon(
        x1,
        upper_start,
        x2,
        upper_end,
        x2,
        lower_end,
        x1,
        lower_start,
        outline=overlay.outline,
        fill=overlay.fill,
        stipple=overlay.stipple,
    )


def _draw_box_overlay(
    canvas: Canvas,
    overlay: StrategyLiveChartBoxOverlay,
    lower: Decimal,
    upper: Decimal,
    bounds: _ChartBounds,
    candle_step: float,
) -> None:
    x1 = _index_to_x(max(0, int(overlay.start_index)), bounds, candle_step)
    x2 = _index_to_x(max(int(overlay.start_index), int(overlay.end_index)), bounds, candle_step)
    y1 = _price_to_y(overlay.upper, lower, upper, bounds)
    y2 = _price_to_y(overlay.lower, lower, upper, bounds)
    canvas.create_rectangle(
        x1,
        y1,
        x2,
        y2,
        outline=overlay.outline,
        fill=overlay.fill,
        stipple=overlay.stipple,
        width=2,
    )


def _draw_point_overlay(
    canvas: Canvas,
    overlay: StrategyLiveChartPointOverlay,
    lower: Decimal,
    upper: Decimal,
    bounds: _ChartBounds,
    candle_step: float,
) -> None:
    if overlay.index < 0:
        return
    x = _index_to_x(overlay.index, bounds, candle_step)
    y = _price_to_y(overlay.price, lower, upper, bounds)
    r = float(overlay.radius)
    canvas.create_oval(x - r, y - r, x + r, y + r, outline=overlay.color, fill="#ffffff", width=2)


def _append_marker(
    target: list[StrategyLiveChartMarker],
    seen: set[tuple[str, Decimal]],
    *,
    key: str,
    label: str,
    price: Decimal | None,
    color: str,
    dash: tuple[int, ...] = (),
    width: int = 1,
) -> None:
    if price is None:
        return
    dedupe_key = (label, price)
    if dedupe_key in seen:
        return
    seen.add(dedupe_key)
    target.append(StrategyLiveChartMarker(key=key, label=label, price=price, color=color, dash=dash, width=width))


def _normalize_period(value: int | None) -> int | None:
    if value is None:
        return None
    if value <= 0:
        return None
    return int(value)


def _axis_marker_indexes(count: int, *, target_count: int) -> list[int]:
    if count <= 0:
        return []
    if count <= target_count:
        return list(range(count))
    last_index = count - 1
    indexes = {0, last_index}
    step = last_index / max(target_count - 1, 1)
    for marker_index in range(1, target_count - 1):
        indexes.add(min(last_index, max(0, round(step * marker_index))))
    return sorted(indexes)


def _price_to_y(price: Decimal, lower: Decimal, upper: Decimal, bounds: _ChartBounds) -> float:
    if upper <= lower:
        return bounds.top + bounds.height / 2
    ratio = float((upper - price) / (upper - lower))
    ratio = min(max(ratio, 0.0), 1.0)
    return bounds.top + bounds.height * ratio


def _layout_marker_label_positions(
    markers: tuple[StrategyLiveChartMarker, ...],
    lower: Decimal,
    upper: Decimal,
    bounds: _ChartBounds,
    *,
    height: int,
    bounds_policy: str,
) -> list[tuple[StrategyLiveChartMarker, float, float]]:
    if not markers:
        return []
    top_limit = max(_MARKER_LABEL_CANVAS_MARGIN + _MARKER_LABEL_HALF_HEIGHT, bounds.top)
    bottom_limit = min(height - _MARKER_LABEL_CANVAS_MARGIN - _MARKER_LABEL_HALF_HEIGHT, bounds.bottom)
    if bottom_limit < top_limit:
        fallback = bounds.top + bounds.height / 2
        return [(marker, fallback, fallback) for marker in markers]

    placements: list[list[object]] = []
    for marker in markers:
        line_y = _price_to_y(marker.price, lower, upper, bounds)
        if bounds_policy == "desk":
            line_y = min(max(line_y, bounds.top + 1.0), bounds.bottom - 1.0)
        label_y = min(max(line_y, top_limit), bottom_limit)
        placements.append([marker, line_y, label_y])
    placements.sort(key=lambda item: float(item[2]))

    for index in range(1, len(placements)):
        prev_y = float(placements[index - 1][2])
        current_y = float(placements[index][2])
        if current_y - prev_y < _MARKER_LABEL_MIN_GAP:
            placements[index][2] = min(bottom_limit, prev_y + _MARKER_LABEL_MIN_GAP)
    for index in range(len(placements) - 2, -1, -1):
        next_y = float(placements[index + 1][2])
        current_y = float(placements[index][2])
        if next_y - current_y < _MARKER_LABEL_MIN_GAP:
            placements[index][2] = max(top_limit, next_y - _MARKER_LABEL_MIN_GAP)

    return [(item[0], float(item[1]), float(item[2])) for item in placements]


def _time_marker_x(
    at: datetime,
    snapshot: StrategyLiveChartSnapshot,
    bounds: _ChartBounds,
    candle_step: float,
) -> float:
    if not snapshot.candles:
        return bounds.left
    target_ms = at.timestamp() * 1000
    closest_index = min(
        range(len(snapshot.candles)),
        key=lambda index: abs(snapshot.candles[index].ts - target_ms),
    )
    return bounds.left + (closest_index + 0.5) * candle_step


def _format_chart_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _candle_color(candle: Candle) -> str:
    if not candle.confirmed:
        return _CANDLE_PENDING
    if candle.close >= candle.open:
        return _CANDLE_RISE
    return _CANDLE_FALL


@dataclass(frozen=True)
class StrategyLiveChartLayout:
    """Pixel + price mapping for the inner chart area (matches render_strategy_live_chart)."""

    width: int
    height: int
    left: float
    top: float
    right: float
    bottom: float
    lower: Decimal
    upper: Decimal
    candle_step: float
    candle_count: int


def measure_strategy_live_chart_canvas(canvas: Canvas) -> tuple[int, int]:
    w, h, ok = strategy_live_chart_canvas_layout(canvas)
    if ok:
        return w, h
    return max(w, 1), max(h, 1)


def compute_strategy_live_chart_layout(
    canvas: Canvas,
    snapshot: StrategyLiveChartSnapshot,
    *,
    bounds_policy: str = "full",
    desk_anchor_prices: tuple[Decimal, ...] | None = None,
) -> StrategyLiveChartLayout | None:
    if not snapshot.candles:
        return None
    width, height = measure_strategy_live_chart_canvas(canvas)
    left = float(_CHART_INSET_LEFT)
    top = float(_CHART_INSET_TOP)
    right = float(width - _CHART_INSET_RIGHT_PAD)
    bottom = float(height - _CHART_INSET_BOTTOM)
    if right - left <= 24 or bottom - top <= 24:
        return None
    lower, upper = strategy_live_chart_price_bounds(
        snapshot, bounds_policy=bounds_policy, desk_anchor_prices=desk_anchor_prices
    )
    n = len(snapshot.candles)
    candle_step = (right - left) / max(n, 1)
    return StrategyLiveChartLayout(
        width=width,
        height=height,
        left=left,
        top=top,
        right=right,
        bottom=bottom,
        lower=lower,
        upper=upper,
        candle_step=candle_step,
        candle_count=n + max(0, int(snapshot.right_pad_bars)),
    )


def append_candles_to_snapshot(
    snapshot: StrategyLiveChartSnapshot,
    candles: list[Candle] | tuple[Candle, ...],
) -> StrategyLiveChartSnapshot:
    merged = {item.ts: item for item in snapshot.candles}
    for candle in candles:
        merged[int(candle.ts)] = candle
    candle_items = tuple(sorted(merged.values(), key=lambda item: item.ts))
    latest_price = snapshot.latest_price
    latest_candle_time = snapshot.latest_candle_time
    latest_candle_confirmed = snapshot.latest_candle_confirmed
    if candle_items:
        latest_price = candle_items[-1].close
        latest_candle_time = datetime.fromtimestamp(candle_items[-1].ts / 1000)
        latest_candle_confirmed = candle_items[-1].confirmed
    markers = tuple(_replace_last_price_marker(snapshot.markers, latest_price))
    return StrategyLiveChartSnapshot(
        session_id=snapshot.session_id,
        candles=candle_items,
        series=snapshot.series,
        markers=markers,
        time_markers=snapshot.time_markers,
        line_overlays=snapshot.line_overlays,
        band_overlays=snapshot.band_overlays,
        box_overlays=snapshot.box_overlays,
        point_overlays=snapshot.point_overlays,
        latest_price=latest_price,
        latest_candle_time=latest_candle_time,
        latest_candle_confirmed=latest_candle_confirmed,
        note=snapshot.note,
        series_plot_end_index=snapshot.series_plot_end_index,
        price_display_tick=snapshot.price_display_tick,
        right_pad_bars=snapshot.right_pad_bars,
    )


def _replace_last_price_marker(
    markers: tuple[StrategyLiveChartMarker, ...],
    latest_price: Decimal | None,
) -> list[StrategyLiveChartMarker]:
    if latest_price is None:
        return list(markers)
    out: list[StrategyLiveChartMarker] = []
    replaced = False
    for marker in markers:
        if marker.key == "last":
            out.append(
                StrategyLiveChartMarker(
                    key=marker.key,
                    label=marker.label,
                    price=latest_price,
                    color=marker.color,
                    dash=marker.dash,
                    width=marker.width,
                )
            )
            replaced = True
        else:
            out.append(marker)
    if not replaced:
        out.append(
            StrategyLiveChartMarker(
                key="last",
                label="最新价",
                price=latest_price,
                color="#bf8700",
                dash=(10, 12),
            )
        )
    return out


def _slice_price_line(line: PriceLine, start: int, end: int) -> PriceLine | None:
    left = max(line.start_index, start)
    right = min(line.end_index, end - 1)
    if right <= left:
        return None
    return PriceLine(
        start_index=left - start,
        start_price=line.value_at(left),
        end_index=right - start,
        end_price=line.value_at(right),
    )


def _slice_line_overlays(
    overlays: tuple[StrategyLiveChartLineOverlay, ...],
    start: int,
    end: int,
) -> tuple[StrategyLiveChartLineOverlay, ...]:
    sliced: list[StrategyLiveChartLineOverlay] = []
    for overlay in overlays:
        line = _slice_price_line(overlay.line, start, end)
        if line is None:
            continue
        sliced.append(
            StrategyLiveChartLineOverlay(
                key=overlay.key,
                label=overlay.label,
                line=line,
                color=overlay.color,
                dash=overlay.dash,
                width=overlay.width,
            )
        )
    return tuple(sliced)


def _slice_band_overlays(
    overlays: tuple[StrategyLiveChartBandOverlay, ...],
    start: int,
    end: int,
) -> tuple[StrategyLiveChartBandOverlay, ...]:
    sliced: list[StrategyLiveChartBandOverlay] = []
    for overlay in overlays:
        left = max(overlay.start_index, start)
        right = min(overlay.end_index, end - 1)
        if right <= left:
            continue
        upper_line = _slice_price_line(overlay.upper_line, start, end)
        lower_line = _slice_price_line(overlay.lower_line, start, end)
        if upper_line is None or lower_line is None:
            continue
        sliced.append(
            StrategyLiveChartBandOverlay(
                key=overlay.key,
                label=overlay.label,
                start_index=left - start,
                end_index=right - start,
                upper_line=upper_line,
                lower_line=lower_line,
                outline=overlay.outline,
                fill=overlay.fill,
                stipple=overlay.stipple,
            )
        )
    return tuple(sliced)


def _slice_box_overlays(
    overlays: tuple[StrategyLiveChartBoxOverlay, ...],
    start: int,
    end: int,
) -> tuple[StrategyLiveChartBoxOverlay, ...]:
    sliced: list[StrategyLiveChartBoxOverlay] = []
    for overlay in overlays:
        left = max(overlay.start_index, start)
        right = min(overlay.end_index, end - 1)
        if right <= left:
            continue
        sliced.append(
            StrategyLiveChartBoxOverlay(
                key=overlay.key,
                label=overlay.label,
                start_index=left - start,
                end_index=right - start,
                upper=overlay.upper,
                lower=overlay.lower,
                outline=overlay.outline,
                fill=overlay.fill,
                stipple=overlay.stipple,
            )
        )
    return tuple(sliced)


def _slice_point_overlays(
    overlays: tuple[StrategyLiveChartPointOverlay, ...],
    start: int,
    end: int,
) -> tuple[StrategyLiveChartPointOverlay, ...]:
    sliced: list[StrategyLiveChartPointOverlay] = []
    for overlay in overlays:
        if not start <= overlay.index < end:
            continue
        sliced.append(
            StrategyLiveChartPointOverlay(
                key=overlay.key,
                label=overlay.label,
                index=overlay.index - start,
                price=overlay.price,
                color=overlay.color,
                radius=overlay.radius,
            )
        )
    return tuple(sliced)


def slice_strategy_live_chart_snapshot(
    snapshot: StrategyLiveChartSnapshot,
    start: int,
    count: int,
) -> StrategyLiveChartSnapshot:
    n = len(snapshot.candles)
    if n == 0 or count <= 0:
        return StrategyLiveChartSnapshot(
            session_id=snapshot.session_id,
            candles=(),
            series=(),
            markers=snapshot.markers,
            time_markers=snapshot.time_markers,
            line_overlays=(),
            band_overlays=(),
            box_overlays=(),
            point_overlays=(),
            latest_price=snapshot.latest_price,
            latest_candle_time=snapshot.latest_candle_time,
            latest_candle_confirmed=snapshot.latest_candle_confirmed,
            note=snapshot.note,
            price_display_tick=snapshot.price_display_tick,
            right_pad_bars=0,
        )
    start = max(0, min(int(start), max(0, n - 1)))
    end = min(n, start + int(count))
    candles = snapshot.candles[start:end]
    new_series: list[StrategyLiveChartOverlaySeries] = []
    for series in snapshot.series:
        if len(series.values) == n:
            new_series.append(
                StrategyLiveChartOverlaySeries(
                    key=series.key,
                    label=series.label,
                    color=series.color,
                    values=series.values[start:end],
                )
            )
    return StrategyLiveChartSnapshot(
        session_id=snapshot.session_id,
        candles=candles,
        series=tuple(new_series),
        markers=snapshot.markers,
        time_markers=snapshot.time_markers,
        line_overlays=_slice_line_overlays(snapshot.line_overlays, start, end),
        band_overlays=_slice_band_overlays(snapshot.band_overlays, start, end),
        box_overlays=_slice_box_overlays(snapshot.box_overlays, start, end),
        point_overlays=_slice_point_overlays(snapshot.point_overlays, start, end),
        latest_price=snapshot.latest_price,
        latest_candle_time=snapshot.latest_candle_time,
        latest_candle_confirmed=snapshot.latest_candle_confirmed,
        note=snapshot.note,
        price_display_tick=snapshot.price_display_tick,
        right_pad_bars=0,
    )


def line_trading_desk_visible_bar_count(
    candle_count: int, desk_visible_bars: int, *, min_bars: int = 30
) -> int:
    if candle_count <= 0:
        return 0
    lo = max(1, int(min_bars))
    return max(lo, min(int(desk_visible_bars), candle_count))


def line_trading_desk_max_view_start(
    candle_count: int,
    visible_bars: int,
    *,
    max_right_pad_bars: int = LINE_TRADING_DESK_MAX_RIGHT_PAD_BARS,
    min_visible_bars: int = 30,
) -> int:
    """允许向右拖出空白：view_start 可大于 n - visible_bars，上限受 max_right_pad_bars 约束。"""
    n = candle_count
    vb = line_trading_desk_visible_bar_count(n, visible_bars, min_bars=min_visible_bars)
    if n <= 0 or vb <= 0:
        return 0
    tail = min(int(max_right_pad_bars), max(40, vb))
    return min(max(0, n - 1), max(0, n - vb) + tail)


def _infer_bar_interval_ts(candles: tuple[Candle, ...]) -> int:
    if len(candles) >= 2:
        return max(1, int(candles[-1].ts) - int(candles[-2].ts))
    return 60_000


def slice_strategy_live_chart_snapshot_with_desk_right_pad(
    snapshot: StrategyLiveChartSnapshot,
    view_start: int,
    view_bars: int,
    *,
    max_right_pad_bars: int = LINE_TRADING_DESK_MAX_RIGHT_PAD_BARS,
    min_visible_bars: int = 30,
) -> StrategyLiveChartSnapshot:
    """与 slice_strategy_live_chart_snapshot 类似，但右侧不足 view_bars 时用平盘占位 K 填满（画布留白）。"""
    n = len(snapshot.candles)
    vb = line_trading_desk_visible_bar_count(n, view_bars, min_bars=min_visible_bars)
    if n == 0 or vb <= 0:
        return StrategyLiveChartSnapshot(
            session_id=snapshot.session_id,
            candles=(),
            series=(),
            markers=snapshot.markers,
            time_markers=snapshot.time_markers,
            line_overlays=(),
            band_overlays=(),
            box_overlays=(),
            point_overlays=(),
            latest_price=snapshot.latest_price,
            latest_candle_time=snapshot.latest_candle_time,
            latest_candle_confirmed=snapshot.latest_candle_confirmed,
            note=snapshot.note,
            series_plot_end_index=None,
            price_display_tick=snapshot.price_display_tick,
            right_pad_bars=0,
        )
    vs_max = line_trading_desk_max_view_start(
        n, vb, max_right_pad_bars=max_right_pad_bars, min_visible_bars=min_visible_bars
    )
    vs = max(0, min(int(view_start), vs_max))
    end_real = min(n, vs + vb)
    real = list(snapshot.candles[vs:end_real])
    pad_n = vb - len(real)
    if pad_n <= 0:
        return slice_strategy_live_chart_snapshot(snapshot, vs, vb)
    anchor = real[-1] if real else snapshot.candles[-1]
    p = anchor.close
    gap = _infer_bar_interval_ts(snapshot.candles)
    ghosts: list[Candle] = []
    for i in range(1, pad_n + 1):
        ts_i = int(anchor.ts) + i * gap
        ghosts.append(
            Candle(
                ts=ts_i,
                open=p,
                high=p,
                low=p,
                close=p,
                volume=Decimal("0"),
                confirmed=True,
            )
        )
    candles = tuple(real + ghosts)
    new_series: list[StrategyLiveChartOverlaySeries] = []
    for series in snapshot.series:
        if len(series.values) != n:
            continue
        seg = list(series.values[vs:end_real])
        if pad_n > 0:
            pad_val = seg[-1] if seg else series.values[min(vs, n - 1)]
            seg.extend([pad_val] * pad_n)
        new_series.append(
            StrategyLiveChartOverlaySeries(
                key=series.key,
                label=series.label,
                color=series.color,
                values=tuple(seg),
            )
        )
    return StrategyLiveChartSnapshot(
        session_id=snapshot.session_id,
        candles=candles,
        series=tuple(new_series),
        markers=snapshot.markers,
        time_markers=snapshot.time_markers,
        line_overlays=_slice_line_overlays(snapshot.line_overlays, vs, end_real),
        band_overlays=_slice_band_overlays(snapshot.band_overlays, vs, end_real),
        box_overlays=_slice_box_overlays(snapshot.box_overlays, vs, end_real),
        point_overlays=_slice_point_overlays(snapshot.point_overlays, vs, end_real),
        latest_price=snapshot.latest_price,
        latest_candle_time=snapshot.latest_candle_time,
        latest_candle_confirmed=snapshot.latest_candle_confirmed,
        note=snapshot.note,
        series_plot_end_index=len(real),
        price_display_tick=snapshot.price_display_tick,
        right_pad_bars=0,
    )


def layout_price_to_y(layout: StrategyLiveChartLayout, price: Decimal) -> float:
    if layout.upper <= layout.lower:
        return layout.top + (layout.bottom - layout.top) / 2
    ratio = float((layout.upper - price) / (layout.upper - layout.lower))
    ratio = min(max(ratio, 0.0), 1.0)
    return layout.top + (layout.bottom - layout.top) * ratio


def layout_price_to_y_unclamped(layout: StrategyLiveChartLayout, price: Decimal) -> float:
    """与 `layout_price_to_y` 相同线性关系，但不把 ratio 限制在 0～1。

    价落在当前纵轴上下界之外时，y 可超出图表内框，用于趋势线/射线在像素上保持真实斜率；
    若用 `layout_price_to_y` 则两端常被钳到同一边，延伸段会变成水平折线。
    """
    if layout.upper <= layout.lower:
        return float(layout.top + (layout.bottom - layout.top) / 2)
    ratio = float((layout.upper - price) / (layout.upper - layout.lower))
    return float(layout.top + (layout.bottom - layout.top) * ratio)


def layout_price_to_y_clamped(layout: StrategyLiveChartLayout, price: Decimal) -> float:
    y = layout_price_to_y(layout, price)
    t = float(layout.top) + 0.5
    b = float(layout.bottom) - 0.5
    if b <= t:
        return float(layout.top + layout.bottom) / 2.0
    return float(min(max(y, t), b))


def layout_y_to_price(layout: StrategyLiveChartLayout, y: float) -> Decimal:
    h = layout.bottom - layout.top
    if h <= 0:
        return (layout.lower + layout.upper) / Decimal("2")
    ratio = Decimal(str(min(max((y - layout.top) / h, 0.0), 1.0)))
    return layout.upper - (layout.upper - layout.lower) * ratio


def layout_bar_index_to_x_center(layout: StrategyLiveChartLayout, bar_index: float) -> float:
    return layout.left + (bar_index + 0.5) * layout.candle_step


def layout_pixel_to_bar_index(layout: StrategyLiveChartLayout, x: float) -> float:
    return (x - layout.left) / layout.candle_step - 0.5


def line_price_through_anchors(
    bar_a: float,
    price_a: Decimal,
    bar_b: float,
    price_b: Decimal,
    bar_t: float,
) -> Decimal:
    da = Decimal(str(bar_b)) - Decimal(str(bar_a))
    if da.copy_abs() < Decimal("1e-12"):
        return price_a
    dt = Decimal(str(bar_t)) - Decimal(str(bar_a))
    return price_a + dt * (price_b - price_a) / da
