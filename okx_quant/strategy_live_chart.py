from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from tkinter import Canvas

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
class StrategyLiveChartSnapshot:
    session_id: str
    candles: tuple[Candle, ...]
    series: tuple[StrategyLiveChartOverlaySeries, ...] = ()
    markers: tuple[StrategyLiveChartMarker, ...] = ()
    time_markers: tuple[StrategyLiveChartTimeMarker, ...] = ()
    latest_price: Decimal | None = None
    latest_candle_time: datetime | None = None
    latest_candle_confirmed: bool = True
    note: str = ""
    #: 若设置：均线等 overlay 只绘制到该索引之前（不含），用于右侧占位 K 不画均线。
    series_plot_end_index: int | None = None
    #: 若设置：纵轴价签、右侧标记价等按该最小变动单位格式化（与合约 tick 一致）。
    price_display_tick: Decimal | None = None


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
    candle_step = bounds.width / max(len(snapshot.candles), 1)
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

    for marker in snapshot.markers:
        y = _price_to_y(marker.price, lower, upper, bounds)
        if bounds_policy == "desk":
            y = min(max(y, bounds.top + 1.0), bounds.bottom - 1.0)
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
        y1 = max(6, y - 10)
        y2 = min(height - 6, y + 10)
        canvas.create_rectangle(x1, y1, x2, y2, outline=marker.color, fill=_PRICE_LABEL_BG)
        canvas.create_text(
            x1 + 6,
            y,
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
        candle_count=n,
    )


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
            latest_price=snapshot.latest_price,
            latest_candle_time=snapshot.latest_candle_time,
            latest_candle_confirmed=snapshot.latest_candle_confirmed,
            note=snapshot.note,
            price_display_tick=snapshot.price_display_tick,
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
        latest_price=snapshot.latest_price,
        latest_candle_time=snapshot.latest_candle_time,
        latest_candle_confirmed=snapshot.latest_candle_confirmed,
        note=snapshot.note,
        price_display_tick=snapshot.price_display_tick,
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
            latest_price=snapshot.latest_price,
            latest_candle_time=snapshot.latest_candle_time,
            latest_candle_confirmed=snapshot.latest_candle_confirmed,
            note=snapshot.note,
            series_plot_end_index=None,
            price_display_tick=snapshot.price_display_tick,
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
        latest_price=snapshot.latest_price,
        latest_candle_time=snapshot.latest_candle_time,
        latest_candle_confirmed=snapshot.latest_candle_confirmed,
        note=snapshot.note,
        series_plot_end_index=len(real),
        price_display_tick=snapshot.price_display_tick,
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
