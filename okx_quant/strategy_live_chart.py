from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from tkinter import Canvas

from okx_quant.indicators import ema
from okx_quant.models import Candle
from okx_quant.pricing import format_decimal

DEFAULT_STRATEGY_LIVE_CHART_CANDLE_LIMIT = 240
DEFAULT_STRATEGY_LIVE_CHART_REFRESH_MS = 5000

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
class StrategyLiveChartSnapshot:
    session_id: str
    candles: tuple[Candle, ...]
    series: tuple[StrategyLiveChartOverlaySeries, ...] = ()
    markers: tuple[StrategyLiveChartMarker, ...] = ()
    latest_price: Decimal | None = None
    latest_candle_time: datetime | None = None
    latest_candle_confirmed: bool = True
    note: str = ""


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
        dash=(2, 2),
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
        latest_price=latest_price,
        latest_candle_time=latest_candle_time,
        latest_candle_confirmed=latest_candle_confirmed,
        note=note,
    )


def strategy_live_chart_price_bounds(snapshot: StrategyLiveChartSnapshot) -> tuple[Decimal, Decimal]:
    price_values: list[Decimal] = []
    for candle in snapshot.candles:
        price_values.append(candle.high)
        price_values.append(candle.low)
    for series in snapshot.series:
        price_values.extend(series.values)
    for marker in snapshot.markers:
        price_values.append(marker.price)
    if not price_values:
        return Decimal("0"), Decimal("1")
    lower = min(price_values)
    upper = max(price_values)
    if lower == upper:
        padding = abs(lower) * Decimal("0.02") if lower != 0 else Decimal("1")
        return lower - padding, upper + padding
    padding = (upper - lower) * Decimal("0.08")
    return lower - padding, upper + padding


def render_strategy_live_chart(canvas: Canvas, snapshot: StrategyLiveChartSnapshot) -> None:
    width = max(int(canvas.winfo_width()), int(float(canvas.cget("width") or 0)), 640)
    height = max(int(canvas.winfo_height()), int(float(canvas.cget("height") or 0)), 420)
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
        return

    bounds = _ChartBounds(left=76, top=40, right=width - 156, bottom=height - 56)
    if bounds.width <= 24 or bounds.height <= 24:
        return

    lower, upper = strategy_live_chart_price_bounds(snapshot)
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
            text=format_decimal(price),
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
    for series in snapshot.series:
        points: list[float] = []
        for index, value in enumerate(series.values):
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
        line_kwargs = {
            "fill": marker.color,
            "width": marker.width,
        }
        if marker.dash:
            line_kwargs["dash"] = marker.dash
        canvas.create_line(bounds.left, y, bounds.right, y, **line_kwargs)
        label_text = f"{marker.label} {format_decimal(marker.price)}"
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


def _format_chart_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _candle_color(candle: Candle) -> str:
    if not candle.confirmed:
        return _CANDLE_PENDING
    if candle.close >= candle.open:
        return _CANDLE_RISE
    return _CANDLE_FALL
