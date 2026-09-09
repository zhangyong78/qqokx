from __future__ import annotations

import math
from decimal import Decimal

from PySide6.QtCharts import QCandlestickSeries, QCandlestickSet, QChart, QChartView, QLineSeries, QValueAxis
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPen

from okx_quant.models import Candle
from roll_terminal_qt.line_trading_core import (
    ChartGeometry,
    LineAnnotation,
    RiskRewardAnnotation,
    compute_rr_target,
    drag_line_annotation,
    drag_rr_annotation,
    nearest_line_hit,
    nearest_rr_hit,
)


_DRAW_LINE_TOOLS = {"line", "horizontal", "stop"}
_DRAW_RR_TOOLS = {"rr_long", "rr_short"}
_DRAW_TOOLS = _DRAW_LINE_TOOLS | _DRAW_RR_TOOLS
_SUPPORTED_TOOLS = _DRAW_TOOLS | {"none", "zoom_range"}
_PRICE_QUANT = Decimal("0.000001")


class LineTradingChartView(QChartView):
    lineCreated = Signal(object)
    rrCreated = Signal(object)
    lineSelected = Signal(int)
    rrSelected = Signal(int)
    lineUpdated = Signal(int, object)
    rrUpdated = Signal(int, object)
    annotationChanged = Signal()

    def __init__(self, parent=None) -> None:  # noqa: ANN001
        self._chart = QChart()
        super().__init__(self._chart, parent)
        self.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._candles: list[Candle] = []
        self._lines: list[LineAnnotation] = []
        self._rr_items: list[RiskRewardAnnotation] = []
        self._selected_line_index = -1
        self._selected_rr_index = -1
        self._active_tool = "none"
        self._drag_start = None
        self._line_drag = None
        self._rr_drag = None

    def set_tool(self, tool: str) -> None:
        self._active_tool = tool if tool in _SUPPORTED_TOOLS else "none"

    def set_candles(self, candles: list[Candle]) -> None:
        self._candles = list(candles)
        self._render()

    def set_annotations(self, lines: list[LineAnnotation], rr_items: list[RiskRewardAnnotation]) -> None:
        self._lines = list(lines)
        self._rr_items = list(rr_items)
        self._render()

    def set_selected_indexes(self, *, line_index: int, rr_index: int) -> None:
        self._selected_line_index = int(line_index)
        self._selected_rr_index = int(rr_index)
        self._render()

    def _render(self) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)

        min_price: Decimal | None = None
        max_price: Decimal | None = None
        max_bar = max(len(self._candles) - 1, 1)

        if self._candles:
            candle_series = QCandlestickSeries()
            candle_series.setName("Candles")
            candle_series.setIncreasingColor(Qt.GlobalColor.red)
            candle_series.setDecreasingColor(Qt.GlobalColor.darkGreen)
            candle_series.setBodyOutlineVisible(False)

            for index, candle in enumerate(self._candles):
                candle_series.append(
                    QCandlestickSet(
                        float(candle.open),
                        float(candle.high),
                        float(candle.low),
                        float(candle.close),
                        index,
                    )
                )
                min_price = candle.low if min_price is None else min(min_price, candle.low)
                max_price = candle.high if max_price is None else max(max_price, candle.high)

            self._chart.addSeries(candle_series)

        for line_index, line in enumerate(self._lines):
            label = (line.label or "").strip() or (line.kind or "line")
            action = (line.desk_ray_action or "notify").strip() or "notify"
            series = QLineSeries()
            series.setName(f"{label} [{action}]")
            pen = QPen(QColor(line.color or "#1d4ed8"))
            pen.setWidth(4 if line_index == self._selected_line_index else 2)
            series.setPen(pen)

            price_a = line.price_a
            price_b = line.price_a if line.kind in {"horizontal", "stop"} else line.price_b
            bar_a = float(line.bar_a)
            bar_b = max_bar + 6 if line.kind in {"horizontal", "stop"} else float(line.bar_b)
            series.append(bar_a, float(price_a))
            series.append(float(bar_b), float(price_b))
            self._chart.addSeries(series)

            max_bar = max(max_bar, bar_a, float(bar_b))
            min_price = price_a if min_price is None else min(min_price, price_a, price_b)
            max_price = price_a if max_price is None else max(max_price, price_a, price_b)

        for rr_index, rr_item in enumerate(self._rr_items):
            start_bar = float(rr_item.bar_entry)
            end_bar = max(max_bar, start_bar) + 6
            rr_lines = (
                ("RR entry", rr_item.price_entry),
                ("RR stop", rr_item.price_stop),
                ("RR tp", rr_item.price_tp),
            )
            for name, price in rr_lines:
                series = QLineSeries()
                series.setName(name)
                pen = QPen(QColor("#f59e0b" if name == "RR entry" else "#dc2626" if name == "RR stop" else "#16a34a"))
                pen.setWidth(4 if rr_index == self._selected_rr_index else 2)
                series.setPen(pen)
                series.append(start_bar, float(price))
                series.append(end_bar, float(price))
                self._chart.addSeries(series)

            max_bar = max(max_bar, end_bar)
            rr_prices = (rr_item.price_entry, rr_item.price_stop, rr_item.price_tp)
            min_price = min(rr_prices) if min_price is None else min(min_price, *rr_prices)
            max_price = max(rr_prices) if max_price is None else max(max_price, *rr_prices)

        if not self._chart.series():
            return

        if min_price is None or max_price is None:
            min_price = Decimal("0")
            max_price = Decimal("1")
        padding = max((max_price - min_price) * Decimal("0.06"), Decimal("0.5"))

        bar_axis = QValueAxis()
        bar_axis.setTitleText("Bar")
        bar_axis.setLabelFormat("%d")
        bar_axis.setRange(0.0, float(max(max_bar, 1)))

        price_axis = QValueAxis()
        price_axis.setTitleText("Price")
        price_axis.setLabelFormat("%.4f")
        price_axis.setRange(float(min_price - padding), float(max_price + padding))

        self._chart.addAxis(bar_axis, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(price_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(bar_axis)
            series.attachAxis(price_axis)

    def mousePressEvent(self, event) -> None:  # noqa: ANN001
        if self._active_tool == "none" and event.button() == Qt.MouseButton.LeftButton:
            hit = self._resolve_hit_target(event.position())
            if hit is not None:
                kind, index = hit
                if kind == "line":
                    self.lineSelected.emit(index)
                elif kind.startswith("line_"):
                    self.lineSelected.emit(index)
                    if index == self._selected_line_index and 0 <= index < len(self._lines):
                        annotation = self._lines[index]
                        if (
                            not annotation.locked
                            and not annotation.desk_ray_triggered
                            and not annotation.desk_ray_submit_pending
                        ):
                            self._line_drag = (index, kind)
                elif kind == "rr":
                    self.rrSelected.emit(index)
                elif kind.startswith("rr_"):
                    self.rrSelected.emit(index)
                    if index == self._selected_rr_index and 0 <= index < len(self._rr_items):
                        annotation = self._rr_items[index]
                        if not annotation.locked:
                            self._rr_drag = (index, kind)
                event.accept()
                return
        if self._active_tool in _DRAW_TOOLS and event.button() == Qt.MouseButton.LeftButton:
            self._drag_start = event.position()
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # noqa: ANN001
        if self._line_drag is not None and event.button() == Qt.MouseButton.LeftButton:
            line_index, handle = self._line_drag
            self._line_drag = None
            if 0 <= line_index < len(self._lines):
                bar, price = self._chart_value_from_position(event.position())
                updated = drag_line_annotation(self._lines[line_index], handle, new_bar=bar, new_price=price)
                self.lineUpdated.emit(line_index, updated)
            event.accept()
            return
        if self._rr_drag is not None and event.button() == Qt.MouseButton.LeftButton:
            rr_index, handle = self._rr_drag
            self._rr_drag = None
            if 0 <= rr_index < len(self._rr_items):
                _bar, price = self._chart_value_from_position(event.position())
                updated = drag_rr_annotation(self._rr_items[rr_index], handle, price)
                self.rrUpdated.emit(rr_index, updated)
            event.accept()
            return
        if (
            self._drag_start is not None
            and self._active_tool in _DRAW_TOOLS
            and event.button() == Qt.MouseButton.LeftButton
        ):
            start = self._drag_start
            end = event.position()
            active_tool = self._active_tool
            self._drag_start = None
            self.set_tool("none")
            start_bar, start_price = self._chart_value_from_position(start)
            end_bar, end_price = self._chart_value_from_position(end)
            if active_tool in _DRAW_LINE_TOOLS:
                self.lineCreated.emit(
                    LineAnnotation(
                        kind=active_tool,
                        label="",
                        bar_a=start_bar,
                        bar_b=end_bar,
                        price_a=start_price,
                        price_b=end_price,
                    )
                )
            else:
                self.rrCreated.emit(_risk_reward_from_drag(active_tool, start_bar, start_price, end_bar, end_price))
            event.accept()
            return
        if self._drag_start is not None:
            self._drag_start = None
            event.accept()
            return
        if self._line_drag is not None:
            self._line_drag = None
            event.accept()
            return
        if self._rr_drag is not None:
            self._rr_drag = None
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def _chart_value_from_position(self, position) -> tuple[float, Decimal]:  # noqa: ANN001
        scene_position = self.mapToScene(position.toPoint())
        mapped = self._chart.mapToValue(scene_position)
        x_value = float(mapped.x())
        y_value = float(mapped.y())
        if not math.isfinite(x_value):
            x_value = float(position.x())
        if not math.isfinite(y_value):
            y_value = float(position.y())
        return x_value, _price_from_value(y_value)

    def _resolve_hit_target(self, position) -> tuple[str, int] | None:  # noqa: ANN001
        geometry = self._current_geometry()
        if geometry is None:
            return None
        scene_position = self.mapToScene(position.toPoint())
        x = float(scene_position.x())
        y = float(scene_position.y())
        tolerance = 10.0
        line_hit = nearest_line_hit(geometry, self._lines, x=x, y=y, tolerance=tolerance)
        if line_hit is not None:
            return (line_hit.kind, line_hit.index)
        rr_hit = nearest_rr_hit(geometry, self._rr_items, x=x, y=y, tolerance=tolerance)
        if rr_hit is not None:
            return (rr_hit.kind, rr_hit.index)
        return None

    def _current_geometry(self) -> ChartGeometry | None:
        axes_x = [axis for axis in self._chart.axes(Qt.Orientation.Horizontal) if isinstance(axis, QValueAxis)]
        axes_y = [axis for axis in self._chart.axes(Qt.Orientation.Vertical) if isinstance(axis, QValueAxis)]
        if not axes_x or not axes_y:
            return None
        plot_area = self._chart.plotArea()
        if plot_area.isEmpty():
            return None
        axis_x = axes_x[0]
        axis_y = axes_y[0]
        return ChartGeometry(
            plot_left=float(plot_area.left()),
            plot_top=float(plot_area.top()),
            plot_width=float(plot_area.width()),
            plot_height=float(plot_area.height()),
            first_bar=float(axis_x.min()),
            last_bar=float(axis_x.max()),
            min_price=Decimal(str(axis_y.min())),
            max_price=Decimal(str(axis_y.max())),
        )


def _price_from_value(value: float) -> Decimal:
    return Decimal(str(round(float(value), 6))).quantize(_PRICE_QUANT)


def _risk_reward_from_drag(
    tool: str,
    start_bar: float,
    start_price: Decimal,
    end_bar: float,
    end_price: Decimal,
) -> RiskRewardAnnotation:
    entry_bar = float(min(start_bar, end_bar))
    side = "long" if tool == "rr_long" else "short"
    r_multiple = Decimal("2")

    if side == "long":
        entry_price = max(start_price, end_price)
        stop_price = min(start_price, end_price)
    else:
        entry_price = min(start_price, end_price)
        stop_price = max(start_price, end_price)

    tp_price = compute_rr_target(side, entry_price, stop_price, r_multiple)
    return RiskRewardAnnotation(
        rr_id="",
        side=side,
        bar_entry=entry_bar,
        bar_stop=entry_bar,
        price_entry=entry_price,
        price_stop=stop_price,
        price_tp=tp_price,
        r_multiple=r_multiple,
    )
