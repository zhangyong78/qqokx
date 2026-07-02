from __future__ import annotations

import csv
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import traceback

from PySide6.QtCharts import QCandlestickSeries, QCandlestickSet, QChart, QChartView, QDateTimeAxis, QValueAxis
from PySide6.QtCore import QDateTime, QMargins, QPointF, QRectF, QThread, Qt, QTimer, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.deribit_volatility_ui import (
    DAY_ALIGN_OPTIONS,
    DERIBIT_BASE_HOURLY_RESOLUTION,
    DERIBIT_CURRENCY_OPTIONS,
    DERIBIT_DEFAULT_VISIBLE_CANDLE_COUNT,
    DERIBIT_FULL_HISTORY_START_TS,
    DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS,
    DERIBIT_RESOLUTION_OPTIONS,
    DERIBIT_VOLATILITY_UI_STATE_KEY,
    DeribitMarketSnapshot,
    OKX_SPOT_SYMBOLS,
    _aggregate_candles_to_resolution,
    _aggregate_price_candles_to_resolution,
    _align_candles_by_timestamp,
    _default_chart_viewport,
    _format_ts,
    _hourly_fetch_start_ts,
    _hourly_history_limit,
    _max_limit_for_resolution_value,
    _merge_deribit_candles,
    _merge_price_candles,
    _next_refresh_delay_ms,
    _normalize_chart_viewport,
    _pan_chart_viewport,
    _resolution_file_label,
    _resolution_label_for_value,
    _snapshot_last_ts,
    _to_average_price_candles,
    _to_average_volatility_candles,
    _zoom_chart_viewport,
)
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import deribit_report_export_dir_path, deribit_volatility_cache_file_path


_DERIBIT_SHARED_CLIENT: DeribitRestClient | None = None
_OKX_SHARED_CLIENT: OkxRestClient | None = None


def _shared_deribit_client() -> DeribitRestClient:
    global _DERIBIT_SHARED_CLIENT
    if _DERIBIT_SHARED_CLIENT is None:
        _DERIBIT_SHARED_CLIENT = DeribitRestClient()
    return _DERIBIT_SHARED_CLIENT


def _shared_okx_client() -> OkxRestClient:
    global _OKX_SHARED_CLIENT
    if _OKX_SHARED_CLIENT is None:
        _OKX_SHARED_CLIENT = OkxRestClient()
    return _OKX_SHARED_CLIENT


def _decimal_places_for_candles(candles: list[DeribitVolatilityCandle | Candle]) -> int:
    places = 2
    for candle in candles:
        for value in (candle.open, candle.high, candle.low, candle.close):
            current = max(0, -value.normalize().as_tuple().exponent)
            places = max(places, min(current, 6))
    return places


class _DeribitFetchThread(QThread):
    snapshot_ready = Signal(int, object)
    error_raised = Signal(int, str)

    def __init__(
        self,
        *,
        request_token: int,
        currency: str,
        resolution: str,
        day_align_label: str,
        limit: int,
        deribit_client: DeribitRestClient,
        market_client: OkxRestClient,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._request_token = request_token
        self._currency = currency
        self._resolution = resolution
        self._day_align_label = day_align_label
        self._limit = limit
        self._deribit_client = deribit_client
        self._market_client = market_client

    def run(self) -> None:
        try:
            snapshot = self._build_snapshot()
        except Exception as exc:  # noqa: BLE001
            self.error_raised.emit(self._request_token, str(exc))
            return
        self.snapshot_ready.emit(self._request_token, snapshot)

    def _build_snapshot(self) -> DeribitMarketSnapshot:
        spot_inst_id = OKX_SPOT_SYMBOLS[self._currency]
        cached_hourly = _load_cached_hourly_series(self._currency)
        now_ts = int(datetime.now().timestamp() * 1000)
        if cached_hourly is None:
            fetch_start_ts = DERIBIT_FULL_HISTORY_START_TS
        else:
            _, cached_volatility, cached_spot, _ = cached_hourly
            fetch_start_ts = _hourly_fetch_start_ts(
                cached_volatility=cached_volatility,
                cached_spot=cached_spot,
            )
        fetched_volatility = self._deribit_client.get_volatility_index_candles(
            self._currency,
            DERIBIT_BASE_HOURLY_RESOLUTION,
            start_ts=fetch_start_ts,
            end_ts=now_ts,
            max_records=None,
        )
        fetched_spot = self._market_client.get_candles_history_range(
            spot_inst_id,
            "1H",
            start_ts=fetch_start_ts,
            end_ts=now_ts,
            limit=_hourly_history_limit(fetch_start_ts, now_ts),
        )
        fetched_spot = [candle for candle in fetched_spot if candle.confirmed]
        if cached_hourly is not None:
            _, cached_volatility, cached_spot, _ = cached_hourly
            volatility_hourly = _merge_deribit_candles(cached_volatility, fetched_volatility)
            spot_hourly = _merge_price_candles(cached_spot, fetched_spot)
        else:
            volatility_hourly = list(fetched_volatility)
            spot_hourly = list(fetched_spot)
        fetched_at = datetime.now()
        _save_cached_hourly_series(
            self._currency,
            spot_inst_id=spot_inst_id,
            volatility_candles=volatility_hourly,
            spot_candles=spot_hourly,
            fetched_at=fetched_at,
        )
        return _build_snapshot_from_hourly(
            currency=self._currency,
            resolution=self._resolution,
            day_align_label=self._day_align_label,
            requested_limit=self._limit,
            spot_inst_id=spot_inst_id,
            volatility_hourly=volatility_hourly,
            spot_hourly=spot_hourly,
            fetched_at=fetched_at,
        )


class LinkedCandlestickChartView(QChartView):
    zoom_requested = Signal(float, bool)
    pan_requested = Signal(float, float)
    reset_requested = Signal()
    hover_changed = Signal(int, float)
    hover_cleared = Signal()

    def __init__(self, *, percent_axis: bool, parent: QWidget | None = None) -> None:
        self._chart = QChart()
        self._chart.legend().hide()
        self._chart.setBackgroundVisible(False)
        self._chart.setMargins(QMargins(8, 8, 8, 8))
        super().__init__(self._chart, parent)
        self._percent_axis = percent_axis
        self._pan_anchor_x: float | None = None
        self.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        self.setBackgroundBrush(QColor("#ffffff"))
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)
        self._title = ""
        self._value_suffix = "%"
        self._empty_message = "暂无可用K线数据。"
        self._candles: list[DeribitVolatilityCandle | Candle] = []
        self._hover_pos: QPointF | None = None
        self._value_min = 0.0
        self._value_max = 1.0
        self._linked_hover_index: int | None = None
        self._linked_hover_y_ratio: float | None = None
        self._price_badge = self._create_hover_label(multiline=False, center=True)
        self._time_badge = self._create_hover_label(multiline=False, center=True)
        self._tooltip_badge = self._create_hover_label(multiline=True, center=False)
        self._hide_hover_overlays()

    def set_chart_payload(
        self,
        *,
        title: str,
        candles: list[DeribitVolatilityCandle | Candle],
        empty_message: str,
    ) -> None:
        self._title = title
        self._empty_message = empty_message
        self._candles = list(candles)
        self._hover_pos = None
        self._linked_hover_index = None
        self._linked_hover_y_ratio = None
        self._hide_hover_overlays()
        self._render_chart(candles)

    def reset_view(self) -> None:
        self.reset_requested.emit()

    def set_linked_hover(self, index: int | None, y_ratio: float | None) -> None:
        self._linked_hover_index = index
        self._linked_hover_y_ratio = y_ratio
        self.viewport().update()

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        plot = self.chart().plotArea()
        width = max(float(plot.width()), 1.0)
        anchor_ratio = min(max((float(event.position().x()) - float(plot.left())) / width, 0.0), 1.0)
        self.zoom_requested.emit(anchor_ratio, event.angleDelta().y() > 0)
        event.accept()

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            self._pan_anchor_x = float(event.position().x())
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        if self._pan_anchor_x is not None and event.buttons() & Qt.MouseButton.LeftButton:
            current_x = float(event.position().x())
            plot = self.chart().plotArea()
            self.pan_requested.emit(current_x - self._pan_anchor_x, max(float(plot.width()), 1.0))
            self._pan_anchor_x = current_x
            self._hover_pos = QPointF(event.position())
            self.viewport().update()
            event.accept()
            return
        plot = self.chart().plotArea()
        position = event.position()
        self._hover_pos = QPointF(position)
        if plot.contains(position) and self._candles:
            hover_index = self._nearest_candle_index(float(position.x()), plot)
            hover_y_ratio = min(max((float(position.y()) - float(plot.top())) / max(float(plot.height()), 1.0), 0.0), 1.0)
            self.hover_changed.emit(hover_index, hover_y_ratio)
        self.viewport().update()
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        self._pan_anchor_x = None
        super().mouseReleaseEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self._pan_anchor_x = None
        self._hover_pos = None
        self.hover_cleared.emit()
        self._hide_hover_overlays()
        self.viewport().update()
        super().leaveEvent(event)

    def mouseDoubleClickEvent(self, event) -> None:  # type: ignore[override]
        self.reset_requested.emit()
        event.accept()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        try:
            if not self._candles:
                self._hide_hover_overlays()
                return
            hover_context = self._resolve_hover_context()
            if hover_context is None:
                self._hide_hover_overlays()
                return
            hover_index, hover_y_ratio = hover_context
            plot = self.chart().plotArea()
            if plot.width() <= 0 or plot.height() <= 0:
                self._hide_hover_overlays()
                return
            candle = self._candles[hover_index]
            hover_x = self._x_for_index(hover_index, plot)
            hover_y = float(plot.top()) + (hover_y_ratio * float(plot.height()))
            marker_y = self._y_for_value(float(candle.close), plot)
            candle_color = QColor("#16a34a" if candle.close >= candle.open else "#dc2626")
            painter = QPainter(self.viewport())
            painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
            painter.setClipping(False)
            hover_pen = QPen(QColor("#6e7781"), 1)
            hover_pen.setStyle(Qt.PenStyle.DashLine)
            painter.setPen(hover_pen)
            painter.drawLine(QPointF(hover_x, plot.top()), QPointF(hover_x, plot.bottom()))
            painter.drawLine(QPointF(plot.left(), hover_y), QPointF(plot.right(), hover_y))
            suffix = "%" if self._percent_axis else ""
            painter.setPen(QPen(candle_color, 2))
            painter.setBrush(QColor("#ffffff"))
            painter.drawEllipse(QPointF(hover_x, marker_y), 4.0, 4.0)
            hover_value = self._value_for_ratio(hover_y_ratio)
            tooltip_lines = (
                QDateTime.fromMSecsSinceEpoch(int(candle.ts)).toString("yyyy-MM-dd HH:mm"),
                f"O {self._format_hover_value(float(candle.open))}{suffix}  H {self._format_hover_value(float(candle.high))}{suffix}",
                f"L {self._format_hover_value(float(candle.low))}{suffix}  C {self._format_hover_value(float(candle.close))}{suffix}",
                f"游标 {self._format_hover_value(hover_value)}{suffix}",
            )
            painter.end()
            self._update_hover_overlays(
                bounds=plot,
                anchor=QPointF(hover_x, hover_y),
                candle_color=candle_color,
                price_text=f"{self._format_hover_value(hover_value)}{suffix}",
                time_text=QDateTime.fromMSecsSinceEpoch(int(candle.ts)).toString("MM-dd HH:mm"),
                tooltip_lines=tooltip_lines,
            )
        except Exception:
            traceback.print_exc()
            self._hide_hover_overlays()
            return

    def _resolve_hover_context(self) -> tuple[int, float] | None:
        hover_index = self._linked_hover_index
        hover_y_ratio = self._linked_hover_y_ratio
        if hover_index is not None and hover_y_ratio is not None and 0 <= hover_index < len(self._candles):
            return hover_index, hover_y_ratio
        plot = self.chart().plotArea()
        position = self._hover_pos
        if position is None or not plot.contains(position) or not self._candles:
            return None
        local_index = self._nearest_candle_index(float(position.x()), plot)
        local_y_ratio = min(max((float(position.y()) - float(plot.top())) / max(float(plot.height()), 1.0), 0.0), 1.0)
        return local_index, local_y_ratio

    def _create_hover_label(self, *, multiline: bool, center: bool) -> QLabel:
        label = QLabel(self)
        label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        label.setVisible(False)
        font = label.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        label.setFont(font)
        label.setWordWrap(multiline)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter if center else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        label.setStyleSheet(
            "QLabel {"
            "background-color: rgba(15, 23, 42, 238);"
            "color: #f8fafc;"
            "border: 1px solid #334155;"
            "border-radius: 5px;"
            "padding: 4px 8px;"
            "}"
        )
        return label

    def _hide_hover_overlays(self) -> None:
        self._price_badge.hide()
        self._time_badge.hide()
        self._tooltip_badge.hide()

    def _update_hover_overlays(
        self,
        *,
        bounds: QRectF,
        anchor: QPointF,
        candle_color: QColor,
        price_text: str,
        time_text: str,
        tooltip_lines: tuple[str, ...],
    ) -> None:
        viewport_geom = self.viewport().geometry()
        viewport = QRectF(self.rect())
        mapped_bounds = QRectF(
            float(viewport_geom.left()) + float(bounds.left()),
            float(viewport_geom.top()) + float(bounds.top()),
            float(bounds.width()),
            float(bounds.height()),
        )
        mapped_anchor = QPointF(
            float(viewport_geom.left()) + float(anchor.x()),
            float(viewport_geom.top()) + float(anchor.y()),
        )
        self._price_badge.setText(price_text)
        self._price_badge.adjustSize()
        price_size = self._price_badge.sizeHint()
        price_x = max(4.0, float(mapped_bounds.left()) - float(price_size.width()) - 8.0)
        price_y = max(
            float(viewport.top()) + 4.0,
            min(
                float(mapped_anchor.y()) - (float(price_size.height()) / 2.0),
                float(viewport.bottom()) - float(price_size.height()) - 4.0,
            ),
        )
        self._price_badge.move(int(round(price_x)), int(round(price_y)))
        self._price_badge.raise_()
        self._price_badge.show()

        self._time_badge.setText(time_text)
        self._time_badge.adjustSize()
        time_size = self._time_badge.sizeHint()
        time_x = max(
            float(viewport.left()) + 4.0,
            min(
                float(mapped_anchor.x()) - (float(time_size.width()) / 2.0),
                float(viewport.right()) - float(time_size.width()) - 4.0,
            ),
        )
        time_y = min(
            float(viewport.bottom()) - float(time_size.height()) - 4.0,
            float(mapped_bounds.bottom()) + 8.0,
        )
        self._time_badge.move(int(round(time_x)), int(round(time_y)))
        self._time_badge.raise_()
        self._time_badge.show()

        self._tooltip_badge.setStyleSheet(
            "QLabel {"
            "background-color: rgba(11, 18, 32, 236);"
            f"border: 1px solid {candle_color.name()};"
            "color: #f8fafc;"
            "border-radius: 7px;"
            "padding: 6px 10px;"
            "}"
        )
        self._tooltip_badge.setText("\n".join(tooltip_lines))
        self._tooltip_badge.adjustSize()
        tooltip_size = self._tooltip_badge.sizeHint()
        place_right = float(mapped_anchor.x()) <= float(mapped_bounds.center().x())
        place_above = float(mapped_anchor.y()) > float(mapped_bounds.center().y())
        tooltip_x = (
            float(mapped_anchor.x()) + 18.0
            if place_right
            else float(mapped_anchor.x()) - float(tooltip_size.width()) - 18.0
        )
        tooltip_y = (
            float(mapped_anchor.y()) - float(tooltip_size.height()) - 18.0
            if place_above
            else float(mapped_anchor.y()) + 18.0
        )
        tooltip_x = max(
            float(mapped_bounds.left()) + 8.0,
            min(tooltip_x, float(mapped_bounds.right()) - float(tooltip_size.width()) - 8.0),
        )
        tooltip_y = max(
            float(mapped_bounds.top()) + 8.0,
            min(tooltip_y, float(mapped_bounds.bottom()) - float(tooltip_size.height()) - 8.0),
        )
        self._tooltip_badge.move(int(round(tooltip_x)), int(round(tooltip_y)))
        self._tooltip_badge.raise_()
        self._tooltip_badge.show()

    def _render_chart(self, candles: list[DeribitVolatilityCandle | Candle]) -> None:
        self.chart().removeAllSeries()
        for axis in list(self.chart().axes()):
            self.chart().removeAxis(axis)
        self.chart().setTitle(self._title)
        if not candles:
            self.chart().setTitle(f"{self._title} | {self._empty_message}")
            return

        increasing_color = QColor("#16a34a")
        decreasing_color = QColor("#dc2626")
        up_series = QCandlestickSeries()
        up_series.setIncreasingColor(increasing_color)
        up_series.setDecreasingColor(increasing_color)
        up_series.setBodyOutlineVisible(False)
        up_series.setCapsVisible(False)
        down_series = QCandlestickSeries()
        down_series.setIncreasingColor(decreasing_color)
        down_series.setDecreasingColor(decreasing_color)
        down_series.setBodyOutlineVisible(False)
        down_series.setCapsVisible(False)

        min_price: Decimal | None = None
        max_price: Decimal | None = None
        first_ts = int(candles[0].ts)
        last_ts = int(candles[-1].ts)
        for candle in candles:
            candle_color = increasing_color if candle.close >= candle.open else decreasing_color
            candle_set = QCandlestickSet(
                float(candle.open),
                float(candle.high),
                float(candle.low),
                float(candle.close),
                int(candle.ts),
            )
            candle_set.setBrush(candle_color)
            if candle.close >= candle.open:
                up_series.append(candle_set)
            else:
                down_series.append(candle_set)
            min_price = candle.low if min_price is None else min(min_price, candle.low)
            max_price = candle.high if max_price is None else max(max_price, candle.high)

        axis_x = QDateTimeAxis()
        axis_x.setFormat("MM-dd HH:mm")
        axis_x.setTickCount(min(8, max(3, len(candles) // 32 + 2)))
        axis_x.setRange(QDateTime.fromMSecsSinceEpoch(first_ts), QDateTime.fromMSecsSinceEpoch(last_ts))

        axis_y = QValueAxis()
        if min_price is None or max_price is None:
            min_value = -1.0
            max_value = 1.0
        else:
            diff = max_price - min_price
            padding = max(diff * Decimal("0.06"), Decimal("0.01"))
            min_value = float(min_price - padding)
            max_value = float(max_price + padding)
        self._value_min = min_value
        self._value_max = max_value
        axis_y.setRange(min_value, max_value)
        places = min(4, _decimal_places_for_candles(candles))
        axis_y.setLabelFormat(f"%.{places}f%%" if self._percent_axis else f"%.{places}f")

        self.chart().addSeries(up_series)
        self.chart().addSeries(down_series)
        self.chart().addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        self.chart().addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        up_series.attachAxis(axis_x)
        up_series.attachAxis(axis_y)
        down_series.attachAxis(axis_x)
        down_series.attachAxis(axis_y)

    def _nearest_candle_index(self, x: float, plot: QRectF) -> int:
        if not self._candles:
            return 0
        if len(self._candles) == 1:
            return 0
        ratio = min(max((x - float(plot.left())) / max(float(plot.width()), 1.0), 0.0), 1.0)
        return min(len(self._candles) - 1, max(0, int(round(ratio * (len(self._candles) - 1)))))

    def _x_for_index(self, index: int, plot: QRectF) -> float:
        if len(self._candles) <= 1:
            return float(plot.center().x())
        ratio = index / max(len(self._candles) - 1, 1)
        return float(plot.left()) + (ratio * float(plot.width()))

    def _value_for_ratio(self, ratio: float) -> float:
        ratio = min(max(ratio, 0.0), 1.0)
        return self._value_max - (ratio * (self._value_max - self._value_min))

    def _y_for_value(self, value: float, plot: QRectF) -> float:
        span = max(self._value_max - self._value_min, 1e-9)
        ratio = (self._value_max - value) / span
        return float(plot.top()) + (min(max(ratio, 0.0), 1.0) * float(plot.height()))

    def _format_hover_value(self, value: float) -> str:
        return f"{value:.2f}" if abs(value) >= 100 else f"{value:.4f}".rstrip("0").rstrip(".")

    def _draw_hover_tooltip(
        self,
        painter: QPainter,
        *,
        bounds: QRectF,
        anchor: QPointF,
        marker_color: QColor,
        lines: tuple[str, ...],
    ) -> None:
        if not lines:
            return
        font = painter.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        padding_x = 10.0
        padding_y = 8.0
        line_height = metrics.height() + 1
        text_width = max(metrics.horizontalAdvance(line) for line in lines)
        box_width = text_width + (padding_x * 2)
        box_height = (line_height * len(lines)) + (padding_y * 2)
        place_right = anchor.x() <= bounds.center().x()
        place_above = anchor.y() > bounds.center().y()
        box_left = anchor.x() + 18.0 if place_right else anchor.x() - box_width - 18.0
        box_top = anchor.y() - box_height - 18.0 if place_above else anchor.y() + 18.0
        box_left = max(bounds.left() + 8.0, min(box_left, bounds.right() - box_width - 8.0))
        box_top = max(bounds.top() + 8.0, min(box_top, bounds.bottom() - box_height - 8.0))
        box_rect = QRectF(box_left, box_top, box_width, box_height)
        shadow_rect = box_rect.translated(2.0, 2.0)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(15, 23, 42, 55))
        painter.drawRoundedRect(shadow_rect, 7.0, 7.0)
        guide_y = max(box_rect.top() + 10.0, min(anchor.y(), box_rect.bottom() - 10.0))
        guide_x = box_rect.left() if place_right else box_rect.right()
        painter.setPen(QPen(marker_color.lighter(120), 1))
        painter.drawLine(anchor, QPointF(guide_x, guide_y))
        painter.setPen(QPen(marker_color.lighter(115), 1))
        painter.setBrush(QColor(11, 18, 32, 236))
        painter.drawRoundedRect(box_rect, 7.0, 7.0)
        text_top = box_top + padding_y + metrics.ascent()
        for index, line in enumerate(lines):
            painter.setPen(QColor("#cbd5e1") if index == 0 else QColor("#f8fafc"))
            painter.drawText(QPointF(box_left + padding_x, text_top), line)
            text_top += line_height

    def _draw_axis_badge(self, painter: QPainter, *, text: str, anchor: QPointF, side: str) -> None:
        font = painter.font()
        font.setFamily("Consolas")
        font.setPointSize(8)
        font.setBold(True)
        painter.setFont(font)
        metrics = painter.fontMetrics()
        padding_x = 9.0
        padding_y = 5.0
        badge_width = metrics.horizontalAdvance(text) + (padding_x * 2)
        badge_height = metrics.height() + (padding_y * 2)
        viewport = QRectF(self.viewport().rect())
        if side == "left":
            badge_left = max(4.0, anchor.x() - badge_width - 8.0)
            badge_top = max(viewport.top() + 4.0, min(anchor.y() - (badge_height / 2), viewport.bottom() - badge_height - 4.0))
        else:
            badge_left = max(viewport.left() + 4.0, min(anchor.x() - (badge_width / 2), viewport.right() - badge_width - 4.0))
            badge_top = min(viewport.bottom() - badge_height - 4.0, anchor.y() + 8.0)
        badge_rect = QRectF(badge_left, badge_top, badge_width, badge_height)
        painter.setPen(QPen(QColor("#334155"), 1))
        painter.setBrush(QColor(15, 23, 42, 238))
        painter.drawRoundedRect(badge_rect, 5.0, 5.0)
        painter.setPen(QColor("#ffffff"))
        text_x = badge_left + ((badge_width - metrics.horizontalAdvance(text)) / 2.0)
        text_y = badge_top + ((badge_height - metrics.height()) / 2.0) + metrics.ascent()
        painter.drawText(QPointF(text_x, text_y), text)


class DeribitVolatilityQtWindow(QMainWindow):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Deribit 波动率指数")
        self.resize(1580, 980)

        self._deribit_client = _shared_deribit_client()
        self._market_client = _shared_okx_client()
        saved_ui_state = _load_saved_ui_state()

        self._currency_combo = QComboBox()
        self._resolution_combo = QComboBox()
        self._day_align_combo = QComboBox()
        self._candle_limit_edit = QLineEdit("300")
        self._average_kline_check = QCheckBox("平均K线")
        self._status_label = QLabel("打开页面后自动加载历史K线，并每小时同步一次。")
        self._summary_label = QLabel("暂无数据。")
        self._spot_chart_title = QLabel("同币种现货K线")

        self._latest_snapshot: DeribitMarketSnapshot | None = None
        self._loading = False
        self._request_token = 0
        self._pending_refresh_after_load = False
        self._pending_force_network = False
        self._fetch_threads: dict[int, _DeribitFetchThread] = {}
        self._chart_viewport_start = 0
        self._chart_viewport_visible: int | None = None

        self._volatility_chart = LinkedCandlestickChartView(percent_axis=True)
        self._spot_chart = LinkedCandlestickChartView(percent_axis=False)
        self._auto_refresh_timer = QTimer(self)
        self._auto_refresh_timer.setSingleShot(True)
        self._auto_refresh_timer.timeout.connect(self._auto_refresh)

        self._build_ui(saved_ui_state)
        self._bootstrap_initial_load()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._auto_refresh_timer.stop()
        self._request_token += 1
        for thread in list(self._fetch_threads.values()):
            thread.wait(100)
        super().closeEvent(event)

    def _build_ui(self, saved_ui_state: dict[str, object]) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        controls = QFrame()
        controls.setObjectName("Panel")
        controls_layout = QGridLayout(controls)
        controls_layout.setContentsMargins(14, 14, 14, 14)
        controls_layout.setHorizontalSpacing(10)
        controls_layout.setVerticalSpacing(10)
        for item in DERIBIT_CURRENCY_OPTIONS:
            self._currency_combo.addItem(item, item)
        for label, value in DERIBIT_RESOLUTION_OPTIONS.items():
            self._resolution_combo.addItem(label, value)
        for label in DAY_ALIGN_OPTIONS:
            self._day_align_combo.addItem(label, label)
        saved_day_align_label = str(saved_ui_state.get("day_align_label") or "北京时间凌晨12点")
        day_align_text = saved_day_align_label if saved_day_align_label in DAY_ALIGN_OPTIONS else "北京时间凌晨12点"
        self._day_align_combo.setCurrentText(day_align_text)
        self._currency_combo.setCurrentText("BTC")
        self._resolution_combo.setCurrentText("1小时")

        refresh_button = QPushButton("立即刷新")
        refresh_button.clicked.connect(self.fetch_history)
        export_button = QPushButton("导出CSV")
        export_button.clicked.connect(self.export_csv)
        reset_button = QPushButton("重置视图")
        reset_button.clicked.connect(self.reset_chart_view)

        controls_layout.addWidget(QLabel("币种"), 0, 0)
        controls_layout.addWidget(self._currency_combo, 0, 1)
        controls_layout.addWidget(QLabel("周期"), 0, 2)
        controls_layout.addWidget(self._resolution_combo, 0, 3)
        controls_layout.addWidget(QLabel("日线对齐"), 0, 4)
        controls_layout.addWidget(self._day_align_combo, 0, 5)
        controls_layout.addWidget(QLabel("K线数量"), 0, 6)
        controls_layout.addWidget(self._candle_limit_edit, 0, 7)
        controls_layout.addWidget(self._average_kline_check, 0, 8)
        controls_layout.addWidget(reset_button, 0, 9)
        controls_layout.addWidget(self._status_label, 1, 0, 1, 7)
        controls_layout.addWidget(refresh_button, 1, 8)
        controls_layout.addWidget(export_button, 1, 9)
        self._status_label.setObjectName("Subtle")
        self._status_label.setWordWrap(True)
        layout.addWidget(controls)

        charts = QFrame()
        charts.setObjectName("Panel")
        charts_layout = QVBoxLayout(charts)
        charts_layout.setContentsMargins(14, 14, 14, 14)
        charts_layout.setSpacing(10)
        title = QLabel("Deribit 波动率指数与同币种现货K线")
        title.setObjectName("SectionTitle")
        self._summary_label.setObjectName("Subtle")
        self._summary_label.setWordWrap(True)
        charts_layout.addWidget(title)
        charts_layout.addWidget(self._summary_label)

        vol_label = QLabel("Deribit 波动率指数K线")
        vol_label.setObjectName("SectionTitle")
        self._spot_chart_title.setObjectName("SectionTitle")
        charts_layout.addWidget(vol_label)
        self._volatility_chart.setMinimumHeight(330)
        charts_layout.addWidget(self._volatility_chart, 1)
        charts_layout.addWidget(self._spot_chart_title)
        self._spot_chart.setMinimumHeight(330)
        charts_layout.addWidget(self._spot_chart, 1)
        layout.addWidget(charts, 1)

        self._currency_combo.currentIndexChanged.connect(self._on_selection_changed)
        self._resolution_combo.currentIndexChanged.connect(self._on_selection_changed)
        self._day_align_combo.currentIndexChanged.connect(self._on_selection_changed)
        self._average_kline_check.stateChanged.connect(self._on_chart_style_changed)
        self._candle_limit_edit.editingFinished.connect(self._on_limit_changed)
        self._volatility_chart.zoom_requested.connect(self._on_chart_zoom_requested)
        self._spot_chart.zoom_requested.connect(self._on_chart_zoom_requested)
        self._volatility_chart.pan_requested.connect(self._on_chart_pan_requested)
        self._spot_chart.pan_requested.connect(self._on_chart_pan_requested)
        self._volatility_chart.reset_requested.connect(self.reset_chart_view)
        self._spot_chart.reset_requested.connect(self.reset_chart_view)
        self._volatility_chart.hover_changed.connect(lambda index, ratio: self._sync_chart_hover(index, ratio))
        self._spot_chart.hover_changed.connect(lambda index, ratio: self._sync_chart_hover(index, ratio))
        self._volatility_chart.hover_cleared.connect(self._clear_chart_hover)
        self._spot_chart.hover_cleared.connect(self._clear_chart_hover)
        self._on_resolution_changed()

    def fetch_history(self) -> None:
        self._refresh_for_current_selection(use_cache=True, force_network=True, supersede_if_loading=True)

    def _bootstrap_initial_load(self) -> None:
        self._on_resolution_changed()
        self._refresh_for_current_selection(use_cache=True, force_network=True, supersede_if_loading=False)
        self._schedule_auto_refresh()

    def _validated_limit(self) -> int | None:
        try:
            limit = int(self._candle_limit_edit.text().strip())
        except ValueError:
            QMessageBox.critical(self, "参数错误", "K线数量必须是整数。")
            return None
        resolution = DERIBIT_RESOLUTION_OPTIONS.get(self._resolution_combo.currentText(), DERIBIT_BASE_HOURLY_RESOLUTION)
        max_limit = _max_limit_for_resolution_value(resolution)
        if limit <= 0 or limit > max_limit:
            QMessageBox.critical(self, "参数错误", f"K线数量必须在 1 到 {max_limit} 之间。")
            return None
        return limit

    def _refresh_for_current_selection(
        self,
        *,
        use_cache: bool,
        force_network: bool,
        supersede_if_loading: bool = False,
    ) -> None:
        limit = self._validated_limit()
        if limit is None:
            return
        currency = self._currency_combo.currentText().strip().upper()
        resolution = str(self._resolution_combo.currentData() or DERIBIT_BASE_HOURLY_RESOLUTION)
        day_align_label = self._day_align_combo.currentText().strip()
        cached: DeribitMarketSnapshot | None = None
        if use_cache:
            cached = _load_cached_snapshot(currency, resolution, day_align_label, limit)
            if cached is not None:
                self._apply_snapshot(cached, request_token=None, from_cache=True)
                if not force_network:
                    return
        if self._loading and not supersede_if_loading:
            self._pending_refresh_after_load = True
            self._pending_force_network = self._pending_force_network or force_network
            self._status_label.setText(
                "已显示本地缓存，当前请求结束后会自动刷新最新选择，请稍候..."
                if cached is not None
                else "当前请求仍在处理中，已排队刷新最新选择，请稍候..."
            )
            return
        self._loading = True
        self._pending_refresh_after_load = False
        self._pending_force_network = False
        self._status_label.setText(
            "首次获取 Deribit 波动率K线和同币种现货K线，正在补全本地缓存，请稍候..."
            if cached is None
            else "正在获取 Deribit 波动率K线和同币种现货K线，请稍候..."
        )
        self._request_token += 1
        request_token = self._request_token
        thread = _DeribitFetchThread(
            request_token=request_token,
            currency=currency,
            resolution=resolution,
            day_align_label=day_align_label,
            limit=limit,
            deribit_client=self._deribit_client,
            market_client=self._market_client,
            parent=self,
        )
        thread.snapshot_ready.connect(self._handle_snapshot_ready)
        thread.error_raised.connect(self._handle_fetch_error)
        thread.finished.connect(lambda token=request_token: self._fetch_threads.pop(token, None))
        self._fetch_threads[request_token] = thread
        thread.start()

    @Slot(int, object)
    def _handle_snapshot_ready(self, request_token: int, snapshot: object) -> None:
        if not isinstance(snapshot, DeribitMarketSnapshot):
            return
        self._apply_snapshot(snapshot, request_token=request_token, from_cache=False)

    @Slot(int, str)
    def _handle_fetch_error(self, request_token: int, message: str) -> None:
        if request_token != self._request_token:
            return
        self._loading = False
        has_current_snapshot = self._latest_snapshot is not None and self._snapshot_matches_current_selection(self._latest_snapshot)
        if has_current_snapshot:
            self._status_label.setText(f"最新网络刷新失败，继续显示当前缓存。{message}")
        else:
            self._status_label.setText("Deribit 波动率指数历史K线获取失败。")
            QMessageBox.critical(self, "获取失败", message)
        if self._pending_refresh_after_load:
            force_network = self._pending_force_network
            self._pending_refresh_after_load = False
            self._pending_force_network = False
            QTimer.singleShot(
                10,
                lambda force=force_network: self._refresh_for_current_selection(
                    use_cache=True,
                    force_network=force,
                    supersede_if_loading=False,
                ),
            )

    def _apply_snapshot(
        self,
        snapshot: DeribitMarketSnapshot,
        *,
        request_token: int | None,
        from_cache: bool,
    ) -> None:
        if request_token is not None and request_token != self._request_token:
            return
        selection_matches = self._snapshot_matches_current_selection(snapshot)
        if not from_cache:
            self._loading = False
        if not selection_matches:
            if self._pending_refresh_after_load:
                force_network = self._pending_force_network
                self._pending_refresh_after_load = False
                self._pending_force_network = False
                QTimer.singleShot(
                    10,
                    lambda force=force_network: self._refresh_for_current_selection(
                        use_cache=True,
                        force_network=force,
                        supersede_if_loading=False,
                    ),
                )
            return
        previous_snapshot = self._latest_snapshot
        previous_last_ts = _snapshot_last_ts(previous_snapshot) if previous_snapshot is not None else None
        previous_key = (
            previous_snapshot.currency,
            previous_snapshot.resolution,
            previous_snapshot.day_align_label,
            previous_snapshot.requested_limit,
        ) if previous_snapshot is not None else None
        current_key = (
            snapshot.currency,
            snapshot.resolution,
            snapshot.day_align_label,
            snapshot.requested_limit,
        )
        current_last_ts = _snapshot_last_ts(snapshot)
        no_new_candle = (
            not from_cache
            and previous_key == current_key
            and previous_last_ts == current_last_ts
            and previous_snapshot is not None
            and len(previous_snapshot.aligned_volatility_candles) == len(snapshot.aligned_volatility_candles)
            and len(previous_snapshot.aligned_spot_candles) == len(snapshot.aligned_spot_candles)
        )
        self._latest_snapshot = snapshot
        self._set_default_chart_view(snapshot)
        resolution_text = _resolution_label_for_value(snapshot.resolution)
        local_note = self._local_note_for_resolution(snapshot.resolution)
        self._spot_chart_title.setText(f"{snapshot.spot_inst_id} 现货K线 | {resolution_text}{local_note}")
        self._status_label.setText(
            "已从本地缓存恢复图表，并在后台继续同步最新数据。"
            if from_cache
            else (
                "Deribit 波动率K线已刷新，当前暂无新增K线。"
                if no_new_candle
                else "Deribit 波动率K线与同币种现货K线获取完成，支持滚轮缩放、左键拖动和双击重置视图。"
            )
        )
        if snapshot.aligned_volatility_candles and snapshot.aligned_spot_candles:
            self._summary_label.setText(
                f"{snapshot.currency} | 周期 {resolution_text}{local_note} | "
                f"波动率 {len(snapshot.volatility_candles)} 根 | 现货 {len(snapshot.spot_candles)} 根 | "
                f"共同时间 {len(snapshot.aligned_volatility_candles)} 根 | "
                f"{_format_ts(snapshot.aligned_volatility_candles[0].ts)} -> {_format_ts(snapshot.aligned_volatility_candles[-1].ts)} | "
                f"获取时间 {snapshot.fetched_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            self._summary_label.setText("当前区间缺少可联动的共同K线数据。")
        self._render_linked_charts(snapshot)
        self._schedule_auto_refresh()
        if self._pending_refresh_after_load:
            force_network = self._pending_force_network
            self._pending_refresh_after_load = False
            self._pending_force_network = False
            QTimer.singleShot(
                10,
                lambda force=force_network: self._refresh_for_current_selection(
                    use_cache=True,
                    force_network=force,
                    supersede_if_loading=False,
                ),
            )

    def export_csv(self) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None or not snapshot.aligned_volatility_candles:
            QMessageBox.information(self, "没有数据", "请先获取历史K线。")
            return
        export_dir = deribit_report_export_dir_path()
        export_dir.mkdir(parents=True, exist_ok=True)
        resolution_text = _resolution_file_label(self._resolution_combo.currentText())
        base_name = f"{snapshot.currency}_{resolution_text}_{snapshot.fetched_at.strftime('%Y%m%d_%H%M%S')}"
        vol_path = export_dir / f"deribit_vol_{base_name}.csv"
        spot_path = export_dir / f"deribit_spot_{base_name}.csv"
        with vol_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "time", "open", "high", "low", "close"])
            for candle in snapshot.aligned_volatility_candles:
                writer.writerow([candle.ts, _format_ts(candle.ts), str(candle.open), str(candle.high), str(candle.low), str(candle.close)])
        with spot_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "time", "open", "high", "low", "close"])
            for candle in snapshot.aligned_spot_candles:
                writer.writerow([candle.ts, _format_ts(candle.ts), str(candle.open), str(candle.high), str(candle.low), str(candle.close)])
        QMessageBox.information(self, "导出成功", f"已导出到：\n{vol_path}\n{spot_path}")

    def reset_chart_view(self) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None:
            return
        self._set_default_chart_view(snapshot)
        self._render_linked_charts(snapshot)

    def _set_default_chart_view(self, snapshot: DeribitMarketSnapshot) -> None:
        start_index, visible_count = _default_chart_viewport(
            len(snapshot.aligned_volatility_candles),
            snapshot.requested_limit,
            min_visible=24,
        )
        self._chart_viewport_start = start_index
        self._chart_viewport_visible = visible_count

    def _render_linked_charts(self, snapshot: DeribitMarketSnapshot) -> None:
        vol_candles = list(snapshot.aligned_volatility_candles)
        spot_candles = list(snapshot.aligned_spot_candles)
        if self._average_kline_check.isChecked():
            vol_candles = _to_average_volatility_candles(vol_candles)
            spot_candles = _to_average_price_candles(spot_candles)
        total = len(vol_candles)
        start_index, visible_count = _normalize_chart_viewport(
            self._chart_viewport_start,
            self._chart_viewport_visible,
            total,
            min_visible=24,
        )
        self._chart_viewport_start = start_index
        self._chart_viewport_visible = visible_count
        end_index = min(total, start_index + visible_count)
        visible_vol = vol_candles[start_index:end_index]
        visible_spot = spot_candles[start_index:end_index]
        resolution_label = self._resolution_combo.currentText()
        self._volatility_chart.set_chart_payload(
            title=f"{snapshot.currency} Deribit 波动率指数K线 | {resolution_label}",
            candles=visible_vol,
            empty_message="暂无可显示的波动率K线。",
        )
        self._spot_chart.set_chart_payload(
            title=f"{snapshot.spot_inst_id} 现货K线 | {resolution_label}",
            candles=visible_spot,
            empty_message="暂无可显示的现货K线。",
        )
        self._clear_chart_hover()

    def _snapshot_matches_current_selection(self, snapshot: DeribitMarketSnapshot) -> bool:
        limit = self._validated_limit()
        if limit is None:
            return False
        current_resolution = str(self._resolution_combo.currentData() or DERIBIT_BASE_HOURLY_RESOLUTION)
        return (
            snapshot.currency == self._currency_combo.currentText().strip().upper()
            and snapshot.resolution == current_resolution
            and snapshot.day_align_label == self._day_align_combo.currentText().strip()
            and snapshot.requested_limit == limit
        )

    def _schedule_auto_refresh(self) -> None:
        self._auto_refresh_timer.stop()
        resolution = str(self._resolution_combo.currentData() or DERIBIT_BASE_HOURLY_RESOLUTION)
        delay_ms = _next_refresh_delay_ms(self._latest_snapshot, resolution)
        self._auto_refresh_timer.start(delay_ms)

    @Slot()
    def _auto_refresh(self) -> None:
        self._refresh_for_current_selection(use_cache=True, force_network=True, supersede_if_loading=False)

    def _local_note_for_resolution(self, resolution: str) -> str:
        if resolution == "1D":
            return f"（本地4小时对齐聚合，{self._day_align_combo.currentText()}收线）"
        if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
            return "（本地聚合）"
        return ""

    @Slot()
    def _on_chart_style_changed(self) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None:
            return
        self._render_linked_charts(snapshot)

    @Slot(float, bool)
    def _on_chart_zoom_requested(self, anchor_ratio: float, zoom_in: bool) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None:
            return
        next_start, next_visible = _zoom_chart_viewport(
            start_index=self._chart_viewport_start,
            visible_count=self._chart_viewport_visible,
            total_count=len(snapshot.aligned_volatility_candles),
            anchor_ratio=anchor_ratio,
            zoom_in=zoom_in,
            min_visible=24,
        )
        if next_start == self._chart_viewport_start and next_visible == self._chart_viewport_visible:
            return
        self._chart_viewport_start = next_start
        self._chart_viewport_visible = next_visible
        self._render_linked_charts(snapshot)

    @Slot(float, float)
    def _on_chart_pan_requested(self, delta_px: float, plot_width: float) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None or not snapshot.aligned_volatility_candles:
            return
        visible_count = self._chart_viewport_visible or len(snapshot.aligned_volatility_candles)
        step = max(float(plot_width) / max(visible_count, 1), 1.0)
        index_delta = int(round(-delta_px / step))
        next_start = _pan_chart_viewport(
            self._chart_viewport_start,
            visible_count,
            len(snapshot.aligned_volatility_candles),
            index_delta,
            min_visible=24,
        )
        if next_start == self._chart_viewport_start:
            return
        self._chart_viewport_start = next_start
        self._render_linked_charts(snapshot)

    @Slot()
    def _on_selection_changed(self) -> None:
        _save_ui_state({"day_align_label": self._day_align_combo.currentText().strip()})
        self._on_resolution_changed()
        self._refresh_for_current_selection(use_cache=True, force_network=True, supersede_if_loading=True)

    @Slot()
    def _on_limit_changed(self) -> None:
        self._refresh_for_current_selection(use_cache=True, force_network=False, supersede_if_loading=True)

    @Slot()
    def _on_resolution_changed(self) -> None:
        is_daily = str(self._resolution_combo.currentData() or DERIBIT_BASE_HOURLY_RESOLUTION) == "1D"
        self._day_align_combo.setEnabled(is_daily)
        if not is_daily:
            self._day_align_combo.setCurrentText("北京时间凌晨12点")

    @Slot(int, float)
    def _sync_chart_hover(self, index: int, y_ratio: float) -> None:
        self._volatility_chart.set_linked_hover(index, y_ratio)
        self._spot_chart.set_linked_hover(index, y_ratio)

    @Slot()
    def _clear_chart_hover(self) -> None:
        self._volatility_chart.set_linked_hover(None, None)
        self._spot_chart.set_linked_hover(None, None)


def _cache_file_path() -> Path:
    return deribit_volatility_cache_file_path()


def _load_cache_payload() -> dict:
    path = _cache_file_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_cache_payload(payload: dict) -> None:
    path = _cache_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


def _load_saved_ui_state() -> dict:
    item = _load_cache_payload().get(DERIBIT_VOLATILITY_UI_STATE_KEY)
    return item if isinstance(item, dict) else {}


def _save_ui_state(state: dict[str, object]) -> None:
    payload = _load_cache_payload()
    payload[DERIBIT_VOLATILITY_UI_STATE_KEY] = state
    _save_cache_payload(payload)


def _hourly_cache_key(currency: str) -> str:
    return f"{currency}|hourly_base"


def _load_cached_hourly_series(
    currency: str,
    *,
    payload: dict | None = None,
) -> tuple[str, list[DeribitVolatilityCandle], list[Candle], datetime] | None:
    cache_payload = payload if payload is not None else _load_cache_payload()
    item = cache_payload.get(_hourly_cache_key(currency))
    if not isinstance(item, dict):
        return None
    try:
        volatility_candles = [
            DeribitVolatilityCandle(
                ts=int(candle["ts"]),
                open=Decimal(str(candle["open"])),
                high=Decimal(str(candle["high"])),
                low=Decimal(str(candle["low"])),
                close=Decimal(str(candle["close"])),
            )
            for candle in item.get("volatility_hourly", [])
        ]
        spot_candles = [
            Candle(
                ts=int(candle["ts"]),
                open=Decimal(str(candle["open"])),
                high=Decimal(str(candle["high"])),
                low=Decimal(str(candle["low"])),
                close=Decimal(str(candle["close"])),
                volume=Decimal(str(candle.get("volume", "0"))),
                confirmed=bool(candle.get("confirmed", True)),
            )
            for candle in item.get("spot_hourly", [])
        ]
        if not volatility_candles or not spot_candles:
            return None
        return (
            str(item.get("spot_inst_id", OKX_SPOT_SYMBOLS[currency])),
            volatility_candles,
            [candle for candle in spot_candles if candle.confirmed],
            datetime.fromisoformat(str(item["fetched_at"])),
        )
    except Exception:
        return None


def _save_cached_hourly_series(
    currency: str,
    *,
    spot_inst_id: str,
    volatility_candles: list[DeribitVolatilityCandle],
    spot_candles: list[Candle],
    fetched_at: datetime,
) -> None:
    payload = _load_cache_payload()
    payload[_hourly_cache_key(currency)] = {
        "spot_inst_id": spot_inst_id,
        "fetched_at": fetched_at.isoformat(),
        "volatility_hourly": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
            }
            for candle in volatility_candles
        ],
        "spot_hourly": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
                "confirmed": candle.confirmed,
            }
            for candle in spot_candles
        ],
    }
    _save_cache_payload(payload)


def _load_cached_snapshot(
    currency: str,
    resolution: str,
    day_align_label: str,
    limit: int,
) -> DeribitMarketSnapshot | None:
    hourly_series = _load_cached_hourly_series(currency)
    if hourly_series is None:
        return None
    spot_inst_id, volatility_hourly, spot_hourly, fetched_at = hourly_series
    return _build_snapshot_from_hourly(
        currency=currency,
        resolution=resolution,
        day_align_label=day_align_label,
        requested_limit=limit,
        spot_inst_id=spot_inst_id,
        volatility_hourly=volatility_hourly,
        spot_hourly=spot_hourly,
        fetched_at=fetched_at,
    )


def _build_snapshot_from_hourly(
    *,
    currency: str,
    resolution: str,
    day_align_label: str,
    requested_limit: int,
    spot_inst_id: str,
    volatility_hourly: list[DeribitVolatilityCandle],
    spot_hourly: list[Candle],
    fetched_at: datetime,
) -> DeribitMarketSnapshot:
    if resolution == DERIBIT_BASE_HOURLY_RESOLUTION:
        volatility_candles = list(volatility_hourly)
        spot_candles = [candle for candle in spot_hourly if candle.confirmed]
    elif resolution == "14400":
        volatility_candles = _aggregate_candles_to_resolution(volatility_hourly, 14_400_000)
        spot_candles = _aggregate_price_candles_to_resolution(
            [candle for candle in spot_hourly if candle.confirmed],
            14_400_000,
        )
    else:
        anchor_offset_ms = _daily_anchor_offset_ms(day_align_label)
        volatility_4h = _aggregate_candles_to_resolution(volatility_hourly, 14_400_000)
        spot_4h = _aggregate_price_candles_to_resolution(
            [candle for candle in spot_hourly if candle.confirmed],
            14_400_000,
        )
        volatility_candles = _aggregate_candles_to_resolution(
            volatility_4h,
            86_400_000,
            anchor_offset_ms=anchor_offset_ms,
        )
        spot_candles = _aggregate_price_candles_to_resolution(
            spot_4h,
            86_400_000,
            anchor_offset_ms=anchor_offset_ms,
        )
    if requested_limit > 0:
        volatility_candles = volatility_candles[-requested_limit:]
        spot_candles = spot_candles[-requested_limit:]
    aligned_volatility, aligned_spot = _align_candles_by_timestamp(volatility_candles, spot_candles)
    return DeribitMarketSnapshot(
        currency=currency,
        resolution=resolution,
        day_align_label=day_align_label,
        requested_limit=requested_limit,
        volatility_candles=volatility_candles,
        spot_inst_id=spot_inst_id,
        spot_candles=spot_candles,
        aligned_volatility_candles=aligned_volatility,
        aligned_spot_candles=aligned_spot,
        fetched_at=fetched_at,
    )


def _daily_anchor_offset_ms(day_align_label: str) -> int:
    anchor_hour = DAY_ALIGN_OPTIONS.get(day_align_label, 0)
    utc_hour = (anchor_hour - 8) % 24
    return utc_hour * 3_600_000
