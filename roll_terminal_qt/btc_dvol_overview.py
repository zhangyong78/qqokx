from __future__ import annotations

import json
import math
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from PySide6.QtCharts import (
    QCandlestickSeries,
    QCandlestickSet,
    QChart,
    QChartView,
    QDateTimeAxis,
    QLineSeries,
    QValueAxis,
)
from PySide6.QtCore import QDateTime, QEvent, QMargins, QPointF, QRectF, Qt, QTimer, Signal, Slot
from PySide6.QtGui import QColor, QFont, QPainter, QPen, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from okx_quant.candle_cache import load_candle_cache
from okx_quant.contract_recommendations import ContractRecommendation, build_contract_recommendation
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.app_paths import ai_snapshots_dir_path
from okx_quant.models import Candle
from okx_quant.persistence import deribit_volatility_cache_file_path


_SHANGHAI = timezone(timedelta(hours=8))
_INTERVALS = ("1W", "1D", "4H", "1H")
_INTERVAL_MS = {
    "1W": 7 * 24 * 60 * 60 * 1000,
    "1D": 24 * 60 * 60 * 1000,
    "4H": 4 * 60 * 60 * 1000,
    "1H": 60 * 60 * 1000,
}
_VISIBLE_BARS = {"1W": 80, "1D": 90, "4H": 100, "1H": 120}
_LONG_IMAGE_WIDTH = 2048
_LONG_CHART_HEIGHTS = {"1W": 430, "1D": 450, "4H": 620, "1H": 620}
_BEIJING_BOUNDARY_OFFSET_MS = -8 * 60 * 60 * 1000
# Unix epoch starts on a Thursday; this anchors weekly DVOL bars to Monday
# 00:00 in Beijing, matching local asset weekly caches.
_BEIJING_MONDAY_WEEK_OFFSET_MS = -(3 * 24 + 8) * 60 * 60 * 1000
# Reject placeholder or malformed values (for example, 3_600_000) before
# weekly aggregation can shift them before the Unix epoch on Windows.
_MIN_VALID_DVOL_TIMESTAMP_MS = 946_684_800_000  # 2000-01-01 UTC


def _format_time(ts: int, *, with_time: bool = False) -> str:
    pattern = "%Y-%m-%d %H:%M" if with_time else "%Y-%m-%d"
    return datetime.fromtimestamp(ts / 1000, _SHANGHAI).strftime(pattern)


def _format_bar_clock(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, _SHANGHAI).strftime("%H:%M")


def _format_duration(minutes: int) -> str:
    hours, remainder = divmod(max(0, int(minutes)), 60)
    if hours:
        return f"{hours}h{remainder:02d}m" if remainder else f"{hours}h"
    return f"{remainder}m"


def _intraday_axis_range(candles: list[Candle], interval: str) -> tuple[int, int, int]:
    """Return a tick-aligned time range for the compact intraday charts.

    ``QDateTimeAxis`` divides its range evenly.  A 100-bar 4H window split
    into three intervals otherwise produces 133h20m ticks, which makes the
    axis look as if the 4H candles were opened at ``:20`` or ``:40``.  Pad the
    left edge by at most two bars so all four tick positions stay on a real
    1H/4H boundary.  The data and linked hover timestamp are untouched.
    """
    interval_ms = _INTERVAL_MS[interval]
    start_ms = int(candles[0].ts)
    end_ms = int(candles[-1].ts) + interval_ms
    tick_count = 4
    tick_intervals = tick_count - 1
    span_ms = max(interval_ms, end_ms - start_ms)
    tick_span_ms = interval_ms * tick_intervals
    aligned_span_ms = ((span_ms + tick_span_ms - 1) // tick_span_ms) * tick_span_ms
    return end_ms - aligned_span_ms, end_ms, tick_count


def _ema(values: list[float], period: int) -> list[float | None]:
    if not values:
        return []
    factor = 2.0 / (period + 1.0)
    result: list[float | None] = []
    current = values[0]
    for value in values:
        current = value * factor + current * (1.0 - factor)
        result.append(current)
    return result


def _ma(values: list[float], period: int) -> list[float | None]:
    result: list[float | None] = []
    for index in range(len(values)):
        if index + 1 < period:
            result.append(None)
        else:
            result.append(sum(values[index - period + 1 : index + 1]) / period)
    return result


def _load_cached_dvol_hourly(asset: str = "BTC") -> list[DeribitVolatilityCandle]:
    """Read an already-fetched asset DVOL cache without invoking Deribit."""
    normalized_asset = asset.strip().upper()
    cache_path: Path = deribit_volatility_cache_file_path()
    candidate_paths = [cache_path]
    recovered_path = cache_path.with_name(f"{normalized_asset.lower()}_dvol_recovered.json")
    if recovered_path != cache_path:
        candidate_paths.append(recovered_path)
    raw_rows = []
    for candidate in candidate_paths:
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if candidate == cache_path:
                raw_rows = payload.get(f"{normalized_asset}|hourly_base", {}).get("volatility_hourly", [])
            else:
                raw_rows = payload.get("volatility_hourly", [])
        except (OSError, ValueError, AttributeError):
            raw_rows = []
        if isinstance(raw_rows, list) and raw_rows:
            break
    candles: list[DeribitVolatilityCandle] = []
    for row in raw_rows if isinstance(raw_rows, list) else []:
        if not isinstance(row, dict):
            continue
        try:
            timestamp = int(row["ts"])
            if timestamp < _MIN_VALID_DVOL_TIMESTAMP_MS:
                continue
            candles.append(
                DeribitVolatilityCandle(
                    ts=timestamp,
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                )
            )
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
    return sorted(candles, key=lambda candle: candle.ts)


def _aggregate_dvol(
    candles: list[DeribitVolatilityCandle],
    interval_ms: int,
    *,
    now_ms: int | None = None,
) -> list[Candle]:
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    anchor_offset = (
        _BEIJING_MONDAY_WEEK_OFFSET_MS
        if interval_ms == _INTERVAL_MS["1W"]
        else _BEIJING_BOUNDARY_OFFSET_MS
    )
    for candle in candles:
        bucket = ((int(candle.ts) - anchor_offset) // interval_ms) * interval_ms + anchor_offset
        buckets.setdefault(bucket, []).append(candle)
    result: list[Candle] = []
    for bucket, group in sorted(buckets.items()):
        ordered = sorted(group, key=lambda candle: candle.ts)
        result.append(
            Candle(
                ts=bucket,
                open=ordered[0].open,
                high=max(candle.high for candle in ordered),
                low=min(candle.low for candle in ordered),
                close=ordered[-1].close,
                volume=Decimal("0"),
                confirmed=bucket + interval_ms <= current_ms,
            )
        )
    return result


class LinkedOverviewChart(QChartView):
    """Compact candlestick chart that broadcasts a shared timestamp hover."""

    # Qt's ``int`` signal type is 32-bit, whereas chart timestamps are Unix
    # milliseconds (currently about 1.7e12).  Use a double across the Qt
    # signal boundary and convert back to an integer before rendering.
    timestamp_hovered = Signal(float)
    hover_cleared = Signal()

    def __init__(self, *, instrument_label: str, interval: str, is_dvol: bool, parent: QWidget | None = None) -> None:
        self._chart = QChart()
        self._chart.legend().hide()
        self._chart.setBackgroundVisible(False)
        self._chart.setMargins(QMargins(3, 3, 3, 3))
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        self._chart.setTitleFont(title_font)
        super().__init__(self._chart, parent)
        self._instrument_label = instrument_label
        self._interval = interval
        self._is_dvol = is_dvol
        self._candle_series: QCandlestickSeries | None = None
        self._hover_line: QLineSeries | None = None
        self._x_min = 0
        self._x_max = 0
        self._y_min = 0.0
        self._y_max = 1.0
        self._bar_state_summary = ""
        self._last_closed_summary = ""
        self.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        self.setMinimumHeight(185)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)

    def set_candles(
        self,
        candles: list[Candle],
        *,
        error: str | None = None,
        now_ms: int | None = None,
        visible_count: int | None = None,
    ) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        self._candle_series = None
        self._hover_line = None
        self._bar_state_summary = ""
        self._last_closed_summary = ""
        if not candles:
            self._chart.setTitle(f"{self._instrument_label} {self._interval} · {error or '暂无本地数据'}")
            return

        closes = [float(candle.close) for candle in candles]
        ema15 = _ema(closes, 15)
        ma50 = _ma(closes, 50)
        requested_visible_count = visible_count if visible_count is not None else _VISIBLE_BARS[self._interval]
        visible_count = min(max(1, int(requested_visible_count)), len(candles))
        start = len(candles) - visible_count
        visible = candles[start:]
        latest = visible[-1]
        current = latest if not latest.confirmed else None
        last_closed = next(
            (candle for candle in reversed(candles[: -1] if current else candles) if candle.confirmed),
            None,
        )
        current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        if current is not None:
            bar_end = int(current.ts) + _INTERVAL_MS[self._interval]
            elapsed_ms = max(0, min(current_ms - int(current.ts), _INTERVAL_MS[self._interval]))
            remaining_ms = max(0, bar_end - current_ms)
            elapsed_minutes = int(elapsed_ms // 60_000)
            remaining_minutes = int(math.ceil(remaining_ms / 60_000))
            progress = min(100.0, max(0.0, elapsed_ms / _INTERVAL_MS[self._interval] * 100.0))
            reference = "高参考" if remaining_minutes <= 15 else "中等参考" if remaining_minutes <= 30 else "低参考"
            self._bar_state_summary = (
                f"当前形成K {_format_bar_clock(current.ts)}–{_format_bar_clock(bar_end)}，"
                f"未收盘，已运行 {_format_duration(elapsed_minutes)} / 剩余 {_format_duration(remaining_minutes)}，"
                f"完成度 {progress:.0f}%，{reference}"
            )
            if last_closed is not None:
                last_closed_end = int(last_closed.ts) + _INTERVAL_MS[self._interval]
                self._last_closed_summary = (
                    f"最近已收盘K {_format_bar_clock(last_closed.ts)}–{_format_bar_clock(last_closed_end)}"
                )
        elif latest.confirmed:
            self._bar_state_summary = f"最新K已收盘：{_format_bar_clock(latest.ts)}–{_format_bar_clock(int(latest.ts) + _INTERVAL_MS[self._interval])}"
        title_suffix = _format_time(latest.ts, with_time=self._interval in {"1H", "4H"})
        value_suffix = "%" if self._is_dvol else ""
        latest_ema15 = ema15[-1]
        latest_ma50 = ma50[-1]
        ema15_text = f"{float(latest_ema15):,.2f}{value_suffix}" if latest_ema15 is not None else "-"
        ma50_text = f"{float(latest_ma50):,.2f}{value_suffix}" if latest_ma50 is not None else "-"
        numeric_title = (
            f"C {float(latest.close):,.2f}{value_suffix}  "
            f"EMA15 {ema15_text}  MA50 {ma50_text} · {title_suffix}"
        )
        if current is not None:
            status_title = (
                f"{self._instrument_label} {self._interval} · ⚠ 未收盘 · "
                f"当前K {_format_bar_clock(current.ts)}–{_format_bar_clock(int(current.ts) + _INTERVAL_MS[self._interval])} · "
                f"已运行 {_format_duration(elapsed_minutes)} / 剩余 {_format_duration(remaining_minutes)} · "
                f"完成度 {progress:.0f}% · {reference}"
            )
            self._chart.setTitle(f"{status_title}\n{numeric_title}")
        else:
            self._chart.setTitle(f"{self._instrument_label} {self._interval} · 已收盘 · {numeric_title}")

        closed_series = QCandlestickSeries()
        closed_series.setIncreasingColor(QColor("#15803d"))
        closed_series.setDecreasingColor(QColor("#dc2626"))
        closed_series.setBodyWidth(0.62)
        closed_series.setCapsWidth(0.18)
        closed_series.setBodyOutlineVisible(False)
        for candle in visible:
            if not candle.confirmed:
                continue
            closed_series.append(
                QCandlestickSet(
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    candle.ts,
                )
            )
        self._chart.addSeries(closed_series)
        self._candle_series = closed_series
        if current is not None:
            current_series = QCandlestickSeries()
            current_series.setIncreasingColor(QColor("#94a3b8"))
            current_series.setDecreasingColor(QColor("#94a3b8"))
            current_series.setBodyWidth(0.62)
            current_series.setCapsWidth(0.18)
            current_series.setBodyOutlineVisible(True)
            current_series.setOpacity(0.60)
            current_series.append(
                QCandlestickSet(
                    float(current.open),
                    float(current.high),
                    float(current.low),
                    float(current.close),
                    current.ts,
                )
            )
            self._chart.addSeries(current_series)
            self._candle_series = current_series

        ema_series = QLineSeries()
        ema_pen = QPen(QColor("#f59e0b"))
        ema_pen.setWidth(2)
        ema_series.setPen(ema_pen)
        ma_series = QLineSeries()
        ma_pen = QPen(QColor("#7c3aed"))
        ma_pen.setWidth(2)
        ma_series.setPen(ma_pen)
        for index in range(start, len(candles)):
            if ema15[index] is not None:
                ema_series.append(candles[index].ts, float(ema15[index]))
            if ma50[index] is not None:
                ma_series.append(candles[index].ts, float(ma50[index]))
        self._chart.addSeries(ema_series)
        self._chart.addSeries(ma_series)

        values = [float(item.low) for item in visible] + [float(item.high) for item in visible]
        values.extend(float(value) for value in ema15[start:] if value is not None)
        values.extend(float(value) for value in ma50[start:] if value is not None)
        raw_min, raw_max = min(values), max(values)
        padding = max((raw_max - raw_min) * 0.08, 0.01 if self._is_dvol else 1.0)
        self._y_min, self._y_max = raw_min - padding, raw_max + padding
        if self._interval in {"1H", "4H"}:
            self._x_min, self._x_max, x_tick_count = _intraday_axis_range(visible, self._interval)
        else:
            self._x_min = int(visible[0].ts)
            self._x_max = int(visible[-1].ts + _INTERVAL_MS[self._interval])
            x_tick_count = 4

        x_axis = QDateTimeAxis()
        x_axis.setFormat("MM-dd\nHH:mm" if self._interval in {"1H", "4H"} else "MM-dd")
        x_axis.setLabelsAngle(-30)
        x_axis.setTickCount(x_tick_count)
        x_axis.setRange(QDateTime.fromMSecsSinceEpoch(self._x_min), QDateTime.fromMSecsSinceEpoch(self._x_max))
        y_axis = QValueAxis()
        y_axis.setLabelFormat("%.1f" if self._is_dvol else "%.0f")
        y_axis.setTickCount(4)
        y_axis.setRange(self._y_min, self._y_max)
        self._chart.addAxis(x_axis, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(y_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(x_axis)
            series.attachAxis(y_axis)

        hover_line = QLineSeries()
        hover_line.setName("")
        hover_pen = QPen(QColor("#64748b"))
        hover_pen.setWidth(1)
        hover_pen.setStyle(Qt.PenStyle.DashLine)
        hover_line.setPen(hover_pen)
        self._chart.addSeries(hover_line)
        hover_line.attachAxis(x_axis)
        hover_line.attachAxis(y_axis)
        self._hover_line = hover_line

    def set_linked_timestamp(self, timestamp: int | None) -> None:
        if self._hover_line is None:
            return
        self._hover_line.clear()
        if timestamp is None or timestamp < self._x_min or timestamp > self._x_max:
            return
        self._hover_line.append(timestamp, self._y_min)
        self._hover_line.append(timestamp, self._y_max)

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        if self._candle_series is not None and self.chart().plotArea().contains(event.position()):
            value = self.chart().mapToValue(QPointF(event.position()), self._candle_series)
            self.timestamp_hovered.emit(value.x())
        super().mouseMoveEvent(event)

    def leaveEvent(self, event) -> None:  # type: ignore[override]
        self.hover_cleared.emit()
        super().leaveEvent(event)


class AIAnalysisLongImageWindow(QWidget):
    """Scrollable preview for the saved AI analysis image content."""

    def __init__(self, content: QWidget, *, asset: str) -> None:
        # Do not attach this preview to the overview panel.  A QWidget with a
        # widget parent is a child surface on Windows, whose maximize button
        # only fills the parent panel instead of the desktop work area.
        super().__init__(None)
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowMinMaxButtonsHint
            | Qt.WindowType.WindowCloseButtonHint
        )
        self.setWindowTitle(f"AI分析长图 · {asset} × {asset}-DVOL")
        self.resize(_LONG_IMAGE_WIDTH, 1000)
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(False)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setWidget(content)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(scroll)


class DvolOverviewPanel(QWidget):
    """One-screen asset / asset-DVOL multi-timeframe overview from local caches."""

    def __init__(self, asset: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._asset = asset.strip().upper()
        self.setWindowTitle(f"{self._asset} × DVOL 总览")
        self.resize(1600, 1080)
        self._screenshot_mode = False
        self._charts: list[LinkedOverviewChart] = []
        self._price_by_interval: dict[str, list[Candle]] = {}
        self._dvol_by_interval: dict[str, list[Candle]] = {}
        self._last_refresh_ms = int(time.time() * 1000)
        self._long_window: AIAnalysisLongImageWindow | None = None
        self._status = QLabel(f"正在读取本地 {self._asset} 与 {self._asset}-DVOL 缓存…")
        self._status.setWordWrap(True)
        self._status.setObjectName("Subtle")
        self._recommendation_snapshot: ContractRecommendation | None = None
        # Kept separate from the program rule result so a manual/AI candidate
        # can never be replaced when the local recommendation is refreshed.
        self._manual_ai_candidate_text = ""
        self._recommendation_box = QGroupBox(f"合约策略建议 · {self._asset}-USDT-SWAP", self)
        recommendation_layout = QHBoxLayout(self._recommendation_box)
        self._recommendation_main = QLabel()
        self._recommendation_details = QLabel()
        for label in (self._recommendation_main, self._recommendation_details):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
            label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
            label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        recommendation_layout.addWidget(self._recommendation_main, 4)
        recommendation_layout.addWidget(self._recommendation_details, 6)
        self._recommendation_box.setToolTip(
            f"使用已收盘{self._asset} K线：日线/4H定方向、1H定触发、周线作背景。"
            "EMA15/MA50与图表一致；EMA差>0.2×ATR14、EMA15三根斜率同向、前20根高低作突破参考。"
            "这些是未经收益回测的规则模板，不是已有自动交易策略的开仓信号；DVOL仅作波动背景。"
        )

        refresh = QPushButton("刷新总览")
        refresh.clicked.connect(self.refresh)
        copy_screenshot = QPushButton("行情总览图")
        copy_screenshot.clicked.connect(self.copy_screenshot)
        long_screenshot = QPushButton("AI分析长图")
        long_screenshot.clicked.connect(self.generate_ai_long_image)
        self._screenshot_button = QPushButton("截图模式")
        self._screenshot_button.setCheckable(True)
        self._screenshot_button.toggled.connect(self.set_screenshot_mode)
        self._toolbar = QWidget(self)
        toolbar_layout = QHBoxLayout(self._toolbar)
        toolbar_layout.setContentsMargins(0, 0, 0, 0)
        toolbar_layout.addWidget(refresh)
        toolbar_layout.addWidget(copy_screenshot)
        toolbar_layout.addWidget(long_screenshot)
        toolbar_layout.addWidget(self._screenshot_button)
        toolbar_layout.addStretch(1)
        toolbar_layout.addWidget(QLabel(f"左 {self._asset} / 右 {self._asset}-DVOL · 悬停任意图联动时间线"))

        grid = QGridLayout()
        self._chart_grid = grid
        grid.setHorizontalSpacing(7)
        grid.setVerticalSpacing(5)
        for row, interval in enumerate(_INTERVALS):
            price_chart = LinkedOverviewChart(instrument_label=self._asset, interval=interval, is_dvol=False, parent=self)
            dvol_chart = LinkedOverviewChart(instrument_label=f"{self._asset}-DVOL", interval=interval, is_dvol=True, parent=self)
            for chart in (price_chart, dvol_chart):
                chart.setMinimumWidth(0)
                chart.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
                chart.timestamp_hovered.connect(self._sync_hover)
                chart.hover_cleared.connect(self._clear_hover)
                self._charts.append(chart)
            grid.addWidget(price_chart, row, 0)
            grid.addWidget(dvol_chart, row, 1)
            is_execution_timeframe = interval in {"4H", "1H"}
            grid.setRowMinimumHeight(row, 210 if is_execution_timeframe else 185)
            grid.setRowStretch(row, 11 if is_execution_timeframe else 9)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 1)
        grid.setColumnMinimumWidth(0, 0)
        grid.setColumnMinimumWidth(1, 0)

        layout = QVBoxLayout(self)
        layout.addWidget(self._toolbar)
        layout.addWidget(self._status)
        layout.addWidget(self._recommendation_box)
        layout.addLayout(grid, 1)
        self.refresh()
        # Recheck cache age while the window remains open, without a network job.
        self._freshness_timer = QTimer(self)
        self._freshness_timer.setInterval(60_000)
        self._freshness_timer.timeout.connect(self._refresh_recommendation_age)
        self._freshness_timer.start()

    def changeEvent(self, event: QEvent) -> None:  # type: ignore[override]
        super().changeEvent(event)
        if event.type() == QEvent.Type.WindowStateChange:
            # QChartView can retain an old backing-store size during a native
            # maximize/restore.  Re-run its layout after the window manager
            # has supplied the final geometry, rather than leaving the two
            # chart columns with stale row positions.
            QTimer.singleShot(0, self._reflow_chart_grid)
            QTimer.singleShot(80, self._reflow_chart_grid)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        QTimer.singleShot(0, self._reflow_chart_grid)

    def _reflow_chart_grid(self) -> None:
        if not hasattr(self, "_chart_grid"):
            return
        layout = self.layout()
        if layout is None:
            return
        layout.activate()
        self._chart_grid.activate()
        for chart in self._charts:
            chart.chart().setPlotArea(QRectF())
            chart.updateGeometry()
            chart.viewport().update()
            chart.update()

    def refresh(self) -> None:
        now_ms = int(time.time() * 1000)
        self._last_refresh_ms = now_ms
        price_by_interval: dict[str, list[Candle]] = {}
        errors: list[str] = []
        for interval in _INTERVALS:
            try:
                price_by_interval[interval] = load_candle_cache(f"{self._asset}-USDT-SWAP", interval, limit=None)
            except Exception as exc:  # noqa: BLE001
                price_by_interval[interval] = []
                errors.append(f"{self._asset} {interval}: {exc}")

        dvol_hourly = _load_cached_dvol_hourly(self._asset)
        dvol_by_interval = {
            interval: _aggregate_dvol(dvol_hourly, _INTERVAL_MS[interval], now_ms=now_ms)
            for interval in _INTERVALS
        }
        self._price_by_interval = price_by_interval
        self._dvol_by_interval = dvol_by_interval
        for chart in self._charts:
            source = dvol_by_interval if chart._is_dvol else price_by_interval
            chart.set_candles(
                source.get(chart._interval, []),
                error=f"{self._asset}-DVOL 本地缓存缺失" if chart._is_dvol else None,
                now_ms=now_ms,
            )

        price_latest = max((items[-1].ts for items in price_by_interval.values() if items), default=None)
        dvol_latest = dvol_hourly[-1].ts if dvol_hourly else None
        status_parts = [
            f"8图联动总览（左 {self._asset} / 右 {self._asset}-DVOL）",
            f"{self._asset} 最新：{_format_time(price_latest, with_time=True) if price_latest else '缺失'}",
            f"DVOL 最新：{_format_time(dvol_latest, with_time=True) if dvol_latest else '缺失'}",
            "悬停任意图可同步时间线",
        ]
        for interval in ("4H", "1H"):
            price_chart = next(
                (chart for chart in self._charts if not chart._is_dvol and chart._interval == interval),
                None,
            )
            if price_chart is not None and price_chart._bar_state_summary:
                detail = price_chart._bar_state_summary
                if price_chart._last_closed_summary:
                    detail += f"；{price_chart._last_closed_summary}"
                status_parts.append(f"{self._asset} {interval}：{detail}")
        if errors:
            status_parts.append("；".join(errors[:2]))
        self._status.setText(" | ".join(status_parts))
        self._recommendation_prices = price_by_interval
        self._recommendation_dvol = [
            Candle(c.ts, c.open, c.high, c.low, c.close, Decimal("0"), True) for c in dvol_hourly
        ]
        self._refresh_recommendation_age()

    def _refresh_recommendation_age(self) -> None:
        snapshot = build_contract_recommendation(
            price_candles=self._recommendation_prices, dvol_hourly=self._recommendation_dvol,
            now_ms=int(time.time() * 1000), asset_label=self._asset,
        )
        self._recommendation_snapshot = snapshot
        hourly = next((t for t in snapshot.trends if t.period == "1H"), None)
        source_time = _format_time(hourly.candle_ts, with_time=True) if hourly else "不可用"
        self._recommendation_box.setTitle(f"合约策略建议 · {self._asset}-USDT-SWAP · {snapshot.strategy}")
        self._recommendation_main.setText(
            f"状态：{snapshot.status}\n依据：{snapshot.reason}\n"
            f"波动：{snapshot.volatility_note}\n参考1H开盘时间：{source_time}（已收盘，UTC+8）"
        )
        self._recommendation_details.setText(
            f"触发：{snapshot.entry_condition}\n失效：{snapshot.invalidation}\n"
            f"管理：{snapshot.management}\n规则候选未经收益回测；需人工确认，不自动下单。"
        )

    @Slot(float)
    def _sync_hover(self, timestamp: float) -> None:
        for chart in self._charts:
            chart.set_linked_timestamp(int(timestamp))

    def _clear_hover(self) -> None:
        for chart in self._charts:
            chart.set_linked_timestamp(None)

    def set_screenshot_mode(self, enabled: bool) -> None:
        self._screenshot_mode = enabled
        self._toolbar.setVisible(not enabled)
        self._screenshot_button.setText("退出截图模式" if enabled else "截图模式")

    def copy_screenshot(self) -> None:
        self._refresh_recommendation_age()
        QApplication.clipboard().setPixmap(self.grab())
        self._status.setText(f"已复制 8 图总览截图到剪贴板 | {self._status.text()}")

    def _build_long_image_content(self) -> QWidget:
        """Build the tall, single-column image used for manual AI review."""
        content = QWidget()
        content.setObjectName("AIAnalysisLongImage")
        content.setStyleSheet(
            "QWidget#AIAnalysisLongImage { background: #f8fafc; color: #0f172a; }"
            "QLabel#LongSection { background: #e2e8f0; border: 1px solid #cbd5e1; "
            "padding: 10px 14px; font-size: 17px; font-weight: 700; }"
        )
        content.setFixedWidth(_LONG_IMAGE_WIDTH)
        layout = QVBoxLayout(content)
        layout.setContentsMargins(28, 24, 28, 28)
        layout.setSpacing(12)

        snapshot = self._recommendation_snapshot
        title = QLabel(
            f"AI分析长图 · {self._asset} / {self._asset}-DVOL · "
            f"{_format_time(self._last_refresh_ms, with_time=True)}（北京时间）"
        )
        title.setStyleSheet("font-size: 24px; font-weight: 700; padding: 4px 0 8px;")
        title.setWordWrap(True)
        layout.addWidget(title)

        status_box = QGroupBox("当前交易状态（仅作人工分析上下文，不自动下单）", content)
        status_layout = QVBoxLayout(status_box)
        status_layout.setContentsMargins(14, 12, 14, 12)
        status_lines = [
            f"数据：{self._asset} 与 {self._asset}-DVOL 来自本地缓存；绿色=上涨，红色=下跌，灰色半透明=当前未收盘K。",
            f"程序策略建议：{snapshot.strategy if snapshot else '暂无'}｜状态：{snapshot.status if snapshot else '暂无'}",
            f"程序依据：{snapshot.reason if snapshot else '暂无策略建议'}",
            f"程序触发：{snapshot.entry_condition if snapshot else '暂无'}｜失效：{snapshot.invalidation if snapshot else '暂无'}",
            f"程序管理：{snapshot.management if snapshot else '暂无'}（仅供人工确认，不自动下单）",
            "人工/AI活动候选交易："
            + (self._manual_ai_candidate_text or "当前没有已保存候选；不会用程序策略建议替代人工候选。"),
        ]
        dvol_available = sum(bool(items) for items in self._dvol_by_interval.values())
        status_lines.append(
            f"DVOL数据：{'可用（覆盖 ' + str(dvol_available) + ' 个周期）' if dvol_available else '缺失，本次仍生成长图并保留缺失提示'}"
        )
        for interval in ("4H", "1H"):
            chart = next(
                (item for item in self._charts if not item._is_dvol and item._interval == interval),
                None,
            )
            if chart is not None and chart._bar_state_summary:
                detail = f"{self._asset} {interval}：{chart._bar_state_summary}"
                if chart._last_closed_summary:
                    detail += f"；{chart._last_closed_summary}"
                status_lines.append(detail)
        status_label = QLabel("\n".join(status_lines), status_box)
        status_label.setWordWrap(True)
        status_label.setStyleSheet("font-size: 17px; line-height: 125%;")
        status_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        status_layout.addWidget(status_label)
        layout.addWidget(status_box)

        def add_section(text: str) -> None:
            section = QLabel(text, content)
            section.setObjectName("LongSection")
            layout.addWidget(section)

        def add_chart(
            instrument_label: str,
            interval: str,
            is_dvol: bool,
            candles: list[Candle],
            *,
            visible_count: int | None = None,
            height: int,
        ) -> None:
            chart = LinkedOverviewChart(
                instrument_label=instrument_label,
                interval=interval,
                is_dvol=is_dvol,
                parent=content,
            )
            chart.set_candles(candles, now_ms=self._last_refresh_ms, visible_count=visible_count)
            chart.setFixedSize(_LONG_IMAGE_WIDTH - 56, height)
            layout.addWidget(chart)

        add_section(f"{self._asset} 行情 · 1W / 1D / 4H / 1H")
        for interval in _INTERVALS:
            add_chart(
                self._asset,
                interval,
                False,
                self._price_by_interval.get(interval, []),
                height=_LONG_CHART_HEIGHTS[interval],
            )

        add_section(f"{self._asset} 局部放大 · 4H 最近30根 / 1H 最近50根")
        add_chart(
            self._asset,
            "4H",
            False,
            self._price_by_interval.get("4H", []),
            visible_count=30,
            height=_LONG_CHART_HEIGHTS["4H"],
        )
        add_chart(
            self._asset,
            "1H",
            False,
            self._price_by_interval.get("1H", []),
            visible_count=50,
            height=_LONG_CHART_HEIGHTS["1H"],
        )

        dvol_section_suffix = "" if dvol_available else " · ⚠ 本地缓存缺失"
        add_section(f"{self._asset}-DVOL 波动率 · 1W / 1D / 4H / 1H{dvol_section_suffix}")
        for interval in _INTERVALS:
            add_chart(
                f"{self._asset}-DVOL",
                interval,
                True,
                self._dvol_by_interval.get(interval, []),
                height=_LONG_CHART_HEIGHTS[interval],
            )

        legend = QLabel(
            "图表说明：已收盘K按真实涨跌显示；当前形成K使用灰色半透明标记并在标题中显示“⚠ 未收盘”、"
            "已运行时间、剩余时间、完成度和参考等级。橙色为 EMA15，紫色为 MA50。",
            content,
        )
        legend.setWordWrap(True)
        legend.setStyleSheet("color: #475569; font-size: 14px; padding-top: 8px;")
        layout.addWidget(legend)
        layout.activate()
        content.resize(_LONG_IMAGE_WIDTH, max(5200, content.sizeHint().height()))
        return content

    def set_manual_ai_candidate(self, text: str | None) -> None:
        """Set the separately maintained human/AI activity candidate text."""
        self._manual_ai_candidate_text = str(text or "").strip()

    def generate_ai_long_image(self) -> None:
        """Refresh local data, show a preview, save and copy the AI long image."""
        self.refresh()
        content = self._build_long_image_content()
        window = AIAnalysisLongImageWindow(content, asset=self._asset)
        self._long_window = window
        window.show()
        window.raise_()
        QApplication.processEvents()
        pixmap: QPixmap = content.grab()
        output_dir = ai_snapshots_dir_path()
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(_SHANGHAI).strftime("%Y%m%d_%H%M%S")
        output_path = output_dir / f"qqokx_AI_Long_{self._asset}_{timestamp}.png"
        suffix = 2
        while output_path.exists():
            output_path = output_dir / f"qqokx_AI_Long_{self._asset}_{timestamp}_{suffix}.png"
            suffix += 1
        if not pixmap.save(str(output_path), "PNG"):
            self._status.setText(f"AI分析长图保存失败：{output_path}")
            return
        QApplication.clipboard().setPixmap(pixmap)
        self._status.setText(
            f"AI分析长图已生成并复制：{output_path}（{pixmap.width()}×{pixmap.height()}）"
        )


class BtcDvolOverviewPanel(DvolOverviewPanel):
    """BTC / BTC-DVOL overview, retained for existing callers."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("BTC", parent)


class EthDvolOverviewPanel(DvolOverviewPanel):
    """ETH / ETH-DVOL overview."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("ETH", parent)
