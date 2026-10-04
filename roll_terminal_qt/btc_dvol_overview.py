from __future__ import annotations

import json
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
from PySide6.QtCore import QDateTime, QMargins, QPointF, Qt, QTimer, Signal, Slot
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QApplication, QGridLayout, QGroupBox, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from okx_quant.candle_cache import load_candle_cache
from okx_quant.contract_recommendations import ContractRecommendation, build_contract_recommendation
from okx_quant.deribit_client import DeribitVolatilityCandle
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
    if not cache_path.exists():
        return []
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        raw_rows = payload.get(f"{normalized_asset}|hourly_base", {}).get("volatility_hourly", [])
    except (OSError, ValueError, AttributeError):
        return []
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


def _aggregate_dvol(candles: list[DeribitVolatilityCandle], interval_ms: int) -> list[Candle]:
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
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
                confirmed=True,
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
        title_font.setPointSize(11)
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
        self.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        self.setMinimumHeight(185)
        self.setMouseTracking(True)
        self.viewport().setMouseTracking(True)

    def set_candles(self, candles: list[Candle], *, error: str | None = None) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        self._candle_series = None
        self._hover_line = None
        if not candles:
            self._chart.setTitle(f"{self._instrument_label} {self._interval} · {error or '暂无本地数据'}")
            return

        closes = [float(candle.close) for candle in candles]
        ema15 = _ema(closes, 15)
        ma50 = _ma(closes, 50)
        visible_count = min(_VISIBLE_BARS[self._interval], len(candles))
        start = len(candles) - visible_count
        visible = candles[start:]
        latest = visible[-1]
        title_suffix = _format_time(latest.ts, with_time=self._interval in {"1H", "4H"})
        value_suffix = "%" if self._is_dvol else ""
        latest_ema15 = ema15[-1]
        latest_ma50 = ma50[-1]
        ema15_text = f"{float(latest_ema15):,.2f}{value_suffix}" if latest_ema15 is not None else "-"
        ma50_text = f"{float(latest_ma50):,.2f}{value_suffix}" if latest_ma50 is not None else "-"
        self._chart.setTitle(
            f"{self._instrument_label} {self._interval} · "
            f"C {float(latest.close):,.2f}{value_suffix}  "
            f"EMA15 {ema15_text}  MA50 {ma50_text} · {title_suffix}"
        )

        candle_series = QCandlestickSeries()
        candle_series.setIncreasingColor(QColor("#15803d"))
        candle_series.setDecreasingColor(QColor("#dc2626"))
        candle_series.setBodyWidth(0.62)
        candle_series.setCapsWidth(0.18)
        candle_series.setBodyOutlineVisible(False)
        for candle in visible:
            candle_series.append(
                QCandlestickSet(
                    float(candle.open),
                    float(candle.high),
                    float(candle.low),
                    float(candle.close),
                    candle.ts,
                )
            )
        self._chart.addSeries(candle_series)
        self._candle_series = candle_series

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


class DvolOverviewPanel(QWidget):
    """One-screen asset / asset-DVOL multi-timeframe overview from local caches."""

    def __init__(self, asset: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._asset = asset.strip().upper()
        self.setWindowTitle(f"{self._asset} × DVOL 总览")
        self.resize(1600, 1080)
        self._screenshot_mode = False
        self._charts: list[LinkedOverviewChart] = []
        self._status = QLabel(f"正在读取本地 {self._asset} 与 {self._asset}-DVOL 缓存…")
        self._status.setWordWrap(True)
        self._status.setObjectName("Subtle")
        self._recommendation_snapshot: ContractRecommendation | None = None
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
        copy_screenshot = QPushButton("复制总览截图")
        copy_screenshot.clicked.connect(self.copy_screenshot)
        self._screenshot_button = QPushButton("截图模式")
        self._screenshot_button.setCheckable(True)
        self._screenshot_button.toggled.connect(self.set_screenshot_mode)
        self._toolbar = QWidget(self)
        toolbar_layout = QHBoxLayout(self._toolbar)
        toolbar_layout.setContentsMargins(0, 0, 0, 0)
        toolbar_layout.addWidget(refresh)
        toolbar_layout.addWidget(copy_screenshot)
        toolbar_layout.addWidget(self._screenshot_button)
        toolbar_layout.addStretch(1)
        toolbar_layout.addWidget(QLabel(f"左 {self._asset} / 右 {self._asset}-DVOL · 悬停任意图联动时间线"))

        grid = QGridLayout()
        grid.setHorizontalSpacing(7)
        grid.setVerticalSpacing(5)
        for row, interval in enumerate(_INTERVALS):
            price_chart = LinkedOverviewChart(instrument_label=self._asset, interval=interval, is_dvol=False, parent=self)
            dvol_chart = LinkedOverviewChart(instrument_label=f"{self._asset}-DVOL", interval=interval, is_dvol=True, parent=self)
            for chart in (price_chart, dvol_chart):
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

    def refresh(self) -> None:
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
            interval: _aggregate_dvol(dvol_hourly, _INTERVAL_MS[interval])
            for interval in _INTERVALS
        }
        for chart in self._charts:
            source = dvol_by_interval if chart._is_dvol else price_by_interval
            chart.set_candles(source.get(chart._interval, []), error=f"{self._asset}-DVOL 本地缓存缺失" if chart._is_dvol else None)

        price_latest = max((items[-1].ts for items in price_by_interval.values() if items), default=None)
        dvol_latest = dvol_hourly[-1].ts if dvol_hourly else None
        status_parts = [
            f"8图联动总览（左 {self._asset} / 右 {self._asset}-DVOL）",
            f"{self._asset} 最新：{_format_time(price_latest, with_time=True) if price_latest else '缺失'}",
            f"DVOL 最新：{_format_time(dvol_latest, with_time=True) if dvol_latest else '缺失'}",
            "悬停任意图可同步时间线",
        ]
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


class BtcDvolOverviewPanel(DvolOverviewPanel):
    """BTC / BTC-DVOL overview, retained for existing callers."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("BTC", parent)


class EthDvolOverviewPanel(DvolOverviewPanel):
    """ETH / ETH-DVOL overview."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("ETH", parent)
