"""Local, frozen BTC RV model surface; all loading happens off the UI thread."""
from __future__ import annotations

from datetime import datetime

from PySide6.QtCharts import QChart, QChartView, QDateTimeAxis, QLineSeries, QValueAxis
from PySide6.QtCore import QDateTime, QThread, QTimeZone, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QAbstractItemView, QHBoxLayout, QLabel, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget

from okx_quant.volatility_prediction import SHANGHAI, predict_local


class _PredictionWorker(QThread):
    ready = Signal(object)
    failed = Signal(str)

    def run(self) -> None:
        try:
            self.ready.emit(predict_local())
        except Exception as exc:
            self.failed.emit(str(exc))


def _timestamp(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=SHANGHAI).timestamp() * 1000)


class VolatilityPredictionPanel(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._worker: _PredictionWorker | None = None
        self._snapshot: dict | None = None
        self._refresh = QPushButton("刷新波动率预测")
        self._refresh.clicked.connect(self.refresh_prediction)
        toolbar = QHBoxLayout()
        toolbar.addWidget(self._refresh)
        toolbar.addStretch()
        toolbar.addWidget(QLabel("本地统计模型 · 冻结参数 · 无联网 / 无大模型"))
        self._status = QLabel("正在读取本地完整日数据…")
        self._status.setWordWrap(True)
        self._cards = QLabel()
        self._cards.setWordWrap(True)
        self._cards.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._cards.setObjectName("VolatilityCards")
        self._chart = QChart()
        self._chart.setBackgroundVisible(False)
        self._chart.setTitle("1日实际波动率：预测与实际复核（同期限，年化）")
        self._view = QChartView(self._chart)
        self._view.setRenderHint(QPainter.RenderHint.Antialiasing)
        self._view.setMinimumHeight(310)
        self._evaluation = QLabel()
        self._evaluation.setWordWrap(True)
        self._evaluation.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._warning = QLabel("预测实际波动，不预测涨跌；不提供期权买卖指令。")
        self._warning.setWordWrap(True)
        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(("目标日（北京时间）", "预测1日RV（年化%）", "实际1日RV（年化%）", "误差（百分点）", "EWMA基准（年化%）"))
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.setMinimumHeight(150)
        layout = QVBoxLayout(self)
        layout.addLayout(toolbar)
        layout.addWidget(self._status)
        layout.addWidget(self._cards)
        layout.addWidget(self._view, 1)
        layout.addWidget(self._evaluation)
        layout.addWidget(self._warning)
        layout.addWidget(self._table)
        self.setStyleSheet("QLabel#VolatilityCards { background: #eef6ff; border: 1px solid #b9d8f5; border-radius: 5px; padding: 12px; font-size: 15px; }")
        self.refresh_prediction()

    def refresh_prediction(self) -> None:
        if self._worker is not None:
            return
        self._refresh.setEnabled(False)
        self._status.setText("正在读取本地已完成 BTC 小时线和 DVOL，计算冻结模型…")
        worker = _PredictionWorker(self)
        worker.ready.connect(self._render)
        worker.failed.connect(self._show_error)
        worker.finished.connect(self._finished)
        self._worker = worker
        worker.start()

    def _finished(self) -> None:
        if self._worker is not None:
            self._worker.deleteLater()
        self._worker = None
        self._refresh.setEnabled(True)

    def _show_error(self, message: str) -> None:
        self._snapshot = None
        self._status.setText(f"停止预测：{message}")
        self._status.setStyleSheet("color: #b45309;")
        self._cards.clear()
        self._evaluation.clear()
        self._table.setRowCount(0)
        self._chart.removeAllSeries()
        self._warning.setText("请先补齐本地数据；失败时不保留旧预测，以免误认为最新结果。")

    def _render(self, snapshot: dict) -> None:
        self._snapshot = snapshot
        self._status.setStyleSheet("" if snapshot["is_current"] else "color: #b45309;")
        self._status.setText(
            f"{snapshot['status']} | 数据 {snapshot['data_start']}～{snapshot['data_end']}（北京时间完整日）"
            f" | {snapshot['complete_daily_samples']} 日 | {snapshot['version']}"
        )
        rows = []
        for forecast in snapshot["forecasts"]:
            target = forecast["target_start"] if forecast["horizon_days"] == 1 else f"{forecast['target_start']}～{forecast['target_end']}"
            rows.append(
                f"<b>未来{forecast['horizon_days']}日 · {forecast['role']}：{forecast['annualized_rv_percent']:.2f}% 年化RV</b>"
                f"　| 波动尺度 {forecast['horizon_move_scale_percent']:.2f}%　| 目标 {target}"
            )
        rows.append(
            f"最新完成日实际RV {snapshot['latest_realized_1d_percent']:.2f}%（1日年化）"
            f"　| DVOL {snapshot['current_dvol_30d_percent']:.2f}%（30日隐含，仅作背景，不作跨期限价差）"
        )
        self._cards.setText("<br>".join(rows))
        evidence = []
        for forecast in snapshot["forecasts"]:
            e = forecast["evaluation"]
            evidence.append(
                f"{forecast['horizon_days']}日：{e['samples']}个样本 | QLIKE {e['qlike']:.3f} / EWMA {e['baseline_qlike']:.3f}"
                f" | 平均绝对误差 {e['mae_vol_points']:.2f} / 基准 {e['baseline_mae_vol_points']:.2f} 个年化百分点"
                f" | 相对评分 {e['score']:.2f}"
            )
        self._evaluation.setText(
            "冻结版本验证：拟合2021～2023 → 2024选参 → 参数截止2024-12-31 → 2025～2026-09-29历史测试\n"
            + "\n".join(evidence) + "\n评分50代表等于基准，不是命中率；7日模型仅作辅助。下表为冻结参数历史复核。"
        )
        self._warning.setText(snapshot["limitations"])
        self._table.setRowCount(0)
        for row in reversed(snapshot["history"][-20:]):
            actual = row["actual_rv_percent"]
            values = (
                row["target"], f"{row['predicted_rv_percent']:.2f}%",
                "待完成" if actual is None else f"{actual:.2f}%",
                "—" if actual is None else f"{abs(actual - row['predicted_rv_percent']):.2f}",
                f"{row['baseline_rv_percent']:.2f}%",
            )
            index = self._table.rowCount()
            self._table.insertRow(index)
            for column, value in enumerate(values):
                self._table.setItem(index, column, QTableWidgetItem(value))
        self._table.resizeColumnsToContents()
        self._render_chart(snapshot["history"])

    def _render_chart(self, history: list[dict]) -> None:
        self._chart.removeAllSeries()
        for axis in list(self._chart.axes()):
            self._chart.removeAxis(axis)
        if not history:
            return
        actual = QLineSeries()
        actual.setName("实际1日RV")
        actual.setPen(QPen(QColor("#64748b"), 2))
        predicted = QLineSeries()
        predicted.setName("冻结模型1日RV预测")
        predicted.setPen(QPen(QColor("#f59e0b"), 2))
        future = QLineSeries()
        future.setName("下一日预测（待验证）")
        pen = QPen(QColor("#ea580c"), 3)
        pen.setStyle(Qt.PenStyle.DashLine)
        future.setPen(pen)
        future.setPointsVisible(True)
        values = []
        for index, row in enumerate(history):
            x = _timestamp(row["target"])
            values.append(row["predicted_rv_percent"])
            if row["actual_rv_percent"] is not None:
                actual.append(x, row["actual_rv_percent"])
                predicted.append(x, row["predicted_rv_percent"])
                values.append(row["actual_rv_percent"])
            else:
                if index:
                    previous = history[index - 1]
                    future.append(_timestamp(previous["target"]), previous["predicted_rv_percent"])
                future.append(x, row["predicted_rv_percent"])
        for series in (actual, predicted, future):
            self._chart.addSeries(series)
        x_axis = QDateTimeAxis()
        x_axis.setFormat("MM-dd")
        x_axis.setTickCount(7)
        x_axis.setTitleText("目标日（北京时间；橙色虚线为下一日预测）")
        tz = QTimeZone(b"Asia/Shanghai")
        x_axis.setRange(QDateTime.fromMSecsSinceEpoch(_timestamp(history[0]["target"]), tz), QDateTime.fromMSecsSinceEpoch(_timestamp(history[-1]["target"]) + 86_400_000, tz))
        y_axis = QValueAxis()
        y_axis.setRange(0, max(max(values) * 1.12, 10))
        y_axis.setLabelFormat("%.0f%%")
        y_axis.setTitleText("1日RV（年化%）")
        self._chart.addAxis(x_axis, Qt.AlignmentFlag.AlignBottom)
        self._chart.addAxis(y_axis, Qt.AlignmentFlag.AlignLeft)
        for series in self._chart.series():
            series.attachAxis(x_axis)
            series.attachAxis(y_axis)

    def can_close(self) -> bool:
        # Avoid destroying a running QThread if the user closes during refresh.
        return self._worker is None or self._worker.wait(2000)
