from __future__ import annotations

from datetime import datetime, timedelta, timezone

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QApplication,
)

from okx_quant.shape_signal_store import (
    SUPPORTED_PATTERNS,
    SUPPORTED_PERIODS,
    load_events,
    save_subscriptions,
)


PATTERN_LABELS = {
    "big_bullish": "大阳线", "big_bearish": "大阴线", "long_upper_shadow": "长上影",
    "long_lower_shadow": "长下影", "double_reversal_up": "双线向上反转", "double_reversal_down": "双线向下反转",
    "inside_bar": "孕线", "top_fractal": "顶分型", "bottom_fractal": "底分型",
}
SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP", "OKB-USDT-SWAP", "ETH-BTC", "SOL-BTC")


class ShapeSignalHistoryDialog(QDialog):
    events_viewed = Signal(object)

    def done(self, result: int) -> None:
        for preview in list(self._previews):
            preview.close()
        if self._previews:
            from PySide6.QtCore import QTimer
            QTimer.singleShot(50, lambda: self.done(result))
            return
        super().done(result)

    def __init__(self, *, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("历史形态信号")
        self.setMinimumSize(1200, 700)
        screen = QApplication.primaryScreen()
        if screen is not None:
            available = screen.availableGeometry()
            self.resize(min(1800, max(1200, int(available.width() * 0.9))), min(1000, max(700, int(available.height() * 0.9))))
        else:
            self.resize(1600, 900)
        layout = QVBoxLayout(self)
        self._hint = QLabel("读取本地历史信号")
        layout.addWidget(self._hint)
        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("品种"))
        self._symbol_filter = QComboBox()
        self._symbol_filter.addItem("全部品种", "")
        self._symbol_filter.currentIndexChanged.connect(self.refresh)
        filter_row.addWidget(self._symbol_filter)
        filter_row.addWidget(QLabel("周期"))
        self._period_filter = QComboBox()
        self._period_filter.addItem("全部周期", "")
        self._period_filter.currentIndexChanged.connect(self.refresh)
        filter_row.addWidget(self._period_filter)
        filter_row.addWidget(QLabel("方向"))
        self._direction_filter = QComboBox()
        self._direction_filter.addItem("全部方向", "")
        self._direction_filter.addItem("多头", "long")
        self._direction_filter.addItem("空头", "short")
        self._direction_filter.addItem("中性", "neutral")
        self._direction_filter.currentIndexChanged.connect(self.refresh)
        filter_row.addWidget(self._direction_filter)
        filter_row.addStretch(1)
        layout.addLayout(filter_row)
        self._table = QTableWidget(0, 9, self)
        self._table.setHorizontalHeaderLabels(("时间", "品种", "周期", "方向", "形态", "收盘价", "评分", "来源", "说明"))
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        header = self._table.horizontalHeader()
        for column in range(8):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.Stretch)
        self._table.setWordWrap(False)
        self._table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._events_by_row: list[dict[str, object]] = []
        self._previews: list[QDialog] = []
        layout.addWidget(self._table, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        preview = buttons.addButton("打开K线预览", QDialogButtonBox.ButtonRole.ActionRole)
        preview.clicked.connect(self._open_selected_preview)
        refresh = buttons.addButton("刷新", QDialogButtonBox.ButtonRole.ActionRole)
        refresh.clicked.connect(self.refresh)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.refresh()

    def refresh(self) -> None:
        all_events = load_events(limit=5000)
        selected_symbol = self._symbol_filter.currentData()
        symbols = sorted({str(event.get("symbol") or "").strip() for event in all_events if str(event.get("symbol") or "").strip()})
        current_values = [self._symbol_filter.itemData(index) for index in range(self._symbol_filter.count())]
        if current_values[1:] != symbols:
            self._symbol_filter.blockSignals(True)
            self._symbol_filter.clear()
            self._symbol_filter.addItem("全部品种", "")
            for symbol in symbols:
                self._symbol_filter.addItem(symbol, symbol)
            restore_index = self._symbol_filter.findData(selected_symbol)
            self._symbol_filter.setCurrentIndex(restore_index if restore_index >= 0 else 0)
            self._symbol_filter.blockSignals(False)
        self._sync_filter_options(
            self._period_filter,
            [(period, period) for period in SUPPORTED_PERIODS if any(str(event.get("period") or "").upper() == period for event in all_events)],
            selected=self._period_filter.currentData(),
            all_label="全部周期",
        )
        self._sync_filter_options(
            self._direction_filter,
            [("多头", "long"), ("空头", "short"), ("中性", "neutral")],
            selected=self._direction_filter.currentData(),
            all_label="全部方向",
        )
        selected_symbol = self._symbol_filter.currentData()
        selected_period = self._period_filter.currentData()
        selected_direction = self._direction_filter.currentData()
        events = [
            event for event in all_events
            if (not selected_symbol or str(event.get("symbol") or "") == str(selected_symbol))
            and (not selected_period or str(event.get("period") or "").upper() == str(selected_period).upper())
            and (not selected_direction or str(event.get("direction") or "").lower() == str(selected_direction).lower())
        ]
        self._events_by_row = [dict(event) for event in events]
        self._table.setRowCount(0)
        for event in events:
            row = self._table.rowCount()
            self._table.insertRow(row)
            ts = int(event.get("candle_ts", 0) or 0)
            when = datetime.fromtimestamp(ts / 1000, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S") if ts else "-"
            values = (when, event.get("symbol", "-"), event.get("period", "-"), event.get("direction", "-"), event.get("pattern_name", "-"), event.get("close", "-"), event.get("score", "-"), event.get("source", "-"), event.get("marker_text", "-"))
            for column, value in enumerate(values):
                self._table.setItem(row, column, QTableWidgetItem(str(value)))
        filter_text = str(selected_symbol) if selected_symbol else "全部品种"
        period_text = str(selected_period) if selected_period else "全部周期"
        direction_labels = {"long": "多头", "short": "空头", "neutral": "中性"}
        direction_text = direction_labels.get(str(selected_direction), "全部方向") if selected_direction else "全部方向"
        self._hint.setText(f"当前筛选：{filter_text} / {period_text} / {direction_text}；显示 {len(events)} / {len(all_events)} 条记录。实时信号和启动补算都会保存在这里。")
        self.events_viewed.emit(self._events_by_row)

    def _selected_event(self) -> dict[str, object] | None:
        row = self._table.currentRow()
        if row < 0 or row >= len(self._events_by_row):
            return None
        return dict(self._events_by_row[row])

    def _on_cell_double_clicked(self, row: int, _column: int) -> None:
        if 0 <= row < self._table.rowCount():
            self._table.selectRow(row)
        self._open_selected_preview()

    def _open_selected_preview(self) -> None:
        event = self._selected_event()
        if event is None:
            QMessageBox.information(self, "K线预览", "请先选择一条形态信号。")
            return
        try:
            candle_ts = int(event.get("candle_ts", 0) or 0)
        except (TypeError, ValueError):
            candle_ts = 0
        symbol = str(event.get("symbol") or "").strip().upper()
        period = str(event.get("period") or "").strip().upper()
        if not symbol or period not in SUPPORTED_PERIODS or candle_ts <= 0:
            QMessageBox.information(self, "K线预览", "这条信号缺少有效的品种、周期或时间，无法打开K线。")
            return
        preview = ShapeSignalPreviewDialog(event=event, main_window=self.parentWidget(), parent=self)
        preview.destroyed.connect(lambda *_args, target=preview: self._previews.remove(target) if target in self._previews else None)
        self._previews.append(preview)
        preview.show()
        preview.raise_()
        preview.activateWindow()

    @staticmethod
    def _sync_filter_options(combo: QComboBox, options: list[tuple[str, str]], *, selected: object, all_label: str) -> None:
        values = [combo.itemData(index) for index in range(combo.count())]
        next_values = [""] + [value for _label, value in options]
        if values == next_values:
            return
        combo.blockSignals(True)
        combo.clear()
        combo.addItem(all_label, "")
        for label, value in options:
            combo.addItem(label, value)
        restore_index = combo.findData(selected)
        combo.setCurrentIndex(restore_index if restore_index >= 0 else 0)
        combo.blockSignals(False)


class ShapeSignalPreviewDialog(QDialog):
    def __init__(self, *, event: dict[str, object], main_window: QWidget | None, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow

        self._main_window = main_window
        self._close_ready = False
        self._close_pending = False
        self._kline = KlineAnalysisWindow(embedded=True, preview_mode=True)
        self.setWindowTitle(
            f"形态信号K线预览 · {event.get('symbol', '-')} · {event.get('period', '-')}"
        )
        self.resize(1680, 980)
        self.setMinimumSize(1280, 760)
        self.setWindowModality(Qt.WindowModality.NonModal)
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)

        layout = QVBoxLayout(self)
        context = QLabel(
            f"{event.get('symbol', '-')}  |  {event.get('period', '-')}  |  "
            f"{event.get('pattern_name', '-')}  |  {event.get('direction', '-')}  |  "
            f"评分 {event.get('score', '-')}  |  {event.get('marker_text', '')}"
        )
        context.setWordWrap(True)
        layout.addWidget(context)
        layout.addWidget(self._kline, 1)

        buttons = QDialogButtonBox()
        open_main = buttons.addButton("在主K线中打开", QDialogButtonBox.ButtonRole.ActionRole)
        close_button = buttons.addButton("关闭", QDialogButtonBox.ButtonRole.RejectRole)
        open_main.clicked.connect(self._open_in_main_kline)
        close_button.clicked.connect(self.close)
        layout.addWidget(buttons)

        self._kline.configure_shape_signal_preview(
            symbol=str(event.get("symbol") or ""),
            period=str(event.get("period") or "1H"),
            candle_ts=int(event.get("candle_ts", 0) or 0),
            pattern_name=str(event.get("pattern_name") or ""),
            direction=str(event.get("direction") or ""),
            score=event.get("score", ""),
            marker_text=str(event.get("marker_text") or ""),
        )

    def _open_in_main_kline(self) -> None:
        target = self._main_window
        if target is None:
            self.close()
            return
        owner: QWidget | None = target
        while owner is not None:
            show_page = getattr(owner, "show_page", None)
            if callable(show_page):
                try:
                    show_page("kline")
                except Exception:
                    pass
                break
            owner = owner.parentWidget()
        opener = getattr(target, "show_shape_signal_context", None)
        if callable(opener):
            try:
                opener(
                    symbol=str(self._kline._selected_symbol()),
                    period=str(self._kline._period_combo.currentText()),
                    candle_ts=int(getattr(self._kline, "_preview_signal_ts", 0) or 0),
                )
            except Exception as exc:  # noqa: BLE001
                QMessageBox.warning(self, "打开主K线", f"无法切换主K线：{exc}")
                return
        self.close()

    def done(self, result: int) -> None:
        if self._close_ready:
            super().done(result)
            return
        if self._close_pending:
            return
        self._close_pending = True
        self.setEnabled(False)
        self._kline.begin_shutdown(lambda: self._finish_close(result))

    def _finish_close(self, result: int) -> None:
        self._close_ready = True
        self.done(result)

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if not self._close_ready:
            event.ignore()
            self.done(QDialog.DialogCode.Rejected)
            return
        super().closeEvent(event)


class ShapeSignalSubscriptionDialog(QDialog):
    def __init__(self, *, monitor: object, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._monitor = monitor
        self.setWindowTitle("后台形态订阅")
        self.resize(1080, 680)
        self._subscriptions = list(getattr(monitor, "subscriptions")())
        root = QVBoxLayout(self)

        self._table = QTableWidget(0, 8, self)
        self._table.setHorizontalHeaderLabels(("状态", "品种", "环境", "周期", "形态数量", "弹窗", "邮件", "订阅ID"))
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self._table, 1)

        form_box = QGroupBox("新增订阅")
        form = QFormLayout(form_box)
        self._symbol = QComboBox()
        self._symbol.addItems(SYMBOLS)
        form.addRow("品种", self._symbol)
        self._environment = QComboBox()
        self._environment.addItem("模拟盘 demo", "demo")
        self._environment.addItem("实盘 live", "live")
        form.addRow("行情环境", self._environment)
        period_row = QHBoxLayout()
        self._period_checks: dict[str, QCheckBox] = {}
        for period in SUPPORTED_PERIODS:
            check = QCheckBox(period)
            check.setChecked(True)
            self._period_checks[period] = check
            period_row.addWidget(check)
        form.addRow("周期", period_row)
        pattern_widget = QWidget()
        pattern_grid = QGridLayout(pattern_widget)
        pattern_grid.setContentsMargins(0, 0, 0, 0)
        self._pattern_checks: dict[str, QCheckBox] = {}
        for index, pattern in enumerate(SUPPORTED_PATTERNS):
            check = QCheckBox(PATTERN_LABELS.get(pattern, pattern))
            check.setChecked(True)
            self._pattern_checks[pattern] = check
            pattern_grid.addWidget(check, index // 3, index % 3)
        form.addRow("形态", pattern_widget)
        options_row = QHBoxLayout()
        self._metric = QComboBox()
        self._metric.addItem("实体排名", "body")
        self._metric.addItem("振幅排名", "range")
        self._top_n = QSpinBox()
        self._top_n.setRange(1, 10)
        self._top_n.setValue(4)
        self._ma_touch = QCheckBox("必须碰均线")
        self._popup = QCheckBox("弹窗")
        self._popup.setChecked(True)
        self._email = QCheckBox("邮件")
        options_row.addWidget(self._metric)
        options_row.addWidget(QLabel("前"))
        options_row.addWidget(self._top_n)
        options_row.addWidget(QLabel("名"))
        options_row.addWidget(self._ma_touch)
        options_row.addWidget(self._popup)
        options_row.addWidget(self._email)
        options_row.addStretch(1)
        form.addRow("过滤", options_row)
        root.addWidget(form_box)

        action_row = QHBoxLayout()
        add = QPushButton("添加订阅")
        add.clicked.connect(self._add)
        toggle = QPushButton("启用/暂停选中")
        toggle.clicked.connect(self._toggle_selected)
        remove = QPushButton("删除选中")
        remove.clicked.connect(self._remove_selected)
        history = QPushButton("打开历史信号")
        history.clicked.connect(self._open_history)
        action_row.addWidget(add)
        action_row.addWidget(toggle)
        action_row.addWidget(remove)
        action_row.addWidget(history)
        action_row.addStretch(1)
        root.addLayout(action_row)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        root.addWidget(buttons)
        self._refresh_table()

    def _refresh_table(self) -> None:
        self._table.setRowCount(0)
        for item in self._subscriptions:
            row = self._table.rowCount()
            self._table.insertRow(row)
            values = ("启用" if item.get("enabled", True) else "暂停", item.get("symbol", "-"), item.get("environment", "demo"), ",".join(item.get("periods", [])), len(item.get("patterns", [])), "是" if item.get("popup_enabled", True) else "否", "是" if item.get("email_enabled", False) else "否", item.get("id", "-"))
            for column, value in enumerate(values):
                self._table.setItem(row, column, QTableWidgetItem(str(value)))

    def _persist(self) -> None:
        save_subscriptions(self._subscriptions)
        reload_method = getattr(self._monitor, "reload_subscriptions", None)
        if callable(reload_method):
            reload_method(self._subscriptions)
        self._refresh_table()

    def _add(self) -> None:
        periods = [key for key, check in self._period_checks.items() if check.isChecked()]
        patterns = [key for key, check in self._pattern_checks.items() if check.isChecked()]
        if not periods or not patterns:
            QMessageBox.information(self, "添加订阅", "至少选择一个周期和一个形态。")
            return
        self._subscriptions.append({
            "symbol": self._symbol.currentText(),
            "environment": self._environment.currentData(),
            "periods": periods,
            "patterns": patterns,
            "metric": self._metric.currentData(),
            "top_n": self._top_n.value(),
            "require_ma_touch": self._ma_touch.isChecked(),
            "popup_enabled": self._popup.isChecked(),
            "email_enabled": self._email.isChecked(),
            "enabled": True,
        })
        self._persist()

    def _selected_index(self) -> int | None:
        rows = self._table.selectionModel().selectedRows()
        if not rows:
            return None
        index = rows[0].row()
        return index if 0 <= index < len(self._subscriptions) else None

    def _toggle_selected(self) -> None:
        index = self._selected_index()
        if index is None:
            return
        self._subscriptions[index]["enabled"] = not bool(self._subscriptions[index].get("enabled", True))
        self._persist()

    def _remove_selected(self) -> None:
        index = self._selected_index()
        if index is None:
            return
        self._subscriptions.pop(index)
        self._persist()

    def _open_history(self) -> None:
        dialog = ShapeSignalHistoryDialog(parent=self)
        owner = self.parentWidget()
        while owner is not None:
            acknowledge = getattr(owner, "_mark_shape_messages_viewed", None)
            if callable(acknowledge):
                dialog.events_viewed.connect(acknowledge)
                dialog.refresh()
                break
            owner = owner.parentWidget()
        dialog.exec()
