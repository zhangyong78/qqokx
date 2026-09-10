from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from PySide6.QtCore import QDate, Qt, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QDateEdit,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from okx_quant.daily_trade_report import (
    DailyTrade,
    DailyTradeReport,
    REPORT_TIMEZONE,
    build_daily_trade_report,
    daily_trade_from_position_history,
    format_report_decimal,
    format_report_price,
    report_to_csv,
    report_to_html,
)
from okx_quant.persistence import list_history_cache_scopes
from roll_terminal_qt.history_service import load_local_position_history
from roll_terminal_qt.account_positions_home import InstrumentKlineDialog


class DailyTradeReportWidget(QWidget):
    """Local transaction summary backed by the positions page history cache."""

    def __init__(self, parent: QWidget | None = None, *, profile_name: str = "") -> None:
        super().__init__(parent)
        self._report: DailyTradeReport | None = None
        self._stopping = False
        self._profile_name = str(profile_name or "").strip()
        self._history_scopes: list[tuple[str, str]] = []
        self._trade_kline_window: InstrumentKlineDialog | None = None
        self._build_ui()
        self._refresh_profiles()
        self.refresh_report()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("数据源：持仓历史本地记录"))
        self._profile_label = QLabel()
        controls.addWidget(self._profile_label)
        controls.addWidget(QLabel("币种"))
        self._asset_combo = QComboBox()
        self._asset_combo.setMinimumWidth(110)
        self._asset_combo.currentTextChanged.connect(lambda _text: self.refresh_report())
        controls.addWidget(self._asset_combo)
        controls.addWidget(QLabel("开始日期"))
        self._start_date = QDateEdit(QDate.currentDate().addDays(-6))
        self._start_date.setCalendarPopup(True)
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        controls.addWidget(self._start_date)
        controls.addWidget(QLabel("结束日期"))
        self._end_date = QDateEdit(QDate.currentDate())
        self._end_date.setCalendarPopup(True)
        self._end_date.setDisplayFormat("yyyy-MM-dd")
        controls.addWidget(self._end_date)
        refresh = QPushButton("刷新")
        refresh.clicked.connect(self.refresh_report)
        controls.addWidget(refresh)
        export_csv = QPushButton("导出 CSV")
        export_csv.clicked.connect(lambda: self._export("csv"))
        controls.addWidget(export_csv)
        export_html = QPushButton("导出 HTML")
        export_html.clicked.connect(lambda: self._export("html"))
        controls.addWidget(export_html)
        controls.addStretch(1)
        layout.addLayout(controls)

        self._status = QLabel("准备读取 OKX 历史仓位。")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)
        layout.addWidget(self._status)

        self._tabs = QTabWidget()
        self._daily_table = self._table(("日期", "API", "开仓", "平仓", "盈利", "亏损", "净盈亏", "未平仓"))
        self._symbol_table = self._table(("日期", "API", "品种", "平仓", "盈利", "亏损", "净盈亏"))
        self._strategy_table = self._table(("日期", "API", "策略", "平仓", "盈利", "亏损", "净盈亏"))
        self._detail_table = self._table(("平仓时间", "API", "品种", "策略", "会话", "方向", "开仓时间", "开仓价", "平仓价", "数量", "净盈亏", "状态", "来源", "原因"))
        self._detail_table.cellDoubleClicked.connect(self._open_trade_kline)
        self._tabs.addTab(self._daily_table, "每日汇总")
        self._tabs.addTab(self._symbol_table, "品种汇总")
        self._tabs.addTab(self._strategy_table, "策略汇总")
        self._tabs.addTab(self._detail_table, "交易明细")
        layout.addWidget(self._tabs, 1)

    @staticmethod
    def _table(headers: tuple[str, ...]) -> QTableWidget:
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setAlternatingRowColors(True)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setStretchLastSection(True)
        return table

    def _refresh_profiles(self) -> None:
        self._history_scopes = list_history_cache_scopes()

    def _refresh_asset_options(self, trades: list[DailyTrade]) -> None:
        current = self._asset_combo.currentText().strip() if self._asset_combo.count() else "全部币种"
        assets = sorted(
            {
                str(trade.symbol or "").strip().upper().split("-", 1)[0]
                for trade in trades
                if str(trade.symbol or "").strip()
            }
        )
        self._asset_combo.blockSignals(True)
        self._asset_combo.clear()
        self._asset_combo.addItem("全部币种", "全部币种")
        for asset in assets:
            self._asset_combo.addItem(asset, asset)
        index = self._asset_combo.findText(current)
        self._asset_combo.setCurrentIndex(index if index >= 0 else 0)
        self._asset_combo.blockSignals(False)

    def _runtime_profile_name(self) -> str:
        return self._profile_name or "未选择API"

    def workspace_profile_name(self) -> str:
        return self._runtime_profile_name()

    def apply_workspace_profile(self, profile_name: str) -> None:
        target = str(profile_name or "").strip()
        if not target or target == self._profile_name:
            return
        self._profile_name = target
        self.refresh_report()

    def set_workspace_managed(self, _managed: bool) -> None:
        return

    def _selected_dates(self) -> tuple[date, date]:
        start = self._start_date.date().toPython()
        end = self._end_date.date().toPython()
        return (start, end) if start <= end else (end, start)

    @Slot()
    def refresh_report(self) -> None:
        if self._stopping:
            return
        start_date, end_date = self._selected_dates()
        trades: list[DailyTrade] = []
        self._refresh_profiles()
        scopes = [
            (profile_name, environment)
            for profile_name, environment in self._history_scopes
            if not self._profile_name or profile_name.casefold() == self._profile_name.casefold()
        ]
        for profile_name, environment in scopes:
            items = load_local_position_history(profile_name, environment, limit=5000)
            trades.extend(
                daily_trade_from_position_history(
                    item,
                    api_name=profile_name,
                    environment=environment,
                )
                for item in items
            )
        self._refresh_asset_options(trades)
        self._report = build_daily_trade_report(
            trades,
            start_date=start_date,
            end_date=end_date,
            api_name=self._profile_name or "全部API",
            asset_filter=str(self._asset_combo.currentData() or "全部币种"),
        )
        self._render_report(self._report)

    def _render_report(self, report: DailyTradeReport) -> None:
        self._daily_table.setRowCount(len(report.daily))
        for row, item in enumerate(report.daily):
            self._set_row(self._daily_table, row, (
                item.report_date.isoformat(), item.api_name, str(item.opened_count), str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_decimal(item.net_pnl, signed=True), str(item.open_count),
            ))
        self._symbol_table.setRowCount(len(report.by_symbol))
        for row, item in enumerate(report.by_symbol):
            self._set_row(self._symbol_table, row, (
                item.report_date.isoformat(), item.api_name, item.group_name, str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_decimal(item.net_pnl, signed=True),
            ))
        self._strategy_table.setRowCount(len(report.by_strategy))
        for row, item in enumerate(report.by_strategy):
            self._set_row(self._strategy_table, row, (
                item.report_date.isoformat(), item.api_name, item.group_name, str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_decimal(item.net_pnl, signed=True),
            ))
        self._detail_table.setRowCount(len(report.trades))
        for row, trade in enumerate(report.trades):
            self._set_row(self._detail_table, row, (
                trade.closed_at.strftime("%Y-%m-%d %H:%M") if trade.closed_at else "-",
                trade.api_name, trade.symbol, trade.strategy_name, trade.session_id, trade.direction,
                trade.opened_at.strftime("%Y-%m-%d %H:%M") if trade.opened_at else "-",
                format_report_price(trade.entry_price, trade.symbol),
                format_report_price(trade.exit_price, trade.symbol),
                str(trade.size or "-"),
                format_report_decimal(trade.net_pnl, signed=True), trade.status, trade.source, trade.close_reason,
            ))
            symbol_item = self._detail_table.item(row, 2)
            if symbol_item is not None:
                symbol_item.setData(Qt.ItemDataRole.UserRole, trade)
        net = sum((item.net_pnl for item in report.daily), start=Decimal("0")) if report.daily else Decimal("0")
        profile_text = self._runtime_profile_name()
        self._profile_label.setText(f"当前 API：{profile_text}")
        self._status.setText(f"{profile_text} | {report.start_date} 至 {report.end_date} | 交易记录 {len(report.trades)} 条 | 已结算净盈亏 {format_report_decimal(net, signed=True)}")

    @staticmethod
    def _set_row(table: QTableWidget, row: int, values: tuple[str, ...]) -> None:
        for column, value in enumerate(values):
            item = QTableWidgetItem(value)
            item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter if column >= 3 else Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(row, column, item)

    def _clear_tables(self) -> None:
        for table in (self._daily_table, self._symbol_table, self._strategy_table, self._detail_table):
            table.setRowCount(0)

    def _open_trade_kline(self, row: int, column: int) -> None:
        """Open the clicked transaction's contract in the K-line analysis window."""
        if column != 2:
            return
        item = self._detail_table.item(row, column)
        trade = item.data(Qt.ItemDataRole.UserRole) if item is not None else None
        symbol = str(getattr(trade, "symbol", "") or (item.text() if item is not None else "")).strip().upper()
        if not symbol or symbol == "-":
            return
        if self._trade_kline_window is None:
            self._trade_kline_window = InstrumentKlineDialog(initial_bar="1H", parent=self)
            self._trade_kline_window.destroyed.connect(lambda: setattr(self, "_trade_kline_window", None))
        markers = []
        for label, timestamp in (("开仓", trade.opened_at), ("平仓", trade.closed_at)):
            if timestamp is not None:
                markers.append((label, int(timestamp.timestamp())))
        self._trade_kline_window.show_instrument(
            inst_id=symbol,
            inst_type="OPTION" if symbol.count("-") >= 3 else "SWAP",
            time_markers=tuple(markers),
        )

    def _export(self, kind: str) -> None:
        if self._report is None:
            QMessageBox.information(self, "暂无数据", "请先刷新日报。")
            return
        suffix = ".csv" if kind == "csv" else ".html"
        path, _ = QFileDialog.getSaveFileName(self, "导出交易汇总", f"trade_summary{suffix}", f"*{suffix}")
        if not path:
            return
        target = Path(path)
        target.write_text(report_to_csv(self._report) if kind == "csv" else report_to_html(self._report), encoding="utf-8-sig" if kind == "csv" else "utf-8")
        self._status.setText(f"已导出：{target}")

    def begin_shutdown(self, callback=None) -> None:  # noqa: ANN001
        self._stopping = True
        if callable(callback):
            callback()
