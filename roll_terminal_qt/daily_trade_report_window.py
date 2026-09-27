from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from PySide6.QtCore import QDate, QSignalBlocker, QThreadPool, QTimer, Qt, Slot
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
    build_daily_trade_report,
    format_report_pnl_with_r,
    format_report_price,
    report_to_csv,
    report_to_html,
    REPORT_TIMEZONE,
    _decimal,
)
from okx_quant.trade_analytics import instrument_type, completed_trades
from roll_terminal_qt.runtime import load_runtime
from roll_terminal_qt.realtime_account_store import get_shared_realtime_account_store
from roll_terminal_qt.history_sync_manager import get_history_sync_manager
from roll_terminal_qt.trade_report_service import ReportLoadTask
from roll_terminal_qt.trade_analytics_widgets import ANALYTICS_STYLE, AnalysisDashboard
from roll_terminal_qt.account_positions_home import (
    InstrumentKlineDialog,
    PositionPriceMarker,
    _position_history_kline_price_markers,
    _position_history_kline_time_markers,
)


class DailyTradeReportWidget(QWidget):
    """Local transaction summary backed by the positions page history cache."""

    def __init__(self, parent: QWidget | None = None, *, profile_name: str = "") -> None:
        super().__init__(parent)
        self._report: DailyTradeReport | None = None
        self._stopping = False
        self._profile_name = str(profile_name or "").strip()
        self._history_scopes: list[tuple[str, str]] = []
        self._environment = "live"
        self._generation = 0
        self._load_running = False
        self._reload_pending = False
        self._all_trades: list[DailyTrade] = []
        self._history_items = []
        self._equity_records = []
        self._fills = []
        self._bills = []
        self._asset_bills = []
        self._snapshot = None
        self._prices = {}
        self._detail_day = None
        self._runtime = None
        self._trade_kline_window: InstrumentKlineDialog | None = None
        self._realtime_store = get_shared_realtime_account_store()
        self._history_manager = get_history_sync_manager()
        self._realtime_store.snapshot_ready.connect(self._apply_account_snapshot)
        self._history_manager.sync_finished.connect(self._on_history_synced)
        self._reload_timer = QTimer(self)
        self._reload_timer.setSingleShot(True)
        self._reload_timer.timeout.connect(self._start_load)
        self._local_timer = QTimer(self)
        self._local_timer.setInterval(60000)
        self._local_timer.timeout.connect(self.refresh_report)
        self._local_timer.start()
        self._account_timer = QTimer(self)
        self._account_timer.setSingleShot(True)
        self._account_timer.setInterval(1000)
        self._account_timer.timeout.connect(self._render_account)
        self._build_ui()
        self._bind_profile()
        self.refresh_report()

    def _build_ui(self) -> None:
        self.setObjectName("TradeReport")
        self.setStyleSheet(ANALYTICS_STYLE)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        controls = QHBoxLayout()
        self._profile_label = QLabel()
        controls.addWidget(self._profile_label)
        controls.addWidget(QLabel("币种"))
        self._asset_combo = QComboBox()
        self._asset_combo.setMinimumWidth(110)
        self._asset_combo.currentTextChanged.connect(self._filter_changed)
        controls.addWidget(self._asset_combo)
        controls.addWidget(QLabel("开始日期"))
        today = datetime.now(REPORT_TIMEZONE).date()
        self._start_date = QDateEdit(QDate(today.year, today.month, today.day).addDays(-29))
        self._start_date.setCalendarPopup(True)
        self._start_date.setDisplayFormat("yyyy-MM-dd")
        controls.addWidget(self._start_date)
        controls.addWidget(QLabel("结束日期"))
        self._end_date = QDateEdit(QDate(today.year, today.month, today.day))
        self._end_date.setCalendarPopup(True)
        self._end_date.setDisplayFormat("yyyy-MM-dd")
        controls.addWidget(self._end_date)
        self._start_date.dateChanged.connect(self._filter_changed)
        self._end_date.dateChanged.connect(self._filter_changed)
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
        shortcuts = QHBoxLayout()
        shortcuts.addWidget(QLabel("快捷范围"))
        for text, days in (("近 7 天", 7), ("近 30 天", 30), ("近 90 天", 90), ("本月", 0)):
            button = QPushButton(text)
            button.clicked.connect(lambda _checked=False, days=days: self._set_range(days))
            shortcuts.addWidget(button)
        shortcuts.addStretch()
        scope_hint = QLabel("北京时间 · 历史仓位本地统计 · U = USDT")
        scope_hint.setProperty("muted", True)
        shortcuts.addWidget(scope_hint)
        layout.addLayout(shortcuts)

        self._status = QLabel("准备读取 OKX 历史仓位。")
        self._status.setObjectName("Subtle")
        self._status.setWordWrap(True)
        layout.addWidget(self._status)

        self._tabs = QTabWidget()
        self._overview = AnalysisDashboard()
        self._contracts = AnalysisDashboard(contracts=True)
        self._overview.day_activated.connect(self._show_day_details)
        self._tabs.addTab(self._overview, "分析概览")
        self._tabs.addTab(self._contracts, "合约分析")
        self._daily_table = self._table(("日期", "API", "开仓", "平仓", "盈利", "亏损", "净盈亏", "未平仓"))
        self._symbol_table = self._table(("日期", "API", "品种", "平仓", "盈利", "亏损", "净盈亏"))
        self._strategy_table = self._table(("日期", "API", "策略", "平仓", "盈利", "亏损", "净盈亏"))
        self._detail_table = self._table(("平仓时间", "API", "品种", "策略", "会话", "方向", "开仓时间", "开仓价", "平仓价", "数量", "净盈亏", "状态", "来源", "原因"))
        self._detail_table.cellDoubleClicked.connect(self._open_trade_kline)
        self._tabs.addTab(self._daily_table, "每日汇总")
        self._tabs.addTab(self._symbol_table, "品种汇总")
        self._tabs.addTab(self._strategy_table, "策略汇总")
        self._detail_page = QWidget()
        detail_layout = QVBoxLayout(self._detail_page)
        detail_header = QHBoxLayout()
        self._detail_hint = QLabel("当前日期区间的交易记录")
        detail_header.addWidget(self._detail_hint, 1)
        clear_day = QPushButton("显示整个区间")
        clear_day.clicked.connect(lambda: self._show_day_details(None))
        detail_header.addWidget(clear_day)
        detail_layout.addLayout(detail_header)
        detail_layout.addWidget(self._detail_table)
        self._tabs.addTab(self._detail_page, "交易明细")
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

    def _bind_profile(self) -> None:
        self._runtime = load_runtime(self._profile_name) if self._profile_name else None
        self._environment = str(getattr(self._runtime, "environment", "live"))
        self._history_scopes = [(self._profile_name, self._environment)] if self._profile_name else []
        self._snapshot = None
        self._prices = {}
        self._overview.set_account(None)
        if self._runtime is not None:
            self._realtime_store.start_if_needed(self._runtime)
            snapshot = self._realtime_store.snapshot_for(profile_name=self._profile_name, environment=self._environment)
            if snapshot is not None:
                self._apply_account_snapshot(snapshot)

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
        if not target:
            return
        runtime = load_runtime(target)
        if target == self._profile_name and str(getattr(runtime, "environment", "live")) == self._environment:
            return
        self._profile_name = target
        self._all_trades = []
        self._history_items = []
        self._equity_records = []
        self._fills = []
        self._bills = []
        self._asset_bills = []
        self._detail_day = None
        self._contracts.selected_day = None
        self._bind_profile()
        self._render_loaded_report()
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
        self._generation += 1
        self._status.setText(f"{self._runtime_profile_name()} · 正在后台读取本地记录…")
        self._reload_timer.start(120)

    def _start_load(self) -> None:
        if self._stopping:
            return
        if self._load_running:
            self._reload_pending = True
            return
        self._load_running = True
        self._reload_pending = False
        self._load_task = ReportLoadTask(self._generation, self._profile_name, self._environment, dict(self._prices))
        self._load_task.signals.finished.connect(self._on_loaded)
        QThreadPool.globalInstance().start(self._load_task)

    @Slot(int, object)
    def _on_loaded(self, generation, result):
        self._load_running = False
        if self._stopping:
            return
        if generation == self._generation:
            if isinstance(result, Exception):
                self._status.setText(f"本地记录读取失败：{result}；仍显示上次结果")
            else:
                self._all_trades = result["trades"]
                self._history_items = result["items"]
                self._equity_records = result["equity"]
                self._fills = result.get("fills", [])
                self._bills = result.get("bills", [])
                self._asset_bills = result.get("asset_bills", [])
                self._refresh_asset_options(self._all_trades)
                self._render_loaded_report()
        if self._reload_pending or generation != self._generation:
            self._reload_timer.start(0)

    def _filter_changed(self, *_args):
        self._detail_day = None
        self._contracts.selected_day = None
        self._render_loaded_report()

    def _set_range(self, days: int):
        now = datetime.now(REPORT_TIMEZONE).date()
        end = QDate(now.year, now.month, now.day)
        start = end.addDays(1 - days) if days else QDate(end.year(), end.month(), 1)
        with QSignalBlocker(self._start_date), QSignalBlocker(self._end_date):
            self._start_date.setDate(start)
            self._end_date.setDate(end)
        self._filter_changed()

    def _render_loaded_report(self) -> None:
        if self._stopping:
            return
        start_date, end_date = self._selected_dates()
        asset = str(self._asset_combo.currentData() or "全部币种")
        trades = [trade for trade in self._all_trades if asset == "全部币种" or trade.symbol.split("-", 1)[0] == asset]
        self._report = build_daily_trade_report(
            trades,
            start_date=start_date,
            end_date=end_date,
            api_name=self._profile_name or "全部API",
            asset_filter=asset,
        )
        self._render_report(self._report)
        self._overview.set_report(self._report, trades, self._equity_records)
        phase2_fills = self._fills
        phase2_bills = self._bills
        phase2_asset_bills = self._asset_bills
        if asset != "全部币种":
            phase2_fills = [item for item in self._fills if str(getattr(item, "inst_id", "")).upper().split("-", 1)[0] == asset]
            phase2_bills = [item for item in self._bills if str(getattr(item, "currency", "")).upper() == asset]
            phase2_asset_bills = [item for item in self._asset_bills if str(getattr(item, "currency", "")).upper() == asset]
        self._overview.set_phase2_data(phase2_fills, phase2_bills, phase2_asset_bills, self._prices)
        self._contracts.set_report(self._report, trades, self._equity_records)
        self._render_account()

    @Slot(object)
    def _apply_account_snapshot(self, snapshot):
        if self._stopping or (snapshot.profile_name, snapshot.environment) != (self._profile_name, self._environment):
            return
        self._snapshot = snapshot
        prices = dict(snapshot.upl_usdt_prices)
        usd_prices = {}
        for asset in getattr(snapshot.account, "details", ()):
            amount, usd = _decimal(getattr(asset, "equity", None)), _decimal(getattr(asset, "equity_usd", None))
            if amount is not None and amount > 0 and usd is not None and usd > 0:
                usd_prices[asset.ccy] = usd / amount
        usdt_usd = usd_prices.get("USDT", Decimal("1"))
        prices.update({ccy: value / usdt_usd for ccy, value in usd_prices.items()})
        prices["USD"] = 1 / usdt_usd
        new_currencies = set(prices) - set(self._prices)
        self._prices = prices
        if not self._account_timer.isActive():
            self._account_timer.start()
        if new_currencies:
            self.refresh_report()

    def _render_account(self):
        if self._snapshot is not None:
            self._overview.set_account(self._snapshot.account, updated_at=getattr(self._snapshot, "account_updated_at", None),
                                       asset_filter=str(self._asset_combo.currentData() or "全部币种"))

    @Slot(str, str, object)
    def _on_history_synced(self, profile_name, environment, _sources):
        if (profile_name, environment) == (self._profile_name, self._environment):
            self.refresh_report()

    def set_page_active(self, active: bool):
        if active:
            self._local_timer.start()
            self.refresh_report()
        else:
            self._local_timer.stop()

    def _show_day_details(self, day):
        self._detail_day = day
        self._apply_detail_day()
        self._tabs.setCurrentWidget(self._detail_page)

    def _apply_detail_day(self):
        self._detail_hint.setText(f"{self._detail_day} 已结算明细" if self._detail_day else "当前日期区间的交易记录")
        if self._report is None:
            return
        for row, trade in enumerate(self._report.trades):
            visible = self._detail_day is None or (trade.is_closed and trade.closed_at is not None and trade.closed_at.date() == self._detail_day)
            self._detail_table.setRowHidden(row, not visible)

    def _render_report(self, report: DailyTradeReport) -> None:
        self._daily_table.setRowCount(len(report.daily))
        for row, item in enumerate(report.daily):
            self._set_row(self._daily_table, row, (
                item.report_date.isoformat(), item.api_name, str(item.opened_count), str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_pnl_with_r(item.net_pnl, item.risk_amount), str(item.open_count),
            ))
        self._symbol_table.setRowCount(len(report.by_symbol))
        for row, item in enumerate(report.by_symbol):
            self._set_row(self._symbol_table, row, (
                item.report_date.isoformat(), item.api_name, item.group_name, str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_pnl_with_r(item.net_pnl, item.risk_amount),
            ))
        self._strategy_table.setRowCount(len(report.by_strategy))
        for row, item in enumerate(report.by_strategy):
            self._set_row(self._strategy_table, row, (
                item.report_date.isoformat(), item.api_name, item.group_name, str(item.closed_count),
                str(item.win_count), str(item.loss_count), format_report_pnl_with_r(item.net_pnl, item.risk_amount),
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
                format_report_pnl_with_r(trade.net_pnl, trade.risk_amount), trade.status, trade.source, trade.close_reason,
            ))
            symbol_item = self._detail_table.item(row, 2)
            if symbol_item is not None:
                symbol_item.setData(Qt.ItemDataRole.UserRole, trade)
            pnl_item = self._detail_table.item(row, 10)
            if pnl_item is not None:
                pnl_item.setToolTip(f"原币净盈亏：{trade.native_net_pnl if trade.native_net_pnl is not None else '--'} {trade.pnl_currency}\n{trade.valuation_note}")
        net = sum((item.net_pnl for item in report.daily), start=Decimal("0")) if report.daily else Decimal("0")
        risk = sum((item.risk_amount for item in report.daily), start=Decimal("0")) if report.daily else Decimal("0")
        profile_text = self._runtime_profile_name()
        self._profile_label.setText(f"API {profile_text} · {'模拟' if self._environment == 'demo' else '实盘'}")
        missing = sum(trade.net_pnl is None for trade in completed_trades(report.trades, report.start_date, report.end_date))
        self._status.setText(f"{report.start_date} 至 {report.end_date} · 本地记录 {len(report.trades)} 条 · 已估值净盈亏 {format_report_pnl_with_r(net, risk)} · 未估值 {missing} 条")
        self._apply_detail_day()

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
        if not symbol or symbol == "-" or trade is None:
            return
        if self._trade_kline_window is None:
            self._trade_kline_window = InstrumentKlineDialog(initial_bar="1H", parent=self)
            self._trade_kline_window.destroyed.connect(lambda: setattr(self, "_trade_kline_window", None))
        closed_ms = int(trade.closed_at.timestamp() * 1000) if trade.closed_at else 0
        matched_history_item = None
        matched_distance = None
        if trade is not None and trade.api_name == self._profile_name and trade.environment == self._environment:
            for history_item in self._history_items:
                if str(getattr(history_item, "inst_id", "") or "").strip().upper() != symbol:
                    continue
                update_ms = int(getattr(history_item, "update_time", 0) or 0)
                distance = abs(update_ms - closed_ms) if update_ms and closed_ms else 0
                if matched_history_item is None or distance < matched_distance:
                    matched_history_item = history_item
                    matched_distance = distance
        if matched_history_item is not None:
            markers = _position_history_kline_price_markers(matched_history_item)
            time_markers = _position_history_kline_time_markers(matched_history_item)
        else:
            markers = ()
            time_markers = tuple(
                (label, int(timestamp.timestamp() * 1000))
                for label, timestamp in (("开仓", trade.opened_at), ("平仓", trade.closed_at))
                if timestamp is not None
            )
        if not markers:
            # Keep the transaction-details entry point useful even when an
            # older/corrupt history snapshot lacks direction or raw position
            # fields.  The report still has the prices and quantity needed for
            # the same visual entry/exit markers.
            direction = str(trade.direction or "").strip().lower()
            direction = "short" if direction == "short" else "long"
            direct_markers = []
            if trade.opened_at is not None and trade.entry_price is not None:
                direct_markers.append(
                    PositionPriceMarker(
                        "entry",
                        int(trade.opened_at.timestamp() * 1000),
                        trade.entry_price,
                        direction,
                        quantity=trade.size,
                        quantity_unit="张" if symbol.count("-") >= 3 else symbol.split("-", 1)[0],
                        entry_value_usdt=None,
                    )
                )
            if trade.closed_at is not None and trade.exit_price is not None:
                direct_markers.append(
                    PositionPriceMarker(
                        "exit",
                        int(trade.closed_at.timestamp() * 1000),
                        trade.exit_price,
                        direction,
                        realized_pnl=trade.net_pnl,
                        quantity=trade.size,
                        quantity_unit="张" if symbol.count("-") >= 3 else symbol.split("-", 1)[0],
                    )
                )
            markers = tuple(direct_markers)
        self._trade_kline_window.show_instrument(
            inst_id=symbol,
            inst_type=trade.inst_type or instrument_type(symbol),
            time_markers=time_markers,
            position_price_markers=markers,
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
        if self._stopping:
            if callable(callback):
                callback()
            return
        self._stopping = True
        self._reload_timer.stop()
        self._local_timer.stop()
        self._account_timer.stop()
        self._realtime_store.snapshot_ready.disconnect(self._apply_account_snapshot)
        self._history_manager.sync_finished.disconnect(self._on_history_synced)
        if callable(callback):
            callback()
