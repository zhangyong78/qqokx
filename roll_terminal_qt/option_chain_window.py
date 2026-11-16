from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import QSettings, Qt, QTimer
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView, QButtonGroup, QComboBox, QFrame, QHBoxLayout, QHeaderView, QLabel, QLineEdit,
    QMainWindow, QMenu, QMessageBox, QPushButton, QScrollArea, QTableWidget,
    QTableWidgetItem, QVBoxLayout, QWidget,
)

from okx_quant.option_strategy import build_option_chain_rows, format_option_expiry_label, parse_option_contract
from okx_quant.option_strategy_ui import _build_option_quote, _format_price
from okx_quant.pricing import format_decimal
from roll_terminal_qt.option_strategy_window import (
    ChainSnapshot, OptionChainLinkedChartDialog, OptionStrategyQtWindow, _OptionChainThread, _detail_coin_amount,
    _detail_usdt_price, _detail_usdt_summary, _format_option_summary_value, _shared_client,
)


class OptionChainWindow(QMainWindow):
    def __init__(self, parent=None, *, profile_name: str = "") -> None:
        super().__init__(parent)
        self.setWindowTitle("期权 T 形报价")
        self.resize(1800, 1000)
        self._profile_name = profile_name
        self._client = _shared_client()
        self._settings = QSettings("qqokx", "roll-terminal-qt")
        self._snapshots = {}
        self._workers = set()
        self._request_id = 0
        saved_family = str(self._settings.value("option_chain/family", "BTC-USD"))
        self._family = saved_family if saved_family in ("BTC-USD", "ETH-USD") else "BTC-USD"
        self._expiry = ""
        self._rows = []
        self._pending = []
        self._strategy = None
        self._strategy_windows = []
        self._linked_chart_dialog = None
        self._closing = False
        self._sized_columns = False
        self._geometry_ready = False
        self._last_refresh_by_family = {}

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        family_bar = QHBoxLayout()
        self._family_buttons = QButtonGroup(self)
        self._family_buttons.setExclusive(True)
        for family in ("BTC-USD", "ETH-USD"):
            button = QPushButton(f"{family} 期权")
            button.setCheckable(True)
            button.setChecked(family == self._family)
            self._family_buttons.addButton(button)
            button.clicked.connect(lambda _checked=False, value=family: self._select_family(value))
            family_bar.addWidget(button)
        self._status = QLabel()
        family_bar.addWidget(self._status, 1)
        self._last_refresh_label = QLabel("最后刷新：—")
        self._last_refresh_label.setToolTip("当前期权系列最后一次成功加载报价的本地时间")
        family_bar.addWidget(self._last_refresh_label)
        refresh = QPushButton("刷新")
        refresh.clicked.connect(self._refresh)
        family_bar.addWidget(refresh)
        layout.addLayout(family_bar)

        self._expiry_container = QWidget()
        self._expiry_layout = QHBoxLayout(self._expiry_container)
        self._expiry_layout.setContentsMargins(0, 0, 0, 0)
        expiry_scroll = QScrollArea()
        expiry_scroll.setWidgetResizable(True)
        expiry_scroll.setWidget(self._expiry_container)
        expiry_scroll.setFixedHeight(60)
        layout.addWidget(expiry_scroll)

        filters = QHBoxLayout()
        filters.addWidget(QLabel("行权价格"))
        self._minimum = QLineEdit()
        self._maximum = QLineEdit()
        self._minimum.setPlaceholderText("最低")
        self._maximum.setPlaceholderText("最高")
        for widget in (self._minimum, self._maximum):
            widget.setMaximumWidth(120)
            widget.textChanged.connect(self._render)
            filters.addWidget(widget)
        filters.addWidget(QLabel("当前价格周围"))
        self._near = QComboBox()
        for label, value in (("全部", 0), ("±5%", 5), ("±10%", 10), ("±20%", 20)):
            self._near.addItem(label, value)
        self._near.currentIndexChanged.connect(self._render)
        filters.addWidget(self._near)
        clear = QPushButton("清除筛选")
        clear.clicked.connect(self._clear_filters)
        filters.addWidget(clear)
        filters.addStretch(1)
        strategy = QPushButton("打开策略计算器")
        strategy.clicked.connect(self._open_strategy)
        filters.addWidget(strategy)
        linked_chart = QPushButton("联动 K 线")
        linked_chart.setToolTip("选中一个行权价后，打开认购 / DVOL / 认沽三栏联动图")
        linked_chart.clicked.connect(self._open_linked_chart)
        filters.addWidget(linked_chart)
        layout.addLayout(filters)

        self._quote_keys = (("bid_size", "买量（币）"), ("bid_usdt", "买价≈USDT"),
                            ("bid", "买价"), ("mark", "标记价"), ("mark_usdt", "标记≈USDT"),
                            ("ask", "卖价"), ("ask_usdt", "卖价≈USDT"), ("ask_size", "卖量（币）"))
        self._greek_keys = (("iv", "IV"), ("delta", "Delta PA"), ("gamma", "Gamma PA"),
                            ("vega", "Vega PA"), ("theta", "Theta PA"), ("theta_usdt", "Theta≈USDT"))
        self._left = tuple(reversed(self._greek_keys)) + self._quote_keys + (("position", "持仓"),)
        self._right = (("position", "持仓"),) + self._quote_keys + self._greek_keys
        self._columns = self._left + (("expiry", "到期日"), ("strike", "行权价")) + self._right
        self._table = QTableWidget(0, len(self._columns))
        self._table.setHorizontalHeaderLabels([label for _, label in self._columns])
        self._table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().hide()
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self._table.horizontalHeader().setStretchLastSection(False)
        menu_button = QPushButton("列设置")
        menu = QMenu(menu_button)
        menu_button.setMenu(menu)
        filters.addWidget(menu_button)
        saved = self._settings.value("option_chain/hidden_columns", [])
        hidden = set(saved if isinstance(saved, list) else [saved])
        self._column_actions = {}
        for key, label in self._columns:
            if key == "strike" or key in self._column_actions:
                continue
            action = menu.addAction(label)
            action.setCheckable(True)
            action.setChecked(key not in hidden)
            action.toggled.connect(lambda visible, name=key: self._set_column_visible(name, visible))
            self._column_actions[key] = action
            self._set_column_visible(key, key not in hidden, save=False)
        layout.addWidget(self._table, 1)
        actions = QHBoxLayout()
        for label, option_type, side in (("添加认购买入", "C", "buy"), ("添加认购卖出", "C", "sell"),
                                         ("添加认沽买入", "P", "buy"), ("添加认沽卖出", "P", "sell")):
            button = QPushButton(label)
            button.clicked.connect(lambda _checked=False, kind=option_type, direction=side: self._add_leg(kind, direction))
            actions.addWidget(button)
        self._pending_label = QLabel("已选 0 条")
        actions.addWidget(self._pending_label)
        clear_pending = QPushButton("清空已选")
        clear_pending.clicked.connect(self._clear_pending)
        actions.addWidget(clear_pending)
        actions.addStretch(1)
        layout.addLayout(actions)
        geometry = self._settings.value("option_chain/geometry")
        if geometry and not self.restoreGeometry(geometry):
            self.resize(1800, 1000)
        saved_width = self._settings.value("option_chain/width", 0, type=int)
        saved_height = self._settings.value("option_chain/height", 0, type=int)
        self._restore_saved_size = (saved_width, saved_height) if saved_width > 0 and saved_height > 0 else None
        self._restore_saved_maximized = self._settings.value("option_chain/maximized", False, type=bool)
        self._geometry_ready = True
        self._restore_filters()
        QTimer.singleShot(0, self._refresh)

    def _set_column_visible(self, key, visible, *, save=True):
        for index, (column_key, _) in enumerate(self._columns):
            if column_key == key:
                self._table.setColumnHidden(index, not visible)
        if save:
            self._settings.setValue("option_chain/hidden_columns", [name for name, action in self._column_actions.items() if not action.isChecked()])

    def _select_family(self, family):
        if family == self._family:
            return
        self._save_filters()
        self._request_id += 1
        self._family = family
        self._show_last_refresh_time(family)
        self._restore_filters()
        if family in self._snapshots:
            self._update_expiries()
            self._render()
        else:
            self._rows = []
            self._table.setRowCount(0)
            self._refresh()

    def _refresh(self):
        self._request_id += 1
        self._status.setText(f"正在加载 {self._family}…")
        thread = _OptionChainThread(request_id=self._request_id, family=self._family, preferred_expiry="",
                                    profile_name=self._profile_name, client=self._client, parent=self)
        self._workers.add(thread)
        thread.snapshot_ready.connect(self._on_snapshot)
        thread.error_raised.connect(self._on_error)
        thread.finished.connect(lambda worker=thread: self._worker_finished(worker))
        thread.start()

    def _worker_finished(self, worker):
        self._workers.discard(worker)
        worker.deleteLater()
        if self._closing and not self._workers:
            self.close()

    def _on_error(self, request_id, message):
        if request_id == self._request_id:
            self._status.setText(f"加载失败：{message}")

    def _on_snapshot(self, request_id, snapshot):
        if self._closing or request_id != self._request_id or not isinstance(snapshot, ChainSnapshot) or snapshot.family != self._family:
            return
        self._snapshots[snapshot.family] = snapshot
        self._last_refresh_by_family[snapshot.family] = datetime.now().astimezone()
        self._show_last_refresh_time(snapshot.family)
        if self._expiry and self._expiry not in snapshot.expiries:
            self._expiry = ""
        self._update_expiries()
        self._render()
        if not self._sized_columns:
            self._table.resizeColumnsToContents()
            widths = self._settings.value("option_chain/column_widths", [])
            if isinstance(widths, list):
                for index, width in enumerate(widths[:self._table.columnCount()]):
                    try:
                        self._table.setColumnWidth(index, max(24, int(width)))
                    except (TypeError, ValueError):
                        continue
            self._sized_columns = True

    def _show_last_refresh_time(self, family):
        refreshed_at = self._last_refresh_by_family.get(family)
        text = refreshed_at.strftime("%Y-%m-%d %H:%M:%S") if refreshed_at else "—"
        self._last_refresh_label.setText(f"最后刷新：{text}")

    def _update_expiries(self):
        while self._expiry_layout.count():
            item = self._expiry_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        snapshot = self._snapshots[self._family]
        position_sides_by_expiry = {}
        for inst_id, position_text in snapshot.position_coin_text_by_inst_id.items():
            expiry = parse_option_contract(inst_id).expiry_code
            sides = position_sides_by_expiry.setdefault(expiry, set())
            if "多 " in position_text:
                sides.add("buy")
            if "空 " in position_text:
                sides.add("sell")
        for expiry in ("", *snapshot.expiries):
            label = format_option_expiry_label(expiry) if expiry else "全部"
            container = QFrame(self._expiry_container)
            selected = expiry == self._expiry
            border_color = "#2563eb" if selected else "#d4dbe5"
            border_width = 2 if selected else 1
            background = "#e8f1ff" if selected else "#ffffff"
            container.setStyleSheet(
                f"QFrame {{ border: {border_width}px solid {border_color}; border-radius: 7px; background: {background}; }}"
            )
            container_layout = QHBoxLayout(container)
            container_layout.setContentsMargins(4, 2, 4, 2)
            button = QPushButton(label)
            button.setFlat(True)
            button.setStyleSheet(
                "border: 0; background: transparent; padding: 3px 5px; "
                + ("color: #1d4ed8; font-weight: 700;" if selected else "")
            )
            button.clicked.connect(lambda _checked=False, value=expiry: self._select_expiry(value))
            container_layout.addWidget(button)
            badge_column = QVBoxLayout()
            badge_column.setContentsMargins(0, 0, 0, 0)
            badge_column.setSpacing(1)
            has_badges = False
            for side, text, color, background in (
                ("buy", "买方", "#087f5b", "#d3f9d8"),
                ("sell", "卖方", "#c92a2a", "#ffe3e3"),
            ):
                if expiry not in position_sides_by_expiry or side not in position_sides_by_expiry[expiry]:
                    continue
                badge = QLabel(text)
                badge.setToolTip(f"{text}期权持仓")
                badge.setStyleSheet(
                    f"color: {color}; background: {background}; border-radius: 5px; "
                    "padding: 1px 4px; font-size: 9px; font-weight: 600; border: 0;"
                )
                badge_column.addWidget(badge)
                has_badges = True
            if has_badges:
                container_layout.addLayout(badge_column)
            self._expiry_layout.addWidget(container)
        self._expiry_layout.addStretch(1)

    def _select_expiry(self, expiry):
        self._expiry = expiry
        self._update_expiries()
        self._render()

    def _clear_filters(self):
        self._minimum.clear()
        self._maximum.clear()
        self._near.setCurrentIndex(0)
        self._render()

    def _save_filters(self):
        prefix = f"option_chain/filters/{self._family}"
        self._settings.setValue(f"{prefix}/expiry", self._expiry)
        self._settings.setValue(f"{prefix}/minimum", self._minimum.text())
        self._settings.setValue(f"{prefix}/maximum", self._maximum.text())
        self._settings.setValue(f"{prefix}/near", self._near.currentIndex())

    def _restore_filters(self):
        prefix = f"option_chain/filters/{self._family}"
        self._expiry = str(self._settings.value(f"{prefix}/expiry", "") or "")
        for widget in (self._minimum, self._maximum, self._near):
            widget.blockSignals(True)
        try:
            minimum = str(self._settings.value(f"{prefix}/minimum", "") or "")
            maximum = str(self._settings.value(f"{prefix}/maximum", "") or "")
            # 73000 was previously used as a sample range and should not keep
            # returning as an implicit default on later launches.
            self._minimum.setText("" if minimum.strip() == "73000" else minimum)
            self._maximum.setText("" if maximum.strip() == "73000" else maximum)
            index = self._settings.value(f"{prefix}/near", 0, type=int)
            self._near.setCurrentIndex(index if 0 <= index < self._near.count() else 0)
        finally:
            for widget in (self._minimum, self._maximum, self._near):
                widget.blockSignals(False)

    def _quote_values(self, quote, snapshot):
        if quote is None:
            return {}
        inst_id = quote.instrument.inst_id
        ticker = snapshot.tickers_by_inst_id.get(inst_id)
        raw = ticker.raw if ticker else {}
        summary = snapshot.option_summaries_by_inst_id.get(inst_id, {})
        result = {"position": snapshot.position_coin_text_by_inst_id.get(inst_id, ""),
                  "bid_size": _detail_coin_amount(quote, raw.get("bidSz")),
                  "ask_size": _detail_coin_amount(quote, raw.get("askSz")),
                  "iv": _format_option_summary_value(summary.get("markVol"), percent=True),
                  "theta_usdt": _detail_usdt_summary(quote, summary.get("theta"), snapshot.coin_usdt_price)}
        for key in ("delta", "gamma", "vega", "theta"):
            result[key] = _format_option_summary_value(summary.get(key))
        for key, value in (("bid", quote.bid_price), ("mark", quote.mark_price), ("ask", quote.ask_price)):
            result[key] = _format_price(value, quote.instrument.tick_size) if value is not None else ""
            result[key + "_usdt"] = _detail_usdt_price(quote, value, snapshot.coin_usdt_price)
        return result

    def _render(self, *_args):
        snapshot = self._snapshots.get(self._family)
        if snapshot is None:
            return
        try:
            low = Decimal(self._minimum.text()) if self._minimum.text().strip() else None
            high = Decimal(self._maximum.text()) if self._maximum.text().strip() else None
            if (low is not None and not low.is_finite()) or (high is not None and not high.is_finite()):
                raise InvalidOperation
        except InvalidOperation:
            self._status.setText("请输入有效的行权价格")
            return
        near = self._near.currentData() or 0
        spot = snapshot.coin_usdt_price or snapshot.underlying_price
        if near and not spot:
            self._rows = []
            self._table.setRowCount(0)
            self._status.setText("当前价格不可用，无法按当前价格周围筛选")
            return
        if near and spot:
            lower, upper = spot * (Decimal(1) - Decimal(near) / 100), spot * (Decimal(1) + Decimal(near) / 100)
            low = max(low, lower) if low is not None else lower
            high = min(high, upper) if high is not None else upper
        grouped = {}
        for instrument in snapshot.family_instruments:
            expiry = parse_option_contract(instrument.inst_id).expiry_code
            if self._expiry and expiry != self._expiry:
                continue
            grouped.setdefault(expiry, []).append(_build_option_quote(instrument, snapshot.tickers_by_inst_id.get(instrument.inst_id)))
        self._rows = [(expiry, row) for expiry in sorted(grouped) for row in build_option_chain_rows(grouped[expiry])
                      if (low is None or row.strike >= low) and (high is None or row.strike <= high)]
        self._table.setRowCount(len(self._rows))
        for row_index, (expiry, row) in enumerate(self._rows):
            call = self._quote_values(row.call_quote, snapshot)
            put = self._quote_values(row.put_quote, snapshot)
            values = [call.get(key, "") for key, _ in self._left] + [format_option_expiry_label(expiry), format_decimal(row.strike)] + [put.get(key, "") for key, _ in self._right]
            for index, ((key, _), value) in enumerate(zip(self._columns, values)):
                item = QTableWidgetItem(value)
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                color = "#0f6b78" if key.endswith("_usdt") else {"bid": "#13803d", "ask": "#c23b3b", "mark": "#2563eb", "iv": "#7c3aed"}.get(key)
                if color:
                    item.setForeground(QColor(color))
                self._table.setItem(row_index, index, item)
        self._status.setText(f"{self._family}　标的≈{format_decimal(spot) if spot else '—'} USDT　{len(self._rows)} 行")

    def _add_leg(self, option_type, side):
        index = self._table.currentRow()
        if index < 0 or index >= len(self._rows):
            QMessageBox.information(self, "添加策略腿", "请先选择一个行权价。")
            return
        row = self._rows[index][1]
        quote = row.call_quote if option_type == "C" else row.put_quote
        if quote is None:
            QMessageBox.information(self, "添加策略腿", "该行没有对应合约。")
            return
        self._pending.append((quote, side))
        self._pending_label.setText(f"已选 {len(self._pending)} 条")
        self._pending_label.setToolTip("\n".join(f"{item.instrument.inst_id} {'买入' if direction == 'buy' else '卖出'}" for item, direction in self._pending))

    def _clear_pending(self):
        self._pending.clear()
        self._pending_label.setText("已选 0 条")
        self._pending_label.setToolTip("")

    def _open_linked_chart(self):
        index = self._table.currentRow()
        if index < 0 or index >= len(self._rows):
            QMessageBox.information(self, "期权联动 K 线", "请先选择一个行权价。")
            return
        chain_row = self._rows[index][1]
        if chain_row.call_quote is None and chain_row.put_quote is None:
            QMessageBox.information(self, "期权联动 K 线", "当前行没有可用的认购或认沽合约。")
            return
        if self._linked_chart_dialog is None:
            self._linked_chart_dialog = OptionChainLinkedChartDialog(
                client=self._client,
                profile_name=self._profile_name,
                parent=self,
            )
        self._linked_chart_dialog.apply_workspace_profile(self._profile_name)
        self._linked_chart_dialog.show_pair(
            call_quote=chain_row.call_quote,
            put_quote=chain_row.put_quote,
            bar="1H",
        )

    def _open_strategy(self):
        if self._strategy is not None and self._strategy._legs and self._pending:
            choice = QMessageBox.question(self, "加入策略", "将所选合约追加到当前策略吗？\n选择“否”会打开新策略。",
                                          QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No | QMessageBox.StandardButton.Cancel)
            if choice == QMessageBox.StandardButton.Cancel:
                return
            if choice == QMessageBox.StandardButton.No:
                self._strategy = None
        if self._strategy is None:
            self._strategy = OptionStrategyQtWindow(
                profile_name=self._profile_name,
                initial_chain_snapshot=self._snapshots.get(self._family),
                initial_expiry=self._expiry,
            )
            self._strategy_windows.append(self._strategy)
            if self._pending:
                family = parse_option_contract(self._pending[0][0].instrument.inst_id).inst_family
                self._strategy._family_combo.setCurrentText(family)
        elif self._family in self._snapshots:
            self._strategy.seed_chain_snapshot(self._snapshots[self._family], self._expiry)
        self._strategy.show()
        self._strategy.raise_()
        while self._pending:
            quote, side = self._pending[0]
            if not self._strategy.add_chain_quote(quote, side):
                self._pending_label.setText(f"已选 {len(self._pending)} 条")
                return
            self._pending.pop(0)
        self._clear_pending()

    def closeEvent(self, event):
        self._closing = True
        self._save_filters()
        self._settings.setValue("option_chain/family", self._family)
        self._settings.setValue("option_chain/geometry", self.saveGeometry())
        self._settings.setValue("option_chain/width", self.width())
        self._settings.setValue("option_chain/height", self.height())
        self._settings.setValue("option_chain/maximized", self.isMaximized())
        self._settings.setValue("option_chain/column_widths", [self._table.columnWidth(i) for i in range(self._table.columnCount())])
        for worker in self._workers:
            worker.requestInterruption()
        if self._workers:
            self.hide()
            event.ignore()
            return
        super().closeEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self._geometry_ready and not (self.windowState() & Qt.WindowState.WindowMinimized):
            self._settings.setValue("option_chain/geometry", self.saveGeometry())

    def showEvent(self, event):
        super().showEvent(event)
        self._geometry_ready = False
        if self._restore_saved_maximized:
            self.showMaximized()
        elif self._restore_saved_size is not None:
            self.resize(*self._restore_saved_size)
        self._geometry_ready = True
