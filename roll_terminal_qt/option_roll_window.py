from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable

from PySide6.QtCore import QThread, Signal, Slot
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
)

from okx_quant.models import Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient, OkxTicker
from okx_quant.option_roll import OptionRollSuggestion, OptionRollTransferPayload, build_option_roll_suggestions, build_option_roll_transfer_payload
from okx_quant.option_strategy import OptionQuote, parse_option_contract
from okx_quant.pricing import format_decimal
from roll_terminal_qt.app_icon import apply_qt_window_icon


def _fmt(value: Decimal | None) -> str:
    return "-" if value is None else format_decimal(value)


@dataclass(frozen=True)
class _ScanResult:
    suggestions: tuple[OptionRollSuggestion, ...]
    instruments: dict[str, Instrument]
    quotes: dict[str, OptionQuote]


class _ScanThread(QThread):
    completed = Signal(int, object)
    failed = Signal(int, str)

    def __init__(self, *, request_id: int, client: OkxRestClient, position: OkxPosition, instrument: Instrument, quote: OptionQuote, preference: str, strike_scope: str, strike_levels: int | None, limit: int, parent: QDialog) -> None:
        super().__init__(parent)
        self._request_id = request_id
        self._client = client
        self._position = position
        self._instrument = instrument
        self._quote = quote
        self._preference = preference
        self._strike_scope = strike_scope
        self._strike_levels = strike_levels
        self._limit = limit

    def run(self) -> None:
        try:
            settlement = self._quote.index_price or self._quote.mark_price or self._quote.last_price
            if settlement is None or settlement <= 0:
                raise ValueError("\u5f53\u524d\u6301\u4ed3\u7f3a\u5c11\u6709\u6548\u6807\u7684\u4ef7\u683c\u3002")
            family = parse_option_contract(self._position.inst_id).inst_family
            instruments = [item for item in self._client.get_option_instruments(inst_family=family) if item.state.strip().lower() == "live"]
            instrument_map = {item.inst_id: item for item in instruments}
            quote_map: dict[str, OptionQuote] = {}
            for ticker in self._client.get_tickers("OPTION", inst_family=family):
                instrument = instrument_map.get(ticker.inst_id)
                if instrument is not None:
                    quote_map[instrument.inst_id] = OptionQuote(
                        instrument=instrument,
                        mark_price=ticker.mark,
                        bid_price=ticker.bid,
                        ask_price=ticker.ask,
                        last_price=ticker.last,
                        index_price=ticker.index,
                    )
            suggestions = build_option_roll_suggestions(
                current_position=self._position,
                current_instrument=self._instrument,
                current_quote=self._quote,
                candidate_instruments=instruments,
                candidate_quotes_by_inst_id=quote_map,
                settlement_price=settlement,
                valuation_time=datetime.now(),
                preference=self._preference,
                strike_scope=self._strike_scope,
                preferred_strike_levels=self._strike_levels,
                max_results=self._limit,
            )
            self.completed.emit(self._request_id, _ScanResult(tuple(suggestions), instrument_map, quote_map))
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._request_id, str(exc))


class OptionRollQtDialog(QDialog):
    def __init__(self, *, client: OkxRestClient, position: OkxPosition, instrument: Instrument, ticker: OkxTicker, api_name: str, send_to_strategy: Callable[[OptionRollTransferPayload], None], parent: QDialog | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("\u671f\u6743\u5c55\u671f\u5efa\u8bae")
        self.resize(1420, 860)
        self._client = client
        self._position = position
        self._instrument = instrument
        self._quote = OptionQuote(instrument=instrument, mark_price=ticker.mark, bid_price=ticker.bid, ask_price=ticker.ask, last_price=ticker.last, index_price=ticker.index)
        self._api_name = api_name
        self._send_to_strategy = send_to_strategy
        self._request_id = 0
        self._thread: _ScanThread | None = None
        self._suggestions: list[OptionRollSuggestion] = []
        self._instruments: dict[str, Instrument] = {}
        self._quotes: dict[str, OptionQuote] = {}
        self._build_ui()
        self._render_position()
        self.start_scan()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        self._position_label = QLabel()
        self._position_label.setObjectName("Subtle")
        self._position_label.setWordWrap(True)
        layout.addWidget(self._position_label)
        filters = QFormLayout()
        self._preference = QComboBox()
        self._scope = QComboBox()
        self._levels = QComboBox()
        self._limit = QComboBox()
        for label, value in (("\u4f18\u5148\u51c0\u6536\u6743\u5229\u91d1", "credit"), ("\u4f18\u5148\u964d\u4f4e\u98ce\u9669", "risk"), ("\u4f18\u5148 Delta \u63a5\u8fd1", "delta"), ("\u4f18\u5148\u65f6\u95f4\u4ef7\u503c", "time_value"), ("\u4f18\u5148\u66f4\u8fd1\u5230\u671f", "near_expiry")):
            self._preference.addItem(label, value)
        for label, value in (("\u66f4\u865a\u503c\u4f18\u5148", "safer_preferred"), ("\u540c\u6267\u884c\u4ef7\u4f18\u5148", "same_preferred"), ("\u540c\u884c\u6743\u4ef7\u53ca\u66f4\u5b89\u5168\u65b9\u5411", "same_and_safer"), ("\u5168\u90e8", "all")):
            self._scope.addItem(label, value)
        for label, value in (("\u4e0d\u9650", None), ("1\u6863\u5185\u4f18\u5148", 1), ("2\u6863\u5185\u4f18\u5148", 2), ("3\u6863\u5185\u4f18\u5148", 3), ("5\u6863\u5185\u4f18\u5148", 5)):
            self._levels.addItem(label, value)
        for value in (10, 20, 30):
            self._limit.addItem(str(value), value)
        filters.addRow("\u76ee\u6807\u504f\u597d", self._preference)
        filters.addRow("\u884c\u6743\u4ef7\u65b9\u5411", self._scope)
        filters.addRow("\u6863\u4f4d\u4f18\u5148", self._levels)
        filters.addRow("\u5019\u9009\u6570\u91cf", self._limit)
        filter_row = QHBoxLayout()
        filter_row.addLayout(filters)
        filter_row.addStretch(1)
        self._scan_button = QPushButton("\u626b\u63cf\u5efa\u8bae")
        self._scan_button.clicked.connect(self.start_scan)
        filter_row.addWidget(self._scan_button)
        layout.addLayout(filter_row)
        self._status = QLabel("\u51c6\u5907\u626b\u63cf\u3002")
        self._status.setObjectName("Subtle")
        layout.addWidget(self._status)
        headers = ("\u5efa\u8bae\u65b0\u4ed3", "\u5c55\u671f\u65b9\u5f0f", "\u8ddd\u5230\u671f\u5929\u6570", "\u9884\u8ba1\u51c0\u6536/\u4ed8", "\u4e70\u4e00\u4ef7", "\u5356\u4e00\u4ef7", "\u6807\u8bb0\u4ef7", "\u65b0 Delta", "\u98ce\u9669\u53d8\u5316", "\u4ef7\u5dee", "\u5efa\u8bae\u7406\u7531")
        self._table = QTableWidget(0, len(headers))
        self._table.setHorizontalHeaderLabels(headers)
        self._table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self._table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.verticalHeader().setVisible(False)
        for index, width in enumerate((260, 120, 96, 110, 90, 90, 90, 90, 110, 90, 380)):
            self._table.setColumnWidth(index, width)
        self._table.itemSelectionChanged.connect(self._render_detail)
        layout.addWidget(self._table, 1)
        self._detail = QTextEdit()
        self._detail.setReadOnly(True)
        self._detail.setMinimumHeight(150)
        layout.addWidget(self._detail)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        self._send_button = buttons.addButton("\u9001\u5165\u671f\u6743\u7b56\u7565\u8ba1\u7b97\u5668", QDialogButtonBox.ButtonRole.ActionRole)
        self._send_button.clicked.connect(self.send_selected_to_strategy)
        buttons.rejected.connect(self.close)
        layout.addWidget(buttons)

    def _render_position(self) -> None:
        parsed = parse_option_contract(self._position.inst_id)
        mark = self._position.mark_price or self._quote.mark_price
        self._position_label.setText(" | ".join((f"API {self._api_name or '-'}", f"\u5408\u7ea6 {self._position.inst_id}", f"\u6570\u91cf {_fmt(abs(self._position.position))}", f"\u6807\u8bb0\u4ef7 {_fmt(mark)}", f"\u4e70\u4e00/\u5356\u4e00 {_fmt(self._quote.bid_price)} / {_fmt(self._quote.ask_price)}", f"\u5230\u671f {parsed.expiry_label}", f"\u884c\u6743\u4ef7 {_fmt(parsed.strike)}")))

    @Slot()
    def start_scan(self) -> None:
        if self._thread is not None and self._thread.isRunning():
            return
        self._request_id += 1
        self._scan_button.setEnabled(False)
        self._status.setText("\u6b63\u5728\u626b\u63cf\u5c55\u671f\u5efa\u8bae...")
        thread = _ScanThread(request_id=self._request_id, client=self._client, position=self._position, instrument=self._instrument, quote=self._quote, preference=str(self._preference.currentData()), strike_scope=str(self._scope.currentData()), strike_levels=self._levels.currentData(), limit=int(self._limit.currentData()), parent=self)
        thread.completed.connect(self._apply_result)
        thread.failed.connect(self._apply_error)
        thread.finished.connect(thread.deleteLater)
        self._thread = thread
        thread.start()

    @Slot(int, object)
    def _apply_result(self, request_id: int, result: object) -> None:
        if request_id != self._request_id or not isinstance(result, _ScanResult):
            return
        self._thread = None
        self._scan_button.setEnabled(True)
        self._suggestions = list(result.suggestions)
        self._instruments, self._quotes = result.instruments, result.quotes
        self._table.setRowCount(len(self._suggestions))
        for row, item in enumerate(self._suggestions):
            values = (item.new_inst_id, item.roll_type, str(item.days_to_expiry), _fmt(item.net_credit), _fmt(item.candidate_bid), _fmt(item.candidate_ask), _fmt(item.candidate_mark), _fmt(item.new_delta), item.risk_change, _fmt(item.price_gap), item.reason)
            for column, value in enumerate(values):
                self._table.setItem(row, column, QTableWidgetItem(value))
        if self._suggestions:
            self._table.selectRow(0)
            self._status.setText(f"\u5df2\u751f\u6210 {len(self._suggestions)} \u6761\u5c55\u671f\u5efa\u8bae\u3002")
        else:
            self._detail.setPlainText("\u672a\u627e\u5230\u7b26\u5408\u6761\u4ef6\u7684\u5019\u9009\u5408\u7ea6\u3002")
            self._status.setText("\u672a\u627e\u5230\u5c55\u671f\u5efa\u8bae\u3002")

    @Slot(int, str)
    def _apply_error(self, request_id: int, message: str) -> None:
        if request_id != self._request_id:
            return
        self._thread = None
        self._scan_button.setEnabled(True)
        self._status.setText(f"\u626b\u63cf\u5931\u8d25\uff1a{message}")
        self._detail.setPlainText(f"\u626b\u63cf\u5931\u8d25\uff1a{message}")

    @Slot()
    def _render_detail(self) -> None:
        row = self._table.currentRow()
        if row < 0 or row >= len(self._suggestions):
            return
        item = self._suggestions[row]
        self._detail.setPlainText("\n".join((f"\u5f53\u524d\u5408\u7ea6\uff1a{item.current_inst_id}", f"\u5efa\u8bae\u65b0\u4ed3\uff1a{item.new_inst_id}", f"\u5c55\u671f\u65b9\u5f0f\uff1a{item.roll_type}", f"\u5e73\u65e7\u53c2\u8003\uff1a{_fmt(item.close_price)} ({item.close_price_source})", f"\u5f00\u65b0\u53c2\u8003\uff1a{_fmt(item.open_price)} ({item.open_price_source})", f"\u9884\u8ba1\u51c0\u6536/\u4ed8\uff1a{_fmt(item.net_credit)}", f"\u98ce\u9669\u53d8\u5316\uff1a{item.risk_change}", f"\u65b0 Delta\uff1a{_fmt(item.new_delta)}", f"\u65f6\u95f4\u4ef7\u503c\uff1a{_fmt(item.current_time_value)} -> {_fmt(item.new_time_value)}", f"\u4ef7\u5dee\uff1a{_fmt(item.price_gap)}", f"\u5efa\u8bae\u7406\u7531\uff1a{item.reason}")))

    @Slot()
    def send_selected_to_strategy(self) -> None:
        row = self._table.currentRow()
        if row < 0 or row >= len(self._suggestions):
            QMessageBox.information(self, "\u9001\u5165\u7b56\u7565\u8ba1\u7b97\u5668", "\u8bf7\u5148\u9009\u62e9\u4e00\u6761\u5c55\u671f\u5efa\u8bae\u3002")
            return
        item = self._suggestions[row]
        instrument, quote = self._instruments.get(item.new_inst_id), self._quotes.get(item.new_inst_id)
        if instrument is None or quote is None:
            QMessageBox.warning(self, "\u9001\u5165\u7b56\u7565\u8ba1\u7b97\u5668", "\u5019\u9009\u5408\u7ea6\u6570\u636e\u7f3a\u5931\uff0c\u8bf7\u91cd\u65b0\u626b\u63cf\u3002")
            return
        payload = build_option_roll_transfer_payload(current_position=self._position, current_instrument=self._instrument, current_quote=self._quote, suggestion=item, candidate_instrument=instrument, candidate_quote=quote)
        self._send_to_strategy(payload)
        self._status.setText("\u5df2\u9001\u5165 Qt \u671f\u6743\u7b56\u7565\u8ba1\u7b97\u5668\u3002")
