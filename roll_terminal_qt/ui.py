from __future__ import annotations

from PySide6.QtCore import Qt, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from roll_terminal_qt.formatting import fmt_decimal
from roll_terminal_qt.market_service import MarketFeedThread
from roll_terminal_qt.models import LegMarket, MarketPairSnapshot
from roll_terminal_qt.style import APP_STYLE


class OrderBookPanel(QFrame):
    def __init__(self, title: str) -> None:
        super().__init__()
        self.setObjectName("Panel")
        self._title = QLabel(title)
        self._title.setObjectName("Metric")
        self._quote = QLabel("等待行情...")
        self._table = QTableWidget(20, 2)
        self._table.setHorizontalHeaderLabels(["价格", "数量"])
        self._table.verticalHeader().setVisible(False)
        self._table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self._table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self._table.horizontalHeader().setStretchLastSection(True)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)
        layout.addWidget(self._title)
        layout.addWidget(self._quote)
        layout.addWidget(self._table, 1)

    def update_leg(self, leg: LegMarket) -> None:
        self._title.setText(f"{leg.inst_id} 盘口 [{leg.source}]")
        self._quote.setText(
            f"最新 {fmt_decimal(leg.last)} | 买一 {fmt_decimal(leg.bid)} | 卖一 {fmt_decimal(leg.ask)}"
        )
        rows = list(reversed(leg.asks[:10])) + list(leg.bids[:10])
        for row_index in range(self._table.rowCount()):
            if row_index >= len(rows):
                self._table.setItem(row_index, 0, QTableWidgetItem(""))
                self._table.setItem(row_index, 1, QTableWidgetItem(""))
                continue
            row = rows[row_index]
            price_item = QTableWidgetItem(fmt_decimal(row.price))
            size_item = QTableWidgetItem(fmt_decimal(row.size))
            price_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            size_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            is_ask = row_index < min(10, len(leg.asks))
            color = QColor("#c83b55" if is_ask else "#1a7f46")
            price_item.setForeground(color)
            size_item.setForeground(color)
            self._table.setItem(row_index, 0, price_item)
            self._table.setItem(row_index, 1, size_item)


class RollTerminalWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("QQOKX Roll Terminal")
        self.resize(1500, 900)
        self._feed = MarketFeedThread(environment="live")
        self._feed.snapshot_ready.connect(self._apply_snapshot)
        self._feed.status_changed.connect(self._set_status)
        self._build_ui()
        self._feed.start()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        self._feed.stop()
        if not self._feed.wait(1500):
            self._feed.terminate()
            self._feed.wait(1500)
        super().closeEvent(event)

    def _build_ui(self) -> None:
        root = QWidget()
        main = QVBoxLayout(root)
        main.setContentsMargins(0, 0, 0, 0)
        main.setSpacing(0)

        header = QFrame()
        header.setObjectName("Header")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(16, 12, 16, 12)
        title = QLabel("现货套利 极速移仓终端")
        title.setObjectName("Title")
        self._status = QLabel("启动中...")
        self._api = QComboBox()
        self._api.addItems(["2211 | live"])
        header_layout.addWidget(title)
        header_layout.addStretch(1)
        header_layout.addWidget(QLabel("API"))
        header_layout.addWidget(self._api)
        header_layout.addWidget(self._status)
        main.addWidget(header)

        controls = QFrame()
        controls.setObjectName("Panel")
        controls_layout = QGridLayout(controls)
        controls_layout.setContentsMargins(14, 12, 14, 12)
        controls_layout.setHorizontalSpacing(10)
        controls_layout.setVerticalSpacing(8)
        self._current = QComboBox()
        self._current.setEditable(True)
        self._current.addItems(["BTC-USD-260626", "BTC-USD-260925"])
        self._target = QComboBox()
        self._target.setEditable(True)
        self._target.addItems(["BTC-USD-260925", "BTC-USD-260703"])
        self._qty = QLineEdit("10")
        self._mode = QComboBox()
        self._mode.addItems(["双腿吃单", "旧合约挂单/新合约吃单", "新合约挂单/旧合约吃单", "双方挂单/先成后市价"])
        switch = QPushButton("切换合约")
        switch.clicked.connect(self._switch_pair)
        execute = QPushButton("执行移仓")
        execute.setObjectName("Primary")
        execute.setEnabled(False)
        execute.setToolTip("下单引擎将在下一阶段接入")
        stop = QPushButton("停止")
        stop.setObjectName("Danger")
        controls_layout.addWidget(QLabel("当前交割"), 0, 0)
        controls_layout.addWidget(self._current, 0, 1)
        controls_layout.addWidget(QLabel("目标交割"), 0, 2)
        controls_layout.addWidget(self._target, 0, 3)
        controls_layout.addWidget(switch, 0, 4)
        controls_layout.addWidget(QLabel("数量(张)"), 1, 0)
        controls_layout.addWidget(self._qty, 1, 1)
        controls_layout.addWidget(QLabel("执行方式"), 1, 2)
        controls_layout.addWidget(self._mode, 1, 3)
        controls_layout.addWidget(execute, 1, 4)
        controls_layout.addWidget(stop, 1, 5)
        main.addWidget(controls)

        metrics = QFrame()
        metrics.setObjectName("Panel")
        metrics_layout = QHBoxLayout(metrics)
        metrics_layout.setContentsMargins(14, 10, 14, 10)
        self._spread = QLabel("绝对价差 - | 价差率 -")
        self._spread.setObjectName("Metric")
        self._source = QLabel("等待盘口...")
        metrics_layout.addWidget(self._spread)
        metrics_layout.addStretch(1)
        metrics_layout.addWidget(self._source)
        main.addWidget(metrics)

        books = QWidget()
        books_layout = QHBoxLayout(books)
        books_layout.setContentsMargins(14, 12, 14, 12)
        books_layout.setSpacing(12)
        self._left_book = OrderBookPanel("左腿盘口")
        self._right_book = OrderBookPanel("右腿盘口")
        books_layout.addWidget(self._left_book, 1)
        books_layout.addWidget(self._right_book, 1)
        main.addWidget(books, 1)
        self.setCentralWidget(root)

    @Slot()
    def _switch_pair(self) -> None:
        current = self._current.currentText().strip().upper()
        target = self._target.currentText().strip().upper()
        self._set_status(f"正在切换：{current} / {target}")
        self._feed.set_pair(current, target)

    @Slot(object)
    def _apply_snapshot(self, snapshot: MarketPairSnapshot) -> None:
        self._left_book.update_leg(snapshot.current)
        self._right_book.update_leg(snapshot.target)
        self._spread.setText(
            f"绝对价差 {fmt_decimal(snapshot.spread_abs)} | 价差率 {fmt_decimal(snapshot.spread_pct, 4)}%"
        )
        self._source.setText(snapshot.status)
        self._status.setText("在线")

    @Slot(str)
    def _set_status(self, text: str) -> None:
        self._status.setText(text)


def run() -> int:
    app = QApplication([])
    app.setStyleSheet(APP_STYLE)
    window = RollTerminalWindow()
    window.show()
    return app.exec()
