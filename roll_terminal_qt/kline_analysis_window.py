from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QPushButton,
    QSplitter,
    QVBoxLayout,
    QWidget,
)


class KlineAnalysisWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("K线分析")
        self.resize(1680, 980)

        root = QWidget()
        self.setCentralWidget(root)

        main_layout = QVBoxLayout(root)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        self._build_header(main_layout)
        self._build_body(main_layout)

    def _build_header(self, parent_layout: QVBoxLayout) -> None:
        header = QFrame()
        header.setObjectName("Panel")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(12, 8, 12, 8)
        header_layout.setSpacing(8)

        title = QLabel("专业K线分析")
        title.setObjectName("SectionTitle")
        header_layout.addWidget(title, 1)

        self._status = QLabel("就绪")
        self._status.setObjectName("Subtle")
        header_layout.addWidget(self._status, 0, Qt.AlignmentFlag.AlignRight)
        parent_layout.addWidget(header)

    def _build_body(self, parent_layout: QVBoxLayout) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setChildrenCollapsible(False)

        control = QFrame()
        control.setObjectName("Panel")
        control_layout = QVBoxLayout(control)
        control_layout.setContentsMargins(12, 12, 12, 12)
        control_layout.setSpacing(10)

        symbol_row = QHBoxLayout()
        symbol_row.addWidget(QLabel("交易对"))
        self._symbol_input = QLineEdit("BTC-USDT-SWAP")
        symbol_row.addWidget(self._symbol_input, 1)
        control_layout.addLayout(symbol_row)

        period_row = QHBoxLayout()
        period_row.addWidget(QLabel("周期"))
        self._period_combo = QComboBox()
        self._period_combo.addItems(["1m", "5m", "15m", "1H", "4H", "1D"])
        self._period_combo.setCurrentText("15m")
        period_row.addWidget(self._period_combo, 1)
        control_layout.addLayout(period_row)

        button_row = QHBoxLayout()
        refresh_btn = QPushButton("加载")
        refresh_btn.setObjectName("Primary")
        refresh_btn.clicked.connect(lambda: self._set_status("加载中"))
        button_row.addWidget(refresh_btn)

        reset_btn = QPushButton("重置视图")
        reset_btn.clicked.connect(lambda: self._set_status("视图已重置"))
        button_row.addWidget(reset_btn)

        open_btn = QPushButton("打开独立图层窗口")
        open_btn.clicked.connect(lambda: self._set_status("未接入: 独立图层窗口"))
        button_row.addWidget(open_btn)
        control_layout.addLayout(button_row)

        control_layout.addWidget(QLabel("常用指标"))
        self._ema_fast = QCheckBox("EMA 9")
        self._ema_mid = QCheckBox("EMA 21")
        self._ema_slow = QCheckBox("EMA 55")
        self._ema_fast.setChecked(True)
        self._ema_mid.setChecked(True)
        self._ema_slow.setChecked(False)
        control_layout.addWidget(self._ema_fast)
        control_layout.addWidget(self._ema_mid)
        control_layout.addWidget(self._ema_slow)
        control_layout.addStretch(1)

        chart_host = QFrame()
        chart_host.setObjectName("Panel")
        chart_layout = QVBoxLayout(chart_host)
        chart_layout.setContentsMargins(12, 12, 12, 12)
        chart_layout.setSpacing(8)

        chart_hint = QLabel(
            "图表区域预留位：这里后续接入 TradingView Lightweight Charts，\n"
            "支持缩放、拖拽、十字、成交量、买卖信号和策略标注。"
        )
        chart_hint.setWordWrap(True)
        chart_hint.setObjectName("Subtle")
        chart_layout.addWidget(chart_hint)
        chart_layout.addStretch(1)

        splitter.addWidget(control)
        splitter.addWidget(chart_host)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 5)
        parent_layout.addWidget(splitter, 1)

    def _set_status(self, text: str) -> None:
        self._status.setText(text)
