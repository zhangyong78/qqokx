"""Native Qt analysis cards and charts for the local trading report."""
from __future__ import annotations

import calendar
from datetime import date, datetime
from decimal import Decimal

from PySide6.QtCharts import QAreaSeries, QChart, QChartView, QDateTimeAxis, QLineSeries, QPieSeries, QValueAxis
from PySide6.QtCore import QDateTime, QMargins, Qt, Signal
from PySide6.QtGui import QBrush, QColor, QFont, QFontMetrics, QLinearGradient, QPainter, QPen
from PySide6.QtWidgets import (
    QComboBox, QFrame, QGridLayout, QHBoxLayout, QHeaderView, QLabel, QPushButton,
    QScrollArea, QSizePolicy, QTableWidget, QTableWidgetItem, QToolTip, QVBoxLayout, QWidget,
)

from okx_quant.daily_trade_report import DailyTradeReport, REPORT_TIMEZONE, _datetime, _decimal
from okx_quant.trade_analytics import (
    PRODUCT_NAMES, ZERO, completed_trades, cumulative_pnl, daily_pnl, equity_points,
    grouped_trades, metrics, opening_equity,
)
from okx_quant.phase2_analytics import cash_flow_summary, spot_cost_basis

GREEN = "#07836e"
RED = "#d44d73"
BLUE = "#4076d6"
INK = "#153248"
MUTED = "#7a8d9e"
PALETTE = (BLUE, "#16b8a6", "#f2b44c", "#916bdd", "#ef859a", "#a0adbb")

ANALYTICS_STYLE = """
QWidget#TradeReport { background: #f0f4f8; color: #153248; }
QWidget#TradeReport QLabel { background: transparent; color: #153248; }
QWidget#TradeReport QLabel[muted="true"] { color: #7a8d9e; }
QWidget#TradeReport QFrame[analyticsCard="true"] { background: white; border: 1px solid #e2eaf1; border-radius: 14px; }
QWidget#TradeReport QFrame#AnalyticsHero { background: #153248; border: 0; border-radius: 14px; }
QWidget#TradeReport QFrame#AnalyticsHero QLabel { color: #e5f3f9; }
QWidget#TradeReport QFrame#AnalyticsHero QLabel[muted="true"] { color: #b0c8d6; }
QWidget#TradeReport QScrollArea { border: 0; background: transparent; }
QWidget#TradeReport QWidget#AnalyticsCanvas { background: #f0f4f8; }
QWidget#TradeReport QTabWidget::pane { border: 0; background: #f0f4f8; }
QWidget#TradeReport QTabBar::tab { background: transparent; color: #6c8295; padding: 10px 19px; border: 0; }
QWidget#TradeReport QTabBar::tab:selected { color: #07836e; background: #e2f3f0; border-radius: 7px; }
QWidget#TradeReport QPushButton { background: #fff; color: #39556c; border: 1px solid #dae4ed; border-radius: 7px; padding: 6px 11px; }
QWidget#TradeReport QPushButton:hover { background: #e8f4f2; border-color: #83c8ba; }
QWidget#TradeReport QPushButton:checked { background: #dff2ec; color: #07836e; border-color: #07836e; }
QWidget#TradeReport QComboBox, QWidget#TradeReport QDateEdit { background: white; border: 1px solid #dae4ed; border-radius: 6px; padding: 5px; }
QWidget#TradeReport QTableWidget { background: white; alternate-background-color: #f6f9fc; border: 0; gridline-color: #edf2f6; selection-background-color: #e0f2ef; selection-color: #153248; }
QWidget#TradeReport QHeaderView::section { background: #f6f9fc; color: #678197; border: 0; padding: 9px 7px; }
"""


def number(value, suffix="", *, signed=False, places=2) -> str:
    if value is None:
        return "--"
    return f"{value:+,.{places}f}{suffix}" if signed else f"{value:,.{places}f}{suffix}"


def duration(seconds) -> str:
    if seconds is None:
        return "--"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} 分钟"
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    return f"{days}天 {hours}小时" if days else f"{hours}小时 {minutes}分"


def label(text="", *, size=None, bold=False, muted=False) -> QLabel:
    widget = QLabel(text)
    widget.setProperty("muted", muted)
    font = QFont(widget.font())
    if size:
        font.setPointSize(size)
    font.setBold(bold)
    widget.setFont(font)
    return widget


class Card(QFrame):
    def __init__(self, title: str, subtitle=""):
        super().__init__()
        self.setProperty("analyticsCard", True)
        self.box = QVBoxLayout(self)
        self.box.setContentsMargins(22, 18, 22, 18)
        self.box.setSpacing(10)
        self.heading = QHBoxLayout()
        self.heading.addWidget(label(title, size=12, bold=True))
        self.heading.addStretch()
        self.box.addLayout(self.heading)
        self.subtitle = label(subtitle, muted=True)
        self.subtitle.setWordWrap(True)
        if subtitle:
            self.box.addWidget(self.subtitle)


class MetricCard(Card):
    def __init__(self, title, subtitle="", *, hero=False):
        super().__init__(title, subtitle)
        if hero:
            self.setObjectName("AnalyticsHero")
        self.hero = hero
        self.value = label("--", size=25, bold=True)
        self.value.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self.box.insertWidget(1, self.value)
        self.setMinimumHeight(122)

    def set_value(self, text: str, value=None):
        self.value.setText(text)
        color = "#55debe" if self.hero and value is not None and value >= 0 else (
            "#ff99b2" if self.hero and value is not None else (RED if value is not None and value < 0 else GREEN if value is not None else INK))
        self.value.setStyleSheet(f"color: {color}; background: transparent;")
        self._fit_value()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fit_value()

    def _fit_value(self):
        font = QFont(self.value.font())
        width = max(100, self.width() - 44)
        for size in range(25, 12, -1):
            font.setPointSize(size)
            if QFontMetrics(font).horizontalAdvance(self.value.text()) <= width:
                break
        self.value.setFont(font)


class CurveCard(Card):
    def __init__(self, title: str, subtitle: str, color=GREEN):
        super().__init__(title, subtitle)
        self.color = color
        self._series_refs = []
        self.figure = label("--", size=23, bold=True)
        self.box.addWidget(self.figure)
        self.chart = QChart()
        self.chart.legend().hide()
        self.chart.setBackgroundVisible(False)
        self.chart.setMargins(QMargins(0, 8, 0, 0))
        self.view = QChartView(self.chart)
        self.view.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.view.setStyleSheet("background: transparent; border: 0;")
        self.view.setMinimumHeight(208)
        self.view.setMaximumHeight(240)
        self.box.addWidget(self.view)
        self.empty = label("尚无可用数据", muted=True)
        self.empty.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.empty.setMinimumHeight(180)
        self.empty.setWordWrap(True)
        self.box.addWidget(self.empty)

    def set_points(self, points, *, unit="U", empty_text="所选区间暂无已结算记录", gap_hours=None):
        self.chart.removeAllSeries()
        self._series_refs.clear()
        for axis in self.chart.axes():
            self.chart.removeAxis(axis)
            axis.deleteLater()
        self.view.setVisible(bool(points))
        self.empty.setVisible(not points)
        self.empty.setText(empty_text)
        self.figure.setText(number(points[-1][1], f" {unit}") if points else "--")
        if not points:
            return
        values = []
        for stamp, amount in points:
            if not isinstance(stamp, datetime):
                stamp = datetime.combine(stamp, datetime.min.time(), REPORT_TIMEZONE)
            values.append((int(stamp.timestamp() * 1000), float(amount)))
        x_axis, y_axis = QDateTimeAxis(), QValueAxis()
        x_axis.setFormat("MM/dd")
        x_axis.setTickCount(4)
        x_axis.setRange(QDateTime.fromMSecsSinceEpoch(values[0][0]), QDateTime.fromMSecsSinceEpoch(max(values[-1][0], values[0][0] + 86400000)))
        low, high = min(v for _, v in values), max(v for _, v in values)
        padding = max((high - low) * .18, abs(high) * .01, .01)
        y_axis.setRange(low - padding, high + padding)
        y_axis.setTickCount(4)
        y_axis.setLabelFormat("%.2f")
        for axis in (x_axis, y_axis):
            axis.setLabelsColor(QColor(MUTED))
            axis.setLineVisible(False)
            axis.setGridLinePen(QPen(QColor("#eaf0f5")))
        x_axis.setGridLineVisible(False)
        self.chart.addAxis(x_axis, Qt.AlignmentFlag.AlignBottom)
        self.chart.addAxis(y_axis, Qt.AlignmentFlag.AlignLeft)
        segments = [[]]
        for point in values:
            if segments[-1] and gap_hours and point[0] - segments[-1][-1][0] > gap_hours * 3600000:
                segments.append([])
            segments[-1].append(point)
        for segment in segments:
            line = QLineSeries()
            line.setPen(QPen(QColor(self.color), 2.3))
            line.setPointsVisible(len(values) < 40)
            for x, y in segment:
                line.append(x, y)
            baseline = QLineSeries()
            baseline.append(segment[0][0], low - padding)
            baseline.append(segment[-1][0], low - padding)
            area = QAreaSeries(line, baseline)
            tint = QColor(self.color)
            tint.setAlpha(40)
            gradient = QLinearGradient(0, 0, 0, 1)
            gradient.setCoordinateMode(QLinearGradient.CoordinateMode.ObjectBoundingMode)
            gradient.setColorAt(0, tint)
            tint.setAlpha(0)
            gradient.setColorAt(1, tint)
            area.setBrush(QBrush(gradient))
            area.setPen(QPen(Qt.PenStyle.NoPen))
            # Separate outline keeps the area baseline from becoming a visible line.
            outline = QLineSeries()
            outline.setPen(QPen(QColor(self.color), 2.3))
            outline.setPointsVisible(len(values) < 40)
            for x, y in segment:
                outline.append(x, y)
            for series in (area, outline):
                self.chart.addSeries(series)
                series.attachAxis(x_axis)
                series.attachAxis(y_axis)
            # QAreaSeries does not own its upper/lower QLineSeries. Keep their
            # Python wrappers alive until the area has been removed from QChart.
            self._series_refs.extend((line, baseline, area, outline))
            outline.hovered.connect(lambda point, entered, unit=unit: self._hover(point, entered, unit))

    def _hover(self, point, entered, unit):
        if entered:
            stamp = datetime.fromtimestamp(point.x() / 1000, REPORT_TIMEZONE)
            QToolTip.showText(self.view.mapToGlobal(self.chart.mapToPosition(point).toPoint()), f"{stamp:%Y-%m-%d %H:%M}\n{point.y():,.2f} {unit}", self.view)
        else:
            QToolTip.hideText()


class PnlCalendar(Card):
    day_clicked = Signal(object)

    def __init__(self, title="每日盈亏日历"):
        super().__init__(title, "北京时间 · 点击日期查看当日明细")
        self.month = date.today().replace(day=1)
        self.start = self.end = date.today()
        self.days = {}
        self.selected = None
        previous, following = QPushButton("‹"), QPushButton("›")
        previous.setFixedWidth(30)
        following.setFixedWidth(30)
        self.month_label = label()
        previous.clicked.connect(lambda: self._move(-1))
        following.clicked.connect(lambda: self._move(1))
        self.heading.addWidget(previous)
        self.heading.addWidget(self.month_label)
        self.heading.addWidget(following)
        self.grid = QGridLayout()
        self.grid.setSpacing(6)
        for col, text in enumerate(("一", "二", "三", "四", "五", "六", "日")):
            title_label = label(text, muted=True)
            title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.grid.addWidget(title_label, 0, col)
            self.grid.setColumnStretch(col, 1)
        self.buttons = []
        for index in range(42):
            button = QPushButton()
            button.setMinimumHeight(46)
            button.setMinimumWidth(0)
            button.clicked.connect(lambda _checked=False, index=index: self._clicked(index))
            self.grid.addWidget(button, index // 7 + 1, index % 7)
            self.buttons.append(button)
        self.box.addLayout(self.grid)

    def set_days(self, days, start, end):
        if (self.start, self.end) != (start, end):
            self.month = end.replace(day=1)
            self.selected = None
        self.days, self.start, self.end = days, start, end
        self._render()

    def _move(self, delta):
        offset = self.month.year * 12 + self.month.month - 1 + delta
        year, month = divmod(offset, 12)
        if 1 <= year <= 9999:
            self.month = date(year, month + 1, 1)
            self._render()

    def _render(self):
        self.month_label.setText(f"{self.month:%Y年%m月}")
        first, count = calendar.monthrange(self.month.year, self.month.month)
        for index, button in enumerate(self.buttons):
            day_number = index - first + 1
            if not 1 <= day_number <= count:
                button.setText("")
                button.setEnabled(False)
                button.setStyleSheet("background: transparent; border: 0;")
                continue
            day = self.month.replace(day=day_number)
            value = self.days.get(day)
            active = self.start <= day <= self.end
            caption = "今天" if day == datetime.now(REPORT_TIMEZONE).date() else str(day_number)
            if day in self.days:
                caption += "\n" + (number(value, signed=True) if value is not None else "待估值")
            button.setText(caption)
            button.setEnabled(active)
            color = GREEN if value is not None and value > 0 else RED if value is not None and value < 0 else MUTED
            background = "#e2f4ed" if value is not None and value > 0 else "#fce6ed" if value is not None and value < 0 else "#f5f8fb"
            border = "1px solid #07836e" if self.selected == day else "1px solid transparent"
            button.setStyleSheet(f"background: {background if active else 'transparent'}; color: {color if active else '#bcc7d0'}; border: {border}; border-radius: 7px; padding: 3px 0;")
            button.setToolTip(f"{day.isoformat()} · {number(value, ' USDT', signed=True) if day in self.days else '无已结算记录'}")

    def _clicked(self, index):
        first = calendar.monthrange(self.month.year, self.month.month)[0]
        self.selected = self.month.replace(day=index - first + 1)
        self._render()
        self.day_clicked.emit(self.selected)


class NumericItem(QTableWidgetItem):
    def __init__(self, text, value=None):
        super().__init__(text)
        self.value = value

    def __lt__(self, other):
        if isinstance(other, NumericItem) and (self.value is not None or other.value is not None):
            if self.value is None:
                return True
            if other.value is None:
                return False
            return self.value < other.value
        return self.text() < other.text()


def compact_table(headers):
    table = QTableWidget(0, len(headers))
    table.setHorizontalHeaderLabels(headers)
    table.verticalHeader().hide()
    table.setShowGrid(False)
    table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
    table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    table.verticalHeader().setDefaultSectionSize(37)
    table.setMinimumHeight(205)
    return table


class AnalysisDashboard(QScrollArea):
    day_activated = Signal(object)

    def __init__(self, *, contracts=False):
        super().__init__()
        self.contracts = contracts
        self.report = None
        self.all_trades = ()
        self.equity_records = []
        self.phase2_fills = ()
        self.phase2_bills = ()
        self.phase2_asset_bills = ()
        self.phase2_prices = {}
        self.selected_day = None
        self.setWidgetResizable(True)
        self.canvas = QWidget()
        self.canvas.setObjectName("AnalyticsCanvas")
        self.grid = QGridLayout(self.canvas)
        self.grid.setContentsMargins(0, 12, 8, 12)
        self.grid.setSpacing(16)
        for column in range(4):
            self.grid.setColumnStretch(column, 1)
        self.setWidget(self.canvas)
        self.notice = label("", muted=True)
        self.notice.setWordWrap(True)
        if contracts:
            self._build_contracts()
        else:
            self._build_overview()

    def _build_overview(self):
        self.period_card = MetricCard("区间已结算净盈亏", "USDT · 完整平仓口径", hero=True)
        self.today_card = MetricCard("今日已结算", "北京时间 · 独立于区间")
        self.equity_card = MetricCard("交易账户总权益", "账户级实时数据 · USD")
        self.return_card = MetricCard("已结算收益率", "净盈亏 / 期初账户权益 ≈")
        for index, card in enumerate((self.period_card, self.today_card, self.equity_card, self.return_card)):
            self.grid.addWidget(card, 0, index)
        self.pnl_curve = CurveCard("收益表现", "累计已结算净盈亏 · 缺少估值的记录不计入")
        self.mode = QComboBox()
        self.mode.addItems(("收益额", "收益率"))
        self.mode.currentIndexChanged.connect(self._render_curves)
        self.pnl_curve.heading.addWidget(self.mode)
        self.equity_curve = CurveCard("资产走势", "账户级权益 · USD · 本地小时快照", BLUE)
        self.grid.addWidget(self.pnl_curve, 1, 0, 1, 2)
        self.grid.addWidget(self.equity_curve, 1, 2, 1, 2)
        self.calendar = PnlCalendar()
        self.calendar.day_clicked.connect(self.day_activated.emit)
        self.products = Card("各产品盈亏", "所选区间 · USDT")
        self.product_values = {}
        for key, name in PRODUCT_NAMES.items():
            row = QHBoxLayout()
            row.addWidget(label(name, size=12))
            row.addStretch()
            value = label("--", size=15, bold=True)
            row.addWidget(value)
            self.products.box.addLayout(row)
            self.product_values[key] = value
        hint = label("现货成本按本地成交 FIFO 估算；账单同步后显示。", muted=True)
        hint.setWordWrap(True)
        self.products.box.addStretch()
        self.products.box.addWidget(hint)
        self.grid.addWidget(self.calendar, 2, 0, 1, 2)
        self.grid.addWidget(self.products, 2, 2, 1, 2)
        self.allocation = Card("资产分布", "当前交易账户 · 正权益占比 · USD")
        self.pie_chart = QChart()
        self.pie_chart.setBackgroundVisible(False)
        self.pie_chart.legend().hide()
        self.pie_view = QChartView(self.pie_chart)
        self.pie_view.setRenderHint(QPainter.RenderHint.Antialiasing)
        self.pie_view.setStyleSheet("background: transparent; border: 0;")
        self.pie_view.setFixedHeight(180)
        self.allocation.box.addWidget(self.pie_view)
        self.asset_legend = label("等待账户快照", muted=True)
        self.asset_legend.setWordWrap(True)
        self.allocation.box.addWidget(self.asset_legend)
        ranking = Card("币种盈亏排行", "所选区间 · 基础币种汇总")
        self.rank_mode = QComboBox()
        self.rank_mode.addItems(("盈利 Top 10", "亏损 Top 10"))
        self.rank_mode.currentIndexChanged.connect(self._render_ranking)
        ranking.heading.addWidget(self.rank_mode)
        self.rank_table = compact_table(("币种", "已结算净盈亏 / USDT"))
        ranking.box.addWidget(self.rank_table)
        self.grid.addWidget(self.allocation, 3, 0, 1, 2)
        self.grid.addWidget(ranking, 3, 2, 1, 2)
        self.cashflows = Card("现金流与对账", "账户账单 · 不把充值提现算成交易收益")
        self.cashflow_values = {}
        for key, name in (("充值", "充值"), ("提现", "提现"), ("内部划转", "内部划转"),
                          ("现货已实现", "现货 FIFO 已实现"), ("账单费用", "账单费用/资金费"),
                          ("资金流调整后", "资金流调整收益率")):
            row = QHBoxLayout()
            row.addWidget(label(name, size=12))
            row.addStretch()
            value = label("--", size=14, bold=True)
            row.addWidget(value)
            self.cashflows.box.addLayout(row)
            self.cashflow_values[key] = value
        self.cashflow_hint = label("等待账单同步", muted=True)
        self.cashflow_hint.setWordWrap(True)
        self.cashflows.box.addWidget(self.cashflow_hint)
        self.grid.addWidget(self.cashflows, 4, 0, 1, 2)
        self.grid.addWidget(self.notice, 5, 0, 1, 4)

    def _build_contracts(self):
        self.calendar = PnlCalendar("合约盈亏日历")
        self.calendar.day_clicked.connect(self._select_day)
        reset = QPushButton("恢复区间")
        reset.clicked.connect(lambda: self._select_day(None))
        self.calendar.heading.addWidget(reset)
        self.grid.addWidget(self.calendar, 0, 0, 2, 2)
        self.net_card = MetricCard("总净盈亏", "USDT · 完整平仓", hero=True)
        self.payoff_card = MetricCard("平均盈亏比", "平均盈利 / |平均亏损|")
        self.win_card = MetricCard("胜率 / 负率", "按已知盈亏的完整仓位")
        self.risk_card = MetricCard("平均风险回报", "每笔净盈亏 / 初始风险金")
        for index, card in enumerate((self.net_card, self.payoff_card, self.win_card, self.risk_card)):
            self.grid.addWidget(card, index // 2, 2 + index % 2)
        self.stats = Card("盈亏总览", "缺少时间、收益率或初始风险金的指标显示 --")
        self.stats_grid = QGridLayout()
        self.stats_grid.setHorizontalSpacing(22)
        self.stats_grid.setVerticalSpacing(13)
        self.stats.box.addLayout(self.stats_grid)
        self.stat_values = {}
        columns = (
            ("总盈利", "总亏损", "平均净盈亏", "平均盈利", "平均亏损"),
            ("平仓仓位数", "盈利 / 亏损 / 持平", "最大连续盈利", "最大连续亏损", "未估值仓位数"),
            ("平均持仓时间", "盈利平均持仓", "亏损平均持仓", "平均已实现收益率", "有效风险金样本"),
            ("总盈利 / 总亏损绝对值", "预期价值", "平均盈亏比", "统计范围", "口径"),
        )
        for column, names in enumerate(columns):
            for row, name in enumerate(names):
                cell = QVBoxLayout()
                cell.setSpacing(5)
                cell.addWidget(label(name, muted=True))
                value = label("--", size=12, bold=True)
                value.setWordWrap(True)
                cell.addWidget(value)
                self.stat_values[name] = value
                self.stats_grid.addLayout(cell, row, column)
            self.stats_grid.setColumnStretch(column, 1)
        self.grid.addWidget(self.stats, 2, 0, 1, 4)
        details = Card("合约盈亏明细", "点击表头排序 · 未知方向不归入多仓或空仓")
        self.contract_table = compact_table(("合约", "总盈亏 / U", "多仓盈亏 / U", "空仓盈亏 / U", "平仓数", "胜率"))
        self.contract_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.contract_table.setSortingEnabled(True)
        details.box.addWidget(self.contract_table)
        self.grid.addWidget(details, 3, 0, 1, 4)
        self.grid.addWidget(self.notice, 4, 0, 1, 4)

    def set_report(self, report: DailyTradeReport, all_trades, equity_records):
        if self.report is None or (report.start_date, report.end_date) != (self.report.start_date, self.report.end_date):
            self.selected_day = None
        self.report, self.all_trades, self.equity_records = report, all_trades, equity_records
        rows = completed_trades(report.trades, report.start_date, report.end_date, contracts_only=self.contracts)
        self.rows = rows
        self.calendar.set_days(daily_pnl(rows), report.start_date, report.end_date)
        missing = sum(t.net_pnl is None for t in rows)
        approximate = sum(bool(t.valuation_note) and t.net_pnl is not None for t in rows)
        partial = sum(not t.is_closed for t in report.trades)
        self.notice.setText(f"本地历史仓位：完整平仓 {len(rows)} 笔 · 参考汇率估值 {approximate} 笔 · 未估值 {missing} 笔 · 部分平仓 {partial} 笔未计入。\n按仓位结束日归集整个生命周期盈亏，非逐日盯市收益；统计范围以本地缓存为准。")
        if self.contracts:
            self._render_contracts()
        else:
            result = metrics(rows)
            self.period_card.set_value(number(result.net_pnl, " U", signed=True), result.net_pnl)
            today = datetime.now(REPORT_TIMEZONE).date()
            todays = completed_trades(all_trades, today, today)
            today_value = metrics(todays).net_pnl
            self.today_card.set_value(number(today_value, " U", signed=True), today_value)
            self.calendar.selected = None
            groups = grouped_trades(rows, "product")
            for key, widget in self.product_values.items():
                amount = metrics(groups.get(key, ())).net_pnl
                widget.setText(number(amount, " U", signed=True))
                widget.setStyleSheet(f"color: {RED if amount is not None and amount < 0 else GREEN if amount is not None else MUTED};")
            self._render_curves()
            self._render_ranking()

    def _render_curves(self):
        if self.report is None:
            return
        report = self.report
        points = cumulative_pnl(self.rows, report.start_date, report.end_date)
        base = opening_equity(self.equity_records, report.start_date)
        net = metrics(self.rows).net_pnl
        ratio = net / base * 100 if base is not None and net is not None else None
        self.return_card.set_value(number(ratio, "%", signed=True), ratio)
        reason = "缺少区间开始时的权益快照，无法计算期初权益基准收益率。"
        self.return_card.setToolTip(reason if base is None else "仅已结算净盈亏占期初账户权益的比例；USD≈USDT，不含浮盈，不是扣除出入金后的账户收益率。")
        if self.mode.currentIndex() == 1:
            points = [(stamp, value / base * 100) for stamp, value in points] if base else []
        self.pnl_curve.set_points(points, unit="%" if self.mode.currentIndex() else "U", empty_text=reason if self.mode.currentIndex() else "所选区间暂无可估值的完整平仓")
        curve = equity_points(self.equity_records, report.start_date, report.end_date)
        self.equity_curve.set_points(curve, unit="USD", empty_text="暂无权益采样。客户端连接账户后每小时保存一次。", gap_hours=2)
        stamps = [_datetime(record.get("time")) for record in self.equity_records]
        stamps = [stamp for stamp in stamps if stamp is not None]
        coverage = f"采样起点 {min(stamps):%Y-%m-%d %H:%M}" if stamps else "尚无本地采样"
        self.equity_curve.subtitle.setText(f"账户级 · {coverage} · 超过 2 小时缺口断开显示")

    def set_phase2_data(self, fills=(), bills=(), asset_bills=(), prices=None):
        if self.contracts:
            return
        self.phase2_fills = tuple(fills or ())
        self.phase2_bills = tuple(bills or ())
        self.phase2_asset_bills = tuple(asset_bills or ())
        self.phase2_prices = dict(prices or {})
        records = spot_cost_basis(self.phase2_fills, usdt_prices=self.phase2_prices)
        if self.report is not None:
            records = tuple(row for row in records if self.report.start_date <= row.close_time.astimezone(REPORT_TIMEZONE).date() <= self.report.end_date)
            bills = tuple(item for item in self.phase2_bills if (_datetime(getattr(item, "bill_time", None)) is not None and self.report.start_date <= _datetime(getattr(item, "bill_time", None)).date() <= self.report.end_date))
            asset_bills = tuple(item for item in self.phase2_asset_bills if (_datetime(getattr(item, "bill_time", None)) is not None and self.report.start_date <= _datetime(getattr(item, "bill_time", None)).date() <= self.report.end_date))
        else:
            bills, asset_bills = self.phase2_bills, self.phase2_asset_bills
        valued = [row.realized_pnl for row in records if row.realized_pnl is not None]
        spot_total = sum(valued, ZERO) if valued else None
        flow = cash_flow_summary(bills, asset_bills)
        self.cashflow_values["现货已实现"].setText(number(spot_total, " U", signed=True))
        def flow_text(values, *, negative=False):
            if not values:
                return "--"
            return " / ".join(
                f"{currency} {number(-amount if negative else amount, signed=True)}"
                for currency, amount in sorted(values.items())
            )
        self.cashflow_values["充值"].setText(flow_text(flow.deposits))
        self.cashflow_values["提现"].setText(flow_text(flow.withdrawals, negative=True))
        self.cashflow_values["内部划转"].setText(flow_text(flow.internal_transfers))
        self.cashflow_values["账单费用"].setText(flow_text(flow.fees))
        adjusted = None
        base = opening_equity(self.equity_records, self.report.start_date) if self.report is not None else None
        equity = equity_points(self.equity_records, self.report.start_date, self.report.end_date) if self.report is not None else []
        external = flow.external_net.get("USDT", ZERO)
        if base is not None and equity and base > 0 and (self.phase2_bills or self.phase2_asset_bills):
            adjusted = (equity[-1][1] - base - external) / base * 100
        self.cashflow_values["资金流调整后"].setText(number(adjusted, "%", signed=True))
        missing = sum(row.realized_pnl is None for row in records)
        self.cashflow_hint.setText(
            f"账单 {flow.row_count} 条 · 现货成交 {len(self.phase2_fills)} 条 · FIFO 已实现 {len(records)} 笔"
            + (f" · {missing} 笔缺少汇率/库存" if missing else "")
            + "。充值提现以账单原币展示，未混加进 USDT 盈亏；调整收益率仅在 USD/USDT 账单和权益采样完整时计算。"
        )
        widget = self.product_values.get("SPOT")
        if widget is not None:
            widget.setText(number(spot_total, " U", signed=True))
            widget.setStyleSheet(f"color: {RED if spot_total is not None and spot_total < 0 else GREEN if spot_total is not None else MUTED};")

    def _render_ranking(self):
        if self.report is None:
            return
        values = [(key, metrics(rows).net_pnl) for key, rows in grouped_trades(self.rows, "asset").items()]
        losses = self.rank_mode.currentIndex() == 1
        values = [(key, amount) for key, amount in values if amount is not None and (amount < 0 if losses else amount > 0)]
        values.sort(key=lambda item: item[1], reverse=not losses)
        self.rank_table.setRowCount(min(10, len(values)))
        for row, (key, amount) in enumerate(values[:10]):
            self.rank_table.setItem(row, 0, QTableWidgetItem(f"{row + 1:02d}   {key}"))
            item = NumericItem(number(amount, signed=True), amount)
            item.setForeground(QColor(RED if amount < 0 else GREEN))
            self.rank_table.setItem(row, 1, item)

    def set_account(self, account, *, updated_at=None, asset_filter="全部币种"):
        if self.contracts:
            return
        total = _decimal(getattr(account, "total_equity", None))
        self.equity_card.set_value(number(total, " USD"))
        stamp = updated_at.astimezone(REPORT_TIMEZONE).strftime("%m-%d %H:%M:%S") if updated_at else "时间未知"
        available = _decimal(getattr(account, "available_equity", None))
        upl = _decimal(getattr(account, "unrealized_pnl", None))
        self.equity_card.subtitle.setText(f"账户级 · {stamp}\n可用 {number(available)} / 浮盈 {number(upl, signed=True)} USD")
        self.pie_chart.removeAllSeries()
        assets = []
        for asset in getattr(account, "details", ()):
            amount = _decimal(getattr(asset, "equity_usd", None))
            if amount is not None and amount.is_finite() and amount > 0:
                assets.append((str(asset.ccy), amount))
        assets.sort(key=lambda item: item[1], reverse=True)
        total_positive = sum((value for _, value in assets), ZERO)
        primary, rest = [], ZERO
        for name, value in assets:
            if len(primary) < 5 and value / total_positive >= Decimal("0.01"):
                primary.append((name, value))
            else:
                rest += value
        if rest:
            primary.append(("其他", rest))
        series = QPieSeries()
        series.setPieSize(.94)
        series.setHoleSize(.64)
        texts = []
        for index, (name, amount) in enumerate(primary):
            color = PALETTE[index % len(PALETTE)]
            item = series.append(name, float(amount))
            item.setColor(QColor(color))
            item.setBorderColor(QColor("white"))
            item.setExploded(asset_filter == name)
            text = f"{name} · {amount / total_positive * 100:.2f}% · {amount:,.2f} USD"
            item.hovered.connect(lambda entered, text=text: self.asset_legend.setToolTip(text) if entered else None)
            texts.append(f'<span style="color:{color}">●</span> {text}')
        self.pie_chart.addSeries(series)
        self.pie_view.setVisible(bool(assets))
        self.asset_legend.setText("<br>".join(texts) if assets else "暂无可用资产快照")
        self.allocation.subtitle.setText("账户级实时分布 · 正权益占比（负债不进入圆环）")

    def _select_day(self, day):
        self.selected_day = day
        self.calendar.selected = day
        self.calendar._render()
        self._render_contracts()

    def _render_contracts(self):
        if self.report is None:
            return
        rows = [t for t in self.rows if self.selected_day is None or t.closed_at.astimezone(REPORT_TIMEZONE).date() == self.selected_day]
        result = metrics(rows)
        self.net_card.set_value(number(result.net_pnl, " U", signed=True), result.net_pnl)
        self.payoff_card.set_value(number(result.payoff_ratio))
        self.win_card.set_value(f"{number(result.win_rate, '%', places=1)} / {number(result.loss_rate, '%', places=1)}")
        self.risk_card.set_value(number(result.average_r, " R", signed=True), result.average_r)
        values = {
            "总盈利": number(result.profit, " U", signed=True), "总亏损": number(result.loss, " U", signed=True),
            "平均净盈亏": number(result.average_pnl, " U", signed=True), "平均盈利": number(result.average_win, " U", signed=True),
            "平均亏损": number(result.average_loss, " U", signed=True), "平仓仓位数": str(result.closed_count),
            "盈利 / 亏损 / 持平": f"{result.win_count} / {result.loss_count} / {result.flat_count}",
            "最大连续盈利": str(result.max_win_streak), "最大连续亏损": str(result.max_loss_streak),
            "未估值仓位数": str(result.closed_count - result.valued_count), "平均持仓时间": duration(result.average_seconds),
            "盈利平均持仓": duration(result.winning_seconds), "亏损平均持仓": duration(result.losing_seconds),
            "平均已实现收益率": number(result.average_return, "%", signed=True), "有效风险金样本": str(result.risk_count),
            "总盈利 / 总亏损绝对值": number(result.profit_factor), "预期价值": number(result.average_pnl, " U", signed=True),
            "平均盈亏比": number(result.payoff_ratio), "统计范围": str(self.selected_day) if self.selected_day else "当前日期区间",
            "口径": "完整平仓 · 净盈亏",
        }
        for key, value in values.items():
            self.stat_values[key].setText(value)
        groups = grouped_trades(rows, "symbol")
        header = self.contract_table.horizontalHeader()
        column, order = header.sortIndicatorSection(), header.sortIndicatorOrder()
        self.contract_table.setSortingEnabled(False)
        self.contract_table.setRowCount(len(groups))
        for row, (symbol, trades) in enumerate(sorted(groups.items())):
            summary = metrics(trades)
            longs = [t for t in trades if t.direction.lower() in {"long", "多", "只做多"}]
            shorts = [t for t in trades if t.direction.lower() in {"short", "空", "只做空"}]
            values = (summary.net_pnl, metrics(longs).net_pnl if longs else ZERO,
                      metrics(shorts).net_pnl if shorts else ZERO, summary.closed_count, summary.win_rate)
            self.contract_table.setItem(row, 0, QTableWidgetItem(symbol))
            for col, value in enumerate(values, start=1):
                text = str(value) if col == 4 else number(value, "%" if col == 5 else "", signed=col < 4)
                item = NumericItem(text, value)
                if col < 4 and value is not None:
                    item.setForeground(QColor(RED if value < 0 else GREEN))
                self.contract_table.setItem(row, col, item)
        self.contract_table.setSortingEnabled(True)
        self.contract_table.sortItems(max(0, column), order)
