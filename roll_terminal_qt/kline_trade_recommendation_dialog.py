from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QGroupBox, QLabel, QVBoxLayout

from okx_quant.kline_trade_recommendations import KlineTradeIdea


def _price(value: Decimal | None) -> str:
    if value is None:
        return "—"
    places = 2 if value >= 100 else 4 if value >= 1 else max(8, 3-value.adjusted()) if value > 0 else 8
    return f"{value:,.{places}f}"


def _time(ts: int | None) -> str:
    return datetime.fromtimestamp(ts/1000, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M") if ts is not None else "—"


class KlineTradeRecommendationDialog(QDialog):
    """Read-only result. No RR creation, order controls, or background workers."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("K线做单参考（非期权）")
        self.setWindowModality(Qt.WindowModality.WindowModal)
        self.resize(820, 480)
        layout = QVBoxLayout(self)
        self._heading = QLabel()
        self._heading.setObjectName("SectionTitle")
        layout.addWidget(self._heading)
        self._context = QLabel()
        self._context.setWordWrap(True)
        layout.addWidget(self._context)
        self._reason = QLabel()
        self._reason.setWordWrap(True)
        self._reason.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self._reason)
        box = QGroupBox("参考计划 · 价格单位为交易对报价币")
        form = QFormLayout(box)
        self._values = {}
        for key, title in (("entry", "参考入场区间"), ("stop", "参考止损"), ("tp2", "参考止盈（2R）"),
                           ("tp3", "参考止盈（3R）"), ("rr", "计划盈亏比"), ("volatility", "ATR14 / ATR占价格")):
            label = QLabel("—")
            form.addRow(title, label)
            self._values[key] = label
        layout.addWidget(box)
        self._conditions = QLabel()
        self._conditions.setWordWrap(True)
        self._conditions.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self._conditions)
        disclaimer = QLabel(
            "使用所选交易对/周期的本地普通、已收盘K线，不使用平均或反转K线。\n"
            "工程默认规则：EMA15/SMA50、ATR14、0.15ATR入场容差、2R/3R计划目标，尚未经收益回测。\n"
            "2R/3R不是价格预测；均线价位及条件会随行情变化，执行前必须重新确认。未计费用、滑点、资金费和流动性；"
            "止损不保证成交价。合约还存在杠杆和强平风险。此页不下单、不设置仓位或杠杆。"
        )
        disclaimer.setObjectName("Subtle")
        disclaimer.setWordWrap(True)
        layout.addWidget(disclaimer)
        layout.addStretch(1)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def set_result(self, idea: KlineTradeIdea) -> None:
        self._heading.setText(f"{idea.symbol} | {idea.period} | {idea.action}")
        boundary = "UTC+8 日界" if idea.period.upper() == "1D" else "UTC 日界" if idea.period.upper() == "1DUTC" else "UTC+8 周一开周" if idea.period.upper() == "1W" else "时间显示UTC+8"
        self._context.setText(f"分析时间 {_time(idea.analyzed_at_ms)} UTC+8 | {boundary}\n"
                              f"最后已收盘K线开盘时间 {_time(idea.candle_ts)} UTC+8 | 收盘 {_price(idea.close)}")
        self._reason.setText(idea.status + "\n" + "\n".join(idea.reasons))
        self._values["entry"].setText(f"{_price(idea.entry_low)} — {_price(idea.entry_high)}" if idea.entry_low is not None else "—")
        self._values["stop"].setText(_price(idea.stop))
        self._values["tp2"].setText(_price(idea.target_2r))
        self._values["tp3"].setText(_price(idea.target_3r))
        self._values["rr"].setText(f"区间中点 1:2 / 1:3；较差入场端约 1:{idea.worst_rr:.2f}（2R目标）" if idea.worst_rr is not None else "—")
        self._values["volatility"].setText(f"{_price(idea.atr_value)} / {idea.atr_percent:.2f}%" if idea.atr_percent is not None else "—")
        self._conditions.setText("触发条件：" + (idea.trigger or "暂不新增交易，等待条件明确。") + "\n失效条件：" + (idea.invalidation or "请先补齐/刷新数据后重新分析。"))
