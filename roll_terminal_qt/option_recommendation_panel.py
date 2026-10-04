"""Read-only, on-demand market suggestions inside the option calculator."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import time

from PySide6.QtCore import Qt, QThread, QTimer, Signal, Slot
from PySide6.QtWidgets import (
    QCheckBox, QGroupBox, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QVBoxLayout, QWidget,
)

from okx_quant.candle_cache import load_candle_cache
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.okx_client import OkxRestClient
from okx_quant.option_recommendations import (
    RecommendationSnapshot, StrategySuggestion, build_option_recommendations,
    DAY_MS, HOUR_MS, _closed_series, _daily_dvol_closes,
)
from okx_quant.option_strategy_ui import (
    _build_option_quote, _load_latest_deribit_option_chart_candles,
    _build_deribit_option_chart_candles, _load_deribit_hourly_series_from_cache,
    _save_deribit_hourly_series_to_cache,
)
from okx_quant.pricing import format_decimal


BJT = timezone(timedelta(hours=8))


def _time_label(ts: int | None) -> str:
    return datetime.fromtimestamp(ts / 1000, BJT).strftime("%m-%d %H:%M") if ts is not None else "-"


def _number(value, places: int = 2) -> str:
    return f"{value:,.{places}f}" if value is not None else "-"


def _load_recommendation_dvol(currency: str, now_ms: int):
    candles, _, note = _load_latest_deribit_option_chart_candles(
        currency, bar="1H", requested_limit=24 * 92, now_ts=now_ms,
    )
    recent = [c for c in _closed_series(candles, HOUR_MS, now_ms) if c.ts >= now_ms - 92 * DAY_MS]
    if len(_daily_dvol_closes(recent)) >= 90:
        return candles, note
    # The shared chart helper only updates the newest hours when a cache exists.
    # Explicitly backfill the bounded statistical window if that cache is short.
    try:
        history = DeribitRestClient().get_volatility_index_candles(
            currency, "3600", start_ts=max(0, now_ms - 92 * DAY_MS),
            end_ts=now_ms, max_records=24 * 92 + 2,
        )
        merged = {c.ts: c for c in _load_deribit_hourly_series_from_cache(currency)}
        merged.update({c.ts: c for c in history})
        raw = [merged[ts] for ts in sorted(merged)]
        if history:
            _save_deribit_hourly_series_to_cache(currency, raw)
        candles, _, _ = _build_deribit_option_chart_candles(raw, bar="1H", requested_limit=24 * 92)
        return candles, note + "；已尝试补齐90天统计历史"
    except Exception as exc:
        return candles, note + f"；统计历史补齐失败（{type(exc).__name__}），仍校验样本量"


class OptionRecommendationThread(QThread):
    snapshotReady = Signal(int, object)
    errorRaised = Signal(int, str)
    progressChanged = Signal(int, str)

    def __init__(self, request_id: int, family: str, parent=None) -> None:
        super().__init__(parent)
        self.request_id = request_id
        self.family = family

    def run(self) -> None:
        try:
            # A separate public client keeps analysis bookkeeping independent
            # of the calculator's position imports and of trading workers.
            client = OkxRestClient()
            currency = self.family.split("-", 1)[0]
            symbol = f"{currency}-USDT-SWAP"
            price_candles = {}
            notes: list[str] = []
            for period in ("1D", "4H", "1H", "15m"):
                if self.isInterruptionRequested():
                    return
                self.progressChanged.emit(self.request_id, f"读取 {symbol} {period}（日线UTC+8）...")
                try:
                    price_candles[period] = client.get_candles_history(symbol, period, limit=180)
                except Exception as exc:
                    price_candles[period] = load_candle_cache(symbol, period, limit=180)
                    notes.append(f"{period} 网络读取失败，使用本地缓存（{type(exc).__name__}）；仍检查时效。")
            if self.isInterruptionRequested():
                return
            self.progressChanged.emit(self.request_id, "读取DVOL及90天波动率背景...")
            dvol, dvol_note = _load_recommendation_dvol(currency, int(time.time() * 1000))
            if dvol_note:
                notes.append(dvol_note)
            if self.isInterruptionRequested():
                return
            self.progressChanged.emit(self.request_id, "核对期权到期日和双边报价...")
            quotes = []
            try:
                instruments = client.get_option_instruments(inst_family=self.family)
                if self.isInterruptionRequested():
                    return
                tickers = client.get_tickers("OPTION", inst_family=self.family)
                quote_now = int(time.time() * 1000)
                ticker_map = {}
                for ticker in tickers:
                    try:
                        quote_ts = int(ticker.raw.get("ts", 0))
                    except (ValueError, TypeError):
                        continue
                    if quote_ts <= 0 or not -60_000 <= quote_now - quote_ts <= 300_000:
                        continue
                    ticker_map[ticker.inst_id] = ticker
                quotes = [_build_option_quote(inst, ticker_map[inst.inst_id]) for inst in instruments
                          if inst.inst_id in ticker_map]
                notes.append("报价读取于 " + _time_label(quote_now) + " UTC+8；排除无时间戳、过期报价，未检查订单簿深度及保证金。")
            except Exception as exc:
                notes.append(f"期权报价读取失败（{type(exc).__name__}），仅展示结构，不提供具体合约。")
            if self.isInterruptionRequested():
                return
            snapshot = build_option_recommendations(
                family=self.family, price_candles=price_candles, dvol_hourly=dvol, quotes=quotes,
                now_ms=int(time.time() * 1000),
            )
            snapshot = replace(snapshot, notes=snapshot.notes + tuple(notes))
            self.snapshotReady.emit(self.request_id, snapshot)
        except Exception as exc:
            if not self.isInterruptionRequested():
                self.errorRaised.emit(self.request_id, f"{type(exc).__name__}: {exc}")


class OptionRecommendationPanel(QWidget):
    shutdownFinished = Signal()
    analysisRequested = Signal(object, object)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setStyleSheet(
            "QLabel { font-size: 13px; }"
            "QLabel#SectionTitle { font-size: 15px; font-weight: 600; }"
            "QLabel#Subtle { font-size: 12px; }"
            "QGroupBox { font-size: 13px; margin-top: 10px; padding-top: 8px; }"
        )
        self._family = "BTC-USD"
        self._request_id = 0
        self._worker: OptionRecommendationThread | None = None
        self._snapshot: RecommendationSnapshot | None = None
        self._closing = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        top = QHBoxLayout()
        heading = QLabel("行情策略推荐")
        heading.setObjectName("SectionTitle")
        top.addWidget(heading)
        top.addStretch(1)
        self._show_high_risk = QCheckBox("显示裸露卖方策略")
        self._show_high_risk.setToolTip("比例卖方、双卖存在未覆盖敞口，单独列为高风险观察。")
        self._show_high_risk.toggled.connect(self._render_snapshot)
        top.addWidget(self._show_high_risk)
        self._refresh_button = QPushButton("分析当前行情")
        self._refresh_button.clicked.connect(self.refresh)
        top.addWidget(self._refresh_button)
        layout.addLayout(top)
        self._status = QLabel()
        self._status.setWordWrap(True)
        layout.addWidget(self._status)
        self._market_label = QLabel()
        self._market_label.setWordWrap(True)
        self._market_label.setStyleSheet("QLabel { background: #e8f0fb; padding: 10px; border-radius: 6px; }")
        layout.addWidget(self._market_label)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        self._cards = QWidget()
        self._cards_layout = QVBoxLayout(self._cards)
        self._cards_layout.setContentsMargins(0, 0, 0, 0)
        scroll.setWidget(self._cards)
        layout.addWidget(scroll, 1)
        self._method_label = QLabel(
            "依据：期权策略执行体系、我的读书笔记。规则判断供策略比较，不是胜率或收益预测。\n"
            "使用已收盘普通K线和Deribit DVOL；点击“带入分析”可载入策略腿进行模拟，不会下单。"
        )
        self._method_label.setWordWrap(True)
        self._method_label.setObjectName("Subtle")
        layout.addWidget(self._method_label)
        self._freshness_timer = QTimer(self)
        self._freshness_timer.setInterval(60_000)
        self._freshness_timer.timeout.connect(self._refresh_freshness)
        self._freshness_timer.start()
        self.set_family(self._family)

    @Slot(str)
    def set_family(self, family: str) -> None:
        normalized = family.strip().upper()
        if normalized == self._family and self._status.text():
            return
        self._family = normalized
        self._request_id += 1
        self._snapshot = None
        if self._worker is not None:
            self._worker.requestInterruption()
        self._clear_cards()
        supported = normalized in {"BTC-USD", "ETH-USD"}
        self._status.setText(f"{normalized or '未选择系列'} | 点击“分析当前行情”读取行情并推荐候选策略。" if supported
                             else "当前规则支持BTC-USD、ETH-USD币本位期权，请切换期权系列。")
        self._market_label.setText("等待分析；先检查方向、波动率和到期时间，再选择结构。")
        self._refresh_button.setEnabled(supported and self._worker is None and not self._closing)

    @Slot()
    def refresh(self) -> None:
        if self._closing or self._worker is not None or self._family not in {"BTC-USD", "ETH-USD"}:
            return
        self._request_id += 1
        self._snapshot = None
        self._clear_cards()
        self._market_label.setText("正在读取四周期K线、DVOL和期权双边报价...")
        self._refresh_button.setEnabled(False)
        self._worker = OptionRecommendationThread(self._request_id, self._family, self)
        self._worker.snapshotReady.connect(self._apply_snapshot)
        self._worker.errorRaised.connect(self._apply_error)
        self._worker.progressChanged.connect(self._apply_progress)
        self._worker.finished.connect(self._worker_finished)
        self._worker.start()

    @Slot(int, str)
    def _apply_progress(self, request_id: int, message: str) -> None:
        if request_id == self._request_id and not self._closing:
            self._status.setText(message)

    @Slot(int, str)
    def _apply_error(self, request_id: int, message: str) -> None:
        if request_id == self._request_id and not self._closing:
            self._status.setText("本次分析失败：" + message)
            self._market_label.setText("未生成推荐；可检查网络后重新分析。")

    @Slot(int, object)
    def _apply_snapshot(self, request_id: int, snapshot: object) -> None:
        if (request_id != self._request_id or self._closing
                or not isinstance(snapshot, RecommendationSnapshot) or snapshot.family != self._family):
            return
        self._snapshot = snapshot
        self._render_snapshot()

    @Slot()
    def _worker_finished(self) -> None:
        thread = self.sender()
        if thread is self._worker:
            self._worker = None
        if thread is not None:
            thread.deleteLater()
        self._refresh_button.setEnabled(self._family in {"BTC-USD", "ETH-USD"} and not self._closing)
        if self._closing:
            self.shutdownFinished.emit()

    def _clear_cards(self) -> None:
        while self._cards_layout.count():
            item = self._cards_layout.takeAt(0)
            if item.widget() is not None:
                item.widget().deleteLater()

    @Slot()
    def _render_snapshot(self) -> None:
        snapshot = self._snapshot
        if snapshot is None:
            return
        self._clear_cards()
        self._status.setText(f"{snapshot.family} | 分析时间 {_time_label(snapshot.analyzed_at_ms)} UTC+8 | 手动刷新")
        trend_text = " / ".join(f"{t.period} {t.direction}（{_time_label(t.candle_ts)}）" for t in snapshot.trends)
        self._market_label.setText(
            f"{trend_text or 'K线数据不足'}\n"
            f"DVOL {_number(snapshot.dvol)} | 24h变化 {_number(snapshot.dvol_change_points)} 个波动率点 | "
            f"历史分位 {_number(snapshot.dvol_percentile)}%（{snapshot.history_days}个UTC+8完整日）\n"
            f"HV5 {_number(snapshot.realized_vol_5)}% / HV20 {_number(snapshot.realized_vol_20)}% | "
            f"DVOL小时线 {_time_label(snapshot.dvol_candle_ts)} UTC+8"
        )
        if snapshot.blockers:
            self._add_text_card("暂不推荐", "\n".join(snapshot.blockers))
        else:
            shown = [s for s in snapshot.suggestions if not s.high_risk or self._show_high_risk.isChecked()]
            for suggestion in shown:
                self._add_suggestion_card(suggestion)
            hidden = sum(s.high_risk for s in snapshot.suggestions) if not self._show_high_risk.isChecked() else 0
            if hidden:
                self._add_text_card("高风险观察", f"{hidden}个裸露卖方候选已折叠，可勾选上方选项查看风险和条件。")
            if not shown and not hidden:
                self._add_text_card("等待条件确认", "当前趋势与波动率条件未形成匹配结构，先观察。")
        self._add_text_card("数据与规则说明", "\n".join(snapshot.notes))
        self._cards_layout.addStretch(1)
        self._refresh_freshness()

    def _add_text_card(self, title: str, text: str) -> QGroupBox:
        card = QGroupBox(title)
        layout = QVBoxLayout(card)
        label = QLabel(text)
        label.setTextFormat(Qt.TextFormat.PlainText)
        label.setWordWrap(True)
        layout.addWidget(label)
        self._cards_layout.addWidget(card)
        return card

    def _add_suggestion_card(self, suggestion: StrategySuggestion) -> None:
        status = "结构已匹配，需人工核对" if suggestion.legs else "结构候选，等待完整报价"
        title = f"{suggestion.name} | {suggestion.dte_min}-{suggestion.dte_max} DTE | {status}"
        if suggestion.high_risk:
            title += " | 裸露卖方风险"
        text = (f"结构：{suggestion.structure}\n依据：{suggestion.reason}\n"
                f"退出/失效：{suggestion.exit_condition}\n风险：{suggestion.risk}\n")
        if suggestion.legs:
            text += f"到期日 {suggestion.expiry}（剩余 {_number(suggestion.dte, 1)} 天，UTC+8 16:00到期）\n"
            text += "\n".join(
                f"{'买入' if leg.side == 'buy' else '卖出'} ×{leg.ratio} | {leg.inst_id} | "
                f"参考权利金 {format_decimal(leg.reference_price)} {self._family.split('-')[0]}"
                for leg in suggestion.legs
            ) + "\n"
        text += f"{suggestion.contract_note}\n规则出处：{suggestion.source}"
        card = self._add_text_card(title, text)
        action_row = QHBoxLayout()
        hint = QLabel("分析数量 = 顶部默认数量（张）× 各腿比例；参考权利金用于模拟入场成本。")
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")
        action_row.addWidget(hint, 1)
        button = QPushButton("带入分析")
        button.setObjectName("ImportRecommendationButton")
        button.setEnabled(bool(suggestion.legs) and all(leg.quote is not None for leg in suggestion.legs))
        button.setToolTip("按顶部默认数量乘结构比例带入；已有策略腿时先确认替换，不会下单。" if button.isEnabled()
                          else "暂无完整合约及面值报价信息，请重新分析后再带入。")
        snapshot = self._snapshot
        button.clicked.connect(lambda _checked=False, snap=snapshot, item=suggestion: self._request_analysis(snap, item))
        action_row.addWidget(button)
        card.layout().insertLayout(0, action_row)

    def _request_analysis(self, snapshot: RecommendationSnapshot | None, suggestion: StrategySuggestion) -> None:
        if (self._closing or not isinstance(snapshot, RecommendationSnapshot)
                or snapshot is not self._snapshot or snapshot.family != self._family
                or suggestion not in snapshot.suggestions or snapshot.blockers or not suggestion.legs):
            return
        if int(time.time() * 1000) - snapshot.analyzed_at_ms > 300_000:
            self._status.setText("推荐结果已超过5分钟，请重新分析当前行情后再带入。")
            return
        self.analysisRequested.emit(snapshot, suggestion)

    @Slot()
    def _refresh_freshness(self) -> None:
        if self._snapshot is not None and int(time.time() * 1000) - self._snapshot.analyzed_at_ms > 300_000:
            self._status.setText(f"{self._family} | 此分析已超过5分钟，属于历史参考，请重新分析当前行情。")

    def shutdown(self) -> bool:
        if not self._closing:
            self._closing = True
            self._request_id += 1
        self._freshness_timer.stop()
        self._refresh_button.setEnabled(False)
        if self._worker is not None:
            self._worker.requestInterruption()
            return False
        return True

    def resume(self) -> None:
        self._closing = False
        self._freshness_timer.start()
        self._refresh_button.setEnabled(self._family in {"BTC-USD", "ETH-USD"} and self._worker is None)
        self._refresh_freshness()
