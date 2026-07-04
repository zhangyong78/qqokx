from __future__ import annotations

from collections import Counter
import json
import re
import threading
import time
from datetime import datetime, timedelta
from decimal import Decimal
from tkinter import Tk
from typing import Callable

from PySide6.QtCore import QThread, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
    QTabWidget,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from roll_terminal_qt.app_icon import apply_qt_window_icon
from okx_quant.models import Candle, Credentials, Instrument, StrategyConfig
from okx_quant.option_roll import is_short_option_position
from okx_quant.option_roll_ui import OptionRollSuggestionWindow
from okx_quant.option_strategy_ui import OptionStrategyCalculatorWindow, _build_option_quote
from okx_quant.okx_client import (
    OkxFillHistoryItem,
    OkxOrderResult,
    OkxPosition,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTradeOrderItem,
)
from okx_quant.persistence import (
    load_account_positions_home_view_prefs,
    load_position_notes_snapshot,
    save_account_positions_home_view_prefs,
    save_position_notes_snapshot,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    ProtectionSessionSnapshot,
    derive_position_direction,
    describe_protection_price_logic,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
)
from okx_quant.pricing import format_decimal, snap_to_increment
from okx_quant.ui_shell import (
    _aggregate_position_metrics,
    _asset_group_row_id,
    _build_current_position_note_record,
    _build_group_detail_text,
    _build_group_row_values,
    _build_position_detail_text,
    _bucket_group_row_id,
    _filter_positions,
    _format_margin_mode,
    _format_optional_approx_usdt,
    _format_optional_decimal,
    _format_optional_decimal_fixed,
    _format_optional_integer,
    _format_optional_usdt,
    _format_optional_usdt_precise,
    _format_position_avg_price,
    _format_position_avg_price_usdt,
    _format_position_market_value,
    _format_position_note_summary,
    _format_position_option_component_usdt,
    _format_position_option_price_component,
    _format_position_quote_price,
    _format_position_quote_price_usdt,
    _format_position_realized_pnl,
    _format_position_size,
    _format_position_unrealized_pnl,
    _format_ratio,
    _format_mark_price,
    _format_position_mark_price_usdt,
    _format_option_trade_side_display,
    _format_okx_ms_timestamp,
    _group_positions_for_tree,
    _format_history_side,
    _normalize_position_note_text,
    _option_search_shortcuts,
    _format_trade_order_price,
    _format_trade_order_size,
    _format_trade_order_state,
    _format_trade_order_fee_cell,
    _build_trade_order_detail_text,
    _build_fill_history_detail_text,
    _build_history_position_note_record,
    _format_fill_history_exec_type,
    _format_fill_history_fee_cell,
    _format_fill_history_pnl,
    _format_fill_history_price,
    _format_fill_history_size,
    _format_position_history_fee_cell,
    _format_position_history_pnl,
    _format_position_history_filter_stats,
    _format_position_history_price,
    _format_position_history_size,
    _format_position_history_trade_side,
    _build_position_history_detail_text,
    _position_history_note_key,
    _position_history_note_summary_text,
    _position_delta_value,
    _position_note_current_key,
    _position_realized_pnl_usdt,
    _position_signed_open_value_approx_usdt,
    _position_theta_usdt,
    _position_tree_row_id,
    _position_unrealized_pnl_usdt,
    _reconcile_current_position_note_records,
    _format_protection_order_mode_label,
    _format_protection_order_price_detail,
    _format_protection_trigger_price_type,
    _resolve_protection_order_mode_value,
    _validate_protection_live_price_availability,
    _validate_protection_price_relationship,
    PROTECTION_ORDER_MODE_OPTIONS,
    PROTECTION_TRIGGER_SOURCE_OPTIONS,
)
from roll_terminal_qt.account_service import AccountFeedThread
from roll_terminal_qt.history_service import FillHistoryFeedThread, OrderHistoryFeedThread, PositionHistoryFeedThread
from roll_terminal_qt.option_strategy_window import CandlestickChartView
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots, profile_requires_password
from roll_terminal_qt.runtime import load_runtime, profile_names


POSITION_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("鍏ㄩ儴绫诲瀷", ""),
    ("浜ゅ壊鍚堢害 FUTURES", "FUTURES"),
    ("姘哥画 SWAP", "SWAP"),
    ("鏈熸潈 OPTION", "OPTION"),
)

POSITION_COLUMNS: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...] = (
    ("inst_type", "绫诲瀷", 72, Qt.AlignmentFlag.AlignCenter),
    ("mgn_mode", "淇濊瘉閲戞ā寮?, 92, Qt.AlignmentFlag.AlignCenter),
    ("time_value", "鏃堕棿浠峰€?, 88, Qt.AlignmentFlag.AlignRight),
    ("time_value_usdt", "鏃堕棿鈮圲SDT", 72, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_value", "鍐呭湪浠峰€?, 88, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_usdt", "鍐呭湪鈮圲SDT", 72, Qt.AlignmentFlag.AlignRight),
    ("bid_price", "涔颁竴浠?, 78, Qt.AlignmentFlag.AlignRight),
    ("bid_usdt", "涔颁竴鈮圲SDT", 78, Qt.AlignmentFlag.AlignRight),
    ("ask_price", "鍗栦竴浠?, 78, Qt.AlignmentFlag.AlignRight),
    ("ask_usdt", "鍗栦竴鈮圲SDT", 78, Qt.AlignmentFlag.AlignRight),
    ("mark", "鏍囪浠?, 84, Qt.AlignmentFlag.AlignRight),
    ("mark_usdt", "鏍囪鈮圲SDT", 72, Qt.AlignmentFlag.AlignRight),
    ("avg", "寮€浠撲环", 84, Qt.AlignmentFlag.AlignRight),
    ("avg_usdt", "寮€浠撯増USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("open_value_usdt", "寮€浠撲环鍊尖増USDT", 116, Qt.AlignmentFlag.AlignRight),
    ("pos", "鎸佷粨閲?, 170, Qt.AlignmentFlag.AlignRight),
    ("option_side", "涔拌喘:鍗栬喘 | 涔版步:鍗栨步", 170, Qt.AlignmentFlag.AlignCenter),
    ("upl", "娴泩浜?, 168, Qt.AlignmentFlag.AlignRight),
    ("upl_usdt", "娴泩鈮圲SDT", 108, Qt.AlignmentFlag.AlignRight),
    ("realized", "宸插疄鐜扮泩浜?, 118, Qt.AlignmentFlag.AlignRight),
    ("realized_usdt", "宸插疄鐜扳増USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("market_value", "甯傚€?, 160, Qt.AlignmentFlag.AlignRight),
    ("liq", "寮哄钩浠?, 92, Qt.AlignmentFlag.AlignRight),
    ("mgn_ratio", "淇濊瘉閲戠巼", 88, Qt.AlignmentFlag.AlignRight),
    ("imr", "鍒濆淇濊瘉閲?, 100, Qt.AlignmentFlag.AlignRight),
    ("mmr", "缁存寔淇濊瘉閲?, 100, Qt.AlignmentFlag.AlignRight),
    ("delta", "Delta(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("gamma", "Gamma(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("vega", "Vega(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("theta", "Theta(PA)", 108, Qt.AlignmentFlag.AlignRight),
    ("theta_usdt", "Theta鈮圲SDT", 108, Qt.AlignmentFlag.AlignRight),
    ("note", "澶囨敞", 200, Qt.AlignmentFlag.AlignLeft),
)

DEFAULT_VISIBLE_COLUMNS: tuple[str, ...] = (
    "inst_type",
    "mgn_mode",
    "mark",
    "mark_usdt",
    "avg",
    "avg_usdt",
    "open_value_usdt",
    "pos",
    "option_side",
    "upl",
    "upl_usdt",
    "realized",
    "market_value",
    "liq",
    "mgn_ratio",
    "imr",
    "mmr",
    "delta",
    "gamma",
    "vega",
    "theta",
    "theta_usdt",
    "note",
)

# Qt 鎸佷粨棣栭〉榛樿鎸夊綋鍓嶄汉宸ユ牎鍑嗗悗鐨勫垪闆嗗拰鍒楀鍚姩锛涘悗缁敤鎴锋嫋鎷藉垪瀹?鍒楄缃粛浼氬啓鍏ユ湰鍦板亸濂借鐩栬繖閲屻€?DEFAULT_VISIBLE_COLUMNS = (
    "ask_price",
    "ask_usdt",
    "avg",
    "avg_usdt",
    "bid_price",
    "bid_usdt",
    "delta",
    "gamma",
    "intrinsic_usdt",
    "intrinsic_value",
    "mark",
    "mark_usdt",
    "market_value",
    "mgn_ratio",
    "note",
    "open_value_usdt",
    "option_side",
    "pos",
    "realized",
    "theta",
    "theta_usdt",
    "time_value",
    "time_value_usdt",
    "upl",
    "upl_usdt",
    "vega",
)

DEFAULT_TREE_LABEL_WIDTH = 221
DEFAULT_TREE_COLUMN_WIDTHS: dict[str, int] = {
    "time_value": 69,
    "time_value_usdt": 72,
    "intrinsic_value": 63,
    "intrinsic_usdt": 66,
    "bid_price": 66,
    "bid_usdt": 71,
    "ask_price": 64,
    "ask_usdt": 69,
    "mark": 56,
    "mark_usdt": 63,
    "avg": 66,
    "avg_usdt": 72,
    "open_value_usdt": 102,
    "pos": 154,
    "option_side": 170,
    "upl": 164,
    "upl_usdt": 72,
    "realized": 102,
    "market_value": 149,
    "mgn_ratio": 55,
    "delta": 84,
    "gamma": 69,
    "vega": 70,
    "theta": 71,
    "theta_usdt": 75,
    "note": 457,
}


def _format_account_level_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    mapping = {
        "1": "绠€鍗曚氦鏄?,
        "2": "鍗曞竵绉嶄繚璇侀噾",
        "3": "璺ㄥ竵绉嶄繚璇侀噾",
        "4": "缁勫悎淇濊瘉閲?,
    }
    return mapping.get(text, text)


def _format_account_position_mode_text(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "-"
    if text == "net":
        return "鍑€鎸佷粨 net"
    if text in {"long_short", "long/short", "long_short_mode"}:
        return "鍙屽悜鎸佷粨 long/short"
    return text


def _format_greeks_type_text(value: str | None) -> str:
    text = str(value or "").strip().upper()
    return text or "-"


def _format_bool_text(value: bool | None) -> str:
    if value is None:
        return "-"
    return "鏄? if value else "鍚?

ORDER_SOURCE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("鍏ㄩ儴鏉ユ簮", ""),
    ("鏅€氬鎵?, "normal"),
    ("绠楁硶濮旀墭", "algo"),
    ("WS 褰撳墠", "ws"),
)

ORDER_STATE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("鍏ㄩ儴鐘舵€?, ""),
    ("绛夊緟涓?, "live"),
    ("閮ㄥ垎鎴愪氦", "partially_filled"),
    ("宸叉垚浜?, "filled"),
    ("宸叉挙鍗?, "canceled"),
    ("澶辫触", "order_failed"),
)

HISTORY_FILL_SIDE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("鍏ㄩ儴鏂瑰悜", ""),
    ("涔板叆", "buy"),
    ("鍗栧嚭", "sell"),
    ("澶氬ご", "long"),
    ("绌哄ご", "short"),
)

HISTORY_MARGIN_MODE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("鍏ㄩ儴妯″紡", ""),
    ("鍏ㄤ粨", "cross"),
    ("閫愪粨", "isolated"),
    ("鐜伴噾", "cash"),
)


def _history_expiry_filter_matches(inst_id: str, expiry_filter: str) -> bool:
    text = expiry_filter.strip().upper().strip("-")
    if not text:
        return True
    normalized = inst_id.strip().upper().strip("-")
    if normalized.startswith(text):
        return True
    parts = normalized.split("-")
    if len(parts) >= 3 and re.fullmatch(r"\d{6,8}", parts[2] or ""):
        expiry = parts[2]
        family_prefix = f"{parts[0]}-{parts[1]}-{expiry}"
        return expiry.startswith(text) or family_prefix.startswith(text)
    return False


POSITION_KLINE_BAR_OPTIONS: tuple[tuple[str, str], ...] = (
    ("15鍒嗛挓", "15m"),
    ("1灏忔椂", "1H"),
    ("4灏忔椂", "4H"),
    ("1澶?, "1D"),
)


class NoteEditorDialog(QDialog):
    def __init__(self, *, title: str, prompt: str, initial_value: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self._result_text: str | None = None
        self.setWindowTitle(title)
        self.resize(520, 320)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        layout.addWidget(QLabel(prompt))

        self._editor = QTextEdit()
        self._editor.setPlainText(initial_value)
        layout.addWidget(self._editor, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("淇濆瓨")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("鍙栨秷")
        layout.addWidget(buttons)

    @property
    def result_text(self) -> str | None:
        return self._result_text

    def _accept(self) -> None:
        self._result_text = _normalize_position_note_text(self._editor.toPlainText())
        self.accept()


class AccountOverviewDialog(QDialog):
    def __init__(self, *, summary_text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("璐︽埛淇℃伅")
        self.resize(920, 680)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        title = QLabel("璐︽埛鎸佷粨姒傝")
        title.setObjectName("SectionTitle")
        layout.addWidget(title)

        detail = QTextEdit()
        detail.setReadOnly(True)
        detail.setPlainText(summary_text)
        layout.addWidget(detail, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("鍏抽棴")
        layout.addWidget(buttons)


class InstrumentKlineLoadThread(QThread):
    loaded = Signal(str, str, str, str, object)
    failed = Signal(str, str, str, str)

    def __init__(self, *, inst_id: str, inst_type: str, bar: str, limit: int = 240) -> None:
        super().__init__()
        self._inst_id = inst_id.strip().upper()
        self._inst_type = inst_type.strip().upper()
        self._bar = bar.strip()
        self._limit = max(60, limit)

    def run(self) -> None:
        source = "mark" if self._inst_type == "OPTION" else "trade"
        try:
            client = OkxRestClient()
            if source == "mark":
                candles = client.get_mark_price_candles(self._inst_id, self._bar, limit=self._limit)
            else:
                candles = client.get_candles_history(self._inst_id, self._bar, limit=self._limit)
            if not candles:
                raise ValueError("褰撳墠鍛ㄦ湡娌℃湁鍙敤 K 绾挎暟鎹€?)
            self.loaded.emit(self._inst_id, self._inst_type, self._bar, source, candles)
        except Exception as exc:  # noqa: BLE001
            self.failed.emit(self._inst_id, self._inst_type, self._bar, str(exc))


class InstrumentKlineDialog(QDialog):
    def __init__(
        self,
        *,
        initial_bar: str = "1H",
        initial_width: int = 1280,
        initial_height: int = 760,
        prefs_changed: Callable[[str, int, int], None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("鍚堢害 K 绾垮浘")
        self.resize(max(int(initial_width), 480), max(int(initial_height), 320))

        self._inst_id = ""
        self._inst_type = ""
        self._current_bar = initial_bar if initial_bar in {bar for _text, bar in POSITION_KLINE_BAR_OPTIONS} else "1H"
        self._load_thread: InstrumentKlineLoadThread | None = None
        self._bar_buttons: dict[str, QPushButton] = {}
        self._prefs_changed = prefs_changed

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        header = QHBoxLayout()
        header.setSpacing(8)
        self._title_label = QLabel("绛夊緟閫夋嫨鎸佷粨")
        self._title_label.setObjectName("SectionTitle")
        self._status_label = QLabel("鐐瑰嚮鎸佷粨鍚庤嚜鍔ㄥ姞杞藉搴?K 绾裤€?)
        self._status_label.setObjectName("Subtle")
        header.addWidget(self._title_label, 1)
        header.addWidget(self._status_label, 2)
        layout.addLayout(header)

        bar_row = QHBoxLayout()
        bar_row.setSpacing(8)
        bar_row.addWidget(QLabel("鍛ㄦ湡"))
        for text, bar in POSITION_KLINE_BAR_OPTIONS:
            button = QPushButton(text)
            button.setCheckable(True)
            button.clicked.connect(lambda _checked=False, target_bar=bar: self._select_bar(target_bar))
            self._bar_buttons[bar] = button
            bar_row.addWidget(button)
        bar_row.addStretch(1)
        layout.addLayout(bar_row)

        self._chart = CandlestickChartView()
        self._chart.show_message("璇风偣鍑讳竴鏉℃寔浠撳姞杞?K 绾?)
        layout.addWidget(self._chart, 1)
        self._sync_bar_buttons()

    def show_instrument(self, *, inst_id: str, inst_type: str) -> None:
        self._inst_id = inst_id.strip().upper()
        self._inst_type = inst_type.strip().upper()
        self._update_title()
        self._load_current_bar()
        self.show()
        self.raise_()
        self.activateWindow()

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._load_thread is not None and self._load_thread.isRunning():
            self._load_thread.requestInterruption()
            self._load_thread.wait(1500)
        self._emit_prefs_changed()
        super().closeEvent(event)

    def resizeEvent(self, event) -> None:  # noqa: ANN001
        super().resizeEvent(event)
        self._emit_prefs_changed()

    def _select_bar(self, bar: str) -> None:
        self._current_bar = bar
        self._sync_bar_buttons()
        self._emit_prefs_changed()
        self._load_current_bar()

    def _sync_bar_buttons(self) -> None:
        for bar, button in self._bar_buttons.items():
            checked = bar == self._current_bar
            button.blockSignals(True)
            button.setChecked(checked)
            button.blockSignals(False)
            button.setObjectName("Primary" if checked else "")
            button.style().unpolish(button)
            button.style().polish(button)

    def _update_title(self) -> None:
        if not self._inst_id:
            self._title_label.setText("绛夊緟閫夋嫨鎸佷粨")
            return
        source_label = "鏍囪浠锋牸K绾? if self._inst_type == "OPTION" else "鎴愪氦浠锋牸K绾?
        self._title_label.setText(f"{self._inst_id} | {source_label}")

    @Slot()
    def _load_current_bar(self) -> None:
        if not self._inst_id:
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        source_label = "鏍囪浠锋牸" if self._inst_type == "OPTION" else "鎴愪氦浠锋牸"
        self._status_label.setText(f"姝ｅ湪鍔犺浇 {self._inst_id} {self._current_bar} {source_label} K 绾?..")
        self._load_thread = InstrumentKlineLoadThread(
            inst_id=self._inst_id,
            inst_type=self._inst_type,
            bar=self._current_bar,
        )
        self._load_thread.loaded.connect(self._apply_loaded_candles)
        self._load_thread.failed.connect(self._apply_load_error)
        self._load_thread.finished.connect(self._clear_finished_thread)
        self._load_thread.start()

    @Slot(str, str, str, str, object)
    def _apply_loaded_candles(
        self,
        inst_id: str,
        inst_type: str,
        bar: str,
        source: str,
        candles: object,
    ) -> None:
        if inst_id != self._inst_id or inst_type != self._inst_type or bar != self._current_bar:
            return
        if not isinstance(candles, list):
            return
        source_label = "鏍囪浠锋牸" if source == "mark" else "鎴愪氦浠锋牸"
        self._chart.set_candles(title=f"{inst_id} {source_label}K绾?| {bar}", candles=candles)
        latest = candles[-1] if candles else None
        latest_text = ""
        latest_time_text = ""
        if isinstance(latest, Candle):
            latest_text = f" | 鏈€鏂?{latest.close}"
            latest_time_text = f" | 鏃堕棿 {datetime.fromtimestamp(latest.ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
        self._status_label.setText(f"{inst_id} | {bar} | {source_label} K 绾垮凡鍔犺浇{latest_text}{latest_time_text}")

    @Slot(str, str, str, str)
    def _apply_load_error(self, inst_id: str, inst_type: str, bar: str, message: str) -> None:
        if inst_id != self._inst_id or inst_type != self._inst_type or bar != self._current_bar:
            return
        self._chart.show_message(f"{inst_id} K 绾垮姞杞藉け璐?)
        self._status_label.setText(f"K 绾垮姞杞藉け璐ワ細{message}")

    @Slot()
    def _clear_finished_thread(self) -> None:
        if self._load_thread is not None:
            self._load_thread.deleteLater()
            self._load_thread = None

    def _emit_prefs_changed(self) -> None:
        if self._prefs_changed is None:
            return
        try:
            self._prefs_changed(self._current_bar, self.width(), self.height())
        except Exception:
            return


class ColumnSettingsDialog(QDialog):
    def __init__(
        self,
        *,
        column_defs: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...],
        visible_columns: set[str],
        toggle_callback,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("鎸佷粨澶х獥鍒楄缃?)
        self.resize(560, 520)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        tip = QLabel("鍙寜鍖哄煙鍕鹃€夋樉绀?闅愯棌鍒椼€俙鍚堢害 / 鍒嗙粍` 涓虹粨鏋勫垪锛屽綋鍓嶅浐瀹氭樉绀恒€?)
        tip.setObjectName("Subtle")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        checks = QGridLayout()
        checks.setHorizontalSpacing(16)
        checks.setVerticalSpacing(8)
        for index, (column_id, heading, _width, _alignment) in enumerate(column_defs):
            checkbox = QCheckBox(heading)
            checkbox.setChecked(column_id in visible_columns)
            checkbox.stateChanged.connect(lambda _state, cid=column_id: toggle_callback(cid))
            checks.addWidget(checkbox, index // 2, index % 2)
        wrapper = QWidget()
        wrapper.setLayout(checks)
        layout.addWidget(wrapper, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("鍏抽棴")
        layout.addWidget(buttons)


class LegacyOptionToolsHost:
    def __init__(
        self,
        *,
        parent: QWidget,
        runtime_provider: Callable[[], object | None],
    ) -> None:
        self._parent = parent
        self._runtime_provider = runtime_provider
        self._client = OkxRestClient()
        self._root: Tk | None = None
        self._pump_timer: QTimer | None = None
        self._option_roll_window: OptionRollSuggestionWindow | None = None
        self._option_strategy_window: OptionStrategyCalculatorWindow | None = None

    def shutdown(self) -> None:
        if self._pump_timer is not None:
            self._pump_timer.stop()
            self._pump_timer.deleteLater()
            self._pump_timer = None
        if self._option_roll_window is not None:
            try:
                self._option_roll_window.destroy()
            except Exception:
                pass
            self._option_roll_window = None
        if self._option_strategy_window is not None:
            try:
                self._option_strategy_window.destroy()
            except Exception:
                pass
            self._option_strategy_window = None
        if self._root is not None:
            try:
                self._root.destroy()
            except Exception:
                pass
            self._root = None

    def open_option_roll(
        self,
        *,
        position: OkxPosition,
        instrument: object,
        ticker: object,
        api_name: str,
    ) -> None:
        root = self._ensure_root()
        if root is None:
            raise RuntimeError("Tk 妗ユ帴绐楀彛鍒濆鍖栧け璐ャ€?)
        quote = _build_option_quote(instrument, ticker)

        def _send_to_strategy(payload: object) -> None:
            if self._option_strategy_window is None or not self._option_strategy_window.window.winfo_exists():
                self._option_strategy_window = OptionStrategyCalculatorWindow(
                    root,
                    self._client,
                    runtime_provider=self._runtime_provider,
                    logger=None,
                )
            self._option_strategy_window.load_roll_transfer_payload(payload)

        if self._option_roll_window is not None and self._option_roll_window.window.winfo_exists():
            self._option_roll_window.load_position(
                position=position,
                instrument=instrument,
                quote=quote,
                api_name=api_name,
                auto_scan=True,
            )
            self._option_roll_window.show()
            return

        self._option_roll_window = OptionRollSuggestionWindow(
            root,
            self._client,
            position=position,
            instrument=instrument,
            quote=quote,
            api_name=api_name,
            send_to_strategy_callback=_send_to_strategy,
            logger=None,
        )

    def _ensure_root(self) -> Tk | None:
        if self._root is not None:
            return self._root
        try:
            root = Tk()
            root.withdraw()
        except Exception:
            return None
        self._root = root
        self._pump_timer = QTimer(self._parent)
        self._pump_timer.timeout.connect(self._pump_events)
        self._pump_timer.start(40)
        return root

    @Slot()
    def _pump_events(self) -> None:
        if self._root is None:
            return
        try:
            self._root.update_idletasks()
            self._root.update()
        except Exception:
            self.shutdown()


class PositionProtectionDialog(QDialog):
    def __init__(
        self,
        *,
        manager: PositionProtectionManager,
        client: OkxRestClient,
        runtime_provider: Callable[[], object | None],
        selected_option_provider: Callable[[], OkxPosition | None],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self._manager = manager
        self._client = client
        self._runtime_provider = runtime_provider
        self._selected_option_provider = selected_option_provider
        self._selected_position: OkxPosition | None = None
        self._form_position_key = ""
        self._session_ids: list[str] = []
        self._last_fixed_price_memory = {"tp": "", "sl": ""}

        self.setWindowTitle("璁剧疆鏈熸潈淇濇姢")
        self.resize(1080, 760)

        self._build_ui()
        self._refresh_from_selection(force=True)
        self._refresh_sessions()

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._refresh_sessions)
        self._refresh_timer.timeout.connect(lambda: self._refresh_from_selection(force=False))
        self._refresh_timer.start(1200)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        top_panel = QFrame()
        top_panel.setObjectName("Panel")
        top_layout = QVBoxLayout(top_panel)
        top_layout.setContentsMargins(12, 12, 12, 12)
        top_layout.setSpacing(10)

        self._title_label = QLabel("璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉℃湡鏉冧粨浣嶃€?)
        self._title_label.setObjectName("SectionTitle")
        self._title_label.setWordWrap(True)
        self._logic_hint = QLabel("淇濇姢閫昏緫浼氳窡闅忎笂鏂归€変腑鐨勬湡鏉冧粨浣嶃€?)
        self._logic_hint.setObjectName("Subtle")
        self._logic_hint.setWordWrap(True)
        top_layout.addWidget(self._title_label)
        top_layout.addWidget(self._logic_hint)

        form = QGridLayout()
        form.setHorizontalSpacing(10)
        form.setVerticalSpacing(10)

        self._trigger_combo = QComboBox()
        for label in PROTECTION_TRIGGER_SOURCE_OPTIONS:
            self._trigger_combo.addItem(label)
        self._trigger_combo.currentIndexChanged.connect(self._on_trigger_source_changed)

        self._spot_symbol_edit = QLineEdit()
        self._tp_trigger_edit = QLineEdit()
        self._sl_trigger_edit = QLineEdit()
        self._tp_mode_combo = QComboBox()
        self._sl_mode_combo = QComboBox()
        for label in PROTECTION_ORDER_MODE_OPTIONS:
            self._tp_mode_combo.addItem(label)
            self._sl_mode_combo.addItem(label)
        self._tp_mode_combo.currentIndexChanged.connect(self._refresh_order_mode_widgets)
        self._sl_mode_combo.currentIndexChanged.connect(self._refresh_order_mode_widgets)
        self._tp_price_edit = QLineEdit()
        self._sl_price_edit = QLineEdit()
        self._tp_slippage_edit = QLineEdit("0")
        self._sl_slippage_edit = QLineEdit("0")
        self._poll_seconds_edit = QLineEdit("2")

        form.addWidget(QLabel("瑙﹀彂鏉′欢"), 0, 0)
        form.addWidget(self._trigger_combo, 0, 1)
        form.addWidget(QLabel("鐜拌揣鏍囩殑"), 0, 2)
        form.addWidget(self._spot_symbol_edit, 0, 3)
        form.addWidget(QLabel("姝㈢泩瑙﹀彂浠?), 1, 0)
        form.addWidget(self._tp_trigger_edit, 1, 1)
        form.addWidget(QLabel("姝㈡崯瑙﹀彂浠?), 1, 2)
        form.addWidget(self._sl_trigger_edit, 1, 3)
        form.addWidget(QLabel("姝㈢泩鎶ュ崟鏂瑰紡"), 2, 0)
        form.addWidget(self._tp_mode_combo, 2, 1)
        form.addWidget(QLabel("姝㈢泩鎶ュ崟浠锋牸"), 2, 2)
        form.addWidget(self._tp_price_edit, 2, 3)
        form.addWidget(QLabel("姝㈢泩婊戠偣"), 3, 0)
        form.addWidget(self._tp_slippage_edit, 3, 1)
        form.addWidget(QLabel("杞绉掓暟"), 3, 2)
        form.addWidget(self._poll_seconds_edit, 3, 3)
        form.addWidget(QLabel("姝㈡崯鎶ュ崟鏂瑰紡"), 4, 0)
        form.addWidget(self._sl_mode_combo, 4, 1)
        form.addWidget(QLabel("姝㈡崯鎶ュ崟浠锋牸"), 4, 2)
        form.addWidget(self._sl_price_edit, 4, 3)
        form.addWidget(QLabel("姝㈡崯婊戠偣"), 5, 0)
        form.addWidget(self._sl_slippage_edit, 5, 1)
        top_layout.addLayout(form)

        action_row = QHBoxLayout()
        start_button = QPushButton("鍚姩淇濇姢")
        start_button.clicked.connect(self._start_selected_position_protection)
        stop_button = QPushButton("鍋滄閫変腑浠诲姟")
        stop_button.clicked.connect(self._stop_selected_position_protection)
        clear_button = QPushButton("娓呴櫎宸茬粨鏉?)
        clear_button.clicked.connect(self._clear_finished_position_protections)
        close_button = QPushButton("鍏抽棴")
        close_button.clicked.connect(self.close)
        action_row.addWidget(start_button)
        action_row.addWidget(stop_button)
        action_row.addWidget(clear_button)
        action_row.addStretch(1)
        action_row.addWidget(close_button)
        top_layout.addLayout(action_row)
        layout.addWidget(top_panel)

        bottom_split = QSplitter(Qt.Orientation.Vertical)

        sessions_panel = QFrame()
        sessions_panel.setObjectName("Panel")
        sessions_layout = QVBoxLayout(sessions_panel)
        sessions_layout.setContentsMargins(10, 10, 10, 10)
        sessions_layout.setSpacing(8)
        self._session_status_label = QLabel("褰撳墠娌℃湁杩愯涓殑鏈熸潈淇濇姢浠诲姟銆?)
        self._session_status_label.setObjectName("Subtle")
        sessions_layout.addWidget(self._session_status_label)
        self._sessions_table = QTableWidget(0, 6)
        self._sessions_table.setHorizontalHeaderLabels(("API", "鏈熸潈鍚堢害", "瑙﹀彂鏉′欢", "鏂瑰悜", "鐘舵€?, "鍚姩鏃堕棿"))
        self._sessions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._sessions_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._sessions_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._sessions_table.verticalHeader().setVisible(False)
        self._sessions_table.horizontalHeader().setStretchLastSection(False)
        self._sessions_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self._sessions_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self._sessions_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        self._sessions_table.itemSelectionChanged.connect(self._refresh_selected_session_detail)
        sessions_layout.addWidget(self._sessions_table, 1)
        bottom_split.addWidget(sessions_panel)

        detail_panel = QFrame()
        detail_panel.setObjectName("Panel")
        detail_layout = QVBoxLayout(detail_panel)
        detail_layout.setContentsMargins(10, 10, 10, 10)
        detail_layout.setSpacing(8)
        detail_title = QLabel("浠诲姟璇︽儏")
        detail_title.setObjectName("SectionTitle")
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        detail_layout.addWidget(detail_title)
        detail_layout.addWidget(self._detail_text, 1)
        bottom_split.addWidget(detail_panel)
        bottom_split.setSizes([340, 240])
        layout.addWidget(bottom_split, 1)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._refresh_timer.stop()
        super().closeEvent(event)

    def showEvent(self, event) -> None:  # type: ignore[override]
        if not self._refresh_timer.isActive():
            self._refresh_timer.start(1200)
        super().showEvent(event)

    def _current_position(self) -> OkxPosition | None:
        current = self._selected_option_provider()
        if current is not None:
            self._selected_position = current
            return current
        return self._selected_position

    def _refresh_from_selection(self, *, force: bool) -> None:
        position = self._selected_option_provider()
        if position is None and self._selected_position is None:
            self._title_label.setText("璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉℃湡鏉冧粨浣嶃€?)
            self._logic_hint.setText("淇濇姢閫昏緫浼氳窡闅忎笂鏂归€変腑鐨勬湡鏉冧粨浣嶃€?)
            return
        if position is None:
            position = self._selected_position
        if position is None:
            return
        self._selected_position = position
        position_key = _position_tree_row_id(position)
        direction = derive_position_direction(position)
        self._title_label.setText(
            f"褰撳墠閫変腑锛歿position.inst_id} | 鏂瑰悜={direction.upper()} | 鎸佷粨閲?{_format_optional_decimal(position.position)} | 寮€浠撳潎浠?{_format_optional_decimal(position.avg_price)}"
        )
        if force or position_key != self._form_position_key:
            self._form_position_key = position_key
            self._trigger_combo.setCurrentIndex(0)
            self._spot_symbol_edit.setText(infer_default_spot_inst_id(position.inst_id))
            self._tp_trigger_edit.clear()
            self._sl_trigger_edit.clear()
            self._tp_mode_combo.setCurrentIndex(1 if self._tp_mode_combo.count() > 1 else 0)
            self._sl_mode_combo.setCurrentIndex(1 if self._sl_mode_combo.count() > 1 else 0)
            self._tp_price_edit.clear()
            self._sl_price_edit.clear()
            self._tp_slippage_edit.setText("0")
            self._sl_slippage_edit.setText("0")
            self._poll_seconds_edit.setText("2")
        self._on_trigger_source_changed()
        self._refresh_order_mode_widgets()

    def _on_trigger_source_changed(self) -> None:
        position = self._current_position()
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self._trigger_combo.currentText(), "option_mark")
        self._spot_symbol_edit.setEnabled(trigger_source == "spot_last")
        if position is None:
            self._logic_hint.setText("淇濇姢閫昏緫浼氳窡闅忎笂鏂归€変腑鐨勬湡鏉冧粨浣嶃€?)
            return
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
        else:
            trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text()) or infer_default_spot_inst_id(position.inst_id)
            trigger_price_type = "last"
        self._logic_hint.setText(
            describe_protection_price_logic(
                option_inst_id=position.inst_id,
                direction=derive_position_direction(position),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,
            )
        )

    def _refresh_order_mode_widgets(self) -> None:
        self._sync_order_mode_widgets(mode_label=self._tp_mode_combo.currentText(), price_edit=self._tp_price_edit, slippage_edit=self._tp_slippage_edit, key="tp")
        self._sync_order_mode_widgets(mode_label=self._sl_mode_combo.currentText(), price_edit=self._sl_price_edit, slippage_edit=self._sl_slippage_edit, key="sl")

    def _sync_order_mode_widgets(self, *, mode_label: str, price_edit: QLineEdit, slippage_edit: QLineEdit, key: str) -> None:
        fixed_mode = _resolve_protection_order_mode_value(mode_label) == "fixed_price"
        if fixed_mode:
            if not price_edit.text().strip():
                price_edit.setText(self._last_fixed_price_memory.get(key, ""))
            price_edit.setEnabled(True)
            slippage_edit.setEnabled(False)
        else:
            current_text = price_edit.text().strip()
            if current_text:
                self._last_fixed_price_memory[key] = current_text
            price_edit.clear()
            price_edit.setEnabled(False)
            slippage_edit.setEnabled(True)

    def _start_selected_position_protection(self) -> None:
        runtime = self._runtime_provider()
        position = self._current_position()
        if runtime is None:
            QMessageBox.warning(self, "鍚姩澶辫触", "褰撳墠娌℃湁鍙敤鐨?API 杩愯鏃躲€?)
            return
        if position is None or position.inst_type != "OPTION":
            QMessageBox.information(self, "鎻愮ず", "璇峰厛鍦ㄥ綋鍓嶆寔浠撲腑閫変腑涓€鏉℃湡鏉冧粨浣嶃€?)
            return
        try:
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self._client, protection, position)
            config = self._build_strategy_config(runtime=runtime, position=position, protection=protection)
            self._manager.start(runtime.credentials, config, protection)
            self._refresh_sessions()
        except Exception as exc:
            QMessageBox.critical(self, "鍚姩淇濇姢澶辫触", str(exc))

    def _stop_selected_position_protection(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            QMessageBox.information(self, "鎻愮ず", "璇峰厛鍦ㄤ笅鏂逛换鍔″垪琛ㄩ噷閫変腑涓€鏉′繚鎶や换鍔°€?)
            return
        try:
            self._manager.stop(session_id)
            self._refresh_sessions()
        except Exception as exc:
            QMessageBox.critical(self, "鍋滄澶辫触", str(exc))

    def _clear_finished_position_protections(self) -> None:
        cleared = self._manager.clear_finished()
        self._refresh_sessions()
        if cleared <= 0:
            QMessageBox.information(self, "鎻愮ず", "褰撳墠娌℃湁鍙竻鐞嗙殑宸茬粨鏉熶换鍔°€?)

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self._trigger_combo.currentText()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 鏍囪浠?
        else:
            trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text())
            if not trigger_inst_id:
                raise ValueError("鐜拌揣瑙﹀彂妯″紡涓嬶紝璇峰～鍐欑幇璐ф爣鐨勶紝渚嬪 BTC-USDT銆?)
            trigger_instrument = self._client.get_instrument(trigger_inst_id)
            if str(trigger_instrument.inst_type or "").upper() != "SPOT":
                raise ValueError("鐜拌揣瑙﹀彂妯″紡涓嬶紝鏍囩殑蹇呴』鏄幇璐т氦鏄撳锛屼緥濡?BTC-USDT銆?)
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 鏈€鏂颁环"

        take_profit_trigger = self._parse_optional_positive_decimal(self._tp_trigger_edit.text(), "姝㈢泩瑙﹀彂浠?)
        stop_loss_trigger = self._parse_optional_positive_decimal(self._sl_trigger_edit.text(), "姝㈡崯瑙﹀彂浠?)
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("姝㈢泩瑙﹀彂浠峰拰姝㈡崯瑙﹀彂浠疯嚦灏戣濉啓涓€涓€?)

        direction = derive_position_direction(position)
        _validate_protection_price_relationship(
            option_inst_id=position.inst_id,
            direction=direction,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            take_profit=take_profit_trigger,
            stop_loss=stop_loss_trigger,
        )

        take_profit_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self._tp_mode_combo.currentText()]
        stop_loss_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self._sl_mode_combo.currentText()]
        take_profit_order_price = self._parse_positive_decimal(self._tp_price_edit.text(), "姝㈢泩鎶ュ崟浠锋牸") if take_profit_order_mode == "fixed_price" else None
        stop_loss_order_price = self._parse_positive_decimal(self._sl_price_edit.text(), "姝㈡崯鎶ュ崟浠锋牸") if stop_loss_order_mode == "fixed_price" else None
        return OptionProtectionConfig(
            option_inst_id=position.inst_id,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            direction=direction,
            pos_side=position.pos_side if position.pos_side and position.pos_side.lower() != "net" else None,
            take_profit_trigger=take_profit_trigger,
            stop_loss_trigger=stop_loss_trigger,
            take_profit_order_mode=take_profit_order_mode,
            take_profit_order_price=take_profit_order_price,
            take_profit_slippage=self._parse_nonnegative_decimal(self._tp_slippage_edit.text(), "姝㈢泩婊戠偣"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self._sl_slippage_edit.text(), "姝㈡崯婊戠偣"),
            poll_seconds=float(self._parse_positive_decimal(self._poll_seconds_edit.text(), "杞绉掓暟")),
            trigger_label=trigger_label,
        )

    def _build_strategy_config(self, *, runtime: object, position: OkxPosition, protection: OptionProtectionConfig) -> StrategyConfig:
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        trade_mode = position.mgn_mode if position.mgn_mode in {"cross", "isolated"} else getattr(runtime, "trade_mode", "cross")
        return StrategyConfig(
            inst_id=protection.trigger_inst_id,
            bar="1H",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if protection.direction == "long" else "short_only",
            position_mode=position_mode,
            environment=getattr(runtime, "environment", "live"),
            tp_sl_trigger_type=protection.trigger_price_type,
            strategy_id="manual_option_protection",
            poll_seconds=protection.poll_seconds,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=protection.trigger_inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _selected_session_id(self) -> str:
        row = self._sessions_table.currentRow()
        if row < 0 or row >= len(self._session_ids):
            return ""
        return self._session_ids[row]

    def _refresh_sessions(self) -> None:
        sessions = self._manager.list_sessions()
        selected_before = self._selected_session_id()
        self._session_status_label.setText(f"褰撳墠淇濇姢浠诲姟锛歿len(sessions)}" if sessions else "褰撳墠娌℃湁杩愯涓殑鏈熸潈淇濇姢浠诲姟銆?)
        self._sessions_table.setRowCount(len(sessions))
        self._session_ids = [item.session_id for item in sessions]
        for row, item in enumerate(sessions):
            values = (
                item.api_name or "-",
                item.option_inst_id,
                item.trigger_label,
                item.direction,
                item.status,
                item.started_at.strftime("%H:%M:%S"),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column in {0, 3, 4, 5}:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._sessions_table.setItem(row, column, cell)
        target_row = -1
        if selected_before and selected_before in self._session_ids:
            target_row = self._session_ids.index(selected_before)
        elif self._session_ids:
            target_row = 0
        if target_row >= 0:
            self._sessions_table.selectRow(target_row)
        else:
            self._detail_text.setPlainText("璇烽€夋嫨涓€鏉′繚鎶や换鍔℃煡鐪嬭鎯呫€?)
        self._refresh_selected_session_detail()

    def _refresh_selected_session_detail(self) -> None:
        session_id = self._selected_session_id()
        sessions = {item.session_id: item for item in self._manager.list_sessions()}
        session = sessions.get(session_id)
        if session is None:
            self._detail_text.setPlainText("璇烽€夋嫨涓€鏉′繚鎶や换鍔℃煡鐪嬭鎯呫€?)
            return
        self._detail_text.setPlainText(
            "\n".join(
                [
                    f"浠诲姟锛歿session.session_id}",
                    f"API閰嶇疆锛歿session.api_name or '-'}",
                    f"鏈熸潈鍚堢害锛歿session.option_inst_id}",
                    f"瑙﹀彂鏉′欢锛歿session.trigger_label}",
                    f"瑙﹀彂鏍囩殑锛歿session.trigger_inst_id}",
                    f"瑙﹀彂浠锋牸绫诲瀷锛歿_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"鏂瑰悜锛歿session.direction}",
                    f"鎸佷粨鏂瑰悜锛歿session.pos_side or '-'}",
                    f"姝㈢泩瑙﹀彂锛歿_format_optional_decimal(session.take_profit_trigger)}",
                    f"姝㈢泩鎶ュ崟鏂瑰紡锛歿_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"姝㈢泩鎶ュ崟浠锋牸锛歿_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"姝㈢泩婊戠偣锛歿_format_optional_decimal(session.take_profit_slippage)}",
                    f"姝㈡崯瑙﹀彂锛歿_format_optional_decimal(session.stop_loss_trigger)}",
                    f"姝㈡崯鎶ュ崟鏂瑰紡锛歿_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"姝㈡崯鎶ュ崟浠锋牸锛歿_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"姝㈡崯婊戠偣锛歿_format_optional_decimal(session.stop_loss_slippage)}",
                    f"杞绉掓暟锛歿session.poll_seconds:g}",
                    f"鐘舵€侊細{session.status}",
                    f"鍚姩鏃堕棿锛歿session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"鏈€鏂扮姸鎬侊細{session.last_message}",
                ]
            )
        )

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw.strip())
        except Exception as exc:
            raise ValueError(f"{field_name} 涓嶆槸鏈夋晥鏁板瓧") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 蹇呴』澶т簬 0")
        return value

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        return self._parse_positive_decimal(cleaned, field_name)

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        if not cleaned:
            return Decimal("0")
        try:
            value = Decimal(cleaned)
        except Exception as exc:
            raise ValueError(f"{field_name} 涓嶆槸鏈夋晥鏁板瓧") from exc
        if value < 0:
            raise ValueError(f"{field_name} 涓嶈兘灏忎簬 0")
        return value


class AccountPositionsHomeWidget(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = load_runtime("159") or load_runtime()
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._profile_switch_guard = False
        self._profile_change_serial = 0
        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None
        self._order_history_feed: OrderHistoryFeedThread | None = None
        self._fill_history_feed: FillHistoryFeedThread | None = None
        self._position_history_feed: PositionHistoryFeedThread | None = None

        self._raw_positions: list[OkxPosition] = []
        self._visible_positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._order_history_items: list[OkxTradeOrderItem] = []
        self._visible_order_history_items: list[OkxTradeOrderItem] = []
        self._fill_history_items: list[OkxFillHistoryItem] = []
        self._visible_fill_history_items: list[OkxFillHistoryItem] = []
        self._fill_history_instruments: dict[str, object] = {}
        self._fill_history_usdt_prices: dict[str, Decimal] = {}
        self._order_history_usdt_prices: dict[str, Decimal] = {}
        self._position_history_items: list[OkxPositionHistoryItem] = []
        self._visible_position_history_items: list[OkxPositionHistoryItem] = []
        self._position_history_instruments: dict[str, object] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._visible_column_ids: set[str] = set(DEFAULT_VISIBLE_COLUMNS)
        self._tree_column_width_overrides: dict[str, int] = {}
        self._expanded_row_keys: set[str] = set()
        self._position_kline_last_bar = "1H"
        self._position_kline_window_width = 1280
        self._position_kline_window_height = 760
        self._fill_history_fetch_limit = 100
        self._position_history_fetch_limit = 300
        self._position_history_last_sync_text = "-"
        self._position_history_filter_resetting = False
        self._selected_position_manual_flatten_running = False
        self._shared_client = OkxRestClient()
        self._protection_manager = PositionProtectionManager(self._shared_client, lambda _message: None)
        self._protection_dialog: PositionProtectionDialog | None = None
        self._legacy_option_tools = LegacyOptionToolsHost(parent=self, runtime_provider=lambda: self._runtime)
        self._instrument_kline_dialog: InstrumentKlineDialog | None = None

        self._current_notes: dict[str, dict[str, object]] = {}
        self._history_notes: dict[str, dict[str, object]] = {}
        self._load_position_notes()
        self._load_positions_view_prefs()

        self._build_ui()
        self._apply_compact_layout_tuning()
        self._positions_view_prefs_save_timer = QTimer(self)
        self._positions_view_prefs_save_timer.setSingleShot(True)
        self._positions_view_prefs_save_timer.timeout.connect(self._save_positions_view_prefs_now)
        self._position_history_render_timer = QTimer(self)
        self._position_history_render_timer.setSingleShot(True)
        self._position_history_render_timer.timeout.connect(self._render_position_history_table)
        self._refresh_profiles()
        self._populate_profile_combo()

        locked_on_start = bool(
            self._last_profile_name and profile_requires_password(self._last_profile_name, self._profile_snapshots)
        )
        if locked_on_start:
            self._account_status.setText(f"API {self._last_profile_name} 鏈В閿?)
            self._order_status.setText("璁㈠崟 WS 绛夊緟 API 瑙ｉ攣")
            self._summary_label.setText("褰撳墠 API 閰嶇疆宸插姞鍒囨崲瀵嗙爜锛岃鍏堣В閿佸悗鍐嶅姞杞借处鎴锋寔浠撱€?)
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def _stop_position_history_thread(self) -> None:
        thread = self._position_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._position_history_feed = None

    def _stop_order_history_thread(self) -> None:
        thread = self._order_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._order_history_feed = None

    def _stop_fill_history_thread(self) -> None:
        thread = self._fill_history_feed
        if thread is None:
            return
        thread.stop()
        if thread.isRunning() and not thread.wait(1600):
            thread.terminate()
            thread.wait(1600)
        self._fill_history_feed = None

    def _start_order_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_order_history_thread()
        elif self._order_history_feed is not None and self._order_history_feed.isRunning():
            return
        self._order_history_feed = OrderHistoryFeedThread(self._runtime, limit=200)
        self._order_history_feed.data_ready.connect(self._apply_order_history_payload)
        self._order_history_feed.status_changed.connect(self._set_order_history_status)
        self._order_history_feed.finished.connect(self._clear_order_history_thread)
        if hasattr(self, "_order_history_summary_label"):
            self._order_history_summary_label.setText("姝ｅ湪鍚屾鍘嗗彶濮旀墭...")
        self._order_history_feed.start()

    def _start_fill_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_fill_history_thread()
        elif self._fill_history_feed is not None and self._fill_history_feed.isRunning():
            return
        self._fill_history_feed = FillHistoryFeedThread(self._runtime, limit=self._fill_history_fetch_limit)
        self._fill_history_feed.data_ready.connect(self._apply_fill_history_payload)
        self._fill_history_feed.status_changed.connect(self._set_fill_history_status)
        self._fill_history_feed.finished.connect(self._clear_fill_history_thread)
        if hasattr(self, "_fill_history_summary_label"):
            self._fill_history_summary_label.setText("姝ｅ湪鍚屾鍘嗗彶鎴愪氦...")
        self._fill_history_feed.start()

    def _start_position_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_position_history_thread()
        elif self._position_history_feed is not None and self._position_history_feed.isRunning():
            return
        self._position_history_feed = PositionHistoryFeedThread(self._runtime, limit=self._position_history_fetch_limit)
        self._position_history_feed.data_ready.connect(self._apply_position_history_payload)
        self._position_history_feed.status_changed.connect(self._set_position_history_status)
        self._position_history_feed.finished.connect(self._clear_position_history_thread)
        self._position_history_summary_label.setText("姝ｅ湪鍚屾鍘嗗彶浠撲綅...")
        self._position_history_feed.start()

    @Slot()
    def _refresh_position_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_position_history_refresh(force_restart=True)

    @Slot()
    def _clear_position_history_thread(self) -> None:
        self._position_history_feed = None
        return

    @Slot()
    def _refresh_order_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_order_history_refresh(force_restart=True)

    @Slot()
    def _refresh_fill_history(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._start_fill_history_refresh(force_restart=True)

    @Slot()
    def _clear_order_history_thread(self) -> None:
        self._order_history_feed = None

    @Slot()
    def _clear_fill_history_thread(self) -> None:
        self._fill_history_feed = None
        return

        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None
        self._position_history_feed: PositionHistoryFeedThread | None = None

        self._raw_positions: list[OkxPosition] = []
        self._visible_positions: list[OkxPosition] = []
        self._orders: list[OrderStatusView] = []
        self._visible_orders: list[OrderStatusView] = []
        self._position_history_items: list[OkxPositionHistoryItem] = []
        self._position_history_instruments: dict[str, object] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, object] = {}
        self._position_tickers: dict[str, object] = {}
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._visible_column_ids: set[str] = set(DEFAULT_VISIBLE_COLUMNS)
        self._expanded_row_keys: set[str] = set()
        self._shared_client = OkxRestClient()
        self._protection_manager = PositionProtectionManager(self._shared_client, lambda _message: None)
        self._protection_dialog: PositionProtectionDialog | None = None
        self._legacy_option_tools = LegacyOptionToolsHost(parent=self, runtime_provider=lambda: self._runtime)

        self._current_notes: dict[str, dict[str, object]] = {}
        self._history_notes: dict[str, dict[str, object]] = {}
        self._load_position_notes()

        self._build_ui()
        self._refresh_profiles()
        self._populate_profile_combo()

        locked_on_start = bool(
            self._last_profile_name and profile_requires_password(self._last_profile_name, self._profile_snapshots)
        )
        if locked_on_start:
            self._account_status.setText(f"API {self._last_profile_name} 鏈В閿?)
            self._order_status.setText("濮旀墭 WS 绛夊緟 API 瑙ｉ攣")
            self._summary_label.setText("褰撳墠 API 閰嶇疆宸插姞鍒囨崲瀵嗙爜锛岃鍏堣В閿佸悗鍐嶅姞杞借处鎴锋寔浠撱€?)
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()

    def shutdown(self) -> None:
        self._save_positions_view_prefs_now()
        self._stop_private_threads()
        self._stop_order_history_thread()
        self._stop_fill_history_thread()
        self._stop_position_history_thread()
        self._protection_manager.stop_all()
        if self._protection_dialog is not None:
            self._protection_dialog.close()
        self._legacy_option_tools.shutdown()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("姝ｅ湪鍒锋柊...")
        self._start_private_threads(force_restart=True)

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("姝ｅ湪鍒锋柊...")
        self._start_private_threads(force_restart=True)

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        filtered = self._filtered_position_history_items()
        selected_key = ""
        row = self._position_history_table.currentRow()
        if 0 <= row < len(self._visible_position_history_items):
            selected_key = self._position_history_row_key(self._visible_position_history_items[row])
        self._visible_position_history_items = filtered
        stats_text = _format_position_history_filter_stats(list(enumerate(filtered)), self._position_history_usdt_prices)
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"鍘嗗彶浠撲綅: {len(self._position_history_items)} 鏉?| 鏈€杩戝悓姝? {self._position_history_last_sync_text} | 褰撳墠鏄剧ず: {len(filtered)}/{len(self._position_history_items)}",
                    f"绛涢€夌粺璁? {stats_text}",
                )
            )
        )
        stats_text = _format_position_history_filter_stats(list(enumerate(filtered)), self._position_history_usdt_prices)
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"鍘嗗彶浠撲綅: {len(self._position_history_items)} 鏉?| 鏈€杩戝悓姝? {self._position_history_last_sync_text} | 褰撳墠鏄剧ず: {len(filtered)}/{len(self._position_history_items)}",
                    f"绛涢€夌粺璁? {stats_text}",
                )
            )
        )
        self._position_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            self._set_table_row(self._position_history_table, row, values, left_align={2, 11})
        self._restore_table_selection(
            self._position_history_table,
            filtered,
            selected_key,
            self._position_history_row_key,
        )
        self._refresh_position_history_detail()
        self._start_order_history_refresh(force_restart=True)
        self._start_fill_history_refresh(force_restart=True)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        layout.addWidget(self._build_header())
        layout.addWidget(self._build_filter_bar())
        layout.addWidget(self._build_positions_panel(), 1)

    def _apply_compact_layout_tuning(self) -> None:
        self.setStyleSheet(
            """
            QWidget {
                font-size: 11px;
            }
            QFrame#HeaderPanel,
            QFrame#Panel,
            QFrame#Guide {
                background: #ffffff;
                border: 1px solid #d7e0ea;
                border-radius: 7px;
            }
            QFrame#ToolbarBand {
                background: #f8fafc;
                border: 1px solid #e2e8f0;
                border-radius: 6px;
            }
            QLabel#SectionTitle {
                font-size: 12px;
                font-weight: 700;
                color: #0f172a;
            }
            QLabel#Subtle {
                color: #64748b;
            }
            QLabel#Badge {
                color: #075985;
                background: #e0f2fe;
                border: 1px solid #bae6fd;
                border-radius: 6px;
                padding: 2px 8px;
                font-weight: 700;
            }
            QPushButton {
                font-size: 11px;
                padding: 2px 8px;
                min-height: 22px;
                border-radius: 5px;
            }
            QPushButton:hover {
                border-color: #93c5fd;
            }
            QPushButton:pressed {
                padding-top: 3px;
                padding-right: 7px;
                padding-bottom: 1px;
                padding-left: 9px;
            }
            QComboBox, QLineEdit {
                font-size: 11px;
                min-height: 22px;
                padding: 1px 6px;
                border-radius: 5px;
            }
            QComboBox:hover, QLineEdit:hover {
                border-color: #93c5fd;
            }
            QTabBar::tab {
                font-size: 11px;
                min-height: 22px;
                padding: 3px 10px;
            }
            QTabBar::tab:hover {
                color: #1d4ed8;
            }
            QTabBar::tab:pressed {
                background: #dbeafe;
            }
            QTabWidget::pane {
                border: 1px solid #d7e0ea;
                border-radius: 6px;
                top: -1px;
            }
            QHeaderView::section {
                padding: 2px 5px;
                min-height: 20px;
            }
            QHeaderView::section:hover {
                background: #f1f5f9;
            }
            QTreeWidget, QTableWidget, QTextEdit {
                border-radius: 5px;
            }
            QSplitter::handle {
                background: #dbe3ec;
                height: 5px;
            }
            """
        )
        for table in self.findChildren(QTableWidget):
            table.verticalHeader().setDefaultSectionSize(21)
        for tree in self.findChildren(QTreeWidget):
            tree.setStyleSheet("QTreeView::item { height: 21px; }")

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("HeaderPanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(5)

        top = QHBoxLayout()
        top.setSpacing(6)
        self._status_badge = QLabel("姝ｅ父")
        self._status_badge.setObjectName("Badge")
        self._account_status = QLabel("鎸佷粨璇诲彇涓?..")
        self._account_status.setObjectName("Subtle")
        self._order_status = QLabel("璁㈠崟WS绛夊緟涓?..")
        self._order_status.setObjectName("Subtle")
        self._summary_label = QLabel("褰撳墠娌℃湁鎸佷粨")
        self._summary_label.setObjectName("Subtle")
        self._summary_label.setWordWrap(False)
        top.addWidget(self._status_badge)
        top.addWidget(self._account_status)
        top.addWidget(self._order_status)
        top.addWidget(self._summary_label, 1)
        top.addStretch(1)
        top.addWidget(QLabel("API閰嶇疆"))
        self._profile_combo = QComboBox()
        self._profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._profile_combo.setMinimumWidth(120)
        top.addWidget(self._profile_combo)
        layout.addLayout(top)

        toolbar = QFrame()
        toolbar.setObjectName("ToolbarBand")
        actions = QHBoxLayout(toolbar)
        actions.setContentsMargins(5, 4, 5, 4)
        actions.setSpacing(4)
        for text, handler, button_attr in (
            ("鍒锋柊", self.refresh_view, ""),
            ("璐︽埛淇℃伅", self._show_account_overview, ""),
            ("灞曞紑鎸佷粨璇︽儏", self._toggle_detail_panel, "_detail_toggle_button"),
            ("鎶樺彔鍘嗗彶鍖哄煙", self._toggle_history_panel, "_history_toggle_button"),
            ("骞充粨閫変腑", self.flatten_selected_position, ""),
            ("缂栬緫澶囨敞", self.edit_selected_position_note, ""),
            ("浠庨€変腑鎸佷粨鎺ョ", self._show_not_ready_action, ""),
            ("鍋滄鎺ョ", self._show_not_ready_action, ""),
            ("璁剧疆鏈熸潈淇濇姢", self._open_position_protection_dialog, ""),
            ("灞曟湡寤鸿", self._open_option_roll_window, ""),
            ("鍒楄缃?, self.open_positions_column_window, ""),
        ):
            button = QPushButton(text)
            button.clicked.connect(handler)
            if button_attr:
                setattr(self, button_attr, button)
            actions.addWidget(button)
        actions.addStretch(1)
        layout.addWidget(toolbar)
        return panel

    def _build_filter_bar(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("Guide")
        layout = QGridLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setHorizontalSpacing(8)
        layout.setVerticalSpacing(5)

        self._type_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._type_combo.addItem(label, value)
        self._type_combo.currentIndexChanged.connect(self._apply_filters)

        self._keyword_edit = QLineEdit()
        self._keyword_edit.setPlaceholderText("鎼滅储鍚堢害 / 甯佺 / 澶囨敞 / 妯″紡")
        self._keyword_edit.textChanged.connect(self._apply_filters)

        self._filter_hint = QLabel("閫変腑鏈熸潈鍚庯紝鍙竴閿甫鍏ュ悎绾︽垨鍒版湡鍓嶇紑銆?)
        self._filter_hint.setObjectName("Subtle")

        self._apply_contract_button = QPushButton("甯﹀叆鍚堢害")
        self._apply_contract_button.clicked.connect(self.apply_selected_option_to_position_search)
        self._apply_contract_button.setEnabled(False)
        self._apply_expiry_button = QPushButton("甯﹀叆鍒版湡鍓嶇紑")
        self._apply_expiry_button.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_search)
        self._apply_expiry_button.setEnabled(False)

        apply_button = QPushButton("搴旂敤绛涢€?)
        apply_button.clicked.connect(self._apply_filters)
        clear_button = QPushButton("娓呯┖绛涢€?)
        clear_button.clicked.connect(self._clear_filters)

        layout.addWidget(QLabel("绫诲瀷"), 0, 0)
        layout.addWidget(self._type_combo, 0, 1)
        layout.addWidget(QLabel("鎼滅储"), 0, 2)
        layout.addWidget(self._keyword_edit, 0, 3, 1, 4)
        layout.addWidget(self._apply_contract_button, 0, 7)
        layout.addWidget(self._apply_expiry_button, 0, 8)
        layout.addWidget(apply_button, 0, 9)
        layout.addWidget(clear_button, 0, 10)
        layout.addWidget(self._filter_hint, 1, 0, 1, 11)
        layout.setColumnStretch(3, 1)
        return panel

    def _build_positions_panel(self) -> QWidget:
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self._build_tree_section())
        splitter.addWidget(self._build_history_tabs_v2())
        splitter.setChildrenCollapsible(False)
        splitter.setSizes([690, 340])
        self._update_panel_toggle_buttons()
        return splitter

    def _build_tree_section(self) -> QWidget:
        wrapper = QWidget()
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        panel = QFrame()
        panel.setObjectName("Panel")
        panel_layout = QVBoxLayout(panel)
        panel_layout.setContentsMargins(8, 7, 8, 7)
        panel_layout.setSpacing(5)

        title_row = QHBoxLayout()
        title = QLabel("褰撳墠鎸佷粨")
        title.setObjectName("SectionTitle")
        self._positions_hint = QLabel("褰撳墠鏄剧ず 0 鏉℃寔浠?| 鐐瑰嚮浠讳竴琛屾煡鐪嬭鎯呫€?)
        self._positions_hint.setObjectName("Subtle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self._expand_toggle_button = QPushButton("灞曞紑鍏ㄩ儴")
        self._expand_toggle_button.clicked.connect(self._toggle_all_positions)
        title_row.addWidget(self._expand_toggle_button)
        title_row.addWidget(self._positions_hint)
        panel_layout.addLayout(title_row)

        self._position_tree = QTreeWidget()
        self._position_tree.setColumnCount(1 + len(POSITION_COLUMNS))
        self._position_tree.setHeaderLabels(["鍚堢害 / 鍒嗙粍", *[item[1] for item in POSITION_COLUMNS]])
        self._position_tree.setAlternatingRowColors(True)
        self._position_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._position_tree.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._position_tree.itemSelectionChanged.connect(self._on_position_selected)
        self._position_tree.itemDoubleClicked.connect(self._on_position_tree_clicked)
        self._position_tree.itemExpanded.connect(self._on_tree_item_expanded)
        self._position_tree.itemCollapsed.connect(self._on_tree_item_collapsed)
        self._position_tree.setRootIsDecorated(True)
        self._position_tree.setUniformRowHeights(True)
        header = self._position_tree.header()
        header.setStretchLastSection(False)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
        header_item = self._position_tree.headerItem()
        for index in range(self._position_tree.columnCount()):
            header_item.setTextAlignment(index, int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
        self._position_tree.setColumnWidth(0, DEFAULT_TREE_LABEL_WIDTH)
        for index, (_column_id, _heading, width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            self._position_tree.setColumnWidth(index, DEFAULT_TREE_COLUMN_WIDTHS.get(_column_id, width))
        self._apply_tree_column_width_overrides()
        self._apply_column_visibility()
        header.sectionResized.connect(self._schedule_positions_view_prefs_save)
        panel_layout.addWidget(self._position_tree, 1)
        layout.addWidget(panel, 1)

        self._detail_panel = QFrame()
        self._detail_panel.setObjectName("Panel")
        detail_layout = QVBoxLayout(self._detail_panel)
        detail_layout.setContentsMargins(12, 12, 12, 12)
        detail_layout.setSpacing(8)
        detail_title = QLabel("鎸佷粨璇︽儏")
        detail_title.setObjectName("SectionTitle")
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        detail_layout.addWidget(detail_title)
        detail_layout.addWidget(self._detail_text, 1)
        self._detail_panel.setVisible(False)
        layout.addWidget(self._detail_panel)
        return wrapper

    def _build_history_tabs(self) -> QWidget:
        self._history_panel = QFrame()
        self._history_panel.setObjectName("Panel")
        layout = QVBoxLayout(self._history_panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_current_orders_tab(), "褰撳墠濮旀墭")
        self._tabs.addTab(self._build_placeholder_tab("鍔ㄦ€佹鐩堟帴绠?, "鍔ㄦ€佹鐩堟帴绠″尯鍧楅鐣欙紝鍚庣画鎸夋棫椤甸潰瀹屾暣杩佺Щ銆?), "鍔ㄦ€佹鐩堟帴绠?)
        self._tabs.addTab(self._build_placeholder_tab("鍘嗗彶濮旀墭", "鍘嗗彶濮旀墭鍖哄潡棰勭暀锛屽悗缁ˉ榻愮瓫閫夊拰鍚屾閫昏緫銆?), "鍘嗗彶濮旀墭")
        self._tabs.addTab(self._build_placeholder_tab("鍘嗗彶鎴愪氦", "鍘嗗彶鎴愪氦鍖哄潡棰勭暀锛屽悗缁ˉ榻愮瓫閫夊拰鍚屾閫昏緫銆?), "鍘嗗彶鎴愪氦")
        self._tabs.addTab(self._build_position_history_tab(), "鍘嗗彶浠撲綅")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._orders_summary_label = QLabel("褰撳墠濮旀墭灏氭湭璇诲彇銆?)
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        layout.addWidget(self._orders_summary_label)

        self._orders_table = QTableWidget(0, 11)
        self._orders_table.setHorizontalHeaderLabels(
            ("鏃堕棿", "鍚堢害", "绫诲瀷", "鐘舵€?, "鏂瑰悜", "濮旀墭绫诲瀷", "濮旀墭浠?, "濮旀墭閲?, "宸叉垚浜?, "浜ゆ槗妯″紡", "clOrdId")
        )
        self._orders_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._orders_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._orders_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._orders_table.verticalHeader().setVisible(False)
        header = self._orders_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.Stretch)
        self._orders_table.itemSelectionChanged.connect(self._refresh_current_order_detail)
        layout.addWidget(self._orders_table, 1)

        detail_title = QLabel("濮旀墭璇︽儏")
        detail_title.setObjectName("SectionTitle")
        self._orders_detail = QTextEdit()
        self._orders_detail.setReadOnly(True)
        self._orders_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑褰撳墠濮旀墭鐨勮鎯呫€?)
        layout.addWidget(detail_title)
        layout.addWidget(self._orders_detail, 1)
        return tab

    def _build_placeholder_tab(self, title_text: str, message: str) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)
        title = QLabel(title_text)
        title.setObjectName("SectionTitle")
        detail = QTextEdit()
        detail.setReadOnly(True)
        detail.setPlainText(message)
        layout.addWidget(title)
        layout.addWidget(detail, 1)
        return tab

    def _build_position_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        head = QHBoxLayout()
        self._position_history_summary_label = QLabel("鍘嗗彶浠撲綅灏氭湭璇诲彇銆?)
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        head.addWidget(self._position_history_summary_label, 1)
        refresh_button = QPushButton("鍚屾鍘嗗彶浠撲綅")
        refresh_button.clicked.connect(self._refresh_position_history)
        head.addWidget(refresh_button)
        layout.addLayout(head)

        self._position_history_table = QTableWidget(0, 12)
        self._position_history_table.setHorizontalHeaderLabels(
            ("鏃堕棿", "绫诲瀷", "鍚堢害", "淇濊瘉閲戞ā寮?, "鎸佷粨妯″紡", "浜ゆ槗鏂瑰悜", "寮€浠撳潎浠?, "骞充粨鍧囦环", "骞充粨鏁伴噺", "鎵嬬画璐?, "鐩堜簭", "澶囨敞")
        )
        self._position_history_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._position_history_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._position_history_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._position_history_table.verticalHeader().setVisible(False)
        header = self._position_history_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(9, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(10, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(11, QHeaderView.ResizeMode.Stretch)
        self._position_history_table.itemSelectionChanged.connect(self._refresh_position_history_detail)
        layout.addWidget(self._position_history_table, 1)

        detail_title = QLabel("鍘嗗彶浠撲綅璇︽儏")
        detail_title.setObjectName("SectionTitle")
        self._position_history_detail = QTextEdit()
        self._position_history_detail.setReadOnly(True)
        self._position_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶浠撲綅鐨勮鎯呫€?)
        layout.addWidget(detail_title)
        layout.addWidget(self._position_history_detail, 1)
        return tab

    def _refresh_profiles(self) -> None:
        snapshots, _selected = load_profile_snapshots()
        self._profile_snapshots = snapshots

    def _populate_profile_combo(self) -> None:
        self._profile_switch_guard = True
        self._profile_combo.clear()
        names = profile_names()
        if names:
            self._profile_combo.addItems(names)
            target = self._last_profile_name or names[0]
            index = self._profile_combo.findText(target)
            self._profile_combo.setCurrentIndex(index if index >= 0 else 0)
        else:
            self._profile_combo.addItem("鏈厤缃?)
        self._profile_switch_guard = False

    def _ensure_runtime_ready(self, *, force_unlock: bool) -> bool:
        profile_name = self._current_profile_name()
        if not profile_name:
            QMessageBox.warning(self, "鏃犳硶鍒锋柊", "褰撳墠鏈厤缃彲鐢ㄧ殑 API Profile銆?)
            return False
        if force_unlock and not ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles):
            return False
        runtime = load_runtime(profile_name)
        if runtime is None:
            QMessageBox.warning(self, "鏃犳硶鍒锋柊", f"API 閰嶇疆 {profile_name} 涓嶅彲鐢紝璇锋鏌ュ嚟璇併€?)
            return False
        self._runtime = runtime
        self._last_profile_name = profile_name
        return True

    def _current_profile_name(self) -> str:
        text = self._profile_combo.currentText().strip()
        return "" if text == "鏈厤缃? else text

    def _stop_private_threads(self) -> None:
        for thread in (self._account_feed, self._order_feed):
            if thread is None:
                continue
            thread.stop()
            if thread.isRunning() and not thread.wait(1600):
                thread.terminate()
                thread.wait(1600)
        self._account_feed = None
        self._order_feed = None

    def _start_private_threads(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_private_threads()
        elif self._account_feed is not None and self._account_feed.isRunning():
            return

        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._account_feed.positions_ready.connect(self._apply_positions_summary)
        self._account_feed.payload_ready.connect(self._apply_positions_payload)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_orders)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._account_feed.start()
        self._order_feed.start()
        self._start_position_history_refresh(force_restart=force_restart)

    def _load_position_notes(self) -> None:
        snapshot = load_position_notes_snapshot()
        current = snapshot.get("current_notes", []) if isinstance(snapshot, dict) else []
        history = snapshot.get("history_notes", []) if isinstance(snapshot, dict) else []
        self._current_notes = {
            str(item.get("record_key", "")).strip(): dict(item)
            for item in current
            if isinstance(item, dict) and str(item.get("record_key", "")).strip()
        }
        self._history_notes = {
            str(item.get("record_key", "")).strip(): dict(item)
            for item in history
            if isinstance(item, dict) and str(item.get("record_key", "")).strip()
        }

    def _save_position_notes(self) -> None:
        save_position_notes_snapshot(
            current_notes=list(self._current_notes.values()),
            history_notes=list(self._history_notes.values()),
        )

    def _note_environment(self) -> str:
        if self._runtime is None:
            return "live"
        return str(self._runtime.environment or "live").strip().lower() or "live"

    def _current_note_text(self, position: OkxPosition) -> str:
        key = _position_note_current_key(self._last_profile_name, self._note_environment(), position)
        record = self._current_notes.get(key)
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _current_note_summary(self, position: OkxPosition) -> str:
        return _format_position_note_summary(self._current_note_text(position))

    def _current_note_map(self) -> dict[str, str]:
        return {_position_tree_row_id(item): self._current_note_text(item) for item in self._raw_positions}

    def _selected_payload(self) -> dict[str, object] | None:
        item = self._position_tree.currentItem()
        if item is None:
            return None
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return None
        return self._position_row_payloads.get(row_key)

    def _selected_position(self) -> OkxPosition | None:
        payload = self._selected_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        return position if isinstance(position, OkxPosition) else None

    def _selected_option_for_shortcut(self) -> OkxPosition | None:
        position = self._selected_position()
        if position is None or position.inst_type != "OPTION":
            return None
        return position

    def _position_action_parent(self) -> QWidget:
        return self

    @staticmethod
    def _normalize_position_manual_flatten_mode(flatten_mode: str) -> str:
        normalized = str(flatten_mode or "").strip().lower()
        return "best_quote" if normalized == "best_quote" else "market"

    @staticmethod
    def _position_manual_flatten_mode_label(flatten_mode: str) -> str:
        return "鎸備拱涓€/鍗栦竴骞充粨" if AccountPositionsHomeWidget._normalize_position_manual_flatten_mode(flatten_mode) == "best_quote" else "甯備环骞充粨"

    def _build_selected_position_manual_flatten_config(self, position: OkxPosition) -> StrategyConfig:
        runtime = self._runtime
        environment = getattr(runtime, "environment", "live") if runtime is not None else "live"
        runtime_trade_mode = getattr(runtime, "trade_mode", "cross") if runtime is not None else "cross"
        normalized_mgn_mode = str(position.mgn_mode or "").strip().lower()
        trade_mode = normalized_mgn_mode if normalized_mgn_mode in {"cross", "isolated"} else runtime_trade_mode
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        direction = derive_position_direction(position)
        return StrategyConfig(
            inst_id=position.inst_id,
            bar="1m",
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if direction == "long" else "short_only",
            position_mode=position_mode,
            environment=environment,
            tp_sl_trigger_type="last",
            strategy_id="manual_position_flatten",
            poll_seconds=10.0,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=position.inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _selected_position_close_size(self, position: OkxPosition) -> Decimal:
        base = position.avail_position
        if base is None or base == 0:
            base = position.position
        return abs(base)

    def _selected_position_flatten_instrument(self, position: OkxPosition) -> Instrument:
        inst_id = str(position.inst_id or "").strip().upper()
        cached = self._position_instruments.get(inst_id)
        if isinstance(cached, Instrument):
            return cached
        get_cached_instrument = getattr(self._shared_client, "get_cached_instrument", None)
        if callable(get_cached_instrument):
            try:
                cached = get_cached_instrument(inst_id)
            except Exception:
                cached = None
            if isinstance(cached, Instrument):
                return cached
        try:
            return self._shared_client.get_instrument(inst_id, prefer_cached=True)
        except TypeError:
            return self._shared_client.get_instrument(inst_id)

    def _resolve_best_quote_flatten_price(self, instrument: Instrument, *, side: str) -> Decimal:
        order_book = None
        try:
            order_book = self._shared_client.get_order_book(instrument.inst_id, depth=5)
        except Exception:
            order_book = None
        ticker = self._shared_client.get_ticker(instrument.inst_id)
        if side == "buy":
            raw_price = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
            if raw_price is None or raw_price <= 0:
                raise ValueError(f"{instrument.inst_id} 褰撳墠缂哄皯涔颁竴浠凤紝鏃犳硶鎸変拱涓€鎸傚钩绌哄崟銆?)
            return snap_to_increment(raw_price, instrument.tick_size, "down")
        raw_price = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw_price is None or raw_price <= 0:
            raise ValueError(f"{instrument.inst_id} 褰撳墠缂哄皯鍗栦竴浠凤紝鏃犳硶鎸夊崠涓€鎸傚钩澶氬崟銆?)
        return snap_to_increment(raw_price, instrument.tick_size, "up")

    def _prepare_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[Credentials, StrategyConfig, Instrument, Decimal, str, str | None, str, str]:
        runtime = self._runtime
        if runtime is None:
            raise ValueError("褰撳墠娌℃湁鍙敤鐨?API 杩愯鏃讹紝鏃犳硶鎵ц骞充粨銆?)
        credentials = runtime.credentials
        config = self._build_selected_position_manual_flatten_config(position)
        instrument = self._selected_position_flatten_instrument(position)
        max_close = snap_to_increment(self._selected_position_close_size(position), instrument.lot_size, "down")
        if max_close < instrument.min_size:
            raise ValueError("褰撳墠閫変腑鎸佷粨鐨勫彲骞虫暟閲忎笉瓒虫渶灏忎笅鍗曢噺锛屾棤娉曠洿鎺ュ钩浠撱€?)
        if close_size is not None:
            if close_size <= 0:
                raise ValueError("骞充粨鏁伴噺蹇呴』澶т簬 0銆?)
            requested = snap_to_increment(close_size, instrument.lot_size, "down")
            if requested <= 0:
                raise ValueError("骞充粨鏁伴噺鎸夋渶灏忓彉鍔ㄥ崟浣嶅悜涓嬪彇鏁村悗涓?0锛岃澧炲ぇ鏁伴噺銆?)
            if requested > max_close:
                raise ValueError(f"骞充粨鏁伴噺涓嶈兘瓒呰繃褰撳墠鍙钩鏁伴噺 {format_decimal(max_close)}銆?)
            closeable_size = requested
        else:
            closeable_size = max_close
        direction = derive_position_direction(position)
        close_side = "sell" if direction == "long" else "buy"
        pos_side = None
        if config.position_mode == "long_short":
            normalized_pos_side = str(position.pos_side or "").strip().lower()
            pos_side = normalized_pos_side if normalized_pos_side in {"long", "short"} else direction
        normalized_mode = self._normalize_position_manual_flatten_mode(flatten_mode)
        return credentials, config, instrument, closeable_size, close_side, pos_side, direction, normalized_mode

    def _submit_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[OkxOrderResult, Decimal | None, str]:
        credentials, config, instrument, closeable_size, close_side, pos_side, _direction, normalized_mode = (
            self._prepare_selected_position_manual_flatten(position, flatten_mode, close_size=close_size)
        )
        if normalized_mode == "best_quote":
            price = self._resolve_best_quote_flatten_price(instrument, side=close_side)
            result = self._shared_client.place_simple_order(
                credentials,
                config,
                inst_id=position.inst_id,
                side=close_side,
                size=closeable_size,
                ord_type="limit",
                pos_side=pos_side,
                price=price,
                reduce_only=True,
            )
            return result, price, normalized_mode
        result = self._shared_client.place_simple_order(
            credentials,
            config,
            inst_id=position.inst_id,
            side=close_side,
            size=closeable_size,
            ord_type="market",
            pos_side=pos_side,
            reduce_only=True,
        )
        return result, None, normalized_mode

    def _schedule_selected_position_manual_flatten_follow_up_refresh(self, flatten_mode: str) -> None:
        normalized_mode = self._normalize_position_manual_flatten_mode(flatten_mode)
        if normalized_mode == "best_quote":
            QTimer.singleShot(450, self.refresh_view)
            QTimer.singleShot(1800, self.refresh_view)
            return
        QTimer.singleShot(650, self.refresh_view)

    def _finish_selected_position_manual_flatten_error(self, exc: Exception) -> None:
        self._selected_position_manual_flatten_running = False
        self._status_badge.setText("姝ｅ父")
        QMessageBox.critical(self, "骞充粨澶辫触", str(exc))

    def _finish_selected_position_manual_flatten_success(
        self,
        *,
        position: OkxPosition,
        result: OkxOrderResult,
        price: Decimal | None,
        normalized_flatten_mode: str,
        direction_label: str,
        close_side_label: str,
        submit_size_text: str,
    ) -> None:
        self._selected_position_manual_flatten_running = False
        self._status_badge.setText("姝ｅ父")
        mode_label = self._position_manual_flatten_mode_label(normalized_flatten_mode)
        order_id = (result.ord_id or "-").strip() or "-"
        client_order_id = (result.cl_ord_id or "-").strip() or "-"
        message = (
            "宸叉彁浜ら€変腑鎸佷粨骞充粨銆俓n\n"
            f"鍚堢害锛歿position.inst_id}\n"
            f"鏂瑰悜锛歿direction_label}\n"
            f"骞充粨鏁伴噺锛歿submit_size_text}\n"
            f"涓嬪崟鏂瑰悜锛歿close_side_label}\n"
            f"鏂瑰紡锛歿mode_label}\n"
            f"璁㈠崟ID锛歿order_id}\n"
            f"瀹㈡埛绔崟鍙凤細{client_order_id}"
        )
        if normalized_flatten_mode == "best_quote" and price is not None:
            message = f"{message}\n鎸傚崟浠凤細{format_decimal(price)}"
        QMessageBox.information(self, "骞充粨宸叉彁浜?, message)
        self._schedule_selected_position_manual_flatten_follow_up_refresh(normalized_flatten_mode)

    def flatten_selected_position(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        if self._selected_position_manual_flatten_running:
            QMessageBox.information(self, "骞充粨", "褰撳墠宸叉湁涓€绗旈€変腑鎸佷粨骞充粨鍦ㄦ彁浜や腑锛岃绋嶅€欍€?)
            return
        position = self._selected_position()
        if position is None:
            QMessageBox.information(self, "骞充粨", "璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉″叿浣撴寔浠撱€?)
            return
        try:
            (
                _credentials,
                _config,
                _instrument,
                preview_close_size,
                preview_close_side,
                _pos_side,
                preview_direction,
                _normalized_mode,
            ) = self._prepare_selected_position_manual_flatten(position, "market")
        except Exception as exc:
            QMessageBox.critical(self, "骞充粨澶辫触", str(exc))
            return

        direction_label = "澶氬ご" if preview_direction == "long" else "绌哄ご"
        close_side_label = "SELL 鍗栧嚭骞充粨" if preview_close_side == "sell" else "BUY 涔板叆骞充粨"
        hold_size_text = format_decimal(abs(position.position))
        closeable_size_text = format_decimal(self._selected_position_close_size(position))
        submit_size_text = format_decimal(preview_close_size)

        dialog = QMessageBox(self)
        dialog.setWindowTitle("骞充粨閫変腑")
        dialog.setIcon(QMessageBox.Icon.Question)
        dialog.setText(
            "\n".join(
                [
                    "璇烽€夋嫨杩欐瀵归€変腑鎸佷粨鐨勫钩浠撴柟寮忋€?,
                    "",
                    f"鍚堢害锛歿position.inst_id}",
                    f"鏂瑰悜锛歿direction_label}",
                    f"褰撳墠鎸佷粨锛歿hold_size_text}",
                    f"褰撳墠鍙钩锛歿closeable_size_text}",
                    f"鏈灏嗘姤鍗曞钩浠撴暟閲忥細{submit_size_text}",
                    f"瀹為檯鎶ュ崟鏂瑰悜锛歿close_side_label}",
                    "",
                    "璇存槑锛?,
                    "1. 甯備环骞充粨浼氱珛鍒绘寜甯傚満鍙垚浜や环鏍兼姤鍗曘€?,
                    "2. 鎸備拱涓€/鍗栦竴骞充粨浼氬厛鎸傚崟锛屾湭鎴愪氦鍓嶆寔浠撲笉浼氭秷澶便€?,
                    "3. 骞冲鎸夊崠涓€鎸傚崟锛屽钩绌烘寜涔颁竴鎸傚崟銆?,
                ]
            )
        )
        market_button = dialog.addButton("甯備环骞充粨", QMessageBox.ButtonRole.AcceptRole)
        best_quote_button = dialog.addButton("鎸備拱涓€/鍗栦竴", QMessageBox.ButtonRole.ActionRole)
        dialog.addButton(QMessageBox.StandardButton.Cancel)
        dialog.exec()
        clicked = dialog.clickedButton()
        if clicked not in {market_button, best_quote_button}:
            return
        flatten_mode = "market" if clicked is market_button else "best_quote"

        self._selected_position_manual_flatten_running = True
        self._status_badge.setText("骞充粨鎻愪氦涓?..")

        def _worker() -> None:
            try:
                result, price, normalized_mode = self._submit_selected_position_manual_flatten(position, flatten_mode)
            except Exception as exc:
                QTimer.singleShot(0, lambda exc=exc: self._finish_selected_position_manual_flatten_error(exc))
                return
            QTimer.singleShot(
                0,
                lambda result=result, price=price, normalized_mode=normalized_mode: self._finish_selected_position_manual_flatten_success(
                    position=position,
                    result=result,
                    price=price,
                    normalized_flatten_mode=normalized_mode,
                    direction_label=direction_label,
                    close_side_label=close_side_label,
                    submit_size_text=submit_size_text,
                ),
            )

        try:
            threading.Thread(target=_worker, name="qt-selected-position-flatten", daemon=True).start()
        except RuntimeError as exc:
            self._selected_position_manual_flatten_running = False
            self._status_badge.setText("姝ｅ父")
            QMessageBox.critical(self, "骞充粨澶辫触", f"绯荤粺绾跨▼璧勬簮涓嶈冻锛屾棤娉曟彁浜ゅ钩浠擄細{exc}")

    def _position_history_note_text(self, item: OkxPositionHistoryItem) -> str:
        key = _position_history_note_key(self._last_profile_name, self._note_environment(), item)
        record = self._history_notes.get(key)
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        selected_row = self._position_history_table.currentRow()
        selected_key = None
        if 0 <= selected_row < len(self._position_history_items):
            current = self._position_history_items[selected_row]
            selected_key = (current.update_time, current.inst_id, current.pos_side, current.direction)
        self._position_history_table.setRowCount(len(self._position_history_items))
        for row, item in enumerate(self._position_history_items):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column not in {2, 11}:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._position_history_table.setItem(row, column, cell)
        self._position_history_summary_label.setText(f"鍘嗗彶浠撲綅锛歿len(self._position_history_items)} 鏉?)
        target_row = -1
        if selected_key is not None:
            for index, item in enumerate(self._position_history_items):
                key = (item.update_time, item.inst_id, item.pos_side, item.direction)
                if key == selected_key:
                    target_row = index
                    break
        elif self._position_history_items:
            target_row = 0
        if target_row >= 0:
            self._position_history_table.selectRow(target_row)
        else:
            self._position_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶浠撲綅鐨勮鎯呫€?)
        self._refresh_position_history_detail()

    def _open_position_protection_dialog(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "璁剧疆鏈熸潈淇濇姢", "璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉℃湡鏉冧粨浣嶃€?)
            return
        if self._protection_dialog is None:
            self._protection_dialog = PositionProtectionDialog(
                manager=self._protection_manager,
                client=self._shared_client,
                runtime_provider=lambda: self._runtime,
                selected_option_provider=self._selected_option_for_shortcut,
                parent=self,
            )
        self._protection_dialog._refresh_from_selection(force=True)
        self._protection_dialog.show()
        self._protection_dialog.raise_()
        self._protection_dialog.activateWindow()

    def _open_option_roll_window(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "灞曟湡寤鸿", "璇峰厛鍦ㄥ綋鍓嶆寔浠撲腑閫変腑涓€鏉℃湡鏉冩寔浠撱€?)
            return
        if not is_short_option_position(position):
            QMessageBox.information(self, "灞曟湡寤鸿", "灞曟湡寤鸿绗竴鐗堝彧鏀寔鏈熸潈鍗栧嚭鏂瑰悜鎸佷粨銆?)
            return
        instrument = self._position_instruments.get(position.inst_id)
        if instrument is None:
            try:
                instrument = self._shared_client.get_instrument(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "灞曟湡寤鸿", f"璇诲彇鍚堢害淇℃伅澶辫触锛歿exc}")
                return
        ticker = self._position_tickers.get(position.inst_id)
        if ticker is None:
            try:
                ticker = self._shared_client.get_ticker(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "灞曟湡寤鸿", f"璇诲彇琛屾儏澶辫触锛歿exc}")
                return
        try:
            self._legacy_option_tools.open_option_roll(
                position=position,
                instrument=instrument,
                ticker=ticker,
                api_name=self._last_profile_name or "",
            )
        except Exception as exc:
            QMessageBox.critical(self, "灞曟湡寤鸿", f"鎵撳紑灞曟湡寤鸿澶辫触锛歿exc}")

    def edit_selected_position_note(self) -> None:
        position = self._selected_position()
        if position is None:
            QMessageBox.information(self, "澶囨敞", "璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉″叿浣撴寔浠撱€?)
            return
        dialog = NoteEditorDialog(
            title="缂栬緫鎸佷粨澶囨敞",
            prompt=f"涓?{position.inst_id} 濉啓澶囨敞銆傜暀绌哄悗淇濆瓨浼氭竻绌哄綋鍓嶆寔浠撳娉ㄣ€?,
            initial_value=self._current_note_text(position),
            parent=self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        record_key = _position_note_current_key(self._last_profile_name, self._note_environment(), position)
        if dialog.result_text:
            previous = self._current_notes.get(record_key)
            record = _build_current_position_note_record(
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                position=position,
                note=dialog.result_text,
                now_ms=int(time.time() * 1000),
                previous=previous,
            )
            if record is not None:
                self._current_notes[record_key] = record
        else:
            self._current_notes.pop(record_key, None)
        self._save_position_notes()
        self._render_positions_tree()

    def open_positions_column_window(self) -> None:
        dialog = ColumnSettingsDialog(
            column_defs=POSITION_COLUMNS,
            visible_columns=set(self._visible_column_ids),
            toggle_callback=self._toggle_column_visibility,
            parent=self,
        )
        dialog.exec()

    def _toggle_column_visibility(self, column_id: str) -> None:
        if column_id in self._visible_column_ids:
            if len(self._visible_column_ids) > 1:
                self._visible_column_ids.remove(column_id)
        else:
            self._visible_column_ids.add(column_id)
        self._apply_column_visibility()
        self._schedule_positions_view_prefs_save()

    def _apply_column_visibility(self) -> None:
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            self._position_tree.setColumnHidden(index, column_id not in self._visible_column_ids)

    def _load_positions_view_prefs(self) -> None:
        try:
            snapshot = load_account_positions_home_view_prefs()
        except Exception:
            return
        raw_visible_columns = snapshot.get("visible_columns")
        if isinstance(raw_visible_columns, list):
            loaded_visible_columns = {
                str(item).strip()
                for item in raw_visible_columns
                if str(item).strip() in {column_id for column_id, *_rest in POSITION_COLUMNS}
            }
            if loaded_visible_columns:
                self._visible_column_ids = loaded_visible_columns
        raw_tree_column_widths = snapshot.get("tree_column_widths")
        if isinstance(raw_tree_column_widths, dict):
            self._tree_column_width_overrides = {
                str(key).strip(): int(value)
                for key, value in raw_tree_column_widths.items()
                if str(key).strip() and str(value).strip().isdigit() and int(value) > 0
            }
        raw_position_kline_bar = str(snapshot.get("position_kline_bar") or "").strip()
        if raw_position_kline_bar in {bar for _text, bar in POSITION_KLINE_BAR_OPTIONS}:
            self._position_kline_last_bar = raw_position_kline_bar
        try:
            loaded_width = int(str(snapshot.get("position_kline_window_width", self._position_kline_window_width)).strip())
            if loaded_width > 0:
                self._position_kline_window_width = loaded_width
        except Exception:
            pass
        try:
            loaded_height = int(str(snapshot.get("position_kline_window_height", self._position_kline_window_height)).strip())
            if loaded_height > 0:
                self._position_kline_window_height = loaded_height
        except Exception:
            pass

    def _apply_tree_column_width_overrides(self) -> None:
        if not hasattr(self, "_position_tree"):
            return
        label_width = self._tree_column_width_overrides.get("__label__")
        if label_width:
            self._position_tree.setColumnWidth(0, label_width)
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            width = self._tree_column_width_overrides.get(column_id)
            if width:
                self._position_tree.setColumnWidth(index, width)

    @Slot()
    def _schedule_positions_view_prefs_save(self, *_args: object) -> None:
        if not hasattr(self, "_positions_view_prefs_save_timer"):
            return
        self._positions_view_prefs_save_timer.start(400)

    def _collect_tree_column_widths(self) -> dict[str, int]:
        if not hasattr(self, "_position_tree"):
            return dict(self._tree_column_width_overrides)
        widths = {"__label__": self._position_tree.columnWidth(0)}
        for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1):
            widths[column_id] = self._position_tree.columnWidth(index)
        return widths

    @Slot()
    def _save_positions_view_prefs_now(self) -> None:
        if hasattr(self, "_positions_view_prefs_save_timer") and self._positions_view_prefs_save_timer.isActive():
            self._positions_view_prefs_save_timer.stop()
        try:
            save_account_positions_home_view_prefs(
                visible_columns=sorted(self._visible_column_ids),
                tree_column_widths=self._collect_tree_column_widths(),
                position_kline_bar=self._position_kline_last_bar,
                position_kline_window_width=self._position_kline_window_width,
                position_kline_window_height=self._position_kline_window_height,
            )
        except Exception:
            return

    def _visible_position_list(self) -> list[OkxPosition]:
        inst_type = str(self._type_combo.currentData() or "").strip().upper()
        keyword = self._keyword_edit.text()
        return _filter_positions(
            self._raw_positions,
            inst_type=inst_type,
            keyword=keyword,
            note_texts=self._current_note_map(),
        )

    def _render_positions_tree(self) -> None:
        self._visible_positions = self._visible_position_list()
        selected_key = ""
        current = self._position_tree.currentItem()
        if current is not None:
            data = current.data(0, Qt.ItemDataRole.UserRole)
            if isinstance(data, str):
                selected_key = data

        self._position_tree.clear()
        self._position_row_payloads.clear()
        groups = _group_positions_for_tree(self._visible_positions)
        bold_font = QFont()
        bold_font.setBold(True)

        for asset_label, buckets in groups.items():
            asset_id = _asset_group_row_id(asset_label)
            asset_positions = [item for bucket in buckets.values() for item in bucket]
            asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
            asset_item = self._make_tree_item(
                row_key=asset_id,
                label=f"{asset_label} 椋庨櫓鍗曞厓",
                values=_build_group_row_values("缁勫悎", asset_metrics),
                kind="group",
                payload_item=asset_positions,
                payload_metrics=asset_metrics,
            )
            asset_item.setFont(0, bold_font)
            self._position_tree.addTopLevelItem(asset_item)
            asset_item.setExpanded(asset_id in self._expanded_row_keys)

            for bucket_label, bucket_positions in buckets.items():
                if bucket_label == "__DIRECT__":
                    for position in bucket_positions:
                        asset_item.addChild(self._build_position_item(position))
                    continue
                bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                bucket_metrics = _aggregate_position_metrics(
                    bucket_positions,
                    self._upl_usdt_prices,
                    self._position_instruments,
                )
                bucket_item = self._make_tree_item(
                    row_key=bucket_id,
                    label=bucket_label,
                    values=_build_group_row_values("鍒嗙粍", bucket_metrics),
                    kind="group",
                    payload_item=bucket_positions,
                    payload_metrics=bucket_metrics,
                )
                bucket_item.setFont(0, bold_font)
                asset_item.addChild(bucket_item)
                bucket_item.setExpanded(bucket_id in self._expanded_row_keys)
                for position in bucket_positions:
                    bucket_item.addChild(self._build_position_item(position))

        self._positions_hint.setText(f"褰撳墠鏄剧ず {len(self._visible_positions)} 鏉℃寔浠?| 鐐瑰嚮浠讳竴琛屾煡鐪嬭鎯呫€?)
        self._update_summary_text()
        self._restore_tree_selection(selected_key)
        self._update_filter_shortcuts()
        self._sync_order_watchlist()
        visible_inst_ids = {item.inst_id.strip().upper() for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()
        self._refresh_detail()
        self._update_expand_toggle_button()

    def _make_tree_item(
        self,
        *,
        row_key: str,
        label: str,
        values: tuple[str, ...],
        kind: str,
        payload_item: object,
        payload_metrics: dict[str, object] | None,
    ) -> QTreeWidgetItem:
        item = QTreeWidgetItem([label, *list(values)])
        item.setData(0, Qt.ItemDataRole.UserRole, row_key)
        self._position_row_payloads[row_key] = {
            "kind": kind,
            "label": label,
            "item": payload_item,
            "metrics": payload_metrics,
        }
        for index, (_column_id, _heading, _width, alignment) in enumerate(POSITION_COLUMNS, start=1):
            item.setTextAlignment(index, int(alignment | Qt.AlignmentFlag.AlignVCenter))
        return item

    def _build_position_item(self, position: OkxPosition) -> QTreeWidgetItem:
        row_key = _position_tree_row_id(position)
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        values = (
            position.inst_type,
            _format_margin_mode(position.mgn_mode),
            _format_position_option_price_component(position, self._upl_usdt_prices, component="time_value"),
            _format_position_option_component_usdt(position, self._upl_usdt_prices, component="time_value"),
            _format_position_option_price_component(position, self._upl_usdt_prices, component="intrinsic_value"),
            _format_position_option_component_usdt(position, self._upl_usdt_prices, component="intrinsic_value"),
            _format_position_quote_price(position, self._position_instruments, self._position_tickers, side="bid"),
            _format_position_quote_price_usdt(position, self._position_tickers, self._upl_usdt_prices, side="bid"),
            _format_position_quote_price(position, self._position_instruments, self._position_tickers, side="ask"),
            _format_position_quote_price_usdt(position, self._position_tickers, self._upl_usdt_prices, side="ask"),
            _format_mark_price(position),
            _format_position_mark_price_usdt(position, self._upl_usdt_prices),
            _format_position_avg_price(position, self._position_instruments),
            _format_position_avg_price_usdt(position, self._upl_usdt_prices),
            _format_optional_approx_usdt(
                _position_signed_open_value_approx_usdt(position, self._position_instruments, self._upl_usdt_prices)
            ),
            _format_position_size(position, self._position_instruments),
            _format_option_trade_side_display(position),
            _format_position_unrealized_pnl(position),
            _format_optional_usdt(_position_unrealized_pnl_usdt(position, self._upl_usdt_prices)),
            _format_position_realized_pnl(position),
            _format_optional_usdt(_position_realized_pnl_usdt(position, self._upl_usdt_prices)),
            _format_position_market_value(position, self._position_instruments, self._upl_usdt_prices),
            _format_optional_decimal(position.liquidation_price),
            _format_ratio(position.margin_ratio, places=2),
            _format_optional_integer(position.initial_margin),
            _format_optional_integer(position.maintenance_margin),
            _format_optional_decimal_fixed(_position_delta_value(position, self._position_instruments), places=5),
            _format_optional_decimal_fixed(position.gamma, places=5),
            _format_optional_decimal_fixed(position.vega, places=5),
            _format_optional_decimal_fixed(position.theta, places=5),
            _format_optional_usdt_precise(_position_theta_usdt(position, self._upl_usdt_prices), places=2),
            self._current_note_summary(position),
        )
        item = self._make_tree_item(
            row_key=row_key,
            label=label,
            values=values,
            kind="position",
            payload_item=position,
            payload_metrics=None,
        )
        pnl_color = None
        if position.unrealized_pnl is not None:
            pnl_color = QColor("#13803d" if position.unrealized_pnl > 0 else "#c23b3b" if position.unrealized_pnl < 0 else "#1f2937")
        if pnl_color is not None:
            for index in (18, 19, 20, 21):
                item.setForeground(index, pnl_color)
        if str(position.mgn_mode or "").strip().lower() == "cross":
            for index in range(0, self._position_tree.columnCount()):
                item.setBackground(index, QColor("#f4f8ff"))
        elif str(position.mgn_mode or "").strip().lower() == "isolated":
            for index in range(0, self._position_tree.columnCount()):
                item.setBackground(index, QColor("#fff4e5"))
        return item

    def _restore_tree_selection(self, selected_key: str) -> None:
        target_item = None
        if selected_key:
            for item in self._iter_tree_items():
                data = item.data(0, Qt.ItemDataRole.UserRole)
                if data == selected_key:
                    target_item = item
                    break
        if target_item is None:
            target_item = self._position_tree.topLevelItem(0)
        if target_item is not None:
            self._position_tree.setCurrentItem(target_item)

    def _iter_tree_items(self) -> list[QTreeWidgetItem]:
        items: list[QTreeWidgetItem] = []

        def _walk(parent: QTreeWidgetItem) -> None:
            items.append(parent)
            for index in range(parent.childCount()):
                _walk(parent.child(index))

        for index in range(self._position_tree.topLevelItemCount()):
            _walk(self._position_tree.topLevelItem(index))
        return items

    def _group_tree_items(self) -> list[QTreeWidgetItem]:
        result: list[QTreeWidgetItem] = []
        for item in self._iter_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            payload = self._position_row_payloads.get(row_key) if isinstance(row_key, str) else None
            if isinstance(payload, dict) and payload.get("kind") == "group":
                result.append(item)
        return result

    def _all_group_rows_expanded(self) -> bool:
        group_items = self._group_tree_items()
        return bool(group_items) and all(item.isExpanded() for item in group_items)

    def _update_expand_toggle_button(self) -> None:
        if not hasattr(self, "_expand_toggle_button"):
            return
        self._expand_toggle_button.setText("鎶樺彔鍏ㄩ儴" if self._all_group_rows_expanded() else "灞曞紑鍏ㄩ儴")

    def _toggle_all_positions(self) -> None:
        if self._all_group_rows_expanded():
            self._collapse_all_positions()
            return
        self._expand_all_positions()

    def _expand_all_positions(self) -> None:
        for item in self._group_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            if not isinstance(row_key, str):
                continue
            self._expanded_row_keys.add(row_key)
            item.setExpanded(True)
        self._update_expand_toggle_button()

    def _collapse_all_positions(self) -> None:
        for item in self._group_tree_items():
            row_key = item.data(0, Qt.ItemDataRole.UserRole)
            if not isinstance(row_key, str):
                continue
            self._expanded_row_keys.discard(row_key)
            item.setExpanded(False)
        self._update_expand_toggle_button()

    def _on_tree_item_expanded(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        payload = self._position_row_payloads.get(row_key)
        if isinstance(payload, dict) and payload.get("kind") == "group":
            self._expanded_row_keys.add(row_key)
            self._update_expand_toggle_button()

    def _on_tree_item_collapsed(self, item: QTreeWidgetItem) -> None:
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return
        self._expanded_row_keys.discard(row_key)
        self._update_expand_toggle_button()

    def _update_summary_text(self) -> None:
        total_count = len(self._raw_positions)
        visible_count = len(self._visible_positions)
        parts = [
            f"API閰嶇疆锛歿self._last_profile_name or '-'}",
            self._account_status.text(),
        ]
        if total_count:
            text = f"褰撳墠浠撲綅锛坽total_count}锛?
            if visible_count != total_count:
                text += f"锛屽綋鍓嶆樉绀?{visible_count}"
            parts.append(text)
        else:
            parts.append("褰撳墠娌℃湁鎸佷粨")
        keyword = self._keyword_edit.text().strip().upper()
        type_label = self._type_combo.currentText().strip()
        if keyword or type_label != "鍏ㄩ儴绫诲瀷":
            parts.append(f"绛涢€夛細{type_label if type_label != '鍏ㄩ儴绫诲瀷' else ''} {'| ' + keyword if keyword else ''}".strip())
        self._summary_label.setText(" | ".join(part for part in parts if part))

    def _update_filter_shortcuts(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._apply_contract_button.setEnabled(enabled)
        self._apply_expiry_button.setEnabled(enabled)
        if not enabled:
            self._filter_hint.setText("閫変腑鏈熸潈鍚庯紝鍙竴閿甫鍏ュ悎绾︽垨鍒版湡鍓嶇紑銆?)
            return
        self._filter_hint.setText(f"宸查€夋湡鏉冿細{contract} | 蹇嵎绛涢€夛細鍚堢害={contract} | 鍒版湡鍓嶇紑={expiry_prefix}")

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            QMessageBox.information(self, "蹇嵎绛涢€?, "璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉℃湡鏉冨悎绾︺€?)
            return
        self._keyword_edit.setText(contract)

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            QMessageBox.information(self, "蹇嵎绛涢€?, "璇峰厛鍦ㄥ綋鍓嶆寔浠撻噷閫変腑涓€鏉℃湡鏉冨悎绾︺€?)
            return
        self._keyword_edit.setText(expiry_prefix)

    def _clear_filters(self) -> None:
        self._type_combo.setCurrentIndex(0)
        self._keyword_edit.clear()

    def _sync_order_watchlist(self) -> None:
        if self._order_feed is None:
            return
        self._order_feed.set_watched_inst_ids({item.inst_id for item in self._visible_positions})

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        selected_ord_id = ""
        current_row = self._orders_table.currentRow()
        if 0 <= current_row < len(self._visible_orders):
            selected_ord_id = self._visible_orders[current_row].ord_id
        self._orders_summary_label.setText(
            f"褰撳墠濮旀墭锛歿len(self._visible_orders)} 鏉?| 浠呮樉绀哄綋鍓嶆寔浠撶浉鍏冲悎绾︺€?
        )
        self._orders_table.setRowCount(len(self._visible_orders))
        for row, order in enumerate(self._visible_orders):
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                order.inst_id,
                order.inst_type or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                order.td_mode or "-",
                order.client_order_id or "-",
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                if column not in {1, 10}:
                    item.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._orders_table.setItem(row, column, item)
        target_row = -1
        if selected_ord_id:
            for index, order in enumerate(self._visible_orders):
                if order.ord_id == selected_ord_id:
                    target_row = index
                    break
        elif self._visible_orders:
            target_row = 0
        if target_row >= 0:
            self._orders_table.selectRow(target_row)
        else:
            self._orders_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑褰撳墠濮旀墭鐨勮鎯呫€?)
        self._refresh_current_order_detail()

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        filtered = self._filtered_current_orders()
        selected_ord_id = ""
        row = self._orders_table.currentRow()
        if 0 <= row < len(filtered):
            selected_ord_id = filtered[row].ord_id
        self._orders_summary_label.setText(f"褰撳墠濮旀墭锛歿len(filtered)} 鏉?| 浠呮樉绀哄綋鍓嶆寔浠撶浉鍏冲悎绾︺€?)
        self._orders_table.setRowCount(len(filtered))
        for row, order in enumerate(filtered):
            feed_source = str(order.raw.get("_feed_source") or "").strip().lower()
            source_kind = str(order.raw.get("_source_kind") or "").strip().lower()
            if feed_source == "rest_pending" and source_kind == "algo":
                source_label = "REST 绠楁硶"
            elif feed_source == "rest_pending":
                source_label = "REST pending"
            else:
                source_label = "WS 褰撳墠"
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                source_label,
                order.inst_type or "-",
                order.inst_id or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                "-",
                "-",
                order.ord_id or "-",
                order.client_order_id or "-",
            )
            self._set_table_row(self._orders_table, row, values, left_align={3, 13})
        self._current_order_rows = filtered
        self._restore_table_selection(self._orders_table, filtered, selected_ord_id, lambda item: item.ord_id or "")
        self._refresh_current_order_detail()

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        filtered = self._filtered_current_orders()
        selected_ord_id = ""
        row = self._orders_table.currentRow()
        if 0 <= row < len(filtered):
            selected_ord_id = filtered[row].ord_id
        self._orders_summary_label.setText(f"褰撳墠濮旀墭锛歿len(filtered)} 鏉?| 浠呮樉绀哄綋鍓嶆寔浠撶浉鍏冲悎绾︺€?)
        self._orders_table.setRowCount(len(filtered))
        for row, order in enumerate(filtered):
            feed_source = str(order.raw.get("_feed_source") or "").strip().lower()
            source_kind = str(order.raw.get("_source_kind") or "").strip().lower()
            if feed_source == "rest_pending" and source_kind == "algo":
                source_label = "REST 绠楁硶"
            elif feed_source == "rest_pending":
                source_label = "REST pending"
            else:
                source_label = "WS 褰撳墠"
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                source_label,
                order.inst_type or "-",
                order.inst_id or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                "-",
                "-",
                order.ord_id or "-",
                order.client_order_id or "-",
            )
            self._set_table_row(self._orders_table, row, values, left_align={3, 13})
        self._current_order_rows = filtered
        self._restore_table_selection(self._orders_table, filtered, selected_ord_id, lambda item: item.ord_id or "")
        self._refresh_current_order_detail()

    def _refresh_current_order_detail(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            self._orders_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑褰撳墠濮旀墭鐨勮鎯呫€?)
            return
        order = self._visible_orders[row]
        lines = [
            f"鍚堢害锛歿order.inst_id or '-'}",
            f"绫诲瀷锛歿order.inst_type or '-'}",
            f"鐘舵€侊細{_format_trade_order_state(order.state)}",
            f"鏂瑰悜锛歿_format_history_side(order.side or '-', order.pos_side or '')}",
            f"浜ゆ槗妯″紡锛歿order.td_mode or '-'}",
            f"濮旀墭绫诲瀷锛歿order.ord_type or '-'}",
            f"濮旀墭浠凤細{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"濮旀墭閲忥細{_format_trade_order_size(order.size)}",
            f"宸叉垚浜わ細{_format_trade_order_size(order.filled_size)}",
            f"鎴愪氦鍧囦环锛歿_format_trade_order_price(order.avg_price, order.inst_id, order.inst_type or '')}",
            f"鏇存柊鏃堕棿锛歿_format_okx_ms_timestamp(order.update_time)}",
            f"鍒涘缓鏃堕棿锛歿_format_okx_ms_timestamp(order.created_time)}",
            f"reduceOnly锛歿'鏄? if order.reduce_only is True else '鍚? if order.reduce_only is False else '-'}",
            f"ordId锛歿order.ord_id or '-'}",
            f"clOrdId锛歿order.client_order_id or '-'}",
            "",
            "鍘熷 WS 鍥炴姤锛?,
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText("\n".join(lines))

    def _refresh_position_history_detail(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        row = self._position_history_table.currentRow()
        if row < 0 or row >= len(self._position_history_items):
            self._position_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶浠撲綅鐨勮鎯呫€?)
            return
        item = self._position_history_items[row]
        self._position_history_detail.setPlainText(
            _build_position_history_detail_text(
                item,
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(item),
            )
        )

    def _refresh_detail(self) -> None:
        payload = self._selected_payload()
        if payload is None:
            self._detail_text.setPlainText("鐐瑰嚮浠讳竴琛屾煡鐪嬫寔浠撹鎯呫€?)
            return
        if payload.get("kind") == "position":
            position = payload.get("item")
            if isinstance(position, OkxPosition):
                self._detail_text.setPlainText(
                    _build_position_detail_text(
                        position,
                        self._upl_usdt_prices,
                        self._position_instruments,
                        note=self._current_note_text(position),
                    )
                )
                return
        label = payload.get("label")
        positions = payload.get("item")
        metrics = payload.get("metrics")
        if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
            self._detail_text.setPlainText(
                _build_group_detail_text(
                    label,
                    positions,
                    metrics,
                    self._upl_usdt_prices,
                    self._position_instruments,
                )
            )
            return
        self._detail_text.setPlainText("鐐瑰嚮浠讳竴琛屾煡鐪嬫寔浠撹鎯呫€?)

    def _show_account_overview(self) -> None:
        dialog = AccountOverviewDialog(summary_text=self._build_account_overview_summary_text(), parent=self)
        dialog.exec()

    def _build_account_overview_summary_text(self) -> str:
        raw_positions = list(self._raw_positions)
        visible_positions = list(self._visible_positions)
        position_metrics = _aggregate_position_metrics(raw_positions, self._upl_usdt_prices, self._position_instruments)
        visible_metrics = _aggregate_position_metrics(visible_positions, self._upl_usdt_prices, self._position_instruments)
        type_counts = Counter(str(item.inst_type or "-").upper() or "-" for item in raw_positions)
        visible_type_counts = Counter(str(item.inst_type or "-").upper() or "-" for item in visible_positions)
        option_long = sum(1 for item in raw_positions if str(item.inst_type or "").upper() == "OPTION" and derive_position_direction(item) == "long")
        option_short = sum(1 for item in raw_positions if str(item.inst_type or "").upper() == "OPTION" and derive_position_direction(item) == "short")
        keyword = self._keyword_edit.text().strip()
        type_filter = self._type_combo.currentText().strip() or "鍏ㄩ儴绫诲瀷"
        runtime = self._runtime
        environment = getattr(runtime, "environment", "") if runtime is not None else ""
        environment_label = "瀹炵洏 live" if str(environment).lower() == "live" else ("妯℃嫙 demo" if str(environment).lower() == "demo" else "-")

        lines = [
            "璐︽埛鍩虹",
            f"褰撳墠 API锛歿self._last_profile_name or '-'}",
            f"鐜锛歿environment_label}",
            f"鎸佷粨鎬绘暟锛歿len(raw_positions)}",
            f"褰撳墠鏄剧ず锛歿len(visible_positions)}",
            f"褰撳墠濮旀墭锛歿len(self._visible_orders)}",
            f"褰撳墠绛涢€夛細绫诲瀷={type_filter} | 鍏抽敭瀛?{keyword or '-'}",
            "",
            "鎸佷粨缁撴瀯",
            "鍏ㄩ儴鎸佷粨绫诲瀷鍒嗗竷锛?
            + (" | ".join(f"{inst_type} {count}" for inst_type, count in sorted(type_counts.items())) if type_counts else "-"),
            "褰撳墠鏄剧ず绫诲瀷鍒嗗竷锛?
            + (" | ".join(f"{inst_type} {count}" for inst_type, count in sorted(visible_type_counts.items())) if visible_type_counts else "-"),
            f"鏈熸潈鏂瑰悜锛氬澶?{option_long} | 绌哄ご {option_short}",
            "",
            "鎸佷粨姹囨€伙紙鍏ㄩ儴锛?,
            f"娴泩浜忥細{_format_optional_decimal_fixed(position_metrics.get('upl') if isinstance(position_metrics.get('upl'), Decimal) else None, places=5, with_sign=True)}",
            f"娴泩鈮圲SDT锛歿_format_optional_usdt(position_metrics.get('upl_usdt') if isinstance(position_metrics.get('upl_usdt'), Decimal) else None)}",
            f"宸插疄鐜扮泩浜忥細{_format_optional_decimal_fixed(position_metrics.get('realized') if isinstance(position_metrics.get('realized'), Decimal) else None, places=5, with_sign=True)}",
            f"宸插疄鐜扳増USDT锛歿_format_optional_usdt(position_metrics.get('realized_usdt') if isinstance(position_metrics.get('realized_usdt'), Decimal) else None)}",
            f"寮€浠撲环鍊尖増USDT锛歿_format_optional_approx_usdt(position_metrics.get('open_value_usdt') if isinstance(position_metrics.get('open_value_usdt'), Decimal) else None)}",
            f"甯傚€尖増USDT锛歿_format_optional_approx_usdt(position_metrics.get('market_value_usdt') if isinstance(position_metrics.get('market_value_usdt'), Decimal) else None)}",
            f"Delta(PA)锛歿_format_optional_decimal_fixed(position_metrics.get('delta') if isinstance(position_metrics.get('delta'), Decimal) else None, places=5)}",
            f"Gamma(PA)锛歿_format_optional_decimal_fixed(position_metrics.get('gamma') if isinstance(position_metrics.get('gamma'), Decimal) else None, places=5)}",
            f"Vega(PA)锛歿_format_optional_decimal_fixed(position_metrics.get('vega') if isinstance(position_metrics.get('vega'), Decimal) else None, places=5)}",
            f"Theta(PA)锛歿_format_optional_decimal_fixed(position_metrics.get('theta') if isinstance(position_metrics.get('theta'), Decimal) else None, places=5)}",
            f"Theta鈮圲SDT锛歿_format_optional_usdt_precise(position_metrics.get('theta_usdt') if isinstance(position_metrics.get('theta_usdt'), Decimal) else None, places=2)}",
            f"鍒濆淇濊瘉閲?IMR)锛歿_format_optional_integer(position_metrics.get('imr') if isinstance(position_metrics.get('imr'), Decimal) else None)}",
            f"缁存寔淇濊瘉閲?MMR)锛歿_format_optional_integer(position_metrics.get('mmr') if isinstance(position_metrics.get('mmr'), Decimal) else None)}",
            "",
            "鎸佷粨姹囨€伙紙褰撳墠鏄剧ず锛?,
            f"娴泩浜忥細{_format_optional_decimal_fixed(visible_metrics.get('upl') if isinstance(visible_metrics.get('upl'), Decimal) else None, places=5, with_sign=True)}",
            f"娴泩鈮圲SDT锛歿_format_optional_usdt(visible_metrics.get('upl_usdt') if isinstance(visible_metrics.get('upl_usdt'), Decimal) else None)}",
            f"宸插疄鐜扳増USDT锛歿_format_optional_usdt(visible_metrics.get('realized_usdt') if isinstance(visible_metrics.get('realized_usdt'), Decimal) else None)}",
            f"甯傚€尖増USDT锛歿_format_optional_approx_usdt(visible_metrics.get('market_value_usdt') if isinstance(visible_metrics.get('market_value_usdt'), Decimal) else None)}",
        ]

        if runtime is not None:
            try:
                overview = self._shared_client.get_account_overview(
                    runtime.credentials,
                    environment=runtime.environment,
                    prefer_cache=True,
                )
                config = self._shared_client.get_account_config(
                    runtime.credentials,
                    environment=runtime.environment,
                )
            except Exception as exc:
                lines.extend(
                    [
                        "",
                        "璐︽埛璧勪骇",
                        f"璇诲彇澶辫触锛歿exc}",
                    ]
                )
                return "\n".join(lines)

            lines.extend(
                [
                    "",
                    "璐︽埛璧勪骇",
                    f"璐︽埛妯″紡锛歿_format_account_level_text(getattr(config, 'account_level', None))}",
                    f"鎸佷粨妯″紡锛歿_format_account_position_mode_text(getattr(config, 'position_mode', None))}",
                    f"Greeks 绫诲瀷锛歿_format_greeks_type_text(getattr(config, 'greeks_type', None))}",
                    f"鑷姩鍊熷竵锛歿_format_bool_text(getattr(config, 'auto_loan', None))}",
                    f"鎬绘潈鐩婏細{_format_optional_usdt_precise(getattr(overview, 'total_equity', None), places=2, with_sign=False)}",
                    f"璋冩暣鍚庢潈鐩婏細{_format_optional_usdt_precise(getattr(overview, 'adjusted_equity', None), places=2, with_sign=False)}",
                    f"鍙敤鏉冪泭锛歿_format_optional_usdt_precise(getattr(overview, 'available_equity', None), places=2, with_sign=False)}",
                    f"鏈疄鐜扮泩浜忥細{_format_optional_usdt_precise(getattr(overview, 'unrealized_pnl', None), places=2)}",
                    f"鍒濆淇濊瘉閲?IMR)锛歿_format_optional_usdt_precise(getattr(overview, 'initial_margin', None), places=2, with_sign=False)}",
                    f"缁存寔淇濊瘉閲?MMR)锛歿_format_optional_usdt_precise(getattr(overview, 'maintenance_margin', None), places=2, with_sign=False)}",
                    f"璁㈠崟鍐荤粨锛歿_format_optional_usdt_precise(getattr(overview, 'order_frozen', None), places=2, with_sign=False)}",
                    f"鎬诲悕涔変环鍊?USD)锛歿_format_optional_usdt_precise(getattr(overview, 'notional_usd', None), places=2, with_sign=False)}",
                ]
            )

            assets = [
                asset
                for asset in getattr(overview, "details", ())
                if (asset.equity_usd is not None and asset.equity_usd != 0)
                or (asset.equity is not None and asset.equity != 0)
                or (asset.available_balance is not None and asset.available_balance != 0)
            ]
            if assets:
                lines.extend(["", f"璧勪骇鏄庣粏 Top {min(len(assets), 12)}"])
                for index, asset in enumerate(assets[:12], start=1):
                    lines.append(
                        f"{index:02d}. {asset.ccy or '-'}"
                        f" | 鏉冪泭={_format_optional_decimal(asset.equity)}"
                        f" | 鍙敤={_format_optional_decimal(asset.available_balance)}"
                        f" | 鍙敤鏉冪泭={_format_optional_decimal(asset.available_equity)}"
                        f" | 鎶樺悎USD={_format_optional_usdt_precise(asset.equity_usd, places=2, with_sign=False)}"
                        f" | 鏈疄鐜?{_format_optional_decimal(asset.unrealized_pnl, with_sign=True)}"
                        f" | 璐熷€?{_format_optional_decimal(asset.liability)}"
                    )

        return "\n".join(lines)

    def _toggle_detail_panel(self) -> None:
        visible = not self._detail_panel.isHidden()
        self._detail_panel.setVisible(not visible)
        self._update_panel_toggle_buttons()

    def _toggle_history_panel(self) -> None:
        visible = not self._history_panel.isHidden()
        self._history_panel.setVisible(not visible)
        self._update_panel_toggle_buttons()

    def _update_panel_toggle_buttons(self) -> None:
        if hasattr(self, "_detail_toggle_button") and hasattr(self, "_detail_panel"):
            self._detail_toggle_button.setText("鎶樺彔鎸佷粨璇︽儏" if not self._detail_panel.isHidden() else "灞曞紑鎸佷粨璇︽儏")
        if hasattr(self, "_history_toggle_button") and hasattr(self, "_history_panel"):
            self._history_toggle_button.setText("鎶樺彔鍘嗗彶鍖哄煙" if not self._history_panel.isHidden() else "灞曞紑鍘嗗彶鍖哄煙")

    def _show_not_ready_action(self) -> None:
        QMessageBox.information(self, "杩佺Щ涓?, "杩欎釜鍏ュ彛宸茬粡棰勭暀鍒颁富椤典笂锛屼笅涓€姝ヤ細鎸夋棫椤甸潰閫昏緫缁х画鎺ュ叆銆?)

    def _apply_filters(self, *_args: object) -> None:
            QMessageBox.warning(self, "切换失败", f"API 配置 {target} 不可用，请检查凭证。")

    def _on_profile_changed(self, *_args: object) -> None:
        if self._profile_switch_guard:
            return
        target = self._current_profile_name()
        if not target or target == self._last_profile_name:
            return
        self._profile_change_serial += 1
        serial = self._profile_change_serial
        QTimer.singleShot(0, lambda target=target, serial=serial: self._apply_profile_change(target, serial))
        return
        if not ensure_profile_unlocked(self, target, self._profile_snapshots, self._unlocked_profiles):
            self._profile_switch_guard = True
            previous_index = self._profile_combo.findText(self._last_profile_name)
            self._profile_combo.setCurrentIndex(previous_index if previous_index >= 0 else 0)
            self._profile_switch_guard = False
            return
        runtime = load_runtime(target)
        if runtime is None:
            QMessageBox.warning(self, "鍒囨崲澶辫触", f"API 閰嶇疆 {target} 涓嶅彲鐢紝璇锋鏌ュ嚟璇併€?)
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._start_private_threads(force_restart=True)

    def _apply_profile_change(self, target: str, serial: int) -> None:
        if serial != self._profile_change_serial:
            return
        if self._profile_switch_guard:
            return
        current = self._current_profile_name()
        if not target or current != target or target == self._last_profile_name:
            return
        if not ensure_profile_unlocked(self, target, self._profile_snapshots, self._unlocked_profiles):
            self._restore_previous_profile_selection()
            return
        runtime = load_runtime(target)
        if runtime is None:
            QMessageBox.warning(self, "閸掑洦宕叉径杈Е", f"API 闁板秶鐤?{target} 娑撳秴褰查悽顭掔礉鐠囬攱顥呴弻銉ュ殶鐠囦降鈧?)
            self._restore_previous_profile_selection()
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._start_private_threads(force_restart=True)

    def _restore_previous_profile_selection(self) -> None:
        self._profile_switch_guard = True
        previous_index = self._profile_combo.findText(self._last_profile_name)
        self._profile_combo.setCurrentIndex(previous_index if previous_index >= 0 else 0)
        self._profile_switch_guard = False

    def _on_position_selected(self) -> None:
        self._update_filter_shortcuts()
        self._refresh_detail()

    def _position_for_tree_item(self, item: QTreeWidgetItem | None) -> OkxPosition | None:
        if item is None:
            return None
        row_key = item.data(0, Qt.ItemDataRole.UserRole)
        if not isinstance(row_key, str):
            return None
        payload = self._position_row_payloads.get(row_key)
        if not isinstance(payload, dict) or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        return position if isinstance(position, OkxPosition) else None

    @Slot(QTreeWidgetItem, int)
    def _on_position_tree_clicked(self, item: QTreeWidgetItem, column: int) -> None:
        if column != 0:
            return
        position = self._position_for_tree_item(item)
        if position is None:
            return
        self._open_position_kline(position)

    def _open_position_kline(self, position: OkxPosition) -> None:
        if self._instrument_kline_dialog is None:
            self._instrument_kline_dialog = InstrumentKlineDialog(
                initial_bar=self._position_kline_last_bar,
                initial_width=self._position_kline_window_width,
                initial_height=self._position_kline_window_height,
                prefs_changed=self._on_position_kline_prefs_changed,
                parent=self,
            )
        self._instrument_kline_dialog.show_instrument(inst_id=position.inst_id, inst_type=position.inst_type)

    def _on_position_kline_prefs_changed(self, bar: str, width: int, height: int) -> None:
        self._position_kline_last_bar = bar if bar in {item_bar for _text, item_bar in POSITION_KLINE_BAR_OPTIONS} else "1H"
        self._position_kline_window_width = max(int(width), 320)
        self._position_kline_window_height = max(int(height), 240)
        self._schedule_positions_view_prefs_save()

    @Slot(str)
    def _set_account_status(self, text: str) -> None:
        self._account_status.setText(text)
        self._update_summary_text()

    @Slot(str)
    def _set_order_status(self, text: str) -> None:
        self._order_status.setText(text)

    @Slot(str)
    def _set_position_history_status(self, text: str) -> None:
        if hasattr(self, "_position_history_summary_label"):
            self._position_history_summary_label.setText(text)

    @Slot(object)
    def _apply_positions_summary(self, _positions: object) -> None:
        self._status_badge.setText("姝ｅ父")

    @Slot(object)
    def _apply_positions_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        positions = payload.get("positions")
        self._raw_positions = list(positions) if isinstance(positions, list) else []
        instruments = payload.get("position_instruments")
        tickers = payload.get("position_tickers")
        prices = payload.get("upl_usdt_prices")
        self._position_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_tickers = dict(tickers) if isinstance(tickers, dict) else {}
        self._upl_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        if self._last_profile_name:
            _reconcile_current_position_note_records(
                self._current_notes,
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                positions=self._raw_positions,
                now_ms=int(time.time() * 1000),
            )
        self._render_positions_tree()

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        visible_inst_ids = {item.inst_id for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _build_history_tabs_v2(self) -> QWidget:
        self._history_panel = QFrame()
        self._history_panel.setObjectName("Panel")
        layout = QVBoxLayout(self._history_panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_current_orders_tab_v2(), "褰撳墠濮旀墭")
        self._tabs.addTab(self._build_placeholder_tab("鍔ㄦ€佹鐩堟帴绠?, "鍔ㄦ€佹鐩堟帴绠￠〉淇濈暀鍦ㄨ繖閲岋紝鍚庣画缁х画鎸夋棫鐗堝畬鏁磋縼绉汇€?), "鍔ㄦ€佹鐩堟帴绠?)
        self._tabs.addTab(self._build_order_history_tab(), "鍘嗗彶濮旀墭")
        self._tabs.addTab(self._build_fill_history_tab(), "鍘嗗彶鎴愪氦")
        self._tabs.addTab(self._build_position_history_tab_v2(), "鍘嗗彶浠撲綅")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab_v2(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._orders_summary_label = QLabel("褰撳墠濮旀墭灏氭湭璇诲彇銆?)
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        top.addWidget(self._orders_summary_label, 1)
        for text, handler in (
            ("鍒锋柊", self.refresh_view),
            ("浠庨€変腑鏉′欢鍗曟帴绠″姩鎬佹鐩?, self._show_not_ready_action),
            ("鎾ゅ崟閫変腑", self._show_not_ready_action),
            ("鎵归噺鎾ゅ綋鍓嶇瓫閫?, self._show_not_ready_action),
        ):
            button = QPushButton(text)
            button.clicked.connect(handler)
            top.addWidget(button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._pending_type_combo = QComboBox()
        self._pending_source_combo = QComboBox()
        self._pending_state_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._pending_type_combo.addItem(label, value)
        for label, value in ORDER_SOURCE_FILTER_OPTIONS:
            self._pending_source_combo.addItem(label, value)
        for label, value in ORDER_STATE_FILTER_OPTIONS:
            self._pending_state_combo.addItem(label, value)
        self._pending_asset_edit = QLineEdit()
        self._pending_expiry_edit = QLineEdit()
        self._pending_keyword_edit = QLineEdit()
        for widget in (
            self._pending_type_combo,
            self._pending_source_combo,
            self._pending_state_combo,
            self._pending_asset_edit,
            self._pending_expiry_edit,
            self._pending_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_current_orders_table)
            else:
                widget.textChanged.connect(self._refresh_current_orders_table)
        apply_button = QPushButton("搴旂敤绛涢€?)
        apply_button.clicked.connect(self._refresh_current_orders_table)
        clear_button = QPushButton("娓呯┖绛涢€?)
        clear_button.clicked.connect(self._clear_pending_order_filters)
        filter_row.addWidget(QLabel("绫诲瀷"), 0, 0)
        filter_row.addWidget(self._pending_type_combo, 0, 1)
        filter_row.addWidget(QLabel("鏉ユ簮"), 0, 2)
        filter_row.addWidget(self._pending_source_combo, 0, 3)
        filter_row.addWidget(QLabel("鐘舵€?), 0, 4)
        filter_row.addWidget(self._pending_state_combo, 0, 5)
        filter_row.addWidget(QLabel("鏍囩殑"), 0, 6)
        filter_row.addWidget(self._pending_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("鍒版湡鍓嶇紑"), 0, 8)
        filter_row.addWidget(self._pending_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("鎼滅储"), 0, 10)
        filter_row.addWidget(self._pending_keyword_edit, 0, 11)
        filter_row.addWidget(apply_button, 0, 12)
        filter_row.addWidget(clear_button, 0, 13)
        layout.addLayout(filter_row)

        self._orders_table = self._build_history_table(
            ("鏃堕棿", "鏉ユ簮", "绫诲瀷", "鍚堢害", "鐘舵€?, "鏂瑰悜", "濮旀墭绫诲瀷", "濮旀墭浠?, "濮旀墭閲?, "宸叉垚浜?, "鎵嬬画璐?, "TP/SL", "璁㈠崟ID", "clOrdId"),
            stretch_columns={3, 11, 13},
        )
        layout.addWidget(self._orders_table, 1)
        return tab

    def _build_order_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._order_history_summary_label = QLabel("鍘嗗彶濮旀墭灏氭湭璇诲彇銆?)
        self._order_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._order_history_summary_label, 1)
        sync_button = QPushButton("鍚屾")
        sync_button.clicked.connect(self._refresh_order_history)
        top.addWidget(sync_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._order_history_type_combo = QComboBox()
        self._order_history_source_combo = QComboBox()
        self._order_history_state_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._order_history_type_combo.addItem(label, value)
        for label, value in ORDER_SOURCE_FILTER_OPTIONS:
            self._order_history_source_combo.addItem(label, value)
        for label, value in ORDER_STATE_FILTER_OPTIONS:
            self._order_history_state_combo.addItem(label, value)
        self._order_history_asset_edit = QLineEdit()
        self._order_history_expiry_edit = QLineEdit()
        self._order_history_keyword_edit = QLineEdit()
        for widget in (
            self._order_history_type_combo,
            self._order_history_source_combo,
            self._order_history_state_combo,
            self._order_history_asset_edit,
            self._order_history_expiry_edit,
            self._order_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_order_history_table)
            else:
                widget.textChanged.connect(self._refresh_order_history_table)
        order_apply = QPushButton("搴旂敤绛涢€?)
        order_apply.clicked.connect(self._refresh_order_history_table)
        order_clear = QPushButton("娓呯┖绛涢€?)
        order_clear.clicked.connect(self._clear_order_history_filters)
        filter_row.addWidget(QLabel("绫诲瀷"), 0, 0)
        filter_row.addWidget(self._order_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("鏉ユ簮"), 0, 2)
        filter_row.addWidget(self._order_history_source_combo, 0, 3)
        filter_row.addWidget(QLabel("鐘舵€?), 0, 4)
        filter_row.addWidget(self._order_history_state_combo, 0, 5)
        filter_row.addWidget(QLabel("鏍囩殑"), 0, 6)
        filter_row.addWidget(self._order_history_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("鍒版湡鍓嶇紑"), 0, 8)
        filter_row.addWidget(self._order_history_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("鎼滅储"), 0, 10)
        filter_row.addWidget(self._order_history_keyword_edit, 0, 11)
        filter_row.addWidget(order_apply, 0, 12)
        filter_row.addWidget(order_clear, 0, 13)
        layout.addLayout(filter_row)

        self._order_history_table = self._build_history_table(
            ("鏃堕棿", "鏉ユ簮", "绫诲瀷", "鍚堢害", "鐘舵€?, "鏂瑰悜", "濮旀墭绫诲瀷", "濮旀墭浠?, "濮旀墭閲?, "宸叉垚浜?, "鎵嬬画璐?, "TP/SL", "璁㈠崟ID", "clOrdId"),
            stretch_columns={3, 11, 13},
        )
        layout.addWidget(self._order_history_table, 1)
        return tab

    def _build_fill_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._fill_history_summary_label = QLabel("鍘嗗彶鎴愪氦灏氭湭璇诲彇銆?)
        self._fill_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._fill_history_summary_label, 1)
        more_button = QPushButton("澧炲姞100鏉?)
        more_button.clicked.connect(self._expand_fill_history_limit)
        top.addWidget(more_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._fill_history_type_combo = QComboBox()
        self._fill_history_side_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._fill_history_type_combo.addItem(label, value)
        for label, value in HISTORY_FILL_SIDE_FILTER_OPTIONS:
            self._fill_history_side_combo.addItem(label, value)
        self._fill_history_asset_edit = QLineEdit()
        self._fill_history_expiry_edit = QLineEdit()
        self._fill_history_keyword_edit = QLineEdit()
        for widget in (
            self._fill_history_type_combo,
            self._fill_history_side_combo,
            self._fill_history_asset_edit,
            self._fill_history_expiry_edit,
            self._fill_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._refresh_fill_history_table)
            else:
                widget.textChanged.connect(self._refresh_fill_history_table)
        fill_apply = QPushButton("搴旂敤绛涢€?)
        fill_apply.clicked.connect(self._refresh_fill_history_table)
        fill_clear = QPushButton("娓呯┖绛涢€?)
        fill_clear.clicked.connect(self._clear_fill_history_filters)
        fill_contract = QPushButton("甯﹀叆鍚堢害")
        fill_contract.clicked.connect(self.apply_selected_option_to_fill_history_search)
        fill_expiry = QPushButton("甯﹀叆鍒版湡鍓嶇紑")
        fill_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_fill_history_search)
        filter_row.addWidget(QLabel("绫诲瀷"), 0, 0)
        filter_row.addWidget(self._fill_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("鏂瑰悜"), 0, 2)
        filter_row.addWidget(self._fill_history_side_combo, 0, 3)
        filter_row.addWidget(QLabel("鏍囩殑"), 0, 4)
        filter_row.addWidget(self._fill_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("鍒版湡鍓嶇紑"), 0, 6)
        filter_row.addWidget(self._fill_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("鎼滅储"), 0, 8)
        filter_row.addWidget(self._fill_history_keyword_edit, 0, 9)
        filter_row.addWidget(fill_contract, 0, 10)
        filter_row.addWidget(fill_expiry, 0, 11)
        filter_row.addWidget(fill_apply, 0, 12)
        filter_row.addWidget(fill_clear, 0, 13)
        layout.addLayout(filter_row)

        self._fill_history_table = self._build_history_table(
            ("鏃堕棿", "绫诲瀷", "鍚堢害", "鏂瑰悜", "鎴愪氦浠?, "鎴愪氦閲?, "鎵嬬画璐?, "宸插疄鐜扮泩浜?, "鎴愪氦绫诲瀷"),
            stretch_columns={2},
        )
        layout.addWidget(self._fill_history_table, 1)
        return tab

    def _build_position_history_tab_v2(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._position_history_summary_label = QLabel("鍘嗗彶浠撲綅灏氭湭璇诲彇銆?)
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        top.addWidget(self._position_history_summary_label, 1)
        more_button = QPushButton("澧炲姞100鏉?)
        more_button.clicked.connect(self._expand_position_history_limit)
        top.addWidget(more_button)
        edit_button = QPushButton("缂栬緫澶囨敞")
        edit_button.clicked.connect(self.edit_selected_position_history_note)
        top.addWidget(edit_button)
        layout.addLayout(top)

        filter_row = QGridLayout()
        filter_row.setHorizontalSpacing(8)
        filter_row.setVerticalSpacing(8)
        self._position_history_type_combo = QComboBox()
        self._position_history_margin_combo = QComboBox()
        for label, value in POSITION_TYPE_OPTIONS:
            self._position_history_type_combo.addItem(label, value)
        for label, value in HISTORY_MARGIN_MODE_FILTER_OPTIONS:
            self._position_history_margin_combo.addItem(label, value)
        self._position_history_asset_edit = QLineEdit()
        self._position_history_expiry_edit = QLineEdit()
        self._position_history_keyword_edit = QLineEdit()
        self._position_history_range_start_edit = QLineEdit()
        self._position_history_range_start_edit.setPlaceholderText("YYYYMMDD")
        self._position_history_range_start_edit.setText(self._default_position_history_start_text())
        self._position_history_range_start_edit.setMaxLength(8)
        self._position_history_range_end_edit = QLineEdit()
        self._position_history_range_end_edit.setPlaceholderText("YYYYMMDD")
        self._position_history_range_end_edit.setText(self._default_position_history_end_text())
        self._position_history_range_end_edit.setMaxLength(8)
        for widget in (
            self._position_history_type_combo,
            self._position_history_margin_combo,
            self._position_history_asset_edit,
            self._position_history_expiry_edit,
            self._position_history_keyword_edit,
        ):
            if isinstance(widget, QComboBox):
                widget.currentIndexChanged.connect(self._schedule_position_history_render)
            else:
                widget.textChanged.connect(self._schedule_position_history_render)
        self._position_history_range_start_edit.editingFinished.connect(self._schedule_position_history_render)
        self._position_history_range_end_edit.editingFinished.connect(self._schedule_position_history_render)
        pos_apply = QPushButton("搴旂敤绛涢€?)
        pos_apply.clicked.connect(self._force_position_history_render)
        pos_clear = QPushButton("娓呯┖绛涢€?)
        pos_clear.clicked.connect(self._clear_position_history_filters)
        pos_contract = QPushButton("甯﹀叆鍚堢害")
        pos_contract.clicked.connect(self.apply_selected_option_to_position_history_search)
        pos_expiry = QPushButton("甯﹀叆鍒版湡鍓嶇紑")
        pos_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_history_search)
        filter_row.addWidget(QLabel("绫诲瀷"), 0, 0)
        filter_row.addWidget(self._position_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("淇濊瘉閲戞ā寮?), 0, 2)
        filter_row.addWidget(self._position_history_margin_combo, 0, 3)
        filter_row.addWidget(QLabel("鏍囩殑"), 0, 4)
        filter_row.addWidget(self._position_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("鍒版湡鍓嶇紑"), 0, 6)
        filter_row.addWidget(self._position_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("鎼滅储"), 0, 8)
        filter_row.addWidget(self._position_history_keyword_edit, 0, 9)
        filter_row.addWidget(pos_contract, 0, 10)
        filter_row.addWidget(pos_expiry, 0, 11)
        filter_row.addWidget(pos_apply, 0, 12)
        filter_row.addWidget(pos_clear, 0, 13)
        filter_row.addWidget(QLabel("鏈湴寮€濮?), 1, 0)
        filter_row.addWidget(self._position_history_range_start_edit, 1, 1)
        filter_row.addWidget(QLabel("鏈湴缁撴潫"), 1, 2)
        filter_row.addWidget(self._position_history_range_end_edit, 1, 3)
        filter_row.addWidget(QLabel("YYYYMMDD 鎴?YYYY-MM-DD锛岀暀绌哄垯涓嶈繃婊?), 1, 4, 1, 10)
        layout.addLayout(filter_row)

        self._position_history_table = self._build_history_table(
            ("鏃堕棿", "绫诲瀷", "鍚堢害", "淇濊瘉閲戞ā寮?, "鎸佷粨妯″紡", "浜ゆ槗鏂瑰悜", "寮€浠撳潎浠?, "骞充粨鍧囦环", "骞充粨鏁伴噺", "鎵嬬画璐?, "鐩堜簭", "澶囨敞"),
            stretch_columns={2, 11},
        )
        layout.addWidget(self._position_history_table, 1)
        self._position_history_summary_label.setMinimumHeight(34)
        return tab

    def _build_history_table(self, headings: tuple[str, ...], *, stretch_columns: set[int]) -> QTableWidget:
        table = QTableWidget(0, len(headings))
        table.setHorizontalHeaderLabels(headings)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        header = table.horizontalHeader()
        header.setStretchLastSection(False)
        for index in range(len(headings)):
            header.setSectionResizeMode(
                index,
                QHeaderView.ResizeMode.Stretch if index in stretch_columns else QHeaderView.ResizeMode.ResizeToContents,
            )
        return table

    def _start_private_threads(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_private_threads()
        elif self._account_feed is not None and self._account_feed.isRunning():
            return
        self._account_feed = AccountFeedThread(self._runtime)
        self._order_feed = OrderFeedThread(self._runtime)
        self._account_feed.positions_ready.connect(self._apply_positions_summary)
        self._account_feed.payload_ready.connect(self._apply_positions_payload)
        self._account_feed.status_changed.connect(self._set_account_status)
        self._order_feed.orders_ready.connect(self._apply_orders)
        self._order_feed.status_changed.connect(self._set_order_status)
        self._account_feed.start()
        self._order_feed.start()
        self._start_order_history_refresh(force_restart=force_restart)
        self._start_fill_history_refresh(force_restart=force_restart)
        self._start_position_history_refresh(force_restart=force_restart)

    def _clear_pending_order_filters(self) -> None:
        self._pending_type_combo.setCurrentIndex(0)
        self._pending_source_combo.setCurrentIndex(0)
        self._pending_state_combo.setCurrentIndex(0)
        self._pending_asset_edit.clear()
        self._pending_expiry_edit.clear()
        self._pending_keyword_edit.clear()

    def _clear_order_history_filters(self) -> None:
        self._order_history_type_combo.setCurrentIndex(0)
        self._order_history_source_combo.setCurrentIndex(0)
        self._order_history_state_combo.setCurrentIndex(0)
        self._order_history_asset_edit.clear()
        self._order_history_expiry_edit.clear()
        self._order_history_keyword_edit.clear()

    def _clear_fill_history_filters(self) -> None:
        self._fill_history_type_combo.setCurrentIndex(0)
        self._fill_history_side_combo.setCurrentIndex(0)
        self._fill_history_asset_edit.clear()
        self._fill_history_expiry_edit.clear()
        self._fill_history_keyword_edit.clear()

    def _clear_position_history_filters(self) -> None:
        self._position_history_filter_resetting = True
        self._position_history_type_combo.setCurrentIndex(0)
        self._position_history_margin_combo.setCurrentIndex(0)
        self._position_history_asset_edit.clear()
        self._position_history_expiry_edit.clear()
        self._position_history_keyword_edit.clear()
        self._position_history_range_start_edit.setText(self._default_position_history_start_text())
        self._position_history_range_end_edit.setText(self._default_position_history_end_text())
        self._position_history_filter_resetting = False
        self._force_position_history_render()

    def apply_selected_option_to_fill_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_fill_history_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(inst_id)
        if not contract:
            QMessageBox.information(self, "甯﹀叆鍚堢害", "璇峰厛鍦ㄥ巻鍙叉垚浜ら噷閫変腑涓€鏉℃湡鏉冭褰曪紝鎴栧湪褰撳墠鎸佷粨閲岄€変腑涓€鏉℃湡鏉冩寔浠撱€?)
            return
        self._fill_history_keyword_edit.setText(contract)
        self._refresh_fill_history_table()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_fill_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "甯﹀叆鍒版湡鍓嶇紑", "璇峰厛鍦ㄥ巻鍙叉垚浜ら噷閫変腑涓€鏉℃湡鏉冭褰曪紝鎴栧湪褰撳墠鎸佷粨閲岄€変腑涓€鏉℃湡鏉冩寔浠撱€?)
            return
        self._fill_history_expiry_edit.setText(expiry_prefix)
        self._refresh_fill_history_table()

    def apply_selected_option_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(inst_id)
        if not contract:
            QMessageBox.information(self, "甯﹀叆鍚堢害", "璇峰厛鍦ㄥ巻鍙蹭粨浣嶉噷閫変腑涓€鏉℃湡鏉冭褰曪紝鎴栧湪褰撳墠鎸佷粨閲岄€変腑涓€鏉℃湡鏉冩寔浠撱€?)
            return
        self._position_history_keyword_edit.setText(contract)
        self._force_position_history_render()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "甯﹀叆鍒版湡鍓嶇紑", "璇峰厛鍦ㄥ巻鍙蹭粨浣嶉噷閫変腑涓€鏉℃湡鏉冭褰曪紝鎴栧湪褰撳墠鎸佷粨閲岄€変腑涓€鏉℃湡鏉冩寔浠撱€?)
            return
        self._position_history_expiry_edit.setText(expiry_prefix)
        self._force_position_history_render()

    def _selected_option_inst_id_for_fill_history_shortcut(self) -> str:
        row = self._fill_history_table.currentRow() if hasattr(self, "_fill_history_table") else -1
        if 0 <= row < len(self._visible_fill_history_items):
            item = self._visible_fill_history_items[row]
            if (item.inst_type or "").strip().upper() == "OPTION":
                return item.inst_id or ""
        position = self._selected_option_for_shortcut()
        return position.inst_id if position is not None else ""

    def _selected_option_inst_id_for_position_history_shortcut(self) -> str:
        item = self._selected_position_history_item()
        if item is not None and (item.inst_type or "").strip().upper() == "OPTION":
            return item.inst_id or ""
        position = self._selected_option_for_shortcut()
        return position.inst_id if position is not None else ""

    def _expand_fill_history_limit(self) -> None:
        self._fill_history_fetch_limit += 100
        self._refresh_fill_history()

    def _expand_position_history_limit(self) -> None:
        self._position_history_fetch_limit += 100
        self._refresh_position_history()

    def _filtered_current_orders(self) -> list[OrderStatusView]:
        items = list(self._visible_orders)
        inst_type = str(self._pending_type_combo.currentData() or "").strip().upper()
        source_filter = str(self._pending_source_combo.currentData() or "").strip().lower()
        state_filter = str(self._pending_state_combo.currentData() or "").strip().lower()
        asset_filter = self._pending_asset_edit.text().strip().upper()
        expiry_filter = self._pending_expiry_edit.text().strip().upper()
        keyword = self._pending_keyword_edit.text().strip().upper()
        result: list[OrderStatusView] = []
        for item in items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            feed_source = str(item.raw.get("_feed_source") or "").strip().lower()
            source_kind = str(item.raw.get("_source_kind") or "").strip().lower()
            if source_filter == "ws" and feed_source != "ws":
                continue
            if source_filter in {"normal", "algo"} and source_kind != source_filter:
                continue
            if state_filter and (item.state or "").strip().lower() != state_filter:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        (item.state or ""),
                        (item.side or ""),
                        (item.pos_side or ""),
                        (item.ord_type or ""),
                        (item.client_order_id or ""),
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        filtered = self._filtered_current_orders()
        selected_ord_id = ""
        row = self._orders_table.currentRow()
        if 0 <= row < len(filtered):
            selected_ord_id = filtered[row].ord_id
        self._orders_summary_label.setText(f"褰撳墠濮旀墭锛歿len(filtered)} 鏉?| 浠呮樉绀哄綋鍓嶆寔浠撶浉鍏冲悎绾︺€?)
        self._orders_table.setRowCount(len(filtered))
        for row, order in enumerate(filtered):
            source_label = "REST pending" if str(order.raw.get("_feed_source") or "") == "rest_pending" else "WS 褰撳墠"
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                source_label,
                order.inst_type or "-",
                order.inst_id or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                _format_trade_order_size(order.size),
                _format_trade_order_size(order.filled_size),
                "-",
                "-",
                order.ord_id or "-",
                order.client_order_id or "-",
            )
            self._set_table_row(self._orders_table, row, values, left_align={3, 13})
        self._current_order_rows = filtered
        self._restore_table_selection(self._orders_table, filtered, selected_ord_id, lambda item: item.ord_id or "")
        self._refresh_current_order_detail()

    def _refresh_current_order_detail(self) -> None:
        if not hasattr(self, "_orders_detail"):
            return
        items = getattr(self, "_current_order_rows", [])
        row = self._orders_table.currentRow() if hasattr(self, "_orders_table") else -1
        if row < 0 or row >= len(items):
            if hasattr(self, "_orders_detail"):
                self._orders_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑褰撳墠濮旀墭鐨勮鎯呫€?)
            return
        order = items[row]
        lines = [
            f"鏃堕棿锛歿_format_okx_ms_timestamp(order.update_time or order.created_time)}",
            f"鍚堢害锛歿order.inst_id or '-'}",
            f"绫诲瀷锛歿order.inst_type or '-'}",
            f"鐘舵€侊細{_format_trade_order_state(order.state)}",
            f"鏂瑰悜锛歿_format_history_side(order.side or '-', order.pos_side or '')}",
            f"濮旀墭绫诲瀷锛歿order.ord_type or '-'}",
            f"濮旀墭浠凤細{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"濮旀墭閲忥細{_format_trade_order_size(order.size)}",
            f"宸叉垚浜わ細{_format_trade_order_size(order.filled_size)}",
            f"浜ゆ槗妯″紡锛歿order.td_mode or '-'}",
            f"璁㈠崟ID锛歿order.ord_id or '-'}",
            f"clOrdId锛歿order.client_order_id or '-'}",
            "",
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText("\n".join(lines))

    def _filtered_order_history_items(self) -> list[OkxTradeOrderItem]:
        inst_type = str(self._order_history_type_combo.currentData() or "").strip().upper()
        source_filter = str(self._order_history_source_combo.currentData() or "").strip().lower()
        state_filter = str(self._order_history_state_combo.currentData() or "").strip().lower()
        asset_filter = self._order_history_asset_edit.text().strip().upper()
        expiry_filter = self._order_history_expiry_edit.text().strip().upper()
        keyword = self._order_history_keyword_edit.text().strip().upper()
        result: list[OkxTradeOrderItem] = []
        for item in self._order_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if source_filter and (item.source_kind or "").strip().lower() != source_filter:
                continue
            if state_filter and (item.state or "").strip().lower() != state_filter:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        item.source_label or "",
                        item.state or "",
                        item.side or "",
                        item.pos_side or "",
                        item.ord_type or "",
                        item.client_order_id or "",
                        item.algo_client_order_id or "",
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_order_history_table(self) -> None:
        if not hasattr(self, "_order_history_table"):
            return
        filtered = self._filtered_order_history_items()
        selected_key = ""
        row = self._order_history_table.currentRow()
        if 0 <= row < len(filtered):
            selected_key = filtered[row].order_id or filtered[row].client_order_id or ""
        self._visible_order_history_items = filtered
        self._order_history_summary_label.setText(f"鍘嗗彶濮旀墭锛氬綋鍓嶆樉绀?{len(filtered)}/{len(self._order_history_items)}")
        self._order_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time or item.created_time),
                item.source_label or "-",
                item.inst_type or "-",
                item.inst_id or "-",
                _format_trade_order_state(item.state),
                _format_history_side(item.side, item.pos_side),
                item.ord_type or "-",
                _format_trade_order_price(item.price, item.inst_id, item.inst_type),
                _format_trade_order_size(item.size),
                _format_trade_order_size(item.filled_size),
                _format_trade_order_fee_cell(item, self._order_history_usdt_prices),
                "-",
                item.order_id or item.algo_id or "-",
                item.client_order_id or item.algo_client_order_id or "-",
            )
            self._set_table_row(self._order_history_table, row, values, left_align={3, 13})
        self._restore_table_selection(
            self._order_history_table,
            filtered,
            selected_key,
            lambda item: item.order_id or item.client_order_id or "",
        )
        self._refresh_order_history_detail()

    def _refresh_order_history_detail(self) -> None:
        if not hasattr(self, "_order_history_detail"):
            return
        row = self._order_history_table.currentRow() if hasattr(self, "_order_history_table") else -1
        if row < 0 or row >= len(self._visible_order_history_items):
            if hasattr(self, "_order_history_detail"):
                self._order_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶濮旀墭鐨勮鎯呫€?)
            return
        item = self._visible_order_history_items[row]
        text = _build_trade_order_detail_text(item)
        self._order_history_detail.setPlainText(
            "\n\n".join((text, json.dumps(item.raw, ensure_ascii=False, indent=2, sort_keys=True)))
        )

    def _filtered_fill_history_items(self) -> list[OkxFillHistoryItem]:
        inst_type = str(self._fill_history_type_combo.currentData() or "").strip().upper()
        side_filter = str(self._fill_history_side_combo.currentData() or "").strip().lower()
        asset_filter = self._fill_history_asset_edit.text().strip().upper()
        expiry_filter = self._fill_history_expiry_edit.text().strip().upper()
        keyword = self._fill_history_keyword_edit.text().strip().upper()
        result: list[OkxFillHistoryItem] = []
        for item in self._fill_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if side_filter and side_filter not in {(item.side or "").strip().lower(), (item.pos_side or "").strip().lower()}:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (inst_id, item.inst_type or "", item.side or "", item.pos_side or "", item.exec_type or "")
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _refresh_fill_history_table(self) -> None:
        if not hasattr(self, "_fill_history_table"):
            return
        filtered = self._filtered_fill_history_items()
        selected_key = ""
        row = self._fill_history_table.currentRow()
        if 0 <= row < len(filtered):
            selected_key = filtered[row].trade_id or filtered[row].order_id or ""
        self._visible_fill_history_items = filtered
        self._fill_history_summary_label.setText(f"鍘嗗彶鎴愪氦锛氬綋鍓嶆樉绀?{len(filtered)}/{len(self._fill_history_items)}")
        self._fill_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.fill_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_history_side(item.side, item.pos_side),
                _format_fill_history_price(item),
                _format_fill_history_size(item, self._fill_history_instruments),
                _format_fill_history_fee_cell(item, self._fill_history_usdt_prices),
                _format_fill_history_pnl(item, self._fill_history_usdt_prices),
                _format_fill_history_exec_type(item.exec_type),
            )
            self._set_table_row(self._fill_history_table, row, values, left_align={2})
        self._restore_table_selection(
            self._fill_history_table,
            filtered,
            selected_key,
            lambda item: item.trade_id or item.order_id or "",
        )
        self._refresh_fill_history_detail()

    def _refresh_fill_history_detail(self) -> None:
        if not hasattr(self, "_fill_history_detail"):
            return
        row = self._fill_history_table.currentRow() if hasattr(self, "_fill_history_table") else -1
        if row < 0 or row >= len(self._visible_fill_history_items):
            if hasattr(self, "_fill_history_detail"):
                self._fill_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶鎴愪氦鐨勮鎯呫€?)
            return
        item = self._visible_fill_history_items[row]
        text = _build_fill_history_detail_text(item, self._fill_history_instruments)
        self._fill_history_detail.setPlainText(
            "\n\n".join((text, json.dumps(item.raw, ensure_ascii=False, indent=2, sort_keys=True)))
        )

    def _filtered_position_history_items(self) -> list[OkxPositionHistoryItem]:
        inst_type = str(self._position_history_type_combo.currentData() or "").strip().upper()
        margin_mode = str(self._position_history_margin_combo.currentData() or "").strip().lower()
        asset_filter = self._position_history_asset_edit.text().strip().upper()
        expiry_filter = self._position_history_expiry_edit.text().strip().upper()
        keyword = self._position_history_keyword_edit.text().strip().upper()
        start_date = self._parse_history_date(self._position_history_range_start_edit.text())
        end_date = self._parse_history_date(self._position_history_range_end_edit.text(), end_of_day=True)
        result: list[OkxPositionHistoryItem] = []
        for item in self._position_history_items:
            if inst_type and (item.inst_type or "").strip().upper() != inst_type:
                continue
            if margin_mode and (item.mgn_mode or "").strip().lower() != margin_mode:
                continue
            inst_id = (item.inst_id or "").strip().upper()
            if asset_filter and not inst_id.startswith(asset_filter + "-"):
                continue
            if expiry_filter and not _history_expiry_filter_matches(inst_id, expiry_filter):
                continue
            if keyword:
                haystack = " ".join(
                    (
                        inst_id,
                        item.inst_type or "",
                        item.mgn_mode or "",
                        item.pos_side or "",
                        item.direction or "",
                        self._position_history_note_text(item),
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            if item.update_time is not None:
                if start_date is not None and item.update_time < start_date:
                    continue
                if end_date is not None and item.update_time > end_date:
                    continue
            result.append(item)
        return result

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        filtered = self._filtered_position_history_items()
        selected_key = ""
        row = self._position_history_table.currentRow()
        if 0 <= row < len(self._visible_position_history_items):
            selected_key = self._position_history_row_key(self._visible_position_history_items[row])
        self._visible_position_history_items = filtered
        stats_text = _format_position_history_filter_stats(
            list(enumerate(filtered)),
            self._position_history_usdt_prices,
        )
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"鍘嗗彶浠撲綅锛歿len(self._position_history_items)} 鏉?| 鏈€杩戝悓姝ワ細{self._position_history_last_sync_text} | 褰撳墠鏄剧ず锛歿len(filtered)}/{len(self._position_history_items)}",
                    f"绛涢€夌粺璁★細{stats_text}",
                )
            )
        )
        self._position_history_table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            values = (
                _format_okx_ms_timestamp(item.update_time),
                item.inst_type or "-",
                item.inst_id or "-",
                _format_margin_mode(item.mgn_mode or ""),
                _format_history_side(None, item.pos_side or item.direction),
                _format_position_history_trade_side(item),
                _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                _format_position_history_size(item, self._position_history_instruments),
                _format_position_history_fee_cell(item, self._position_history_usdt_prices),
                _format_position_history_pnl(item.pnl, item, usdt_prices=self._position_history_usdt_prices),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            self._set_table_row(self._position_history_table, row, values, left_align={2, 11})
        self._restore_table_selection(
            self._position_history_table,
            filtered,
            selected_key,
            self._position_history_row_key,
        )

    def _refresh_position_history_detail(self) -> None:
        if not hasattr(self, "_position_history_detail"):
            return
        row = self._position_history_table.currentRow() if hasattr(self, "_position_history_table") else -1
        if row < 0 or row >= len(self._visible_position_history_items):
            if hasattr(self, "_position_history_detail"):
                self._position_history_detail.setPlainText("杩欓噷浼氭樉绀洪€変腑鍘嗗彶浠撲綅鐨勮鎯呫€?)
            return
        item = self._visible_position_history_items[row]
        self._position_history_detail.setPlainText(
            _build_position_history_detail_text(
                item,
                self._position_history_usdt_prices,
                self._position_history_instruments,
                note=self._position_history_note_text(item),
            )
        )

    def _selected_position_history_item(self) -> OkxPositionHistoryItem | None:
        row = self._position_history_table.currentRow() if hasattr(self, "_position_history_table") else -1
        if row < 0 or row >= len(self._visible_position_history_items):
            return None
        return self._visible_position_history_items[row]

    def edit_selected_position_history_note(self) -> None:
        item = self._selected_position_history_item()
        if item is None:
            QMessageBox.information(self, "缂栬緫澶囨敞", "璇峰厛閫夋嫨涓€鏉″巻鍙蹭粨浣嶃€?)
            return
        dialog = NoteEditorDialog(
            title="缂栬緫鍘嗗彶浠撲綅澶囨敞",
            prompt=f"涓?{item.inst_id} 濉啓澶囨敞銆?,
            initial_value=self._position_history_note_text(item),
            parent=self,
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        record_key = _position_history_note_key(self._last_profile_name, self._note_environment(), item)
        if dialog.result_text:
            record = _build_history_position_note_record(
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                item=item,
                note=dialog.result_text,
                now_ms=int(time.time() * 1000),
                previous=self._history_notes.get(record_key),
            )
            if record is not None:
                self._history_notes[record_key] = record
        else:
            self._history_notes.pop(record_key, None)
        self._save_position_notes()
        self._render_position_history_table()

    @Slot(str)
    def _set_order_history_status(self, text: str) -> None:
        if hasattr(self, "_order_history_summary_label"):
            self._order_history_summary_label.setText(text)

    @Slot(str)
    def _set_fill_history_status(self, text: str) -> None:
        if hasattr(self, "_fill_history_summary_label"):
            self._fill_history_summary_label.setText(text)

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        visible_inst_ids = {item.inst_id.strip().upper() for item in self._visible_positions}
        self._visible_orders = [
            item for item in self._orders if not visible_inst_ids or item.inst_id.strip().upper() in visible_inst_ids
        ]
        self._refresh_current_orders_table()

    @Slot(object)
    def _apply_order_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        prices = payload.get("usdt_prices")
        self._order_history_items = list(items) if isinstance(items, list) else []
        self._order_history_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        self._refresh_order_history_table()

    @Slot(object)
    def _apply_fill_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        prices = payload.get("usdt_prices")
        self._fill_history_items = list(items) if isinstance(items, list) else []
        self._fill_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._fill_history_usdt_prices = dict(prices) if isinstance(prices, dict) else {}
        self._refresh_fill_history_table()

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        self._position_history_items = list(items) if isinstance(items, list) else []
        self._position_history_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        self._position_history_usdt_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        self._position_history_last_sync_text = time.strftime("%H:%M:%S")
        self._render_position_history_table()

    def _set_table_row(
        self,
        table: QTableWidget,
        row: int,
        values: tuple[str, ...],
        *,
        left_align: set[int] | None = None,
    ) -> None:
        left_align = left_align or set()
        for column, value in enumerate(values):
            cell = QTableWidgetItem(str(value))
            if column in left_align:
                cell.setTextAlignment(int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter))
            else:
                cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
            table.setItem(row, column, cell)

    def _restore_table_selection(
        self,
        table: QTableWidget,
        items: list[object],
        selected_key: str,
        key_fn: Callable[[object], str],
    ) -> None:
        target_row = -1
        if selected_key:
            for index, item in enumerate(items):
                if key_fn(item) == selected_key:
                    target_row = index
                    break
        elif items:
            target_row = 0
        if target_row >= 0:
            table.selectRow(target_row)

    def _position_history_row_key(self, item: OkxPositionHistoryItem) -> str:
        return "|".join(
            (
                str(item.update_time or ""),
                item.inst_id or "",
                item.pos_side or "",
                item.direction or "",
                str(item.close_size or ""),
            )
        )

    def _parse_history_date(self, raw: str, *, end_of_day: bool = False) -> int | None:
        text = raw.strip()
        if not text:
            return None
        try:
            normalized = text.replace("-", "").replace("/", "").replace(".", "")
            parsed = time.strptime(normalized, "%Y%m%d")
            base = int(time.mktime(parsed)) * 1000
            return base + (24 * 60 * 60 * 1000 - 1 if end_of_day else 0)
        except Exception:
            return None

    def _default_position_history_start_text(self) -> str:
        return "20260101"

    def _default_position_history_end_text(self) -> str:
        return (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    @Slot()
    def _schedule_position_history_render(self) -> None:
        if self._position_history_filter_resetting:
            return
        if not hasattr(self, "_position_history_render_timer"):
            self._render_position_history_table()
            return
        self._position_history_render_timer.start(120)

    @Slot()
    def _force_position_history_render(self) -> None:
        if hasattr(self, "_position_history_render_timer") and self._position_history_render_timer.isActive():
            self._position_history_render_timer.stop()
        self._render_position_history_table()

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("姝ｅ湪鍒锋柊...")
        self._start_private_threads(force_restart=True)
