from __future__ import annotations

from collections import Counter
import json
import re
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from PySide6.QtCore import QSignalBlocker, QThread, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
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
from roll_terminal_qt.option_roll_window import OptionRollQtDialog
from okx_quant.log_utils import append_log_line
from okx_quant.app_paths import data_root
from okx_quant.models import Candle, Credentials, EmailNotificationConfig, Instrument, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.option_roll import is_short_option_position
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
    load_notification_snapshot,
    load_position_notes_snapshot,
    save_account_positions_home_view_prefs,
    save_position_notes_snapshot,
    verify_profile_switch_password,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    ProtectionSessionSnapshot,
    derive_position_direction,
    describe_protection_price_logic,
    infer_default_spot_inst_id,
    infer_protection_profit_on_rise,
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
    _format_network_error_message,
    _format_okx_ms_timestamp,
    _group_positions_for_tree,
    _format_history_side,
    _normalize_position_note_text,
    _option_search_shortcuts,
    _format_trade_order_price,
    _format_trade_order_size,
    _format_trade_order_coin_size,
    _format_trade_order_coin_filled_size,
    _format_trade_order_state,
    _format_trade_order_fee_cell,
    _format_trade_order_tp_sl,
    _trade_order_cancel_reference,
    _trade_order_program_owner_label,
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
    _position_contract_value_snapshot,
    _position_realized_pnl_usdt,
    _position_signed_open_value_approx_usdt,
    _position_theta_usdt,
    _position_tree_row_id,
    _position_unrealized_pnl_usdt,
    _reconcile_current_position_note_records,
    _inherit_position_history_notes,
    _prune_closed_current_position_notes,
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
from roll_terminal_qt.incremental_views import keyed_row_delta
from roll_terminal_qt.option_strategy_window import CandlestickChartView
from roll_terminal_qt.order_service import OrderFeedThread, OrderStatusView
from roll_terminal_qt.perf_metrics import measure_ui_step
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots, profile_requires_password
from roll_terminal_qt.realtime_account_store import AccountRealtimeSnapshot, get_shared_realtime_account_store
from roll_terminal_qt.shared_order_store import SharedOrderSnapshot, get_shared_order_store
from roll_terminal_qt.runtime import load_runtime, profile_names


def _debug_log(message: str) -> None:
    stream = getattr(sys, "stdout", None)
    if stream is None:
        return
    try:
        stream.write(f"{message}\n")
        stream.flush()
    except Exception:
        return


def _build_optional_protection_notifier(profile_name: str | None) -> EmailNotifier | None:
    snapshot = load_notification_snapshot()
    if not bool(snapshot.get("enabled", False)):
        return None
    recipients = tuple(
        item.strip()
        for item in re.split(r"[,\n;]+", str(snapshot.get("recipient_emails", "")))
        if item.strip()
    )
    normalized_profile = (profile_name or "").strip()
    sender_overrides = dict(snapshot.get("api_sender_email_overrides", {}))
    sender_email = str(sender_overrides.get(normalized_profile, "")).strip() or str(snapshot.get("sender_email", "")).strip()
    notification_config = EmailNotificationConfig(
        enabled=bool(snapshot.get("enabled", False)),
        smtp_host=str(snapshot.get("smtp_host", "")).strip(),
        smtp_port=int(snapshot.get("smtp_port", 465) or 465),
        smtp_username=str(snapshot.get("smtp_username", "")).strip(),
        smtp_password=str(snapshot.get("smtp_password", "")),
        sender_email=sender_email,
        recipient_emails=recipients,
        use_ssl=bool(snapshot.get("use_ssl", True)),
        notify_trade_fills=bool(snapshot.get("notify_trade_fills", True)),
        notify_signals=bool(snapshot.get("notify_signals", True)),
        notify_errors=bool(snapshot.get("notify_errors", True)),
    )
    if not notification_config.enabled:
        return None
    return EmailNotifier(
        notification_config,
        logger=lambda message: append_log_line(f"[邮件 持仓保护] {message}"),
    )


POSITION_TYPE_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部类型", ""),
    ("交割合约 FUTURES", "FUTURES"),
    ("永续 SWAP", "SWAP"),
    ("期权 OPTION", "OPTION"),
)

def _current_order_view_source_kind(order: OrderStatusView) -> str:
    raw = order.raw if isinstance(order.raw, dict) else {}
    return str(raw.get("_source_kind") or "").strip().lower() or "normal"


def _current_order_view_source_label(order: OrderStatusView) -> str:
    raw = order.raw if isinstance(order.raw, dict) else {}
    feed_source = str(raw.get("_feed_source") or "").strip().lower()
    source_kind = _current_order_view_source_kind(order)
    if feed_source == "rest_pending" and source_kind == "algo":
        return "REST 算法"
    if feed_source == "rest_pending":
        return "REST pending"
    if source_kind == "algo":
        return "WS 当前算法"
    return "WS 当前"


_CURRENT_ORDER_TERMINAL_STATES = frozenset(
    {
        "canceled",
        "filled",
        "order_failed",
        "failed",
        "mmp_canceled",
    }
)


def _current_order_state_is_terminal(state: str | None) -> bool:
    return str(state or "").strip().lower() in _CURRENT_ORDER_TERMINAL_STATES


def _current_order_raw_decimal(raw: dict[str, object], *keys: str) -> Decimal | None:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        try:
            return Decimal(text)
        except Exception:
            continue
    return None


def _current_order_extract_tp_sl_fields(raw: dict[str, object]) -> dict[str, Decimal | str | None]:
    take_profit_trigger_price = _current_order_raw_decimal(raw, "tpTriggerPx", "takeProfitTriggerPrice")
    take_profit_order_price = _current_order_raw_decimal(raw, "tpOrdPx", "takeProfitOrdPx")
    take_profit_trigger_price_type = raw.get("tpTriggerPxType") or raw.get("takeProfitTriggerPxType")
    stop_loss_trigger_price = _current_order_raw_decimal(raw, "slTriggerPx", "stopLossTriggerPrice")
    stop_loss_order_price = _current_order_raw_decimal(raw, "slOrdPx", "stopLossOrdPx")
    stop_loss_trigger_price_type = raw.get("slTriggerPxType") or raw.get("stopLossTriggerPxType")
    attach_algo_orders = raw.get("attachAlgoOrds")
    if isinstance(attach_algo_orders, list):
        for item in attach_algo_orders:
            if not isinstance(item, dict):
                continue
            if take_profit_trigger_price is None:
                take_profit_trigger_price = _current_order_raw_decimal(item, "tpTriggerPx", "takeProfitTriggerPrice")
            if take_profit_order_price is None:
                take_profit_order_price = _current_order_raw_decimal(item, "tpOrdPx", "takeProfitOrdPx")
            if take_profit_trigger_price_type is None:
                take_profit_trigger_price_type = item.get("tpTriggerPxType") or item.get("takeProfitTriggerPxType")
            if stop_loss_trigger_price is None:
                stop_loss_trigger_price = _current_order_raw_decimal(item, "slTriggerPx", "stopLossTriggerPrice")
            if stop_loss_order_price is None:
                stop_loss_order_price = _current_order_raw_decimal(item, "slOrdPx", "stopLossOrdPx")
            if stop_loss_trigger_price_type is None:
                stop_loss_trigger_price_type = item.get("slTriggerPxType") or item.get("stopLossTriggerPxType")
    return {
        "take_profit_trigger_price": take_profit_trigger_price,
        "take_profit_order_price": take_profit_order_price,
        "take_profit_trigger_price_type": (
            str(take_profit_trigger_price_type).strip() if take_profit_trigger_price_type is not None else None
        ),
        "stop_loss_trigger_price": stop_loss_trigger_price,
        "stop_loss_order_price": stop_loss_order_price,
        "stop_loss_trigger_price_type": (
            str(stop_loss_trigger_price_type).strip() if stop_loss_trigger_price_type is not None else None
        ),
    }


def _current_order_view_to_trade_order_item(order: OrderStatusView) -> OkxTradeOrderItem:
    raw = dict(order.raw) if isinstance(order.raw, dict) else {}
    algo_id = str(raw.get("algoId") or "").strip() or None
    algo_cl_ord_id = str(raw.get("algoClOrdId") or "").strip() or None
    tp_sl = _current_order_extract_tp_sl_fields(raw)
    return OkxTradeOrderItem(
        source_kind=_current_order_view_source_kind(order),
        source_label=_current_order_view_source_label(order),
        created_time=order.created_time,
        update_time=order.update_time,
        inst_id=order.inst_id,
        inst_type=order.inst_type,
        side=order.side or None,
        pos_side=order.pos_side or None,
        td_mode=order.td_mode or None,
        ord_type=order.ord_type or None,
        state=order.state or None,
        price=order.price,
        size=order.size,
        filled_size=order.filled_size,
        avg_price=order.avg_price,
        order_id=order.ord_id or None,
        algo_id=algo_id,
        client_order_id=order.client_order_id or None,
        algo_client_order_id=algo_cl_ord_id or (order.client_order_id or None),
        pnl=_current_order_raw_decimal(raw, "pnl"),
        fee=_current_order_raw_decimal(raw, "fee", "actualFee", "fillFee"),
        fee_currency=(
            str(raw.get("feeCcy") or raw.get("actualFeeCcy") or raw.get("fillFeeCcy") or "").strip() or None
        ),
        reduce_only=order.reduce_only,
        trigger_price=_current_order_raw_decimal(raw, "triggerPx", "triggerPrice"),
        trigger_price_type=(str(raw.get("triggerPxType") or raw.get("triggerPriceType") or "").strip() or None),
        order_price=_current_order_raw_decimal(raw, "orderPx") or order.price,
        actual_price=_current_order_raw_decimal(raw, "actualPx", "avgPx", "fillPx") or order.avg_price,
        actual_size=_current_order_raw_decimal(raw, "actualSz", "accFillSz", "fillSz") or order.filled_size,
        actual_side=(str(raw.get("actualSide") or "").strip() or order.side or None),
        take_profit_trigger_price=tp_sl["take_profit_trigger_price"],
        take_profit_order_price=tp_sl["take_profit_order_price"],
        take_profit_trigger_price_type=tp_sl["take_profit_trigger_price_type"],
        stop_loss_trigger_price=tp_sl["stop_loss_trigger_price"],
        stop_loss_order_price=tp_sl["stop_loss_order_price"],
        stop_loss_trigger_price_type=tp_sl["stop_loss_trigger_price_type"],
        raw=raw,
    )


def _current_order_view_cancel_reference(order: OrderStatusView) -> str:
    return _trade_order_cancel_reference(_current_order_view_to_trade_order_item(order))


def _current_order_view_program_owner_label(order: OrderStatusView) -> str | None:
    return _trade_order_program_owner_label(_current_order_view_to_trade_order_item(order))


def _current_order_view_owner_display_label(order: OrderStatusView) -> str:
    return _current_order_view_program_owner_label(order) or "未识别来源"


def _current_order_cancel_result_failed(result: OkxOrderResult) -> bool:
    return str(result.s_code or "").strip() not in {"", "0"}


def _current_order_cancel_result_error_message(order: OrderStatusView, result: OkxOrderResult) -> str:
    cancel_id = _current_order_view_cancel_reference(order) or "-"
    s_code = str(result.s_code or "").strip() or "-"
    s_msg = str(result.s_msg or "").strip() or "accepted"
    return (
        f"{_current_order_view_source_label(order)} 撤单失败。\n\n"
        f"合约：{order.inst_id or '-'}\n"
        f"标识：{cancel_id}\n"
        f"返回：sCode={s_code} | sMsg={s_msg}"
    )


POSITION_COLUMNS: tuple[tuple[str, str, int, Qt.AlignmentFlag], ...] = (
    ("inst_type", "类型", 72, Qt.AlignmentFlag.AlignCenter),
    ("mgn_mode", "保证金模式", 92, Qt.AlignmentFlag.AlignCenter),
    ("time_value", "时间价值", 88, Qt.AlignmentFlag.AlignRight),
    ("time_value_usdt", "时间≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_value", "内在价值", 88, Qt.AlignmentFlag.AlignRight),
    ("intrinsic_usdt", "内在≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("bid_price", "买一价", 78, Qt.AlignmentFlag.AlignRight),
    ("bid_usdt", "买一≈USDT", 78, Qt.AlignmentFlag.AlignRight),
    ("ask_price", "卖一价", 78, Qt.AlignmentFlag.AlignRight),
    ("ask_usdt", "卖一≈USDT", 78, Qt.AlignmentFlag.AlignRight),
    ("mark", "标记价", 84, Qt.AlignmentFlag.AlignRight),
    ("mark_usdt", "标记≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("avg", "开仓价", 84, Qt.AlignmentFlag.AlignRight),
    ("avg_usdt", "开仓≈USDT", 72, Qt.AlignmentFlag.AlignRight),
    ("open_value_usdt", "开仓价值≈USDT", 116, Qt.AlignmentFlag.AlignRight),
    ("break_even", "保本价格", 96, Qt.AlignmentFlag.AlignRight),
    ("pos", "持仓量", 170, Qt.AlignmentFlag.AlignRight),
    ("option_side", "买购:卖购 | 买沽:卖沽", 170, Qt.AlignmentFlag.AlignCenter),
    ("upl", "浮盈亏", 168, Qt.AlignmentFlag.AlignRight),
    ("upl_usdt", "浮盈≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("realized", "已实现盈亏", 118, Qt.AlignmentFlag.AlignRight),
    ("realized_usdt", "已实现≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("market_value", "市值", 160, Qt.AlignmentFlag.AlignRight),
    ("liq", "强平价", 92, Qt.AlignmentFlag.AlignRight),
    ("mgn_ratio", "保证金率", 88, Qt.AlignmentFlag.AlignRight),
    ("imr", "初始保证金", 100, Qt.AlignmentFlag.AlignRight),
    ("mmr", "维持保证金", 100, Qt.AlignmentFlag.AlignRight),
    ("delta", "Delta(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("gamma", "Gamma(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("vega", "Vega(PA)", 82, Qt.AlignmentFlag.AlignRight),
    ("theta", "Theta(PA)", 108, Qt.AlignmentFlag.AlignRight),
    ("theta_usdt", "Theta≈USDT", 108, Qt.AlignmentFlag.AlignRight),
    ("note", "备注", 200, Qt.AlignmentFlag.AlignLeft),
)

_POSITION_TREE_COLUMN_INDEX = {
    column_id: index for index, (column_id, _heading, _width, _alignment) in enumerate(POSITION_COLUMNS, start=1)
}
_POSITION_POSITIVE_COLOR = "#13803d"
_POSITION_NEGATIVE_COLOR = "#c23b3b"
_POSITION_TIME_INTRINSIC_COLOR = "#7c3aed"
_POSITION_BID_ASK_COLOR = "#d97706"
_POSITION_MARK_AVG_COLOR = "#2563eb"


def _position_display_foreground_colors(
    *,
    time_value_text: str,
    intrinsic_value_text: str,
    bid_price_text: str,
    ask_price_text: str,
    mark_price_text: str,
    avg_price_text: str,
    break_even_text: str,
    market_value_text: str,
    unrealized_pnl: Decimal | None,
) -> dict[str, QColor]:
    colors: dict[str, QColor] = {}
    for column_id, text, color in (
        ("time_value", time_value_text, _POSITION_TIME_INTRINSIC_COLOR),
        ("intrinsic_value", intrinsic_value_text, _POSITION_TIME_INTRINSIC_COLOR),
        ("bid_price", bid_price_text, _POSITION_BID_ASK_COLOR),
        ("ask_price", ask_price_text, _POSITION_BID_ASK_COLOR),
        ("mark", mark_price_text, _POSITION_MARK_AVG_COLOR),
        ("avg", avg_price_text, _POSITION_MARK_AVG_COLOR),
    ):
        if text.strip() not in {"", "-", "--"}:
            colors[column_id] = QColor(color)
    if unrealized_pnl is not None:
        if unrealized_pnl > 0:
            pnl_color = QColor(_POSITION_POSITIVE_COLOR)
        elif unrealized_pnl < 0:
            pnl_color = QColor(_POSITION_NEGATIVE_COLOR)
        else:
            pnl_color = None
        if pnl_color is not None and market_value_text.strip() not in {"", "-", "--"}:
            colors["market_value"] = pnl_color
        if pnl_color is not None and break_even_text.strip() not in {"", "-", "--"}:
            colors["break_even"] = pnl_color
    return colors


def _position_is_short(position: object) -> bool:
    pos_side = str(getattr(position, "pos_side", "") or "").strip().lower()
    if pos_side == "short":
        return True
    if pos_side == "long":
        return False
    return Decimal(str(getattr(position, "position", "0") or "0")) < 0


def _break_even_taker_fee_rate(profile: dict[str, str], *, inst_type: str) -> Decimal:
    del inst_type
    raw = str(profile.get("futures_taker_fee_rate") or "0.0360").strip()
    try:
        return max(Decimal(raw) / Decimal("100"), Decimal("0"))
    except Exception:
        return Decimal("0.00036")


def _position_break_even_price(
    position: object,
    upl_usdt_prices: dict[str, Decimal],
    *,
    fee_rate: Decimal,
) -> Decimal | None:
    """Return the underlying/contract break-even price with two taker fees."""
    inst_type = str(getattr(position, "inst_type", "") or "").strip().upper()
    avg_price = getattr(position, "avg_price", None)
    if not isinstance(avg_price, Decimal) or avg_price <= 0:
        return None
    two_way_fee_rate = max(fee_rate, Decimal("0")) * Decimal("2")
    is_short = _position_is_short(position)
    if inst_type in {"SWAP", "FUTURES"}:
        multiplier = Decimal("1") - two_way_fee_rate if is_short else Decimal("1") + two_way_fee_rate
        return avg_price * multiplier
    if inst_type != "OPTION":
        return None
    parts = str(getattr(position, "inst_id", "") or "").strip().upper().split("-")
    if len(parts) < 5:
        return None
    try:
        strike = Decimal(parts[3])
    except Exception:
        return None
    option_kind = parts[4]
    asset = parts[0]
    premium = avg_price
    margin_ccy = str(getattr(position, "margin_ccy", "") or "").strip().upper()
    if margin_ccy not in {"USDT", "USD", "USDC"}:
        underlying_price = upl_usdt_prices.get(asset)
        if underlying_price is None or underlying_price <= 0:
            return None
        premium *= underlying_price
    premium_with_fee = premium * (Decimal("1") + two_way_fee_rate)
    if option_kind == "C":
        return strike + premium_with_fee if not is_short else strike + premium * (Decimal("1") - two_way_fee_rate)
    if option_kind == "P":
        return strike - premium_with_fee if not is_short else strike - premium * (Decimal("1") - two_way_fee_rate)
    return None


def _group_row_values_with_break_even(group_type: str, metrics: dict[str, object]) -> tuple[str, ...]:
    values = list(_build_group_row_values(group_type, metrics))
    values.insert(15, "--")
    return tuple(values)

DEFAULT_VISIBLE_COLUMNS: tuple[str, ...] = (
    "inst_type",
    "mgn_mode",
    "mark",
    "mark_usdt",
    "avg",
    "avg_usdt",
    "open_value_usdt",
    "break_even",
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

# Qt 账户持仓页默认可见列，作为软件生成的默认设置。
DEFAULT_VISIBLE_COLUMNS: tuple[str, ...] = (
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
    "break_even",
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
    "break_even": 92,
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
        "1": "简单交易",
        "2": "单币种保证金",
        "3": "跨币种保证金",
        "4": "组合保证金",
    }
    return mapping.get(text, text)


def _format_account_position_mode_text(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "-"
    if text == "net":
        return "净持仓 net"
    if text in {"long_short", "long/short", "long_short_mode"}:
        return "双向持仓 long/short"
    return text


def _format_greeks_type_text(value: str | None) -> str:
    text = str(value or "").strip().upper()
    return text or "-"


def _format_bool_text(value: bool | None) -> str:
    if value is None:
        return "-"
    return "是" if value else "否"

ORDER_SOURCE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部来源", ""),
    ("普通委托", "normal"),
    ("算法委托", "algo"),
    ("WS 当前", "ws"),
)

ORDER_STATE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部状态", ""),
    ("等待成交", "live"),
    ("部分成交", "partially_filled"),
    ("已成交", "filled"),
    ("已撤单", "canceled"),
    ("失败", "order_failed"),
)

HISTORY_FILL_SIDE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部方向", ""),
    ("买入", "buy"),
    ("卖出", "sell"),
    ("多头", "long"),
    ("空头", "short"),
)

HISTORY_MARGIN_MODE_FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("全部模式", ""),
    ("全仓", "cross"),
    ("逐仓", "isolated"),
    ("现金", "cash"),
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
    ("15分钟", "15m"),
    ("1小时", "1H"),
    ("4小时", "4H"),
    ("1天", "1D"),
)


def _format_trade_order_size_with_coin(item: object, instruments: dict[str, object], *, filled: bool) -> str:
    size_text = _format_trade_order_size(getattr(item, "filled_size" if filled else "size", None))
    coin_text = (
        _format_trade_order_coin_filled_size(item, instruments)
        if filled
        else _format_trade_order_coin_size(item, instruments)
    )
    return size_text if coin_text in {"-", size_text} else f"{size_text} ({coin_text})"
POSITION_KLINE_BAR_MS = {
    "15m": 15 * 60 * 1000,
    "1H": 60 * 60 * 1000,
    "4H": 4 * 60 * 60 * 1000,
    "1D": 24 * 60 * 60 * 1000,
}


def _position_kline_timestamp(*values: object) -> int | None:
    for value in values:
        try:
            timestamp = int(str(value or "").strip())
        except (TypeError, ValueError):
            continue
        if timestamp <= 0:
            continue
        return timestamp * 1000 if timestamp < 100_000_000_000 else timestamp
    return None


def _position_history_kline_time_markers(item: OkxPositionHistoryItem) -> tuple[tuple[str, int], ...]:
    raw = item.raw if isinstance(item.raw, dict) else {}

    opened_at = _position_kline_timestamp(raw.get("openTime"), raw.get("openTs"), raw.get("cTime"), raw.get("createdTime"))
    closed_at = _position_kline_timestamp(raw.get("closeTime"), raw.get("closeTs"), raw.get("uTime"), raw.get("updateTime"), item.update_time)
    markers: list[tuple[str, int]] = []
    if opened_at is not None:
        markers.append(("开仓", opened_at))
    if closed_at is not None:
        markers.append(("平仓", closed_at))
    return tuple(markers)


def _current_position_kline_time_markers(
    position: OkxPosition,
    history_items: list[OkxPositionHistoryItem],
) -> tuple[tuple[str, int], ...]:
    inst_id = str(getattr(position, "inst_id", "") or "").strip().upper()
    if not inst_id:
        return ()
    raw = position.raw if isinstance(position.raw, dict) else {}
    markers: list[tuple[str, int]] = []
    opened_at = _position_kline_timestamp(
        raw.get("openTime"),
        raw.get("openTs"),
        raw.get("cTime"),
        raw.get("createdTime"),
    )
    if opened_at is not None:
        markers.append(("开仓", opened_at))
    for item in history_items:
        if str(item.inst_id or "").strip().upper() != inst_id:
            continue
        markers.extend(_position_history_kline_time_markers(item))
    seen: set[tuple[str, int]] = set()
    unique_markers: list[tuple[str, int]] = []
    for marker in markers:
        if marker in seen:
            continue
        seen.add(marker)
        unique_markers.append(marker)
    return tuple(unique_markers)


def _position_kline_candle_limit(
    bar: str,
    time_markers: tuple[tuple[str, int], ...],
    *,
    now_ms: int | None = None,
) -> int:
    base_limit = 480
    bar_ms = POSITION_KLINE_BAR_MS.get(bar)
    timestamps = [timestamp for _label, timestamp in time_markers if timestamp > 0]
    if bar_ms is None or not timestamps:
        return base_limit
    current_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    oldest_timestamp = min(timestamps)
    if oldest_timestamp >= current_ms:
        return base_limit
    required_bars = ((current_ms - oldest_timestamp) + bar_ms - 1) // bar_ms
    return min(max(base_limit, required_bars + 32), 2000)


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
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("保存")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        layout.addWidget(buttons)

    @property
    def result_text(self) -> str | None:
        return self._result_text

    def _accept(self) -> None:
        self._result_text = _normalize_position_note_text(self._editor.toPlainText())
        self.accept()


class QuantityInputDialog(QDialog):
    def __init__(
        self,
        *,
        title: str,
        prompt: str,
        initial_value: str,
        unit_text: str,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self._result_text: str | None = None
        self.setWindowTitle(title)
        self.resize(420, 140)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        prompt_label = QLabel(prompt)
        prompt_label.setWordWrap(True)
        layout.addWidget(prompt_label)

        row = QHBoxLayout()
        row.setSpacing(8)
        self._edit = QLineEdit(initial_value)
        self._edit.selectAll()
        row.addWidget(self._edit, 1)
        unit_label = QLabel(unit_text)
        unit_label.setObjectName("Subtle")
        row.addWidget(unit_label)
        layout.addLayout(row)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    @property
    def result_text(self) -> str | None:
        return self._result_text

    def _accept(self) -> None:
        self._result_text = self._edit.text().strip()
        self.accept()


class AccountOverviewDialog(QDialog):
    def __init__(self, *, summary_text: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("账户信息")
        self.resize(920, 680)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        title = QLabel("账户持仓概览")
        title.setObjectName("SectionTitle")
        layout.addWidget(title)

        detail = QTextEdit()
        detail.setReadOnly(True)
        detail_font = QFont("Consolas")
        detail_font.setPointSize(10)
        detail.setFont(detail_font)
        detail.setPlainText(summary_text)
        layout.addWidget(detail, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class InstrumentKlineLoadThread(QThread):
    loaded = Signal(str, str, str, str, object)
    failed = Signal(str, str, str, str)

    def __init__(self, *, inst_id: str, inst_type: str, bar: str, limit: int = 240, before_ts: int | None = None) -> None:
        super().__init__()
        self._inst_id = inst_id.strip().upper()
        self._inst_type = inst_type.strip().upper()
        self._bar = bar.strip()
        self._limit = max(60, limit)
        self._before_ts = before_ts if before_ts is not None and before_ts > 0 else None

    def run(self) -> None:
        source = "mark" if self._inst_type == "OPTION" else "trade"
        try:
            client = OkxRestClient()
            if source == "mark":
                candles = (
                    client.get_mark_price_candles_before(self._inst_id, self._bar, self._before_ts, limit=self._limit)
                    if self._before_ts is not None
                    else client.get_mark_price_candles(self._inst_id, self._bar, limit=self._limit)
                )
            else:
                candles = (
                    client.get_candles_history_before(self._inst_id, self._bar, self._before_ts, limit=self._limit)
                    if self._before_ts is not None
                    else client.get_candles_history(self._inst_id, self._bar, limit=self._limit)
                )
            if not candles:
                if self._before_ts is not None:
                    self.loaded.emit(self._inst_id, self._inst_type, self._bar, source, [])
                    return
                raise ValueError("当前周期没有可用 K 线数据。")
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
        self.setWindowTitle("合约 K 线图")
        self.resize(max(int(initial_width), 480), max(int(initial_height), 320))

        self._inst_id = ""
        self._inst_type = ""
        self._underlying_usdt_price: Decimal | None = None
        self._underlying_usdt_basis = ""
        self._option_entry_price: Decimal | None = None
        self._time_markers: tuple[tuple[str, int], ...] = ()
        self._current_bar = initial_bar if initial_bar in {bar for _text, bar in POSITION_KLINE_BAR_OPTIONS} else "1H"
        self._load_thread: InstrumentKlineLoadThread | None = None
        self._loading_older_candles = False
        self._no_more_older_candles = False
        self._bar_buttons: dict[str, QPushButton] = {}
        self._prefs_changed = prefs_changed

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        header = QHBoxLayout()
        header.setSpacing(8)
        self._title_label = QLabel("等待选择持仓")
        self._title_label.setObjectName("SectionTitle")
        self._status_label = QLabel("点击持仓后自动加载对应 K 线。")
        self._status_label.setObjectName("Subtle")
        header.addWidget(self._title_label, 1)
        header.addWidget(self._status_label, 2)
        layout.addLayout(header)

        bar_row = QHBoxLayout()
        bar_row.setSpacing(8)
        bar_row.addWidget(QLabel("周期"))
        for text, bar in POSITION_KLINE_BAR_OPTIONS:
            button = QPushButton(text)
            button.setCheckable(True)
            button.clicked.connect(lambda _checked=False, target_bar=bar: self._select_bar(target_bar))
            self._bar_buttons[bar] = button
            bar_row.addWidget(button)
        bar_row.addStretch(1)
        layout.addLayout(bar_row)

        self._chart = CandlestickChartView()
        # Use a small relay lambda here.  PySide can fail to resolve a dynamically
        # imported decorated slot on this dialog's meta-object at runtime.
        self._chart.older_data_requested.connect(lambda before_ts: self._load_older_candles(int(before_ts)))
        self._chart.show_message("请点击一条持仓加载 K 线")
        layout.addWidget(self._chart, 1)
        self._sync_bar_buttons()

    def show_instrument(
        self,
        *,
        inst_id: str,
        inst_type: str,
        underlying_usdt_price: Decimal | None = None,
        underlying_usdt_basis: str = "",
        option_entry_price: Decimal | None = None,
        time_markers: tuple[tuple[str, int], ...] = (),
    ) -> None:
        self._inst_id = inst_id.strip().upper()
        self._inst_type = inst_type.strip().upper()
        self._underlying_usdt_price = underlying_usdt_price if underlying_usdt_price is not None and underlying_usdt_price > 0 else None
        self._underlying_usdt_basis = underlying_usdt_basis.strip() if self._underlying_usdt_price is not None else ""
        self._option_entry_price = option_entry_price if option_entry_price is not None and option_entry_price > 0 else None
        self._time_markers = tuple(time_markers)
        self._no_more_older_candles = False
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
            self._title_label.setText("等待选择持仓")
            return
        source_label = "标记价格K线" if self._inst_type == "OPTION" else "成交价格K线"
        self._title_label.setText(f"{self._inst_id} | {source_label}")

    @Slot()
    def _load_current_bar(self) -> None:
        if not self._inst_id:
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        source_label = "标记价格" if self._inst_type == "OPTION" else "成交价格"
        self._status_label.setText(f"正在加载 {self._inst_id} {self._current_bar} {source_label} K 线...")
        self._load_thread = InstrumentKlineLoadThread(
            inst_id=self._inst_id,
            inst_type=self._inst_type,
            bar=self._current_bar,
            limit=_position_kline_candle_limit(self._current_bar, self._time_markers),
        )
        self._loading_older_candles = False
        self._load_thread.loaded.connect(self._apply_loaded_candles)
        self._load_thread.failed.connect(self._apply_load_error)
        self._load_thread.finished.connect(self._clear_finished_thread)
        self._load_thread.start()

    def _load_older_candles(self, before_ts: int) -> None:
        if not self._inst_id or self._no_more_older_candles:
            return
        if self._load_thread is not None and self._load_thread.isRunning():
            return
        if before_ts <= 0:
            return
        self._loading_older_candles = True
        self._status_label.setText(f"正在加载 {self._inst_id} 更早的 K 线...")
        self._load_thread = InstrumentKlineLoadThread(
            inst_id=self._inst_id,
            inst_type=self._inst_type,
            bar=self._current_bar,
            limit=240,
            before_ts=before_ts,
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
        source_label = "标记价格" if source == "mark" else "成交价格"
        if self._loading_older_candles:
            self._loading_older_candles = False
            if not candles:
                self._no_more_older_candles = True
                self._status_label.setText(f"{inst_id} | 已加载到最早的 K 线数据")
                return
            loaded_more = self._chart.prepend_candles(candles)
            if not loaded_more:
                self._no_more_older_candles = True
                self._status_label.setText(f"{inst_id} | 已加载到最早的 K 线数据")
                return
            self._status_label.setText(f"{inst_id} | 已加载更多 K 线，可继续向左拖动")
            return
        else:
            self._chart.set_candles(
                title=f"{inst_id} {source_label}K线 | {bar}",
                candles=candles,
                show_moving_averages=True,
                tooltip_close_usdt_rate=self._underlying_usdt_price if inst_type == "OPTION" else None,
                tooltip_close_usdt_basis=self._underlying_usdt_basis if inst_type == "OPTION" else "",
                tooltip_entry_price=self._option_entry_price if inst_type == "OPTION" else None,
                time_markers=self._time_markers,
            )
        latest = candles[-1] if candles else None
        latest_text = ""
        latest_time_text = ""
        if isinstance(latest, Candle):
            latest_text = f" | 最新价 {latest.close}"
            latest_time_text = f" | 时间 {datetime.fromtimestamp(latest.ts / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
        self._status_label.setText(f"{inst_id} | {bar} | {source_label} K 线已加载{latest_text}{latest_time_text}")

    @Slot(str, str, str, str)
    def _apply_load_error(self, inst_id: str, inst_type: str, bar: str, message: str) -> None:
        if inst_id != self._inst_id or inst_type != self._inst_type or bar != self._current_bar:
            return
        if self._loading_older_candles:
            self._loading_older_candles = False
            self._status_label.setText(f"更早的 K 线加载失败：{message}；继续向左拖动可重试")
            return
        self._chart.show_message(f"{inst_id} K 线加载失败")
        self._status_label.setText(f"K 线加载失败：{message}")

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
        self.setWindowTitle("持仓大窗列设置")
        self.resize(560, 520)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)
        tip = QLabel("可按区域勾选显示/隐藏列。`合约 / 分组` 为结构列，当前固定显示。")
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
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
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

    def shutdown(self) -> None:
        return

    def open_option_roll(
        self,
        *,
        position: OkxPosition,
        instrument: object,
        ticker: object,
        api_name: str,
    ) -> None:
        project_root = Path(__file__).resolve().parents[1]
        command = [
            sys.executable,
            str(project_root / "main.py"),
            "--data-dir",
            str(data_root()),
            "--option-roll-inst-id",
            position.inst_id,
            "--option-roll-pos-side",
            position.pos_side,
            "--option-roll-size",
            str(position.position),
            "--option-roll-profile",
            api_name,
        ]
        subprocess.Popen(command, cwd=str(project_root))


class PositionProtectionDialog(QDialog):
    def __init__(
        self,
        *,
        manager: PositionProtectionManager,
        client: OkxRestClient,
        runtime_provider: Callable[[], object | None],
        selected_option_provider: Callable[[], OkxPosition | None],
        notifier_provider: Callable[[], EmailNotifier | None] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self._manager = manager
        self._client = client
        self._runtime_provider = runtime_provider
        self._selected_option_provider = selected_option_provider
        self._notifier_provider = notifier_provider
        self._selected_position: OkxPosition | None = None
        self._form_position_key = ""
        self._session_ids: list[str] = []
        self._last_fixed_price_memory = {"tp": "", "sl": ""}
        self._last_abnormal_protection_alert: dict[str, str] = {}

        self.setWindowTitle("设置期权保护")
        self.resize(1080, 760)

        self._build_ui()
        self._safe_refresh_from_selection(force=True, context="init")
        self._safe_refresh_sessions(context="init")

        self._refresh_timer = QTimer(self)
        self._refresh_timer.timeout.connect(self._on_refresh_timer_timeout)
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

        self._title_label = QLabel("请先在当前持仓里选中一条期权仓位。")
        self._title_label.setObjectName("SectionTitle")
        self._title_label.setWordWrap(True)
        self._logic_hint = QLabel("保护逻辑会跟随上方选中的期权仓位。")
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
        self._trigger_combo.currentIndexChanged.connect(self._safe_handle_trigger_source_changed)

        self._spot_symbol_edit = QLineEdit()
        self._tp_trigger_edit = QLineEdit()
        self._sl_trigger_edit = QLineEdit()
        self._tp_mode_combo = QComboBox()
        self._sl_mode_combo = QComboBox()
        for label in PROTECTION_ORDER_MODE_OPTIONS:
            self._tp_mode_combo.addItem(label)
            self._sl_mode_combo.addItem(label)
        self._tp_mode_combo.currentIndexChanged.connect(self._safe_handle_order_mode_changed)
        self._sl_mode_combo.currentIndexChanged.connect(self._safe_handle_order_mode_changed)
        self._tp_price_edit = QLineEdit()
        self._sl_price_edit = QLineEdit()
        self._tp_slippage_edit = QLineEdit("0")
        self._sl_slippage_edit = QLineEdit("0")
        self._poll_seconds_edit = QLineEdit("2")

        form.addWidget(QLabel("触发条件"), 0, 0)
        form.addWidget(self._trigger_combo, 0, 1)
        form.addWidget(QLabel("现货标的"), 0, 2)
        form.addWidget(self._spot_symbol_edit, 0, 3)
        form.addWidget(QLabel("止盈触发价"), 1, 0)
        form.addWidget(self._tp_trigger_edit, 1, 1)
        form.addWidget(QLabel("止损触发价"), 1, 2)
        form.addWidget(self._sl_trigger_edit, 1, 3)
        form.addWidget(QLabel("止盈报单方式"), 2, 0)
        form.addWidget(self._tp_mode_combo, 2, 1)
        form.addWidget(QLabel("止盈报单价格"), 2, 2)
        form.addWidget(self._tp_price_edit, 2, 3)
        form.addWidget(QLabel("止盈滑点"), 3, 0)
        form.addWidget(self._tp_slippage_edit, 3, 1)
        form.addWidget(QLabel("轮询秒数"), 3, 2)
        form.addWidget(self._poll_seconds_edit, 3, 3)
        form.addWidget(QLabel("止损报单方式"), 4, 0)
        form.addWidget(self._sl_mode_combo, 4, 1)
        form.addWidget(QLabel("止损报单价格"), 4, 2)
        form.addWidget(self._sl_price_edit, 4, 3)
        form.addWidget(QLabel("止损滑点"), 5, 0)
        form.addWidget(self._sl_slippage_edit, 5, 1)
        top_layout.addLayout(form)

        action_row = QHBoxLayout()
        start_button = QPushButton("启动保护")
        start_button.clicked.connect(self._start_selected_position_protection)
        stop_button = QPushButton("停止选中任务")
        stop_button.clicked.connect(self._stop_selected_position_protection)
        clear_button = QPushButton("清除已结束")
        clear_button.clicked.connect(self._clear_finished_position_protections)
        close_button = QPushButton("关闭")
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
        self._session_status_label = QLabel("当前没有运行中的期权保护任务。")
        self._session_status_label.setObjectName("Subtle")
        sessions_layout.addWidget(self._session_status_label)
        session_headers = (
            "API",
            "期权合约",
            "触发条件",
            "触发标的",
            "价格类型",
            "方向",
            "持仓方向",
            "止盈触发",
            "止损触发",
            "报单方式",
            "轮询",
            "状态",
            "启动时间",
        )
        self._sessions_table = QTableWidget(0, len(session_headers))
        self._sessions_table.setHorizontalHeaderLabels(session_headers)
        self._sessions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._sessions_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._sessions_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._sessions_table.verticalHeader().setVisible(False)
        self._configure_sessions_table_columns(len(session_headers))
        self._sessions_table.itemSelectionChanged.connect(self._safe_handle_selected_session_detail_changed)
        sessions_layout.addWidget(self._sessions_table, 1)
        bottom_split.addWidget(sessions_panel)

        detail_panel = QFrame()
        detail_panel.setObjectName("Panel")
        detail_layout = QVBoxLayout(detail_panel)
        detail_layout.setContentsMargins(10, 10, 10, 10)
        detail_layout.setSpacing(8)
        detail_title = QLabel("任务详情")
        detail_title.setObjectName("SectionTitle")
        self._detail_text = QTextEdit()
        self._detail_text.setReadOnly(True)
        detail_layout.addWidget(detail_title)
        detail_layout.addWidget(self._detail_text, 1)
        bottom_split.addWidget(detail_panel)
        bottom_split.setSizes([340, 240])
        layout.addWidget(bottom_split, 1)

    def _configure_sessions_table_columns(self, column_count: int) -> None:
        header = self._sessions_table.horizontalHeader()
        header.setStretchLastSection(False)
        for column in range(column_count):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)
        for column in (1, 2, 3, 9, 12):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.Interactive)
        default_widths = {
            0: 120,
            1: 210,
            2: 170,
            3: 120,
            9: 230,
            12: 96,
        }
        for column, width in default_widths.items():
            self._sessions_table.setColumnWidth(column, width)
        self._sessions_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

    def _report_refresh_exception(self, context: str, exc: Exception) -> None:
        message = f"期权保护窗口刷新异常（{context}）：{exc}"
        append_log_line(f"[期权保护窗口] {message}")
        trace_text = traceback.format_exc().strip()
        if trace_text:
            for line in trace_text.splitlines():
                append_log_line(f"[期权保护窗口] {line}")
        if hasattr(self, "_session_status_label"):
            self._session_status_label.setText("期权保护窗口刷新异常，请重新打开后重试。")
        if hasattr(self, "_detail_text"):
            self._detail_text.setPlainText(message)

    def _safe_refresh_sessions(self, *, context: str) -> bool:
        try:
            self._refresh_sessions()
            return True
        except Exception as exc:
            self._report_refresh_exception(context, exc)
            return False

    def _safe_refresh_from_selection(self, *, force: bool, context: str) -> bool:
        try:
            self._refresh_from_selection(force=force)
            return True
        except Exception as exc:
            self._report_refresh_exception(context, exc)
            return False

    def _safe_refresh_selected_session_detail(self, *, context: str) -> bool:
        try:
            self._refresh_selected_session_detail()
            return True
        except Exception as exc:
            self._report_refresh_exception(context, exc)
            return False

    @Slot()
    def _safe_handle_selected_session_detail_changed(self) -> None:
        self._safe_refresh_selected_session_detail(context="selection")

    @Slot()
    def _safe_handle_trigger_source_changed(self) -> None:
        try:
            self._on_trigger_source_changed()
            if PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self._trigger_combo.currentText(), "option_mark") == "spot_last":
                self._maybe_autofill_spot_trigger_prices()
        except Exception as exc:
            self._report_refresh_exception("trigger_source", exc)

    @Slot()
    def _safe_handle_order_mode_changed(self) -> None:
        try:
            self._refresh_order_mode_widgets()
        except Exception as exc:
            self._report_refresh_exception("order_mode", exc)

    @Slot()
    def _on_refresh_timer_timeout(self) -> None:
        self._safe_refresh_sessions(context="timer")
        self._safe_refresh_from_selection(force=False, context="timer")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._refresh_timer.stop()
        super().closeEvent(event)

    def showEvent(self, event) -> None:  # type: ignore[override]
        if not self._refresh_timer.isActive():
            self._refresh_timer.start(1200)
        super().showEvent(event)

    def _current_position(self) -> OkxPosition | None:
        try:
            current = self._selected_option_provider()
        except Exception as exc:
            self._report_refresh_exception("selected_option_provider", exc)
            current = None
        if current is not None:
            self._selected_position = current
            return current
        return self._selected_position

    def _refresh_from_selection(self, *, force: bool) -> None:
        position = self._current_position()
        if position is None and self._selected_position is None:
            self._title_label.setText("请先在当前持仓里选中一条期权仓位。")
            self._logic_hint.setText("保护逻辑会跟随上方选中的期权仓位。")
            return
        if position is None:
            return
        self._selected_position = position
        position_key = _position_tree_row_id(position)
        direction = derive_position_direction(position)
        self._title_label.setText(
            f"当前选中：{position.inst_id} | 方向={direction.upper()} | 持仓={_format_optional_decimal(position.position)} | 开仓均价={_format_optional_decimal(position.avg_price)}"
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
            self._logic_hint.setText("保护逻辑会跟随上方选中的期权仓位。")
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

    def _maybe_autofill_spot_trigger_prices(self) -> None:
        tp_blank = not self._tp_trigger_edit.text().strip()
        sl_blank = not self._sl_trigger_edit.text().strip()
        if not tp_blank and not sl_blank:
            return
        position = self._current_position()
        if position is None:
            return
        trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text()) or infer_default_spot_inst_id(position.inst_id)
        if not trigger_inst_id:
            return
        current_price = self._client.get_trigger_price(trigger_inst_id, "last")
        offset = Decimal("200")
        profit_on_rise = infer_protection_profit_on_rise(
            option_inst_id=position.inst_id,
            direction=derive_position_direction(position),
            trigger_inst_id=trigger_inst_id,
            trigger_price_type="last",
        )
        take_profit_price = current_price + offset if profit_on_rise else current_price - offset
        stop_loss_price = current_price - offset if profit_on_rise else current_price + offset
        if tp_blank:
            self._tp_trigger_edit.setText(format_decimal(take_profit_price))
        if sl_blank:
            self._sl_trigger_edit.setText(format_decimal(stop_loss_price))

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
            QMessageBox.warning(self, "启动失败", "当前没有可用的 API 运行时。")
            return
        if position is None or position.inst_type != "OPTION":
            QMessageBox.information(self, "提示", "请先在当前持仓中选中一条期权仓位。")
            return
        try:
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self._client, protection, position)
            self._manager.set_notifier(self._notifier_provider() if self._notifier_provider is not None else None)
            config = self._build_strategy_config(runtime=runtime, position=position, protection=protection)
            self._manager.start(runtime.credentials, config, protection)
            self._safe_refresh_sessions(context="start")
        except Exception as exc:
            QMessageBox.critical(self, "启动保护失败", str(exc))

    def _stop_selected_position_protection(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            QMessageBox.information(self, "提示", "请先在下方任务列表里选中一条保护任务。")
            return
        try:
            self._manager.stop(session_id)
            self._refresh_sessions()
        except Exception as exc:
            QMessageBox.critical(self, "停止失败", str(exc))

    def _clear_finished_position_protections(self) -> None:
        cleared = self._manager.clear_finished()
        self._safe_refresh_sessions(context="clear_finished")
        if cleared <= 0:
            QMessageBox.information(self, "提示", "当前没有可清理的已结束任务。")

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self._trigger_combo.currentText()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 标记价"
        else:
            trigger_inst_id = normalize_spot_inst_id(self._spot_symbol_edit.text())
            if not trigger_inst_id:
                raise ValueError("现货触发模式下，请填写现货标的，例如 BTC-USDT。")
            trigger_instrument = self._client.get_instrument(trigger_inst_id)
            if str(trigger_instrument.inst_type or "").upper() != "SPOT":
                raise ValueError("现货触发模式下，标的必须是现货交易对，例如 BTC-USDT。")
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 最新价"

        take_profit_trigger = self._parse_optional_positive_decimal(self._tp_trigger_edit.text(), "止盈触发价")
        stop_loss_trigger = self._parse_optional_positive_decimal(self._sl_trigger_edit.text(), "止损触发价")
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("止盈触发价和止损触发价至少要填写一个。")

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
        take_profit_order_price = self._parse_positive_decimal(self._tp_price_edit.text(), "止盈报单价格") if take_profit_order_mode == "fixed_price" else None
        stop_loss_order_price = self._parse_positive_decimal(self._sl_price_edit.text(), "止损报单价格") if stop_loss_order_mode == "fixed_price" else None
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
            take_profit_slippage=self._parse_nonnegative_decimal(self._tp_slippage_edit.text(), "止盈滑点"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self._sl_slippage_edit.text(), "止损滑点"),
            poll_seconds=float(self._parse_positive_decimal(self._poll_seconds_edit.text(), "轮询秒数")),
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
        self._session_status_label.setText(f"当前保护任务：{len(sessions)}" if sessions else "当前没有运行中的期权保护任务。")
        self._sessions_table.setRowCount(len(sessions))
        self._session_ids = [item.session_id for item in sessions]
        for row, item in enumerate(sessions):
            order_mode_summary = (
                "止盈/止损: "
                f"{_format_protection_order_mode_label(item.take_profit_order_mode)}/"
                f"{_format_protection_order_mode_label(item.stop_loss_order_mode)}"
            )
            values = (
                item.api_name or "-",
                item.option_inst_id,
                item.trigger_label,
                item.trigger_inst_id,
                _format_protection_trigger_price_type(item.trigger_price_type),
                item.direction,
                item.pos_side or "-",
                _format_optional_decimal(item.take_profit_trigger),
                _format_optional_decimal(item.stop_loss_trigger),
                order_mode_summary,
                f"{item.poll_seconds:g}s",
                item.status,
                item.started_at.strftime("%H:%M:%S"),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column in {0, 4, 5, 6, 7, 8, 10, 11, 12}:
                    cell.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
                self._sessions_table.setItem(row, column, cell)
        target_row = -1
        if selected_before and selected_before in self._session_ids:
            target_row = self._session_ids.index(selected_before)
        elif self._session_ids:
            target_row = 0
        if target_row >= 0:
            self._sessions_table.selectRow(target_row)
        else:
            self._detail_text.setPlainText("请选择一条保护任务查看详情。")
        self._refresh_selected_session_detail()

    def _refresh_selected_session_detail(self) -> None:
        session_id = self._selected_session_id()
        sessions = {item.session_id: item for item in self._manager.list_sessions()}
        session = sessions.get(session_id)
        if session is None:
            self._detail_text.setPlainText("请选择一条保护任务查看详情。")
            return

        status = str(session.status)
        last_message = (session.last_message or "").strip()
        detail_lines = [
            f"任务：{session.session_id}",
            f"API配置：{session.api_name or '-'}",
            f"期权合约：{session.option_inst_id}",
            f"触发条件：{session.trigger_label}",
            f"触发标的：{session.trigger_inst_id}",
            f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
            f"方向：{session.direction}",
            f"持仓方向：{session.pos_side or '-'}",
            f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
            f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
            f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
            f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
            f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
            f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
            f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
            f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
            f"轮询秒数：{session.poll_seconds:g}",
            f"状态：{status}",
            f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        if last_message:
            if status == "异常":
                detail_lines.extend(["", f"异常原因：{last_message}"])
                previous_message = self._last_abnormal_protection_alert.get(session.session_id, "")
                if previous_message != last_message:
                    append_log_line(
                        f"[期权保护窗口] 保护任务 {session.session_id} 进入异常状态，请关注。{last_message}"
                    )
                    self._last_abnormal_protection_alert[session.session_id] = last_message
            else:
                detail_lines.extend(["", f"最新状态：{last_message}"])
                self._last_abnormal_protection_alert.pop(session.session_id, None)
        self._detail_text.setPlainText("\n".join(detail_lines))

    def _refresh_selected_session_detail_legacy(self) -> None:
        session_id = self._selected_session_id()
        sessions = {item.session_id: item for item in self._manager.list_sessions()}
        session = sessions.get(session_id)
        if session is None:
            self._detail_text.setPlainText("请选择一条保护任务查看详情。")
            return
        self._detail_text.setPlainText(
            "\n".join(
                [
                    f"任务：{session.session_id}",
                    f"API配置：{session.api_name or '-'}",
                    f"期权合约：{session.option_inst_id}",
                    f"触发条件：{session.trigger_label}",
                    f"触发标的：{session.trigger_inst_id}",
                    f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"方向：{session.direction}",
                    f"持仓方向：{session.pos_side or '-'}",
                    f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
                    f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
                    f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
                    f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
                    f"轮询秒数：{session.poll_seconds:g}",
                    f"状态：{session.status}",
                    f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"最新状态：{session.last_message}",
                ]
            )
        )

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw.strip())
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
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
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value


class AccountPositionsHomeWidget(QWidget):
    _ui_callback = Signal(object)
    _selected_position_manual_flatten_callback = Signal(object)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = load_runtime("moni") or load_runtime()
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._last_profile_name = self._runtime.credential_profile_name if self._runtime is not None else ""
        self._profile_switch_guard = False
        self._profile_change_serial = 0
        self._profile_change_ready = False
        self._private_thread_generation = 0
        self._profile_switch_in_progress = False
        self._profile_switch_requested_target = ""
        self._profile_switch_requested_runtime = None
        self._profile_switch_requested_serial = 0
        self._profile_switch_deadline_monotonic = 0.0
        self._profile_switch_force_terminate_sent = False
        self._profile_switch_poll_timer: QTimer | None = None
        self._profile_unlock_dialog: QDialog | None = None
        self._account_feed: AccountFeedThread | None = None
        self._order_feed: OrderFeedThread | None = None
        self._order_history_feed: OrderHistoryFeedThread | None = None
        self._fill_history_feed: FillHistoryFeedThread | None = None
        self._position_history_feed: PositionHistoryFeedThread | None = None
        self._retired_threads: list[QThread] = []
        self._shutdown_in_progress = False
        self._shutdown_finish_callbacks: list[Callable[[], None]] = []
        self._shutdown_deadline_monotonic = 0.0
        self._shutdown_force_terminate_sent = False
        self._shutdown_poll_timer: QTimer | None = None

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
        self._current_order_canceling = False
        self._selected_position_manual_flatten_running = False
        self._shared_client = OkxRestClient()
        self._realtime_store = get_shared_realtime_account_store(client=self._shared_client)
        self._realtime_store.snapshot_ready.connect(self._apply_realtime_snapshot)
        self._realtime_store.status_changed.connect(self._set_realtime_status)
        self._protection_manager = PositionProtectionManager(self._shared_client, lambda _message: None)
        self._protection_dialog: PositionProtectionDialog | None = None
        self._legacy_option_tools = LegacyOptionToolsHost(parent=self, runtime_provider=lambda: self._runtime)
        self._instrument_kline_dialog: InstrumentKlineDialog | None = None
        self._ui_callback.connect(self._run_ui_callback)
        self._selected_position_manual_flatten_callback.connect(self._run_selected_position_manual_flatten_callback)

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
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("订单 WS 等待 API 解锁")
            self._summary_label.setText("当前 API 配置已加切换密码，请先解锁后再加载账户持仓。")
        else:
            if self._last_profile_name:
                self._unlocked_profiles.add(self._last_profile_name)
            self._start_private_threads()
        self._profile_change_ready = True

    def workspace_profile_name(self) -> str:
        return self._last_profile_name

    def connection_snapshot(self) -> dict[str, object]:
        text = self._account_status.text().strip() if hasattr(self, "_account_status") else ""
        private_online = any(token in text for token in ("实时更新", "已通过", "WS 在线", "WS在线"))
        if "未解锁" in text:
            private_status = "账户未解锁"
        elif "失败" in text or "断" in text:
            private_status = "账户连接异常"
        else:
            private_status = "账户连接中"
        return {
            "public_online": False,
            "private_online": private_online,
            "private_status": "私有WS在线" if private_online else private_status,
        }

    def set_workspace_managed(self, managed: bool) -> None:
        visible = not bool(managed)
        self._profile_label.setVisible(visible)
        self._profile_combo.setVisible(visible)

    def _select_profile_without_signal(self, profile_name: str) -> None:
        with QSignalBlocker(self._profile_combo):
            index = self._profile_combo.findText(profile_name)
            if index >= 0:
                self._profile_combo.setCurrentIndex(index)

    def apply_workspace_profile(self, profile_name: str) -> None:
        target = profile_name.strip()
        if not target or target == self._last_profile_name:
            return
        self._unlocked_profiles.add(target)
        self._select_profile_without_signal(target)
        self._profile_change_serial += 1
        serial = self._profile_change_serial
        self._set_profile_switch_in_progress(True)
        QTimer.singleShot(0, lambda: self._apply_profile_change(target, serial))

    def _retire_thread(self, thread: QThread) -> None:
        try:
            thread.disconnect()
        except Exception:
            pass
        if thread in self._retired_threads:
            return
        self._retired_threads.append(thread)

        def _cleanup() -> None:
            if thread in self._retired_threads:
                self._retired_threads.remove(thread)
            thread.deleteLater()

        thread.finished.connect(_cleanup)

    def _flush_retired_threads(self, *, wait_ms: int = 2500, terminate_wait_ms: int = 1000) -> None:
        pending = list(self._retired_threads)
        for thread in pending:
            try:
                if thread.isRunning():
                    thread.wait(wait_ms)
                if thread.isRunning():
                    thread.terminate()
                    thread.wait(terminate_wait_ms)
            except Exception:
                pass
            try:
                if thread in self._retired_threads:
                    self._retired_threads.remove(thread)
            except Exception:
                pass
            try:
                thread.deleteLater()
            except Exception:
                pass

    def _set_profile_switch_in_progress(self, in_progress: bool) -> None:
        self._profile_switch_in_progress = in_progress
        if hasattr(self, "_profile_combo"):
            try:
                self._profile_combo.setEnabled(not in_progress)
            except Exception:
                pass

    def _clear_profile_switch_request(self) -> None:
        timer = self._profile_switch_poll_timer
        if timer is not None and timer.isActive():
            timer.stop()
        self._profile_switch_requested_target = ""
        self._profile_switch_requested_runtime = None
        self._profile_switch_requested_serial = 0
        self._profile_switch_deadline_monotonic = 0.0
        self._profile_switch_force_terminate_sent = False
        self._set_profile_switch_in_progress(False)

    def _ensure_profile_switch_poll_timer(self) -> QTimer:
        timer = self._profile_switch_poll_timer
        if timer is not None:
            return timer
        timer = QTimer(self)
        timer.setInterval(100)
        timer.timeout.connect(self._poll_profile_switch_completion)
        self._profile_switch_poll_timer = timer
        return timer

    def _begin_profile_switch_restart(self, target: str, runtime: object, serial: int) -> None:
        if serial != self._profile_change_serial:
            self._clear_profile_switch_request()
            return
        self._profile_switch_requested_target = target
        self._profile_switch_requested_runtime = runtime
        self._profile_switch_requested_serial = serial
        self._profile_switch_deadline_monotonic = time.monotonic() + 3.0
        self._profile_switch_force_terminate_sent = False
        self._stop_private_threads(wait_ms=0)
        self._stop_order_history_thread(wait_ms=0)
        self._stop_fill_history_thread(wait_ms=0)
        self._stop_position_history_thread(wait_ms=0)
        self._poll_profile_switch_completion()

    def _poll_profile_switch_completion(self) -> None:
        running_threads: list[QThread] = []
        for thread in list(self._retired_threads):
            try:
                if thread.isRunning():
                    running_threads.append(thread)
                    continue
            except Exception:
                pass
            try:
                if thread in self._retired_threads:
                    self._retired_threads.remove(thread)
            except Exception:
                pass
            try:
                thread.deleteLater()
            except Exception:
                pass
        if running_threads:
            deadline = self._profile_switch_deadline_monotonic
            if (not self._profile_switch_force_terminate_sent) and deadline and time.monotonic() >= deadline:
                self._profile_switch_force_terminate_sent = True
                for thread in running_threads:
                    try:
                        thread.terminate()
                    except Exception:
                        pass
            timer = self._ensure_profile_switch_poll_timer()
            if not timer.isActive():
                timer.start()
            return
        timer = self._profile_switch_poll_timer
        if timer is not None and timer.isActive():
            timer.stop()
        if self._profile_switch_requested_serial != self._profile_change_serial:
            self._clear_profile_switch_request()
            current = self._current_profile_name()
            if current and current != self._last_profile_name:
                QTimer.singleShot(
                    0,
                    lambda current=current, serial=self._profile_change_serial: self._dispatch_profile_change(current, serial),
                )
            return
        target = self._profile_switch_requested_target
        runtime = self._profile_switch_requested_runtime
        if not target or runtime is None:
            self._clear_profile_switch_request()
            return
        self._runtime = runtime
        self._last_profile_name = target
        self._unlocked_profiles.add(target)
        self._start_private_threads()
        self._clear_profile_switch_request()

    def _stop_position_history_thread(self, *, wait_ms: int = 1600) -> None:
        thread = self._position_history_feed
        if thread is None:
            return
        try:
            thread.disconnect()
        except Exception:
            pass
        thread.stop()
        self._position_history_feed = None
        if wait_ms <= 0:
            if thread.isRunning():
                self._retire_thread(thread)
                return
            thread.deleteLater()
            return
        if thread.isRunning() and not thread.wait(wait_ms):
            self._retire_thread(thread)
            return
        thread.deleteLater()

    def _stop_order_history_thread(self, *, wait_ms: int = 1600) -> None:
        thread = self._order_history_feed
        if thread is None:
            return
        try:
            thread.disconnect()
        except Exception:
            pass
        thread.stop()
        self._order_history_feed = None
        if wait_ms <= 0:
            if thread.isRunning():
                self._retire_thread(thread)
                return
            thread.deleteLater()
            return
        if thread.isRunning() and not thread.wait(wait_ms):
            self._retire_thread(thread)
            return
        thread.deleteLater()

    def _stop_fill_history_thread(self, *, wait_ms: int = 1600) -> None:
        thread = self._fill_history_feed
        if thread is None:
            return
        try:
            thread.disconnect()
        except Exception:
            pass
        thread.stop()
        self._fill_history_feed = None
        if wait_ms <= 0:
            if thread.isRunning():
                self._retire_thread(thread)
                return
            thread.deleteLater()
            return
        if thread.isRunning() and not thread.wait(wait_ms):
            self._retire_thread(thread)
            return
        thread.deleteLater()

    def _start_order_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        shared_order_store = getattr(self, "_shared_order_store", None)
        if shared_order_store is not None:
            shared_order_store.request_refresh(
                runtime=self._runtime,
                profile_name=str(getattr(self, "_last_profile_name", "") or "").strip(),
            )
            return
        if self._order_history_feed is not None and self._order_history_feed.isRunning():
            return
        if self._order_history_feed is not None:
            self._stop_order_history_thread(wait_ms=0)
        generation = self._private_thread_generation
        self._order_history_feed = OrderHistoryFeedThread(self._runtime, limit=200)
        self._order_history_feed.data_ready.connect(
            lambda payload, generation=generation: self._apply_order_history_payload(payload)
            if generation == self._private_thread_generation
            else None
        )
        self._order_history_feed.status_changed.connect(
            lambda text, generation=generation: self._set_order_history_status(text)
            if generation == self._private_thread_generation
            else None
        )
        self._order_history_feed.finished.connect(self._clear_order_history_thread)
        if hasattr(self, "_order_history_summary_label"):
            self._order_history_summary_label.setText("正在同步历史委托...")
        self._order_history_feed.start()

    def _start_fill_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if self._fill_history_feed is not None and self._fill_history_feed.isRunning():
            return
        if self._fill_history_feed is not None:
            self._stop_fill_history_thread(wait_ms=0)
        generation = self._private_thread_generation
        self._fill_history_feed = FillHistoryFeedThread(self._runtime, limit=self._fill_history_fetch_limit)
        self._fill_history_feed.data_ready.connect(
            lambda payload, generation=generation: self._apply_fill_history_payload(payload)
            if generation == self._private_thread_generation
            else None
        )
        self._fill_history_feed.status_changed.connect(
            lambda text, generation=generation: self._set_fill_history_status(text)
            if generation == self._private_thread_generation
            else None
        )
        self._fill_history_feed.finished.connect(self._clear_fill_history_thread)
        if hasattr(self, "_fill_history_summary_label"):
            self._fill_history_summary_label.setText("正在同步历史成交...")
        self._fill_history_feed.start()

    def _start_position_history_refresh(self, *, force_restart: bool = False) -> None:
        if self._runtime is None:
            return
        if self._position_history_feed is not None and self._position_history_feed.isRunning():
            return
        if self._position_history_feed is not None:
            self._stop_position_history_thread(wait_ms=0)
        generation = self._private_thread_generation
        self._position_history_feed = PositionHistoryFeedThread(self._runtime, limit=self._position_history_fetch_limit)
        self._position_history_feed.data_ready.connect(
            lambda payload, generation=generation: self._apply_position_history_payload(payload)
            if generation == self._private_thread_generation
            else None
        )
        self._position_history_feed.status_changed.connect(
            lambda text, generation=generation: self._set_position_history_status(text)
            if generation == self._private_thread_generation
            else None
        )
        self._position_history_feed.finished.connect(self._clear_position_history_thread)
        self._position_history_summary_label.setText("正在同步历史仓位...")
        self._position_history_feed.start()

    @Slot()
    def _refresh_position_history(self) -> None:
        self._position_history_range_end_edit.setText(datetime.now().strftime("%Y%m%d"))
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
            self._account_status.setText(f"API {self._last_profile_name} 未解锁")
            self._order_status.setText("委托 WS 等待 API 解锁")
            self._summary_label.setText("当前 API 配置已加切换密码，请先解锁后再加载账户持仓。")
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
        self._flush_retired_threads()

    def begin_shutdown(self, finished: Callable[[], None] | None = None) -> None:
        if finished is not None:
            self._shutdown_finish_callbacks.append(finished)
        if self._shutdown_in_progress:
            return
        self._shutdown_in_progress = True
        self._shutdown_deadline_monotonic = time.monotonic() + 3.0
        self._shutdown_force_terminate_sent = False
        self._save_positions_view_prefs_now()
        self._stop_private_threads(wait_ms=0)
        self._stop_order_history_thread(wait_ms=0)
        self._stop_fill_history_thread(wait_ms=0)
        self._stop_position_history_thread(wait_ms=0)
        self._protection_manager.stop_all()
        if self._protection_dialog is not None:
            self._protection_dialog.close()
        self._legacy_option_tools.shutdown()
        if self._shutdown_poll_timer is None:
            self._shutdown_poll_timer = QTimer(self)
            self._shutdown_poll_timer.setInterval(100)
            self._shutdown_poll_timer.timeout.connect(self._poll_shutdown_completion)
        self._poll_shutdown_completion()

    def _poll_shutdown_completion(self) -> None:
        pending = list(self._retired_threads)
        running_threads: list[QThread] = []
        for thread in pending:
            try:
                if thread.isRunning():
                    running_threads.append(thread)
                    continue
            except Exception:
                pass
            try:
                if thread in self._retired_threads:
                    self._retired_threads.remove(thread)
            except Exception:
                pass
            try:
                thread.deleteLater()
            except Exception:
                pass
        if not running_threads:
            if self._shutdown_poll_timer is not None:
                self._shutdown_poll_timer.stop()
            self._finish_shutdown_callbacks()
            return
        if (not self._shutdown_force_terminate_sent) and time.monotonic() >= self._shutdown_deadline_monotonic:
            self._shutdown_force_terminate_sent = True
            self._shutdown_deadline_monotonic = time.monotonic() + 1.0
            for thread in running_threads:
                try:
                    thread.terminate()
                except Exception:
                    pass
        if self._shutdown_poll_timer is not None and not self._shutdown_poll_timer.isActive():
            self._shutdown_poll_timer.start()

    def _finish_shutdown_callbacks(self) -> None:
        callbacks = list(self._shutdown_finish_callbacks)
        self._shutdown_finish_callbacks.clear()
        self._shutdown_in_progress = False
        self._shutdown_force_terminate_sent = False
        self._shutdown_deadline_monotonic = 0.0
        for callback in callbacks:
            try:
                callback()
            except Exception:
                pass

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        # Refresh must not tear down live private feeds; rapid clicks otherwise
        # create overlapping WebSocket and account worker shutdowns.
        self._start_private_threads(force_restart=False)

    def refresh_view(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        self._status_badge.setText("正在刷新...")
        self._start_private_threads(force_restart=False)

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

    def _restart_live_feeds_for_manual_refresh(self) -> None:
        self._stop_private_threads(wait_ms=0)
        self._start_private_threads(force_restart=False, start_history=False)

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        self._position_history_table.setHorizontalHeaderItem(10, QTableWidgetItem("\u5df2\u5b9e\u73b0\u6536\u76ca"))
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
                    f"历史仓位: {len(self._position_history_items)} 条 | 最近同步: {self._position_history_last_sync_text} | 当前显示: {len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计: {stats_text}",
                )
            )
        )
        stats_text = _format_position_history_filter_stats(list(enumerate(filtered)), self._position_history_usdt_prices)
        self._position_history_summary_label.setText(
            "\n".join(
                (
                    f"历史仓位: {len(self._position_history_items)} 条 | 最近同步: {self._position_history_last_sync_text} | 当前显示: {len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计: {stats_text}",
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
                _format_position_history_pnl(
                    item.realized_pnl,
                    item,
                    with_sign=True,
                    usdt_prices=self._position_history_usdt_prices,
                ),
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
            tree.setStyleSheet(
                "QTreeView::item { height: 21px; }"
                "QTreeView::item:selected { background: #e8f1ff; color: #111827; }"
                "QTreeView::item:selected:active { background: #e8f1ff; color: #111827; }"
                "QTreeView::item:selected:!active { background: #e8f1ff; color: #111827; }"
            )

    def _build_header(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("HeaderPanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        layout.setSpacing(5)

        top = QHBoxLayout()
        top.setSpacing(6)
        self._status_badge = QLabel("正常")
        self._status_badge.setObjectName("Badge")
        self._account_status = QLabel("持仓读取中...")
        self._account_status.setObjectName("Subtle")
        self._order_status = QLabel("订单WS等待中...")
        self._order_status.setObjectName("Subtle")
        self._summary_label = QLabel("当前没有持仓")
        self._summary_label.setObjectName("Subtle")
        self._summary_label.setWordWrap(False)
        top.addWidget(self._status_badge)
        top.addWidget(self._account_status)
        top.addWidget(self._order_status)
        top.addWidget(self._summary_label, 1)
        top.addStretch(1)
        self._profile_label = QLabel("API配置")
        top.addWidget(self._profile_label)
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
            ("刷新", self.refresh_view, ""),
            ("账户信息", self._show_account_overview, ""),
            ("展开持仓详情", self._toggle_detail_panel, "_detail_toggle_button"),
            ("折叠历史区域", self._toggle_history_panel, "_history_toggle_button"),
            ("平仓选中", self.flatten_selected_position, ""),
            ("编辑备注", self.edit_selected_position_note, ""),
            ("从选中持仓接管", self._show_not_ready_action, ""),
            ("停止接管", self._show_not_ready_action, ""),
            ("设置期权保护", self._open_position_protection_dialog, ""),
            ("展期建议", self._open_option_roll_window, ""),
            ("列设置", self.open_positions_column_window, ""),
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
        self._keyword_edit.setPlaceholderText("搜索合约 / 币种 / 到期日 / 模式")
        self._keyword_edit.textChanged.connect(self._apply_filters)

        self._filter_hint = QLabel("选中期权后，可一键带入合约或到期前缀。")
        self._filter_hint.setObjectName("Subtle")

        self._apply_contract_button = QPushButton("带入合约")
        self._apply_contract_button.clicked.connect(self.apply_selected_option_to_position_search)
        self._apply_contract_button.setEnabled(False)
        self._apply_expiry_button = QPushButton("带入到期前缀")
        self._apply_expiry_button.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_search)
        self._apply_expiry_button.setEnabled(False)

        apply_button = QPushButton("应用筛选")
        apply_button.clicked.connect(self._apply_filters)
        clear_button = QPushButton("清空筛选")
        clear_button.clicked.connect(self._clear_filters)

        layout.addWidget(QLabel("类型"), 0, 0)
        layout.addWidget(self._type_combo, 0, 1)
        layout.addWidget(QLabel("搜索"), 0, 2)
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
        title = QLabel("当前持仓")
        title.setObjectName("SectionTitle")
        self._positions_hint = QLabel("当前显示 0 条持仓 | 点击任一行查看详情。")
        self._positions_hint.setObjectName("Subtle")
        title_row.addWidget(title)
        title_row.addStretch(1)
        self._expand_toggle_button = QPushButton("展开全部")
        self._expand_toggle_button.clicked.connect(self._toggle_all_positions)
        title_row.addWidget(self._expand_toggle_button)
        title_row.addWidget(self._positions_hint)
        panel_layout.addLayout(title_row)

        self._position_tree = QTreeWidget()
        self._position_tree.setColumnCount(1 + len(POSITION_COLUMNS))
        self._position_tree.setHeaderLabels(["合约 / 分组", *[item[1] for item in POSITION_COLUMNS]])
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
        detail_title = QLabel("持仓详情")
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
        self._tabs.addTab(self._build_current_orders_tab(), "当前委托")
        self._tabs.addTab(self._build_placeholder_tab("动态止盈接管", "动态止盈接管区块预留，后续按旧页面完整迁移。"), "动态止盈接管")
        self._tabs.addTab(self._build_placeholder_tab("历史委托", "历史委托区块预留，后续补齐筛选和同步逻辑。"), "历史委托")
        self._tabs.addTab(self._build_placeholder_tab("历史成交", "历史成交区块预留，后续补齐筛选和同步逻辑。"), "历史成交")
        self._tabs.addTab(self._build_position_history_tab(), "历史仓位")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._orders_summary_label = QLabel("当前委托尚未读取。")
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        layout.addWidget(self._orders_summary_label)

        self._orders_table = QTableWidget(0, 11)
        self._orders_table.setHorizontalHeaderLabels(
            ("时间", "合约", "类型", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "交易模式", "clOrdId")
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

        detail_title = QLabel("委托详情")
        detail_title.setObjectName("SectionTitle")
        self._orders_detail = QTextEdit()
        self._orders_detail.setReadOnly(True)
        self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
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
        self._position_history_summary_label = QLabel("历史仓位尚未读取。")
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        head.addWidget(self._position_history_summary_label, 1)
        refresh_button = QPushButton("同步历史仓位")
        refresh_button.clicked.connect(self._refresh_position_history)
        head.addWidget(refresh_button)
        layout.addLayout(head)

        self._position_history_table = QTableWidget(0, 12)
        self._position_history_table.setHorizontalHeaderLabels(
            ("时间", "类型", "合约", "保证金模式", "持仓模式", "交易方向", "开仓均价", "平仓均价", "平仓数量", "手续费", "盈亏", "备注")
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

        detail_title = QLabel("历史仓位详情")
        detail_title.setObjectName("SectionTitle")
        self._position_history_detail = QTextEdit()
        self._position_history_detail.setReadOnly(True)
        self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
        layout.addWidget(detail_title)
        layout.addWidget(self._position_history_detail, 1)
        return tab

    def _refresh_profiles(self) -> None:
        snapshots, _selected = load_profile_snapshots()
        self._profile_snapshots = snapshots

    def _populate_profile_combo(self) -> None:
        self._profile_switch_guard = True
        with QSignalBlocker(self._profile_combo):
            self._profile_combo.clear()
            names = profile_names()
            if names:
                self._profile_combo.addItems(names)
                target = self._last_profile_name or names[0]
                index = self._profile_combo.findText(target)
                self._profile_combo.setCurrentIndex(index if index >= 0 else 0)
            else:
                self._profile_combo.addItem("未配置")
        self._profile_switch_guard = False

    def _ensure_runtime_ready(self, *, force_unlock: bool) -> bool:
        profile_name = self._current_profile_name()
        if not profile_name:
            QMessageBox.warning(self, "无法刷新", "当前未配置可用的 API Profile。")
            return False
        if force_unlock and not ensure_profile_unlocked(self, profile_name, self._profile_snapshots, self._unlocked_profiles):
            return False
        runtime = load_runtime(profile_name)
        if runtime is None:
            QMessageBox.warning(self, "无法刷新", f"API 配置 {profile_name} 不可用，请检查凭证。")
            return False
        self._runtime = runtime
        self._last_profile_name = profile_name
        return True

    def _current_profile_name(self) -> str:
        text = self._profile_combo.currentText().strip()
        return "" if text == "未配置" else text

    def _stop_private_threads(self, *, wait_ms: int = 1600) -> None:
        self._private_thread_generation += 1
        realtime_store = getattr(self, "_realtime_store", None)
        if realtime_store is not None:
            realtime_store.stop()
        for thread in (self._account_feed, self._order_feed):
            if thread is None:
                continue
            try:
                thread.disconnect()
            except Exception:
                pass
            thread.stop()
            if wait_ms <= 0:
                if thread.isRunning():
                    self._retire_thread(thread)
                    continue
                thread.deleteLater()
                continue
            if thread.isRunning() and not thread.wait(wait_ms):
                self._retire_thread(thread)
                continue
            thread.deleteLater()
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
    def _parse_positive_decimal(raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(str(raw).strip())
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    @staticmethod
    def _position_manual_flatten_mode_label(flatten_mode: str) -> str:
        return "挂买一/卖一平仓" if AccountPositionsHomeWidget._normalize_position_manual_flatten_mode(flatten_mode) == "best_quote" else "市价平仓"

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

    @staticmethod
    def _position_asset_symbol(position: OkxPosition) -> str:
        parts = str(position.inst_id or "").strip().upper().split("-")
        return parts[0] if parts and parts[0] else "币"

    @staticmethod
    def _position_contract_multiplier(instrument: Instrument | None) -> Decimal:
        if instrument is not None and instrument.ct_mult is not None and instrument.ct_mult > 0:
            return instrument.ct_mult
        return Decimal("1")

    @staticmethod
    def _selected_position_contract_size_to_display_amount(
        position: OkxPosition,
        instrument: Instrument | None,
        contract_size: Decimal,
    ) -> tuple[Decimal, str]:
        size = abs(contract_size)
        if position.inst_type == "SPOT":
            return size, AccountPositionsHomeWidget._position_asset_symbol(position)
        contract_value, contract_currency = _position_contract_value_snapshot(position, instrument)
        if contract_value is not None and contract_value > 0 and contract_currency:
            multiplier = AccountPositionsHomeWidget._position_contract_multiplier(instrument)
            quote_currency = contract_currency.upper()
            if quote_currency in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
                reference_price = position.mark_price or position.last_price or position.avg_price
                if reference_price is not None and reference_price > 0:
                    return (size * contract_value * multiplier / reference_price), AccountPositionsHomeWidget._position_asset_symbol(position)
            return size * contract_value * multiplier, quote_currency
        return size, "张"

    def _selected_position_close_display_amount(
        self,
        position: OkxPosition,
        instrument: Instrument | None,
    ) -> tuple[Decimal, str]:
        contract_size = position.avail_position
        if contract_size is None or contract_size == 0:
            contract_size = position.position
        return AccountPositionsHomeWidget._selected_position_contract_size_to_display_amount(
            position,
            instrument,
            abs(contract_size),
        )

    @staticmethod
    def _format_amount_with_unit(amount: Decimal, unit: str) -> str:
        return f"{format_decimal(amount)} {unit}".strip()

    @staticmethod
    def _selected_position_order_size_text(
        position: OkxPosition,
        instrument: Instrument | None,
        order_size: Decimal,
    ) -> str:
        if position.inst_type == "SPOT":
            return AccountPositionsHomeWidget._format_amount_with_unit(
                abs(order_size),
                AccountPositionsHomeWidget._position_asset_symbol(position),
            )
        return f"{format_decimal(abs(order_size))} 张"

    @staticmethod
    def _convert_selected_position_close_coin_to_order_size(
        position: OkxPosition,
        instrument: Instrument,
        close_amount: Decimal,
    ) -> Decimal:
        if close_amount <= 0:
            raise ValueError("平仓币数必须大于 0。")
        if position.inst_type == "SPOT":
            return snap_to_increment(close_amount, instrument.lot_size, "down")

        contract_value, contract_currency = _position_contract_value_snapshot(position, instrument)
        if contract_value is None or contract_value <= 0 or not contract_currency:
            raise ValueError("当前合约缺少币数换算所需的合约面值信息，暂时无法按币数平仓。")

        multiplier = AccountPositionsHomeWidget._position_contract_multiplier(instrument)
        denominator = contract_value * multiplier
        if denominator <= 0:
            raise ValueError("当前合约缺少有效合约面值，暂时无法按币数平仓。")

        quote_currency = contract_currency.upper()
        if quote_currency in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            if reference_price is None or reference_price <= 0:
                raise ValueError("当前合约缺少有效参考价格，无法把币数换算成下单张数。")
            raw_size = close_amount * reference_price / denominator
        else:
            raw_size = close_amount / denominator

        return snap_to_increment(raw_size, instrument.lot_size, "down")

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
                raise ValueError(f"{instrument.inst_id} 当前缺少买一价，无法按买一挂平空单。")
            return snap_to_increment(raw_price, instrument.tick_size, "down")
        raw_price = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw_price is None or raw_price <= 0:
            raise ValueError(f"{instrument.inst_id} 当前缺少卖一价，无法按卖一挂平多单。")
        return snap_to_increment(raw_price, instrument.tick_size, "up")

    @staticmethod
    def _derive_total_equity_btc(
        total_equity: Decimal | None,
        details: tuple[object, ...] | None,
    ) -> Decimal | None:
        if total_equity is None or not isinstance(details, tuple):
            return None
        for asset in details:
            if str(getattr(asset, "ccy", "") or "").strip().upper() != "BTC":
                continue
            equity = getattr(asset, "equity", None)
            equity_usd = getattr(asset, "equity_usd", None)
            if not isinstance(equity, Decimal) or not isinstance(equity_usd, Decimal) or equity == 0:
                return None
            btc_price = abs(equity_usd) / abs(equity)
            if btc_price <= 0:
                return None
            return total_equity / btc_price
        return None

    def _prepare_selected_position_manual_flatten(
        self,
        position: OkxPosition,
        flatten_mode: str,
        *,
        close_size: Decimal | None = None,
    ) -> tuple[Credentials, StrategyConfig, Instrument, Decimal, str, str | None, str, str]:
        runtime = self._runtime
        if runtime is None:
            raise ValueError("当前没有可用的 API 运行时，无法执行平仓。")
        credentials = runtime.credentials
        config = self._build_selected_position_manual_flatten_config(position)
        instrument = self._selected_position_flatten_instrument(position)
        max_close = snap_to_increment(self._selected_position_close_size(position), instrument.lot_size, "down")
        if max_close < instrument.min_size:
            raise ValueError("当前选中持仓的可平数量不足最小下单量，无法直接平仓。")
        if close_size is not None:
            max_close_amount, max_close_unit = self._selected_position_close_display_amount(position, instrument)
            requested = self._convert_selected_position_close_coin_to_order_size(position, instrument, close_size)
            if requested <= 0:
                raise ValueError("输入币数按合约最小变动单位换算后为 0，请增大数量。")
            if requested > max_close:
                raise ValueError(
                    f"平仓币数不能超过当前可平数量 {self._format_amount_with_unit(max_close_amount, max_close_unit)}。"
                )
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
        if normalized_mode == "market" and instrument.inst_type == "OPTION":
            result = self._shared_client.place_aggressive_limit_order(
                credentials,
                config,
                instrument,
                side=close_side,
                size=closeable_size,
                pos_side=None,
            )
            return result, None, normalized_mode
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

    def _selected_position_manual_flatten_after(self, delay_ms: int, callback) -> None:
        if delay_ms <= 0:
            self._selected_position_manual_flatten_callback.emit(callback)
            return
        QTimer.singleShot(delay_ms, callback)

    @Slot(object)
    def _run_selected_position_manual_flatten_callback(self, callback: object) -> None:
        if callable(callback):
            callback()

    @Slot(object)
    def _run_ui_callback(self, callback: object) -> None:
        if not callable(callback):
            return
        try:
            callback()
        except Exception as exc:  # noqa: BLE001
            self._current_order_canceling = False
            QMessageBox.critical(self, "操作失败", str(exc))

    def _log_selected_position_manual_flatten(self, message: str) -> None:
        profile_name = (self._last_profile_name or self._current_profile_name() or "-").strip() or "-"
        environment = (self._note_environment() or "-").strip() or "-"
        line = f"[manual_flatten] [{profile_name}/{environment}] {message}"
        try:
            append_log_line(line)
        except Exception:
            pass
        _debug_log(line)

    def _finish_selected_position_manual_flatten_error(self, exc: Exception) -> None:
        self._selected_position_manual_flatten_running = False
        self._status_badge.setText("失败")
        self._summary_label.setText(f"平仓失败：{exc}")
        self._log_selected_position_manual_flatten(f"失败 | {exc}")
        QMessageBox.critical(self, "平仓失败", str(exc))

    def _selected_position_manual_flatten_result_failed(self, result: OkxOrderResult) -> bool:
        return str(result.s_code or "").strip() not in {"", "0"}

    def _selected_position_manual_flatten_result_error_message(
        self,
        *,
        position: OkxPosition,
        result: OkxOrderResult,
        close_side_label: str,
        submit_size_text: str,
    ) -> str:
        profile_name = (self._last_profile_name or self._current_profile_name() or "-").strip() or "-"
        s_code = str(result.s_code or "").strip() or "-"
        s_msg = str(result.s_msg or "").strip() or "accepted"
        order_id = (result.ord_id or "-").strip() or "-"
        client_order_id = (result.cl_ord_id or "-").strip() or "-"
        reason = self._selected_position_manual_flatten_result_reason_text(s_code=s_code, s_msg=s_msg)
        return (
            "当前下单被交易所拒绝。\n\n"
            f"API配置：{profile_name}\n"
            f"合约：{position.inst_id}\n"
            f"平仓数量：{submit_size_text}\n"
            f"下单方向：{close_side_label}\n"
            f"订单ID：{order_id}\n"
            f"客户端单号：{client_order_id}\n"
            f"返回：sCode={s_code} | sMsg={s_msg}\n"
            f"原因解释：{reason}"
        )

    def _selected_position_manual_flatten_result_reason_text(self, *, s_code: str, s_msg: str) -> str:
        code = (s_code or "").strip()
        lowered = (s_msg or "").strip().lower()
        if code == "50120" or "doesn't have permission to use this function" in lowered:
            return (
                "这个 API Key 没有交易权限，当前更像只有查询权限。"
                "常见原因是：创建 API 时没有勾选交易权限，或该 Key 被设成只读。"
                "请到 OKX 的 API 管理里检查是否开启 Trade/交易权限。"
            )
        if "read-only" in lowered or "read only" in lowered:
            return "这个 API Key 是只读权限，不能下单。请改成带交易权限的 API Key。"
        if code == "401" or "http 401" in lowered:
            return "交易接口返回 401，当前凭证未通过授权校验。请检查 API 权限、密钥是否有效，以及当前环境是否匹配。"
        return "交易接口已拒绝本次下单。请检查 API 权限、账户模式和下单参数是否与当前账户设置一致。"

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
        order_size_text: str | None = None,
    ) -> None:
        self._selected_position_manual_flatten_running = False
        self._status_badge.setText("正常")
        mode_label = self._position_manual_flatten_mode_label(normalized_flatten_mode)
        order_id = (result.ord_id or "-").strip() or "-"
        client_order_id = (result.cl_ord_id or "-").strip() or "-"
        self._log_selected_position_manual_flatten(
            f"提交成功 | instId={position.inst_id} | mode={normalized_flatten_mode} | side={close_side_label} | "
            f"submit={submit_size_text} | orderSize={order_size_text or '-'} | ordId={order_id} | clOrdId={client_order_id} | "
            f"sCode={result.s_code or '-'} | sMsg={result.s_msg or '-'}"
        )
        message = (
            "已提交选中持仓平仓。\n\n"
            f"合约：{position.inst_id}\n"
            f"方向：{direction_label}\n"
            f"平仓币数：{submit_size_text}\n"
            f"下单方向：{close_side_label}\n"
            f"方式：{mode_label}\n"
            f"订单ID：{order_id}\n"
            f"客户端单号：{client_order_id}"
        )
        if order_size_text:
            message = f"{message}\n实际下单量：{order_size_text}"
        if normalized_flatten_mode == "best_quote" and price is not None:
            message = f"{message}\n挂单价：{format_decimal(price)}"
        QMessageBox.information(self, "平仓已提交", message)
        self._schedule_selected_position_manual_flatten_follow_up_refresh(normalized_flatten_mode)

    def flatten_selected_position(self) -> None:
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        if self._selected_position_manual_flatten_running:
            QMessageBox.information(self, "平仓", "当前已有一笔选中持仓平仓在提交中，请稍候。")
            return
        position = self._selected_position()
        if position is None:
            QMessageBox.information(self, "平仓", "请先在当前持仓里选中一条具体持仓。")
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
            QMessageBox.critical(self, "平仓失败", str(exc))
            return

        direction_label = "多头" if preview_direction == "long" else "空头"
        close_side_label = "SELL 卖出平仓" if preview_close_side == "sell" else "BUY 买入平仓"
        instrument = self._selected_position_flatten_instrument(position)
        hold_amount_text = self._format_amount_with_unit(
            *self._selected_position_contract_size_to_display_amount(
                position,
                instrument,
                abs(position.position),
            )
        )
        closeable_amount, closeable_unit = self._selected_position_close_display_amount(position, instrument)
        closeable_amount_text = self._format_amount_with_unit(closeable_amount, closeable_unit)
        submit_amount_text = self._format_amount_with_unit(
            *self._selected_position_contract_size_to_display_amount(
                position,
                instrument,
                preview_close_size,
            )
        )
        order_size_text = self._selected_position_order_size_text(position, instrument, preview_close_size)
        size_dialog = QuantityInputDialog(
            title="平仓币数",
            prompt=f"输入本次平仓币数（默认可平全部，单位：{closeable_unit}）",
            initial_value=format_decimal(closeable_amount),
            unit_text=closeable_unit,
            parent=self,
        )
        if size_dialog.exec() != QDialog.DialogCode.Accepted:
            return
        requested_close_size = size_dialog.result_text or ""
        try:
            requested_close_size = self._parse_positive_decimal(requested_close_size, "平仓币数")
            (
                _credentials,
                _config,
                _instrument,
                preview_close_size,
                preview_close_side,
                _pos_side,
                preview_direction,
                _normalized_mode,
            ) = self._prepare_selected_position_manual_flatten(
                position,
                "market",
                close_size=requested_close_size,
            )
            submit_amount_text = self._format_amount_with_unit(
                *self._selected_position_contract_size_to_display_amount(
                    position,
                    instrument,
                    preview_close_size,
                )
            )
            order_size_text = self._selected_position_order_size_text(position, instrument, preview_close_size)
        except Exception as exc:
            QMessageBox.critical(self, "平仓失败", str(exc))
            return

        dialog = QMessageBox(self)
        dialog.setWindowTitle("平仓选中")
        dialog.setIcon(QMessageBox.Icon.Question)
        dialog.setText(
            "\n".join(
                [
                    "请选择这次对选中持仓的平仓方式。",
                    "",
                    f"合约：{position.inst_id}",
                    f"方向：{direction_label}",
                    f"当前持仓：{hold_amount_text}",
                    f"当前可平：{closeable_amount_text}",
                    f"本次将报单平仓币数：{submit_amount_text}",
                    f"换算下单量：{order_size_text}",
                    f"实际报单方向：{close_side_label}",
                    "",
                    "说明：",
                    "1. 市价平仓会立刻按市场可成交价格报单。",
                    "2. 挂买一/卖一平仓会先挂单，未成交前持仓不会消失。",
                    "3. 平多按卖一挂单，平空按买一挂单。",
                ]
            )
        )
        market_button = dialog.addButton("市价平仓", QMessageBox.ButtonRole.AcceptRole)
        best_quote_button = dialog.addButton("挂买一/卖一", QMessageBox.ButtonRole.ActionRole)
        dialog.addButton(QMessageBox.StandardButton.Cancel)
        dialog.exec()
        clicked = dialog.clickedButton()
        if clicked not in {market_button, best_quote_button}:
            return
        flatten_mode = "market" if clicked is market_button else "best_quote"
        self._log_selected_position_manual_flatten(
            f"用户确认平仓 | instId={position.inst_id} | direction={direction_label} | closeSide={close_side_label} | "
            f"mode={flatten_mode} | submit={submit_amount_text} | orderSize={order_size_text}"
        )

        self._selected_position_manual_flatten_running = True
        self._status_badge.setText("平仓提交中...")

        def _worker() -> None:
            try:
                self._log_selected_position_manual_flatten(
                    f"线程开始 | instId={position.inst_id} | mode={flatten_mode} | closeSizeInput={requested_close_size} | "
                    f"submit={submit_amount_text} | orderSize={order_size_text}"
                )
                result, price, normalized_mode = self._submit_selected_position_manual_flatten(
                    position,
                    flatten_mode,
                    close_size=requested_close_size,
                )
                self._log_selected_position_manual_flatten(
                    f"交易所返回 | instId={position.inst_id} | mode={normalized_mode} | ordId={result.ord_id or '-'} | "
                    f"clOrdId={result.cl_ord_id or '-'} | sCode={result.s_code or '-'} | sMsg={result.s_msg or '-'} | "
                    f"price={format_decimal(price) if price is not None else '-'}"
                )
            except Exception as exc:
                import traceback

                self._log_selected_position_manual_flatten(
                    f"线程异常 | instId={position.inst_id} | mode={flatten_mode} | error={exc}\n{traceback.format_exc()}"
                )
                self._selected_position_manual_flatten_after(
                    0,
                    lambda exc=exc: self._finish_selected_position_manual_flatten_error(exc),
                )
                return
            if self._selected_position_manual_flatten_result_failed(result):
                error_message = self._selected_position_manual_flatten_result_error_message(
                    position=position,
                    result=result,
                    close_side_label=close_side_label,
                    submit_size_text=submit_amount_text,
                )
                self._log_selected_position_manual_flatten(
                    f"交易所拒单 | instId={position.inst_id} | mode={normalized_mode} | detail={error_message}"
                )
                self._selected_position_manual_flatten_after(
                    0,
                    lambda message=error_message: self._finish_selected_position_manual_flatten_error(RuntimeError(message)),
                )
                return
            self._selected_position_manual_flatten_after(
                0,
                lambda result=result, price=price, normalized_mode=normalized_mode: self._finish_selected_position_manual_flatten_success(
                    position=position,
                    result=result,
                    price=price,
                    normalized_flatten_mode=normalized_mode,
                    direction_label=direction_label,
                    close_side_label=close_side_label,
                    submit_size_text=submit_amount_text,
                    order_size_text=order_size_text,
                ),
            )

        try:
            threading.Thread(target=_worker, name="qt-selected-position-flatten", daemon=True).start()
        except RuntimeError as exc:
            self._selected_position_manual_flatten_running = False
            self._status_badge.setText("正常")
            QMessageBox.critical(self, "平仓失败", f"系统线程资源不足，无法提交平仓：{exc}")

    def _position_history_note_text(self, item: OkxPositionHistoryItem) -> str:
        key = _position_history_note_key(self._last_profile_name, self._note_environment(), item)
        record = self._history_notes.get(key)
        return _normalize_position_note_text(record.get("note", "")) if isinstance(record, dict) else ""

    def _render_position_history_table(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        self._position_history_table.setHorizontalHeaderItem(10, QTableWidgetItem("\u5df2\u5b9e\u73b0\u6536\u76ca"))
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
                _format_position_history_pnl(
                    item.realized_pnl,
                    item,
                    with_sign=True,
                    usdt_prices=self._position_history_usdt_prices,
                ),
                _position_history_note_summary_text(item, self._position_history_note_text(item)),
            )
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                if column not in {2, 11}:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                self._position_history_table.setItem(row, column, cell)
        self._position_history_summary_label.setText(f"历史仓位：{len(self._position_history_items)} 条")
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
            self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
        self._refresh_position_history_detail()

    def _open_position_protection_dialog(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "设置期权保护", "请先在当前持仓里选中一条期权仓位。")
            return
        if self._protection_dialog is None:
            self._protection_dialog = PositionProtectionDialog(
                manager=self._protection_manager,
                client=self._shared_client,
                runtime_provider=lambda: self._runtime,
                selected_option_provider=self._selected_option_for_shortcut,
                notifier_provider=lambda: _build_optional_protection_notifier(
                    self._last_profile_name or self._current_profile_name() or None
                ),
                parent=None,
            )
            self._protection_dialog.setWindowFlag(Qt.WindowType.Window, True)
            self._protection_dialog.destroyed.connect(lambda *_args: setattr(self, "_protection_dialog", None))
        self._protection_dialog._safe_refresh_from_selection(force=True, context="open_dialog")
        self._protection_dialog.show()
        self._protection_dialog.raise_()
        self._protection_dialog.activateWindow()

    def _open_option_roll_window(self) -> None:
        position = self._selected_option_for_shortcut()
        if position is None:
            QMessageBox.information(self, "\u5c55\u671f\u5efa\u8bae", "\u8bf7\u5148\u5728\u5f53\u524d\u6301\u4ed3\u4e2d\u9009\u4e2d\u4e00\u6761\u671f\u6743\u6301\u4ed3\u3002")
            return
        if not is_short_option_position(position):
            QMessageBox.information(self, "\u5c55\u671f\u5efa\u8bae", "\u5c55\u671f\u5efa\u8bae\u76ee\u524d\u4ec5\u652f\u6301\u671f\u6743\u5356\u51fa\u65b9\u5411\u6301\u4ed3\u3002")
            return
        instrument = self._position_instruments.get(position.inst_id)
        if instrument is None:
            try:
                instrument = self._shared_client.get_instrument(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "\u5c55\u671f\u5efa\u8bae", f"\u8bfb\u53d6\u5408\u7ea6\u4fe1\u606f\u5931\u8d25\uff1a{exc}")
                return
        ticker = self._position_tickers.get(position.inst_id)
        if ticker is None:
            try:
                ticker = self._shared_client.get_ticker(position.inst_id)
            except Exception as exc:
                QMessageBox.critical(self, "\u5c55\u671f\u5efa\u8bae", f"\u8bfb\u53d6\u884c\u60c5\u5931\u8d25\uff1a{exc}")
                return
        dialog = getattr(self, "_option_roll_dialog", None)
        if dialog is not None:
            dialog.close()
        self._option_roll_dialog = OptionRollQtDialog(
            client=self._shared_client,
            position=position,
            instrument=instrument,
            ticker=ticker,
            api_name=self._last_profile_name or "",
            send_to_strategy=self._send_option_roll_to_strategy,
            parent=self,
        )
        self._option_roll_dialog.destroyed.connect(lambda *_args: setattr(self, "_option_roll_dialog", None))
        self._option_roll_dialog.show()
        self._option_roll_dialog.raise_()
        self._option_roll_dialog.activateWindow()

    def _send_option_roll_to_strategy(self, payload: object) -> None:
        from roll_terminal_qt.option_strategy_window import OptionStrategyQtWindow

        window = getattr(self, "_option_strategy_window", None)
        if window is None:
            window = OptionStrategyQtWindow()
            window.destroyed.connect(lambda *_args: setattr(self, "_option_strategy_window", None))
            self._option_strategy_window = window
        window.load_roll_transfer_payload(payload)
        window.show()
        window.raise_()
        window.activateWindow()
    def edit_selected_position_note(self) -> None:
        position = self._selected_position()
        if position is None:
            QMessageBox.information(self, "备注", "请先在当前持仓里选中一条具体持仓。")
            return
        dialog = NoteEditorDialog(
            title="编辑持仓备注",
            prompt=f"为 {position.inst_id} 填写备注。留空后保存会清空当前持仓备注。",
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
        with measure_ui_step("positions_apply", rows=len(self._raw_positions)):
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
                label=f"{asset_label} 风险单元",
                values=_group_row_values_with_break_even("组合", asset_metrics),
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
                    values=_group_row_values_with_break_even("分组", bucket_metrics),
                    kind="group",
                    payload_item=bucket_positions,
                    payload_metrics=bucket_metrics,
                )
                bucket_item.setFont(0, bold_font)
                asset_item.addChild(bucket_item)
                bucket_item.setExpanded(bucket_id in self._expanded_row_keys)
                for position in bucket_positions:
                    bucket_item.addChild(self._build_position_item(position))

        self._positions_hint.setText(f"当前显示 {len(self._visible_positions)} 条持仓 | 点击任一行查看详情。")
        self._update_summary_text()
        self._restore_tree_selection(selected_key)
        self._update_filter_shortcuts()
        self._sync_order_watchlist()
        self._visible_orders = list(self._orders)
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
            _format_optional_decimal_fixed(
                _position_break_even_price(
                    position,
                    self._upl_usdt_prices,
                    fee_rate=_break_even_taker_fee_rate(
                        self._profile_snapshots.get(self._last_profile_name, {}),
                        inst_type=position.inst_type,
                    ),
                ),
                places=2,
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
        for column_id, color in _position_display_foreground_colors(
            time_value_text=values[2],
            intrinsic_value_text=values[4],
            bid_price_text=values[6],
            ask_price_text=values[8],
            mark_price_text=values[10],
            avg_price_text=values[12],
            break_even_text=values[15],
            market_value_text=values[22],
            unrealized_pnl=position.unrealized_pnl,
        ).items():
            item.setForeground(_POSITION_TREE_COLUMN_INDEX[column_id], color)
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
        self._expand_toggle_button.setText("折叠全部" if self._all_group_rows_expanded() else "展开全部")

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
            f"API配置：{self._last_profile_name or '-'}",
            self._account_status.text(),
        ]
        if total_count:
            text = f"当前仓位（{total_count}）"
            if visible_count != total_count:
                text += f"，当前显示 {visible_count}"
            parts.append(text)
        else:
            parts.append("当前没有持仓")
        keyword = self._keyword_edit.text().strip().upper()
        type_label = self._type_combo.currentText().strip()
        if keyword or type_label != "全部类型":
            parts.append(f"筛选：{type_label if type_label != '全部类型' else ''} {'| ' + keyword if keyword else ''}".strip())
        self._summary_label.setText(" | ".join(part for part in parts if part))

    def _update_filter_shortcuts(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._apply_contract_button.setEnabled(enabled)
        self._apply_expiry_button.setEnabled(enabled)
        if not enabled:
            self._filter_hint.setText("选中期权后，可一键带入合约或到期前缀。")
            return
        self._filter_hint.setText(f"已选期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}")

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            QMessageBox.information(self, "快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self._keyword_edit.setText(contract)

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_option_for_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            QMessageBox.information(self, "快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self._keyword_edit.setText(expiry_prefix)

    def _clear_filters(self) -> None:
        self._type_combo.setCurrentIndex(0)
        self._keyword_edit.clear()

    def _sync_order_watchlist(self) -> None:
        if self._order_feed is None:
            return
        # Current orders must include pending conditional/algo orders even when
        # they are not on the same contract set as the currently visible
        # positions (for example hedge or trigger orders on BTC-USDT-SWAP while
        # the position tree is filtered to option contracts).
        self._order_feed.set_watched_inst_ids(set())

    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        selected_ord_id = ""
        current_row = self._orders_table.currentRow()
        if 0 <= current_row < len(self._visible_orders):
            selected_ord_id = self._visible_orders[current_row].ord_id
        self._orders_summary_label.setText(f"当前委托：{len(self._visible_orders)} 条")
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
            self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
        self._refresh_current_order_detail()

    def _refresh_current_order_detail(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(self._visible_orders):
            self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
            return
        order = self._visible_orders[row]
        lines = [
            f"合约：{order.inst_id or '-'}",
            f"类型：{order.inst_type or '-'}",
            f"状态：{_format_trade_order_state(order.state)}",
            f"方向：{_format_history_side(order.side or '-', order.pos_side or '')}",
            f"交易模式：{order.td_mode or '-'}",
            f"委托类型：{order.ord_type or '-'}",
            f"委托价：{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"委托量：{_format_trade_order_size(order.size)}",
            f"已成交：{_format_trade_order_size(order.filled_size)}",
            f"成交均价：{_format_trade_order_price(order.avg_price, order.inst_id, order.inst_type or '')}",
            f"更新时间：{_format_okx_ms_timestamp(order.update_time)}",
            f"创建时间：{_format_okx_ms_timestamp(order.created_time)}",
            f"reduceOnly：{'是' if order.reduce_only is True else '否' if order.reduce_only is False else '-'}",
            f"ordId：{order.ord_id or '-'}",
            f"clOrdId：{order.client_order_id or '-'}",
            "",
            "原始 WS 数据",
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText("\n".join(lines))

    def _refresh_position_history_detail(self) -> None:
        if not hasattr(self, "_position_history_table"):
            return
        row = self._position_history_table.currentRow()
        if row < 0 or row >= len(self._position_history_items):
            self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
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
            self._detail_text.setPlainText("点击任一行查看持仓详情。")
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
        self._detail_text.setPlainText("点击任一行查看持仓详情。")

    def _show_account_overview(self) -> None:
        dialog = AccountOverviewDialog(summary_text=self._build_account_overview_summary_text(), parent=self)
        dialog.exec()

    def _build_account_overview_summary_text(self) -> str:
        raw_positions = list(self._raw_positions)
        visible_positions = list(self._visible_positions)
        format_btc_amount = lambda value: _format_optional_decimal_fixed(value, places=8) if isinstance(value, Decimal) else "-"
        position_metrics = _aggregate_position_metrics(raw_positions, self._upl_usdt_prices, self._position_instruments)
        visible_metrics = _aggregate_position_metrics(visible_positions, self._upl_usdt_prices, self._position_instruments)
        type_counts = Counter(str(item.inst_type or "-").upper() or "-" for item in raw_positions)
        visible_type_counts = Counter(str(item.inst_type or "-").upper() or "-" for item in visible_positions)
        option_long = sum(1 for item in raw_positions if str(item.inst_type or "").upper() == "OPTION" and derive_position_direction(item) == "long")
        option_short = sum(1 for item in raw_positions if str(item.inst_type or "").upper() == "OPTION" and derive_position_direction(item) == "short")
        keyword = self._keyword_edit.text().strip()
        type_filter = self._type_combo.currentText().strip() or "全部类型"
        runtime = self._runtime
        environment = getattr(runtime, "environment", "") if runtime is not None else ""
        environment_label = "实盘 live" if str(environment).lower() == "live" else ("模拟 demo" if str(environment).lower() == "demo" else "-")

        lines = [
            "账户基础",
            f"当前 API：{self._last_profile_name or '-'}",
            f"环境：{environment_label}",
            f"持仓总数：{len(raw_positions)}",
            f"当前显示：{len(visible_positions)}",
            f"当前委托：{len(self._visible_orders)}",
            f"当前筛选：类型={type_filter} | 关键字={keyword or '-'}",
            "",
            "持仓结构",
            "全部持仓类型分布："
            + (" | ".join(f"{inst_type} {count}" for inst_type, count in sorted(type_counts.items())) if type_counts else "-"),
            "当前显示类型分布："
            + (" | ".join(f"{inst_type} {count}" for inst_type, count in sorted(visible_type_counts.items())) if visible_type_counts else "-"),
            f"期权方向：多头 {option_long} | 空头 {option_short}",
            "",
            "持仓汇总（全部）",
            f"浮盈亏：{_format_optional_decimal_fixed(position_metrics.get('upl') if isinstance(position_metrics.get('upl'), Decimal) else None, places=5, with_sign=True)}",
            f"浮盈≈USDT：{_format_optional_usdt(position_metrics.get('upl_usdt') if isinstance(position_metrics.get('upl_usdt'), Decimal) else None)}",
            f"已实现盈亏：{_format_optional_decimal_fixed(position_metrics.get('realized') if isinstance(position_metrics.get('realized'), Decimal) else None, places=5, with_sign=True)}",
            f"已实现≈USDT：{_format_optional_usdt(position_metrics.get('realized_usdt') if isinstance(position_metrics.get('realized_usdt'), Decimal) else None)}",
            f"开仓价值≈USDT：{_format_optional_approx_usdt(position_metrics.get('open_value_usdt') if isinstance(position_metrics.get('open_value_usdt'), Decimal) else None)}",
            f"市值≈USDT：{_format_optional_approx_usdt(position_metrics.get('market_value_usdt') if isinstance(position_metrics.get('market_value_usdt'), Decimal) else None)}",
            f"Delta(PA)：{_format_optional_decimal_fixed(position_metrics.get('delta') if isinstance(position_metrics.get('delta'), Decimal) else None, places=5)}",
            f"Gamma(PA)：{_format_optional_decimal_fixed(position_metrics.get('gamma') if isinstance(position_metrics.get('gamma'), Decimal) else None, places=5)}",
            f"Vega(PA)：{_format_optional_decimal_fixed(position_metrics.get('vega') if isinstance(position_metrics.get('vega'), Decimal) else None, places=5)}",
            f"Theta(PA)：{_format_optional_decimal_fixed(position_metrics.get('theta') if isinstance(position_metrics.get('theta'), Decimal) else None, places=5)}",
            f"Theta≈USDT：{_format_optional_usdt_precise(position_metrics.get('theta_usdt') if isinstance(position_metrics.get('theta_usdt'), Decimal) else None, places=2)}",
            f"初始保证金(IMR)：{_format_optional_integer(position_metrics.get('imr') if isinstance(position_metrics.get('imr'), Decimal) else None)}",
            f"维持保证金(MMR)：{_format_optional_integer(position_metrics.get('mmr') if isinstance(position_metrics.get('mmr'), Decimal) else None)}",
            "",
            "持仓汇总（当前显示）",
            f"浮盈亏：{_format_optional_decimal_fixed(visible_metrics.get('upl') if isinstance(visible_metrics.get('upl'), Decimal) else None, places=5, with_sign=True)}",
            f"浮盈≈USDT：{_format_optional_usdt(visible_metrics.get('upl_usdt') if isinstance(visible_metrics.get('upl_usdt'), Decimal) else None)}",
            f"已实现≈USDT：{_format_optional_usdt(visible_metrics.get('realized_usdt') if isinstance(visible_metrics.get('realized_usdt'), Decimal) else None)}",
            f"市值≈USDT：{_format_optional_approx_usdt(visible_metrics.get('market_value_usdt') if isinstance(visible_metrics.get('market_value_usdt'), Decimal) else None)}",
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
                        "账户资产",
                        f"读取失败：{exc}",
                    ]
                )
                return "\n".join(lines)

            lines.extend(
                [
                    "",
                    "账户资产",
                    f"账户模式：{_format_account_level_text(getattr(config, 'account_level', None))}",
                    f"持仓模式：{_format_account_position_mode_text(getattr(config, 'position_mode', None))}",
                    f"Greeks 类型：{_format_greeks_type_text(getattr(config, 'greeks_type', None))}",
                    f"自动借币：{_format_bool_text(getattr(config, 'auto_loan', None))}",
                    f"总权益：{_format_optional_usdt_precise(getattr(overview, 'total_equity', None), places=2, with_sign=False)}",
                    f"总权益（约BTC）：{format_btc_amount(AccountPositionsHomeWidget._derive_total_equity_btc(getattr(overview, 'total_equity', None), getattr(overview, 'details', ()) ))} BTC",
                    f"调整后权益：{_format_optional_usdt_precise(getattr(overview, 'adjusted_equity', None), places=2, with_sign=False)}",
                    f"可用权益：{_format_optional_usdt_precise(getattr(overview, 'available_equity', None), places=2, with_sign=False)}",
                    f"未实现盈亏：{_format_optional_usdt_precise(getattr(overview, 'unrealized_pnl', None), places=2)}",
                    f"初始保证金(IMR)：{_format_optional_usdt_precise(getattr(overview, 'initial_margin', None), places=2, with_sign=False)}",
                    f"维持保证金(MMR)：{_format_optional_usdt_precise(getattr(overview, 'maintenance_margin', None), places=2, with_sign=False)}",
                    f"订单冻结：{_format_optional_usdt_precise(getattr(overview, 'order_frozen', None), places=2, with_sign=False)}",
                    f"总名义价值(USD)：{_format_optional_usdt_precise(getattr(overview, 'notional_usd', None), places=2, with_sign=False)}",
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
                lines.extend(["", f"资产明细 Top {min(len(assets), 12)}"])
                for index, asset in enumerate(assets[:12], start=1):
                    lines.append(
                        f"{index:02d}. {asset.ccy or '-'}"
                        f" | 权益={format_btc_amount(asset.equity)}"
                        f" | 可用={format_btc_amount(asset.available_balance)}"
                        f" | 可用权益={format_btc_amount(asset.available_equity)}"
                        f" | 折合USD={_format_optional_usdt_precise(asset.equity_usd, places=2, with_sign=False)}"
                        f" | 未实现={_format_optional_decimal_fixed(asset.unrealized_pnl, places=6, with_sign=True) if isinstance(asset.unrealized_pnl, Decimal) else '-'}"
                        f" | 负债={format_btc_amount(asset.liability)}"
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
            self._detail_toggle_button.setText("折叠持仓详情" if not self._detail_panel.isHidden() else "展开持仓详情")
        if hasattr(self, "_history_toggle_button") and hasattr(self, "_history_panel"):
            self._history_toggle_button.setText("折叠历史区域" if not self._history_panel.isHidden() else "展开历史区域")

    def _show_not_ready_action(self) -> None:
        sender = self.sender()
        text = "-"
        if isinstance(sender, QPushButton):
            text = sender.text().strip() or "-"
        route = "placeholder"
        if text == "????":
            route = "cancel_selected_current_order"
        try:
            append_log_line(f"[qt_positions] click_entry | action={text} | route={route}")
        except Exception:
            pass
        if isinstance(sender, QPushButton) and text == "????":
            self._cancel_selected_current_order()
            return
        QMessageBox.information(self, "迁移", "这个入口已经预留到主页上，下一步会按旧页面逻辑继续接入。")

    def _apply_filters(self, *_args: object) -> None:
        self._render_positions_tree()

    def _on_profile_changed(self, *_args: object) -> None:
        if (
            self._profile_switch_guard
            or not self._profile_change_ready
            or getattr(self, "_profile_switch_in_progress", False)
        ):
            return
        target = self._current_profile_name()
        if not target or target == self._last_profile_name:
            return
        self._profile_change_serial += 1
        serial = self._profile_change_serial
        self._set_profile_switch_in_progress(True)
        _debug_log(f"[profile_switch] request | target={target} | serial={serial}")
        QTimer.singleShot(0, lambda target=target, serial=serial: self._dispatch_profile_change(target, serial))

    def _dispatch_profile_change(self, target: str, serial: int) -> None:
        if serial != self._profile_change_serial:
            return
        if QApplication.activePopupWidget() is not None:
            QTimer.singleShot(0, lambda target=target, serial=serial: self._dispatch_profile_change(target, serial))
            return
        self._apply_profile_change(target, serial)

    def _apply_profile_change(self, target: str, serial: int) -> None:
        if serial != self._profile_change_serial:
            self._clear_profile_switch_request()
            return
        if self._profile_switch_guard:
            self._clear_profile_switch_request()
            return
        current = self._current_profile_name()
        if not target or current != target or target == self._last_profile_name:
            self._clear_profile_switch_request()
            return
        if profile_requires_password(target, self._profile_snapshots) and target not in self._unlocked_profiles:
            self._prompt_profile_unlock(target, serial)
            return
        runtime = load_runtime(target)
        if runtime is None:
            QMessageBox.warning(self, "切换失败", f"API 配置 {target} 不可用，请检查凭证。")
            self._restore_previous_profile_selection()
            self._clear_profile_switch_request()
            return
        _debug_log(f"[profile_switch] apply | target={target} | serial={serial}")
        self._begin_profile_switch_restart(target, runtime, serial)

    def _prompt_profile_unlock(self, target: str, serial: int) -> None:
        if serial != self._profile_change_serial:
            return
        existing = self._profile_unlock_dialog
        if existing is not None:
            existing.close()
            existing.deleteLater()
            self._profile_unlock_dialog = None

        dialog = QDialog(self)
        dialog.setModal(True)
        dialog.setWindowTitle("输入 API 切换密码")
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(10)

        prompt = QLabel(f"API 配置 {target} 已设置切换密码，请输入密码后继续。", dialog)
        prompt.setWordWrap(True)
        layout.addWidget(prompt)

        password_edit = QLineEdit(dialog)
        password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        password_edit.setPlaceholderText("切换密码")
        layout.addWidget(password_edit)

        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
            parent=dialog,
        )
        buttons.button(QDialogButtonBox.StandardButton.Ok).setText("确定")
        buttons.button(QDialogButtonBox.StandardButton.Cancel).setText("取消")
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        def _finish(result_code: int) -> None:
            if self._profile_unlock_dialog is dialog:
                self._profile_unlock_dialog = None
            password = password_edit.text()
            dialog.deleteLater()
            if serial != self._profile_change_serial:
                self._clear_profile_switch_request()
                return
            if self._current_profile_name() != target:
                self._clear_profile_switch_request()
                return
            if result_code != int(QDialog.DialogCode.Accepted):
                self._restore_previous_profile_selection()
                self._clear_profile_switch_request()
                return
            if verify_profile_switch_password(self._profile_snapshots.get(target, {}), password):
                self._unlocked_profiles.add(target)
                QTimer.singleShot(0, lambda target=target, serial=serial: self._apply_profile_change(target, serial))
                return
            QMessageBox.warning(self, "密码错误", f"API 配置 {target} 的切换密码不正确。")
            self._restore_previous_profile_selection()
            self._clear_profile_switch_request()

        dialog.finished.connect(_finish)
        self._profile_unlock_dialog = dialog
        QTimer.singleShot(0, password_edit.setFocus)
        dialog.open()

    def _restore_previous_profile_selection(self) -> None:
        self._profile_switch_guard = True
        with QSignalBlocker(self._profile_combo):
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

    def _open_position_kline(self, position: OkxPosition, *, time_markers: tuple[tuple[str, int], ...] = ()) -> None:
        if self._instrument_kline_dialog is None:
            self._instrument_kline_dialog = InstrumentKlineDialog(
                initial_bar=self._position_kline_last_bar,
                initial_width=self._position_kline_window_width,
                initial_height=self._position_kline_window_height,
                prefs_changed=self._on_position_kline_prefs_changed,
                parent=self,
            )
        if not time_markers:
            time_markers = _current_position_kline_time_markers(position, self._position_history_items)
        underlying_usdt_price: Decimal | None = None
        underlying_usdt_basis = ""
        option_entry_price: Decimal | None = None
        if position.inst_type == "OPTION":
            underlying = position.inst_id.split("-", 1)[0].strip().upper()
            candidate = self._upl_usdt_prices.get(underlying)
            if not isinstance(candidate, Decimal) or candidate <= 0:
                raw = position.raw if isinstance(position.raw, dict) else {}
                for key in ("idxPx", "indexPx", "underlyingPx", "ulyPx"):
                    try:
                        fallback = Decimal(str(raw.get(key, "")).strip())
                    except Exception:
                        continue
                    if fallback > 0:
                        candidate = fallback
                        break
            if isinstance(candidate, Decimal) and candidate > 0:
                underlying_usdt_price = candidate
                underlying_usdt_basis = f"{underlying}-USDT {candidate}（打开图时）"
            if isinstance(position.avg_price, Decimal) and position.avg_price > 0:
                option_entry_price = position.avg_price
        self._instrument_kline_dialog.show_instrument(
            inst_id=position.inst_id,
            inst_type=position.inst_type,
            underlying_usdt_price=underlying_usdt_price,
            underlying_usdt_basis=underlying_usdt_basis,
            option_entry_price=option_entry_price,
            time_markers=time_markers,
        )

    @Slot(int, int)
    def _on_position_history_table_clicked(self, row: int, column: int) -> None:
        if column != 2 or row < 0 or row >= len(self._visible_position_history_items):
            return
        item = self._visible_position_history_items[row]
        if not item.inst_id or not item.inst_type:
            return
        self._open_position_history_kline(item)

    def _open_position_history_kline(self, item: OkxPositionHistoryItem) -> None:
        self._open_position_kline(
            SimpleNamespace(
                inst_id=item.inst_id,
                inst_type=item.inst_type,
                avg_price=item.open_avg_price,
                raw=item.raw,
            ),
            time_markers=_position_history_kline_time_markers(item),
        )

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
        self._status_badge.setText("正常")

    @Slot(object)
    def _apply_positions_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        positions = payload.get("positions")
        next_positions = list(positions) if isinstance(positions, list) else []
        instruments = payload.get("position_instruments")
        tickers = payload.get("position_tickers")
        prices = payload.get("upl_usdt_prices")
        next_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        next_tickers = dict(tickers) if isinstance(tickers, dict) else {}
        next_prices = dict(prices) if isinstance(prices, dict) else {}
        positions_changed = next_positions != self._raw_positions
        if (
            not positions_changed
            and next_instruments == self._position_instruments
            and next_tickers == self._position_tickers
            and next_prices == self._upl_usdt_prices
        ):
            return
        self._raw_positions = next_positions
        self._position_instruments = next_instruments
        self._position_tickers = next_tickers
        self._upl_usdt_prices = next_prices
        if self._last_profile_name and positions_changed:
            notes_changed = _reconcile_current_position_note_records(
                self._current_notes,
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                positions=self._raw_positions,
                now_ms=int(time.time() * 1000),
            )
            if notes_changed:
                self._save_position_notes()
        self._render_positions_tree()

    @Slot(object)
    def _apply_orders(self, orders: object) -> None:
        self._orders = list(orders) if isinstance(orders, list) else []
        self._visible_orders = list(self._orders)
        with measure_ui_step("orders_apply", rows=len(self._orders)):
            self._refresh_current_orders_table()

    @Slot(object)
    def _apply_realtime_snapshot(self, snapshot: object) -> None:
        if not isinstance(snapshot, AccountRealtimeSnapshot):
            return
        runtime = self._runtime
        if runtime is None:
            return
        if snapshot.profile_name != self._last_profile_name:
            return
        if snapshot.environment != str(getattr(runtime, "environment", "") or ""):
            return
        latest_positions = list(snapshot.positions)
        if latest_positions != self._raw_positions:
            instruments = snapshot.position_instruments or self._position_instruments
            tickers = snapshot.position_tickers or self._position_tickers
            prices = snapshot.upl_usdt_prices or self._upl_usdt_prices
            self._apply_positions_payload(
                {
                    "positions": latest_positions,
                    "position_instruments": dict(instruments),
                    "position_tickers": dict(tickers),
                    "upl_usdt_prices": dict(prices),
                }
            )
            self._apply_positions_summary(latest_positions)
        self._apply_orders(list(snapshot.orders))

    @Slot(str)
    def _set_realtime_status(self, text: str) -> None:
        self._set_account_status(text)
        self._set_order_status(text)

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
        self._tabs.addTab(self._build_current_orders_tab_v2(), "当前委托")
        self._tabs.addTab(
            self._build_placeholder_tab("动态止盈接管", "动态止盈接管页保留在这里，后续继续按旧版完整迁移。"),
            "动态止盈接管",
        )
        self._tabs.addTab(self._build_order_history_tab(), "历史委托")
        self._tabs.addTab(self._build_fill_history_tab(), "历史成交")
        self._tabs.addTab(self._build_position_history_tab_v2(), "历史仓位")
        layout.addWidget(self._tabs, 1)
        return self._history_panel

    def _build_current_orders_tab_v2(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._orders_summary_label = QLabel("当前委托尚未读取。")
        self._orders_summary_label.setObjectName("Subtle")
        self._orders_summary_label.setWordWrap(True)
        top.addWidget(self._orders_summary_label, 1)
        for text, handler in (
            ("刷新", self.refresh_view),
            ("从选中条件单接管动态止盈", self._show_not_ready_action),
            ("撤单选中", self._cancel_selected_current_order),
            ("批量撤当前筛选", self._show_not_ready_action),
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
        apply_button = QPushButton("应用筛选")
        apply_button.clicked.connect(self._refresh_current_orders_table)
        clear_button = QPushButton("清空筛选")
        clear_button.clicked.connect(self._clear_pending_order_filters)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._pending_type_combo, 0, 1)
        filter_row.addWidget(QLabel("来源"), 0, 2)
        filter_row.addWidget(self._pending_source_combo, 0, 3)
        filter_row.addWidget(QLabel("状态"), 0, 4)
        filter_row.addWidget(self._pending_state_combo, 0, 5)
        filter_row.addWidget(QLabel("标的"), 0, 6)
        filter_row.addWidget(self._pending_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("到期前缀"), 0, 8)
        filter_row.addWidget(self._pending_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("搜索"), 0, 10)
        filter_row.addWidget(self._pending_keyword_edit, 0, 11)
        filter_row.addWidget(apply_button, 0, 12)
        filter_row.addWidget(clear_button, 0, 13)
        layout.addLayout(filter_row)

        self._orders_table = self._build_history_table(
            ("时间", "来源", "类型", "合约", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "手续费", "TP/SL", "订单ID", "clOrdId"),
            stretch_columns={3, 11, 13},
        )
        self._orders_table.setColumnWidth(8, 180)
        self._orders_table.setColumnWidth(9, 180)
        layout.addWidget(self._orders_table, 1)
        return tab

    def _build_order_history_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        top = QHBoxLayout()
        self._order_history_summary_label = QLabel("历史委托尚未读取。")
        self._order_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._order_history_summary_label, 1)
        sync_button = QPushButton("同步")
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
        order_apply = QPushButton("应用筛选")
        order_apply.clicked.connect(self._refresh_order_history_table)
        order_clear = QPushButton("清空筛选")
        order_clear.clicked.connect(self._clear_order_history_filters)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._order_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("来源"), 0, 2)
        filter_row.addWidget(self._order_history_source_combo, 0, 3)
        filter_row.addWidget(QLabel("状态"), 0, 4)
        filter_row.addWidget(self._order_history_state_combo, 0, 5)
        filter_row.addWidget(QLabel("标的"), 0, 6)
        filter_row.addWidget(self._order_history_asset_edit, 0, 7)
        filter_row.addWidget(QLabel("到期前缀"), 0, 8)
        filter_row.addWidget(self._order_history_expiry_edit, 0, 9)
        filter_row.addWidget(QLabel("搜索"), 0, 10)
        filter_row.addWidget(self._order_history_keyword_edit, 0, 11)
        filter_row.addWidget(order_apply, 0, 12)
        filter_row.addWidget(order_clear, 0, 13)
        layout.addLayout(filter_row)

        self._order_history_table = self._build_history_table(
            ("时间", "来源", "类型", "合约", "状态", "方向", "委托类型", "委托价", "委托量", "已成交", "手续费", "TP/SL", "订单ID", "clOrdId"),
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
        self._fill_history_summary_label = QLabel("历史成交尚未读取。")
        self._fill_history_summary_label.setObjectName("Subtle")
        top.addWidget(self._fill_history_summary_label, 1)
        more_button = QPushButton("增加100条")
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
        fill_apply = QPushButton("应用筛选")
        fill_apply.clicked.connect(self._refresh_fill_history_table)
        fill_clear = QPushButton("清空筛选")
        fill_clear.clicked.connect(self._clear_fill_history_filters)
        fill_contract = QPushButton("带入合约")
        fill_contract.clicked.connect(self.apply_selected_option_to_fill_history_search)
        fill_expiry = QPushButton("带入到期前缀")
        fill_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_fill_history_search)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._fill_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("方向"), 0, 2)
        filter_row.addWidget(self._fill_history_side_combo, 0, 3)
        filter_row.addWidget(QLabel("标的"), 0, 4)
        filter_row.addWidget(self._fill_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("到期前缀"), 0, 6)
        filter_row.addWidget(self._fill_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("搜索"), 0, 8)
        filter_row.addWidget(self._fill_history_keyword_edit, 0, 9)
        filter_row.addWidget(fill_contract, 0, 10)
        filter_row.addWidget(fill_expiry, 0, 11)
        filter_row.addWidget(fill_apply, 0, 12)
        filter_row.addWidget(fill_clear, 0, 13)
        layout.addLayout(filter_row)

        self._fill_history_table = self._build_history_table(
            ("时间", "类型", "合约", "方向", "成交价", "成交量", "手续费", "已实现盈亏", "成交类型"),
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
        self._position_history_summary_label = QLabel("历史仓位尚未读取。")
        self._position_history_summary_label.setObjectName("Subtle")
        self._position_history_summary_label.setWordWrap(True)
        top.addWidget(self._position_history_summary_label, 1)
        more_button = QPushButton("增加100条")
        more_button.clicked.connect(self._expand_position_history_limit)
        top.addWidget(more_button)
        edit_button = QPushButton("编辑备注")
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
        pos_apply = QPushButton("应用筛选")
        pos_apply.clicked.connect(self._force_position_history_render)
        pos_clear = QPushButton("清空筛选")
        pos_clear.clicked.connect(self._clear_position_history_filters)
        pos_contract = QPushButton("带入合约")
        pos_contract.clicked.connect(self.apply_selected_option_to_position_history_search)
        pos_expiry = QPushButton("带入到期前缀")
        pos_expiry.clicked.connect(self.apply_selected_option_expiry_prefix_to_position_history_search)
        filter_row.addWidget(QLabel("类型"), 0, 0)
        filter_row.addWidget(self._position_history_type_combo, 0, 1)
        filter_row.addWidget(QLabel("保证金模式"), 0, 2)
        filter_row.addWidget(self._position_history_margin_combo, 0, 3)
        filter_row.addWidget(QLabel("标的"), 0, 4)
        filter_row.addWidget(self._position_history_asset_edit, 0, 5)
        filter_row.addWidget(QLabel("到期前缀"), 0, 6)
        filter_row.addWidget(self._position_history_expiry_edit, 0, 7)
        filter_row.addWidget(QLabel("搜索"), 0, 8)
        filter_row.addWidget(self._position_history_keyword_edit, 0, 9)
        filter_row.addWidget(pos_contract, 0, 10)
        filter_row.addWidget(pos_expiry, 0, 11)
        filter_row.addWidget(pos_apply, 0, 12)
        filter_row.addWidget(pos_clear, 0, 13)
        filter_row.addWidget(QLabel("本地开始"), 1, 0)
        filter_row.addWidget(self._position_history_range_start_edit, 1, 1)
        filter_row.addWidget(QLabel("本地结束"), 1, 2)
        filter_row.addWidget(self._position_history_range_end_edit, 1, 3)
        filter_row.addWidget(QLabel("YYYYMMDD 或 YYYY-MM-DD，留空则不过滤"), 1, 4, 1, 10)
        layout.addLayout(filter_row)

        self._position_history_table = self._build_history_table(
            ("时间", "类型", "合约", "保证金模式", "持仓模式", "交易方向", "开仓均价", "平仓均价", "平仓数量", "手续费", "盈亏", "备注"),
            stretch_columns={2, 11},
        )
        self._position_history_table.cellDoubleClicked.connect(self._on_position_history_table_clicked)
        self._position_history_table.setColumnWidth(0, 170)
        self._position_history_table.setColumnWidth(9, 220)
        self._position_history_table.setColumnWidth(10, 240)
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
                QHeaderView.ResizeMode.Stretch if index in stretch_columns else QHeaderView.ResizeMode.Interactive,
            )
        table.setColumnWidth(0, 180)
        return table

    def _start_private_threads(self, *, force_restart: bool = False, start_history: bool = True) -> None:
        if self._runtime is None:
            return
        if force_restart:
            self._stop_private_threads()
        self._private_thread_generation += 1
        generation = self._private_thread_generation
        profile_name = self._last_profile_name or "-"
        _debug_log(f"[profile_switch] start_realtime_store | profile={profile_name} | generation={generation}")
        self._realtime_store.start(self._runtime)
        if start_history:
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
            QMessageBox.information(self, "带入合约", "请先在历史成交里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._fill_history_keyword_edit.setText(contract)
        self._refresh_fill_history_table()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_fill_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "带入到期前缀", "请先在历史成交里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._fill_history_expiry_edit.setText(expiry_prefix)
        self._refresh_fill_history_table()

    def apply_selected_option_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        contract, _expiry_prefix = _option_search_shortcuts(inst_id)
        if not contract:
            QMessageBox.information(self, "带入合约", "请先在历史仓位里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
            return
        self._position_history_keyword_edit.setText(contract)
        self._force_position_history_render()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        inst_id = self._selected_option_inst_id_for_position_history_shortcut()
        _contract, expiry_prefix = _option_search_shortcuts(inst_id)
        if not expiry_prefix:
            QMessageBox.information(self, "带入到期前缀", "请先在历史仓位里选中一条期权记录，或在当前持仓里选中一条期权持仓。")
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
            if not state_filter and _current_order_state_is_terminal(item.state):
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
                        (item.ord_id or ""),
                        (item.client_order_id or ""),
                        str(item.raw.get("algoId") or ""),
                        str(item.raw.get("algoClOrdId") or ""),
                    )
                ).upper()
                if keyword not in haystack:
                    continue
            result.append(item)
        return result

    def _format_current_order_size_with_coin(self, order: object, *, filled: bool) -> str:
        instruments = dict(self._position_instruments)
        inst_id = str(getattr(order, "inst_id", "") or "").strip()
        inst_type = str(getattr(order, "inst_type", "") or "").upper()
        if inst_id and inst_type in {"SWAP", "FUTURES", "OPTION"} and inst_id not in instruments:
            cache = getattr(self, "_current_order_instruments", {})
            instrument = cache.get(inst_id)
            if instrument is None:
                try:
                    instrument = self._shared_client.get_instrument(inst_id)
                except Exception:
                    instrument = None
                if instrument is not None:
                    cache[inst_id] = instrument
                    self._current_order_instruments = cache
            if instrument is not None:
                instruments[inst_id] = instrument
        return _format_trade_order_size_with_coin(order, instruments, filled=filled)
    def _refresh_current_orders_table(self) -> None:
        if not hasattr(self, "_orders_table"):
            return
        filtered = self._filtered_current_orders()
        selected_ord_id = ""
        row = self._orders_table.currentRow()
        if 0 <= row < len(filtered):
            selected_ord_id = filtered[row].ord_id
        self._orders_summary_label.setText(f"当前委托：{len(filtered)} 条")
        table_rows: list[tuple[object, tuple[str, ...]]] = []
        for row, order in enumerate(filtered):
            item = _current_order_view_to_trade_order_item(order)
            feed_source = str(order.raw.get("_feed_source") or "").strip().lower()
            source_kind = str(order.raw.get("_source_kind") or "").strip().lower()
            if feed_source == "rest_pending" and source_kind == "algo":
                source_label = "REST 算法"
            elif feed_source == "rest_pending":
                source_label = "REST pending"
            else:
                source_label = "WS 当前"
            values = (
                _format_okx_ms_timestamp(order.update_time or order.created_time),
                source_label,
                order.inst_type or "-",
                order.inst_id or "-",
                _format_trade_order_state(order.state),
                _format_history_side(order.side or "-", order.pos_side or ""),
                order.ord_type or "-",
                _format_trade_order_price(order.price, order.inst_id, order.inst_type or ""),
                self._format_current_order_size_with_coin(order, filled=False),
                self._format_current_order_size_with_coin(order, filled=True),
                _format_trade_order_fee_cell(item),
                _format_trade_order_tp_sl(item),
                item.order_id or item.algo_id or "-",
                item.client_order_id or item.algo_client_order_id or "-",
            )
            identity = OrderFeedThread._view_identity(order) or ("row", row, order.inst_id, order.update_time)
            table_rows.append((identity, values))
        previous_rows = getattr(self, "_current_order_table_rows", ())
        delta = keyed_row_delta(previous_rows, table_rows)
        if delta.structure_changed:
            self._orders_table.setRowCount(len(table_rows))
            for row, (_identity, values) in enumerate(table_rows):
                self._set_table_row(self._orders_table, row, values, left_align={3, 13})
        else:
            changed_keys = set(delta.changed_keys)
            for row, (identity, values) in enumerate(table_rows):
                if identity in changed_keys:
                    self._update_table_row(self._orders_table, row, values, left_align={3, 13})
        self._current_order_table_rows = tuple(table_rows)
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
                self._orders_detail.setPlainText("这里会显示选中当前委托的详情。")
            return
        order = items[row]
        item = _current_order_view_to_trade_order_item(order)
        lines = [
            f"时间：{_format_okx_ms_timestamp(order.update_time or order.created_time)}",
            f"合约：{order.inst_id or '-'}",
            f"类型：{order.inst_type or '-'}",
            f"状态：{_format_trade_order_state(order.state)}",
            f"方向：{_format_history_side(order.side or '-', order.pos_side or '')}",
            f"委托类型：{order.ord_type or '-'}",
            f"委托价：{_format_trade_order_price(order.price, order.inst_id, order.inst_type or '')}",
            f"委托量：{_format_trade_order_size(order.size)}",
            f"已成交：{_format_trade_order_size(order.filled_size)}",
            f"交易模式：{order.td_mode or '-'}",
            f"订单ID：{order.ord_id or '-'}",
            f"clOrdId：{order.client_order_id or '-'}",
            "",
            json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True),
        ]
        self._orders_detail.setPlainText(
            f"{_build_trade_order_detail_text(item)}\n\n{json.dumps(order.raw, ensure_ascii=False, indent=2, sort_keys=True)}"
        )

    def _selected_current_order(self) -> OrderStatusView | None:
        if not hasattr(self, "_orders_table"):
            return None
        filtered = getattr(self, "_current_order_rows", None)
        if not isinstance(filtered, list):
            filtered = self._filtered_current_orders()
        row = self._orders_table.currentRow()
        if row < 0 or row >= len(filtered):
            return None
        item = filtered[row]
        return item if isinstance(item, OrderStatusView) else None

    def _cancel_selected_current_order(self) -> None:
        if self._current_order_canceling:
            QMessageBox.information(self, "撤单", "当前已有一笔撤单请求在处理中，请稍等。")
            return
        if not self._ensure_runtime_ready(force_unlock=True):
            return
        order = self._selected_current_order()
        if order is None:
            QMessageBox.information(self, "撤单", "请先在当前委托里选中一条要撤销的委托。")
            return
        owner_label = _current_order_view_owner_display_label(order)
        cancel_id = _current_order_view_cancel_reference(order)
        if not cancel_id:
            QMessageBox.information(self, "撤单", "这条委托缺少可用订单 ID，暂时无法撤单。")
            return
        source_notice = ""
        if owner_label == "未识别来源":
            source_notice = "\n\n提示：这条委托没有匹配到本程序 clOrdId 规则，将按交易所订单标识直接发起撤单。"
        confirm_message = (
            f"确认撤销这条{_current_order_view_source_label(order)}吗？\n\n"
            f"程序来源：{owner_label}\n"
            f"合约：{order.inst_id or '-'}\n"
            f"方向：{_format_history_side(order.side or '-', order.pos_side or '')}\n"
            f"状态：{_format_trade_order_state(order.state)}\n"
            f"标识：{cancel_id}"
            f"{source_notice}"
        )
        if not QMessageBox.question(
            self,
            "撤单确认",
            confirm_message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        ) == QMessageBox.StandardButton.Yes:
            return
        runtime = self._runtime
        if runtime is None:
            QMessageBox.warning(self, "撤单", "当前没有 API 凭证，无法发起撤单。")
            return
        self._current_order_canceling = True
        self._orders_summary_label.setText(
            f"正在撤单：{_current_order_view_source_label(order)} | {order.inst_id or '-'} | {cancel_id}"
        )
        threading.Thread(
            target=self._cancel_selected_current_order_worker,
            args=(runtime.credentials, self._note_environment(), order, owner_label),
            daemon=True,
        ).start()

    def _cancel_selected_current_order_worker(
        self,
        credentials: Credentials,
        environment: str,
        order: OrderStatusView,
        owner_label: str,
    ) -> None:
        try:
            result = self._cancel_selected_current_order_request(credentials, environment=environment, order=order)
            if _current_order_cancel_result_failed(result):
                self._ui_callback.emit(
                    lambda order=order, owner_label=owner_label, message=_current_order_cancel_result_error_message(order, result), environment=environment: self._apply_current_order_cancel_error(
                        order,
                        owner_label,
                        message,
                        environment,
                    )
                )
                return
            note = ""
            effective_environment = environment
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    result = self._cancel_selected_current_order_request(credentials, environment=alternate, order=order)
                    if _current_order_cancel_result_failed(result):
                        self._ui_callback.emit(
                            lambda order=order, owner_label=owner_label, message=_current_order_cancel_result_error_message(order, result), environment=alternate: self._apply_current_order_cancel_error(
                                order,
                                owner_label,
                                message,
                                environment,
                            )
                        )
                        return
                    note = f"撤单自动切换到{'实盘' if alternate == 'live' else '模拟'}环境执行。"
                    effective_environment = alternate
                except Exception as retry_exc:  # noqa: BLE001
                    self._ui_callback.emit(
                        lambda order=order, owner_label=owner_label, message=str(retry_exc), environment=environment: self._apply_current_order_cancel_error(
                            order,
                            owner_label,
                            message,
                            environment,
                        )
                    )
                    return
            else:
                self._ui_callback.emit(
                    lambda order=order, owner_label=owner_label, message=message, environment=environment: self._apply_current_order_cancel_error(
                        order,
                        owner_label,
                        message,
                        environment,
                    )
                )
                return
        self._ui_callback.emit(
            lambda order=order, result=result, owner_label=owner_label, note=note, effective_environment=effective_environment: self._apply_current_order_cancel_result(
                order,
                result,
                owner_label,
                note,
                effective_environment,
            )
        )

    def _cancel_selected_current_order_request(
        self,
        credentials: Credentials,
        *,
        environment: str,
        order: OrderStatusView,
    ) -> OkxOrderResult:
        item = _current_order_view_to_trade_order_item(order)
        if item.source_kind == "algo":
            return self._shared_client.cancel_algo_order(
                credentials,
                environment=environment,
                inst_id=item.inst_id,
                algo_id=item.algo_id or None,
                algo_cl_ord_id=item.algo_client_order_id or item.client_order_id or None,
            )
        return self._shared_client.cancel_order_by_id(
            credentials,
            environment=environment,
            inst_id=item.inst_id,
            ord_id=item.order_id or None,
            cl_ord_id=item.client_order_id or None,
        )

    def _apply_current_order_cancel_result(
        self,
        order: OrderStatusView,
        result: OkxOrderResult,
        owner_label: str,
        note: str,
        effective_environment: str,
    ) -> None:
        self._current_order_canceling = False
        cancel_id = _current_order_view_cancel_reference(order) or result.ord_id or result.cl_ord_id or "-"
        summary = f"撤单请求已提交：{order.inst_id or '-'} | {cancel_id}"
        if note:
            summary = f"{summary} | {note}"
        self._orders_summary_label.setText(summary)
        QMessageBox.information(
            self,
            "撤单结果",
            (
                "撤单请求已提交。\n\n"
                f"程序来源：{owner_label}\n"
                f"来源：{_current_order_view_source_label(order)}\n"
                f"合约：{order.inst_id or '-'}\n"
                f"标识：{cancel_id}\n"
                f"返回：sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            ),
        )
        self.refresh_view()
        self._refresh_order_history()

    def _apply_current_order_cancel_error(
        self,
        order: OrderStatusView,
        owner_label: str,
        message: str,
        environment: str,
    ) -> None:
        self._current_order_canceling = False
        friendly_message = _format_network_error_message(message)
        self._orders_summary_label.setText(f"撤单失败：{friendly_message}")
        QMessageBox.warning(
            self,
            "撤单失败",
            (
                f"{_current_order_view_source_label(order)} 撤单失败。\n\n"
                f"程序来源：{owner_label}\n"
                f"环境：{'实盘 live' if environment == 'live' else '模拟 demo'}\n"
                f"合约：{order.inst_id or '-'}\n"
                f"原因：{friendly_message}"
            ),
        )

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
        self._order_history_summary_label.setText(f"历史委托：当前显示 {len(filtered)}/{len(self._order_history_items)}")
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
                _format_trade_order_tp_sl(item),
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
                self._order_history_detail.setPlainText("这里会显示选中历史委托的详情。")
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
        self._fill_history_summary_label.setText(f"历史成交：当前显示 {len(filtered)}/{len(self._fill_history_items)}")
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
                self._fill_history_detail.setPlainText("这里会显示选中历史成交的详情。")
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
        self._position_history_table.setHorizontalHeaderItem(10, QTableWidgetItem("\u5df2\u5b9e\u73b0\u6536\u76ca"))
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
                    f"历史仓位：{len(self._position_history_items)} 条 | 最近同步：{self._position_history_last_sync_text} | 当前显示：{len(filtered)}/{len(self._position_history_items)}",
                    f"筛选统计：{stats_text}",
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
                _format_position_history_pnl(
                    item.realized_pnl,
                    item,
                    with_sign=True,
                    usdt_prices=self._position_history_usdt_prices,
                ),
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
                self._position_history_detail.setPlainText("这里会显示选中历史仓位的详情。")
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
            QMessageBox.information(self, "编辑备注", "请先选择一条历史仓位。")
            return
        dialog = NoteEditorDialog(
            title="编辑历史仓位备注",
            prompt=f"为 {item.inst_id} 填写备注。留空后保存会清空历史仓位备注。",
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
        self._visible_orders = list(self._orders)
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
        next_items = list(items) if isinstance(items, list) else []
        next_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        next_prices = dict(prices) if isinstance(prices, dict) else {}
        if (
            next_items == self._fill_history_items
            and next_instruments == self._fill_history_instruments
            and next_prices == self._fill_history_usdt_prices
        ):
            return
        self._fill_history_items = next_items
        self._fill_history_instruments = next_instruments
        self._fill_history_usdt_prices = next_prices
        self._refresh_fill_history_table()

    @Slot(object)
    def _apply_position_history_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        items = payload.get("items")
        instruments = payload.get("instruments")
        usdt_prices = payload.get("usdt_prices")
        next_items = list(items) if isinstance(items, list) else []
        next_instruments = dict(instruments) if isinstance(instruments, dict) else {}
        next_prices = dict(usdt_prices) if isinstance(usdt_prices, dict) else {}
        notes_changed = False
        if self._last_profile_name:
            now_ms = int(time.time() * 1000)
            notes_changed = _inherit_position_history_notes(
                self._current_notes,
                self._history_notes,
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
                position_history=next_items,
                now_ms=now_ms,
            )
            notes_changed = _prune_closed_current_position_notes(
                self._current_notes,
                self._history_notes,
                profile_name=self._last_profile_name,
                environment=self._note_environment(),
            ) or notes_changed
            if notes_changed:
                self._save_position_notes()
        if (
            not notes_changed
            and next_items == self._position_history_items
            and next_instruments == self._position_history_instruments
            and next_prices == self._position_history_usdt_prices
        ):
            return
        self._position_history_items = next_items
        self._position_history_instruments = next_instruments
        self._position_history_usdt_prices = next_prices
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

    def _update_table_row(
        self,
        table: QTableWidget,
        row: int,
        values: tuple[str, ...],
        *,
        left_align: set[int] | None = None,
    ) -> None:
        left_align = left_align or set()
        for column, value in enumerate(values):
            text = str(value)
            cell = table.item(row, column)
            if cell is None:
                cell = QTableWidgetItem(text)
                if column in left_align:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter))
                else:
                    cell.setTextAlignment(int(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter))
                table.setItem(row, column, cell)
            elif cell.text() != text:
                cell.setText(text)

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
        return datetime.now().strftime("%Y%m%d")

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
        self._status_badge.setText("正在刷新...")
        self._restart_live_feeds_for_manual_refresh()


_ACCOUNT_POSITIONS_HOME_ORIGINAL_INIT = AccountPositionsHomeWidget.__init__
_ACCOUNT_POSITIONS_HOME_ORIGINAL_APPLY_ORDERS = AccountPositionsHomeWidget._apply_orders
_ACCOUNT_POSITIONS_HOME_ORIGINAL_APPLY_ORDER_HISTORY_PAYLOAD = AccountPositionsHomeWidget._apply_order_history_payload


def _account_positions_home_shared_init(self: AccountPositionsHomeWidget, parent: QWidget | None = None) -> None:
    self._shared_order_store = get_shared_order_store()
    _ACCOUNT_POSITIONS_HOME_ORIGINAL_INIT(self, parent)
    self._shared_order_store.snapshot_changed.connect(self._apply_shared_order_snapshot)
    runtime = getattr(self, "_runtime", None)
    profile_name = str(getattr(self, "_last_profile_name", "") or "").strip()
    environment = str(getattr(runtime, "environment", "") or "").strip()
    if profile_name:
        self._apply_shared_order_snapshot(
            profile_name,
            environment,
            self._shared_order_store.snapshot_for(profile_name=profile_name, environment=environment),
        )


def _account_positions_home_apply_orders(self: AccountPositionsHomeWidget, orders: object) -> None:
    _ACCOUNT_POSITIONS_HOME_ORIGINAL_APPLY_ORDERS(self, orders)
    shared_order_store = getattr(self, "_shared_order_store", None)
    runtime = getattr(self, "_runtime", None)
    profile_name = str(getattr(self, "_last_profile_name", "") or "").strip()
    environment = str(getattr(runtime, "environment", "") or "").strip()
    if runtime is None or not profile_name or shared_order_store is None:
        return
    shared_order_store.publish_current_orders(
        profile_name=profile_name,
        environment=environment,
        orders=list(self._orders),
    )


def _account_positions_home_apply_order_history_payload(self: AccountPositionsHomeWidget, payload: object) -> None:
    if not isinstance(payload, dict):
        return
    items = payload.get("items")
    prices = payload.get("usdt_prices")
    next_items = list(items) if isinstance(items, list) else []
    next_prices = dict(prices) if isinstance(prices, dict) else {}
    if (
        next_items == list(getattr(self, "_order_history_items", []))
        and next_prices == dict(getattr(self, "_order_history_usdt_prices", {}))
    ):
        return
    _ACCOUNT_POSITIONS_HOME_ORIGINAL_APPLY_ORDER_HISTORY_PAYLOAD(self, payload)
    shared_order_store = getattr(self, "_shared_order_store", None)
    runtime = getattr(self, "_runtime", None)
    profile_name = str(getattr(self, "_last_profile_name", "") or "").strip()
    environment = str(getattr(runtime, "environment", "") or "").strip()
    if runtime is None or not profile_name or shared_order_store is None:
        return
    shared_order_store.publish_history_orders(
        profile_name=profile_name,
        environment=environment,
        orders=list(self._order_history_items),
        usdt_prices=dict(self._order_history_usdt_prices),
    )


def _account_positions_home_apply_shared_order_snapshot(
    self: AccountPositionsHomeWidget,
    profile_name: str,
    environment: str,
    snapshot: object,
) -> None:
    runtime = getattr(self, "_runtime", None)
    current_profile_name = str(getattr(self, "_last_profile_name", "") or "").strip()
    current_environment = str(getattr(runtime, "environment", "") or "").strip()
    if profile_name != current_profile_name or environment != current_environment:
        return
    if not isinstance(snapshot, SharedOrderSnapshot):
        return
    next_orders = list(snapshot.current_order_views)
    next_history_orders = list(snapshot.history_orders)
    next_history_prices = dict(snapshot.history_order_usdt_prices)
    current_changed = next_orders != list(getattr(self, "_orders", []))
    history_changed = (
        next_history_orders != list(getattr(self, "_order_history_items", []))
        or next_history_prices != dict(getattr(self, "_order_history_usdt_prices", {}))
    )
    if current_changed:
        self._orders = next_orders
        self._visible_orders = list(next_orders)
        self._refresh_current_orders_table()
    if history_changed:
        self._order_history_items = next_history_orders
        self._order_history_usdt_prices = next_history_prices
        self._refresh_order_history_table()


AccountPositionsHomeWidget.__init__ = _account_positions_home_shared_init
AccountPositionsHomeWidget._apply_orders = _account_positions_home_apply_orders
AccountPositionsHomeWidget._apply_order_history_payload = _account_positions_home_apply_order_history_payload
AccountPositionsHomeWidget._apply_shared_order_snapshot = _account_positions_home_apply_shared_order_snapshot
