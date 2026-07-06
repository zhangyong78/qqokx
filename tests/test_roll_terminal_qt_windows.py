from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import textwrap
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from PySide6.QtCharts import QChart, QLineSeries, QValueAxis
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox, QPushButton, QSizePolicy
from okx_quant.persistence import (
    build_profile_switch_password_snapshot,
    load_kline_analysis_workspace_entries,
    save_kline_analysis_workspace_entries,
)
from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.auto_channel_window import _safe_text as auto_safe_text
from roll_terminal_qt.deribit_volatility_window import (
    DeribitVolatilityQtWindow,
    _attach_series_to_axes_once,
    _build_moving_average_series,
)
from roll_terminal_qt.line_trading_window import (
    LINE_TRADING_DESK_TOOL_ACTIONS,
    LineTradingQtWindow,
    _build_annotation_key,
    _compute_rr_target,
    _safe_text as line_safe_text,
    _split_annotation_key,
)
from roll_terminal_qt.line_trading_core import LineAnnotation, RiskRewardAnnotation
from roll_terminal_qt.profile_access import profile_requires_password
from roll_terminal_qt.smart_order_window import _safe_text as smart_safe_text
from roll_terminal_qt.smart_order_window import (
    SMART_ORDER_COMPACT_ROOT_MARGINS,
    SMART_ORDER_COMPACT_SPLITTER_SIZES,
    SMART_ORDER_LOG_MIN_HEIGHT,
    SMART_ORDER_TASK_DETAIL_HEIGHT,
)
from roll_terminal_qt.kline_analysis_window import (
    KlineAnalysisWindow,
    _AUTO_REFRESH_DEFAULT_ENABLED,
    _DEFAULT_DUAL_PRIMARY_PERIOD,
    _DEFAULT_DUAL_SECONDARY_PERIOD,
    _DEFAULT_SINGLE_CHART_PERIOD,
    _EMA15_LINE_WIDTH,
    _SMA50_LINE_WIDTH,
    _apply_drag_to_line_rule,
    _build_display_times_ms,
    _default_chart_stack_splitter_sizes,
    _default_kline_splitter_sizes,
    _display_x_for_candle_time,
    _debug_log,
    _line_handle_visual,
    _line_time_tolerance_seconds,
    _line_price_tolerance,
    _next_secondary_chart_kind_button_text,
    _next_secondary_layout_button_text,
    _default_native_x_range_with_right_padding,
    _prefer_native_chart_backend,
    _default_native_visible_range,
    _native_right_padding_ms,
    _ordered_trend_endpoints,
    _resolve_interaction_cursor_mode,
    _resolve_candle_time_from_x_value,
    _to_sma,
    _bar_to_ms,
    _is_local_cache_stale,
)


class RollTerminalQtWindowHelperTests(QtWidgetTestCase):

    def test_split_annotation_key_supports_standard_triplet(self) -> None:
        self.assertEqual(
            _split_annotation_key("api1|BTC-USDT-SWAP|1H"),
            ("api1", "BTC-USDT-SWAP", "1H"),
        )

    def test_build_annotation_key_normalizes_symbol(self) -> None:
        self.assertEqual(
            _build_annotation_key("api1", "btc-usdt-swap", "1H"),
            "api1|BTC-USDT-SWAP|1H",
        )

    def test_safe_text_normalizes_empty_values(self) -> None:
        for func in (line_safe_text, smart_safe_text, auto_safe_text):
            self.assertEqual(func(None), "-")
            self.assertEqual(func(""), "-")
            self.assertEqual(func("  ok  "), "ok")

    def test_smart_order_window_uses_compact_layout_defaults(self) -> None:
        self.assertEqual(SMART_ORDER_COMPACT_ROOT_MARGINS, (10, 10, 10, 10))
        self.assertEqual(SMART_ORDER_COMPACT_SPLITTER_SIZES, (560, 1000))
        self.assertLessEqual(SMART_ORDER_TASK_DETAIL_HEIGHT, 82)
        self.assertLessEqual(SMART_ORDER_LOG_MIN_HEIGHT, 132)

    def test_compute_rr_target_supports_long_and_short(self) -> None:
        self.assertEqual(
            _compute_rr_target("long", Decimal("100"), Decimal("95"), Decimal("2")),
            Decimal("110"),
        )
        self.assertEqual(
            _compute_rr_target("short", Decimal("100"), Decimal("105"), Decimal("2")),
            Decimal("90"),
        )

    def test_line_trading_desk_tool_actions_match_expected_order(self) -> None:
        self.assertEqual(
            [value for _label, value in LINE_TRADING_DESK_TOOL_ACTIONS],
            [
                "refresh",
                "reset",
                "zoom_range",
                "line",
                "horizontal",
                "stop",
                "rr_long",
                "rr_short",
                "clear",
                "open_long",
                "open_short",
            ],
        )

    def test_line_trading_refresh_syncs_current_session_once(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_sync_current_session_views", autospec=True) as sync_mock,
        ):
            window = LineTradingQtWindow()
            try:
                sync_mock.reset_mock()

                window.refresh_entries()

                self.assertEqual(sync_mock.call_count, 1)
                self.assertEqual(window._selected_session_key, "api1|BTC-USDT-SWAP|1H")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_account_tables_show_placeholder_rows(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                self.assertEqual(window._positions_table.rowCount(), 1)
                self.assertEqual(window._positions_table.item(0, 0).text(), "账户数据")
                self.assertEqual(window._current_orders_table.item(0, 4).text(), "未加载")
                self.assertEqual(window._order_history_table.item(0, 5).text(), "未加载")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_close_qty_unit_updates_with_position_selection(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        position_btc = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avg_price=Decimal("60000"),
            mark_price=Decimal("61000"),
            upl=Decimal("1000"),
        )
        position_eth = SimpleNamespace(
            inst_id="ETH-USDT-SWAP",
            pos_side="short",
            position=Decimal("2"),
            avg_price=Decimal("3000"),
            mark_price=Decimal("3100"),
            upl=Decimal("80"),
        )
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._apply_account_snapshot([position_btc, position_eth], [], [])
                self.assertEqual(window._position_close_qty_unit_label.text(), "BTC")

                window._positions_table.selectRow(1)
                self.assertEqual(window._position_close_qty_unit_label.text(), "ETH")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_close_selected_position_falls_back_to_single_row(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                position = SimpleNamespace(
                    inst_id="BTC-USDT-SWAP",
                    pos_side="long",
                    position=Decimal("1"),
                    avg_price=Decimal("60000"),
                    mark_price=Decimal("61000"),
                    upl=Decimal("1000"),
                    mgn_mode="cross",
                )
                window._apply_account_snapshot([position], [], [])
                self.assertIs(window._selected_position(), position)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_account_panel_uses_chinese_action_labels(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                button_texts = {button.text() for button in window.findChildren(QPushButton)}
                self.assertIn("市价平仓选中", button_texts)
                self.assertIn("挂买一/卖一平仓", button_texts)
                self.assertIn("撤销委托", button_texts)
                self.assertIn("切换选中锁定", button_texts)
                self.assertIn("删除选中", button_texts)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_window_uses_chinese_toolbar_and_tabs(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                button_texts = {button.text() for button in window.findChildren(QPushButton)}
                self.assertIn("重置视图", button_texts)
                self.assertIn("区间放大", button_texts)
                self.assertIn("趋势线", button_texts)
                self.assertIn("水平射线", button_texts)
                self.assertIn("止损线", button_texts)
                self.assertIn("盈亏比·多", button_texts)
                self.assertIn("盈亏比·空", button_texts)
                self.assertIn("清空线", button_texts)
                self.assertIn("开多", button_texts)
                self.assertIn("开空", button_texts)
                self.assertTrue(window._rr_fee_offset_check.isChecked())
                self.assertEqual(window._account_tabs.tabText(0), "当前持仓")
                self.assertEqual(window._account_tabs.tabText(1), "当前委托")
                self.assertEqual(window._account_tabs.tabText(2), "历史委托")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_account_tables_render_loaded_symbol_rows(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        fake_client = SimpleNamespace(
            get_positions=lambda credentials, **kwargs: [
                SimpleNamespace(
                    inst_id="BTC-USDT-SWAP",
                    pos_side="long",
                    position=Decimal("1"),
                    avg_price=Decimal("60000"),
                    mark_price=Decimal("61000"),
                    upl=Decimal("1000"),
                )
            ],
            get_pending_orders=lambda credentials, **kwargs: [
                SimpleNamespace(
                    inst_id="BTC-USDT-SWAP",
                    side="buy",
                    price=Decimal("60000"),
                    size=Decimal("1"),
                    state="live",
                    order_id="oid-1",
                )
            ],
            get_order_history=lambda credentials, **kwargs: [
                SimpleNamespace(
                    update_time=1_720_000_000_000,
                    inst_id="BTC-USDT-SWAP",
                    side="sell",
                    price=Decimal("62000"),
                    size=Decimal("1"),
                    state="filled",
                )
            ],
        )
        with (
            patch(
                "roll_terminal_qt.line_trading_window.load_profile_snapshots",
                return_value=(
                    {
                        "api1": {
                            "api_key": "k",
                            "secret_key": "s",
                            "passphrase": "p",
                            "environment": "demo",
                        }
                    },
                    "api1",
                ),
            ),
            patch("roll_terminal_qt.line_trading_window.load_notification_snapshot", return_value={}),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.ensure_profile_unlocked", return_value=True),
            patch("roll_terminal_qt.line_trading_window._shared_client", return_value=fake_client),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch.object(
                LineTradingQtWindow,
                "_start_background_action",
                autospec=True,
                side_effect=lambda self, *, task_name, worker, on_success=None, on_error=None: on_success(worker()),
            ),
        ):
            window = LineTradingQtWindow()
            try:
                self.assertEqual(window._positions_table.item(0, 0).text(), "BTC-USDT-SWAP")
                self.assertEqual(window._positions_table.item(0, 1).text(), "long")
                self.assertEqual(window._current_orders_table.item(0, 0).text(), "BTC-USDT-SWAP")
                self.assertEqual(window._current_orders_table.item(0, 4).text(), "live")
                self.assertEqual(window._order_history_table.item(0, 1).text(), "BTC-USDT-SWAP")
                self.assertEqual(window._order_history_table.item(0, 5).text(), "filled")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_account_load_dispatches_to_background_action(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_session_symbol", return_value="BTC-USDT-SWAP"),
                ):
                    window._populate_account_data()

                self.assertEqual(action_mock.call_args.kwargs["task_name"], "load-account-data")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_background_action_starts_daemon_thread(self) -> None:
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value={}),
            patch("roll_terminal_qt.line_trading_window.threading.Thread") as thread_cls,
        ):
            thread_instance = thread_cls.return_value
            window = LineTradingQtWindow()
            try:
                window._start_background_action(
                    task_name="unit-test",
                    worker=lambda: None,
                )

                kwargs = thread_cls.call_args.kwargs
                self.assertEqual(kwargs["name"], "qt-line-trading-unit-test")
                self.assertTrue(kwargs["daemon"])
                thread_instance.start.assert_called_once()
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_workbench_log_writes_line_desk_file(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.current_log_timestamp", return_value="07-05 09:30:00"),
            patch("roll_terminal_qt.line_trading_window.append_line_desk_log_line") as append_log_mock,
        ):
            window = LineTradingQtWindow()
            try:
                window._selected_session_key = "api1|BTC-USDT-SWAP|1H"
                append_log_mock.reset_mock()
                window._append_workbench_log("test message")

                append_log_mock.assert_called_once_with("[07-05 09:30:00] BTC-USDT-SWAP | test message")
                self.assertIn("BTC-USDT-SWAP | test message", window._workbench_log.toPlainText())
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_rr_created_appends_rr_payload(self) -> None:
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        rr_annotation = SimpleNamespace(
            rr_id="",
            side="long",
            bar_entry=100.0,
            bar_stop=100.0,
            price_entry=Decimal("400"),
            price_stop=Decimal("120"),
            price_tp=Decimal("960"),
            r_multiple=Decimal("2"),
            locked=False,
        )
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._selected_session_key = "api1|BTC-USDT-SWAP|1H"

                window._on_chart_rr_created(rr_annotation)

                rr_items = window._entries["api1|BTC-USDT-SWAP|1H"]["rr"]
                self.assertEqual(len(rr_items), 1)
                self.assertEqual(rr_items[0]["side"], "long")
                self.assertEqual(rr_items[0]["price_tp"], "960")
                self.assertEqual(window._selected_rr_index, 0)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_ray_trigger_selection_syncs_line_form(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                        "desk_ray_triggered": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._ray_trigger_table.selectRow(0)
                window._on_ray_trigger_selected()

                self.assertEqual(window._selected_line_index, 0)
                self.assertEqual(window._line_table.currentRow(), 0)
                self.assertEqual(window._line_label_edit.text(), "L1")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_line_selected_syncs_line_tables(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._chart_view.lineSelected.emit(0)

                self.assertEqual(window._selected_line_index, 0)
                self.assertEqual(window._line_table.currentRow(), 0)
                self.assertEqual(window._ray_trigger_table.currentRow(), 0)
                self.assertEqual(window._line_price_a_edit.text(), "100")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_rr_selected_syncs_rr_tables(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._chart_view.rrSelected.emit(0)

                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_table.currentRow(), 0)
                self.assertEqual(window._rr_action_table.currentRow(), 0)
                self.assertEqual(window._rr_entry_edit.text(), "100")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_selection_updates_chart_view_state(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
        ):
            window = LineTradingQtWindow()
            try:
                window._chart_view.lineSelected.emit(0)
                self.assertEqual(window._chart_view._selected_line_index, 0)
                self.assertEqual(window._chart_view._selected_rr_index, -1)

                window._chart_view.rrSelected.emit(0)
                self.assertEqual(window._chart_view._selected_line_index, -1)
                self.assertEqual(window._chart_view._selected_rr_index, 0)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_rr_updated_persists_adjusted_payload(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "long",
                        "price_entry": "100",
                        "price_stop": "95",
                        "price_tp": "110",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "bar_stop": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                annotation = RiskRewardAnnotation(
                    rr_id="rr-1",
                    side="long",
                    bar_entry=3,
                    bar_stop=3,
                    price_entry=Decimal("100"),
                    price_stop=Decimal("90"),
                    price_tp=Decimal("120"),
                    r_multiple=Decimal("2"),
                    locked=False,
                )
                with patch.object(window, "_save_entries", return_value=None):
                    window._chart_view.rrUpdated.emit(0, annotation)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["rr"][0]
                self.assertEqual(payload["price_stop"], "90")
                self.assertEqual(payload["price_tp"], "120")
                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_stop_edit.text(), "90")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_line_updated_persists_adjusted_payload(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                        "desk_ray_triggered": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                annotation = LineAnnotation(
                    kind="horizontal",
                    label="L1",
                    bar_a=1,
                    bar_b=2,
                    price_a=Decimal("105"),
                    price_b=Decimal("105"),
                    desk_ray_action="notify",
                    desk_ray_triggered=False,
                    locked=False,
                )
                with patch.object(window, "_save_entries", return_value=None):
                    window._chart_view.lineUpdated.emit(0, annotation)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]
                self.assertEqual(payload["price_a"], "105")
                self.assertEqual(payload["price_b"], "105")
                self.assertEqual(window._selected_line_index, 0)
                self.assertEqual(window._line_price_a_edit.text(), "105")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_line_selected_clears_rr_selection(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._chart_view.rrSelected.emit(0)
                window._chart_view.lineSelected.emit(0)

                self.assertEqual(window._selected_line_index, 0)
                self.assertEqual(window._selected_rr_index, -1)
                self.assertEqual(window._rr_table.currentRow(), -1)
                self.assertEqual(window._rr_action_table.currentRow(), -1)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_chart_rr_selected_clears_line_selection(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._chart_view.lineSelected.emit(0)
                window._chart_view.rrSelected.emit(0)

                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._selected_line_index, -1)
                self.assertEqual(window._line_table.currentRow(), -1)
                self.assertEqual(window._ray_trigger_table.currentRow(), -1)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_toggle_selected_ray_lock_updates_payload(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._selected_line_index = 0
                with patch.object(window, "_save_entries", return_value=None):
                    window._toggle_selected_ray_lock()

                self.assertTrue(window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]["locked"])
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_ray_double_click_updates_horizontal_prices(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.QInputDialog.getText", return_value=("123.45", True)),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_get_instrument", return_value=None),
                    patch.object(window, "_save_entries", return_value=None),
                ):
                    window._on_ray_trigger_cell_double_clicked(0, 2)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]
                self.assertEqual(payload["price_a"], "123.45")
                self.assertEqual(payload["price_b"], "123.45")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_ray_double_click_cancel_keeps_prices(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "line",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "105",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.QInputDialog.getText", return_value=("", False)),
        ):
            window = LineTradingQtWindow()
            try:
                with patch.object(window, "_save_entries", return_value=None) as save_mock:
                    window._on_ray_trigger_cell_double_clicked(0, 3)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]
                self.assertEqual(payload["price_a"], "100")
                self.assertEqual(payload["price_b"], "105")
                save_mock.assert_not_called()
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_ray_double_click_locked_item_is_rejected(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": True,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_show_info", return_value=None) as info_mock,
                    patch.object(window, "_show_error", return_value=None) as error_mock,
                    patch.object(window, "_save_entries", return_value=None) as save_mock,
                    patch("roll_terminal_qt.line_trading_window.QInputDialog.getText") as dialog_mock,
                ):
                    window._on_ray_trigger_cell_double_clicked(0, 2)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]
                self.assertEqual(payload["price_a"], "100")
                self.assertEqual(payload["price_b"], "100")
                info_mock.assert_called_once()
                error_mock.assert_not_called()
                save_mock.assert_not_called()
                dialog_mock.assert_not_called()
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_ray_double_click_snaps_price_to_tick(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [
                    {
                        "kind": "horizontal",
                        "label": "L1",
                        "desk_ray_action": "notify",
                        "price_a": "100",
                        "price_b": "100",
                        "bar_a": 1,
                        "bar_b": 2,
                        "locked": False,
                    }
                ],
                "rr": [],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.QInputDialog.getText", return_value=("123.4", True)),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_get_instrument", return_value=SimpleNamespace(tick_size=Decimal("0.5"))),
                    patch.object(window, "_save_entries", return_value=None),
                ):
                    window._on_ray_trigger_cell_double_clicked(0, 2)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["lines"][0]
                self.assertEqual(payload["price_a"], "123.5")
                self.assertEqual(payload["price_b"], "123.5")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_rr_action_selection_syncs_rr_form(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._rr_action_table.selectRow(0)
                window._on_rr_action_selected()

                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_table.currentRow(), 0)
                self.assertEqual(window._rr_entry_edit.text(), "100")
                self.assertEqual(window._rr_stop_edit.text(), "105")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_toggle_selected_rr_lock_updates_payload(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                window._selected_rr_index = 0
                with patch.object(window, "_save_entries", return_value=None):
                    window._toggle_selected_rr_lock()

                self.assertTrue(window._entries["api1|BTC-USDT-SWAP|1H"]["rr"][0]["locked"])
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_rr_double_click_updates_entry_and_tp(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "long",
                        "price_entry": "100",
                        "price_stop": "95",
                        "price_tp": "110",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.QInputDialog.getText", return_value=("101", True)),
        ):
            window = LineTradingQtWindow()
            try:
                with patch.object(window, "_save_entries", return_value=None):
                    window._on_rr_action_cell_double_clicked(0, 2)

                payload = window._entries["api1|BTC-USDT-SWAP|1H"]["rr"][0]
                self.assertEqual(payload["price_entry"], "101")
                self.assertEqual(
                    payload["price_tp"],
                    "113",
                )
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_rr_double_click_cancel_keeps_values(self) -> None:
        entries = {
            "api1|BTC-USDT-SWAP|1H": {
                "lines": [],
                "rr": [
                    {
                        "rr_id": "rr-1",
                        "side": "short",
                        "price_entry": "100",
                        "price_stop": "105",
                        "price_tp": "90",
                        "r_multiple": "2",
                        "bar_entry": 3,
                        "locked": False,
                    }
                ],
            }
        }
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_reload_chart", return_value=None),
            patch("roll_terminal_qt.line_trading_window.QInputDialog.getText", return_value=("", False)),
        ):
            window = LineTradingQtWindow()
            try:
                before = dict(window._entries["api1|BTC-USDT-SWAP|1H"]["rr"][0])
                with patch.object(window, "_save_entries", return_value=None) as save_mock:
                    window._on_rr_action_cell_double_clicked(0, 2)

                self.assertEqual(window._entries["api1|BTC-USDT-SWAP|1H"]["rr"][0], before)
                save_mock.assert_not_called()
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_rr_submit_dispatches_to_background_action(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        instrument = SimpleNamespace()
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.build_rr_order_intent", return_value={
                "entry_price": Decimal("100"),
                "stop_price": Decimal("95"),
                "take_profit": Decimal("110"),
            }),
            patch("roll_terminal_qt.line_trading_window.determine_order_size", return_value=Decimal("1")),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_rr_payload", return_value={
                        "side": "long",
                        "price_entry": "100",
                        "price_stop": "95",
                        "price_tp": "110",
                    }),
                    patch.object(window, "_session_symbol", return_value="BTC-USDT-SWAP"),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch.object(window, "_prompt_positive_decimal", return_value=Decimal("100")),
                ):
                    window._submit_rr_order_from_selected("limit")

                self.assertEqual(action_mock.call_args.kwargs["task_name"], "submit-rr-order")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_rr_submit_applies_fee_offset_to_take_profit(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        instrument = SimpleNamespace(tick_size=Decimal("0.0001"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch(
                "roll_terminal_qt.line_trading_window.build_rr_order_intent",
                return_value={
                    "entry_price": Decimal("100"),
                    "stop_price": Decimal("95"),
                    "take_profit": Decimal("110"),
                },
            ),
            patch("roll_terminal_qt.line_trading_window.determine_order_size", return_value=Decimal("1")),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(
                LineTradingQtWindow,
                "_start_background_action",
                autospec=True,
                side_effect=lambda self, *, task_name, worker, on_success=None, on_error=None: on_success(worker()),
            ),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_rr_payload", return_value={
                        "side": "long",
                        "price_entry": "100",
                        "price_stop": "95",
                        "price_tp": "110",
                    }),
                    patch.object(window, "_session_symbol", return_value="BTC-USDT-SWAP"),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch.object(window, "_prompt_positive_decimal", return_value=Decimal("100")),
                    patch.object(window, "_populate_account_data", return_value=None),
                    patch.object(window, "_append_workbench_log", return_value=None),
                    patch.object(
                        window._client,
                        "place_limit_order",
                        return_value=SimpleNamespace(ord_id="ord-1", cl_ord_id=None),
                    ) as place_limit_order,
                ):
                    window._submit_rr_order_from_selected("limit")

                plan = place_limit_order.call_args.args[2]
                self.assertEqual(plan.take_profit, Decimal("110.0720"))
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_dispatches_to_background_action(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("1"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                ):
                    window._flatten_selected_position("market")

                self.assertEqual(action_mock.call_args.kwargs["task_name"], "flatten-position")
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_rejects_close_size_above_available(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window, "_show_error", autospec=True) as error_mock,
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                ):
                    window._position_close_qty_edit.setText("1")
                    window._flatten_selected_position("market")

                    self.assertIsNone(action_mock.call_args)
                    self.assertTrue(error_mock.called)
                    error_message = str(error_mock.call_args.args[1] if len(error_mock.call_args.args) > 1 else "")
                    self.assertIn("不能超过当前可平仓", error_message)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_confirmation_contains_key_fields(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_append_workbench_log", return_value=None),
            patch.object(LineTradingQtWindow, "_set_status", return_value=None),
        ):
            window = LineTradingQtWindow()
            captured = {}

            def _capture_question(parent, title, message) -> QMessageBox.StandardButton:
                captured["title"] = title
                captured["message"] = str(message)
                return QMessageBox.StandardButton.Yes

            try:
                with (
                    patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch("roll_terminal_qt.line_trading_window.QMessageBox.question", side_effect=_capture_question),
                ):
                    window._flatten_selected_position("market")

                    self.assertEqual(captured.get("title"), "确认平仓")
                    self.assertEqual(action_mock.call_args.kwargs["task_name"], "flatten-position")
                    message = captured.get("message", "")
                    self.assertIn("BTC-USDT-SWAP", message)
                    self.assertIn("数量：", message)
                    self.assertIn("0.5", message)
                    self.assertIn("市价平仓", message)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_best_quote_flatten_uses_price_and_mode_label(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_append_workbench_log", return_value=None),
            patch.object(LineTradingQtWindow, "_set_status", return_value=None),
        ):
            window = LineTradingQtWindow()
            captured = {}

            def _capture_question(parent, title, message) -> QMessageBox.StandardButton:
                captured["title"] = title
                captured["message"] = str(message)
                return QMessageBox.StandardButton.Yes

            try:
                with (
                    patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch("roll_terminal_qt.line_trading_window.QMessageBox.question", side_effect=_capture_question),
                    patch.object(window._client, "place_simple_order", return_value=SimpleNamespace(ord_id="ord-1", cl_ord_id=None)) as place_order,
                    patch.object(window, "_resolve_best_quote_flatten_price", return_value=Decimal("61234.56")),
                ):
                    window._position_close_qty_edit.setText("")
                    window._flatten_selected_position("best_quote")

                    self.assertEqual(captured.get("title"), "确认平仓")
                    self.assertEqual(action_mock.call_args.kwargs["task_name"], "flatten-position")
                    self.assertIn("挂买一/卖一平仓", captured.get("message", ""))

                    worker = action_mock.call_args.kwargs["worker"]
                    worker()

                    self.assertEqual(place_order.call_count, 1)
                    args = place_order.call_args.kwargs
                    self.assertEqual(args["ord_type"], "limit")
                    self.assertEqual(args["price"], Decimal("61234.56"))
                    self.assertEqual(args["size"], Decimal("0.5"))
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_coin_input_converts_to_contracts(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="long",
            position=Decimal("30"),
            avail_position=Decimal("30"),
            mark_price=Decimal("50000"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(
            lot_size=Decimal("0.0001"),
            min_size=Decimal("0.0001"),
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
            tick_size=Decimal("0.1"),
        )
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
            patch.object(LineTradingQtWindow, "_append_workbench_log", return_value=None),
            patch.object(LineTradingQtWindow, "_set_status", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch.object(window._client, "place_simple_order", return_value=SimpleNamespace(ord_id="ord-1", cl_ord_id=None)) as place_order,
                ):
                    window._position_close_qty_edit.setText("0.2")
                    window._flatten_selected_position("market")

                    worker = action_mock.call_args.kwargs["worker"]
                    worker()

                    self.assertEqual(place_order.call_count, 1)
                    self.assertEqual(place_order.call_args.kwargs["size"], Decimal("20"))
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_coin_input_exceeds_available_after_convert(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            pos_side="long",
            position=Decimal("10"),
            avail_position=Decimal("10"),
            mark_price=Decimal("50000"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(
            lot_size=Decimal("0.0001"),
            min_size=Decimal("0.0001"),
            ct_val=Decimal("0.01"),
            ct_val_ccy="BTC",
            tick_size=Decimal("0.1"),
        )
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window, "_show_error", autospec=True) as error_mock,
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                ):
                    window._position_close_qty_edit.setText("0.2")
                    window._flatten_selected_position("market")

                    self.assertIsNone(action_mock.call_args)
                    self.assertTrue(error_mock.called)
                    self.assertIn("不能超过当前可平仓", str(error_mock.call_args.args[1] if len(error_mock.call_args.args) > 1 else ""))
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_best_quote_flatten_rejects_price_fetch_failure(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"), tick_size=Decimal("0.1"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch.object(LineTradingQtWindow, "_append_workbench_log", return_value=None),
            patch.object(LineTradingQtWindow, "_set_status", return_value=None),
            patch.object(LineTradingQtWindow, "_selected_position", return_value=position),
            patch.object(LineTradingQtWindow, "_build_runtime", return_value=runtime),
            patch.object(LineTradingQtWindow, "_get_instrument", return_value=instrument),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_show_error", return_value=None) as show_error,
            patch.object(window := LineTradingQtWindow(), "_resolve_best_quote_flatten_price", side_effect=RuntimeError("bid/ask unavailable")),
        ):
            try:
                with (
                    patch.object(window, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window._client, "place_simple_order", return_value=SimpleNamespace(ord_id="ord-1", cl_ord_id=None)) as place_order,
                ):
                    window._flatten_selected_position("best_quote")

                    self.assertEqual(action_mock.call_args.kwargs["task_name"], "flatten-position")
                    worker = action_mock.call_args.kwargs["worker"]
                    self.assertRaises(RuntimeError, worker)
                    self.assertEqual(place_order.call_count, 0)
                    self.assertFalse(show_error.called)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_empty_input_uses_available_size(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        position = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            pos_side="long",
            position=Decimal("1"),
            avail_position=Decimal("0.5"),
            mgn_mode="cross",
        )
        instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"), tick_size=Decimal("0.1"))
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
            patch.object(LineTradingQtWindow, "_append_workbench_log", return_value=None),
            patch.object(LineTradingQtWindow, "_set_status", return_value=None),
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_position", return_value=position),
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                    patch.object(window._client, "place_simple_order", return_value=SimpleNamespace(ord_id="ord-1", cl_ord_id=None)) as place_order,
                ):
                    window._position_close_qty_edit.setText("")
                    window._flatten_selected_position("market")

                    worker = action_mock.call_args.kwargs["worker"]
                    worker()

                    self.assertEqual(place_order.call_count, 1)
                    self.assertEqual(place_order.call_args.kwargs["size"], Decimal("0.5"))
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_flatten_requires_selection_when_multiple_positions(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
        ):
            window = LineTradingQtWindow()
            try:
                instrument = SimpleNamespace(lot_size=Decimal("0.0001"), min_size=Decimal("0.0001"))
                position_1 = SimpleNamespace(
                    inst_id="BTC-USDT-SWAP",
                    pos_side="long",
                    position=Decimal("1"),
                    avail_position=Decimal("1"),
                    mgn_mode="cross",
                )
                position_2 = SimpleNamespace(
                    inst_id="ETH-USDT-SWAP",
                    pos_side="short",
                    position=Decimal("2"),
                    avail_position=Decimal("2"),
                    mgn_mode="cross",
                )
                window._positions_table.selectRow(-1)
                window._positions_table.clearSelection()
                window._apply_account_snapshot([position_1, position_2], [], [])
                with (
                    patch.object(window, "_start_background_action", autospec=True) as action_mock,
                    patch.object(window, "_show_error", autospec=True) as error_mock,
                    patch.object(window, "_build_runtime", return_value=runtime),
                    patch.object(window, "_get_instrument", return_value=instrument),
                ):
                    window._flatten_selected_position("market")
                    self.assertIsNone(action_mock.call_args)
                    self.assertTrue(error_mock.called)
                    error_msg = str(error_mock.call_args.args[1] if len(error_mock.call_args.args) > 1 else "")
                    self.assertIn("请先选中一条持仓", error_msg)
            finally:
                self.__class__.dispose_widget(window)

    def test_line_trading_cancel_dispatches_to_background_action(self) -> None:
        runtime = SimpleNamespace(
            credentials=object(),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
        )
        order = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            side="buy",
            state="live",
            order_id="oid-1",
            client_order_id=None,
            algo_id=None,
            algo_client_order_id=None,
            source_kind="rest",
        )
        entries = {"api1|BTC-USDT-SWAP|1H": {"lines": [], "rr": []}}
        with (
            patch("roll_terminal_qt.line_trading_window.load_profile_snapshots", return_value=({}, "")),
            patch("roll_terminal_qt.line_trading_window.load_line_trading_desk_annotations_entries", return_value=entries),
            patch("roll_terminal_qt.line_trading_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
            patch.object(LineTradingQtWindow, "_start_background_action", autospec=True) as action_mock,
        ):
            window = LineTradingQtWindow()
            try:
                with (
                    patch.object(window, "_selected_pending_order", return_value=order),
                    patch.object(window, "_build_runtime", return_value=runtime),
                ):
                    window._cancel_selected_pending_order()

                self.assertEqual(action_mock.call_args.kwargs["task_name"], "cancel-order")
            finally:
                self.__class__.dispose_widget(window)

    def test_profile_requires_password_detects_protected_payload(self) -> None:
        payload = build_profile_switch_password_snapshot("secret-1")
        self.assertTrue(profile_requires_password("api1", {"api1": payload}))
        self.assertFalse(profile_requires_password("api2", {"api1": payload}))

    def test_bar_to_ms_supports_mh_and_d_units(self) -> None:
        self.assertEqual(_bar_to_ms("3m"), 180_000)
        self.assertEqual(_bar_to_ms("4H"), 14_400_000)
        self.assertEqual(_bar_to_ms("1D"), 86_400_000)

    def test_secondary_layout_button_text_shows_next_layout_mode(self) -> None:
        self.assertEqual(_next_secondary_layout_button_text("vertical"), "左右分屏")
        self.assertEqual(_next_secondary_layout_button_text("horizontal"), "上下分屏")

    def test_secondary_chart_kind_button_text_shows_next_chart_kind(self) -> None:
        self.assertEqual(_next_secondary_chart_kind_button_text("kline"), "BTC波动率")
        self.assertEqual(_next_secondary_chart_kind_button_text("volatility"), "副图K线")

    def test_kline_debug_log_tolerates_missing_stdout(self) -> None:
        with patch("sys.stdout", None):
            _debug_log("[kline] test")

    def test_kline_symbol_input_uses_bounded_header_width(self) -> None:
        window = KlineAnalysisWindow()
        try:
            self.assertLessEqual(window._symbol_input.maximumWidth(), 420)
            self.assertEqual(
                window._symbol_input.sizePolicy().horizontalPolicy(),
                QSizePolicy.Policy.Preferred,
            )
        finally:
            self.__class__.dispose_widget(window)

    def test_is_local_cache_stale_by_age(self) -> None:
        now_ms = 1_600_000_000_000
        candle = SimpleNamespace(ts=1_600_000_000_000 - 61_000, confirmed=True)
        self.assertTrue(_is_local_cache_stale([candle], "1m", now_ms=now_ms))

    def test_is_local_cache_stale_by_gap(self) -> None:
        now_ms = 1_600_000_000_000
        candles = [
            SimpleNamespace(ts=1_600_000_000_000 - 300_000, confirmed=True),
            SimpleNamespace(ts=1_600_000_000_000 - 120_000, confirmed=True),
            SimpleNamespace(ts=1_600_000_000_000, confirmed=True),
        ]
        # 180s gap should be treated as stale for 1m cache.
        self.assertTrue(_is_local_cache_stale(candles, "1m", now_ms=now_ms + 30_000))

    def test_moving_average_helpers(self) -> None:
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.assertEqual(
            _to_sma(closes, 3),
            [1.0, 1.5, 2.0, 3.0, 4.0],
        )

    def test_deribit_overlay_moving_average_series_keeps_length_and_ma50_warmup(self) -> None:
        closes = [Decimal(str(value)) for value in range(1, 61)]

        ema15, ma50 = _build_moving_average_series(closes)

        self.assertEqual(len(ema15), 60)
        self.assertEqual(len(ma50), 60)
        self.assertEqual(ema15[0], Decimal("1"))
        self.assertTrue(all(value is None for value in ma50[:49]))
        self.assertEqual(ma50[49], Decimal("25.5"))
        self.assertEqual(ma50[-1], Decimal("35.5"))

    def test_attach_series_to_axes_once_skips_existing_bindings(self) -> None:
        chart = QChart()
        series = QLineSeries()
        chart.addSeries(series)
        axis_x = QValueAxis()
        axis_y = QValueAxis()
        chart.addAxis(axis_x, Qt.AlignmentFlag.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignmentFlag.AlignLeft)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)

        _attach_series_to_axes_once(series, axis_x, axis_y)

        self.assertEqual(len(series.attachedAxes()), 2)

    def test_deribit_window_enables_moving_average_overlay_by_default(self) -> None:
        window = DeribitVolatilityQtWindow()
        try:
            self.assertTrue(window._moving_average_check.isChecked())
        finally:
            self.__class__.dispose_widget(window)

    def test_deribit_window_uses_compact_top_action_buttons(self) -> None:
        window = DeribitVolatilityQtWindow()
        try:
            buttons = {button.text(): button for button in window.findChildren(QPushButton)}
            self.assertEqual(buttons["重置视图"].minimumHeight(), 24)
            self.assertEqual(buttons["立即刷新"].minimumHeight(), 24)
            self.assertEqual(buttons["导出CSV"].minimumHeight(), 24)
        finally:
            self.__class__.dispose_widget(window)

    def test_qt_window_unittest_process_exits_zero(self) -> None:
        script = textwrap.dedent(
            """
            import os
            import sys
            import unittest

            os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
            import tests.test_roll_terminal_qt_windows as target_module

            suite = unittest.defaultTestLoader.loadTestsFromName(
                "RollTerminalQtWindowHelperTests.test_deribit_window_uses_compact_top_action_buttons",
                target_module,
            )
            result = unittest.TextTestRunner(verbosity=2).run(suite)
            sys.exit(0 if result.wasSuccessful() else 1)
            """
        )
        env = dict(os.environ)
        env["QT_QPA_PLATFORM"] = "offscreen"
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(Path(__file__).resolve().parents[1]),
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"returncode={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}",
        )

    def test_native_chart_backend_preferred_when_gpu_disabled(self) -> None:
        with patch.dict(os.environ, {"QTWEBENGINE_DISABLE_GPU": "1"}, clear=False):
            self.assertTrue(_prefer_native_chart_backend())

    def test_default_native_visible_range_focuses_latest_bars(self) -> None:
        self.assertEqual(_default_native_visible_range(1200), (960.0, 1199.0))
        self.assertEqual(_default_native_visible_range(100), (0.0, 99.0))

    def test_default_kline_splitter_sizes_follow_requested_ratio(self) -> None:
        left_width, right_width = _default_kline_splitter_sizes(2048)
        self.assertEqual((left_width, right_width), (225, 1823))

    def test_default_chart_stack_splitter_sizes_preserve_top_bias(self) -> None:
        top_height, bottom_height = _default_chart_stack_splitter_sizes(900)
        self.assertEqual((top_height, bottom_height), (279, 621))
        self.assertLess(top_height, bottom_height)

    def test_kline_overlay_line_widths_keep_sma_more_prominent(self) -> None:
        self.assertEqual(_EMA15_LINE_WIDTH, 2)
        self.assertEqual(_SMA50_LINE_WIDTH, 3)
        self.assertGreater(_SMA50_LINE_WIDTH, _EMA15_LINE_WIDTH)

    def test_default_single_chart_period_is_4h(self) -> None:
        self.assertEqual(_DEFAULT_SINGLE_CHART_PERIOD, "4H")

    def test_default_dual_chart_periods_are_day_and_4h(self) -> None:
        self.assertEqual(_DEFAULT_DUAL_PRIMARY_PERIOD, "1D")
        self.assertEqual(_DEFAULT_DUAL_SECONDARY_PERIOD, "4H")

    def test_line_drag_helpers_order_and_shift_trend_endpoints(self) -> None:
        self.assertEqual(
            _ordered_trend_endpoints(200, 20.0, 100, 10.0),
            (100, 10.0, 200, 20.0),
        )
        moved = _apply_drag_to_line_rule(
            {
                "kind": "trend",
                "time_a": 100,
                "price_a": 10.0,
                "time_b": 200,
                "price_b": 20.0,
            },
            drag_mode="move",
            candle_time=160,
            price=17.5,
            anchor_line={"time_a": 100, "price_a": 10.0, "time_b": 200, "price_b": 20.0},
            anchor_candle_time=150,
            anchor_price=15.0,
        )
        self.assertEqual((moved["time_a"], moved["time_b"]), (110, 210))
        self.assertEqual((moved["price_a"], moved["price_b"]), (12.5, 22.5))

    def test_line_drag_endpoint_update_reorders_reversed_times(self) -> None:
        updated = _apply_drag_to_line_rule(
            {
                "kind": "trend",
                "time_a": 100,
                "price_a": 10.0,
                "time_b": 200,
                "price_b": 20.0,
            },
            drag_mode="endpoint_a",
            candle_time=260,
            price=26.0,
        )
        self.assertEqual((updated["time_a"], updated["time_b"]), (200, 260))
        self.assertEqual((updated["price_a"], updated["price_b"]), (20.0, 26.0))

    def test_line_time_tolerance_scales_with_bars(self) -> None:
        self.assertEqual(_line_time_tolerance_seconds(900_000, bars=3), 2700)

    def test_interaction_cursor_mode_allows_dragging_when_locked(self) -> None:
        self.assertEqual(
            _resolve_interaction_cursor_mode("dragging", interaction_locked=True, draw_mode_enabled=False),
            "dragging",
        )
        self.assertEqual(
            _resolve_interaction_cursor_mode("default", interaction_locked=False, draw_mode_enabled=True),
            "crosshair",
        )

    def test_line_price_tolerance_keeps_endpoints_easier_to_grab(self) -> None:
        endpoint_tolerance = _line_price_tolerance(10_000.0, 60_000.0, emphasis="endpoint")
        body_tolerance = _line_price_tolerance(10_000.0, 60_000.0, emphasis="body")
        self.assertGreater(endpoint_tolerance, body_tolerance)

    def test_line_handle_visual_emphasizes_hovered_endpoint(self) -> None:
        idle = _line_handle_visual("endpoint_a", hovered_drag_mode=None)
        hovered = _line_handle_visual("endpoint_a", hovered_drag_mode="endpoint_a")
        self.assertGreater(hovered["radius"], idle["radius"])
        self.assertNotEqual(hovered["fill"], idle["fill"])

    def test_line_handle_visual_gives_both_handles_move_hint(self) -> None:
        move_hint = _line_handle_visual("endpoint_b", hovered_drag_mode="move")
        idle = _line_handle_visual("endpoint_b", hovered_drag_mode=None)
        self.assertGreater(move_hint["radius"], idle["radius"])
        self.assertEqual(move_hint["inner_fill"], idle["inner_fill"])

    def test_default_native_x_range_reserves_blank_space_on_right(self) -> None:
        display_times = [index * 900_000 for index in range(100)]
        start_x, end_x = _default_native_x_range_with_right_padding(display_times, display_step_ms=900_000)
        self.assertEqual(end_x, float(display_times[-1]) + _native_right_padding_ms(900_000))
        self.assertLess(start_x, end_x)
        self.assertGreater(end_x - display_times[-1], 0.0)

    def test_resolve_candle_time_from_x_value_supports_future_blank_area(self) -> None:
        candles = [
            {"time": 1_700_000_000},
            {"time": 1_700_000_900},
            {"time": 1_700_001_800},
        ]
        display_times = [0, 900_000, 1_800_000]
        candle_time = _resolve_candle_time_from_x_value(
            candles,
            display_times,
            x_value=2_250_000,
            display_step_ms=900_000,
        )
        self.assertEqual(candle_time, 1_700_002_700)

    def test_display_x_for_candle_time_projects_future_line_endpoint(self) -> None:
        candles = [
            {"time": 1_700_000_000},
            {"time": 1_700_000_900},
            {"time": 1_700_001_800},
        ]
        display_times = [0, 900_000, 1_800_000]
        display_x = _display_x_for_candle_time(
            candles,
            display_times,
            candle_time=1_700_003_600,
            display_step_ms=900_000,
        )
        self.assertEqual(display_x, 3_600_000.0)

    def test_kline_auto_refresh_defaults_to_enabled(self) -> None:
        self.assertTrue(_AUTO_REFRESH_DEFAULT_ENABLED)

    def test_build_display_times_ms_compresses_missing_sessions(self) -> None:
        candles = [
            {"time": 1_700_000_000},
            {"time": 1_700_000_900},
            {"time": 1_700_100_000},
        ]
        display_times = _build_display_times_ms(candles, "15m")
        self.assertEqual(len(display_times), 3)
        self.assertEqual(display_times[1] - display_times[0], 900_000)
        self.assertEqual(display_times[2] - display_times[1], 900_000)

    def test_kline_workspace_persistence_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "kline_analysis_workspace.json"
            save_kline_analysis_workspace_entries(
                {
                    "BTC-USDT-SWAP|15m": {
                        "lines": [{"label": "H-01"}],
                        "alerts": {"ma_cross": {"enabled": True}},
                        "events": [{"message": "cross above"}],
                    }
                },
                path=target,
            )
            loaded = load_kline_analysis_workspace_entries(target)
        self.assertIn("BTC-USDT-SWAP|15m", loaded)
        self.assertEqual(loaded["BTC-USDT-SWAP|15m"]["lines"][0]["label"], "H-01")


if __name__ == "__main__":
    unittest.main()
