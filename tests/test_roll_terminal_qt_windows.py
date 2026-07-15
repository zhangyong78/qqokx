from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import textwrap
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtCharts import QChart, QLineSeries, QValueAxis
from PySide6.QtCore import QPointF, QThread, Qt
from PySide6.QtWidgets import QDoubleSpinBox, QLabel, QMessageBox, QPushButton, QSizePolicy, QTabWidget, QWidget
from okx_quant.analysis import ChannelDetectionConfig
from okx_quant.models import Candle
from okx_quant.arbitrage.models import ArbitrageTradeRuntime
from okx_quant.kline_rr_trade import RRTradeLedgerEntry, RRTradeOrderLink, build_rr_trade_plan
from okx_quant.persistence import (
    build_profile_switch_password_snapshot,
    load_kline_analysis_workspace_entries,
    save_kline_analysis_workspace_entries,
)
from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.auto_channel_window import _safe_text as auto_safe_text
from roll_terminal_qt.deribit_volatility_window import (
    DeribitVolatilityQtWindow,
    LinkedCandlestickChartView,
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
from roll_terminal_qt.option_strategy_window import CandlestickChartView
from roll_terminal_qt.perf_metrics import measure_ui_step
from roll_terminal_qt.kline_account_drawer import AccountDrawerLoadThread
from roll_terminal_qt.smart_order_window import (
    SMART_ORDER_COMPACT_ROOT_MARGINS,
    SMART_ORDER_COMPACT_SPLITTER_SIZES,
    SMART_ORDER_LOG_MIN_HEIGHT,
    SMART_ORDER_TASK_DETAIL_HEIGHT,
)
from roll_terminal_qt.kline_analysis_window import (
    KlineAlertSnapshot,
    KlineChartPayload,
    KlineAnalysisWindow,
    KlineDataLoader,
    KLINE_SYMBOL_OPTIONS,
    RRCardDialog,
    _AUTO_REFRESH_DEFAULT_ENABLED,
    _DEFAULT_DUAL_PRIMARY_PERIOD,
    _DEFAULT_DUAL_SECONDARY_PERIOD,
    _DEFAULT_SINGLE_CHART_PERIOD,
    _EMA15_LINE_WIDTH,
    _SMA50_LINE_WIDTH,
    _apply_drag_to_line_rule,
    _build_box_history_overlays,
    _build_box_current_overlay,
    _build_channel_current_overlays,
    _build_rr_overlay_snapshot,
    _extend_history_box_end_index,
    _build_display_times_ms,
    _compute_axis_y_padding,
    _axis_y_label_format,
    _compute_hover_overlay_layout,
    _default_chart_stack_splitter_sizes,
    _default_kline_splitter_sizes,
    _display_x_for_candle_time,
    _debug_log,
    _line_handle_visual,
    _line_time_tolerance_seconds,
    _line_price_tolerance,
    _merge_realtime_candle_payload,
    _next_secondary_chart_kind_button_text,
    _next_secondary_layout_button_text,
    _default_native_x_range_with_right_padding,
    _prefer_native_chart_backend,
    _default_native_visible_range,
    _native_right_padding_ms,
    _ordered_trend_endpoints,
    _reverse_kline_chart_payload,
    _rr_box_end_display_x,
    _rr_ledger_blocks_editing,
    _rr_plan_position_text,
    _resolve_interaction_cursor_mode,
    _resolve_candle_time_from_x_value,
    _slice_chart_payload_tail,
    _to_sma,
    _bar_to_ms,
    _is_local_cache_stale,
    _volatility_currency_for_symbol,
)
import roll_terminal_qt.kline_analysis_window as kline_analysis_module
from roll_terminal_qt.launcher import LauncherWindow
from roll_terminal_qt.ui import RollTerminalWindow
from roll_terminal_qt.workspace_shell import LocalTaskCount


class RollTerminalQtWindowHelperTests(QtWidgetTestCase):

    def test_disabled_kline_research_layers_are_not_computed_in_loader(self) -> None:
        candles = [
            Candle(index * 3_600_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), False)
            for index in range(60)
        ]
        loader = KlineDataLoader(
            request_id=1,
            symbol="BTC-USDT-SWAP",
            period="4H",
            limit=1200,
            workspace_entry={},
        )

        with (
            patch("roll_terminal_qt.kline_analysis_window._build_box_history_overlays") as history_boxes,
            patch("roll_terminal_qt.kline_analysis_window._build_box_current_overlay") as current_box,
            patch("roll_terminal_qt.kline_analysis_window._build_replay_signal_markers") as shape_signals,
        ):
            loader._build_payload(
                candles=candles,
                local_count=len(candles),
                remote_added_count=0,
                has_network_fallback=False,
                local_stale=False,
                include_alerts=False,
            )

        history_boxes.assert_not_called()
        current_box.assert_not_called()
        shape_signals.assert_not_called()

    def test_kline_shutdown_requests_loader_interruption_without_waiting(self) -> None:
        loader = MagicMock()
        loader.isRunning.return_value = True
        app = SimpleNamespace(
            _refresh_timer=MagicMock(),
            _rr_monitor_timer=MagicMock(),
            _realtime_candle_unsubscribe=None,
            _loader=loader,
            _secondary_loader=None,
            _secondary_volatility_loader=None,
            _deferred_chart_render_timer=MagicMock(),
            _layout_refresh_timer=MagicMock(),
        )
        finished: list[bool] = []

        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot") as schedule:
            KlineAnalysisWindow.begin_shutdown(app, lambda: finished.append(True))

        loader.requestInterruption.assert_called_once()
        loader.wait.assert_not_called()
        self.assertEqual(finished, [])
        schedule.assert_called_once()

    def test_current_channel_overlay_reuses_research_snapshot_band(self) -> None:
        class Line:
            def __init__(self, value: str) -> None:
                self._value = Decimal(value)

            def value_at(self, index: int) -> Decimal:
                return self._value

        band = SimpleNamespace(
            start_index=4,
            end_index=9,
            upper_line=Line("101"),
            lower_line=Line("99"),
            label="自动通道",
            outline="#2563eb",
            fill="#dbeafe",
        )
        candles = [Candle(index * 60_000, Decimal("100"), Decimal("102"), Decimal("98"), Decimal("100"), Decimal("1"), False) for index in range(12)]
        config = ChannelDetectionConfig(min_anchor_distance=8, min_channel_bars=18, max_violations=8)

        with patch(
            "roll_terminal_qt.kline_analysis_window.build_auto_channel_live_chart_snapshot",
            return_value=SimpleNamespace(band_overlays=(band,)),
        ) as build_snapshot:
            overlays = _build_channel_current_overlays(candles, config=config)

        self.assertIs(build_snapshot.call_args.kwargs["channel_config"], config)
        self.assertEqual(len(overlays), 1)
        self.assertEqual(overlays[0]["mode"], "current")
        self.assertEqual(overlays[0]["start_index"], 4)
        self.assertEqual(overlays[0]["end_index"], 9)
        self.assertEqual(overlays[0]["upper_start"], 101.0)
        self.assertEqual(overlays[0]["lower_start"], 99.0)

    def test_disabled_auto_channel_does_not_expose_channel_layer(self) -> None:
        check = MagicMock()
        check.isChecked.return_value = False
        app = SimpleNamespace(_auto_channel_check=check)
        payload = SimpleNamespace(channel_overlays=[{"label": "自动通道"}])

        self.assertEqual(KlineAnalysisWindow._visible_channel_overlays(app, payload), [])

    def test_kline_auto_channel_display_controls_default_to_disabled(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}):
            window = KlineAnalysisWindow(embedded=True)
            try:
                self.assertFalse(window._auto_box_check.isChecked())
                self.assertFalse(window._history_box_check.isChecked())
                self.assertFalse(window._auto_channel_check.isChecked())
                self.assertFalse(window._box_breakout_alert_check.isChecked())
                self.assertEqual(window._auto_channel_anchor_spin.value(), 8)
                self.assertEqual(window._auto_channel_min_bars_spin.value(), 18)
                self.assertEqual(window._auto_channel_violations_spin.value(), 8)
            finally:
                self.__class__.dispose_widget(window)

    def test_slicing_chart_payload_rebases_channel_boundaries(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": index, "open": 1, "high": 2, "low": 0, "close": 1, "volume": 1} for index in range(6)],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], raw_candles=[], stats={},
            channel_overlays=[{
                "start_index": 2, "end_index": 5,
                "upper_start": 110.0, "upper_end": 140.0,
                "lower_start": 90.0, "lower_end": 120.0,
            }],
        )

        sliced = _slice_chart_payload_tail(payload, 3)

        self.assertEqual(sliced.channel_overlays[0]["start_index"], 0)
        self.assertEqual(sliced.channel_overlays[0]["upper_start"], 120.0)
        self.assertEqual(sliced.channel_overlays[0]["lower_start"], 100.0)

    def test_roll_workspace_profile_switch_preserves_bound_auto_runtime(self) -> None:
        runtime_api1 = SimpleNamespace(credential_profile_name="api1")
        runtime_api2 = SimpleNamespace(credential_profile_name="api2")
        app = SimpleNamespace(
            _runtime=runtime_api2,
            _auto_enabled=True,
            _auto_task_runtime=runtime_api1,
            _last_profile_name="api1",
            _unlocked_profiles=set(),
            _apply_api_profile=MagicMock(),
        )

        RollTerminalWindow.apply_workspace_profile(app, "api2")

        app._apply_api_profile.assert_called_once_with("api2")
        self.assertIs(RollTerminalWindow._auto_execution_runtime(app), runtime_api1)
        self.assertIs(app._auto_task_runtime, runtime_api1)

    def test_roll_workspace_managed_hides_duplicate_api_controls(self) -> None:
        app = SimpleNamespace(_api_label=MagicMock(), _api=MagicMock())

        RollTerminalWindow.set_workspace_managed(app, True)

        app._api_label.setVisible.assert_called_once_with(False)
        app._api.setVisible.assert_called_once_with(False)

    def test_launcher_groups_hidden_page_tasks_by_api(self) -> None:
        header = MagicMock()
        status_label = MagicMock()
        app = SimpleNamespace(
            _pages={
                "kline": SimpleNamespace(
                    local_task_counts=lambda: (LocalTaskCount("api1", rr=2),)
                ),
                "roll": SimpleNamespace(
                    local_task_counts=lambda: (LocalTaskCount("api2", arbitrage=1),)
                ),
            },
            _workspace_header=header,
            _local_task_status=status_label,
        )

        LauncherWindow._refresh_local_task_status(app)

        header.set_task_text.assert_called_once_with("api1：RR 2｜api2：套利 1")
        status_label.setText.assert_called_once_with("api1：RR 2｜api2：套利 1")

    def test_launcher_connection_status_distinguishes_public_and_private_state(self) -> None:
        header = MagicMock()
        app = SimpleNamespace(
            _pages={
                "kline": SimpleNamespace(
                    connection_snapshot=lambda: {
                        "public_online": True,
                        "private_online": False,
                        "private_status": "",
                    }
                ),
                "account": SimpleNamespace(
                    connection_snapshot=lambda: {
                        "public_online": False,
                        "private_online": False,
                        "private_status": "账户未解锁",
                    }
                ),
            },
            _active_profile_name="api1",
            _workspace_header=header,
        )

        LauncherWindow._refresh_workspace_connection_status(app)

        header.set_connection_text.assert_called_once_with("● 行情在线 · 账户未解锁", False)

    def test_kline_local_task_counts_group_bound_profiles(self) -> None:
        app = SimpleNamespace(
            _monitorable_rr_trade_ledger_entries=lambda: [
                SimpleNamespace(plan=SimpleNamespace(profile_name="api1")),
                SimpleNamespace(plan=SimpleNamespace(profile_name="api2")),
            ],
            _workspace_entries={
                "BTC-USDT-SWAP|4H": {
                    "lines": [
                        {"enabled": True, "trade_enabled": True, "trade_profile_name": "api1"},
                        {"enabled": True, "trade_enabled": False, "trade_profile_name": "api2"},
                    ]
                },
                "ETH-USDT-SWAP|1H": {
                    "lines": [
                        {"enabled": True, "trade_enabled": True, "trade_profile_name": "api2"},
                    ]
                },
            },
            _active_profile_name=lambda: "api2",
        )

        counts = KlineAnalysisWindow.local_task_counts(app)

        self.assertEqual(
            counts,
            (
                LocalTaskCount("api1", rr=1, line_conditions=1),
                LocalTaskCount("api2", rr=1, line_conditions=1),
            ),
        )

    def test_launcher_opens_kline_by_default_and_lazily_constructs_account(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()
                self.embedded = embedded

            def set_page_active(self, active: bool) -> None:
                return

        home_factory = MagicMock(side_effect=Home)
        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", home_factory),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
        ):
            launcher = LauncherWindow()
            try:
                self.assertEqual(launcher.current_page_key(), "kline")
                self.assertIsInstance(launcher._page_stack.currentWidget(), Kline)
                self.assertEqual(launcher._pages.keys(), {"kline"})
                home_factory.assert_not_called()

                launcher.show_page("account")

                home_factory.assert_called_once()
                self.assertEqual(launcher.current_page_key(), "account")
                self.assertEqual(launcher._child_windows, [])
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_global_profile_change_updates_loaded_pages_once(self) -> None:
        class ProfilePage(QWidget):
            def __init__(self, *args, embedded: bool = False, **kwargs) -> None:  # noqa: ANN002, ANN003
                super().__init__()
                self.applied_profiles: list[str] = []

            def apply_workspace_profile(self, profile_name: str) -> None:
                self.applied_profiles.append(profile_name)

            def begin_shutdown(self, callback=None) -> None:  # noqa: ANN001
                if callback is not None:
                    callback()

            def refresh_view(self) -> None:
                return

        runtimes = {
            "api1": SimpleNamespace(credential_profile_name="api1", environment="demo"),
            "api2": SimpleNamespace(credential_profile_name="api2", environment="live"),
        }
        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", ProfilePage),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", ProfilePage),
            patch(
                "roll_terminal_qt.launcher.load_profile_snapshots",
                return_value=({"api1": {}, "api2": {}}, "api1"),
                create=True,
            ),
            patch(
                "roll_terminal_qt.launcher.load_runtime",
                side_effect=lambda name=None: runtimes[name or "api1"],
                create=True,
            ),
            patch("roll_terminal_qt.launcher.ensure_profile_unlocked", return_value=True, create=True) as unlock,
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("account")

                launcher._request_workspace_profile("api2")

                self.assertEqual(launcher.active_profile_name(), "api2")
                self.assertEqual(launcher._workspace_header.profile_combo.currentText(), "api2")
                self.assertEqual(launcher._workspace_header.environment_label.text(), "实盘")
                self.assertEqual(launcher._pages["kline"].applied_profiles[-1], "api2")
                self.assertEqual(launcher._pages["account"].applied_profiles[-1], "api2")
                unlock.assert_called_once()
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_rejected_global_profile_change_restores_previous_selection(self) -> None:
        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

            def begin_shutdown(self, callback=None) -> None:  # noqa: ANN001
                if callback is not None:
                    callback()

        runtime = SimpleNamespace(credential_profile_name="api1", environment="demo")
        with (
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
            patch(
                "roll_terminal_qt.launcher.load_profile_snapshots",
                return_value=({"api1": {}, "api2": {}}, "api1"),
                create=True,
            ),
            patch("roll_terminal_qt.launcher.load_runtime", return_value=runtime, create=True),
            patch("roll_terminal_qt.launcher.ensure_profile_unlocked", return_value=False, create=True),
        ):
            launcher = LauncherWindow()
            try:
                launcher._workspace_header.profile_combo.setCurrentText("api2")

                self.assertEqual(launcher.active_profile_name(), "api1")
                self.assertEqual(launcher._workspace_header.profile_combo.currentText(), "api1")
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_defaults_to_moni_even_when_saved_profile_is_different(self) -> None:
        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

            def apply_workspace_profile(self, profile_name: str) -> None:
                self.profile_name = profile_name

        runtimes = {
            "moni": SimpleNamespace(credential_profile_name="moni", environment="demo"),
            "api2": SimpleNamespace(credential_profile_name="api2", environment="live"),
        }
        with (
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
            patch(
                "roll_terminal_qt.launcher.load_profile_snapshots",
                return_value=({"api2": {}, "moni": {}}, "api2"),
            ),
            patch("roll_terminal_qt.launcher.load_runtime", side_effect=lambda name=None: runtimes[name or "api2"]),
        ):
            launcher = LauncherWindow()
            try:
                self.assertEqual(launcher.active_profile_name(), "moni")
                self.assertEqual(launcher._workspace_header.profile_combo.currentText(), "moni")
                self.assertEqual(launcher._pages["kline"].profile_name, "moni")
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_global_header_returns_to_account_home(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

            def set_page_active(self, active: bool) -> None:
                return

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
        ):
            launcher = LauncherWindow()
            try:
                launcher._workspace_header.action("page:account").trigger()

                self.assertEqual(launcher._active_page_key, "account")
                self.assertIs(launcher._page_stack.currentWidget(), launcher._home_widget)
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_kline_navigation_keeps_one_persistent_embedded_page(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()
                self.embedded = embedded
                self.page_active: list[bool] = []

            def set_page_active(self, active: bool) -> None:
                self.page_active.append(active)

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("kline")
                first = launcher._pages["kline"]
                launcher.show_page("account")
                launcher.show_page("kline")

                self.assertIs(launcher._pages["kline"], first)
                self.assertIs(first.parent(), launcher._page_stack)
                self.assertEqual(launcher._child_windows, [])
                self.assertTrue(first.embedded)
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_roll_navigation_embeds_persistent_page(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Roll(QWidget):
            pass

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.RollTerminalWindow", Roll),
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("roll")
                first = launcher._pages["roll"]
                launcher.show_page("account")
                launcher.show_page("roll")

                self.assertIs(launcher._pages["roll"], first)
                self.assertIs(first.parent(), launcher._page_stack)
                self.assertEqual(launcher._child_windows, [])
            finally:
                self.__class__.dispose_widget(launcher)

    def test_close_warns_when_embedded_kline_has_local_tasks(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

            @staticmethod
            def local_task_summary():
                return {"rr": 1, "line_conditions": 0, "arbitrage": 0}

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
            patch("roll_terminal_qt.launcher.QMessageBox.question", return_value=QMessageBox.StandardButton.No),
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("kline")
                launcher.close()
                self.assertFalse(launcher._shutdown_in_progress)
            finally:
                self.__class__.dispose_widget(launcher)

    def test_launcher_shows_embedded_local_task_summary(self) -> None:
        class Home(QWidget):
            def begin_shutdown(self, callback):  # noqa: ANN001
                callback()

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

            @staticmethod
            def local_task_summary():
                return {"rr": 2, "line_conditions": 1, "arbitrage": 0}

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("kline")
                launcher._refresh_local_task_status()
                self.assertEqual(launcher._local_task_status.text(), "RR 2 | 条件单 1")
            finally:
                launcher._pages.pop("kline").setParent(None)
                self.__class__.dispose_widget(launcher)

    def test_hiding_chart_keeps_rr_monitor_running(self) -> None:
        chart_host = MagicMock()
        button = MagicMock()
        rr_timer = MagicMock()
        app = SimpleNamespace(_chart_host=chart_host, _hide_chart_btn=button, _rr_monitor_timer=rr_timer)

        KlineAnalysisWindow._toggle_chart_visibility(app, True)

        chart_host.setVisible.assert_called_once_with(False)
        button.setText.assert_called_once_with("显示图表")
        rr_timer.stop.assert_not_called()

    def test_kline_embedded_defaults_to_visible_chart_with_patterns_disabled(self) -> None:
        window = KlineAnalysisWindow(embedded=True)
        try:
            self.assertFalse(window._hide_chart_btn.isChecked())
            self.assertTrue(window._ema9.isChecked())
            self.assertTrue(window._ema21.isChecked())
            self.assertFalse(window._show_1h_shape_signal_check.isChecked())
            self.assertFalse(window._show_4h_shape_signal_check.isChecked())
            self.assertFalse(window._show_1d_shape_signal_check.isChecked())
            self.assertFalse(window._shape_signal_ma_touch_check.isChecked())
            self.assertFalse(window.pattern_signals_enabled())
            self.assertTrue(window._api_profile_combo.isHidden())
        finally:
            self.__class__.dispose_widget(window)

    def test_kline_embedded_collapses_pattern_controls_into_settings_menu(self) -> None:
        window = KlineAnalysisWindow(embedded=True)
        try:
            self.assertTrue(window._shape_signal_group.isHidden())
            self.assertFalse(window._shape_settings_button.isHidden())
            self.assertEqual(window._shape_settings_button.text(), "形态：关")

            window._shape_setting_actions["4H"].setChecked(True)

            self.assertTrue(window._show_4h_shape_signal_check.isChecked())
            self.assertEqual(window._shape_settings_button.text(), "形态：开")
        finally:
            self.__class__.dispose_widget(window)

    def test_kline_embedded_accepts_workspace_profile_without_changing_symbol_or_period(self) -> None:
        runtime_api1 = SimpleNamespace(
            credential_profile_name="api1",
            environment="demo",
            credentials=SimpleNamespace(profile_name="api1"),
        )
        runtime_api2 = SimpleNamespace(
            credential_profile_name="api2",
            environment="live",
            credentials=SimpleNamespace(profile_name="api2"),
        )
        with patch(
            "roll_terminal_qt.kline_analysis_window.load_runtime",
            side_effect=lambda profile_name=None: runtime_api2 if profile_name == "api2" else runtime_api1,
        ):
            window = KlineAnalysisWindow(embedded=True)
        try:
            symbol = window._symbol_combo.currentText()
            period = window._period_combo.currentText()
            with (
                patch(
                    "roll_terminal_qt.kline_analysis_window.load_runtime",
                    side_effect=lambda profile_name=None: runtime_api2 if profile_name == "api2" else runtime_api1,
                ),
                patch.object(window, "_load_data") as load_data,
            ):
                window.apply_workspace_profile("api2")

            self.assertEqual(window.workspace_profile_name(), "api2")
            self.assertIs(window._runtime, runtime_api2)
            self.assertEqual(window._symbol_combo.currentText(), symbol)
            self.assertEqual(window._period_combo.currentText(), period)
            load_data.assert_called_once()
        finally:
            self.__class__.dispose_widget(window)

    def test_realtime_candle_merge_replaces_open_bar_without_history_reload(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": 1_000, "open": 10.0, "high": 12.0, "low": 9.0, "close": 10.0, "volume": 2.0}],
            ema_9=[{"time": 1_000, "value": 10.0}],
            ema_21=[{"time": 1_000, "value": 10.0}],
            ema_55=[{"time": 1_000, "value": 10.0}],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[Candle(1_000, Decimal("10"), Decimal("12"), Decimal("9"), Decimal("10"), Decimal("2"), False)],
            stats={"returned": 1},
        )

        updated = _merge_realtime_candle_payload(
            payload,
            Candle(1_000, Decimal("10"), Decimal("13"), Decimal("9"), Decimal("11"), Decimal("3"), False),
        )

        self.assertEqual(updated.candles[0]["close"], 11.0)
        self.assertEqual(updated.raw_candles[0].volume, Decimal("3"))

    def test_window_realtime_candle_does_not_call_history_loader_or_full_renderer(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": 1, "open": 10.0, "high": 12.0, "low": 9.0, "close": 10.0, "volume": 2.0}],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[],
            raw_candles=[Candle(1_000, Decimal("10"), Decimal("12"), Decimal("9"), Decimal("10"), Decimal("2"), False)],
            stats={},
        )
        app = SimpleNamespace(
            _pending_payload=payload,
            _loaded_primary_request_key=("primary",),
            _primary_payload_cache={},
            _remember_payload_cache=MagicMock(),
            _apply_realtime_candle_to_chart=MagicMock(),
            _load_data=MagicMock(),
            _render_to_chart=MagicMock(),
        )

        KlineAnalysisWindow._apply_realtime_candle(
            app,
            Candle(1_000, Decimal("10"), Decimal("12"), Decimal("9"), Decimal("11"), Decimal("2"), False),
        )

        app._load_data.assert_not_called()
        app._render_to_chart.assert_not_called()
        app._apply_realtime_candle_to_chart.assert_called_once()

    def test_measure_ui_step_logs_elapsed_ms(self) -> None:
        messages: list[str] = []
        with patch("roll_terminal_qt.perf_metrics.append_log_line", side_effect=messages.append):
            with measure_ui_step("orders_apply", rows=3):
                pass

        self.assertEqual(len(messages), 1)
        self.assertIn("[qt_perf] orders_apply", messages[0])
        self.assertIn("elapsed_ms=", messages[0])
        self.assertIn("rows=3", messages[0])

    def test_kline_account_drawer_load_thread_fetches_positions_and_orders_in_parallel(self) -> None:
        barrier = threading.Barrier(2)

        class FakeClient:
            def get_positions(self, credentials, *, environment):  # noqa: ANN001
                self._touch(credentials, environment)
                barrier.wait(timeout=0.5)
                time.sleep(0.02)
                return ["position-1"]

            def get_pending_orders(self, credentials, *, environment, limit, include_algo):  # noqa: ANN001
                self._touch(credentials, environment, limit, include_algo)
                barrier.wait(timeout=0.5)
                time.sleep(0.02)
                return ["order-1"]

            @staticmethod
            def _touch(*_args, **_kwargs) -> None:
                return

        runtime = SimpleNamespace(credentials=object(), environment="demo")
        thread = AccountDrawerLoadThread(
            request_generation=7,
            runtime=runtime,
            client=FakeClient(),
        )
        completed: list[tuple[int, object]] = []
        failed: list[tuple[int, str]] = []
        thread.completed.connect(lambda generation, snapshot: completed.append((generation, snapshot)))
        thread.failed.connect(lambda generation, message: failed.append((generation, message)))

        thread.run()

        self.assertEqual(failed, [])
        self.assertEqual(len(completed), 1)
        generation, snapshot = completed[0]
        self.assertEqual(generation, 7)
        self.assertEqual(tuple(snapshot.positions), ("position-1",))
        self.assertEqual(tuple(snapshot.orders), ("order-1",))

    def test_launcher_waits_for_open_child_windows_before_home_shutdown(self) -> None:
        class ShutdownHome(QWidget):
            def __init__(self, parent=None) -> None:  # noqa: ANN001
                super().__init__(parent)
                self.shutdown_calls = 0

            def begin_shutdown(self, _finished) -> None:  # noqa: ANN001
                self.shutdown_calls += 1

            def refresh_view(self) -> None:
                return

        class BlockingChild(QWidget):
            def closeEvent(self, event) -> None:  # noqa: ANN001
                event.ignore()

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", ShutdownHome),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
        ):
            launcher = LauncherWindow()
            launcher.show_page("account")
        child = BlockingChild()
        try:
            child.show()
            launcher._child_windows.append(child)

            launcher._begin_shutdown()

            self.assertEqual(launcher._home_widget.shutdown_calls, 0)
            child.hide()
            launcher._wait_for_child_windows_shutdown()
            self.assertEqual(launcher._home_widget.shutdown_calls, 1)
        finally:
            child.deleteLater()
            launcher.hide()
            launcher.deleteLater()
            self._app.processEvents()

    def test_launcher_waits_for_every_workspace_page_shutdown_callback(self) -> None:
        class Home(QWidget):
            def __init__(self, parent=None) -> None:  # noqa: ANN001
                super().__init__(parent)
                self.callback = None

            def begin_shutdown(self, callback) -> None:  # noqa: ANN001
                self.callback = callback

            def refresh_view(self) -> None:
                return

        class Kline(QWidget):
            def __init__(self, *, embedded: bool = False) -> None:
                super().__init__()
                self.callback = None

            def begin_shutdown(self, callback=None) -> None:  # noqa: ANN001
                self.callback = callback

        class Roll(QWidget):
            def __init__(self) -> None:
                super().__init__()
                self.callback = None

            def begin_shutdown(self, callback=None) -> None:  # noqa: ANN001
                self.callback = callback

        with (
            patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", Home),
            patch("roll_terminal_qt.launcher.KlineAnalysisWindow", Kline),
            patch("roll_terminal_qt.launcher.RollTerminalWindow", Roll),
        ):
            launcher = LauncherWindow()
            try:
                launcher.show_page("account")
                launcher.show_page("roll")
                launcher._finish_shutdown = MagicMock()
                launcher._wait_for_child_windows_shutdown()

                kline = launcher._pages["kline"]
                home = launcher._pages["account"]
                roll = launcher._pages["roll"]
                self.assertTrue(callable(kline.callback))
                self.assertTrue(callable(home.callback))
                self.assertTrue(callable(roll.callback))
                kline.callback()
                home.callback()
                launcher._finish_shutdown.assert_not_called()
                roll.callback()
                launcher._finish_shutdown.assert_called_once()
            finally:
                self.__class__.dispose_widget(launcher)

    def test_roll_shutdown_requests_thread_stop_without_blocking_wait(self) -> None:
        feed = MagicMock()
        account = MagicMock()
        order = MagicMock()
        for thread in (feed, account, order):
            thread.isRunning.return_value = True
        app = SimpleNamespace(
            _shutdown_callbacks=[],
            _shutdown_requested=False,
            _runtime_thread_generation=0,
            _feed=feed,
            _account_feed=account,
            _order_feed=order,
            _target_thread=None,
            _execution_thread=None,
        )
        finished: list[bool] = []

        with patch("roll_terminal_qt.ui.QTimer.singleShot") as schedule:
            RollTerminalWindow.begin_shutdown(app, lambda: finished.append(True))

        for thread in (feed, account, order):
            thread.stop.assert_called_once()
            thread.wait.assert_not_called()
        self.assertEqual(finished, [])
        schedule.assert_called_once()

    def test_roll_history_refresh_batches_rows_without_per_row_resize(self) -> None:
        table = MagicMock()
        app = SimpleNamespace(
            _execution_history_records=[
                {
                    "timestamp": "2026-07-12 19:00:00", "profile": "moni", "task": "移仓",
                    "current_inst_id": "BTC-USD-260626", "target_inst_id": "BTC-USD-260925",
                    "qty": "1", "status": "完成", "avg_spread_line": "价差：1",
                    "fee_line": "", "net_spread_line": "净价差：1", "message": "", "success": True,
                },
            ],
            _history_table=table,
            _refresh_execution_history_summary=MagicMock(),
            _history_metric_value=RollTerminalWindow._history_metric_value,
            _extract_history_fee_usdt=RollTerminalWindow._extract_history_fee_usdt,
            _history_fee_display=lambda text: RollTerminalWindow._history_fee_display(app, text),
        )

        RollTerminalWindow._refresh_execution_history_view(app)

        table.setRowCount.assert_any_call(1)
        table.resizeRowToContents.assert_not_called()

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
                    self.assertIn("不能超过当前可平仓位", error_message)
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
                    self.assertIn("不能超过当前可平仓位", str(error_mock.call_args.args[1] if len(error_mock.call_args.args) > 1 else ""))
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

    def test_kline_symbol_combo_uses_bounded_header_width(self) -> None:
        window = KlineAnalysisWindow()
        try:
            self.assertLessEqual(window._symbol_combo.maximumWidth(), 420)
            self.assertEqual(
                window._symbol_combo.sizePolicy().horizontalPolicy(),
                QSizePolicy.Policy.Preferred,
            )
        finally:
            self.__class__.dispose_widget(window)

    def test_kline_symbol_options_and_volatility_currency_mapping(self) -> None:
        self.assertEqual(
            KLINE_SYMBOL_OPTIONS,
            (
                "BTC-USDT-SWAP",
                "ETH-USDT-SWAP",
                "SOL-USDT-SWAP",
                "DOGE-USDT-SWAP",
                "BNB-USDT-SWAP",
                "OKB-USDT-SWAP",
                "ETH-BTC",
            ),
        )
        self.assertEqual(_volatility_currency_for_symbol("BTC-USDT-SWAP"), "BTC")
        self.assertEqual(_volatility_currency_for_symbol("ETH-USDT-SWAP"), "ETH")
        self.assertEqual(_volatility_currency_for_symbol("ETH-BTC"), "ETH")
        self.assertIsNone(_volatility_currency_for_symbol("SOL-USDT-SWAP"))
        self.assertIsNone(_volatility_currency_for_symbol("DOGE-USDT-SWAP"))
        self.assertIsNone(_volatility_currency_for_symbol("BNB-USDT-SWAP"))
        self.assertIsNone(_volatility_currency_for_symbol("OKB-USDT-SWAP"))

    def test_kline_symbol_combo_offers_only_configured_symbols(self) -> None:
        window = KlineAnalysisWindow()
        try:
            self.assertEqual(
                [window._symbol_combo.itemText(index) for index in range(window._symbol_combo.count())],
                list(KLINE_SYMBOL_OPTIONS),
            )
            self.assertFalse(window._symbol_combo.isEditable())
        finally:
            self.__class__.dispose_widget(window)

    def test_selecting_symbol_without_volatility_reverts_secondary_chart_to_kline(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
        try:
            window._secondary_chart_check.setChecked(True)
            window._secondary_chart_kind_mode = "volatility"
            window._symbol_combo.setCurrentText("SOL-USDT-SWAP")

            self.assertEqual(window._secondary_chart_kind(), "kline")
            self.assertFalse(window._secondary_chart_kind_btn.isEnabled())
        finally:
            self.__class__.dispose_widget(window)

    def test_kline_window_initializes_api_runtime_context(self) -> None:
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api2"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="api2",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["api1", "api2"]),
            patch("roll_terminal_qt.kline_analysis_window.load_runtime", return_value=runtime),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._api_profile_combo.currentText(), "api2")
                self.assertEqual(window._runtime, runtime)
                self.assertEqual(window._active_profile_name(), "api2")
                self.assertEqual(window._active_environment(), "demo")
                self.assertIn("api2", window._account_context.text())
                self.assertIn("demo", window._account_context.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_window_prefers_moni_api_runtime_context_when_available(self) -> None:
        runtime_api1 = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api1"),
            environment="live",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="api1",
        )
        runtime_moni = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["api1", "moni"]),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_runtime",
                side_effect=lambda profile_name=None: runtime_moni if profile_name == "moni" else runtime_api1,
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._api_profile_combo.currentText(), "moni")
                self.assertEqual(window._runtime, runtime_moni)
                self.assertEqual(window._active_profile_name(), "moni")
                self.assertEqual(window._active_environment(), "demo")
                self.assertIn("moni", window._account_context.text())
                self.assertIn("demo", window._account_context.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_window_prefers_moni_over_current_global_runtime_when_available(self) -> None:
        runtime_reapai = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="ReapAI"),
            environment="live",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="ReapAI",
        )
        runtime_moni = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["moni", "ReapAI"]),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_runtime",
                side_effect=lambda profile_name=None: runtime_moni if profile_name == "moni" else runtime_reapai,
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._api_profile_combo.currentText(), "moni")
                self.assertEqual(window._runtime, runtime_moni)
                self.assertIn("moni", window._account_context.text())
                self.assertIn("demo", window._account_context.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_window_switches_api_runtime_context(self) -> None:
        runtime_1 = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api1"),
            environment="live",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="api1",
        )
        runtime_2 = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api2"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="api2",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["api1", "api2"]),
            patch("roll_terminal_qt.kline_analysis_window.load_runtime", side_effect=[runtime_1, runtime_2]),
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window, "_load_data") as load_data:
                    window._api_profile_combo.setCurrentText("api2")

                self.assertEqual(window._runtime, runtime_2)
                self.assertEqual(window._active_profile_name(), "api2")
                self.assertEqual(window._active_environment(), "demo")
                self.assertIn("api2", window._account_context.text())
                self.assertIn("demo", window._account_context.text())
                load_data.assert_called_once()
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_window_filters_rr_ledger_entries_by_profile_environment_and_symbol(self) -> None:
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api2"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="api2",
        )
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        matching = RRTradeLedgerEntry(
            entry_id="rr-1",
            status="created",
            plan=build_rr_trade_plan(
                plan_id="rr-1",
                profile_name="api2",
                environment="demo",
                instrument=instrument,
                direction="long",
                entry_execution_mode="limit",
                management_mode="fixed_tp",
                trigger_price_type="last",
                risk_amount=Decimal("100"),
                entry_price=Decimal("60000"),
                stop_loss_price=Decimal("59000"),
                direct_take_profit_r=Decimal("5"),
                round_trip_fee_rate=Decimal("0"),
            ),
        ).to_dict()
        different_profile = RRTradeLedgerEntry(
            entry_id="rr-2",
            status="created",
            plan=build_rr_trade_plan(
                plan_id="rr-2",
                profile_name="api1",
                environment="demo",
                instrument=instrument,
                direction="long",
                entry_execution_mode="limit",
                management_mode="fixed_tp",
                trigger_price_type="last",
                risk_amount=Decimal("100"),
                entry_price=Decimal("60000"),
                stop_loss_price=Decimal("59000"),
                direct_take_profit_r=Decimal("5"),
                round_trip_fee_rate=Decimal("0"),
            ),
        ).to_dict()
        different_symbol = RRTradeLedgerEntry(
            entry_id="rr-3",
            status="created",
            plan=build_rr_trade_plan(
                plan_id="rr-3",
                profile_name="api2",
                environment="demo",
                instrument=SimpleNamespace(**{**instrument.__dict__, "inst_id": "ETH-USDT-SWAP", "uly": "ETH-USDT", "inst_family": "ETH-USDT"}),
                direction="long",
                entry_execution_mode="limit",
                management_mode="fixed_tp",
                trigger_price_type="last",
                risk_amount=Decimal("100"),
                entry_price=Decimal("3000"),
                stop_loss_price=Decimal("2900"),
                direct_take_profit_r=Decimal("5"),
                round_trip_fee_rate=Decimal("0"),
            ),
        ).to_dict()
        with (
            patch("roll_terminal_qt.kline_analysis_window.profile_names", return_value=["api2"]),
            patch("roll_terminal_qt.kline_analysis_window.load_runtime", return_value=runtime),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot",
                return_value={"entries": [matching, different_profile, different_symbol]},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                filtered = window._matching_rr_trade_ledger_entries()
                self.assertEqual([entry.entry_id for entry in filtered], ["rr-1"])
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_workspace_rr_items_reload_into_rr_table(self) -> None:
        payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument",
                return_value=SimpleNamespace(tick_size=Decimal("0.1")),
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._rr_table.rowCount(), 1)
                self.assertEqual(window._rr_table.item(0, 0).text(), "多头")
                self.assertEqual(window._rr_table.item(0, 1).text(), "60000.0")
                self.assertEqual(window._rr_table.item(0, 3).text(), "62000.0")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_workspace_rr_items_format_long_price_decimals_for_table(self) -> None:
        payload = {
            "rr_id": "rr-1",
            "side": "short",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "63610.9729516",
            "price_stop": "64402.7794297",
            "price_tp": "62027.3599952",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument",
                return_value=SimpleNamespace(tick_size=Decimal("0.1")),
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._rr_table.item(0, 1).text(), "63611.0")
                self.assertEqual(window._rr_table.item(0, 2).text(), "64402.8")
                self.assertEqual(window._rr_table.item(0, 3).text(), "62027.4")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_apply_alert_snapshot_preserves_current_rr_items(self) -> None:
        payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch.object(KlineAnalysisWindow, "_toggle_shape_signal_size_metric", lambda self: None, create=True),
            patch.object(KlineAnalysisWindow, "_refresh_shape_signal_size_metric_button", lambda self: None, create=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                snapshot = KlineAlertSnapshot(
                    workspace_entry={"lines": [], "alerts": {}, "events": []},
                    new_events=[],
                    structure={},
                )

                window._apply_alert_snapshot(snapshot)

                entry = window._workspace_entry()
                self.assertEqual(len(entry["rr"]), 1)
                self.assertEqual(entry["rr"][0]["rr_id"], "rr-1")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_save_rr_item_writes_workspace_payload_and_preview(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_side_combo.setCurrentIndex(0)
                window._rr_entry_edit.setText("60000")
                window._rr_stop_edit.setText("59000")
                window._rr_r_edit.setText("2")
                window._rr_bar_edit.setText("12")

                window._save_rr_item()

                entry = window._workspace_entry()
                rr_items = entry.get("rr", [])
                self.assertEqual(len(rr_items), 1)
                saved = rr_items[0]
                self.assertEqual(saved["side"], "long")
                self.assertEqual(saved["price_entry"], "60000")
                self.assertEqual(saved["price_stop"], "59000")
                self.assertEqual(saved["price_tp"], "62000")
                self.assertEqual(saved["r_multiple"], "2")
                self.assertEqual(saved["management_mode"], "fixed_tp")
                self.assertEqual(saved["direct_take_profit_r"], "2")
                self.assertEqual(window._rr_table.rowCount(), 1)
                self.assertIn("62000", window._rr_preview.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_build_selected_rr_trade_plan_requires_explicit_execution_mode(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"),
            environment="demo",
            trade_mode="cross",
            position_mode="net",
            credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = runtime
                window._rr_entry_edit.setText("60000")
                window._rr_stop_edit.setText("59000")
                window._rr_r_edit.setValue(2.0)
                window._rr_bar_edit.setText("12")
                window._save_rr_item()
                window._rr_execution_mode_combo.setCurrentIndex(
                    window._rr_execution_mode_combo.findData("chase_best_quote")
                )
                with patch.object(window, "_instrument_for_symbol", return_value=instrument):
                    plan = window._build_selected_rr_trade_plan()

                self.assertEqual(plan.entry_execution_mode, "chase_best_quote")
                self.assertEqual(plan.environment, "demo")
                trade_ref = window._workspace_entry()["rr"][0]["trade_ref"]
                self.assertTrue(trade_ref)
                self.assertTrue(plan.plan_id.endswith(trade_ref))
                self.assertEqual(window._rr_trade_ledger_snapshot.get("entries", []), [])
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_save_rr_trade_ledger_entry_binds_exact_refs_to_selected_rr(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries"),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot"),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_entry_edit.setText("60000")
                window._rr_stop_edit.setText("59000")
                window._rr_r_edit.setValue(2.0)
                window._rr_bar_edit.setText("12")
                window._save_rr_item()
                trade_ref = str(window._workspace_entry()["rr"][0]["trade_ref"])
                plan = build_rr_trade_plan(
                    plan_id=f"moni:BTC-USDT-SWAP:{trade_ref}",
                    profile_name="moni",
                    environment="demo",
                    instrument=instrument,
                    direction="long",
                    entry_execution_mode="limit",
                    management_mode="fixed_tp",
                    trigger_price_type="last",
                    risk_amount=Decimal("100"),
                    entry_price=Decimal("60000"),
                    stop_loss_price=Decimal("59000"),
                    direct_take_profit_r=Decimal("2"),
                    round_trip_fee_rate=Decimal("0"),
                )
                entry = RRTradeLedgerEntry(
                    entry_id=plan.plan_id,
                    status="entry_working",
                    plan=plan,
                    entry_order=RRTradeOrderLink(
                        role="entry",
                        channel="order",
                        order_id="ord-1",
                        client_id="cl-1",
                        state="live",
                        size=Decimal("3"),
                        price=Decimal("60000"),
                    ),
                    stop_loss_order=RRTradeOrderLink(
                        role="stop_loss",
                        channel="algo",
                        algo_id="algo-sl-1",
                        client_id="algo-cl-sl-1",
                        state="pending",
                        trigger_price=Decimal("59000"),
                    ),
                    take_profit_order=RRTradeOrderLink(
                        role="take_profit",
                        channel="algo",
                        algo_id="algo-tp-1",
                        client_id="algo-cl-tp-1",
                        state="pending",
                        trigger_price=Decimal("62000"),
                    ),
                )

                window._save_rr_trade_ledger_entry(entry)

                binding = window._workspace_entry()["rr"][0].get("trade_binding", {})
                self.assertEqual(binding["plan_id"], plan.plan_id)
                self.assertEqual(binding["entry_order_id"], "ord-1")
                self.assertEqual(binding["entry_client_id"], "cl-1")
                self.assertEqual(binding["stop_loss_algo_id"], "algo-sl-1")
                self.assertEqual(binding["take_profit_algo_id"], "algo-tp-1")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_cancel_rr_trade_uses_bound_ledger_entry_instead_of_rebuilding_plan(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        rr_payload = {
            "rr_id": "rr-1",
            "trade_ref": "trade-ref-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "62295.3",
            "price_stop": "60893.7",
            "price_tp": "63416.6",
            "r_multiple": "0.8",
            "management_mode": "fixed_tp",
            "locked": False,
            "trade_binding": {"plan_id": "moni:BTC-USDT-SWAP:trade-ref-1"},
        }
        plan = build_rr_trade_plan(
            plan_id="moni:BTC-USDT-SWAP:trade-ref-1",
            profile_name="moni",
            environment="demo",
            instrument=instrument,
            direction="long",
            entry_execution_mode="limit",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("2"),
            round_trip_fee_rate=Decimal("0"),
        )
        ledger = RRTradeLedgerEntry(
            entry_id=plan.plan_id,
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="3732030867860197376", client_id="rrent-real-1", state="live"),
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot",
                return_value={"entries": [ledger.to_dict()]},
            ),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = SimpleNamespace(credentials=object())
                window._rr_table.setCurrentCell(0, 0)
                with (
                    patch.object(window, "_build_selected_rr_trade_plan", side_effect=AssertionError("cancel should not rebuild plan")),
                    patch.object(window, "_start_rr_execution_action") as start_execution,
                ):
                    window._cancel_selected_rr_trade()

                self.assertEqual(start_execution.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_remove_rr_item_cancels_bound_entry_before_deleting(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        rr_payload = {
            "rr_id": "rr-1",
            "trade_ref": "trade-ref-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "62295.3",
            "price_stop": "60893.7",
            "price_tp": "63416.6",
            "r_multiple": "0.8",
            "management_mode": "fixed_tp",
            "locked": False,
            "trade_binding": {"plan_id": "moni:BTC-USDT-SWAP:trade-ref-1"},
        }
        plan = build_rr_trade_plan(
            plan_id="moni:BTC-USDT-SWAP:trade-ref-1",
            profile_name="moni",
            environment="demo",
            instrument=instrument,
            direction="long",
            entry_execution_mode="limit",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("2"),
            round_trip_fee_rate=Decimal("0"),
        )
        ledger = RRTradeLedgerEntry(
            entry_id=plan.plan_id,
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(
                role="entry",
                channel="order",
                order_id="3732030867860197376",
                client_id="rrent-real-1",
                state="live",
            ),
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot",
                return_value={"entries": [ledger.to_dict()]},
            ),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = SimpleNamespace(credentials=object())
                window._rr_table.setCurrentCell(0, 0)
                with patch.object(window, "_start_rr_execution_action") as start_execution:
                    window._remove_rr_item()

                self.assertEqual(start_execution.call_count, 1)
                self.assertEqual(len(window._workspace_entry()["rr"]), 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_remove_rr_item_purges_local_placeholder_rr_without_api_cancel(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "trade_ref": "trade-ref-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "management_mode": "fixed_tp",
            "locked": False,
            "trade_binding": {"plan_id": "moni:BTC-USDT-SWAP:trade-ref-1"},
        }
        plan = build_rr_trade_plan(
            plan_id="moni:BTC-USDT-SWAP:trade-ref-1",
            profile_name="moni",
            environment="demo",
            instrument=SimpleNamespace(
                inst_id="BTC-USDT-SWAP",
                inst_type="SWAP",
                tick_size=Decimal("0.1"),
                lot_size=Decimal("1"),
                min_size=Decimal("1"),
                state="live",
                settle_ccy="USDT",
                ct_val=Decimal("0.01"),
                ct_mult=Decimal("1"),
                ct_val_ccy="BTC",
                uly="BTC-USDT",
                inst_family="BTC-USDT",
            ),
            direction="long",
            entry_execution_mode="limit",
            management_mode="fixed_tp",
            trigger_price_type="last",
            risk_amount=Decimal("100"),
            entry_price=Decimal("60000"),
            stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("2"),
            round_trip_fee_rate=Decimal("0"),
        )
        ledger = RRTradeLedgerEntry(
            entry_id=plan.plan_id,
            status="entry_working",
            plan=plan,
            entry_order=RRTradeOrderLink(role="entry", channel="order", order_id="ord-1", client_id="cl-1", state="live"),
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot",
                return_value={"entries": [ledger.to_dict()]},
            ),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_analysis_workspace_entries", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.save_kline_rr_trade_ledger_snapshot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_table.setCurrentCell(0, 0)
                with patch.object(window, "_start_rr_execution_action") as start_execution:
                    window._remove_rr_item()

                self.assertEqual(start_execution.call_count, 0)
                self.assertEqual(window._workspace_entry()["rr"], [])
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_enable_rr_trade_queues_when_execution_channel_busy(self) -> None:
        plan = SimpleNamespace(
            plan_id="moni:BTC-USDT-SWAP:trade-ref-2",
            inst_id="BTC-USDT-SWAP",
            direction="long",
            entry_execution_mode="limit",
            stop_loss_price=Decimal("62924.4"),
            take_profit_price=Decimal("64757.9"),
        )
        rr_payload = {
            "rr_id": "rr-2",
            "trade_ref": "trade-ref-2",
            "side": "long",
            "bar_entry": 1200.0,
            "bar_stop": 1201.0,
            "price_entry": "63535.6",
            "price_stop": "62924.4",
            "price_tp": "64757.9",
            "r_multiple": "2",
            "management_mode": "fixed_tp",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = SimpleNamespace(credentials=object())
                window._rr_table.setCurrentCell(0, 0)
                window._rr_execution_in_flight = True
                with (
                    patch.object(window, "_build_selected_rr_trade_plan", return_value=plan),
                    patch.object(window, "_find_rr_trade_ledger_entry", return_value=None),
                    patch("roll_terminal_qt.kline_analysis_window._rr_plan_position_text", return_value="0.16 BTC (16.36张)"),
                    patch("roll_terminal_qt.kline_analysis_window.QMessageBox.question", return_value=QMessageBox.StandardButton.Yes),
                ):
                    window._enable_selected_rr_trade()

                self.assertEqual(len(window._pending_rr_execution_requests), 1)
                self.assertIn("已排队", window._status.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_build_line_trade_plan_uses_trigger_line_price_and_event_key(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            state="live", settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"), environment="demo", trade_mode="cross", position_mode="net", credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = runtime
                window._workspace_entry()["lines"] = [{
                    "id": "line-1",
                    "kind": "horizontal",
                    "label": "Breakout long",
                    "action": "long",
                    "enabled": True,
                    "trade_enabled": True,
                    "time_a": 100,
                    "price_a": 60000.0,
                    "time_b": 100,
                    "price_b": 60000.0,
                    "stop_loss_price": 59000.0,
                    "risk_amount": 125.0,
                    "direct_take_profit_r": 2.5,
                    "management_mode": "trail_after_2r",
                    "entry_execution_mode": "chase_best_quote",
                    "fee_offset_enabled": False,
                    "trade_profile_name": "api1",
                    "trade_environment": "live",
                }]
                event = {"kind": "line_alert", "line_id": "line-1", "trade_action": "long", "trade_enabled": True, "candle_time": 200}
                with patch.object(window, "_instrument_for_symbol", return_value=instrument):
                    plan = window._build_line_trade_plan_from_event(event)

                self.assertEqual(plan.plan_id, "api1:BTC-USDT-SWAP:line-1:200")
                self.assertEqual(plan.profile_name, "api1")
                self.assertEqual(plan.environment, "live")
                self.assertEqual(plan.direction, "long")
                self.assertEqual(plan.entry_price, Decimal("60000.0"))
                self.assertEqual(plan.stop_loss_price, Decimal("59000.0"))
                self.assertEqual(plan.entry_execution_mode, "chase_best_quote")
                self.assertEqual(plan.management_mode, "trail_after_2r")
                self.assertEqual(plan.risk_amount, Decimal("125.0"))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_line_trade_toggle_is_saved_per_selected_line(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = ArbitrageTradeRuntime(
                    credentials=SimpleNamespace(profile_name="moni"),
                    environment="demo",
                    trade_mode="cross",
                    position_mode="net",
                    credential_profile_name="moni",
                )
                window._workspace_entry()["lines"] = [{
                    "id": "line-1", "kind": "horizontal", "label": "Breakout long", "trigger": "cross_above",
                    "action": "long", "enabled": True, "time_a": 100, "price_a": 60000.0, "time_b": 100, "price_b": 60000.0,
                }]
                window._populate_line_table(selected_index=0)
                window._line_trade_enabled_check.setChecked(True)
                window._line_trade_execution_mode_combo.setCurrentIndex(
                    window._line_trade_execution_mode_combo.findData("chase_best_quote")
                )
                window._update_selected_line()

                self.assertTrue(window._workspace_entry()["lines"][0]["trade_enabled"])
                self.assertEqual(window._workspace_entry()["lines"][0]["entry_execution_mode"], "chase_best_quote")
                self.assertEqual(window._workspace_entry()["lines"][0]["trade_profile_name"], "moni")
                self.assertEqual(window._workspace_entry()["lines"][0]["trade_environment"], "demo")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_line_price_table_and_editor_support_horizontal_and_trend_lines(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._workspace_entry()["lines"] = [
                    {
                        "id": "horizontal-1", "kind": "horizontal", "label": "H", "trigger": "cross_above",
                        "action": "notify", "enabled": True, "time_a": 100, "price_a": 60000.0,
                        "time_b": 100, "price_b": 60000.0,
                    },
                    {
                        "id": "trend-1", "kind": "trend", "label": "T", "trigger": "cross_above",
                        "action": "notify", "enabled": True, "time_a": 100, "price_a": 60000.0,
                        "time_b": 200, "price_b": 62000.0,
                    },
                ]

                window._populate_line_table(selected_index=0)
                self.assertEqual(window._line_table.columnCount(), 6)
                self.assertEqual(window._line_table.item(0, 2).text(), "60000.0")
                self.assertTrue(window._line_price_b_edit.isHidden())
                window._line_price_a_edit.setText("61000")
                window._update_selected_line()
                self.assertEqual(window._workspace_entry()["lines"][0]["price_a"], 61000.0)
                self.assertEqual(window._workspace_entry()["lines"][0]["price_b"], 61000.0)

                window._populate_line_table(selected_index=1)
                self.assertEqual(window._line_table.item(1, 2).text(), "60000.0 → 62000.0")
                self.assertFalse(window._line_price_b_edit.isHidden())
                window._line_price_a_edit.setText("60500")
                window._line_price_b_edit.setText("62500")
                window._update_selected_line()
                self.assertEqual(window._workspace_entry()["lines"][1]["price_a"], 60500.0)
                self.assertEqual(window._workspace_entry()["lines"][1]["price_b"], 62500.0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_line_trade_events_require_global_arming_and_deduplicate_plan(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            state="live", settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"), environment="demo", trade_mode="cross", position_mode="net", credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = runtime
                window._workspace_entry()["lines"] = [{
                    "id": "line-1", "kind": "horizontal", "label": "Breakout long", "action": "long", "enabled": True,
                    "trade_enabled": True, "time_a": 100, "price_a": 60000.0, "time_b": 100, "price_b": 60000.0,
                    "stop_loss_price": 59000.0, "risk_amount": 125.0, "direct_take_profit_r": 2.0,
                    "management_mode": "fixed_tp", "entry_execution_mode": "limit", "fee_offset_enabled": False,
                    "trade_profile_name": "api1", "trade_environment": "live",
                }]
                event = {"kind": "line_alert", "line_id": "line-1", "trade_action": "long", "trade_enabled": True, "candle_time": 200}
                with patch.object(window, "_instrument_for_symbol", return_value=instrument):
                    self.assertEqual(window._build_armed_line_trade_plans([event]), [])
                    window._line_trade_armed_check.blockSignals(True)
                    window._line_trade_armed_check.setChecked(True)
                    window._line_trade_armed_check.blockSignals(False)
                    plans = window._build_armed_line_trade_plans([event, event])

                self.assertEqual(len(plans), 1)
                self.assertEqual(plans[0].plan_id, "api1:BTC-USDT-SWAP:line-1:200")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_line_trade_queue_dispatches_through_rr_execution_service(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            state="live", settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"), environment="demo", trade_mode="cross", position_mode="net", credential_profile_name="moni",
        )
        bound_runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="api1"), environment="live", trade_mode="cross", position_mode="net", credential_profile_name="api1",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = runtime
                window._workspace_entry()["lines"] = [{
                    "id": "line-1", "kind": "horizontal", "label": "Breakout long", "action": "long", "enabled": True,
                    "trade_enabled": True, "time_a": 100, "price_a": 60000.0, "time_b": 100, "price_b": 60000.0,
                    "stop_loss_price": 59000.0, "risk_amount": 125.0, "direct_take_profit_r": 2.0,
                    "management_mode": "fixed_tp", "entry_execution_mode": "limit", "fee_offset_enabled": False,
                    "trade_profile_name": "api1", "trade_environment": "live",
                }]
                event = {"kind": "line_alert", "line_id": "line-1", "trade_action": "long", "trade_enabled": True, "candle_time": 200}
                window._line_trade_armed_check.blockSignals(True)
                window._line_trade_armed_check.setChecked(True)
                window._line_trade_armed_check.blockSignals(False)
                with (
                    patch.object(window, "_instrument_for_symbol", return_value=instrument),
                    patch.object(window, "_start_rr_execution_action") as start_execution,
                    patch("roll_terminal_qt.kline_analysis_window.load_runtime", return_value=bound_runtime) as load_bound_runtime,
                ):
                    window._enqueue_line_trade_events([event])

                self.assertEqual(start_execution.call_count, 1)
                load_bound_runtime.assert_called_with("api1")
                self.assertEqual(len(window._line_trade_execution_queue), 0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_monitorable_rr_entries_are_not_filtered_by_visible_profile(self) -> None:
        api1 = SimpleNamespace(status="protected", plan=SimpleNamespace(profile_name="api1"))
        api2 = SimpleNamespace(status="entry_working", plan=SimpleNamespace(profile_name="api2"))
        stopped = SimpleNamespace(status="cancelled", plan=SimpleNamespace(profile_name="api3"))
        app = SimpleNamespace(
            _all_rr_trade_ledger_entries=lambda: [api1, api2, stopped],
            _rr_trade_execution_service=SimpleNamespace(
                should_monitor_status=lambda status: status in {"protected", "entry_working"}
            ),
        )

        entries = KlineAnalysisWindow._monitorable_rr_trade_ledger_entries(app)

        self.assertEqual([entry.plan.profile_name for entry in entries], ["api1", "api2"])

    def test_kline_rr_monitor_rotates_across_bound_profiles(self) -> None:
        api1 = SimpleNamespace(plan=SimpleNamespace(profile_name="api1"))
        api2 = SimpleNamespace(plan=SimpleNamespace(profile_name="api2"))
        app = SimpleNamespace(
            _rr_monitor_cursor=0,
            _monitorable_rr_trade_ledger_entries=lambda: [api1, api2],
        )

        first = KlineAnalysisWindow._next_monitorable_rr_entry(app)
        second = KlineAnalysisWindow._next_monitorable_rr_entry(app)

        self.assertIs(first, api1)
        self.assertIs(second, api2)

    def test_kline_alert_snapshot_forwards_line_events_to_trade_queue(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                event = {"kind": "line_alert", "line_id": "line-1", "trade_action": "long", "trade_enabled": True, "candle_time": 200}
                snapshot = KlineAlertSnapshot(
                    workspace_entry={"lines": [], "rr": [], "alerts": {}, "events": [event]},
                    new_events=[event],
                    structure={},
                )
                with patch.object(window, "_enqueue_line_trade_events") as enqueue:
                    window._apply_alert_snapshot(snapshot)

                enqueue.assert_called_once_with([event])
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_close_stops_rr_monitor_and_refresh_timers(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_monitor_timer.start()
                window._refresh_timer.start()
                self.assertTrue(window._rr_monitor_timer.isActive())
                self.assertTrue(window._refresh_timer.isActive())

                window.close()

                self.assertFalse(window._rr_monitor_timer.isActive())
                self.assertFalse(window._refresh_timer.isActive())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_condition_status_shows_attached_stop_and_take_profit_orders(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                ledger = SimpleNamespace(
                    status="protected",
                    stop_loss_order=SimpleNamespace(algo_id="stop-algo-1", state="live", trigger_price=Decimal("59000")),
                    take_profit_order=SimpleNamespace(algo_id="take-algo-1", state="live", trigger_price=Decimal("62000")),
                    events=(),
                    filled_size=Decimal("3"),
                    remaining_size=Decimal("0"),
                )
                item = {"rr_id": "rr-1", "side": "long", "price_entry": "60000", "price_stop": "59000", "price_tp": "62000", "r_multiple": "2"}
                with patch.object(window, "_rr_ledger_entry_for_item", return_value=ledger):
                    window._refresh_rr_tracking_summary(item)

                self.assertIn("止损条件单：已挂", window._rr_condition_status.text())
                self.assertIn("止盈条件单：已挂", window._rr_condition_status.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_execution_success_callback_runs_on_gui_thread(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                entry = RRTradeLedgerEntry(
                    entry_id="thread-callback-entry",
                    status="entry_working",
                    plan=SimpleNamespace(plan_id="thread-callback-plan"),
                )
                callback_threads: list[QThread] = []
                with patch.object(window, "_find_rr_trade_ledger_entry", return_value=entry):
                    window._start_rr_execution_action(
                        action=lambda: entry,
                        on_success=lambda _entry: callback_threads.append(QThread.currentThread()),
                    )
                    deadline = time.monotonic() + 3.0
                    while not callback_threads and time.monotonic() < deadline:
                        self._app.processEvents()
                        time.sleep(0.01)

                self.assertEqual(callback_threads, [self._app.thread()])
                deadline = time.monotonic() + 3.0
                while window._rr_execution_thread is not None and time.monotonic() < deadline:
                    self._app.processEvents()
                    time.sleep(0.01)
                self.assertIsNone(window._rr_execution_thread)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_cancel_rr_keeps_workspace_payload_unchanged(self) -> None:
        rr_payload = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 1195.0, "bar_stop": 1195.0,
            "price_entry": "62295.3", "price_stop": "60893.7", "price_tp": "63416.6",
            "r_multiple": "0.8", "management_mode": "fixed_tp", "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}}),
        ):
            window = KlineAnalysisWindow()
            try:
                before = dict(window._workspace_entry()["rr"][0])
                ledger = SimpleNamespace()
                window._runtime = SimpleNamespace(credentials=object())
                window._rr_table.setCurrentCell(0, 0)
                with (
                    patch.object(window, "_rr_ledger_entry_for_item", return_value=ledger),
                    patch.object(window, "_start_rr_execution_action") as start_execution,
                ):
                    window._cancel_selected_rr_trade()

                self.assertEqual(window._workspace_entry()["rr"][0], before)
                self.assertEqual(start_execution.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_status_rows_keep_fixed_height_when_trade_state_changes(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertEqual(window._rr_tracking_summary.minimumHeight(), window._rr_tracking_summary.maximumHeight())
                self.assertEqual(window._rr_condition_status.minimumHeight(), window._rr_condition_status.maximumHeight())
                self.assertFalse(window._rr_condition_status.wordWrap())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_left_control_panel_is_vertically_scrollable_and_compact(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertIs(window._control_scroll.widget(), window._control_panel)
                self.assertTrue(window._control_scroll.widgetResizable())
                self.assertEqual(window._control_scroll.horizontalScrollBarPolicy(), Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
                self.assertEqual(window._control_scroll.verticalScrollBarPolicy(), Qt.ScrollBarPolicy.ScrollBarAsNeeded)
                self.assertLessEqual(window._line_table.minimumHeight(), 112)
                self.assertLessEqual(window._event_log.minimumHeight(), 84)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_overlay_shows_base_quantity_with_contract_count(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", settle_ccy="USDT", state="live", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        snapshot = _build_rr_overlay_snapshot(
            {
                "side": "long", "price_entry": "60000", "price_stop": "59000", "price_tp": "62000",
                "r_multiple": "2", "risk_amount": "30", "leverage": "1",
            },
            instrument=instrument,
            price_increment=Decimal("0.1"),
        )

        self.assertIn("币量 0.03 BTC (3张)", snapshot["overlay_mid_text"])

    def test_kline_rr_confirmation_position_text_prefers_base_size_with_contracts(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", settle_ccy="USDT", state="live", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        plan = build_rr_trade_plan(
            plan_id="moni:BTC-USDT-SWAP:rr-1", profile_name="moni", environment="demo", instrument=instrument,
            direction="long", entry_execution_mode="limit", management_mode="fixed_tp", trigger_price_type="last",
            risk_amount=Decimal("30"), entry_price=Decimal("60000"), stop_loss_price=Decimal("59000"),
            direct_take_profit_r=Decimal("2"), round_trip_fee_rate=Decimal("0"),
        )

        self.assertEqual(_rr_plan_position_text(plan), "0.03 BTC (3张)")

    def test_kline_rr_trade_plan_uses_fee_offset_for_protection_management(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP", inst_type="SWAP", tick_size=Decimal("0.1"), lot_size=Decimal("1"), min_size=Decimal("1"),
            state="live", settle_ccy="USDT", ct_val=Decimal("0.01"), ct_mult=Decimal("1"), ct_val_ccy="BTC", uly="BTC-USDT", inst_family="BTC-USDT",
        )
        runtime = ArbitrageTradeRuntime(
            credentials=SimpleNamespace(profile_name="moni"), environment="demo", trade_mode="cross", position_mode="net", credential_profile_name="moni",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._runtime = runtime
                window._rr_entry_edit.setText("60000")
                window._rr_stop_edit.setText("59000")
                window._rr_r_edit.setValue(5.0)
                window._rr_bar_edit.setText("12")
                window._rr_fee_offset_check.setChecked(True)
                window._save_rr_item()
                with patch.object(window, "_instrument_for_symbol", return_value=instrument):
                    plan = window._build_selected_rr_trade_plan()

                self.assertGreater(plan.round_trip_fee_rate, Decimal("0"))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_save_rr_item_supports_trail_after_1r_management_mode(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_side_combo.setCurrentIndex(0)
                window._rr_entry_edit.setText("60000")
                window._rr_stop_edit.setText("59000")
                window._rr_r_edit.setValue(5.0)
                window._rr_management_mode_combo.setCurrentIndex(window._rr_management_mode_combo.findData("trail_after_1r"))
                window._rr_bar_edit.setText("12")

                window._save_rr_item()

                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["management_mode"], "trail_after_1r")
                self.assertEqual(saved["direct_take_profit_r"], "5")
                self.assertEqual(saved["management_trigger_price"], "61000")
                self.assertEqual(window._rr_table.item(0, 4).text(), "1:1到保本")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_r_multiple_spinbox_uses_point_one_step(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertIsInstance(window._rr_r_edit, QDoubleSpinBox)
                self.assertAlmostEqual(window._rr_r_edit.singleStep(), 0.1)
                self.assertEqual(window._rr_r_edit.decimals(), 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_save_rr_item_applies_fee_offset_to_take_profit(self) -> None:
        instrument = SimpleNamespace(tick_size=Decimal("0.1"))
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
            patch("roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument", return_value=instrument),
            patch("roll_terminal_qt.kline_analysis_window._dynamic_two_taker_fee_offset_live", return_value=Decimal("0.2")),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_side_combo.setCurrentIndex(0)
                window._rr_entry_edit.setText("100")
                window._rr_stop_edit.setText("95")
                window._rr_r_edit.setValue(2.0)
                window._rr_fee_offset_check.setChecked(True)
                window._rr_bar_edit.setText("12")

                window._save_rr_item()

                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["price_tp"], "110.2")
                self.assertEqual(saved["r_multiple"], "2")
                self.assertTrue(saved["fee_offset_enabled"])
                self.assertIn("110.2", window._rr_preview.text())
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_preview_applies_fee_offset_to_take_profit(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        instrument = SimpleNamespace(tick_size=Decimal("0.1"))
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch("roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument", return_value=instrument),
            patch("roll_terminal_qt.kline_analysis_window._dynamic_two_taker_fee_offset_live", return_value=Decimal("0.2")),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setValue(2.0)
                window._rr_fee_offset_check.setChecked(True)
                window._set_draw_tool("rr_long")
                window._pending_rr_start = ("long", 1, 105.0)

                window._update_draw_preview(candle_time=2, price=100.0)

                preview = getattr(window._native_chart_view, "_preview_rr_item", None)
                self.assertIsInstance(preview, dict)
                self.assertEqual(preview["r_multiple"], "2")
                self.assertEqual(preview["price_tp"], 115.2)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_render_to_chart_passes_workspace_rr_items_to_native_chart_context(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 1.0,
            "bar_stop": 1.0,
            "price_entry": "1.5",
            "price_stop": "1.0",
            "price_tp": "2.5",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window._native_chart_view, "set_chart_context") as set_chart_context:
                    window._render_to_chart(payload)
                self.assertEqual(len(set_chart_context.call_args.kwargs["workspace_rr_items"]), 1)
                self.assertEqual(set_chart_context.call_args.kwargs["workspace_rr_items"][0]["rr_id"], "rr-1")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_selection_rerenders_loaded_chart(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = KlineChartPayload(
                    candles=[{"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}],
                    ema_9=[],
                    ema_21=[],
                    ema_55=[],
                    trend_indicator=[],
                    signal_markers=[],
                    box_overlays=[],
                    raw_candles=[],
                    stats={},
                    alert_snapshot=None,
                )
                with patch.object(window, "_render_to_chart") as render_to_chart:
                    window._rr_table.setCurrentCell(0, 0)
                render_to_chart.assert_called_once_with(window._pending_payload)
                self.assertEqual(window._selected_rr_index, 0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_selection_formats_form_prices_and_preview(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "short",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "65492.39757787627",
            "price_stop": "61890.36391413952",
            "price_tp": "72696.46490534977",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument",
                return_value=SimpleNamespace(tick_size=Decimal("0.1")),
            ),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._rr_table.setCurrentCell(0, 0)
                self.assertEqual(window._rr_entry_edit.text(), "65492.4")
                self.assertEqual(window._rr_stop_edit.text(), "61890.4")
                self.assertEqual(window._rr_preview.text(), "自动止盈：72696.5")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_workspace_uses_compact_tracking_summary(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "62530.6",
            "price_stop": "61414.4",
            "price_tp": "64763.1",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                self.assertTrue(window._rr_form.isHidden())
                window._rr_table.setCurrentCell(0, 0)
                summary = window._rr_tracking_summary.text()
                self.assertIn("rr-1", summary)
                self.assertIn("多头", summary)
                self.assertIn("入场 62530.6", summary)
                self.assertIn("止损 61414.4", summary)
                self.assertIn("止盈 64763.1", summary)
                self.assertIn("R 1:2", summary)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_tracking_summary_includes_live_ledger_state(self) -> None:
        rr_payload = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 12.0, "bar_stop": 12.0,
            "price_entry": "60000", "price_stop": "59000", "price_tp": "65000", "r_multiple": "5", "locked": False,
        }
        ledger_entry = SimpleNamespace(
            plan=SimpleNamespace(plan_id="moni:BTC-USDT-SWAP:rr-1"),
            status="protected_trailing",
            filled_size=Decimal("10"),
            remaining_size=Decimal("0"),
            stop_loss_order=SimpleNamespace(trigger_price=Decimal("62000")),
            events=(SimpleNamespace(message="stop moved to lock 2R"),),
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}}),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window, "_matching_rr_trade_ledger_entries", return_value=[ledger_entry]):
                    window._rr_table.setCurrentCell(0, 0)
                summary = window._rr_tracking_summary.text()
                self.assertIn("锁盈中", summary)
                self.assertIn("成交 10张", summary)
                self.assertIn("剩余 0张", summary)
                self.assertIn("当前止损 62000", summary)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_window_layout_refresh_rerenders_loaded_chart(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = payload
                window._page_ready = True
                with (
                    patch.object(window, "_apply_secondary_chart_layout") as apply_secondary_layout,
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._refresh_chart_layout_after_window_change()
                apply_secondary_layout.assert_called_once()
                render_to_chart.assert_called_once_with(payload)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_table_click_selects_and_double_click_opens_rr_card_dialog(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 12.0,
            "bar_stop": 12.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window, "_open_rr_card_for_selected") as open_card:
                    window._rr_table.cellClicked.emit(0, 0)
                    open_card.assert_not_called()
                    window._rr_table.cellDoubleClicked.emit(0, 0)
                open_card.assert_called_once()
                self.assertEqual(window._selected_rr_index, 0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_render_to_chart_enriches_rr_overlay_labels(self) -> None:
        payload = KlineChartPayload(
            candles=[{"time": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 1.0,
            "price_entry": "60000",
            "price_stop": "59000",
            "price_tp": "62000",
            "r_multiple": "2",
            "risk_amount": "100",
            "account_size": "1000",
            "locked": False,
        }
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
            patch("roll_terminal_qt.kline_analysis_window.OkxRestClient.get_instrument", return_value=instrument),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window._native_chart_view, "set_chart_context") as set_chart_context:
                    window._render_to_chart(payload)
                rr_item = set_chart_context.call_args.kwargs["workspace_rr_items"][0]
                self.assertIn("止盈", rr_item["overlay_tp_text"])
                self.assertIn("入场", rr_item["overlay_entry_text"])
                self.assertIn("RR", rr_item["overlay_mid_text"])
            finally:
                self.__class__.dispose_widget(window)

    def test_rr_card_dialog_uses_dark_tabs_layout(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        dialog = RRCardDialog(
            parent=None,
            item={
                "rr_id": "rr-1",
                "side": "long",
                "bar_entry": 1,
                "price_entry": "60000",
                "price_stop": "59000",
                "price_tp": "62000",
                "r_multiple": "2",
                "risk_amount": "100",
                "account_size": "1000",
            },
            instrument=instrument,
            symbol="BTC-USDT-SWAP",
            period="1H",
            price_increment=Decimal("0.1"),
        )
        try:
            tabs = dialog.findChild(QTabWidget)
            self.assertEqual(tabs.count(), 4)
            self.assertIn("#0f172a", dialog.styleSheet())
            self.assertIn("QDoubleSpinBox", dialog.styleSheet())
            self.assertIn("QCheckBox", dialog.styleSheet())
            self.assertIn("QTabBar::tab:selected", dialog.styleSheet())
            self.assertIn("background: #0f172a", dialog.styleSheet())
            self.assertIn("color: #e5edf7", dialog.styleSheet())
        finally:
            self.__class__.dispose_widget(dialog)

    def test_rr_card_dialog_omits_account_size_field(self) -> None:
        instrument = SimpleNamespace(
            inst_id="BTC-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_size=Decimal("1"),
            state="live",
            settle_ccy="USDT",
            ct_val=Decimal("0.01"),
            ct_mult=Decimal("1"),
            ct_val_ccy="BTC",
            uly="BTC-USDT",
            inst_family="BTC-USDT",
        )
        dialog = RRCardDialog(
            parent=None,
            item={
                "rr_id": "rr-1",
                "side": "long",
                "bar_entry": 1,
                "price_entry": "60000",
                "price_stop": "59000",
                "price_tp": "62000",
                "r_multiple": "2",
                "risk_amount": "100",
                "account_size": "1000",
            },
            instrument=instrument,
            symbol="BTC-USDT-SWAP",
            period="1H",
            price_increment=Decimal("0.1"),
        )
        try:
            label_texts = {label.text() for label in dialog.findChildren(QLabel)}
            self.assertNotIn("账户规模", label_texts)
            dialog.accept()
            payload = dialog.result_payload()
            self.assertIsInstance(payload, dict)
            self.assertNotIn("account_size", payload)
        finally:
            self.__class__.dispose_widget(dialog)

    def test_kline_chart_click_prefers_rr_selection_and_clears_line_selection(self) -> None:
        line_payload = {
            "kind": "horizontal",
            "label": "L1",
            "trigger": "notify",
            "action": "notify",
            "time_a": 1,
            "price_a": 100.0,
            "time_b": 1,
            "price_b": 100.0,
            "enabled": True,
        }
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 110.0, "low": 103.0, "close": 107.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={
                    f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {
                        "lines": [line_payload],
                        "rr": [rr_payload],
                    }
                },
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_table.setCurrentCell(0, 0)
                window._line_table.setCurrentCell(0, 0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", return_value=(1, 105.0)),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._selected_line_index, -1)
                self.assertEqual(window._rr_table.currentRow(), 0)
                self.assertEqual(window._line_table.currentRow(), -1)
                render_to_chart.assert_called_once_with(chart_payload)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_click_keeps_line_selection_when_rr_not_hit(self) -> None:
        line_payload = {
            "kind": "horizontal",
            "label": "L1",
            "trigger": "notify",
            "action": "notify",
            "time_a": 1,
            "price_a": 100.0,
            "time_b": 1,
            "price_b": 100.0,
            "enabled": True,
        }
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 1.0,
            "bar_stop": 1.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 99.0, "high": 103.0, "low": 98.0, "close": 100.0, "volume": 10.0},
                {"time": 2, "open": 100.0, "high": 110.0, "low": 99.0, "close": 107.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={
                    f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {
                        "lines": [line_payload],
                        "rr": [rr_payload],
                    }
                },
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                with (
                    patch.object(window, "_resolve_primary_chart_click", return_value=(1, 100.0)),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                self.assertEqual(window._selected_line_index, 0)
                self.assertEqual(window._selected_rr_index, -1)
                self.assertEqual(window._line_table.currentRow(), 0)
                self.assertEqual(window._rr_table.currentRow(), -1)
                render_to_chart.assert_called_once_with(chart_payload)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_long_draw_tool_appends_workspace_rr(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 110.0, "low": 100.0, "close": 107.0, "volume": 8.0},
                {"time": 3, "open": 107.0, "high": 112.0, "low": 104.0, "close": 110.0, "volume": 9.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (2, 100.0)]),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                entry = window._workspace_entry()
                rr_items = entry.get("rr", [])
                self.assertEqual(len(rr_items), 1)
                saved = rr_items[0]
                self.assertEqual(saved["side"], "long")
                self.assertEqual(saved["price_entry"], "105")
                self.assertEqual(saved["price_stop"], "100")
                self.assertEqual(saved["price_tp"], "115")
                self.assertEqual(saved["r_multiple"], "2")
                self.assertEqual(saved["bar_entry"], 0)
                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_table.currentRow(), 0)
                self.assertEqual(window._draw_tool, "none")
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_long_reverse_drag_still_appends_workspace_rr(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 104.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 100.0), (2, 105.0)]),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                rr_items = window._workspace_entry().get("rr", [])
                self.assertEqual(len(rr_items), 1)
                self.assertEqual(rr_items[0]["price_entry"], "105")
                self.assertEqual(rr_items[0]["price_stop"], "100")
                self.assertEqual(rr_items[0]["price_tp"], "115")
                self.assertEqual(window._draw_tool, "none")
                self.assertIsNone(window._pending_rr_start)
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_new_rr_uses_a_fresh_id_when_another_rr_is_selected(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], raw_candles=[], stats={}, alert_snapshot=None,
        )
        existing_rr = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 0.0, "bar_stop": 0.0,
            "price_entry": "105", "price_stop": "100", "price_tp": "115", "r_multiple": "2", "locked": False,
        }
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [existing_rr]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._selected_rr_index = 0
                window._rr_r_edit.setText("2")

                self.assertTrue(
                    window._append_rr_rule_from_chart(
                        side="long",
                        entry_candle_time=2,
                        entry_price=110.0,
                        stop_candle_time=2,
                        stop_price=105.0,
                    )
                )

                self.assertEqual(
                    [item["rr_id"] for item in window._workspace_entry()["rr"]],
                    ["rr-1", "rr-2"],
                )
                self.assertTrue(window._workspace_entry()["rr"][1]["trade_ref"])
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_long_invalid_equal_stop_keeps_tool_for_retry(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 104.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                with patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (2, 105.0)]):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                entry = window._workspace_entry()
                self.assertEqual(entry.get("rr", []), [])
                self.assertEqual(window._draw_tool, "rr_long")
                self.assertEqual(window._pending_rr_start, ("long", 1, 105.0))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_short_draw_tool_appends_workspace_rr(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 107.0, "volume": 8.0},
                {"time": 3, "open": 107.0, "high": 112.0, "low": 104.0, "close": 110.0, "volume": 9.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_short")
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (2, 110.0)]),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                entry = window._workspace_entry()
                rr_items = entry.get("rr", [])
                self.assertEqual(len(rr_items), 1)
                saved = rr_items[0]
                self.assertEqual(saved["side"], "short")
                self.assertEqual(saved["price_entry"], "105")
                self.assertEqual(saved["price_stop"], "110")
                self.assertEqual(saved["price_tp"], "95")
                self.assertEqual(saved["r_multiple"], "2")
                self.assertEqual(saved["bar_entry"], 0)
                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_table.currentRow(), 0)
                self.assertEqual(window._draw_tool, "none")
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_short_reverse_drag_still_appends_workspace_rr(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 107.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_short")
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 110.0), (2, 105.0)]),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                rr_items = window._workspace_entry().get("rr", [])
                self.assertEqual(len(rr_items), 1)
                self.assertEqual(rr_items[0]["price_entry"], "105")
                self.assertEqual(rr_items[0]["price_stop"], "110")
                self.assertEqual(rr_items[0]["price_tp"], "95")
                self.assertEqual(window._draw_tool, "none")
                self.assertIsNone(window._pending_rr_start)
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_short_invalid_equal_stop_keeps_tool_for_retry(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 104.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_short")
                with patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (2, 105.0)]):
                    window._on_native_chart_clicked(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                entry = window._workspace_entry()
                self.assertEqual(entry.get("rr", []), [])
                self.assertEqual(window._draw_tool, "rr_short")
                self.assertEqual(window._pending_rr_start, ("short", 1, 105.0))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_pointer_drag_release_appends_workspace_rr_once(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 110.0, "low": 100.0, "close": 107.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (2, 100.0)]),
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                    window._on_native_chart_clicked(0.0, 0.0)
                rr_items = window._workspace_entry().get("rr", [])
                self.assertEqual(len(rr_items), 1)
                self.assertEqual(rr_items[0]["price_entry"], "105")
                self.assertEqual(rr_items[0]["price_stop"], "100")
                self.assertEqual(rr_items[0]["price_tp"], "115")
                self.assertEqual(window._draw_tool, "none")
                self.assertIsNone(window._pending_rr_start)
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_preview_updates_after_first_click(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                window._pending_rr_start = ("long", 1, 105.0)

                window._update_draw_preview(candle_time=2, price=100.0)

                preview = getattr(window._native_chart_view, "_preview_rr_item", None)
                self.assertIsInstance(preview, dict)
                self.assertEqual(preview["side"], "long")
                self.assertEqual(preview["price_entry"], 105.0)
                self.assertEqual(preview["price_stop"], 100.0)
                self.assertEqual(preview["price_tp"], 115.0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_short_preview_updates_after_first_click(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_short")
                window._pending_rr_start = ("short", 1, 105.0)

                window._update_draw_preview(candle_time=2, price=110.0)

                preview = getattr(window._native_chart_view, "_preview_rr_item", None)
                self.assertIsInstance(preview, dict)
                self.assertEqual(preview["side"], "short")
                self.assertEqual(preview["price_entry"], 105.0)
                self.assertEqual(preview["price_stop"], 110.0)
                self.assertEqual(preview["price_tp"], 95.0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_long_preview_normalizes_reverse_drag(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                window._pending_rr_start = ("long", 1, 100.0)

                window._update_draw_preview(candle_time=2, price=105.0)

                preview = getattr(window._native_chart_view, "_preview_rr_item", None)
                self.assertIsInstance(preview, dict)
                self.assertEqual(preview["side"], "long")
                self.assertEqual(preview["price_entry"], 105.0)
                self.assertEqual(preview["price_stop"], 100.0)
                self.assertEqual(preview["price_tp"], 115.0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_bar_index_for_candle_time_supports_future_blank_area(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1_700_000_000, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 1_700_000_900, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                self.assertEqual(window._bar_index_for_candle_time(1_700_001_800), 2)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_preview_supports_future_blank_area_start(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1_700_000_000, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 1_700_000_900, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                window._pending_rr_start = ("long", 1_700_001_800, 105.0)

                window._update_draw_preview(candle_time=1_700_002_700, price=100.0)

                preview = getattr(window._native_chart_view, "_preview_rr_item", None)
                self.assertIsInstance(preview, dict)
                self.assertEqual(preview["bar_entry"], 2)
                self.assertEqual(preview["price_entry"], 105.0)
                self.assertEqual(preview["price_stop"], 100.0)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_rr_preview_clears_after_rr_saved(self) -> None:
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": []}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_r_edit.setText("2")
                window._set_draw_tool("rr_long")
                window._pending_rr_start = ("long", 1, 105.0)
                window._update_draw_preview(candle_time=2, price=100.0)
                self.assertIsInstance(getattr(window._native_chart_view, "_preview_rr_item", None), dict)

                with (
                    patch.object(window, "_resolve_primary_chart_click", return_value=(2, 100.0)),
                    patch.object(window, "_render_to_chart"),
                ):
                    window._on_native_chart_clicked(0.0, 0.0)
                self.assertIsNone(getattr(window._native_chart_view, "_preview_rr_item", None))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_drag_selected_rr_stop_updates_workspace_payload(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_rr_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 100.0), (1, 98.0), (1, 98.0)]),
                    patch.object(window, "_current_chart_pointer_scene_pos", side_effect=[QPointF(10.0, 10.0), QPointF(22.0, 10.0)]),
                    patch.object(window, "_save_workspace_snapshot") as save_snapshot,
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_moved(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                entry = window._workspace_entry()
                saved = entry["rr"][0]
                self.assertEqual(saved["price_stop"], "98")
                self.assertEqual(saved["price_tp"], "119")
                self.assertEqual(saved["r_multiple"], "2")
                self.assertEqual(window._selected_rr_index, 0)
                self.assertEqual(window._rr_stop_edit.text(), "98.0")
                save_snapshot.assert_called_once_with()
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_drag_selected_rr_entry_shifts_entire_block(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_rr_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 105.0), (1, 107.0), (1, 107.0)]),
                    patch.object(window, "_current_chart_pointer_scene_pos", side_effect=[QPointF(10.0, 10.0), QPointF(24.0, 10.0)]),
                    patch.object(window, "_save_workspace_snapshot"),
                    patch.object(window, "_render_to_chart"),
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_moved(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["price_entry"], "107")
                self.assertEqual(saved["price_stop"], "102")
                self.assertEqual(saved["price_tp"], "117")
                self.assertEqual(saved["r_multiple"], "2")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_hit_test_returns_move_mode_inside_unlocked_box(self) -> None:
        rr_payload = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 0.0, "bar_stop": 0.0,
            "price_entry": "105", "price_stop": "100", "price_tp": "115", "r_multiple": "2", "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], raw_candles=[], stats={}, alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                with patch.object(window, "_rr_ledger_entry_for_item", return_value=SimpleNamespace(status="cancelled")):
                    self.assertEqual(window._rr_hit_test(candle_time=2, price=108.0), {"index": 0, "drag_mode": "rr_move"})
                    self.assertEqual(window._rr_hit_test(candle_time=2, price=105.0), {"index": 0, "drag_mode": "rr_move"})
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_editing_is_blocked_only_for_active_trade_states(self) -> None:
        self.assertFalse(_rr_ledger_blocks_editing(None))
        self.assertFalse(_rr_ledger_blocks_editing(SimpleNamespace(status="cancelled")))
        self.assertFalse(_rr_ledger_blocks_editing(SimpleNamespace(status="manual_review")))
        self.assertTrue(_rr_ledger_blocks_editing(SimpleNamespace(status="entry_working")))
        self.assertTrue(_rr_ledger_blocks_editing(SimpleNamespace(status="entry_partially_filled")))
        self.assertTrue(_rr_ledger_blocks_editing(SimpleNamespace(status="protected")))
        self.assertTrue(_rr_ledger_blocks_editing(SimpleNamespace(status="protected_cancelled_remainder")))

    def test_kline_rr_legacy_ledger_link_rejects_same_id_with_different_prices(self) -> None:
        item = {
            "rr_id": "rr-1", "side": "long",
            "price_entry": "63185.17000697939", "price_stop": "61030.30564832505", "price_tp": "67494.9",
        }
        stale_entry = SimpleNamespace(
            plan=SimpleNamespace(
                plan_id="moni:BTC-USDT-SWAP:rr-1",
                direction="long",
                entry_price=Decimal("62821.696303743282"),
                stop_loss_price=Decimal("59963.824282203116"),
                take_profit_price=Decimal("66822.7"),
            ),
            status="entry_working",
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}),
        ):
            window = KlineAnalysisWindow()
            try:
                with patch.object(window, "_matching_rr_trade_ledger_entries", return_value=[stale_entry]):
                    self.assertIsNone(window._rr_ledger_entry_for_item(item))
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_rr_move_drag_translates_time_and_prices_from_press_anchor(self) -> None:
        rr_payload = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 0.0, "bar_stop": 0.0,
            "price_entry": "105", "price_stop": "100", "price_tp": "115", "r_multiple": "2", "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], raw_candles=[], stats={}, alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}}),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._rr_drag_state = {
                    "index": 0, "drag_mode": "rr_move", "anchor_candle_time": 1, "anchor_price": 108.0,
                    "anchor_rr": dict(rr_payload), "active": True,
                }

                self.assertTrue(window._apply_rr_drag_update(candle_time=2, price=110.0))

                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["bar_entry"], 1.0)
                self.assertEqual(saved["bar_stop"], 1.0)
                self.assertEqual(saved["price_entry"], "107")
                self.assertEqual(saved["price_stop"], "102")
                self.assertEqual(saved["price_tp"], "117")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_drag_selected_rr_take_profit_updates_r_multiple(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "short",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "110",
            "price_tp": "95",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_rr_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 95.0), (1, 90.0), (1, 90.0)]),
                    patch.object(window, "_current_chart_pointer_scene_pos", side_effect=[QPointF(10.0, 10.0), QPointF(26.0, 10.0)]),
                    patch.object(window, "_rr_ledger_entry_for_item", return_value=None),
                    patch.object(window, "_save_workspace_snapshot"),
                    patch.object(window, "_render_to_chart"),
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_moved(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["price_entry"], "105")
                self.assertEqual(saved["price_stop"], "110")
                self.assertEqual(saved["price_tp"], "90")
                self.assertEqual(saved["r_multiple"], "3")
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_click_selected_rr_handle_does_not_update_workspace_payload(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_rr_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 100.0), (1, 100.0)]),
                    patch.object(window, "_current_chart_pointer_scene_pos", return_value=QPointF(10.0, 10.0)),
                    patch.object(window, "_save_workspace_snapshot") as save_snapshot,
                    patch.object(window, "_render_to_chart"),
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                saved = window._workspace_entry()["rr"][0]
                self.assertEqual(saved["price_entry"], "105")
                self.assertEqual(saved["price_stop"], "100")
                self.assertEqual(saved["price_tp"], "115")
                self.assertIsNone(window._rr_drag_state)
                save_snapshot.assert_not_called()
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_first_click_on_unselected_rr_only_selects_it(self) -> None:
        first_rr = {
            "rr_id": "rr-1", "side": "long", "bar_entry": 0.0, "bar_stop": 0.0,
            "price_entry": "105", "price_stop": "100", "price_tp": "115", "r_multiple": "2", "locked": False,
        }
        second_rr = {
            "rr_id": "rr-2", "side": "short", "bar_entry": 1.0, "bar_stop": 1.0,
            "price_entry": "110", "price_stop": "115", "price_tp": "100", "r_multiple": "2", "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 116.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[], ema_21=[], ema_55=[], trend_indicator=[], signal_markers=[], box_overlays=[], raw_candles=[], stats={}, alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [first_rr, second_rr]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_rr_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", return_value=(2, 110.0)),
                    patch.object(window, "_rr_hit_test", return_value={"index": 1, "drag_mode": "rr_move"}),
                    patch.object(window, "_render_to_chart"),
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)

                self.assertEqual(window._selected_rr_index, 1)
                self.assertIsNone(window._rr_drag_state)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_drag_selected_first_line_keeps_index_zero(self) -> None:
        line_payload = {
            "id": "line-1",
            "kind": "horizontal",
            "label": "L1",
            "trigger": "touch",
            "action": "notify",
            "time_a": 1,
            "price_a": 100.0,
            "time_b": 1,
            "price_b": 100.0,
            "enabled": True,
            "color": "#1d4ed8",
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 99.0, "high": 103.0, "low": 98.0, "close": 100.0, "volume": 10.0},
                {"time": 2, "open": 100.0, "high": 110.0, "low": 99.0, "close": 107.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"lines": [line_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                window._populate_line_table(selected_index=0)
                with (
                    patch.object(window, "_resolve_primary_chart_click", side_effect=[(1, 100.0), (1, 98.0)]),
                    patch.object(window, "_save_workspace_snapshot") as save_snapshot,
                    patch.object(window, "_render_to_chart") as render_to_chart,
                ):
                    window._on_chart_pointer_pressed(0.0, 0.0)
                    window._on_chart_pointer_released(0.0, 0.0)
                entry = window._workspace_entry()
                saved = entry["lines"][0]
                self.assertEqual(saved["price_a"], 98.0)
                self.assertEqual(saved["price_b"], 98.0)
                self.assertEqual(window._selected_line_index, 0)
                save_snapshot.assert_called_once_with()
                self.assertGreaterEqual(render_to_chart.call_count, 1)
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_chart_hover_rr_sets_move_cursor(self) -> None:
        rr_payload = {
            "rr_id": "rr-1",
            "side": "long",
            "bar_entry": 0.0,
            "bar_stop": 0.0,
            "price_entry": "105",
            "price_stop": "100",
            "price_tp": "115",
            "r_multiple": "2",
            "locked": False,
        }
        chart_payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 102.0, "high": 108.0, "low": 99.0, "close": 105.0, "volume": 10.0},
                {"time": 2, "open": 105.0, "high": 112.0, "low": 100.0, "close": 110.0, "volume": 8.0},
            ],
            ema_9=[],
            ema_21=[],
            ema_55=[],
            trend_indicator=[],
            signal_markers=[],
            box_overlays=[],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
            patch(
                "roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries",
                return_value={f"BTC-USDT-SWAP|{_DEFAULT_SINGLE_CHART_PERIOD}": {"rr": [rr_payload]}},
            ),
        ):
            window = KlineAnalysisWindow()
            try:
                window._pending_payload = chart_payload
                with (
                    patch.object(window, "_resolve_primary_chart_click", return_value=(1, 100.0)),
                    patch.object(window, "_line_hit_test", return_value=None),
                    patch.object(window, "_rr_hit_test", return_value={"index": 0, "drag_mode": "rr_stop"}),
                    patch.object(window._native_chart_view, "set_interaction_cursor_mode") as set_cursor,
                ):
                    window._on_chart_pointer_moved(0.0, 0.0)
                set_cursor.assert_called_once_with("move")
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

    def test_reverse_kline_chart_payload_mirrors_price_series_around_first_close(self) -> None:
        payload = KlineChartPayload(
            candles=[
                {"time": 1, "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1.0},
                {"time": 2, "open": 109.0, "high": 112.0, "low": 101.0, "close": 106.0, "volume": 2.0},
            ],
            ema_9=[{"time": 1, "value": 105.0}, {"time": 2, "value": 107.0}],
            ema_21=[{"time": 1, "value": 104.0}, {"time": 2, "value": 106.0}],
            ema_55=[{"time": 1, "value": 103.0}, {"time": 2, "value": 105.0}],
            trend_indicator=[{"time": 1, "value": 0.5}],
            signal_markers=[{"time": 1, "direction": "long"}],
            box_overlays=[{"start_index": 0, "end_index": 1, "upper": 112.0, "lower": 90.0}],
            raw_candles=[],
            stats={},
            alert_snapshot=None,
        )

        reversed_payload = _reverse_kline_chart_payload(payload)

        self.assertEqual(reversed_payload.candles[0]["open"], 105.0)
        self.assertEqual(reversed_payload.candles[0]["close"], 110.0)
        self.assertEqual(reversed_payload.candles[0]["high"], 120.0)
        self.assertEqual(reversed_payload.candles[0]["low"], 100.0)
        self.assertGreater(reversed_payload.candles[0]["close"], reversed_payload.candles[0]["open"])
        self.assertLess(reversed_payload.candles[1]["close"], reversed_payload.candles[1]["open"])
        self.assertEqual(reversed_payload.candles[1]["open"], 104.0)
        self.assertEqual(reversed_payload.candles[1]["close"], 101.0)
        self.assertEqual(reversed_payload.ema_9[1]["value"], 103.0)
        self.assertEqual(reversed_payload.box_overlays[0]["upper"], 120.0)
        self.assertEqual(reversed_payload.box_overlays[0]["lower"], 98.0)
        self.assertTrue(reversed_payload.stats["reverse_kline"])
        self.assertEqual(reversed_payload.stats["reverse_anchor_price"], 105.0)

    def test_build_box_current_overlay_returns_current_effective_box(self) -> None:
        candles = [SimpleNamespace(open=100.0, high=101.0, low=99.0, close=100.0) for _ in range(60)]
        with patch(
            "roll_terminal_qt.kline_analysis_window.detect_boxes",
            return_value=[
                SimpleNamespace(
                    start_index=8,
                    end_index=59,
                    upper=Decimal("103"),
                    lower=Decimal("97"),
                    upper_touches=2,
                    lower_touches=3,
                    violations=0,
                    score=Decimal("91"),
                )
            ],
        ):
            overlays = _build_box_current_overlay(candles)

        self.assertEqual(len(overlays), 1)
        self.assertEqual(overlays[0]["mode"], "current")
        self.assertEqual(overlays[0]["start_index"], 8)
        self.assertEqual(overlays[0]["end_index"], 59)
        self.assertEqual(overlays[0]["touches"], 5)

    def test_build_box_current_overlay_rejects_too_long_box(self) -> None:
        candles = [SimpleNamespace(open=100.0, high=101.0, low=99.0, close=100.0) for _ in range(80)]
        with patch(
            "roll_terminal_qt.kline_analysis_window.detect_boxes",
            return_value=[
                SimpleNamespace(
                    start_index=0,
                    end_index=79,
                    upper=Decimal("103"),
                    lower=Decimal("97"),
                    upper_touches=4,
                    lower_touches=4,
                    violations=0,
                    score=Decimal("120"),
                )
            ],
        ):
            self.assertEqual(_build_box_current_overlay(candles), [])

    def test_build_box_current_overlay_rejects_too_wide_box(self) -> None:
        candles = [SimpleNamespace(open=100.0, high=101.0, low=99.0, close=100.0) for _ in range(60)]
        with patch(
            "roll_terminal_qt.kline_analysis_window.detect_boxes",
            return_value=[
                SimpleNamespace(
                    start_index=12,
                    end_index=59,
                    upper=Decimal("110"),
                    lower=Decimal("90"),
                    upper_touches=4,
                    lower_touches=4,
                    violations=0,
                    score=Decimal("95"),
                )
            ],
        ):
            self.assertEqual(_build_box_current_overlay(candles), [])

    def test_extend_history_box_end_index_advances_until_breakout_bar(self) -> None:
        highs = [101.0, 101.0, 101.2, 101.3, 101.1, 101.0]
        lows = [99.0, 99.2, 99.1, 99.0, 99.3, 98.2]
        opens = [100.0, 100.2, 100.1, 100.4, 100.3, 99.0]
        closes = [100.1, 100.1, 100.5, 100.6, 100.4, 98.4]
        atr_values = [0.8, 0.8, 0.8, 0.8, 0.8, 0.8]

        end_index = _extend_history_box_end_index(
            start_index=0,
            end_index=2,
            upper=101.4,
            lower=98.9,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            atr_values=atr_values,
        )

        self.assertEqual(end_index, 4)

    def test_build_box_history_overlays_extends_box_until_breakout_bar(self) -> None:
        candles = []
        for index in range(60):
            close = 100.2
            low = 99.2
            if index == 39:
                close = 97.9
                low = 97.7
            candles.append(SimpleNamespace(open=100.0, high=101.0, low=low, close=close))

        def fake_score_manual_style_box_window(**kwargs):
            if kwargs["start_index"] == 25 and kwargs["end_index"] == 36:
                return {
                    "start_index": 25,
                    "end_index": 36,
                    "upper": 101.4,
                    "lower": 98.9,
                    "touches": 4,
                    "violations": 0,
                    "trend": "up",
                    "score": 95.0,
                }
            return None

        with patch(
            "roll_terminal_qt.kline_analysis_window._score_manual_style_box_window",
            side_effect=fake_score_manual_style_box_window,
        ):
            overlays = _build_box_history_overlays(candles)

        matching = next(item for item in overlays if item["start_index"] == 25)
        self.assertEqual(matching["end_index"], 38)

    def test_visible_box_overlays_separates_current_and_history_modes(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                payload = KlineChartPayload(
                    candles=[],
                    ema_9=[],
                    ema_21=[],
                    ema_55=[],
                    trend_indicator=[],
                    signal_markers=[],
                    box_overlays=[
                        {"mode": "history", "start_index": 1, "end_index": 4, "upper": 10.0, "lower": 8.0},
                        {"mode": "current", "start_index": 5, "end_index": 9, "upper": 11.0, "lower": 9.0},
                    ],
                    raw_candles=[],
                    stats={},
                    alert_snapshot=None,
                )

                window._auto_box_check.setChecked(True)
                window._history_box_check.setChecked(False)
                current_only = window._visible_box_overlays(payload)

                window._auto_box_check.setChecked(False)
                window._history_box_check.setChecked(True)
                history_only = window._visible_box_overlays(payload)
            finally:
                self.__class__.dispose_widget(window)

        self.assertEqual([item["mode"] for item in current_only], ["current"])
        self.assertEqual([item["mode"] for item in history_only], ["history"])

    def test_request_keys_include_reverse_kline_state(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._symbol_combo.setCurrentText("BTC-USDT-SWAP")
                window._use_native_chart = True
                window._reverse_kline_check.setChecked(True)
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = "kline"
                primary_key = window._current_primary_request_key()
                secondary_key = window._current_secondary_request_key(symbol="BTC-USDT-SWAP")

                self.assertTrue(primary_key[-1])
                self.assertTrue(secondary_key[-1])
            finally:
                self.__class__.dispose_widget(window)

    def test_primary_average_secondary_normal_control_available_for_dual_kline_layouts(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                self.assertFalse(window._primary_average_secondary_normal_check.isEnabled())
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = "kline"
                window._secondary_layout_mode_value = "vertical"
                window._update_secondary_controls_state()
                self.assertTrue(window._primary_average_secondary_normal_check.isEnabled())

                window._secondary_layout_mode_value = "horizontal"
                window._update_secondary_controls_state()
                self.assertTrue(window._primary_average_secondary_normal_check.isEnabled())

                window._primary_average_secondary_normal_check.setChecked(True)
                window._secondary_chart_kind_mode = "volatility"
                window._update_secondary_controls_state()
                self.assertFalse(window._primary_average_secondary_normal_check.isEnabled())
                self.assertFalse(window._primary_average_secondary_normal_check.isChecked())
            finally:
                self.__class__.dispose_widget(window)

    def test_primary_average_secondary_normal_request_keys_use_primary_average_only(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._symbol_combo.setCurrentText("BTC-USDT-SWAP")
                window._use_native_chart = True
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = "kline"
                window._secondary_layout_mode_value = "horizontal"
                window._primary_average_secondary_normal_check.setChecked(True)

                primary_key = window._current_primary_request_key()
                secondary_key = window._current_secondary_request_key(symbol="BTC-USDT-SWAP")

                self.assertTrue(primary_key[5])
                self.assertFalse(secondary_key[6])

                window._secondary_layout_mode_value = "vertical"
                primary_key = window._current_primary_request_key()
                secondary_key = window._current_secondary_request_key(symbol="BTC-USDT-SWAP")

                self.assertTrue(primary_key[5])
                self.assertFalse(secondary_key[6])
            finally:
                self.__class__.dispose_widget(window)

    def test_primary_average_secondary_normal_is_mutually_exclusive_with_global_average(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = "kline"
                window._secondary_layout_mode_value = "horizontal"
                window._update_secondary_controls_state()

                window._secondary_average_kline_check.setChecked(True)
                window._primary_average_secondary_normal_check.setChecked(True)
                self.assertFalse(window._secondary_average_kline_check.isChecked())
                self.assertTrue(window._primary_average_secondary_normal_check.isChecked())

                window._secondary_average_kline_check.setChecked(True)
                self.assertTrue(window._secondary_average_kline_check.isChecked())
                self.assertFalse(window._primary_average_secondary_normal_check.isChecked())
            finally:
                self.__class__.dispose_widget(window)

    def test_secondary_sync_period_button_shows_same_period_switch_for_volatility(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_kind_mode = "volatility"
                window._refresh_secondary_chart_kind_button()
                window._refresh_secondary_sync_period_button()
                self.assertEqual(window._secondary_chart_kind_btn.text(), "副图K线")
                self.assertEqual(window._secondary_sync_period_btn.text(), "同周期切换")
            finally:
                self.__class__.dispose_widget(window)

    def test_switching_secondary_chart_to_volatility_resets_both_periods_to_1h_and_recent_view(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
        ):
            window = KlineAnalysisWindow()
            try:
                window._secondary_chart_check.setChecked(True)
                window._period_combo.setCurrentText("1D")
                window._secondary_period_combo.setCurrentText("4H")
                window._set_chart_view_range_mode("full")

                with (
                    patch.object(window, "_apply_chart_view_range") as apply_range,
                    patch.object(window, "_load_data") as load_data,
                ):
                    window._on_secondary_chart_kind_cycle_clicked()

                self.assertEqual(window._secondary_chart_kind(), "volatility")
                self.assertEqual(window._period_combo.currentText(), "1H")
                self.assertEqual(window._secondary_period_combo.currentText(), "1H")
                self.assertEqual(window._secondary_sync_period_btn.text(), "同周期切换")
                self.assertEqual(window._chart_view_range_mode, "recent")
                apply_range.assert_called_once()
                load_data.assert_called_once()
            finally:
                self.__class__.dispose_widget(window)

    def test_load_data_previews_cached_primary_payload_before_loader_returns(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._page_ready = True
                payload = KlineChartPayload(
                    candles=[{"time": 1, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0}],
                    ema_9=[],
                    ema_21=[],
                    ema_55=[],
                    trend_indicator=[],
                    signal_markers=[],
                    box_overlays=[],
                    raw_candles=[],
                    stats={"returned": 1},
                    alert_snapshot=None,
                )
                request_key = window._current_primary_request_key()
                window._primary_payload_cache[request_key] = payload

                with (
                    patch.object(window, "_render_loaded_payload") as render_cached,
                    patch("roll_terminal_qt.kline_analysis_window.KlineDataLoader.start") as start_loader,
                ):
                    window._load_data()

                render_cached.assert_called_once_with(payload)
                start_loader.assert_called_once()
                self.assertIs(window._pending_payload, payload)
                self.assertEqual(window._loaded_primary_request_key, request_key)
            finally:
                self.__class__.dispose_widget(window)

    def test_load_secondary_data_previews_cached_secondary_payload_before_loader_returns(self) -> None:
        with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
            window = KlineAnalysisWindow()
            try:
                window._use_native_chart = True
                window._page_ready = True
                window._secondary_chart_check.blockSignals(True)
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_check.blockSignals(False)
                payload = KlineChartPayload(
                    candles=[{"time": 1, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0}],
                    ema_9=[],
                    ema_21=[],
                    ema_55=[],
                    trend_indicator=[],
                    signal_markers=[],
                    box_overlays=[],
                    raw_candles=[],
                    stats={"returned": 1},
                    alert_snapshot=None,
                )
                request_key = window._current_secondary_request_key(symbol=window._symbol_combo.currentText().strip().upper())
                window._secondary_payload_cache[request_key] = payload

                with (
                    patch.object(window, "_render_secondary_chart") as render_cached,
                    patch("roll_terminal_qt.kline_analysis_window.KlineDataLoader.start") as start_loader,
                ):
                    window._load_secondary_data(symbol=window._symbol_combo.currentText().strip().upper())

                render_cached.assert_called_once_with(payload)
                start_loader.assert_called_once()
                self.assertIs(window._secondary_pending_payload, payload)
                self.assertEqual(window._loaded_secondary_request_key, request_key)
            finally:
                self.__class__.dispose_widget(window)

    def test_recent_view_uses_smaller_period_range_when_dual_chart_linked(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                window._secondary_chart_check.blockSignals(True)
                window._secondary_chart_check.setChecked(True)
                window._secondary_chart_check.blockSignals(False)
                window._period_combo.blockSignals(True)
                window._secondary_period_combo.blockSignals(True)
                window._period_combo.setCurrentText("1D")
                window._secondary_period_combo.setCurrentText("4H")
                window._period_combo.blockSignals(False)
                window._secondary_period_combo.blockSignals(False)
                window._set_chart_view_range_mode("recent")

                primary_view = window._native_chart_view
                secondary_view = window._secondary_native_chart_view

                with (
                    patch.object(primary_view, "set_recent_view_range") as primary_recent,
                    patch.object(secondary_view, "set_recent_view_range") as secondary_recent,
                    patch.object(primary_view, "current_x_range", return_value=(100.0, 200.0)) as primary_range,
                    patch.object(secondary_view, "current_x_range", return_value=(300.0, 400.0)) as secondary_range,
                    patch.object(primary_view, "set_external_x_range") as primary_external,
                    patch.object(secondary_view, "set_external_x_range") as secondary_external,
                ):
                    window._apply_chart_view_range()

                secondary_recent.assert_called_once_with()
                primary_recent.assert_not_called()
                secondary_range.assert_called_once_with()
                primary_range.assert_not_called()
                primary_external.assert_called_once_with(300.0, 400.0)
                secondary_external.assert_not_called()
            finally:
                self.__class__.dispose_widget(window)

    def test_kline_native_render_clears_old_view_context_before_chart_rebuild(self) -> None:
        with (
            patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None),
            patch("roll_terminal_qt.kline_analysis_window._prefer_native_chart_backend", return_value=True),
        ):
            window = KlineAnalysisWindow()
            try:
                chart = window._native_chart
                chart_view = window._native_chart_view
                self.assertIsNotNone(chart)
                self.assertIsNotNone(chart_view)

                chart_view._axis_x = object()
                chart_view._axis_y = object()
                chart_view._candles = [{"time": 1, "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1.0}]
                chart_view.capture_view_state = lambda: None

                payload = KlineChartPayload(
                    candles=[
                        {"time": 1, "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "volume": 1.0},
                        {"time": 2, "open": 105.0, "high": 112.0, "low": 101.0, "close": 108.0, "volume": 2.0},
                    ],
                    ema_9=[],
                    ema_21=[],
                    ema_55=[],
                    trend_indicator=[],
                    signal_markers=[],
                    box_overlays=[],
                    raw_candles=[],
                    stats={},
                    alert_snapshot=None,
                )

                original_remove_all_series = chart.removeAllSeries

                def remove_all_series_guard() -> None:
                    self.assertIsNone(chart_view._axis_x)
                    self.assertIsNone(chart_view._axis_y)
                    self.assertEqual(chart_view._candles, [])
                    original_remove_all_series()

                with patch.object(chart, "removeAllSeries", side_effect=remove_all_series_guard):
                    window._render_native_chart_target(
                        chart=chart,
                        chart_view=chart_view,
                        payload=payload,
                        period="1D",
                        title_suffix="主图",
                        include_workspace_lines=False,
                        is_secondary=False,
                    )
            finally:
                self.__class__.dispose_widget(window)

    def test_deribit_linked_chart_clears_state_before_chart_rebuild(self) -> None:
        view = LinkedCandlestickChartView(percent_axis=False)
        try:
            view._candles = [
                Candle(
                    ts=1,
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                    volume=Decimal("1"),
                    confirmed=True,
                )
            ]
            view._hover_pos = object()
            view._linked_hover_index = 0
            view._linked_hover_y_ratio = 0.5
            original_remove_all_series = view.chart().removeAllSeries

            def remove_all_series_guard() -> None:
                self.assertEqual(view._candles, [])
                self.assertIsNone(view._hover_pos)
                self.assertIsNone(view._linked_hover_index)
                self.assertIsNone(view._linked_hover_y_ratio)
                original_remove_all_series()

            candles = [
                Candle(
                    ts=1_700_000_000_000,
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
                Candle(
                    ts=1_700_003_600_000,
                    open=Decimal("105"),
                    high=Decimal("112"),
                    low=Decimal("101"),
                    close=Decimal("108"),
                    volume=Decimal("2"),
                    confirmed=True,
                ),
            ]
            with patch.object(view.chart(), "removeAllSeries", side_effect=remove_all_series_guard):
                view.set_chart_payload(title="test", candles=candles, empty_message="empty")
        finally:
            self.__class__.dispose_widget(view)

    def test_option_candlestick_chart_clears_state_before_chart_rebuild(self) -> None:
        view = CandlestickChartView()
        try:
            view._axis_x = object()
            view._axis_y = object()
            view._candles = [
                Candle(
                    ts=1,
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                    volume=Decimal("1"),
                    confirmed=True,
                )
            ]
            view._hover_pos = object()
            view._linked_hover_index = 0
            view._linked_hover_y_ratio = 0.5
            original_remove_all_series = view.chart().removeAllSeries

            def remove_all_series_guard() -> None:
                self.assertIsNone(view._axis_x)
                self.assertIsNone(view._axis_y)
                self.assertEqual(view._candles, [])
                self.assertIsNone(view._hover_pos)
                self.assertIsNone(view._linked_hover_index)
                self.assertIsNone(view._linked_hover_y_ratio)
                original_remove_all_series()

            candles = [
                Candle(
                    ts=1_700_000_000_000,
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                    volume=Decimal("1"),
                    confirmed=True,
                ),
                Candle(
                    ts=1_700_003_600_000,
                    open=Decimal("105"),
                    high=Decimal("112"),
                    low=Decimal("101"),
                    close=Decimal("108"),
                    volume=Decimal("2"),
                    confirmed=True,
                ),
            ]
            with patch.object(view.chart(), "removeAllSeries", side_effect=remove_all_series_guard):
                view.set_candles(title="test", candles=candles)
        finally:
            self.__class__.dispose_widget(view)

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

    def test_kline_window_unittest_process_exits_zero(self) -> None:
        script = textwrap.dedent(
            """
            import os
            import sys
            import unittest

            os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
            import tests.test_roll_terminal_qt_windows as target_module

            suite = unittest.defaultTestLoader.loadTestsFromName(
                "RollTerminalQtWindowHelperTests.test_kline_symbol_combo_uses_bounded_header_width",
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

    def test_hover_overlay_layout_moves_tooltip_above_volume_reserved_band(self) -> None:
        layout = _compute_hover_overlay_layout(
            viewport_top=0.0,
            viewport_bottom=500.0,
            bounds_top=20.0,
            bounds_bottom=420.0,
            anchor_y=210.0,
            price_height=24.0,
            tooltip_height=150.0,
            volume_reserved_height=72.0,
        )
        self.assertLess(layout["tooltip_y"] + 150.0, 420.0 - 72.0)
        self.assertEqual(layout["tooltip_side"], "above")

    def test_hover_overlay_layout_clamps_price_badge_above_volume_reserved_band(self) -> None:
        layout = _compute_hover_overlay_layout(
            viewport_top=0.0,
            viewport_bottom=500.0,
            bounds_top=20.0,
            bounds_bottom=420.0,
            anchor_y=360.0,
            price_height=30.0,
            tooltip_height=90.0,
            volume_reserved_height=72.0,
        )
        self.assertLessEqual(layout["price_y"] + 30.0, 420.0 - 72.0)

    def test_hover_tooltip_prefers_right_padding_after_last_candle(self) -> None:
        self.assertTrue(hasattr(kline_analysis_module, "_compute_hover_tooltip_x"))
        tooltip_x = kline_analysis_module._compute_hover_tooltip_x(
            bounds_left=0.0,
            bounds_right=900.0,
            anchor_x=380.0,
            data_right_x=640.0,
            tooltip_width=180.0,
        )
        self.assertEqual(tooltip_x, 652.0)

    def test_axis_y_padding_reserves_more_space_below_for_volume_overlay(self) -> None:
        top_padding, bottom_padding = _compute_axis_y_padding(100.0, 200.0)
        self.assertGreater(bottom_padding, top_padding)

    def test_axis_y_padding_keeps_min_price_above_volume_reserved_band(self) -> None:
        min_price = 100.0
        max_price = 200.0
        plot_height = 1000.0
        volume_reserved_ratio = 0.18
        top_padding, bottom_padding = _compute_axis_y_padding(min_price, max_price)
        axis_min = min_price - bottom_padding
        axis_max = max_price + top_padding
        span = axis_max - axis_min
        min_price_y = ((axis_max - min_price) / span) * plot_height
        volume_band_top = plot_height * (1.0 - volume_reserved_ratio)
        self.assertLess(min_price_y, volume_band_top)

    def test_axis_y_padding_scales_for_doge_price_range(self) -> None:
        top_padding, bottom_padding = _compute_axis_y_padding(0.14, 0.15)
        self.assertLess(top_padding, 0.01)
        self.assertLess(bottom_padding, 0.01)

    def test_axis_y_label_format_preserves_doge_and_eth_btc_precision(self) -> None:
        self.assertEqual(_axis_y_label_format(0.14, 0.15), "%.5f")
        self.assertEqual(_axis_y_label_format(0.03, 0.04), "%.6f")
        self.assertEqual(_axis_y_label_format(64_000.0, 65_000.0), "%.2f")

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

    def test_rr_box_end_display_x_extends_fixed_bar_span(self) -> None:
        display_times = [0, 900_000, 1_800_000]
        self.assertEqual(_rr_box_end_display_x(display_times, display_step_ms=900_000, bar_entry=0, width_bars=6), 5_400_000.0)
        self.assertEqual(_rr_box_end_display_x(display_times, display_step_ms=900_000, bar_entry=2, width_bars=6), 7_200_000.0)

    def test_rr_box_end_display_x_supports_future_bar_entry(self) -> None:
        display_times = [0, 900_000, 1_800_000]
        self.assertEqual(_rr_box_end_display_x(display_times, display_step_ms=900_000, bar_entry=3, width_bars=6), 8_100_000.0)

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
