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
from PySide6.QtWidgets import QPushButton
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
    _build_annotation_key,
    _compute_rr_target,
    _safe_text as line_safe_text,
    _split_annotation_key,
)
from roll_terminal_qt.profile_access import profile_requires_password
from roll_terminal_qt.smart_order_window import _safe_text as smart_safe_text
from roll_terminal_qt.kline_analysis_window import (
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
    _line_handle_visual,
    _line_time_tolerance_seconds,
    _line_price_tolerance,
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

    def test_compute_rr_target_supports_long_and_short(self) -> None:
        self.assertEqual(
            _compute_rr_target("long", Decimal("100"), Decimal("95"), Decimal("2")),
            Decimal("110"),
        )
        self.assertEqual(
            _compute_rr_target("short", Decimal("100"), Decimal("105"), Decimal("2")),
            Decimal("90"),
        )

    def test_profile_requires_password_detects_protected_payload(self) -> None:
        payload = build_profile_switch_password_snapshot("secret-1")
        self.assertTrue(profile_requires_password("api1", {"api1": payload}))
        self.assertFalse(profile_requires_password("api2", {"api1": payload}))

    def test_bar_to_ms_supports_mh_and_d_units(self) -> None:
        self.assertEqual(_bar_to_ms("3m"), 180_000)
        self.assertEqual(_bar_to_ms("4H"), 14_400_000)
        self.assertEqual(_bar_to_ms("1D"), 86_400_000)

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
