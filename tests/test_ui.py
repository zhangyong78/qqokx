from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.ui import (
    QuantApp,
    RefreshHealthState,
    _coerce_log_file_path,
    _format_network_error_message,
    _mark_refresh_health_failure,
    _mark_refresh_health_success,
    _refresh_indicator_badge_text,
    _refresh_health_is_stale,
)


class UiHelpersTest(TestCase):
    def test_format_network_error_message_read_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("The read operation timed out"),
            "\u7f51\u7edc\u8bfb\u53d6\u8d85\u65f6\uff0c\u8bf7\u7a0d\u540e\u91cd\u8bd5\u3002",
        )

    def test_format_network_error_message_handshake_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("_ssl.c:983: The handshake operation timed out"),
            "\u7f51\u7edc\u63e1\u624b\u8d85\u65f6\uff0c\u8bf7\u7a0d\u540e\u91cd\u8bd5\u3002",
        )

    def test_format_network_error_message_summarizes_cloudflare_html_502(self) -> None:
        message = """
HTTP 502: <!DOCTYPE html>
<html lang="en-US">
<head><title>okx.com | 502: Bad gateway</title></head>
<body>
<span class="code-label">Error code 502</span>
<span class="leading-1.3 text-2xl text-red-error">Error</span>
<span class="cf-footer-item sm:block sm:mb-1">Cloudflare Ray ID: <strong class="font-semibold">9edcdbc80bb66da2</strong></span>
</body>
</html>
"""
        summary = _format_network_error_message(message)
        self.assertIn("HTTP 502", summary)
        self.assertIn("OKX\u6e90\u7ad9\u5f02\u5e38", summary)
        self.assertIn("RayID=9edcdbc80bb66da2", summary)
        self.assertNotIn("<!DOCTYPE html>", summary)

    def test_coerce_log_file_path_returns_none_for_blank_value(self) -> None:
        self.assertIsNone(_coerce_log_file_path("   "))

    def test_coerce_log_file_path_returns_path_for_non_blank_value(self) -> None:
        self.assertEqual(
            _coerce_log_file_path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
            Path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
        )

    def test_refresh_health_marks_stale_after_three_failures_with_cached_success(self) -> None:
        state = RefreshHealthState("\u6301\u4ed3", last_success_at=datetime(2026, 4, 18, 12, 0, 0))

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 10))
        self.assertFalse(_refresh_health_is_stale(state))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 20))
        self.assertFalse(_refresh_health_is_stale(state))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 30))

        self.assertTrue(_refresh_health_is_stale(state))
        self.assertEqual(state.consecutive_failures, 3)
        self.assertEqual(state.last_error_summary, "HTTP 502")
        self.assertEqual(state.stale_since, datetime(2026, 4, 18, 12, 0, 30))

    def test_refresh_health_success_resets_failure_and_stale_flags(self) -> None:
        state = RefreshHealthState(
            "\u5f53\u524d\u59d4\u6258",
            last_success_at=datetime(2026, 4, 18, 12, 0, 0),
            consecutive_failures=3,
            stale_since=datetime(2026, 4, 18, 12, 0, 30),
            last_error_summary="HTTP 502",
        )

        _mark_refresh_health_success(state, at=datetime(2026, 4, 18, 12, 1, 0))

        self.assertFalse(_refresh_health_is_stale(state))
        self.assertEqual(state.last_success_at, datetime(2026, 4, 18, 12, 1, 0))
        self.assertEqual(state.consecutive_failures, 0)
        self.assertIsNone(state.stale_since)
        self.assertIsNone(state.last_error_summary)

    def test_refresh_indicator_badge_text_transitions_from_idle_to_warning_to_stale(self) -> None:
        state = RefreshHealthState("\u6301\u4ed3")
        self.assertEqual(_refresh_indicator_badge_text(state), "\u672a\u8bfb")

        _mark_refresh_health_success(state, at=datetime(2026, 4, 18, 12, 0, 0))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u6b63\u5e38")

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 10))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u544a\u8b66 x1")

        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 20))
        _mark_refresh_health_failure(state, "HTTP 502", at=datetime(2026, 4, 18, 12, 0, 30))
        self.assertEqual(_refresh_indicator_badge_text(state), "\u8fc7\u671f x3")

    def test_session_can_be_cleared_only_when_stopped_and_not_running(self) -> None:
        stopped = SimpleNamespace(status="\u5df2\u505c\u6b62", engine=SimpleNamespace(is_running=False))
        stopping = SimpleNamespace(status="\u505c\u6b62\u4e2d", engine=SimpleNamespace(is_running=False))
        running = SimpleNamespace(status="\u8fd0\u884c\u4e2d", engine=SimpleNamespace(is_running=True))

        self.assertTrue(QuantApp._session_can_be_cleared(stopped))
        self.assertFalse(QuantApp._session_can_be_cleared(stopping))
        self.assertFalse(QuantApp._session_can_be_cleared(running))

    def test_next_session_selection_after_clear_prefers_existing_selected_session(self) -> None:
        self.assertEqual(
            QuantApp._next_session_selection_after_clear("S02", ("S01", "S02", "S03")),
            "S02",
        )

    def test_next_session_selection_after_clear_falls_back_to_first_remaining(self) -> None:
        self.assertEqual(
            QuantApp._next_session_selection_after_clear("S02", ("S03", "S04")),
            "S03",
        )

    def test_next_session_selection_after_clear_returns_none_when_empty(self) -> None:
        self.assertIsNone(QuantApp._next_session_selection_after_clear("S02", ()))

    def test_session_blocks_history_deletion_when_running_or_stopping(self) -> None:
        running = SimpleNamespace(status="\u8fd0\u884c\u4e2d", engine=SimpleNamespace(is_running=True))
        stopping = SimpleNamespace(status="\u505c\u6b62\u4e2d", engine=SimpleNamespace(is_running=False))
        stopped = SimpleNamespace(status="\u5df2\u505c\u6b62", engine=SimpleNamespace(is_running=False))

        self.assertTrue(QuantApp._session_blocks_history_deletion(running))
        self.assertTrue(QuantApp._session_blocks_history_deletion(stopping))
        self.assertFalse(QuantApp._session_blocks_history_deletion(stopped))

    def test_next_history_selection_after_mutation_prefers_existing_selected_record(self) -> None:
        self.assertEqual(
            QuantApp._next_history_selection_after_mutation("R02", ("R01", "R02", "R03")),
            "R02",
        )

    def test_next_history_selection_after_mutation_falls_back_to_first_remaining(self) -> None:
        self.assertEqual(
            QuantApp._next_history_selection_after_mutation("R02", ("R03", "R04")),
            "R03",
        )

    def test_next_history_selection_after_mutation_returns_none_when_empty(self) -> None:
        self.assertIsNone(QuantApp._next_history_selection_after_mutation("R02", ()))
