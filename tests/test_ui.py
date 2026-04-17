from unittest import TestCase

from pathlib import Path
from types import SimpleNamespace

from okx_quant.ui import QuantApp, _coerce_log_file_path, _format_network_error_message


class UiHelpersTest(TestCase):
    def test_format_network_error_message_read_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("The read operation timed out"),
            "网络读取超时，请稍后重试。",
        )

    def test_format_network_error_message_handshake_timeout(self) -> None:
        self.assertEqual(
            _format_network_error_message("_ssl.c:983: The handshake operation timed out"),
            "网络握手超时，请稍后重试。",
        )

    def test_coerce_log_file_path_returns_none_for_blank_value(self) -> None:
        self.assertIsNone(_coerce_log_file_path("   "))

    def test_coerce_log_file_path_returns_path_for_non_blank_value(self) -> None:
        self.assertEqual(
            _coerce_log_file_path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
            Path(r"D:\qqokx\logs\strategy_sessions\2026-04-17\session.log"),
        )

    def test_session_can_be_cleared_only_when_stopped_and_not_running(self) -> None:
        stopped = SimpleNamespace(status="已停止", engine=SimpleNamespace(is_running=False))
        stopping = SimpleNamespace(status="停止中", engine=SimpleNamespace(is_running=False))
        running = SimpleNamespace(status="运行中", engine=SimpleNamespace(is_running=True))

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
        running = SimpleNamespace(status="运行中", engine=SimpleNamespace(is_running=True))
        stopping = SimpleNamespace(status="停止中", engine=SimpleNamespace(is_running=False))
        stopped = SimpleNamespace(status="已停止", engine=SimpleNamespace(is_running=False))

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
