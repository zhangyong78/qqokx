from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.ui import (
    QuantApp,
    RefreshHealthState,
    StrategyStopCleanupResult,
    _coerce_log_file_path,
    _format_network_error_message,
    _mark_refresh_health_failure,
    _mark_refresh_health_success,
    _refresh_indicator_badge_text,
    _refresh_health_is_stale,
    _session_order_prefixes,
    _trade_order_belongs_to_session,
    _trade_order_session_role,
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

    def test_trade_order_session_role_matches_regular_strategy_order_prefix(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托-多头",
        )
        prefix = _session_order_prefixes(session)[0]
        order = SimpleNamespace(
            client_order_id=f"{prefix}ent041816084800001",
            algo_client_order_id="",
        )

        self.assertEqual(_trade_order_session_role(order, session), "ent")
        self.assertTrue(_trade_order_belongs_to_session(order, session))

    def test_trade_order_session_role_matches_protective_algo_order_prefix(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托-多头",
        )
        prefix = _session_order_prefixes(session)[0]
        order = SimpleNamespace(
            client_order_id="",
            algo_client_order_id=f"{prefix}slg041816084800001",
        )

        self.assertEqual(_trade_order_session_role(order, session), "slg")
        self.assertTrue(_trade_order_belongs_to_session(order, session))

    def test_trade_order_session_role_does_not_match_other_session(self) -> None:
        session = SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托-多头",
        )
        other_session = SimpleNamespace(
            session_id="S02",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA 动态委托-多头",
        )
        other_prefix = _session_order_prefixes(other_session)[0]
        order = SimpleNamespace(
            client_order_id=f"{other_prefix}ent041816084800001",
            algo_client_order_id="",
        )

        self.assertIsNone(_trade_order_session_role(order, session))
        self.assertFalse(_trade_order_belongs_to_session(order, session))

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


class _FastEvent:
    def wait(self, timeout: float | None = None) -> bool:
        return False


class StrategyStopCleanupTest(TestCase):
    def _make_session(self) -> SimpleNamespace:
        return SimpleNamespace(
            session_id="S01",
            strategy_id="ema_dynamic_order",
            strategy_name="EMA dynamic order long",
            symbol="ETH-USDT-SWAP",
            api_name="moni",
            status="\u505c\u6b62\u4e2d",
            stop_cleanup_in_progress=True,
            stopped_at=None,
            ended_reason="",
            engine=SimpleNamespace(is_running=False),
            config=SimpleNamespace(
                environment="demo",
                trade_inst_id="ETH-USDT-SWAP",
                inst_id="ETH-USDT-SWAP",
            ),
        )

    def _make_order(
        self,
        session: SimpleNamespace,
        *,
        role: str,
        source_kind: str = "ord",
        state: str = "live",
        filled_size: str = "0",
    ) -> SimpleNamespace:
        prefix = _session_order_prefixes(session)[0]
        client_order_id = f"{prefix}{role}041816084800001" if source_kind != "algo" else ""
        algo_client_order_id = f"{prefix}{role}041816084800001" if source_kind == "algo" else ""
        return SimpleNamespace(
            source_kind=source_kind,
            source_label="algo" if source_kind == "algo" else "\u59d4\u6258",
            inst_id="ETH-USDT-SWAP",
            side="buy",
            pos_side="long",
            client_order_id=client_order_id,
            algo_client_order_id=algo_client_order_id,
            order_id="7001",
            algo_id="9001" if source_kind == "algo" else "",
            filled_size=Decimal(filled_size),
            state=state,
            avg_price=Decimal("2500"),
            price=Decimal("2500"),
            inst_type="SWAP",
        )

    def _make_position(self, *, size: str = "1") -> SimpleNamespace:
        return SimpleNamespace(
            inst_id="ETH-USDT-SWAP",
            pos_side="long",
            position=Decimal(size),
            avg_price=Decimal("2495"),
        )

    def _make_snapshot(
        self,
        *,
        environment: str = "demo",
        pending_orders: list[SimpleNamespace] | None = None,
        order_history: list[SimpleNamespace] | None = None,
        positions: list[SimpleNamespace] | None = None,
        environment_note: str | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            effective_environment=environment,
            pending_orders=list(pending_orders or []),
            order_history=list(order_history or []),
            positions=list(positions or []),
            environment_note=environment_note,
        )

    def test_perform_stop_session_cleanup_collects_successful_cancel(self) -> None:
        session = self._make_session()
        entry_order = self._make_order(session, role="ent")
        initial_snapshot = self._make_snapshot(pending_orders=[entry_order])
        final_snapshot = self._make_snapshot()
        cancel_requests: list[tuple[str, SimpleNamespace]] = []

        def cancel_pending_order_request(credentials, *, environment: str, item: SimpleNamespace):
            cancel_requests.append((environment, item))
            return SimpleNamespace(s_code="0", s_msg="")

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=cancel_pending_order_request,
        )

        result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertEqual(cancel_requests, [("demo", entry_order)])
        self.assertEqual(len(result.cancel_requested_summaries), 1)
        self.assertFalse(result.cancel_failed_summaries)
        self.assertFalse(result.remaining_pending_summaries)
        self.assertFalse(result.needs_manual_review)
        self.assertEqual(result.final_reason, "\u7528\u6237\u624b\u52a8\u505c\u6b62")

    def test_perform_stop_session_cleanup_flags_failed_cancel_for_manual_review(self) -> None:
        session = self._make_session()
        entry_order = self._make_order(session, role="ent")
        initial_snapshot = self._make_snapshot(pending_orders=[entry_order])
        final_snapshot = self._make_snapshot(pending_orders=[entry_order])

        def cancel_pending_order_request(credentials, *, environment: str, item: SimpleNamespace):
            raise RuntimeError("The read operation timed out")

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=cancel_pending_order_request,
        )

        with patch("okx_quant.ui.threading.Event", return_value=_FastEvent()):
            result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertFalse(result.cancel_requested_summaries)
        self.assertEqual(len(result.cancel_failed_summaries), 1)
        self.assertEqual(len(result.remaining_pending_summaries), 1)
        self.assertTrue(result.needs_manual_review)
        self.assertIn("\u64a4\u5355\u672a\u5b8c\u5168\u786e\u8ba4", result.final_reason)

    def test_perform_stop_session_cleanup_flags_filled_order_with_open_position(self) -> None:
        session = self._make_session()
        filled_order = self._make_order(session, role="ent", state="filled", filled_size="1")
        open_position = self._make_position()
        initial_snapshot = self._make_snapshot()
        final_snapshot = self._make_snapshot(order_history=[filled_order], positions=[open_position])

        app = SimpleNamespace(
            _load_strategy_stop_cleanup_snapshot_with_fallback=lambda session_, credentials_: initial_snapshot,
            _load_strategy_stop_cleanup_snapshot=lambda session_, credentials_, environment: final_snapshot,
            _cancel_pending_order_request=lambda credentials, *, environment, item: None,
        )

        result = QuantApp._perform_stop_session_cleanup(app, session, SimpleNamespace())

        self.assertFalse(result.cancel_requested_summaries)
        self.assertEqual(len(result.filled_order_summaries), 1)
        self.assertEqual(len(result.open_position_summaries), 1)
        self.assertTrue(result.needs_manual_review)
        self.assertIn("\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u4ed3\u4f4d", result.final_reason)

    def test_apply_stop_session_cleanup_result_warns_for_manual_review(self) -> None:
        session = self._make_session()
        log_messages: list[str] = []
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=lambda session_, message: log_messages.append(message),
            _current_credential_profile=lambda: "moni",
            refresh_positions=MagicMock(),
            refresh_order_views=MagicMock(),
        )
        result = StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment="demo",
            cancel_failed_summaries=("entry cancel timeout",),
            remaining_pending_summaries=("entry still pending",),
            filled_order_summaries=("filled entry",),
            open_position_summaries=("ETH-USDT-SWAP [long] | size=1",),
            needs_manual_review=True,
            final_reason="\u7528\u6237\u624b\u52a8\u505c\u6b62\uff08\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u4ed3\u4f4d\uff0c\u9700\u4eba\u5de5\u5224\u65ad\uff09",
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning, patch(
            "okx_quant.ui.messagebox.showinfo"
        ) as showinfo:
            QuantApp._apply_stop_session_cleanup_result(app, result)

        self.assertEqual(session.status, "\u5df2\u505c\u6b62")
        self.assertFalse(session.stop_cleanup_in_progress)
        self.assertEqual(session.ended_reason, result.final_reason)
        self.assertTrue(any("\u68c0\u6d4b\u5230\u5df2\u6210\u4ea4\u59d4\u6258" in message for message in log_messages))
        self.assertTrue(any("\u68c0\u6d4b\u5230\u4ecd\u6709\u4ed3\u4f4d" in message for message in log_messages))
        showwarning.assert_called_once()
        showinfo.assert_not_called()
        app.refresh_positions.assert_called_once()
        app.refresh_order_views.assert_called_once()

    def test_apply_stop_session_cleanup_result_shows_info_when_clean(self) -> None:
        session = self._make_session()
        log_messages: list[str] = []
        app = SimpleNamespace(
            sessions={session.session_id: session},
            _upsert_session_row=MagicMock(),
            _refresh_selected_session_details=MagicMock(),
            _sync_strategy_history_from_session=MagicMock(),
            _log_session_message=lambda session_, message: log_messages.append(message),
            _current_credential_profile=lambda: "moni",
            refresh_positions=MagicMock(),
            refresh_order_views=MagicMock(),
        )
        result = StrategyStopCleanupResult(
            session_id=session.session_id,
            effective_environment="demo",
            cancel_requested_summaries=("entry cancel accepted",),
            needs_manual_review=False,
            final_reason="\u7528\u6237\u624b\u52a8\u505c\u6b62",
        )

        with patch("okx_quant.ui.messagebox.showwarning") as showwarning, patch(
            "okx_quant.ui.messagebox.showinfo"
        ) as showinfo:
            QuantApp._apply_stop_session_cleanup_result(app, result)

        self.assertEqual(session.status, "\u5df2\u505c\u6b62")
        self.assertFalse(session.stop_cleanup_in_progress)
        self.assertTrue(any("\u5df2\u63d0\u4ea4\u64a4\u5355 1 \u6761" in message for message in log_messages))
        showwarning.assert_not_called()
        showinfo.assert_called_once()
