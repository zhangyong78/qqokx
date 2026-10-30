import threading
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.ui_refresh_workers import RefreshWorkers, record_server_resources, refresh_queries_paused, submit_refresh_query
from okx_quant.ui_shell import QuantApp


class RefreshWorkersTest(TestCase):
    def test_server_resource_samples_are_limited_to_once_every_five_minutes(self):
        app = SimpleNamespace(_enqueue_log=MagicMock())
        with patch("okx_quant.ui_refresh_workers.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui_refresh_workers.time.monotonic", side_effect=[100, 101, 401]
        ), patch("okx_quant.ui_refresh_workers._system_memory_load_percent", return_value=72), patch(
            "okx_quant.ui_refresh_workers._process_resource_snapshot",
            return_value={"private_mb": 250, "working_set_mb": 150, "handles": 120},
        ) as native_query:
            for _ in range(3):
                record_server_resources(app)
        self.assertEqual(2, native_query.call_count)
        self.assertEqual(2, app._enqueue_log.call_count)
        self.assertIn("进程私有内存=250MB", app._enqueue_log.call_args.args[0])

    def test_server_memory_pressure_pauses_queries_and_recovers(self):
        app = SimpleNamespace(_enqueue_log=MagicMock())
        with patch("okx_quant.ui_refresh_workers.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui_refresh_workers.time.monotonic", return_value=100
        ), patch("okx_quant.ui_refresh_workers._system_memory_load_percent", return_value=97):
            self.assertTrue(refresh_queries_paused(app))
            self.assertTrue(refresh_queries_paused(app))
        app._enqueue_log.assert_called_once()
        with patch("okx_quant.ui_refresh_workers.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui_refresh_workers.time.monotonic", return_value=131
        ), patch("okx_quant.ui_refresh_workers._system_memory_load_percent", return_value=70):
            self.assertFalse(refresh_queries_paused(app))

    def test_local_queries_do_not_use_server_memory_gate(self):
        with patch("okx_quant.ui_refresh_workers.APP_DISTRIBUTION", "local"), patch(
            "okx_quant.ui_refresh_workers._system_memory_load_percent"
        ) as check:
            self.assertFalse(refresh_queries_paused(SimpleNamespace()))
        check.assert_not_called()

    def test_server_idle_positions_skip_enrichment(self):
        with patch("okx_quant.ui.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui.refresh_queries_paused", return_value=False
        ), patch("okx_quant.ui.submit_refresh_query") as submit:
            QuantApp._start_positions_enrichment_refresh(
                SimpleNamespace(), positions=[], environment="live", profile_name="a", generation=1
            )
        submit.assert_not_called()

    def test_server_snapshot_queue_failure_preserves_forced_refresh(self):
        key = ("a", "live")
        app = SimpleNamespace(
            _session_position_snapshot_targets=lambda: [key],
            _session_positions_snapshot_refreshing=False,
            _session_position_snapshot_force_refresh_keys={key},
            _refresh_session_position_snapshots_worker=MagicMock(),
        )
        with patch("okx_quant.ui.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui.refresh_queries_paused", return_value=False
        ), patch("okx_quant.ui.submit_refresh_query", side_effect=MemoryError):
            QuantApp._schedule_session_position_snapshot_sync(app)
        self.assertFalse(app._session_positions_snapshot_refreshing)
        self.assertTrue(app._session_positions_snapshot_refresh_requested)
        self.assertEqual({key}, app._session_position_snapshot_force_refresh_keys)

    def test_server_price_query_reuses_bounded_workers(self):
        targets = {("a", "live"): {"BTC-USDT-SWAP"}}
        app = SimpleNamespace(
            _running_session_market_price_targets=lambda: targets,
            _refresh_running_session_market_price_worker=MagicMock(),
        )
        with patch("okx_quant.ui.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui.refresh_queries_paused", return_value=False
        ), patch("okx_quant.ui.submit_refresh_query") as submit:
            QuantApp._schedule_running_session_market_price_sync(app)
        submit.assert_called_once_with(app, app._refresh_running_session_market_price_worker, targets)
        self.assertTrue(app._running_session_market_price_refreshing)

    def test_server_position_display_refresh_has_a_floor(self):
        app = SimpleNamespace(position_refresh_interval_label=SimpleNamespace(get=lambda: "fast"))
        with patch("okx_quant.ui.POSITION_REFRESH_INTERVAL_OPTIONS", {"fast": 1000}):
            with patch("okx_quant.ui.APP_DISTRIBUTION", "server"):
                self.assertEqual(30_000, QuantApp._position_refresh_interval_ms(app))
            with patch("okx_quant.ui.APP_DISTRIBUTION", "local"):
                self.assertEqual(1000, QuantApp._position_refresh_interval_ms(app))

    def test_server_render_throttling_keeps_runtime_bookkeeping_running(self):
        session = SimpleNamespace(
            engine=SimpleNamespace(is_running=True), status="运行中", runtime_status="运行中", ended_reason=""
        )
        app = SimpleNamespace(sessions={"s": session}, root=MagicMock(), status_text=MagicMock())
        for method in (
            "_refresh_session_live_pnl_cache", "_update_session_runtime_heartbeat_state", "_upsert_session_row",
            "_sync_strategy_history_from_session", "_refresh_trader_desk_runtime", "_refresh_running_session_summary",
            "_apply_running_session_tree_sort_order", "_update_settings_summary", "_refresh_selected_session_details",
            "_refresh_strategy_book_window", "_refresh_status",
        ):
            setattr(app, method, MagicMock())
        with patch("okx_quant.ui.APP_DISTRIBUTION", "server"), patch(
            "okx_quant.ui.time.monotonic", side_effect=[100, 100.5, 102.1]
        ), patch("okx_quant.ui.refresh_queries_paused", side_effect=[False, True]):
            for _ in range(3):
                QuantApp._refresh_status(app)
        self.assertEqual(3, app._sync_strategy_history_from_session.call_count)
        self.assertEqual(3, app._update_session_runtime_heartbeat_state.call_count)
        self.assertEqual(3, app._refresh_trader_desk_runtime.call_count)
        self.assertEqual(3, app.root.after.call_count)
        app._upsert_session_row.assert_called_once_with(session, reorder=False)
        app._refresh_running_session_summary.assert_called_once()

    def test_workers_are_reused_and_queue_is_bounded(self):
        workers = RefreshWorkers(max_workers=1, max_pending=1)
        started, release, finished = threading.Event(), threading.Event(), threading.Event()
        identifiers = []

        def blocking():
            identifiers.append(threading.get_ident())
            started.set()
            release.wait(5)

        def following():
            identifiers.append(threading.get_ident())
            finished.set()

        try:
            workers.submit(blocking)
            self.assertTrue(started.wait(2))
            workers.submit(following)
            with self.assertRaisesRegex(RuntimeError, "队列已满"):
                workers.submit(following)
        finally:
            release.set()
        self.assertTrue(finished.wait(2))
        self.assertEqual(identifiers[0], identifiers[1])

    def test_thread_start_failure_enters_cooldown_then_can_retry(self):
        app = SimpleNamespace()
        finished = threading.Event()
        with patch("okx_quant.ui_refresh_workers.threading.Thread.start", side_effect=RuntimeError("can't start new thread")):
            with self.assertRaises(RuntimeError):
                submit_refresh_query(app, finished.set)
        self.assertTrue(refresh_queries_paused(app))
        self.assertEqual([], app._display_refresh_workers._threads)
        app._display_refresh_retry_at = 0
        submit_refresh_query(app, finished.set)
        self.assertTrue(finished.wait(2))

    def test_equity_submission_failure_clears_busy_and_stops_loop(self):
        app = SimpleNamespace(
            _running_session_account_equity_last_scan_at=None,
            _account_overview_refreshing_keys=set(),
            _account_overview_cache_by_key={},
            _account_overview_last_error_by_key={},
            _credentials_for_profile_or_none=MagicMock(return_value=object()),
            _refresh_session_account_overview_worker=MagicMock(),
            _enqueue_log=MagicMock(),
        )
        with patch("okx_quant.ui.submit_refresh_query", side_effect=MemoryError) as submit:
            QuantApp._refresh_running_session_account_equities_if_needed(
                app, target_keys=(("a", "live"), ("b", "live"))
            )
        self.assertFalse(app._account_overview_refreshing_keys)
        self.assertIn(("a", "live"), app._account_overview_last_error_by_key)
        submit.assert_called_once()

    def test_console_quick_edit_is_disabled_without_changing_other_flags(self):
        import main

        kernel32 = MagicMock()

        def get_mode(handle, mode_pointer):
            mode_pointer._obj.value = 0x0040 | 0x0001 | 0x0002
            return 1

        kernel32.GetConsoleMode.side_effect = get_mode
        with patch.object(main.sys, "platform", "win32"), patch.object(
            main.ctypes, "windll", SimpleNamespace(kernel32=kernel32), create=True
        ):
            main._disable_windows_console_quick_edit()
        self.assertEqual(0x0080 | 0x0001 | 0x0002, kernel32.SetConsoleMode.call_args.args[1])
