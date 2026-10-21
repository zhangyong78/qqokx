"""Shutdown must not destroy local trade/reconciliation workers in flight."""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QThread
import pytest

from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor
from roll_terminal_qt.history_sync_manager import HistorySyncManager
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from roll_terminal_qt.launcher import LauncherWindow


class HeldWorker(QThread):
    def __init__(self):
        super().__init__()
        self.release = threading.Event()
        self.stop_requested = False

    def run(self):
        self.release.wait(10)

    def stop(self):
        self.stop_requested = True


def test_kline_shutdown_waits_for_area_reconciliation_action():
    worker = HeldWorker()
    completed = []
    host = SimpleNamespace(_area_shape_demo_workers={worker}, _retired_loaders=set(),
                           _shutdown_callbacks=[lambda: completed.append(True)])
    worker.start()
    try:
        with patch('roll_terminal_qt.kline_analysis_window.QTimer.singleShot') as schedule:
            KlineAnalysisWindow._poll_shutdown_loaders(host)
            assert not completed
            assert not getattr(host, '_shutdown_complete', False)
            schedule.assert_called_once()
            worker.release.set()
            assert worker.wait(2000)
            KlineAnalysisWindow._poll_shutdown_loaders(host)
        assert completed == [True]
    finally:
        worker.release.set()
        worker.wait(2000)


def test_launcher_does_not_quit_while_history_thread_is_alive():
    worker = HeldWorker()
    manager = HistorySyncManager()
    manager._threads[('test', 'demo')] = worker
    app = MagicMock()
    host = SimpleNamespace(_shutdown_started_at=None,
        _shape_signal_monitor=SimpleNamespace(stop=MagicMock(), is_running=lambda: False),
        _area_shape_monitor=SimpleNamespace(stop=MagicMock(), is_running=lambda: False),
        _area_shape_execution_monitor=SimpleNamespace(stop=MagicMock(), is_running=lambda: False),
        _history_sync_manager=manager,
        deleteLater=MagicMock())
    host._finish_shutdown = lambda: LauncherWindow._finish_shutdown(host)
    worker.start()
    try:
        with patch('roll_terminal_qt.launcher.QApplication.instance', return_value=app), \
             patch('roll_terminal_qt.launcher.QTimer.singleShot') as schedule, \
             patch('roll_terminal_qt.launcher.append_log_line'):
            host._finish_shutdown()
            assert worker.stop_requested
            app.quit.assert_not_called()
            host.deleteLater.assert_not_called()
            schedule.assert_called_once()
            worker.release.set()
            assert worker.wait(2000)
            host._finish_shutdown()
        app.quit.assert_called_once()
        host.deleteLater.assert_called_once()
        host._area_shape_execution_monitor.stop.assert_called_once()
    finally:
        worker.release.set()
        worker.wait(2000)


def test_delayed_startup_history_timer_cannot_start_work_during_shutdown():
    host = SimpleNamespace(_shutdown_in_progress=True, _active_profile_name='unused',
                           _history_sync_manager=MagicMock())
    with patch('roll_terminal_qt.launcher.load_runtime') as load:
        LauncherWindow._start_background_history_sync(host)
    load.assert_not_called()
    host._history_sync_manager.request_sync.assert_not_called()


@pytest.mark.parametrize('callback,payload', [
    ('_on_area_shape_demo_action_completed', {}), ('_on_area_shape_demo_action_failed', 'error'),
])
def test_late_area_action_callbacks_do_not_touch_closing_widgets(callback, payload):
    host = SimpleNamespace(_shutdown_requested=True)
    getattr(KlineAnalysisWindow, callback)(host, payload)
    assert vars(host) == {'_shutdown_requested': True}


def test_arm_worker_is_retained_until_inflight_preflight_returns():
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _: None)
    entered, release = threading.Event(), threading.Event()

    def arm(**_kwargs):
        entered.set()
        release.wait(5)

    monitor.arm_plan = arm
    monitor.request_arm(plan=SimpleNamespace(plan_id='held'), runtime=SimpleNamespace(environment='demo'),
                        unlocked=True, paper_monitor=None)
    assert entered.wait(2)
    workers = list(monitor._arm_workers)
    try:
        monitor.stop()
        assert monitor.is_running()
        monitor.request_arm(plan=SimpleNamespace(plan_id='second'), runtime=SimpleNamespace(environment='demo'),
                            unlocked=True, paper_monitor=None)
        assert len(monitor._arm_workers) == 1
    finally:
        release.set()
        for worker in workers:
            worker.join(2)
    assert not monitor.is_running()
