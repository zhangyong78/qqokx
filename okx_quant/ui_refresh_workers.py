"""Bounded, reusable daemon workers for account display queries."""

from __future__ import annotations

import logging
import queue
import threading
import time
import ctypes
import sys
from collections.abc import Callable

from okx_quant.app_meta import APP_DISTRIBUTION


class _MemoryStatus(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_uint32), ("load", ctypes.c_uint32),
        ("total_phys", ctypes.c_uint64), ("avail_phys", ctypes.c_uint64),
        ("total_page", ctypes.c_uint64), ("avail_page", ctypes.c_uint64),
        ("total_virtual", ctypes.c_uint64), ("avail_virtual", ctypes.c_uint64),
        ("avail_extended", ctypes.c_uint64),
    ]


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [("cb", ctypes.c_uint32), ("page_faults", ctypes.c_uint32)] + [
        (name, ctypes.c_size_t) for name in (
            "peak_working_set", "working_set", "peak_paged_pool", "paged_pool",
            "peak_nonpaged_pool", "nonpaged_pool", "pagefile", "peak_pagefile", "private_bytes",
        )
    ]


def _process_resource_snapshot() -> dict[str, int]:
    if sys.platform != "win32":
        return {}
    result: dict[str, int] = {}
    try:
        process = ctypes.c_void_p(-1)  # Current-process pseudo handle; no handle to close.
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        query = ctypes.windll.psapi.GetProcessMemoryInfo
        query.argtypes = [ctypes.c_void_p, ctypes.POINTER(_ProcessMemoryCounters), ctypes.c_uint32]
        if query(process, ctypes.byref(counters), counters.cb):
            result["private_mb"] = counters.private_bytes // (1024 * 1024)
            result["working_set_mb"] = counters.working_set // (1024 * 1024)
        count = ctypes.c_uint32()
        if ctypes.windll.kernel32.GetProcessHandleCount(process, ctypes.byref(count)):
            result["handles"] = count.value
    except (AttributeError, OSError):
        pass
    return result


def record_server_resources(app: object) -> None:
    """Record growth evidence cheaply, without adding a monitoring thread."""
    if APP_DISTRIBUTION != "server":
        return
    now = time.monotonic()
    if now < getattr(app, "_server_resource_log_at", 0.0):
        return
    app._server_resource_log_at = now + 300.0
    try:
        snapshot = _process_resource_snapshot()
        load = _system_memory_load_percent()
        workers = getattr(app, "_display_refresh_workers", None)
        pending = workers._jobs.qsize() if workers is not None else 0
        app._enqueue_log(
            f"服务器资源 | 系统内存={load if load is not None else '-'}%"
            f" | 进程私有内存={snapshot.get('private_mb', '-')}MB"
            f" | 工作集={snapshot.get('working_set_mb', '-')}MB"
            f" | Python活动线程={threading.active_count()}"
            f" | 句柄={snapshot.get('handles', '-')} | 显示等待任务={pending}"
        )
    except (RuntimeError, MemoryError, OSError):
        # An optional diagnostic must not interrupt runtime bookkeeping.
        pass


def _system_memory_load_percent() -> int | None:
    if sys.platform != "win32":
        return None
    try:
        status = _MemoryStatus()
        status.length = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.load)
    except (AttributeError, OSError):
        pass
    return None


class RefreshWorkers:
    def __init__(self, max_workers: int = 2, max_pending: int = 8) -> None:
        self._jobs: queue.Queue = queue.Queue(maxsize=max_pending)
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self._max_workers = max_workers

    def submit(self, callback: Callable, *args: object) -> None:
        with self._lock:
            self._threads = [thread for thread in self._threads if thread.is_alive()]
            if len(self._threads) < self._max_workers:
                thread = threading.Thread(target=self._run, name="account-display-refresh", daemon=True)
                thread.start()
                self._threads.append(thread)
            try:
                self._jobs.put_nowait((callback, args))
            except queue.Full as exc:
                raise RuntimeError("账户显示刷新队列已满") from exc

    def _run(self) -> None:
        while True:
            job = self._jobs.get()
            try:
                callback, args = job
                callback(*args)
            except Exception:
                logging.getLogger(__name__).exception("账户显示后台刷新失败")
            finally:
                # Do not retain the last account snapshot while waiting for work.
                del job, callback, args
                self._jobs.task_done()


def refresh_queries_paused(app: object) -> bool:
    now = time.monotonic()
    if now < getattr(app, "_display_refresh_retry_at", 0.0):
        return True
    if APP_DISTRIBUTION == "server" and now >= getattr(app, "_display_memory_check_at", 0.0):
        app._display_memory_check_at = now + 15.0
        load = _system_memory_load_percent()
        if load is not None and load >= 90:
            app._display_refresh_retry_at = now + 30.0
            if now >= getattr(app, "_display_memory_log_at", 0.0):
                app._display_memory_log_at = now + 300.0
                app._enqueue_log(f"服务器内存使用率 {load}%，暂停账户显示查询 30 秒；策略监控继续运行。")
            return True
    return False


def submit_refresh_query(app: object, callback: Callable, *args: object) -> None:
    try:
        workers = getattr(app, "_display_refresh_workers", None)
        if workers is None:
            workers = RefreshWorkers()
            app._display_refresh_workers = workers
        workers.submit(callback, *args)
    except (RuntimeError, MemoryError):
        app._display_refresh_retry_at = time.monotonic() + 60.0
        raise
