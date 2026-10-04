from __future__ import annotations

import queue
from types import SimpleNamespace

from okx_quant.arbitrage_fast_app import ArbitrageFastApp
from okx_quant.ui_shell import _RecentLogQueue


def test_recent_log_queue_keeps_latest_lines_without_blocking() -> None:
    log_queue = _RecentLogQueue(maxsize=3)
    for index in range(6):
        log_queue.put_nowait(f"line-{index}")

    assert [log_queue.get_nowait() for _ in range(3)] == ["line-3", "line-4", "line-5"]


def test_fast_app_log_queue_keeps_latest_line_when_full() -> None:
    app = SimpleNamespace(_log_queue=queue.Queue(maxsize=2))
    ArbitrageFastApp._enqueue_log(app, "one")
    ArbitrageFastApp._enqueue_log(app, "two")
    ArbitrageFastApp._enqueue_log(app, "three")

    assert [app._log_queue.get_nowait(), app._log_queue.get_nowait()] == ["two", "three"]
