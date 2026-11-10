from __future__ import annotations

import sys
import threading
import time
import weakref
from tkinter import TclError

import pytest

from okx_quant.ui_callback_queue import ServerTk
from okx_quant.ui_refresh_workers import _process_resource_snapshot


@pytest.fixture(scope="module")
def root():
    try:
        window = ServerTk()
    except TclError as exc:
        pytest.skip(f"Tk display unavailable: {exc}")
    window.withdraw()
    yield window
    if not window._callbacks_closed:
        window.destroy()


def worker_call(function):
    results, errors = [], []

    def run():
        try:
            results.append(function())
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive(), "worker unexpectedly waited for Tcl's main loop"
    return results, errors


def drain(root):
    deadline = time.monotonic() + 3
    while root.pending_worker_callbacks:
        assert time.monotonic() < deadline
        root.update()
        time.sleep(0.002)


def test_worker_callbacks_run_on_owner_and_release_payloads(root):
    calls = []
    results, errors = worker_call(lambda: root.after(0, calls.append, threading.get_ident()))
    assert not errors
    assert results[0].startswith("worker-after-")
    assert not calls
    drain(root)
    assert len(calls) == 1
    assert root.pending_worker_callbacks == 0
    assert not root._callback_inbox


def test_worker_callback_executes_on_main_thread(root):
    calls = []
    worker_call(lambda: root.after(0, lambda: calls.append(threading.get_ident())))
    drain(root)
    assert calls == [threading.get_ident()]


def test_delayed_callback_keeps_deadline_and_releases_snapshot(root):
    class Snapshot:
        pass

    calls = []
    snapshot = Snapshot()
    reference = weakref.ref(snapshot)
    started = time.monotonic()
    worker_call(lambda: root.after(150, lambda value: calls.append(time.monotonic()), snapshot))
    del snapshot
    assert reference() is not None
    drain(root)
    assert calls[0] - started >= 0.14
    assert reference() is None


@pytest.mark.parametrize("scheduled", [False, True])
def test_cancel_delayed_worker_callback(root, scheduled):
    calls = []
    results, errors = worker_call(lambda: root.after(10000, calls.append, "unexpected"))
    assert not errors
    if scheduled:
        time.sleep(0.06)
        root.update()
        assert root._worker_callbacks[results[0]].native_id is not None
    root.after_cancel(results[0])
    assert root.pending_worker_callbacks == 0
    assert not root._callback_inbox
    root.update()
    assert not calls


def test_worker_can_cancel_without_entering_tcl(root):
    calls = []
    results, _ = worker_call(lambda: root.after(10000, calls.append, "unexpected"))
    _, errors = worker_call(lambda: root.after_cancel(results[0]))
    assert not errors
    drain(root)
    assert not calls


def test_queue_capacity_is_explicit(root):
    def fill():
        return [root.after(10000, lambda: None) for _ in range(512)]

    tokens, errors = worker_call(fill)
    assert not errors
    _, errors = worker_call(lambda: root.after(0, lambda: None))
    assert isinstance(errors[0], RuntimeError)
    for token in tokens[0]:
        root.after_cancel(token)
    assert not root._callback_inbox


def test_callback_failure_does_not_stop_delivery(root):
    errors, calls = [], []
    root.report_callback_exception = lambda *args: errors.append(args)

    def fail():
        raise ValueError("test callback")

    worker_call(lambda: root.after(0, fail))
    worker_call(lambda: root.after(0, calls.append, "delivered"))
    drain(root)
    assert len(errors) == 1
    assert calls == ["delivered"]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Tcl handle regression")
def test_short_lived_worker_notifications_do_not_accumulate_handles(root):
    # Warm up Tk/threading, then simulate 1000 minutes of short-lived workers.
    worker_call(lambda: root.after(0, lambda: None))
    drain(root)
    baseline = _process_resource_snapshot()
    calls = []
    for _ in range(10):
        for _ in range(100):
            _, errors = worker_call(lambda: root.after(0, calls.append, 1))
            assert not errors
        drain(root)
    final = _process_resource_snapshot()
    assert len(calls) == 1000
    assert final["handles"] <= baseline["handles"] + 10, (baseline, final)
    assert final["private_mb"] <= baseline["private_mb"] + 4, (baseline, final)


def test_shutdown_releases_pending_callbacks_and_rejects_new_work(root):
    worker_call(lambda: root.after(10000, lambda: None))
    root.destroy()
    assert root.pending_worker_callbacks == 0
    assert not root._callback_inbox
    _, errors = worker_call(lambda: root.after(0, lambda: None))
    assert isinstance(errors[0], RuntimeError)
