"""Keep server worker notifications out of the Windows Tcl thread allocator.

Tcl keeps per-thread state after a short-lived Python thread calls Tk.  In a
long-running server, creating threads which call ``after`` can retain an event
handle and roughly 16 KB per thread even after the Python thread has exited.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import threading
import time
from tkinter import Tk
from typing import Callable


@dataclass
class _Callback:
    token: str
    deadline: float
    function: Callable
    args: tuple
    native_id: str | None = None


class ServerTk(Tk):
    """Use native Tk scheduling on the owner thread and a bounded inbox elsewhere."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._callback_owner = threading.get_ident()
        self._callback_lock = threading.Lock()
        self._callback_inbox: deque[_Callback] = deque()
        self._worker_callbacks: dict[str, _Callback] = {}
        self._callback_serial = 0
        self._callbacks_closed = False
        self._callback_pump_id = super().after(50, self._drain_worker_callbacks)

    @property
    def pending_worker_callbacks(self) -> int:
        with self._callback_lock:
            return len(self._worker_callbacks)

    def after(self, ms, func=None, *args):
        if threading.get_ident() == self._callback_owner:
            return super().after(ms, func, *args)
        if func is None:
            raise RuntimeError("后台线程不能使用 Tk 的阻塞等待")
        deadline = time.monotonic() + max(0, int(ms)) / 1000
        with self._callback_lock:
            if self._callbacks_closed:
                raise RuntimeError("界面已关闭")
            if len(self._worker_callbacks) >= 512:
                # Never silently drop settlement, recovery or account results.
                raise RuntimeError("界面回调队列已满")
            self._callback_serial += 1
            token = f"worker-after-{self._callback_serial}"
            callback = _Callback(token, deadline, func, args)
            self._worker_callbacks[token] = callback
            self._callback_inbox.append(callback)
            return token

    def after_cancel(self, identifier):
        if threading.get_ident() != self._callback_owner:
            self.after(0, self.after_cancel, identifier)
            return
        with self._callback_lock:
            callback = self._worker_callbacks.pop(identifier, None)
            if callback is not None and callback.native_id is None:
                self._callback_inbox.remove(callback)
        if callback is not None:
            if callback.native_id is not None:
                super().after_cancel(callback.native_id)
        elif not str(identifier).startswith("worker-after-"):
            super().after_cancel(identifier)

    def _run_worker_callback(self, callback: _Callback) -> None:
        with self._callback_lock:
            if self._worker_callbacks.pop(callback.token, None) is None:
                return
        callback.function(*callback.args)

    def _drain_worker_callbacks(self) -> None:
        # Each tick is bounded so a busy display cannot starve the main loop.
        for _ in range(100):
            with self._callback_lock:
                if not self._callback_inbox:
                    break
                callback = self._callback_inbox.popleft()
                active = callback.token in self._worker_callbacks
            if active:
                delay = max(0, round((callback.deadline - time.monotonic()) * 1000))
                callback.native_id = super().after(delay, self._run_worker_callback, callback)
        self._callback_pump_id = super().after(50, self._drain_worker_callbacks)

    def destroy(self) -> None:
        with self._callback_lock:
            self._callbacks_closed = True
            callbacks = list(self._worker_callbacks.values())
            self._worker_callbacks.clear()
            self._callback_inbox.clear()
        super().after_cancel(self._callback_pump_id)
        for callback in callbacks:
            if callback.native_id is not None:
                super().after_cancel(callback.native_id)
        super().destroy()
