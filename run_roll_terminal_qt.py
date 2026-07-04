from __future__ import annotations

import ctypes
import faulthandler
import io
import os
import sys
import threading
import traceback
from datetime import datetime
from pathlib import Path

from okx_quant.app_paths import logs_dir_path


class _TeeTextStream(io.TextIOBase):
    def __init__(self, *, file_handle: io.TextIOWrapper, original: io.TextIOBase | None) -> None:
        self._file_handle = file_handle
        self._original = original

    @property
    def encoding(self) -> str:
        return getattr(self._file_handle, "encoding", "utf-8")

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._file_handle.write(text)
        self._file_handle.flush()
        original = self._original
        if original is not None:
            try:
                original.write(text)
                original.flush()
            except Exception:
                pass
        return len(text)

    def flush(self) -> None:
        self._file_handle.flush()
        original = self._original
        if original is not None:
            try:
                original.flush()
            except Exception:
                pass


def _set_console_title() -> None:
    try:
        ctypes.windll.kernel32.SetConsoleTitleW("量化交易控制台")
    except Exception:
        pass


def _console_log_path() -> Path:
    target = logs_dir_path() / "roll_terminal_qt"
    target.mkdir(parents=True, exist_ok=True)
    return target / f"console_{datetime.now().strftime('%Y-%m-%d')}.log"


def _write_runtime_banner(log_path: Path) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 启动量化交易控制台\n")


def _install_runtime_logging() -> Path:
    log_path = _console_log_path()
    _write_runtime_banner(log_path)
    file_handle = log_path.open("a", encoding="utf-8", buffering=1)
    stdout_stream = _TeeTextStream(file_handle=file_handle, original=sys.__stdout__)
    stderr_stream = _TeeTextStream(file_handle=file_handle, original=sys.__stderr__)
    sys.stdout = stdout_stream
    sys.stderr = stderr_stream

    try:
        faulthandler.enable(file=file_handle, all_threads=True)
    except Exception:
        pass

    def _log_uncaught_exception(exc_type, exc_value, exc_traceback) -> None:  # type: ignore[no-untyped-def]
        if issubclass(exc_type, KeyboardInterrupt):
            try:
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
            except Exception:
                pass
            return
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 未捕获异常：", file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

    sys.excepthook = _log_uncaught_exception

    if hasattr(threading, "excepthook"):

        def _threading_excepthook(args: threading.ExceptHookArgs) -> None:
            print(
                f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 线程未捕获异常：{args.thread.name if args.thread else 'unknown'}",
                file=sys.stderr,
            )
            traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback, file=sys.stderr)

        threading.excepthook = _threading_excepthook

    print(f"[启动日志] 控制台输出已写入：{log_path}", file=sys.stderr)
    return log_path


def _configure_qt_webengine_runtime() -> None:
    flags = os.environ.get("QTWEBENGINE_CHROMIUM_FLAGS", "").strip()
    extra_flags = (
        "--disable-direct-composition",
        "--disable-gpu",
        "--disable-gpu-compositing",
        "--disable-gpu-rasterization",
    )
    merged_flags = [flag for flag in flags.split() if flag]
    for flag in extra_flags:
        if flag not in merged_flags:
            merged_flags.append(flag)
    if merged_flags:
        os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = " ".join(merged_flags)
    os.environ.setdefault("QT_OPENGL", "software")
    os.environ.setdefault("QT_QUICK_BACKEND", "software")
    os.environ.setdefault("QTWEBENGINE_DISABLE_GPU", "1")
    print(
        "[QQOKX] QtWebEngine fallback:"
        f" QT_OPENGL={os.environ.get('QT_OPENGL')}"
        f" QT_QUICK_BACKEND={os.environ.get('QT_QUICK_BACKEND')}"
        f" QTWEBENGINE_DISABLE_GPU={os.environ.get('QTWEBENGINE_DISABLE_GPU')}"
        f" QTWEBENGINE_CHROMIUM_FLAGS={os.environ.get('QTWEBENGINE_CHROMIUM_FLAGS')}",
        file=sys.stderr,
    )


def main() -> int:
    _set_console_title()
    _install_runtime_logging()
    _configure_qt_webengine_runtime()
    from roll_terminal_qt.launcher import run

    return run()


if __name__ == "__main__":
    raise SystemExit(main())
