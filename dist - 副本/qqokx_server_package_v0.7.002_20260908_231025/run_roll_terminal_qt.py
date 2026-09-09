from __future__ import annotations

import ctypes
import faulthandler
import importlib.util
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


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _venv_python_candidates() -> tuple[Path, ...]:
    scripts_dir = _repo_root() / ".venv" / "Scripts"
    current_name = Path(sys.executable).name.lower()
    ordered_names = ("pythonw.exe", "python.exe") if current_name == "pythonw.exe" else ("python.exe", "pythonw.exe")
    return tuple(scripts_dir / name for name in ordered_names)


def _bootstrap_local_venv() -> None:
    if importlib.util.find_spec("PySide6") is not None:
        return
    current_executable = Path(sys.executable).resolve()
    for candidate in _venv_python_candidates():
        if not candidate.exists():
            continue
        if candidate.resolve() == current_executable:
            return
        os.execv(str(candidate), [str(candidate), str(Path(__file__).resolve()), *sys.argv[1:]])


def _set_console_title() -> None:
    try:
        ctypes.windll.kernel32.SetConsoleTitleW("量化合约终端")
    except Exception:
        pass


def _console_log_path() -> Path:
    target = logs_dir_path() / "roll_terminal_qt"
    target.mkdir(parents=True, exist_ok=True)
    return target / f"console_{datetime.now().strftime('%Y-%m-%d')}.log"


def _write_runtime_banner(log_path: Path) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 启动量化合约终端\n")


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
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 崩溃线程：", file=sys.stderr)
        traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)

    sys.excepthook = _log_uncaught_exception

    if hasattr(threading, "excepthook"):

        def _threading_excepthook(args: threading.ExceptHookArgs) -> None:
            print(
                f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 工作线程崩溃：{args.thread.name if args.thread else 'unknown'}",
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


def _ensure_qt_dependency() -> bool:
    if sys.maxsize <= 2 ** 32:
        print("[QQOKX] 当前是32位 Python，建议改用 64 位 Python 3.11+ 后启动。", file=sys.stderr)

    try:
        __import__("PySide6")
        return True
    except Exception as exc:
        print(f"[QQOKX] 当前启动解释器：{sys.executable}", file=sys.stderr)
        print("[QQOKX] 未检测到 Qt 运行时依赖 PySide6。请先安装后再启动：", file=sys.stderr)
        print(f"[QQOKX] 安装命令：{sys.executable} -m pip install -r roll_terminal_qt_requirements.txt", file=sys.stderr)
        print("[QQOKX] 当前目录要求：在与程序同一目录执行上述命令，或先切到同目录再执行。", file=sys.stderr)
        print(f"[QQOKX] 原始错误：{type(exc).__name__}: {exc}", file=sys.stderr)
        return False


def main() -> int:
    _set_console_title()
    _bootstrap_local_venv()
    _install_runtime_logging()
    _configure_qt_webengine_runtime()
    if not _ensure_qt_dependency():
        return 1
    from roll_terminal_qt.launcher import run

    return run()


if __name__ == "__main__":
    raise SystemExit(main())
