from __future__ import annotations

import re
import threading
from datetime import datetime
from pathlib import Path


_TIMESTAMP_PREFIX_RE = re.compile(r"^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\](?:\s|$)")
_LOG_FILE_LOCK = threading.Lock()


def current_log_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_log_timestamp(message: str, *, timestamp: str | None = None) -> str:
    text = (message or "").strip()
    if not text:
        return text
    if _TIMESTAMP_PREFIX_RE.match(text):
        return text
    prefix = timestamp or current_log_timestamp()
    return f"[{prefix}] {text}"


def logs_dir(*, base_dir: str | Path | None = None) -> Path:
    root = Path(base_dir) if base_dir is not None else Path(__file__).resolve().parents[1]
    return root / "logs"


def daily_log_file_path(
    *, for_time: datetime | None = None, base_dir: str | Path | None = None
) -> Path:
    target_time = for_time or datetime.now()
    return logs_dir(base_dir=base_dir) / f"{target_time.strftime('%Y-%m-%d')}.log"


def append_log_line(
    message: str,
    *,
    timestamp: str | None = None,
    now: datetime | None = None,
    base_dir: str | Path | None = None,
) -> str:
    target_time = now or datetime.now()
    line = ensure_log_timestamp(
        message,
        timestamp=timestamp or target_time.strftime("%Y-%m-%d %H:%M:%S"),
    )
    if not line:
        return line
    path = daily_log_file_path(for_time=target_time, base_dir=base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOG_FILE_LOCK:
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.write("\n")
    return line
