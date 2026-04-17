from __future__ import annotations

import re
import threading
from datetime import datetime
from pathlib import Path


_TIMESTAMP_PREFIX_RE = re.compile(r"^\[(?:\d{4}-)?\d{2}-\d{2} \d{2}:\d{2}:\d{2}\](?:\s|$)")
_LOG_FILE_LOCK = threading.Lock()
_STRATEGY_LOGS_DIR_NAME = "strategy_sessions"
_SAFE_LOG_TOKEN_RE = re.compile(r"[^A-Za-z0-9._-]+")


def current_log_timestamp() -> str:
    return datetime.now().strftime("%m-%d %H:%M:%S")


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


def strategy_session_logs_dir(*, base_dir: str | Path | None = None) -> Path:
    return logs_dir(base_dir=base_dir) / _STRATEGY_LOGS_DIR_NAME


def _safe_log_token(value: str) -> str:
    cleaned = _SAFE_LOG_TOKEN_RE.sub("_", value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "session"


def strategy_session_log_file_path(
    *,
    started_at: datetime,
    session_id: str,
    strategy_name: str,
    symbol: str,
    api_name: str = "",
    base_dir: str | Path | None = None,
) -> Path:
    date_dir = strategy_session_logs_dir(base_dir=base_dir) / started_at.strftime("%Y-%m-%d")
    tokens = [
        started_at.strftime("%Y%m%d_%H%M%S_%f"),
        _safe_log_token(api_name) if api_name else "",
        _safe_log_token(session_id),
        _safe_log_token(strategy_name),
        _safe_log_token(symbol),
    ]
    file_name = "__".join(token for token in tokens if token) + ".log"
    return date_dir / file_name


def daily_log_file_path(
    *, for_time: datetime | None = None, base_dir: str | Path | None = None
) -> Path:
    target_time = for_time or datetime.now()
    return logs_dir(base_dir=base_dir) / f"{target_time.strftime('%Y-%m-%d')}.log"


def _append_line_to_file(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line)
        handle.write("\n")


def append_preformatted_log_line(
    line: str,
    *,
    path: str | Path,
) -> str:
    text = (line or "").strip()
    if not text:
        return text
    target = Path(path)
    with _LOG_FILE_LOCK:
        _append_line_to_file(target, text)
    return text


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
        timestamp=timestamp or target_time.strftime("%m-%d %H:%M:%S"),
    )
    if not line:
        return line
    path = daily_log_file_path(for_time=target_time, base_dir=base_dir)
    with _LOG_FILE_LOCK:
        _append_line_to_file(path, line)
    return line
