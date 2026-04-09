from __future__ import annotations

import re
from datetime import datetime


_TIMESTAMP_PREFIX_RE = re.compile(r"^\[\d{2}:\d{2}:\d{2}\](?:\s|$)")


def current_log_timestamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def ensure_log_timestamp(message: str, *, timestamp: str | None = None) -> str:
    text = (message or "").strip()
    if not text:
        return text
    if _TIMESTAMP_PREFIX_RE.match(text):
        return text
    prefix = timestamp or current_log_timestamp()
    return f"[{prefix}] {text}"
