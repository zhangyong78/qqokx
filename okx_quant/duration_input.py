"""Parse human-friendly duration strings into seconds (non-negative int)."""

from __future__ import annotations

import re


_DURATION_SPECS: list[tuple[re.Pattern[str], int]] = [
    (re.compile(r"(\d+)\s*(?:days?|天)", re.IGNORECASE), 86400),
    (re.compile(r"(\d+)\s*(?:hours?|hrs?|小时)", re.IGNORECASE), 3600),
    (re.compile(r"(\d+)\s*(?:minutes?|mins?|分钟)", re.IGNORECASE), 60),
    (re.compile(r"(\d+)\s*(?:seconds?|秒)", re.IGNORECASE), 1),
    (re.compile(r"(\d+)\s*(?:时|h)", re.IGNORECASE), 3600),
    (re.compile(r"(\d+)\s*(?:分|m)", re.IGNORECASE), 60),
    (re.compile(r"(\d+)\s*s", re.IGNORECASE), 1),
    (re.compile(r"(\d+)\s*d", re.IGNORECASE), 86400),
]


def parse_nonnegative_duration_seconds(raw: str, *, field_name: str) -> int:
    """Parse a duration for UI fields: plain integer seconds, or composed units (e.g. ``2h30m``, ``1天``)."""
    text = raw.strip()
    if not text or text == "0":
        return 0
    if re.fullmatch(r"\d+", text):
        value = int(text)
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value
    compact = re.sub(r"\s+", "", text)
    total = 0
    index = 0
    while index < len(compact):
        matched = False
        for pattern, multiplier in _DURATION_SPECS:
            match = pattern.match(compact, index)
            if match is not None:
                total += int(match.group(1)) * multiplier
                index = match.end()
                matched = True
                break
        if not matched:
            raise ValueError(
                f"{field_name} 无法解析：请填非负整数秒（如 300），"
                f"或组合写法（如 5m、2h30m、1天2小时、90分）。"
            )
    return total


def try_parse_nonnegative_duration_seconds(raw: str) -> int | None:
    """Best-effort parse for hint text; returns ``None`` if invalid or empty."""
    try:
        return parse_nonnegative_duration_seconds(raw, field_name="")
    except ValueError:
        return None


def format_duration_cn_compact(seconds: int) -> str:
    """Human-readable Chinese duration, omitting zero components (except ``0秒``)."""
    seconds = max(int(seconds), 0)
    if seconds == 0:
        return "0秒"
    parts: list[str] = []
    remaining = seconds
    for unit, label in (
        (86400, "天"),
        (3600, "小时"),
        (60, "分"),
        (1, "秒"),
    ):
        count, remaining = divmod(remaining, unit)
        if count:
            parts.append(f"{count}{label}")
    return "".join(parts) if parts else "0秒"
