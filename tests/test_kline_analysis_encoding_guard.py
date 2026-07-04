from __future__ import annotations

import ast
import re
from pathlib import Path


TARGET = Path(__file__).resolve().parents[1] / "roll_terminal_qt" / "kline_analysis_window.py"
SUSPICIOUS_FRAGMENTS = (
    "鍛ㄦ湡",
    "鍙屽浘",
    "鍔犺浇",
    "鑷姩",
    "閲嶇疆",
    "鍧囩嚎",
    "棰勮",
    "鐢荤嚎",
    "璇疯緭鍏",
    "鏃堕棿",
    "鏃ョ嚎瓒嬪娍",
    "鍥捐〃",
    "鎴愪氦閲",
    "涓€",
    "楂?",
    "浣?",
    "鏀?",
    "閲?",
)


def _read_source() -> str:
    return TARGET.read_text(encoding="utf-8")


def test_kline_analysis_source_is_utf8_and_parseable() -> None:
    source = _read_source()
    ast.parse(source, filename=str(TARGET))


def test_kline_analysis_source_has_no_known_mojibake_markers() -> None:
    source = _read_source()
    assert "\ufffd" not in source, "found Unicode replacement character in source"
    assert re.search(r"[\ue000-\uf8ff]", source) is None, "found private-use Unicode character in source"
    hits = [fragment for fragment in SUSPICIOUS_FRAGMENTS if fragment in source]
    assert not hits, f"found mojibake fragments: {hits}"
