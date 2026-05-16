from __future__ import annotations

from pathlib import Path


def build_research_brief() -> str:
    template_path = Path(__file__).resolve().parent.parent / "research_brief_template.md"
    return template_path.read_text(encoding="utf-8")
