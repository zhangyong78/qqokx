from __future__ import annotations

import tomllib
from pathlib import Path


APP_NAME = "OKX 策略工作台"
DEFAULT_APP_VERSION = "0.1.0"


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_app_version() -> str:
    pyproject = project_root() / "pyproject.toml"
    if not pyproject.exists():
        return DEFAULT_APP_VERSION
    try:
        payload = tomllib.loads(pyproject.read_text(encoding="utf-8"))
        project = payload.get("project", {})
        version = str(project.get("version", "")).strip()
        return version or DEFAULT_APP_VERSION
    except Exception:
        return DEFAULT_APP_VERSION


APP_VERSION = load_app_version()


def build_app_title() -> str:
    return f"{APP_NAME} v{APP_VERSION}"


def build_version_info_text() -> str:
    return (
        f"{APP_NAME}\n"
        f"版本：v{APP_VERSION}\n"
        f"版本来源：pyproject.toml"
    )
