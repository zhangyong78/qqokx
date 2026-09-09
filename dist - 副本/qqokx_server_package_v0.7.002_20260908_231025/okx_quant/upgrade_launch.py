from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path


UPGRADE_LAUNCH_MODE_AUTO = "auto"
UPGRADE_LAUNCH_MODE_CUSTOM = "custom"
UPGRADE_LAUNCH_MODE_NONE = "none"
UPGRADE_LAUNCH_MODE_VALUES = (
    UPGRADE_LAUNCH_MODE_AUTO,
    UPGRADE_LAUNCH_MODE_CUSTOM,
    UPGRADE_LAUNCH_MODE_NONE,
)
UPGRADE_CUSTOM_EXECUTABLE_NAME = "qqokx.exe"


@dataclass(frozen=True)
class UpgradeLaunchPlan:
    mode: str
    command: tuple[str, ...] | None
    working_directory: str | None
    resolved_executable: str | None

    @property
    def should_launch(self) -> bool:
        return bool(self.command)


class UpgradeLaunchManager:
    @staticmethod
    def default_mode(*, frozen: bool | None = None) -> str:
        is_frozen = bool(getattr(sys, "frozen", False) if frozen is None else frozen)
        return UPGRADE_LAUNCH_MODE_AUTO if is_frozen else UPGRADE_LAUNCH_MODE_NONE

    @classmethod
    def normalize_mode(cls, value: object, *, frozen: bool | None = None) -> str:
        text = str(value or "").strip().lower()
        if text in UPGRADE_LAUNCH_MODE_VALUES:
            return text
        return cls.default_mode(frozen=frozen)

    @staticmethod
    def normalize_custom_launch_path(value: object) -> str:
        return str(value or "").strip()

    @classmethod
    def resolve_custom_launch_path(cls, value: object) -> Path:
        raw = cls.normalize_custom_launch_path(value)
        if not raw:
            raise ValueError("请选择启动目录，或填写 qqokx.exe 的完整路径。")
        candidate = Path(raw).expanduser()
        if candidate.exists() and candidate.is_dir():
            candidate = candidate / UPGRADE_CUSTOM_EXECUTABLE_NAME
        elif candidate.suffix.lower() != ".exe":
            candidate = candidate / UPGRADE_CUSTOM_EXECUTABLE_NAME
        resolved = candidate.resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"启动目标不存在：{resolved}")
        if resolved.is_dir():
            raise ValueError(f"启动目标无效，未找到 {UPGRADE_CUSTOM_EXECUTABLE_NAME}：{resolved}")
        if resolved.name.lower() != UPGRADE_CUSTOM_EXECUTABLE_NAME:
            raise ValueError(f"自定义启动目标必须是 {UPGRADE_CUSTOM_EXECUTABLE_NAME}。")
        return resolved

    @classmethod
    def build_plan(
        cls,
        *,
        mode: object,
        current_version_command: list[str] | tuple[str, ...],
        current_version_workdir: str | Path,
        custom_launch_path: object = "",
    ) -> UpgradeLaunchPlan:
        normalized_mode = cls.normalize_mode(mode)
        if normalized_mode == UPGRADE_LAUNCH_MODE_NONE:
            return UpgradeLaunchPlan(
                mode=normalized_mode,
                command=None,
                working_directory=None,
                resolved_executable=None,
            )
        if normalized_mode == UPGRADE_LAUNCH_MODE_AUTO:
            command = tuple(str(part) for part in current_version_command if str(part).strip())
            if not command:
                raise ValueError("当前版本启动命令为空，无法自动启动。")
            return UpgradeLaunchPlan(
                mode=normalized_mode,
                command=command,
                working_directory=str(Path(current_version_workdir).resolve()),
                resolved_executable=command[0],
            )
        resolved = cls.resolve_custom_launch_path(custom_launch_path)
        return UpgradeLaunchPlan(
            mode=normalized_mode,
            command=(str(resolved),),
            working_directory=str(resolved.parent),
            resolved_executable=str(resolved),
        )

