from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from okx_quant.app_paths import data_root, logs_dir_path, state_dir_path
from okx_quant.auto_channel_preview import build_auto_channel_preview_snapshot
from okx_quant.persistence import (
    line_trading_desk_annotations_file_path,
    load_credentials_profiles_snapshot,
    load_line_trading_desk_annotations_entries,
    load_smart_order_favorites_snapshot,
    load_smart_order_tasks_snapshot,
    smart_order_favorites_file_path,
    smart_order_tasks_file_path,
)


@dataclass(frozen=True)
class ModuleOverview:
    status: str
    phase: str
    summary_lines: tuple[str, ...]
    data_paths: tuple[Path, ...] = ()
    next_steps: tuple[str, ...] = ()


@dataclass(frozen=True)
class LauncherModuleSpec:
    key: str
    title: str
    subtitle: str
    status: str


def launcher_module_specs() -> tuple[LauncherModuleSpec, ...]:
    return (
        LauncherModuleSpec(
            key="roll",
            title="专业套利终端",
            subtitle="主壳负责统一入口、共享配置和核心交易流程。",
            status="Qt 主模块",
        ),
        LauncherModuleSpec(
            key="smart-order",
            title="无限下单",
            subtitle="纯 Qt 版直接接入共享任务、收藏、仓位限制和实时状态。",
            status="Qt 原生",
        ),
        LauncherModuleSpec(
            key="line-trading",
            title="划线交易台",
            subtitle="纯 Qt 版统一管理共享射线注解和 RR 区块。",
            status="Qt 原生",
        ),
        LauncherModuleSpec(
            key="auto-channel",
            title="自动通道",
            subtitle="纯 Qt 版统一做样例、市场行情和历史快照结构分析。",
            status="Qt 原生",
        ),
    )


def build_roll_module_overview() -> ModuleOverview:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    selected_profile = str(snapshot.get("selected_profile", "") or "").strip() if isinstance(snapshot, dict) else ""
    return ModuleOverview(
        status="已接入",
        phase="Qt 主壳",
        summary_lines=(
            f"共享数据根目录：{data_root()}",
            f"可用 API Profile：{len(profiles) if isinstance(profiles, dict) else 0} 个"
            + (f" | 当前：{selected_profile}" if selected_profile else ""),
            f"日志目录：{logs_dir_path()}",
        ),
        data_paths=(logs_dir_path(), state_dir_path()),
        next_steps=("继续把主壳上的运行态监控、日志聚合和模块跳转做得更顺手。",),
    )


def build_smart_order_module_overview() -> ModuleOverview:
    snapshot = load_smart_order_tasks_snapshot()
    favorites = load_smart_order_favorites_snapshot()
    tasks = snapshot.get("tasks", []) if isinstance(snapshot, dict) else []
    favorite_items = favorites.get("favorites", []) if isinstance(favorites, dict) else []
    locked_inst_id = str(snapshot.get("locked_inst_id", "") or "").strip() if isinstance(snapshot, dict) else ""
    return ModuleOverview(
        status="Qt 原生",
        phase="任务与下单一体化",
        summary_lines=(
            f"任务数：{len(tasks) if isinstance(tasks, list) else 0}",
            f"收藏标的：{len(favorite_items) if isinstance(favorite_items, list) else 0}",
            f"当前锁定标的：{locked_inst_id or '-'}",
        ),
        data_paths=(smart_order_tasks_file_path(), smart_order_favorites_file_path()),
        next_steps=("继续细化盘口交互和任务恢复体验。",),
    )


def build_line_trading_module_overview() -> ModuleOverview:
    entries = load_line_trading_desk_annotations_entries()
    session_count = len(entries)
    total_lines = 0
    total_rr = 0
    for entry in entries.values():
        lines = entry.get("lines")
        rr = entry.get("rr")
        if isinstance(lines, list):
            total_lines += len(lines)
        if isinstance(rr, list):
            total_rr += len(rr)
    return ModuleOverview(
        status="Qt 原生",
        phase="共享注解管理",
        summary_lines=(
            f"已保存会话：{session_count}",
            f"画线条目：{total_lines}",
            f"盈亏比区域：{total_rr}",
        ),
        data_paths=(line_trading_desk_annotations_file_path(),),
        next_steps=("继续把图形编辑和下单联动往 Qt 画布里迁。",),
    )


def build_auto_channel_module_overview() -> ModuleOverview:
    snapshot = build_auto_channel_preview_snapshot()
    return ModuleOverview(
        status="Qt 原生",
        phase="结构分析",
        summary_lines=(
            f"样例 K 线：{len(snapshot.candles)}",
            f"通道覆盖层：{len(snapshot.band_overlays)} | 箱体覆盖层：{len(snapshot.box_overlays)}",
            f"分析摘要：{snapshot.note or '-'}",
        ),
        next_steps=("继续强化结构快照的对比、标注和结果确认链路。",),
    )


def build_module_overview(module_key: str) -> ModuleOverview:
    normalized = module_key.strip().lower()
    if normalized == "roll":
        return build_roll_module_overview()
    if normalized == "smart-order":
        return build_smart_order_module_overview()
    if normalized == "line-trading":
        return build_line_trading_module_overview()
    if normalized == "auto-channel":
        return build_auto_channel_module_overview()
    raise KeyError(f"unknown module: {module_key}")
