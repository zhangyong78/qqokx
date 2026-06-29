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
            subtitle="Qt 全量接入模块，负责套利开平仓、交割移仓与监控。",
            status="已接入",
        ),
        LauncherModuleSpec(
            key="smart-order",
            title="无限下单",
            subtitle="先完成 Qt 主壳入口和共享状态归拢，再逐步迁移交互与执行面板。",
            status="迁移底座",
        ),
        LauncherModuleSpec(
            key="line-trading",
            title="划线交易台",
            subtitle="先集中管理入口与注解状态，再把图表、画线和下单链路搬到 Qt。",
            status="迁移底座",
        ),
        LauncherModuleSpec(
            key="auto-channel",
            title="自动通道",
            subtitle="复用现有分析内核，先以 Qt 模块页承接预览与后续扩展。",
            status="迁移底座",
        ),
    )


def build_roll_module_overview() -> ModuleOverview:
    snapshot = load_credentials_profiles_snapshot()
    profiles = snapshot.get("profiles", {}) if isinstance(snapshot, dict) else {}
    profile_count = len(profiles) if isinstance(profiles, dict) else 0
    selected_profile = str(snapshot.get("selected_profile", "") or "").strip() if isinstance(snapshot, dict) else ""
    return ModuleOverview(
        status="已接入",
        phase="Qt 全量模块",
        summary_lines=(
            f"共享数据根目录：{data_root()}",
            f"可用 API Profile：{profile_count} 个" + (f" | 当前：{selected_profile}" if selected_profile else ""),
            f"日志目录：{logs_dir_path()}",
        ),
        data_paths=(logs_dir_path(), state_dir_path()),
        next_steps=(
            "继续把套利运行态与模块管理页打通，支持从主壳直接查看模块心跳和日志。",
        ),
    )


def build_smart_order_module_overview() -> ModuleOverview:
    snapshot = load_smart_order_tasks_snapshot()
    favorites = load_smart_order_favorites_snapshot()
    tasks = snapshot.get("tasks", []) if isinstance(snapshot, dict) else []
    favorite_items = favorites.get("favorites", []) if isinstance(favorites, dict) else []
    locked_inst_id = str(snapshot.get("locked_inst_id", "") or "").strip() if isinstance(snapshot, dict) else ""
    return ModuleOverview(
        status="迁移底座",
        phase="Qt 入口已建立",
        summary_lines=(
            f"任务数：{len(tasks) if isinstance(tasks, list) else 0}",
            f"收藏标的：{len(favorite_items) if isinstance(favorite_items, list) else 0}",
            f"当前锁定标的：{locked_inst_id or '-'}",
        ),
        data_paths=(smart_order_tasks_file_path(), smart_order_favorites_file_path()),
        next_steps=(
            "下一步优先迁移任务列表、参数面板和运行状态条，再接入实盘执行线程。",
        ),
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
        status="迁移底座",
        phase="Qt 入口已建立",
        summary_lines=(
            f"已保存会话：{session_count}",
            f"画线条目：{total_lines}",
            f"盈亏比区域：{total_rr}",
        ),
        data_paths=(line_trading_desk_annotations_file_path(),),
        next_steps=(
            "下一步优先迁移实时图表画布、画线交互和开仓参数卡。",
        ),
    )


def build_auto_channel_module_overview() -> ModuleOverview:
    snapshot = build_auto_channel_preview_snapshot()
    return ModuleOverview(
        status="迁移底座",
        phase="Qt 入口已建立",
        summary_lines=(
            f"样例 K 线：{len(snapshot.candles)}",
            f"通道覆盖层：{len(snapshot.band_overlays)} | 箱体覆盖层：{len(snapshot.box_overlays)}",
            f"分析摘要：{snapshot.note or '-'}",
        ),
        next_steps=(
            "下一步优先接入真实行情快照、参数调节面板和结果确认流。",
        ),
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

