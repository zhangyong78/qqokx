from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from PySide6.QtCore import QCoreApplication, QSettings, QTimer, Qt, QUrl, Slot
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QInputDialog,
    QProgressDialog,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QStackedWidget,
)

from okx_quant.app_meta import APP_VERSION, build_version_info_text
from okx_quant.ai_snapshot import load_ai_watchlist, normalize_watchlist, save_ai_watchlist
from okx_quant.app_paths import ai_snapshots_dir_path, config_dir_path, data_root, logs_dir_path, state_dir_path
from okx_quant.log_utils import append_log_line
from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget
from roll_terminal_qt.history_sync_manager import HISTORY_SYNC_SOURCES, get_history_sync_manager
from roll_terminal_qt.daily_trade_report_window import DailyTradeReportWidget
from roll_terminal_qt.app_icon import apply_qt_application_identity, apply_qt_window_icon
from roll_terminal_qt.auto_channel_window import AutoChannelWindow
from roll_terminal_qt.deribit_volatility_window import DeribitVolatilityQtWindow
from roll_terminal_qt.line_trading_window import LineTradingQtWindow
from roll_terminal_qt.module_overview import ModuleOverview, build_module_overview, launcher_module_specs
from roll_terminal_qt.option_strategy_window import OptionStrategyQtWindow
from roll_terminal_qt.option_roll_execution_window import OptionRollExecutionQtWindow
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from roll_terminal_qt.ai_snapshot_service import AIQuickSnapshotWorker, AISnapshotWorker
from roll_terminal_qt.sample_prediction_window import SamplePredictionWindow
from roll_terminal_qt.perf_metrics import measure_ui_step
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots
from roll_terminal_qt.runtime import load_runtime
from roll_terminal_qt.smart_order_window import SmartOrderQtWindow
from okx_quant.shape_signal_monitor import ShapeSignalMonitor
from okx_quant.shape_signal_store import load_events, mark_events_read
from roll_terminal_qt.style import APP_STYLE, apply_global_font_mode, normalize_global_font_mode
from roll_terminal_qt.ui import RollTerminalWindow
from roll_terminal_qt.workspace_shell import (
    LocalTaskCount,
    WorkspaceHeader,
    format_local_task_counts,
    preferred_profile_name,
)


def module_choices() -> tuple[str, ...]:
    return ("home",) + tuple(spec.key for spec in launcher_module_specs())


def parse_ai_quick_snapshot_symbols(text: str) -> list[str]:
    """Return selected symbols, using BTC when the input is intentionally blank."""
    if not str(text or "").strip():
        return ["BTC"]
    return normalize_watchlist(part.strip() for part in str(text).replace("，", ",").split(","))


def _standalone_command(module_key: str) -> str:
    return f"pythonw run_roll_terminal_qt.pyw --module {module_key}"


class SharedDataDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("数据中心")
        self.resize(780, 360)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        title = QLabel("数据中心")
        title.setObjectName("SectionTitle")
        subtitle = QLabel("这里集中展示程序共用的数据目录、配置目录、状态目录和日志目录。")
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        panel = QFrame()
        panel.setObjectName("Guide")
        grid = QGridLayout(panel)
        grid.setContentsMargins(16, 16, 16, 16)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(10)
        for row, (label, value) in enumerate(
            (
                ("数据根目录", str(data_root())),
                ("配置目录", str(config_dir_path())),
                ("状态目录", str(state_dir_path())),
                ("日志目录", str(logs_dir_path())),
                ("AI 快照目录", str(ai_snapshots_dir_path())),
            )
        ):
            key_label = QLabel(label)
            key_label.setObjectName("GuideText")
            value_label = QLabel(value)
            value_label.setObjectName("GuideText")
            value_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            grid.addWidget(key_label, row, 0)
            grid.addWidget(value_label, row, 1)
        layout.addWidget(panel)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.accepted.connect(self.accept)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class AISnapshotContentsDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("AI 快照内容说明")
        self.resize(820, 680)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        title = QLabel("AI 快照会导出的数据")
        title.setObjectName("SectionTitle")
        subtitle = QLabel("快照完全只读，现货不纳入，不会执行下单、撤单或修改备注。")
        subtitle.setObjectName("Subtle")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        details = QTextEdit()
        details.setReadOnly(True)
        details.setPlainText(
            "1. 快照基本信息\n"
            "   生成时间、有效分析时间、账户别名、API Profile、运行环境。\n\n"
            "2. 账户权益\n"
            "   总权益、可用权益、保证金、名义价值和账户币种明细。\n\n"
            "3. 人工衍生品持仓\n"
            "   永续、交割合约、期权；方向、数量、开仓价、标记价、指数价、\n"
            "   杠杆、保证金、强平价、盈亏、资金费率、期权 Greeks、备注和原始字段。\n\n"
            "4. 持仓与关注品种行情\n"
            "   1W、1D、4H、1H，各周期最多 250 根；每根包含 OHLCV、EMA15、\n"
            "   MA50、is_closed、bar_start、bar_end、elapsed_minutes、minutes_to_close、\n"
            "   bar_progress_pct、bar_status、reference_level 和 indicator_is_provisional。\n"
            "   每个周期同时提供 last_closed_bar 和 current_bar。\n"
            "   持仓品种与手动关注品种会自动合并去重。\n\n"
            "5. DVOL 波动率\n"
            "   BTC/ETH 的 1H、4H、1D DVOL，包含 OHLC、EMA15、MA50 和 is_closed。\n\n"
            "6. 最近人工成交\n"
            "   最近 7 天、最多 100 条；包含成交时间、合约、方向、价格、数量、\n"
            "   手续费、盈亏、订单/成交编号和原始成交字段。\n\n"
            "7. 组合汇总\n"
            "   持仓数量、品种数量、总 Delta/Gamma/Vega/Theta、总已实现和未实现盈亏。\n\n"
            "8. 数据质量\n"
            "   生成前会直接向 OKX 和波动率网站补充最新 1H 数据，检查更新时间、\n"
            "   两边是否一致、周期是否足够 250 根，并记录 PASS/DEGRADED 和 warnings。\n"
            "   market_fetch_age_seconds / volatility_fetch_age_seconds 表示抓取后等待时间，\n"
            "   不代表当前K线已经运行多久；K线运行时间使用 elapsed_minutes。"
            "\n\n"
            "9. AI 精简快照\n"
            "   可按 BTC、ETH 等标的单独生成；行情采用压缩数组格式，\n"
            "   1W/1D/4H/1H 默认保留 40/80/100/120 根，DVOL 默认保留 60/80/100 根，\n"
            "   只包含所选标的及其人工衍生品持仓。"
        )
        layout.addWidget(details, 1)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText("关闭")
        layout.addWidget(buttons)


class HistorySyncDialog(QDialog):
    def __init__(
        self,
        *,
        parent: QWidget,
        profile_provider,
        runtime_provider,
        export_fills_callback: Callable[[], None],
        export_positions_callback: Callable[[], None],
    ) -> None:  # noqa: ANN001
        super().__init__(parent)
        apply_qt_window_icon(self)
        self.setWindowTitle("历史数据同步中心")
        self.resize(760, 520)
        self._profile_provider = profile_provider
        self._runtime_provider = runtime_provider
        self._export_fills_callback = export_fills_callback
        self._export_positions_callback = export_positions_callback
        self._manager = get_history_sync_manager()
        self._source_checks: dict[str, QCheckBox] = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)
        title = QLabel("历史数据同步中心")
        title.setObjectName("SectionTitle")
        layout.addWidget(title)
        hint = QLabel(
            "快速增量同步只检查最近数据；深度检查才会分页补齐历史成交、历史委托、历史仓位和账单。"
            "关闭窗口不会停止后台同步。"
        )
        hint.setWordWrap(True)
        hint.setObjectName("Subtle")
        layout.addWidget(hint)

        source_panel = QFrame()
        source_panel.setObjectName("Guide")
        source_layout = QHBoxLayout(source_panel)
        source_layout.addWidget(QLabel("同步内容："))
        for source, label in (("fills", "历史成交"), ("orders", "历史委托"), ("positions", "历史仓位"),
                              ("bills", "账户账单"), ("asset_bills", "充值提现")):
            check = QCheckBox(label)
            check.setChecked(True)
            self._source_checks[source] = check
            source_layout.addWidget(check)
        source_layout.addStretch(1)
        layout.addWidget(source_panel)

        self._status = QLabel("等待操作。")
        self._status.setWordWrap(True)
        self._status.setMinimumHeight(46)
        layout.addWidget(self._status)
        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMinimumHeight(220)
        layout.addWidget(self._log, 1)

        export_panel = QFrame()
        export_panel.setObjectName("Guide")
        export_layout = QHBoxLayout(export_panel)
        export_layout.addWidget(QLabel("完整导出："))
        export_fills_button = QPushButton("同步最新并导出全部复盘成交")
        export_fills_button.setToolTip("快速增量同步成交和委托后，导出本地全部成交记录")
        export_fills_button.clicked.connect(self._export_fills)
        export_layout.addWidget(export_fills_button)
        export_positions_button = QPushButton("同步最新并导出全部历史仓位")
        export_positions_button.setToolTip("快速增量同步历史仓位后，导出本地全部已结束仓位")
        export_positions_button.clicked.connect(self._export_positions)
        export_layout.addWidget(export_positions_button)
        export_layout.addStretch(1)
        layout.addWidget(export_panel)

        buttons = QHBoxLayout()
        quick_button = QPushButton("快速增量同步")
        quick_button.clicked.connect(lambda: self._start(False))
        buttons.addWidget(quick_button)
        deep_button = QPushButton("深度检查 / 补历史")
        deep_button.clicked.connect(lambda: self._start(True))
        buttons.addWidget(deep_button)
        stop_button = QPushButton("停止当前同步")
        stop_button.clicked.connect(self._stop)
        buttons.addWidget(stop_button)
        buttons.addStretch(1)
        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.close)
        buttons.addWidget(close_button)
        layout.addLayout(buttons)

        self._manager.status_changed.connect(self._on_status)
        self._manager.sync_failed.connect(self._on_failed)
        self._manager.sync_finished.connect(self._on_finished)

    def _current_context(self):  # noqa: ANN202
        runtime = self._runtime_provider()
        profile_name = str(self._profile_provider() or "").strip()
        environment = str(getattr(runtime, "environment", "") or "").strip()
        return profile_name, environment, runtime

    def _selected_sources(self) -> tuple[str, ...]:
        return tuple(source for source in HISTORY_SYNC_SOURCES if self._source_checks[source].isChecked())

    def _start(self, deep: bool) -> None:
        profile_name, _environment, runtime = self._current_context()
        sources = self._selected_sources()
        if runtime is None or not profile_name:
            self._status.setText("当前没有可用的 API 账户。")
            return
        if not sources:
            self._status.setText("请至少选择一种历史数据。")
            return
        accepted = self._manager.request_sync(
            runtime=runtime,
            profile_name=profile_name,
            sources=sources,
            deep=deep,
        )
        self._status.setText("已启动深度检查。" if deep and accepted else ("已启动快速增量同步。" if accepted else "当前已有同步任务，请等待完成。"))

    def _stop(self) -> None:
        profile_name, environment, _runtime = self._current_context()
        self._manager.stop(profile_name=profile_name, environment=environment)
        self._status.setText("已请求停止；当前网络请求结束后停止。")

    def _export_fills(self) -> None:
        self._status.setText("正在准备同步最新成交和委托，完成后打开导出窗口...")
        self._export_fills_callback()

    def _export_positions(self) -> None:
        self._status.setText("正在准备同步最新历史仓位，完成后打开导出窗口...")
        self._export_positions_callback()

    def _matches(self, profile_name: str, environment: str) -> bool:
        current_profile, current_environment, _runtime = self._current_context()
        return profile_name == current_profile and environment == current_environment

    def _on_status(self, profile_name: str, environment: str, source: str, message: str) -> None:
        if not self._matches(profile_name, environment):
            return
        self._status.setText(message)
        self._log.append(message)

    def _on_failed(self, profile_name: str, environment: str, source: str, message: str) -> None:
        if not self._matches(profile_name, environment):
            return
        text = f"{source} 同步失败：{message}"
        self._status.setText(text)
        self._log.append(text)

    def _on_finished(self, profile_name: str, environment: str, completed: object) -> None:
        if not self._matches(profile_name, environment):
            return
        completed_items = completed.get("completed", ()) if isinstance(completed, dict) else completed
        text = f"同步完成：{', '.join(str(item) for item in completed_items) or '没有完成的项目'}"
        self._status.setText(text)
        self._log.append(text)


class ModuleOverviewWindow(QMainWindow):
    def __init__(self, *, module_key: str, title: str, subtitle: str) -> None:
        super().__init__()
        apply_qt_window_icon(self)
        self._module_key = module_key
        self._title_text = title
        self._subtitle_text = subtitle
        self.setWindowTitle(f"{title} - Qt 模块页")
        self.resize(900, 640)

        root = QWidget()
        layout = QVBoxLayout(root)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)

        title_row = QHBoxLayout()
        title_label = QLabel(title)
        title_label.setObjectName("SectionTitle")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("Subtle")
        subtitle_label.setWordWrap(True)
        header = QVBoxLayout()
        header.addWidget(title_label)
        header.addWidget(subtitle_label)
        header_widget = QWidget()
        header_widget.setLayout(header)
        title_row.addWidget(header_widget, 1)

        self._status_badge = QLabel("")
        self._status_badge.setObjectName("Panel")
        self._status_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._status_badge.setMinimumWidth(120)
        title_row.addWidget(self._status_badge, 0, Qt.AlignmentFlag.AlignTop)
        layout.addLayout(title_row)

        self._phase_label = QLabel("")
        self._phase_label.setObjectName("Subtle")
        self._phase_label.setWordWrap(True)
        layout.addWidget(self._phase_label)

        self._summary_text = QTextEdit()
        self._summary_text.setReadOnly(True)
        self._summary_text.setMinimumHeight(220)
        layout.addWidget(self._summary_text, 1)

        footer = QHBoxLayout()
        self._command_label = QLabel(_standalone_command(module_key))
        self._command_label.setObjectName("Subtle")
        self._command_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        footer.addWidget(self._command_label, 1)
        refresh_button = QPushButton("刷新摘要")
        refresh_button.clicked.connect(self.refresh_overview)
        footer.addWidget(refresh_button)
        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.close)
        footer.addWidget(close_button)
        layout.addLayout(footer)

        self.setCentralWidget(root)
        self.refresh_overview()

    @Slot()
    def refresh_overview(self) -> None:
        overview = build_module_overview(self._module_key)
        self._apply_overview(overview)

    def _apply_overview(self, overview: ModuleOverview) -> None:
        self._status_badge.setText(overview.status)
        self._phase_label.setText(f"当前阶段：{overview.phase}")
        lines = ["模块摘要"]
        lines.extend(f"- {line}" for line in overview.summary_lines)
        if overview.data_paths:
            lines.append("")
            lines.append("共享文件")
            lines.extend(f"- {path}" for path in overview.data_paths)
        if overview.next_steps:
            lines.append("")
            lines.append("下一步")
            lines.extend(f"- {line}" for line in overview.next_steps)
        self._summary_text.setPlainText("\n".join(lines))


class ModuleCard(QFrame):
    def __init__(self, *, module_key: str, title: str, subtitle: str, status: str, open_callback) -> None:
        super().__init__()
        apply_qt_window_icon(self)
        self._module_key = module_key
        self._open_callback = open_callback
        self.setObjectName("Panel")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        top = QHBoxLayout()
        title_label = QLabel(title)
        title_label.setObjectName("SectionTitle")
        top.addWidget(title_label, 1)
        badge = QLabel(status)
        badge.setObjectName("Subtle")
        badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        badge.setMinimumWidth(88)
        top.addWidget(badge, 0, Qt.AlignmentFlag.AlignTop)
        layout.addLayout(top)

        subtitle_label = QLabel(subtitle)
        subtitle_label.setWordWrap(True)
        subtitle_label.setObjectName("Subtle")
        layout.addWidget(subtitle_label)

        self._summary_label = QLabel("")
        self._summary_label.setWordWrap(True)
        self._summary_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self._summary_label, 1)

        footer = QHBoxLayout()
        open_button = QPushButton("打开模块")
        open_button.clicked.connect(self._open_module)
        footer.addWidget(open_button)
        refresh_button = QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh_summary)
        footer.addWidget(refresh_button)
        layout.addLayout(footer)

        self.refresh_summary()

    @Slot()
    def refresh_summary(self) -> None:
        overview = build_module_overview(self._module_key)
        summary = [f"阶段：{overview.phase}"]
        summary.extend(f"- {line}" for line in overview.summary_lines[:3])
        summary.append(f"独立启动：{_standalone_command(self._module_key)}")
        self._summary_label.setText("\n".join(summary))

    @Slot()
    def _open_module(self) -> None:
        self._open_callback(self._module_key)


class LauncherWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self._settings = QSettings("qqokx", "roll-terminal-qt")
        self._global_font_mode = normalize_global_font_mode(self._settings.value("ui/global-font-mode", "standard"))
        app = QApplication.instance()
        if app is not None:
            self._global_font_mode = apply_global_font_mode(app, self._global_font_mode)
        self._child_windows: list[QWidget] = []
        self._shared_data_dialog: SharedDataDialog | None = None
        self._ai_snapshot_contents_dialog: AISnapshotContentsDialog | None = None
        self._history_sync_dialog: HistorySyncDialog | None = None
        self._history_sync_manager = get_history_sync_manager()
        self._shutdown_in_progress = False
        self._home_shutdown_started = False
        self._shutdown_pending_page_keys: set[str] = set()
        self._shutdown_started_at: datetime | None = None
        self._home_widget: AccountPositionsHomeWidget | None = None
        self._pages: dict[str, QWidget] = {}
        self._active_page_key = ""
        self._page_switch_in_progress = False
        self._pending_page_key: str | None = None
        self._active_profile_name = ""
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        self._workspace_profile_serial = 0
        self._ai_snapshot_worker: AISnapshotWorker | None = None
        self._ai_quick_snapshot_worker: AIQuickSnapshotWorker | None = None
        self._ai_snapshot_progress_dialog: QProgressDialog | None = None
        self._shape_signal_monitor = ShapeSignalMonitor(self)
        self._shape_signal_monitor.status_changed.connect(self._on_shape_monitor_status)
        self._shape_signal_monitor.signal_detected.connect(self._on_shape_signal_detected)
        self._shape_popup_boxes: list[QMessageBox] = []
        self._shape_popup_box: QMessageBox | None = None
        self._shape_popup_display_events: list[dict[str, object]] = []
        self._shape_message_dialog = None
        self._shape_monitor_status = "等待后台形态监控启动"
        self._shape_startup_events: list[dict[str, object]] = []
        self._shape_startup_popup_timer = QTimer(self)
        self._shape_startup_popup_timer.setSingleShot(True)
        self._shape_startup_popup_timer.timeout.connect(self._show_shape_startup_summary)
        workspace_root = QWidget(self)
        workspace_layout = QVBoxLayout(workspace_root)
        workspace_layout.setContentsMargins(0, 0, 0, 0)
        workspace_layout.setSpacing(0)
        self._workspace_header = WorkspaceHeader(workspace_root)
        self._workspace_header.set_global_font_mode(self._global_font_mode)
        self._workspace_header.page_requested.connect(self.show_page)
        self._workspace_header.tool_requested.connect(self._handle_workspace_tool)
        self._workspace_header.profile_requested.connect(self._request_workspace_profile)
        self._history_sync_manager.status_changed.connect(self._on_history_sync_status)
        self._history_sync_manager.sync_failed.connect(self._on_history_sync_failed)
        self._page_stack = QStackedWidget(workspace_root)
        workspace_layout.addWidget(self._workspace_header)
        workspace_layout.addWidget(self._page_stack, 1)
        self._local_task_status = QLabel("")
        self._local_task_status.setObjectName("Subtle")
        self.statusBar().addPermanentWidget(self._local_task_status)
        self._local_task_timer = QTimer(self)
        self._local_task_timer.setInterval(1000)
        self._local_task_timer.timeout.connect(self._refresh_workspace_status)
        self._local_task_timer.start()
        self.setWindowTitle(f"量化交易控制台（本地版本） v{APP_VERSION}")
        self.resize(1680, 980)
        self.setCentralWidget(workspace_root)
        self._build_menu()
        self._refresh_shape_message_badge()
        self._shape_signal_monitor.start()
        self._initialize_workspace_profiles()
        QTimer.singleShot(1500, self._start_background_history_sync)
        with measure_ui_step("launcher_first_show"):
            self.show_page("kline")

    def current_page_key(self) -> str:
        return self._active_page_key

    def active_profile_name(self) -> str:
        return self._active_profile_name

    def _initialize_workspace_profiles(self) -> None:
        self._profile_snapshots, selected = load_profile_snapshots()
        names = list(self._profile_snapshots)
        target = preferred_profile_name(names, selected=selected)
        runtime = load_runtime(target) if target else None
        if runtime is not None:
            runtime_profile = str(getattr(runtime, "credential_profile_name", "") or "").strip()
            target = runtime_profile or target
        self._active_profile_name = target
        environment = str(getattr(runtime, "environment", "") or "").strip() if runtime is not None else ""
        self._workspace_header.set_profiles(names, target, environment)

    @Slot(str)
    def _request_workspace_profile(self, profile_name: str) -> None:
        target = profile_name.strip()
        previous = self._active_profile_name
        if not target or target == previous:
            return
        previous_runtime = load_runtime(previous) if previous else None
        if previous_runtime is not None:
            self._history_sync_manager.stop(
                profile_name=previous,
                environment=str(getattr(previous_runtime, "environment", "") or "").strip(),
            )
        self._profile_snapshots, _selected = load_profile_snapshots()
        runtime = load_runtime(target)
        if runtime is None or not ensure_profile_unlocked(
            self,
            target,
            self._profile_snapshots,
            self._unlocked_profiles,
        ):
            self._workspace_header.restore_profile(previous)
            return
        self._active_profile_name = target
        environment = str(getattr(runtime, "environment", "") or "").strip()
        self._workspace_header.set_profiles(list(self._profile_snapshots), target, environment)
        self._workspace_profile_serial += 1
        serial = self._workspace_profile_serial
        for page in tuple(self._pages.values()):
            if self._is_chart_page(page):
                QTimer.singleShot(
                    0,
                    lambda page=page, target=target, serial=serial: self._apply_page_workspace_profile(
                        page, target, serial
                    ),
                )
            else:
                self._apply_page_workspace_profile(page, target, serial)
        for window in tuple(self._child_windows):
            apply_profile = getattr(window, "apply_workspace_profile", None)
            if callable(apply_profile):
                apply_profile(target)
        QTimer.singleShot(500, self._start_background_history_sync)

    @Slot()
    def _start_background_history_sync(self) -> None:
        profile_name = str(self._active_profile_name or "").strip()
        runtime = load_runtime(profile_name) if profile_name else None
        if runtime is None:
            return
        self._history_sync_manager.request_sync(
            runtime=runtime,
            profile_name=profile_name,
            sources=HISTORY_SYNC_SOURCES,
            deep=False,
        )

    @Slot(str, str, str, str)
    def _on_history_sync_status(self, profile_name: str, environment: str, source: str, message: str) -> None:
        if profile_name != str(self._active_profile_name or "").strip():
            return
        self.statusBar().showMessage(message, 5000)

    @Slot(str, str, str, str)
    def _on_history_sync_failed(self, profile_name: str, environment: str, source: str, message: str) -> None:
        if profile_name != str(self._active_profile_name or "").strip():
            return
        self.statusBar().showMessage(f"{source} 同步失败：{message}", 12000)

    @staticmethod
    def _is_chart_page(page: QWidget) -> bool:
        return page.__class__.__name__ == "KlineAnalysisWindow"

    def _apply_page_workspace_profile(self, page: QWidget, profile_name: str, serial: int) -> None:
        if self._shutdown_in_progress or serial != self._workspace_profile_serial:
            return
        if page not in self._pages.values():
            return
        apply_profile = getattr(page, "apply_workspace_profile", None)
        if callable(apply_profile):
            apply_profile(profile_name)

    def _create_roll_page(self) -> QWidget:
        """Create the roll page while keeping lightweight test/embed stubs compatible."""
        try:
            return RollTerminalWindow(profile_name=self._active_profile_name)
        except (TypeError, AttributeError) as exc:
            # Some embedded/test replacements intentionally expose the old
            # zero-argument QWidget constructor. They can still receive the
            # profile through the normal workspace-profile hook below.
            try:
                page = RollTerminalWindow()
            except (TypeError, AttributeError):
                raise exc
            apply_profile = getattr(page, "apply_workspace_profile", None)
            if callable(apply_profile):
                apply_profile(self._active_profile_name)
            return page

    def _create_page(self, page_key: str) -> QWidget:
        if page_key == "kline":
            page = KlineAnalysisWindow(embedded=True)
            set_monitor = getattr(page, "set_shape_signal_monitor", None)
            if callable(set_monitor):
                set_monitor(self._shape_signal_monitor)
            return page
        if page_key == "account":
            page = AccountPositionsHomeWidget(self)
            set_workspace_managed = getattr(page, "set_workspace_managed", None)
            if callable(set_workspace_managed):
                set_workspace_managed(True)
            self._home_widget = page
            return page
        if page_key == "roll":
            page = self._create_roll_page()
            set_workspace_managed = getattr(page, "set_workspace_managed", None)
            if callable(set_workspace_managed):
                set_workspace_managed(True)
        elif page_key == "daily-report":
            page = DailyTradeReportWidget(self, profile_name=self._active_profile_name)
        elif page_key == "smart-order":
            page = SmartOrderQtWindow()
        else:
            raise KeyError(f"unknown page: {page_key}")
        page.setWindowFlags(Qt.WindowType.Widget)
        return page

    def show_page(self, page_key: str) -> None:
        normalized = page_key.strip().lower()
        if normalized == "kline-analysis":
            normalized = "kline"
        if normalized not in {"account", "kline", "roll", "daily-report", "smart-order"}:
            raise KeyError(f"unknown page: {page_key}")
        if self._shutdown_in_progress:
            return
        if self._page_switch_in_progress:
            self._pending_page_key = normalized
            return
        self._page_switch_in_progress = True
        newly_created = False
        try:
            page = self._pages.get(normalized)
            if page is None:
                page = self._create_page(normalized)
                self._pages[normalized] = page
                self._page_stack.addWidget(page)
                newly_created = True
            previous = self._pages.get(self._active_page_key)
            if previous is not None and previous is not page:
                set_active = getattr(previous, "set_page_active", None)
                if callable(set_active):
                    set_active(False)
            self._page_stack.setCurrentWidget(page)
            self._active_page_key = normalized
            self._workspace_header.set_active_page(normalized)
            set_active = getattr(page, "set_page_active", None)
            if callable(set_active):
                set_active(True)
            self._refresh_local_task_status()
            self._refresh_workspace_connection_status()
            if newly_created and self._active_profile_name:
                if self._is_chart_page(page):
                    QTimer.singleShot(
                        0,
                        lambda page=page, target=self._active_profile_name, serial=self._workspace_profile_serial:
                        self._apply_page_workspace_profile(page, target, serial),
                    )
                else:
                    self._apply_page_workspace_profile(page, self._active_profile_name, self._workspace_profile_serial)
        finally:
            self._page_switch_in_progress = False
            pending = self._pending_page_key
            self._pending_page_key = None
            if pending is not None and pending != normalized:
                QTimer.singleShot(0, lambda pending=pending: self.show_page(pending))

    def closeEvent(self, event) -> None:  # noqa: ANN001
        if self._shutdown_in_progress:
            event.ignore()
            return
        summary = self._local_task_summary()
        if any(summary.values()):
            message = "检测到本地任务仍在运行：" + " | ".join(
                f"{label} {count}"
                for label, count in (("RR", summary["rr"]), ("条件单", summary["line_conditions"]), ("套利", summary["arbitrage"]))
                if count
            )
            answer = QMessageBox.question(
                self,
                "确认关闭",
                f"{message}\n\n关闭将停止本机监控任务，交易所已挂出的订单不会被撤销。是否继续？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if answer != QMessageBox.StandardButton.Yes:
                event.ignore()
                return
        self._shutdown_in_progress = True
        event.ignore()
        self.setEnabled(False)
        if "[closing]" not in self.windowTitle():
            self.setWindowTitle(f"{self.windowTitle()} [closing]")
        self.repaint()
        QTimer.singleShot(0, self._begin_shutdown)

    def _local_task_summary(self) -> dict[str, int]:
        summary = {"rr": 0, "line_conditions": 0, "arbitrage": 0}
        for page in self._pages.values():
            getter = getattr(page, "local_task_summary", None)
            if not callable(getter):
                continue
            try:
                payload = getter()
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            for key in summary:
                summary[key] += max(0, int(payload.get(key, 0) or 0))
        return summary

    @Slot()
    def _refresh_local_task_status(self) -> None:
        bound_counts: list[LocalTaskCount] = []
        legacy_summary = {"rr": 0, "line_conditions": 0, "arbitrage": 0}
        for page in self._pages.values():
            getter = getattr(page, "local_task_counts", None)
            if callable(getter):
                try:
                    bound_counts.extend(getter())
                except Exception:
                    pass
                continue
            legacy_getter = getattr(page, "local_task_summary", None)
            if not callable(legacy_getter):
                continue
            try:
                payload = legacy_getter()
            except Exception:
                continue
            if isinstance(payload, dict):
                for key in legacy_summary:
                    legacy_summary[key] += max(0, int(payload.get(key, 0) or 0))
        text = format_local_task_counts(bound_counts)
        legacy_parts = [
            f"RR {legacy_summary['rr']}" if legacy_summary["rr"] else "",
            f"条件单 {legacy_summary['line_conditions']}" if legacy_summary["line_conditions"] else "",
            f"套利 {legacy_summary['arbitrage']}" if legacy_summary["arbitrage"] else "",
        ]
        legacy_text = " | ".join(part for part in legacy_parts if part)
        if legacy_text:
            text = f"{text}｜{legacy_text}" if text else legacy_text
        self._workspace_header.set_task_text(text)
        self._local_task_status.setText(text)

    @Slot()
    def _refresh_workspace_status(self) -> None:
        self._refresh_local_task_status()
        self._refresh_workspace_connection_status()

    def _refresh_workspace_connection_status(self) -> None:
        snapshots: list[dict[str, object]] = []
        for page in self._pages.values():
            getter = getattr(page, "connection_snapshot", None)
            if not callable(getter):
                continue
            try:
                snapshot = getter()
            except Exception:
                continue
            if isinstance(snapshot, dict):
                snapshots.append(snapshot)
        public_online = any(bool(item.get("public_online", False)) for item in snapshots)
        private_online = any(bool(item.get("private_online", False)) for item in snapshots)
        private_status = next(
            (str(item.get("private_status", "") or "").strip() for item in snapshots if item.get("private_status")),
            "",
        )
        market_text = "● 行情在线" if public_online else "○ 行情连接中"
        if not self._active_profile_name:
            account_text = "账户未配置"
        elif private_online:
            account_text = "私有WS在线"
        else:
            account_text = private_status or "账户待加载"
        healthy = public_online and (private_online or not self._active_profile_name)
        self._workspace_header.set_connection_text(f"{market_text} · {account_text}", healthy)

    def _begin_shutdown(self) -> None:
        started_at = datetime.now()
        self._shutdown_started_at = started_at
        print(f"[launcher] shutdown_begin | ts={started_at.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        try:
            append_log_line(f"[launcher] shutdown_begin | ts={started_at.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:
            pass
        self._request_child_windows_close()
        self._wait_for_child_windows_shutdown()

    def _request_child_windows_close(self) -> None:
        for window in list(self._child_windows):
            try:
                if window.isVisible():
                    window.close()
            except RuntimeError:
                continue

    def _wait_for_child_windows_shutdown(self) -> None:
        pending_windows: list[QWidget] = []
        for window in list(self._child_windows):
            try:
                if window.isVisible():
                    pending_windows.append(window)
            except RuntimeError:
                continue
        if pending_windows:
            QTimer.singleShot(150, self._wait_for_child_windows_shutdown)
            return
        if self._home_shutdown_started:
            return
        self._home_shutdown_started = True
        self._shutdown_pending_page_keys = set(self._pages)
        if not self._shutdown_pending_page_keys:
            self._finish_shutdown()
            return
        for page_key, page in self._pages.items():
            begin_shutdown = getattr(page, "begin_shutdown", None)
            if callable(begin_shutdown):
                try:
                    begin_shutdown(lambda key=page_key: self._on_workspace_page_shutdown_finished(key))
                except TypeError:
                    begin_shutdown()
                    self._on_workspace_page_shutdown_finished(page_key)
                continue
            self._on_workspace_page_shutdown_finished(page_key)

    def _on_workspace_page_shutdown_finished(self, page_key: str) -> None:
        self._shutdown_pending_page_keys.discard(page_key)
        if not self._home_shutdown_started or self._shutdown_pending_page_keys:
            return
        self._finish_shutdown()

    def _finish_shutdown(self) -> None:
        finished_at = datetime.now()
        started_at = self._shutdown_started_at or finished_at
        elapsed = (finished_at - started_at).total_seconds()
        print(
            f"[launcher] shutdown_end | ts={finished_at.strftime('%Y-%m-%d %H:%M:%S')} | elapsed={elapsed:.3f}s",
            flush=True,
        )
        try:
            append_log_line(
                f"[launcher] shutdown_end | ts={finished_at.strftime('%Y-%m-%d %H:%M:%S')} | elapsed={elapsed:.3f}s"
            )
        except Exception:
            pass
        self._shape_signal_monitor.stop()
        self._history_sync_manager.shutdown()
        self.deleteLater()
        app = QApplication.instance()
        if app is not None:
            app.quit()

    def _build_menu(self) -> None:
        self.menuBar().hide()

    @Slot(object)
    def _on_shape_signal_detected(self, event: object) -> None:
        if not isinstance(event, dict):
            return
        self._refresh_shape_message_badge()
        dialog = self._shape_message_dialog
        if dialog is not None and dialog.isVisible() and dialog.isActiveWindow():
            dialog.refresh()
        if not bool(event.get("popup_enabled", True)):
            return
        event_copy = dict(event)
        event_key = LauncherWindow._shape_popup_event_key(event_copy)
        pending_keys = {LauncherWindow._shape_popup_event_key(item) for item in self._shape_startup_events}
        displayed_events = getattr(self, "_shape_popup_display_events", [])
        displayed_keys = {LauncherWindow._shape_popup_event_key(item) for item in displayed_events}
        if event_key in pending_keys or event_key in displayed_keys:
            return
        current_box = getattr(self, "_shape_popup_box", None)
        if current_box is not None and current_box.isVisible() and displayed_events:
            event_ts = LauncherWindow._shape_popup_event_time(event_copy)
            displayed_times = {LauncherWindow._shape_popup_event_time(item) for item in displayed_events}
            if event_ts in displayed_times:
                displayed_events.append(event_copy)
                self._update_shape_popup_box()
                return
        # 将短时间内到达的实时信号和启动补算信号合并，避免连续弹出多个窗口。
        self._shape_startup_events.append(event_copy)
        if not self._shape_startup_popup_timer.isActive():
            self._shape_startup_popup_timer.start(3000)

    @staticmethod
    def _shape_popup_event_time(event: dict[str, object]) -> int:
        try:
            return int(event.get("candle_ts", 0) or 0)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _shape_popup_event_key(event: dict[str, object]) -> tuple[str, str, str, int, str, str]:
        return (
            str(event.get("symbol") or "").strip().upper(),
            str(event.get("period") or "").strip().upper(),
            str(event.get("pattern_id") or "").strip().lower(),
            LauncherWindow._shape_popup_event_time(event),
            str(event.get("direction") or "").strip().lower(),
            str(event.get("environment") or "demo").strip().lower(),
        )

    @staticmethod
    def _shape_popup_title(events: list[dict[str, object]]) -> str:
        startup_count = sum(str(event.get("source") or "") == "startup" for event in events)
        live_count = len(events) - startup_count
        if live_count and startup_count:
            return f"收到 {len(events)} 条形态信号（实时 {live_count}，启动补算 {startup_count}）："
        if live_count:
            return f"收到 {live_count} 条实时形态信号："
        return f"启动补算发现 {startup_count} 条形态信号："

    def _shape_popup_text(self, events: list[dict[str, object]]) -> str:
        lines = [self._shape_popup_title(events), ""]
        for event in events[:20]:
            lines.append(
                f"{event.get('symbol', '-')} {event.get('period', '-')} | "
                f"{event.get('pattern_name', '-')} {event.get('direction', '-')} | "
                f"{event.get('close', '-')}"
            )
        if len(events) > 20:
            lines.append(f"……另有 {len(events) - 20} 条，请打开历史形态信号查看。")
        return "\n".join(lines)

    def _update_shape_popup_box(self) -> None:
        box = self._shape_popup_box
        if box is not None and box.isVisible():
            box.setText(self._shape_popup_text(self._shape_popup_display_events))

    @Slot(str)
    def _on_shape_monitor_status(self, message: str) -> None:
        self._shape_monitor_status = str(message)
        self._refresh_shape_message_badge()
        self.statusBar().showMessage(str(message), 5000)

    def _refresh_shape_message_badge(self) -> None:
        unread = sum(not item.get("read_at") for item in load_events(limit=5000))
        self._workspace_header.set_shape_message_status(unread, self._shape_monitor_status)

    @Slot(object)
    def _mark_shape_messages_viewed(self, events: object) -> None:
        if not isinstance(events, list):
            return
        try:
            mark_events_read(events)
        except (OSError, ValueError) as exc:
            self.statusBar().showMessage(f"信号已读状态保存失败：{exc}", 5000)
        self._refresh_shape_message_badge()

    def _open_shape_message_center(self) -> None:
        from roll_terminal_qt.shape_signal_dialog import ShapeSignalHistoryDialog

        dialog = self._shape_message_dialog
        if dialog is None:
            page = self._pages.get("kline")
            if page is None:
                self.show_page("kline")
                page = self._pages.get("kline")
            dialog = ShapeSignalHistoryDialog(parent=page or self)
            dialog.events_viewed.connect(self._mark_shape_messages_viewed)
            self._shape_message_dialog = dialog
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()
        dialog.refresh()

    @staticmethod
    def _is_yesterday_shape_event(event: dict[str, object]) -> bool:
        try:
            candle_ts = int(event.get("candle_ts", 0) or 0)
        except (TypeError, ValueError):
            return False
        if candle_ts <= 0:
            return False
        shanghai = timezone(timedelta(hours=8))
        event_day = datetime.fromtimestamp(candle_ts / 1000, shanghai).date()
        yesterday = (datetime.now(shanghai) - timedelta(days=1)).date()
        return event_day == yesterday

    def _show_shape_startup_summary(self) -> None:
        events = self._shape_startup_events
        self._shape_startup_events = []
        if not events:
            return
        self._shape_popup_display_events = list(events)
        box = QMessageBox(self)
        box.setWindowTitle("形态信号汇总")
        box.setIcon(QMessageBox.Icon.Information)
        box.setText(self._shape_popup_text(events))
        box.setStandardButtons(QMessageBox.StandardButton.Ok)
        box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self._shape_popup_boxes.append(box)
        self._shape_popup_box = box

        def _on_shape_popup_destroyed(*_args: object, target: QMessageBox = box) -> None:
            if target in self._shape_popup_boxes:
                self._shape_popup_boxes.remove(target)
            if self._shape_popup_box is target:
                self._shape_popup_box = None
                self._shape_popup_display_events = []

        box.destroyed.connect(_on_shape_popup_destroyed)
        box.open()

    @Slot(str)
    def _handle_workspace_tool(self, tool_key: str) -> None:
        normalized = tool_key.strip().lower()
        if normalized == "shape-messages":
            self._open_shape_message_center()
            return
        if normalized.startswith("font-"):
            self._set_global_font_mode(normalized.removeprefix("font-"))
            return
        if normalized == "rr-monitor":
            self.show_page("kline")
            page = self._pages.get("kline")
            opener = getattr(page, "open_rr_monitor_dialog", None)
            if callable(opener):
                opener()
            self._refresh_local_task_status()
            return
        if normalized == "smart-order":
            self.show_page(normalized)
            return
        if normalized in {"option-strategy", "deribit-volatility", "option-roll"}:
            self.open_module_window(normalized)
            return
        if normalized == "ai-snapshot":
            self._start_ai_snapshot()
            return
        if normalized == "ai-quick-snapshot":
            self._start_ai_quick_snapshot()
            return
        if normalized == "ai-snapshot-info":
            self._show_ai_snapshot_contents()
            return
        if normalized == "sample-prediction":
            self.open_module_window(normalized)
            return
        if normalized == "paths":
            self._show_shared_data_dialog()
            return
        if normalized == "history-sync":
            self._show_history_sync_dialog()
            return
        if normalized == "logs":
            self._open_roll_terminal_logs_directory()
            return
        if normalized == "version":
            self._show_version_info()
            return
        raise KeyError(f"unknown workspace tool: {tool_key}")

    @Slot()
    def _show_ai_snapshot_contents(self) -> None:
        if self._ai_snapshot_contents_dialog is None:
            self._ai_snapshot_contents_dialog = AISnapshotContentsDialog(self)
        self._ai_snapshot_contents_dialog.show()
        self._ai_snapshot_contents_dialog.raise_()
        self._ai_snapshot_contents_dialog.activateWindow()

    def _start_ai_quick_snapshot(self) -> None:
        if (
            (self._ai_snapshot_worker is not None and self._ai_snapshot_worker.isRunning())
            or (self._ai_quick_snapshot_worker is not None and self._ai_quick_snapshot_worker.isRunning())
        ):
            QMessageBox.information(self, "AI 精简快照", "当前已有 AI 快照任务在后台生成，请稍候。")
            return
        profile_name = self._active_profile_name.strip()
        runtime = load_runtime(profile_name) if profile_name else None
        if runtime is None:
            QMessageBox.warning(self, "AI 精简快照", "当前没有可用的 API Profile。")
            return
        text, accepted = QInputDialog.getText(
            self,
            "AI 精简快照",
            "输入要分析的标的（逗号分隔，例如 BTC 或 BTC, ETH）。\n"
            "留空将默认使用 BTC。\n\n只输出选择的标的，不会附带其他币种。",
            text="BTC",
        )
        if not accepted:
            return
        symbols = parse_ai_quick_snapshot_symbols(text)
        if not symbols:
            QMessageBox.warning(self, "AI 精简快照", "未识别到有效标的，请输入 BTC、ETH 等标的；留空可使用 BTC。")
            return
        worker = AIQuickSnapshotWorker(runtime, profile_name=profile_name, symbols=symbols)
        self._ai_quick_snapshot_worker = worker
        progress_dialog = QProgressDialog("正在生成 AI 精简快照…", "", 0, 0, self)
        progress_dialog.setWindowTitle("AI 精简快照")
        progress_dialog.setCancelButton(None)
        progress_dialog.setMinimumDuration(0)
        progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        progress_dialog.setAutoClose(False)
        progress_dialog.setAutoReset(False)
        progress_dialog.show()
        self._ai_snapshot_progress_dialog = progress_dialog
        worker.progress.connect(progress_dialog.setLabelText)
        worker.progress.connect(lambda message: self.statusBar().showMessage(message))
        worker.succeeded.connect(self._on_ai_quick_snapshot_succeeded)
        worker.failed.connect(self._on_ai_quick_snapshot_failed)
        worker.finished.connect(lambda: self.statusBar().clearMessage())
        worker.finished.connect(self._close_ai_snapshot_progress)
        worker.finished.connect(worker.deleteLater)
        worker.start()

    @Slot(object)
    def _on_ai_quick_snapshot_succeeded(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        self._ai_quick_snapshot_worker = None
        target = str(payload.get("file_path", ""))
        symbols = ", ".join(str(item) for item in payload.get("symbols", []) or [])
        quality = payload.get("data_quality", {})
        status = str(quality.get("market", "")) if isinstance(quality, dict) else ""
        box = QMessageBox(self)
        box.setWindowTitle("AI 精简快照已生成")
        box.setText(f"已生成：{symbols}\n行情状态：{status or '未知'}\n\n{target}")
        open_button = box.addButton("打开所在目录", QMessageBox.ButtonRole.ActionRole)
        box.addButton("关闭", QMessageBox.ButtonRole.RejectRole)
        box.exec()
        if box.clickedButton() is open_button and target:
            self._open_local_path(Path(target).parent, title="打开精简快照目录")

    @Slot(str)
    def _on_ai_quick_snapshot_failed(self, message: str) -> None:
        self._ai_quick_snapshot_worker = None
        QMessageBox.critical(self, "AI 精简快照失败", message or "未知错误")

    def _start_ai_snapshot(self) -> None:
        if self._ai_snapshot_worker is not None and self._ai_snapshot_worker.isRunning():
            QMessageBox.information(self, "AI 快照", "当前已有快照任务在后台生成，请稍候。")
            return
        profile_name = self._active_profile_name.strip()
        runtime = load_runtime(profile_name) if profile_name else None
        if runtime is None:
            QMessageBox.warning(self, "AI 快照", "当前没有可用的 API Profile。")
            return
        existing = load_ai_watchlist(profile_name)
        text, accepted = QInputDialog.getText(
            self,
            "关注品种",
            "输入要关注的衍生品基础品种（逗号分隔，例如 BTC, ETH, SOL）。\n\n持仓品种会自动加入；现货不会导出。",
            text=", ".join(existing),
        )
        if not accepted:
            return
        watchlist = normalize_watchlist(part.strip() for part in text.replace("，", ",").split(","))
        try:
            save_ai_watchlist(profile_name, watchlist)
        except Exception as exc:
            QMessageBox.critical(self, "AI 快照", f"保存关注品种失败：{exc}")
            return
        worker = AISnapshotWorker(runtime, profile_name=profile_name, watchlist=watchlist)
        self._ai_snapshot_worker = worker
        progress_dialog = QProgressDialog("正在补充最新行情…", "", 0, 0, self)
        progress_dialog.setWindowTitle("AI 快照")
        progress_dialog.setCancelButton(None)
        progress_dialog.setMinimumDuration(0)
        progress_dialog.setWindowModality(Qt.WindowModality.WindowModal)
        progress_dialog.setAutoClose(False)
        progress_dialog.setAutoReset(False)
        progress_dialog.show()
        self._ai_snapshot_progress_dialog = progress_dialog
        worker.progress.connect(progress_dialog.setLabelText)
        worker.progress.connect(lambda message: self.statusBar().showMessage(message))
        worker.succeeded.connect(self._on_ai_snapshot_succeeded)
        worker.failed.connect(self._on_ai_snapshot_failed)
        worker.finished.connect(lambda: self.statusBar().clearMessage())
        worker.finished.connect(self._close_ai_snapshot_progress)
        worker.finished.connect(worker.deleteLater)
        self.statusBar().showMessage("正在生成 AI 快照…")
        worker.start()

    @Slot(object)
    def _on_ai_snapshot_succeeded(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        target = str(payload.get("file_path", ""))
        summary = payload.get("portfolio_summary", {})
        assets = len(payload.get("assets", {}) or {}) if isinstance(payload.get("assets"), dict) else 0
        positions = int(summary.get("position_count", 0) or 0) if isinstance(summary, dict) else 0
        quality = payload.get("data_quality", {})
        quality_status = str(quality.get("status", "")) if isinstance(quality, dict) else ""
        self._ai_snapshot_worker = None
        box = QMessageBox(self)
        box.setWindowTitle("AI 快照已生成")
        box.setText(f"已导出 {assets} 个品种、{positions} 条持仓。\n数据质量：{quality_status or '未知'}\n\n{target}")
        open_button = box.addButton("打开所在目录", QMessageBox.ButtonRole.ActionRole)
        box.addButton("关闭", QMessageBox.ButtonRole.RejectRole)
        box.exec()
        if box.clickedButton() is open_button and target:
            self._open_local_path(Path(target).parent, title="打开快照目录")

    @Slot(str)
    def _on_ai_snapshot_failed(self, message: str) -> None:
        self._ai_snapshot_worker = None
        QMessageBox.critical(self, "AI 快照失败", message or "未知错误")

    @Slot()
    def _close_ai_snapshot_progress(self) -> None:
        dialog = self._ai_snapshot_progress_dialog
        self._ai_snapshot_progress_dialog = None
        if dialog is not None:
            dialog.close()
            dialog.deleteLater()

    def _set_global_font_mode(self, mode: str) -> None:
        app = QApplication.instance()
        if app is None:
            return
        self._global_font_mode = apply_global_font_mode(app, mode)
        self._settings.setValue("ui/global-font-mode", self._global_font_mode)
        self._settings.sync()
        self._workspace_header.set_global_font_mode(self._global_font_mode)
        for page in self._pages.values():
            refresh_global_font = getattr(page, "refresh_global_font", None)
            if callable(refresh_global_font):
                refresh_global_font()

    @Slot()
    def _show_shared_data_dialog(self) -> None:
        if self._shared_data_dialog is None:
            self._shared_data_dialog = SharedDataDialog(self)
        self._shared_data_dialog.show()
        self._shared_data_dialog.raise_()
        self._shared_data_dialog.activateWindow()

    @Slot()
    def _show_history_sync_dialog(self) -> None:
        if self._history_sync_dialog is None:
            self._history_sync_dialog = HistorySyncDialog(
                parent=self,
                profile_provider=self.active_profile_name,
                runtime_provider=lambda: load_runtime(self.active_profile_name()),
                export_fills_callback=self._export_all_fills_from_history_center,
                export_positions_callback=self._export_all_positions_from_history_center,
            )
        self._history_sync_dialog.show()
        self._history_sync_dialog.raise_()
        self._history_sync_dialog.activateWindow()

    def _account_home_for_history_export(self) -> AccountPositionsHomeWidget | None:
        self.show_page("account")
        return self._home_widget

    @Slot()
    def _export_all_fills_from_history_center(self) -> None:
        page = self._account_home_for_history_export()
        if page is None:
            self.statusBar().showMessage("持仓页面尚未准备完成，暂时无法导出历史成交。", 8000)
            return
        page.sync_and_export_all_local_fill_history()

    @Slot()
    def _export_all_positions_from_history_center(self) -> None:
        page = self._account_home_for_history_export()
        if page is None:
            self.statusBar().showMessage("持仓页面尚未准备完成，暂时无法导出历史仓位。", 8000)
            return
        page.sync_and_export_position_history(all_local=True)

    @Slot()
    def _show_home_summary_hint(self) -> None:
        QMessageBox.information(
            self,
            "账户持仓工作台",
            "当前主页面已经切换为账户持仓工作台，数据目录和模块入口都已收进上方菜单。",
        )

    @Slot()
    def _show_version_info(self) -> None:
        QMessageBox.information(self, "版本信息", build_version_info_text())

    def _open_local_path(self, target_path, *, title: str) -> None:  # noqa: ANN001
        try:
            if getattr(target_path, "suffix", ""):
                target_path.parent.mkdir(parents=True, exist_ok=True)
                if not target_path.exists():
                    target_path.touch()
            else:
                target_path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            QMessageBox.critical(self, title, f"无法创建目标路径：{exc}")
            return
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(target_path))):
            QMessageBox.warning(self, title, f"系统未能打开：\n{target_path}")

    @Slot()
    def _open_roll_terminal_logs_directory(self) -> None:
        self._open_local_path(logs_dir_path() / "roll_terminal_qt", title="打开日志目录")

    @Slot()
    def _open_today_console_log(self) -> None:
        today_log = logs_dir_path() / "roll_terminal_qt" / f"console_{datetime.now().strftime('%Y-%m-%d')}.log"
        self._open_local_path(today_log, title="打开今日日志")

    @Slot(str)
    def open_module_window(self, module_key: str) -> None:
        print(f"[launcher] open_module_window begin | module={module_key}", flush=True)
        normalized = module_key.strip().lower()
        if normalized == "kline-analysis":
            self.show_page("kline")
            return
        if normalized == "roll":
            self.show_page("roll")
            return
        if normalized == "smart-order":
            self.show_page(normalized)
            return
        window = create_module_window(module_key, profile_name=self._active_profile_name)
        print(f"[launcher] open_module_window created | module={module_key} | type={type(window).__name__}", flush=True)
        self._child_windows.append(window)
        window.destroyed.connect(
            lambda *_args, target=window: self._child_windows.remove(target) if target in self._child_windows else None
        )
        window.show()
        print(f"[launcher] open_module_window shown | module={module_key}", flush=True)
        window.raise_()
        window.activateWindow()


def create_module_window(module_key: str, *, profile_name: str = "") -> QWidget:
    normalized = module_key.strip().lower()
    if normalized == "roll":
        try:
            window = RollTerminalWindow(profile_name=profile_name)
        except (TypeError, AttributeError) as exc:
            try:
                window = RollTerminalWindow()
            except (TypeError, AttributeError):
                raise exc
            apply_profile = getattr(window, "apply_workspace_profile", None)
            if callable(apply_profile):
                apply_profile(profile_name)
        apply_qt_window_icon(window)
        return window
    if normalized == "kline-analysis":
        window = KlineAnalysisWindow()
        apply_qt_window_icon(window)
        return window
    if normalized == "smart-order":
        window = SmartOrderQtWindow()
        apply_qt_window_icon(window)
        return window
    if normalized == "line-trading":
        window = LineTradingQtWindow()
        apply_qt_window_icon(window)
        return window
    if normalized == "auto-channel":
        window = AutoChannelWindow()
        apply_qt_window_icon(window)
        return window
    if normalized == "deribit-volatility":
        window = DeribitVolatilityQtWindow()
        apply_qt_window_icon(window)
        return window
    if normalized == "option-strategy":
        window = OptionStrategyQtWindow(profile_name=profile_name)
        apply_qt_window_icon(window)
        return window
    if normalized == "option-roll":
        window = OptionRollExecutionQtWindow(profile_name=profile_name)
        apply_qt_window_icon(window)
        return window
    if normalized == "sample-prediction":
        window = SamplePredictionWindow()
        apply_qt_window_icon(window)
        return window
    for spec in launcher_module_specs():
        if spec.key == normalized:
            window = ModuleOverviewWindow(module_key=spec.key, title=spec.title, subtitle=spec.subtitle)
            apply_qt_window_icon(window)
            return window
    raise KeyError(f"unknown module: {module_key}")


def create_root_window(module_key: str) -> QWidget:
    normalized = module_key.strip().lower()
    if normalized == "home":
        return LauncherWindow()
    return create_module_window(normalized)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run QQOKX Qt terminal shell")
    parser.add_argument(
        "--module",
        choices=module_choices(),
        default="home",
        help="Module surface to launch",
    )
    return parser


def run(argv: Iterable[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    app = QApplication.instance()
    if app is None:
        QCoreApplication.setAttribute(Qt.ApplicationAttribute.AA_UseSoftwareOpenGL)
        app = QApplication(sys.argv[:1])
    apply_qt_application_identity(app)
    app.setStyleSheet(APP_STYLE)
    window = create_root_window(args.module)
    apply_qt_window_icon(window)
    window.show()
    return app.exec()
