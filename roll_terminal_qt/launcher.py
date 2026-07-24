from __future__ import annotations

import argparse
import sys
from datetime import datetime
from typing import Iterable

from PySide6.QtCore import QCoreApplication, QTimer, Qt, QUrl, Slot
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QStackedWidget,
)

from okx_quant.app_meta import APP_VERSION, build_version_info_text
from okx_quant.app_paths import config_dir_path, data_root, logs_dir_path, state_dir_path
from okx_quant.log_utils import append_log_line
from roll_terminal_qt.account_positions_home import AccountPositionsHomeWidget
from roll_terminal_qt.app_icon import apply_qt_application_identity, apply_qt_window_icon
from roll_terminal_qt.auto_channel_window import AutoChannelWindow
from roll_terminal_qt.deribit_volatility_window import DeribitVolatilityQtWindow
from roll_terminal_qt.line_trading_window import LineTradingQtWindow
from roll_terminal_qt.module_overview import ModuleOverview, build_module_overview, launcher_module_specs
from roll_terminal_qt.option_strategy_window import OptionStrategyQtWindow
from roll_terminal_qt.kline_analysis_window import KlineAnalysisWindow
from roll_terminal_qt.perf_metrics import measure_ui_step
from roll_terminal_qt.profile_access import ensure_profile_unlocked, load_profile_snapshots
from roll_terminal_qt.runtime import load_runtime
from roll_terminal_qt.smart_order_window import SmartOrderQtWindow
from roll_terminal_qt.style import APP_STYLE
from roll_terminal_qt.ui import RollTerminalWindow
from roll_terminal_qt.workspace_shell import (
    LocalTaskCount,
    WorkspaceHeader,
    format_local_task_counts,
    preferred_profile_name,
)


def module_choices() -> tuple[str, ...]:
    return ("home",) + tuple(spec.key for spec in launcher_module_specs())


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
        self._child_windows: list[QWidget] = []
        self._shared_data_dialog: SharedDataDialog | None = None
        self._shutdown_in_progress = False
        self._home_shutdown_started = False
        self._shutdown_pending_page_keys: set[str] = set()
        self._shutdown_started_at: datetime | None = None
        self._home_widget: AccountPositionsHomeWidget | None = None
        self._pages: dict[str, QWidget] = {}
        self._active_page_key = ""
        self._active_profile_name = ""
        self._profile_snapshots: dict[str, dict[str, str]] = {}
        self._unlocked_profiles: set[str] = set()
        workspace_root = QWidget(self)
        workspace_layout = QVBoxLayout(workspace_root)
        workspace_layout.setContentsMargins(0, 0, 0, 0)
        workspace_layout.setSpacing(0)
        self._workspace_header = WorkspaceHeader(workspace_root)
        self._workspace_header.page_requested.connect(self.show_page)
        self._workspace_header.tool_requested.connect(self._handle_workspace_tool)
        self._workspace_header.profile_requested.connect(self._request_workspace_profile)
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
        self.setWindowTitle("量化交易控制台")
        self.resize(1680, 980)
        self.setCentralWidget(workspace_root)
        self._build_menu()
        self._initialize_workspace_profiles()
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
        for page in self._pages.values():
            apply_profile = getattr(page, "apply_workspace_profile", None)
            if callable(apply_profile):
                apply_profile(target)

    def _create_page(self, page_key: str) -> QWidget:
        if page_key == "kline":
            return KlineAnalysisWindow(embedded=True)
        if page_key == "account":
            page = AccountPositionsHomeWidget(self)
            set_workspace_managed = getattr(page, "set_workspace_managed", None)
            if callable(set_workspace_managed):
                set_workspace_managed(True)
            self._home_widget = page
            return page
        if page_key == "roll":
            page = RollTerminalWindow()
            set_workspace_managed = getattr(page, "set_workspace_managed", None)
            if callable(set_workspace_managed):
                set_workspace_managed(True)
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
        if normalized not in {"account", "kline", "roll", "smart-order"}:
            raise KeyError(f"unknown page: {page_key}")
        page = self._pages.get(normalized)
        if page is None:
            page = self._create_page(normalized)
            self._pages[normalized] = page
            self._page_stack.addWidget(page)
            apply_profile = getattr(page, "apply_workspace_profile", None)
            if self._active_profile_name and callable(apply_profile):
                apply_profile(self._active_profile_name)
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
        self.deleteLater()
        app = QApplication.instance()
        if app is not None:
            app.quit()

    def _build_menu(self) -> None:
        self.menuBar().hide()

    @Slot(str)
    def _handle_workspace_tool(self, tool_key: str) -> None:
        normalized = tool_key.strip().lower()
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
        if normalized in {"option-strategy", "deribit-volatility"}:
            self.open_module_window(normalized)
            return
        if normalized == "paths":
            self._show_shared_data_dialog()
            return
        if normalized == "logs":
            self._open_roll_terminal_logs_directory()
            return
        if normalized == "version":
            self._show_version_info()
            return
        raise KeyError(f"unknown workspace tool: {tool_key}")

    @Slot()
    def _show_shared_data_dialog(self) -> None:
        if self._shared_data_dialog is None:
            self._shared_data_dialog = SharedDataDialog(self)
        self._shared_data_dialog.show()
        self._shared_data_dialog.raise_()
        self._shared_data_dialog.activateWindow()

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
        window = create_module_window(module_key)
        print(f"[launcher] open_module_window created | module={module_key} | type={type(window).__name__}", flush=True)
        self._child_windows.append(window)
        window.destroyed.connect(
            lambda *_args, target=window: self._child_windows.remove(target) if target in self._child_windows else None
        )
        window.show()
        print(f"[launcher] open_module_window shown | module={module_key}", flush=True)
        window.raise_()
        window.activateWindow()


def create_module_window(module_key: str) -> QWidget:
    normalized = module_key.strip().lower()
    if normalized == "roll":
        window = RollTerminalWindow()
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
        window = OptionStrategyQtWindow()
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
