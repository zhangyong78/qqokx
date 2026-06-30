from __future__ import annotations

import argparse
import sys
from typing import Iterable

from PySide6.QtCore import Qt, Slot
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QPushButton,
    QScrollArea,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from okx_quant.app_paths import config_dir_path, data_root, logs_dir_path, state_dir_path
from roll_terminal_qt.auto_channel_window import AutoChannelWindow
from roll_terminal_qt.line_trading_window import LineTradingQtWindow
from roll_terminal_qt.module_overview import ModuleOverview, build_module_overview, launcher_module_specs
from roll_terminal_qt.smart_order_window import SmartOrderQtWindow
from roll_terminal_qt.style import APP_STYLE
from roll_terminal_qt.ui import RollTerminalWindow


def module_choices() -> tuple[str, ...]:
    return ("home",) + tuple(spec.key for spec in launcher_module_specs())


def _standalone_command(module_key: str) -> str:
    return f"python run_roll_terminal_qt.py --module {module_key}"


class ModuleOverviewWindow(QMainWindow):
    def __init__(self, *, module_key: str, title: str, subtitle: str) -> None:
        super().__init__()
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
        title = QLabel(title)
        title.setObjectName("SectionTitle")
        subtitle_label = QLabel(subtitle)
        subtitle_label.setObjectName("Subtle")
        subtitle_label.setWordWrap(True)
        header = QVBoxLayout()
        header.addWidget(title)
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
        summary.extend(f"• {line}" for line in overview.summary_lines[:3])
        summary.append(f"独立启动：{_standalone_command(self._module_key)}")
        self._summary_label.setText("\n".join(summary))

    @Slot()
    def _open_module(self) -> None:
        self._open_callback(self._module_key)


class LauncherWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self._child_windows: list[QWidget] = []
        self.setWindowTitle("Qt 专业终端主壳")
        self.resize(1280, 860)

        root = QWidget()
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(20, 20, 20, 20)
        root_layout.setSpacing(16)

        hero = QFrame()
        hero.setObjectName("Panel")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(24, 24, 24, 24)
        hero_layout.setSpacing(8)
        title = QLabel("Qt 专业终端主壳")
        title.setObjectName("SectionTitle")
        intro = QLabel(
            "一个首页管理多个独立模块。当前先把入口、共享状态和专业套利终端接稳，"
            "再把无限下单、划线交易台、自动通道逐步迁到 Qt。"
        )
        intro.setObjectName("Subtle")
        intro.setWordWrap(True)
        hero_layout.addWidget(title)
        hero_layout.addWidget(intro)
        root_layout.addWidget(hero)

        shared_panel = QFrame()
        shared_panel.setObjectName("Guide")
        shared_layout = QGridLayout(shared_panel)
        shared_layout.setContentsMargins(18, 18, 18, 18)
        shared_layout.setHorizontalSpacing(18)
        shared_layout.setVerticalSpacing(8)
        shared_title = QLabel("共享配置与数据")
        shared_title.setObjectName("GuideTitle")
        shared_layout.addWidget(shared_title, 0, 0, 1, 2)
        for row, (label, value) in enumerate(
            (
                ("数据根目录", str(data_root())),
                ("配置目录", str(config_dir_path())),
                ("状态目录", str(state_dir_path())),
                ("日志目录", str(logs_dir_path())),
            ),
            start=1,
        ):
            key_label = QLabel(label)
            key_label.setObjectName("GuideText")
            value_label = QLabel(value)
            value_label.setObjectName("GuideText")
            value_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            shared_layout.addWidget(key_label, row, 0)
            shared_layout.addWidget(value_label, row, 1)
        root_layout.addWidget(shared_panel)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        content = QWidget()
        grid = QGridLayout(content)
        grid.setContentsMargins(4, 4, 4, 4)
        grid.setHorizontalSpacing(14)
        grid.setVerticalSpacing(14)
        for index, spec in enumerate(launcher_module_specs()):
            card = ModuleCard(
                module_key=spec.key,
                title=spec.title,
                subtitle=spec.subtitle,
                status=spec.status,
                open_callback=self.open_module_window,
            )
            grid.addWidget(card, index // 2, index % 2)
        scroll.setWidget(content)
        root_layout.addWidget(scroll, 1)
        self.setCentralWidget(root)

    @Slot(str)
    def open_module_window(self, module_key: str) -> None:
        window = create_module_window(module_key)
        self._child_windows.append(window)
        window.destroyed.connect(lambda *_args, target=window: self._child_windows.remove(target) if target in self._child_windows else None)
        window.show()
        window.raise_()
        window.activateWindow()


def create_module_window(module_key: str) -> QWidget:
    normalized = module_key.strip().lower()
    if normalized == "roll":
        return RollTerminalWindow()
    if normalized == "smart-order":
        return SmartOrderQtWindow()
    if normalized == "line-trading":
        return LineTradingQtWindow()
    if normalized == "auto-channel":
        return AutoChannelWindow()
    for spec in launcher_module_specs():
        if spec.key == normalized:
            return ModuleOverviewWindow(module_key=spec.key, title=spec.title, subtitle=spec.subtitle)
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
        app = QApplication(sys.argv[:1])
    app.setStyleSheet(APP_STYLE)
    window = create_root_window(args.module)
    window.show()
    return app.exec()
