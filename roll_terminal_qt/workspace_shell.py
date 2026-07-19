from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

from PySide6.QtCore import QSignalBlocker, Signal
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QComboBox, QFrame, QHBoxLayout, QLabel, QMenu, QSizePolicy, QToolButton


WorkspacePageKey = Literal["kline", "account", "roll", "smart-order"]


@dataclass(frozen=True)
class LocalTaskCount:
    profile_name: str
    rr: int = 0
    line_conditions: int = 0
    arbitrage: int = 0


def merge_local_task_counts(items: Iterable[LocalTaskCount]) -> tuple[LocalTaskCount, ...]:
    merged: dict[str, LocalTaskCount] = {}
    for item in items:
        profile_name = str(item.profile_name or "").strip()
        if not profile_name:
            continue
        current = merged.get(profile_name, LocalTaskCount(profile_name))
        merged[profile_name] = LocalTaskCount(
            profile_name=profile_name,
            rr=current.rr + max(0, int(item.rr)),
            line_conditions=current.line_conditions + max(0, int(item.line_conditions)),
            arbitrage=current.arbitrage + max(0, int(item.arbitrage)),
        )
    return tuple(merged[name] for name in sorted(merged, key=str.casefold))


def format_local_task_counts(items: Iterable[LocalTaskCount]) -> str:
    profiles: list[str] = []
    for item in merge_local_task_counts(items):
        parts = [
            f"RR {item.rr}" if item.rr else "",
            f"条件单 {item.line_conditions}" if item.line_conditions else "",
            f"套利 {item.arbitrage}" if item.arbitrage else "",
        ]
        summary = " / ".join(part for part in parts if part)
        if summary:
            profiles.append(f"{item.profile_name}：{summary}")
    return "｜".join(profiles)


def preferred_profile_name(
    names: Sequence[str],
    *,
    current: str = "",
    last: str = "",
    selected: str = "",
) -> str:
    normalized = [str(name).strip() for name in names if str(name).strip()]
    for candidate in (current, last):
        target = str(candidate or "").strip()
        if target in normalized:
            return target
    if "moni" in normalized:
        return "moni"
    selected_target = str(selected or "").strip()
    if selected_target in normalized:
        return selected_target
    return normalized[0] if normalized else ""


class WorkspaceHeader(QFrame):
    page_requested = Signal(str)
    profile_requested = Signal(str)
    tool_requested = Signal(str)

    _ROUTES = (
        ("page:kline", "K线"),
        ("page:account", "持仓"),
        ("page:roll", "专业套利"),
        ("tool:smart-order", "无限下单"),
        ("option:option-strategy", "期权策略计算器"),
        ("option:deribit-volatility", "Deribit 波动率"),
        ("settings:paths", "数据目录与路径"),
        ("settings:logs", "日志"),
        ("settings:version", "版本信息"),
    )

    def __init__(self, parent=None) -> None:  # noqa: ANN001
        super().__init__(parent)
        self.setObjectName("WorkspaceHeader")
        self._actions: dict[str, QAction] = {}
        self._page_buttons: dict[str, QToolButton] = {}

        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 5, 10, 5)
        layout.setSpacing(5)

        brand = QLabel("量化交易台", self)
        brand.setObjectName("WorkspaceBrand")
        layout.addWidget(brand)

        for route_key, text in self._ROUTES[:3]:
            page_key = route_key.split(":", 1)[1]
            action = self._register_action(route_key, text)
            button = QToolButton(self)
            button.setDefaultAction(action)
            button.setCheckable(True)
            button.setAutoExclusive(True)
            button.setObjectName("WorkspacePageButton")
            self._page_buttons[page_key] = button
            layout.addWidget(button)

        trading_tools_button = self._menu_button("交易工具", self._ROUTES[3:4])
        trading_tools_button.setCheckable(True)
        trading_tools_button.setAutoExclusive(True)
        trading_tools_button.setObjectName("WorkspacePageButton")
        self._page_buttons["smart-order"] = trading_tools_button
        layout.addWidget(trading_tools_button)
        layout.addWidget(self._menu_button("期权工具", self._ROUTES[4:6]))
        layout.addStretch(1)

        self.connection_label = QLabel("行情连接中", self)
        self.connection_label.setObjectName("WorkspaceConnection")
        layout.addWidget(self.connection_label)

        self.environment_label = QLabel("-", self)
        self.environment_label.setObjectName("WorkspaceEnvironment")
        layout.addWidget(self.environment_label)

        self.profile_combo = QComboBox(self)
        self.profile_combo.setMinimumWidth(112)
        self.profile_combo.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.profile_combo.currentTextChanged.connect(self._emit_profile_request)
        layout.addWidget(self.profile_combo)

        self.task_button = QToolButton(self)
        self.task_button.setText("无本地任务")
        self.task_button.setToolTip("按 API 显示 RR、条件单和套利任务")
        layout.addWidget(self.task_button)

        layout.addWidget(self._menu_button("⚙", self._ROUTES[6:]))
        self.setStyleSheet(
            """
            QFrame#WorkspaceHeader { background: #10273a; border: 0; }
            QLabel#WorkspaceBrand { color: #f4f8fb; font-weight: 700; padding-right: 10px; }
            QLabel#WorkspaceConnection { color: #9edfcf; }
            QLabel#WorkspaceEnvironment { color: #d7e4ec; }
            QToolButton { color: #d7e4ec; background: transparent; border: 0; padding: 7px 11px; }
            QToolButton:hover { background: #1a3a51; }
            QToolButton#WorkspacePageButton:checked { background: #1c4059; border-bottom: 2px solid #2cc5b2; }
            QComboBox { min-height: 27px; }
            """
        )

    def _register_action(self, route_key: str, text: str) -> QAction:
        action = QAction(text, self)
        action.setData(route_key)
        action.triggered.connect(lambda _checked=False, key=route_key: self._emit_route(key))
        self._actions[route_key] = action
        return action

    def _menu_button(self, text: str, routes: Sequence[tuple[str, str]]) -> QToolButton:
        menu = QMenu(self)
        for route_key, label in routes:
            menu.addAction(self._register_action(route_key, label))
        button = QToolButton(self)
        button.setText(text)
        button.setMenu(menu)
        button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        return button

    def _emit_route(self, route_key: str) -> None:
        route_type, value = route_key.split(":", 1)
        if route_type == "page":
            self.page_requested.emit(value)
        else:
            self.tool_requested.emit(value)

    def _emit_profile_request(self, profile_name: str) -> None:
        target = profile_name.strip()
        if target and target != "未配置":
            self.profile_requested.emit(target)

    def action(self, route_key: str) -> QAction:
        return self._actions[route_key]

    def route_keys(self) -> tuple[str, ...]:
        return tuple(self._actions)

    def set_active_page(self, page_key: str) -> None:
        button = self._page_buttons.get(page_key)
        if button is not None:
            button.setChecked(True)

    def set_profiles(self, names: Sequence[str], selected: str, environment: str) -> None:
        normalized = [str(name).strip() for name in names if str(name).strip()]
        with QSignalBlocker(self.profile_combo):
            self.profile_combo.clear()
            self.profile_combo.addItems(normalized or ["未配置"])
            index = self.profile_combo.findText(selected)
            self.profile_combo.setCurrentIndex(index if index >= 0 else 0)
        self.environment_label.setText("模拟" if environment == "demo" else ("实盘" if environment else "-"))

    def restore_profile(self, profile_name: str) -> None:
        with QSignalBlocker(self.profile_combo):
            index = self.profile_combo.findText(profile_name)
            if index >= 0:
                self.profile_combo.setCurrentIndex(index)

    def set_connection_text(self, text: str, healthy: bool) -> None:
        self.connection_label.setText(text)
        self.connection_label.setStyleSheet("color: #9edfcf;" if healthy else "color: #f0b36a;")

    def set_task_text(self, text: str) -> None:
        self.task_button.setText(text or "无本地任务")
