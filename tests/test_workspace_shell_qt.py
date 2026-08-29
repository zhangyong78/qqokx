from __future__ import annotations

from PySide6.QtWidgets import QToolButton

from tests.qt_test_case import QtWidgetTestCase
import roll_terminal_qt.workspace_shell as workspace_shell

from roll_terminal_qt.workspace_shell import (
    LocalTaskCount,
    WorkspaceHeader,
    format_local_task_counts,
    merge_local_task_counts,
)


class WorkspaceShellQtTests(QtWidgetTestCase):
    def test_merge_local_task_counts_groups_by_profile(self) -> None:
        merged = merge_local_task_counts(
            [
                LocalTaskCount("moni", rr=2),
                LocalTaskCount("api2", line_conditions=1),
                LocalTaskCount("moni", arbitrage=1),
                LocalTaskCount("", rr=9),
            ]
        )

        self.assertEqual(
            merged,
            (
                LocalTaskCount("api2", line_conditions=1),
                LocalTaskCount("moni", rr=2, arbitrage=1),
            ),
        )
        self.assertEqual(format_local_task_counts(merged), "api2：条件单 1｜moni：RR 2 / 套利 1")

    def test_workspace_header_emits_page_profile_and_tool_requests(self) -> None:
        header = WorkspaceHeader()
        pages: list[str] = []
        profiles: list[str] = []
        tools: list[str] = []
        header.page_requested.connect(pages.append)
        header.profile_requested.connect(profiles.append)
        header.tool_requested.connect(tools.append)
        header.set_profiles(["moni", "api2"], "moni", "demo")

        header.action("page:account").trigger()
        header.profile_combo.setCurrentText("api2")
        header.action("tool:smart-order").trigger()

        self.assertEqual(pages, ["account"])
        self.assertEqual(profiles, ["api2"])
        self.assertEqual(tools, ["smart-order"])
        self.assertEqual(header.environment_label.text(), "模拟")

    def test_workspace_header_exposes_every_unique_route(self) -> None:
        header = WorkspaceHeader()

        self.assertEqual(
            set(header.route_keys()),
            {
                "page:kline",
                "page:account",
                "page:roll",
                "tool:smart-order",
                "option:option-strategy",
                "option:deribit-volatility",
                "settings:paths",
                "settings:logs",
                "settings:version",
            },
        )

    def test_workspace_header_marks_trading_tools_active_for_smart_order(self) -> None:
        header = WorkspaceHeader()
        trading_tools_button = next(
            button for button in header.findChildren(QToolButton) if button.text() == "交易工具"
        )
        self.assertEqual(trading_tools_button.objectName(), "WorkspacePageButton")

        header.set_active_page("roll")
        self.assertTrue(header._page_buttons["roll"].isChecked())

        header.set_active_page("smart-order")

        self.assertTrue(trading_tools_button.isChecked())
        self.assertFalse(header._page_buttons["roll"].isChecked())

    def test_preferred_profile_uses_159_before_saved_selection(self) -> None:
        self.assertEqual(
            workspace_shell.preferred_profile_name(["api2", "159", "api1"], selected="api2"),
            "159",
        )
        self.assertEqual(
            workspace_shell.preferred_profile_name(["api2", "159"], current="api2", selected="159"),
            "api2",
        )

    def test_workspace_header_restores_profile_without_emitting_request(self) -> None:
        header = WorkspaceHeader()
        requested: list[str] = []
        header.profile_requested.connect(requested.append)
        header.set_profiles(["moni", "api2"], "api2", "live")

        header.restore_profile("moni")

        self.assertEqual(header.profile_combo.currentText(), "moni")
        self.assertEqual(requested, [])
        self.assertEqual(header.environment_label.text(), "实盘")


if __name__ == "__main__":
    import unittest

    unittest.main()
