from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from roll_terminal_qt.launcher import module_choices
from roll_terminal_qt.module_overview import (
    build_auto_channel_module_overview,
    build_line_trading_module_overview,
    build_module_overview,
    build_roll_module_overview,
    build_smart_order_module_overview,
)


class RollTerminalLauncherTests(unittest.TestCase):
    def test_module_choices_include_home_and_all_modules(self) -> None:
        self.assertEqual(
            module_choices(),
            ("home", "roll", "smart-order", "line-trading", "auto-channel"),
        )

    def test_roll_module_overview_uses_shared_data_root_summary(self) -> None:
        with (
            patch("roll_terminal_qt.module_overview.data_root", return_value=Path("D:/data")),
            patch("roll_terminal_qt.module_overview.logs_dir_path", return_value=Path("D:/data/logs")),
            patch("roll_terminal_qt.module_overview.state_dir_path", return_value=Path("D:/data/state")),
            patch(
                "roll_terminal_qt.module_overview.load_credentials_profiles_snapshot",
                return_value={"selected_profile": "api2", "profiles": {"api1": {}, "api2": {}}},
            ),
        ):
            overview = build_roll_module_overview()

        self.assertEqual(overview.status, "已接入")
        self.assertIn("共享数据根目录：D:\\data", overview.summary_lines[0])
        self.assertIn("可用 API Profile：2 个 | 当前：api2", overview.summary_lines[1])
        self.assertEqual(len(overview.data_paths), 2)

    def test_smart_order_module_overview_summarizes_tasks_and_favorites(self) -> None:
        with (
            patch(
                "roll_terminal_qt.module_overview.load_smart_order_tasks_snapshot",
                return_value={"tasks": [{}, {}, {}], "locked_inst_id": "BTC-USDT"},
            ),
            patch(
                "roll_terminal_qt.module_overview.load_smart_order_favorites_snapshot",
                return_value={"favorites": [{"inst_id": "BTC-USDT", "inst_type": "SPOT"}]},
            ),
            patch("roll_terminal_qt.module_overview.smart_order_tasks_file_path", return_value=Path("D:/state/tasks.json")),
            patch(
                "roll_terminal_qt.module_overview.smart_order_favorites_file_path",
                return_value=Path("D:/state/favorites.json"),
            ),
        ):
            overview = build_smart_order_module_overview()

        self.assertEqual(overview.summary_lines[0], "任务数：3")
        self.assertEqual(overview.summary_lines[1], "收藏标的：1")
        self.assertEqual(overview.summary_lines[2], "当前锁定标的：BTC-USDT")
        self.assertEqual(len(overview.data_paths), 2)

    def test_line_trading_module_overview_counts_lines_and_rr_blocks(self) -> None:
        with patch(
            "roll_terminal_qt.module_overview.load_line_trading_desk_annotations_entries",
            return_value={
                "api|BTC-USDT|1H": {"lines": [1, 2], "rr": [1]},
                "api|ETH-USDT|15m": {"lines": [1], "rr": [1, 2, 3]},
            },
        ):
            overview = build_line_trading_module_overview()

        self.assertEqual(overview.summary_lines, ("已保存会话：2", "画线条目：3", "盈亏比区域：4"))

    def test_auto_channel_module_overview_reads_overlay_counts(self) -> None:
        fake_snapshot = type(
            "_Snapshot",
            (),
            {
                "candles": [1] * 64,
                "band_overlays": [1, 2],
                "box_overlays": [1],
                "note": "自动通道已识别 1 条通道",
            },
        )()
        with patch("roll_terminal_qt.module_overview.build_auto_channel_preview_snapshot", return_value=fake_snapshot):
            overview = build_auto_channel_module_overview()

        self.assertEqual(overview.summary_lines[0], "样例 K 线：64")
        self.assertEqual(overview.summary_lines[1], "通道覆盖层：2 | 箱体覆盖层：1")
        self.assertEqual(overview.summary_lines[2], "分析摘要：自动通道已识别 1 条通道")

    def test_build_module_overview_rejects_unknown_key(self) -> None:
        with self.assertRaises(KeyError):
            build_module_overview("unknown")


if __name__ == "__main__":
    unittest.main()
