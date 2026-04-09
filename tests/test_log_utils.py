from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from okx_quant.log_utils import append_log_line, daily_log_file_path, ensure_log_timestamp


class LogUtilsTest(unittest.TestCase):
    def test_adds_datetime_when_missing(self) -> None:
        self.assertEqual(
            ensure_log_timestamp(
                "[邮件 信号监控] 邮件已发送 | [QQOKX] 信号监控",
                timestamp="04-09 10:35:12",
            ),
            "[04-09 10:35:12] [邮件 信号监控] 邮件已发送 | [QQOKX] 信号监控",
        )

    def test_keeps_existing_datetime(self) -> None:
        self.assertEqual(
            ensure_log_timestamp(
                "[2026-04-09 12:34:56] [持仓保护] 已触发",
                timestamp="04-09 09:08:07",
            ),
            "[2026-04-09 12:34:56] [持仓保护] 已触发",
        )

    def test_keeps_existing_short_datetime(self) -> None:
        self.assertEqual(
            ensure_log_timestamp(
                "[04-09 12:34:56] [持仓保护] 已触发",
                timestamp="04-09 09:08:07",
            ),
            "[04-09 12:34:56] [持仓保护] 已触发",
        )

    def test_trims_blank_message(self) -> None:
        self.assertEqual(ensure_log_timestamp("   "), "")

    def test_daily_log_file_path_uses_date(self) -> None:
        target = datetime(2026, 4, 9, 10, 35, 12)
        path = daily_log_file_path(for_time=target, base_dir="D:/qqokx")
        self.assertEqual(path, Path("D:/qqokx/logs/2026-04-09.log"))

    def test_append_log_line_writes_daily_log_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            now = datetime(2026, 4, 9, 10, 35, 12)
            line = append_log_line(
                "[邮件 信号监控] 邮件已发送 | [QQOKX] 信号监控",
                now=now,
                base_dir=temp_dir,
            )
            path = Path(temp_dir) / "logs" / "2026-04-09.log"
            self.assertTrue(path.exists())
            self.assertTrue(line.startswith("[04-09 10:35:12] "))
            self.assertEqual(path.read_text(encoding="utf-8").splitlines(), [line])

    def test_append_log_line_preserves_existing_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            line = append_log_line(
                "[2026-04-09 12:34:56] [持仓保护] 已触发",
                now=datetime(2026, 4, 9, 13, 0, 0),
                base_dir=temp_dir,
            )
            self.assertEqual(line, "[2026-04-09 12:34:56] [持仓保护] 已触发")
            path = Path(temp_dir) / "logs" / "2026-04-09.log"
            self.assertEqual(path.read_text(encoding="utf-8").strip(), line)


if __name__ == "__main__":
    unittest.main()
