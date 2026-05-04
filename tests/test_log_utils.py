from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from okx_quant.log_utils import (
    append_line_desk_log_line,
    append_log_line,
    append_preformatted_log_line,
    daily_log_file_path,
    ensure_log_timestamp,
    line_desk_daily_log_path,
    read_daily_log_tail,
    strategy_session_log_file_path,
)


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

    def test_line_desk_daily_log_path(self) -> None:
        target = datetime(2026, 5, 3, 21, 0, 0)
        path = line_desk_daily_log_path(for_time=target, base_dir="D:/qqokx")
        self.assertEqual(path, Path("D:/qqokx/logs/line_desk/2026-05-03.log"))

    def test_append_line_desk_log_line_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            now = datetime(2026, 5, 3, 21, 0, 20)
            line = "[05-03 21:00:20] BTC-USDT-SWAP | 射线触发 | test"
            append_line_desk_log_line(line, now=now, base_dir=temp_dir)
            path = Path(temp_dir) / "logs" / "line_desk" / "2026-05-03.log"
            self.assertTrue(path.exists())
            self.assertEqual(path.read_text(encoding="utf-8").strip(), line)

    def test_read_daily_log_tail_returns_last_lines(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            now = datetime(2026, 4, 9, 10, 35, 12)
            for i in range(5):
                append_log_line(f"line-{i}", now=now, base_dir=temp_dir)
            tail = read_daily_log_tail(3, for_time=now, base_dir=temp_dir)
            self.assertEqual(len(tail), 3)
            self.assertTrue(tail[-1].endswith("line-4"))

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

    def test_strategy_session_log_file_path_uses_dedicated_strategy_folder(self) -> None:
        target = strategy_session_log_file_path(
            started_at=datetime(2026, 4, 17, 17, 9, 51, 123456),
            session_id="S02",
            strategy_name="EMA 动态委托-空头",
            symbol="ETH-USDT-SWAP",
            api_name="QQzhangyong",
            base_dir="D:/qqokx",
        )
        self.assertEqual(
            target,
            Path(
                "D:/qqokx/logs/strategy_sessions/2026-04-17/"
                "20260417_170951_123456__QQzhangyong__S02__EMA__ETH-USDT-SWAP.log"
            ),
        )

    def test_append_preformatted_log_line_writes_given_line_without_reformatting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "logs" / "strategy_sessions" / "2026-04-17" / "session.log"
            line = append_preformatted_log_line(
                "[04-17 17:09:51] [QQzhangyong] [S02 EMA 动态委托-空头 ETH-USDT-SWAP] 已请求停止。",
                path=path,
            )
            self.assertTrue(path.exists())
            self.assertEqual(
                path.read_text(encoding="utf-8").splitlines(),
                [line],
            )


if __name__ == "__main__":
    unittest.main()
