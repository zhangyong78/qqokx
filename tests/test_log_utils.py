from __future__ import annotations

import unittest

from okx_quant.log_utils import ensure_log_timestamp


class LogUtilsTest(unittest.TestCase):
    def test_adds_timestamp_when_missing(self) -> None:
        self.assertEqual(
            ensure_log_timestamp("[邮件 信号监控] 邮件已发送", timestamp="12:34:56"),
            "[12:34:56] [邮件 信号监控] 邮件已发送",
        )

    def test_keeps_existing_timestamp(self) -> None:
        self.assertEqual(
            ensure_log_timestamp("[12:34:56] [持仓保护] 已触发", timestamp="09:08:07"),
            "[12:34:56] [持仓保护] 已触发",
        )

    def test_trims_blank_message(self) -> None:
        self.assertEqual(ensure_log_timestamp("   "), "")


if __name__ == "__main__":
    unittest.main()
