from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.strategy_status_email import (
    STATUS_EMAIL_TIMES,
    claim_status_email_slot,
    latest_due_status_email_slot,
    load_sent_status_email_slots,
    status_email_slot_key,
    strategy_status_email_state_file_path,
)


class StrategyStatusEmailScheduleTest(TestCase):
    def test_fixed_times_are_0800_1200_1600_2000(self) -> None:
        self.assertEqual(
            tuple(item.strftime("%H:%M") for item in STATUS_EMAIL_TIMES),
            ("08:00", "12:00", "16:00", "20:00"),
        )

    def test_first_check_only_establishes_baseline(self) -> None:
        self.assertIsNone(
            latest_due_status_email_slot(None, datetime(2026, 7, 18, 12, 30))
        )

    def test_crossing_one_slot_returns_that_slot(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 18, 7, 59, 50),
                datetime(2026, 7, 18, 8, 0, 10),
            ),
            datetime(2026, 7, 18, 8, 0),
        )

    def test_crossing_multiple_slots_returns_only_latest(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 18, 7, 30),
                datetime(2026, 7, 18, 16, 5),
            ),
            datetime(2026, 7, 18, 16, 0),
        )

    def test_crossing_midnight_can_return_previous_day_2000(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 17, 19, 30),
                datetime(2026, 7, 18, 7, 0),
            ),
            datetime(2026, 7, 17, 20, 0),
        )

    def test_slot_key_is_stable(self) -> None:
        self.assertEqual(
            status_email_slot_key(datetime(2026, 7, 18, 8, 0)),
            "2026-07-18T08:00",
        )

    def test_claim_persists_and_rejects_duplicate_after_reload(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            slot = datetime(2026, 7, 18, 8, 0)
            self.assertTrue(claim_status_email_slot(slot, path=path, now=slot))
            self.assertFalse(claim_status_email_slot(slot, path=path, now=slot))
            self.assertEqual(load_sent_status_email_slots(path), {"2026-07-18T08:00"})

    def test_claim_prunes_keys_older_than_14_days(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            path.write_text(
                json.dumps({"sent_slots": ["2026-06-01T08:00", "2026-07-17T20:00"]}),
                encoding="utf-8",
            )
            claim_status_email_slot(
                datetime(2026, 7, 18, 8, 0),
                path=path,
                now=datetime(2026, 7, 18, 8, 0),
            )
            self.assertEqual(
                load_sent_status_email_slots(path),
                {"2026-07-17T20:00", "2026-07-18T08:00"},
            )

    def test_corrupt_state_logs_and_falls_back_to_empty(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            path.write_text("not-json", encoding="utf-8")
            logs: list[str] = []
            self.assertEqual(load_sent_status_email_slots(path, logger=logs.append), set())
            self.assertEqual(len(logs), 1)
            self.assertIn("读取策略状态邮件记录失败", logs[0])

    def test_default_state_path_uses_expected_name(self) -> None:
        with TemporaryDirectory() as temp_dir:
            self.assertEqual(
                strategy_status_email_state_file_path(Path(temp_dir)).name,
                "strategy_status_email_state.json",
            )
