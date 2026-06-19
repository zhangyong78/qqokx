from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.email_schedule_manager import (
    format_event_id,
    format_task_result,
    load_email_archive_records,
    normalize_event_task_name,
    summarize_event_message,
    task_slot_label,
    task_sort_key,
)


class EmailScheduleManagerHelpersTest(TestCase):
    def test_task_slot_label_reads_suffix(self) -> None:
        self.assertEqual(task_slot_label("QQOKX BTC Analysis Email 0800"), "08:00")
        self.assertEqual(task_slot_label("QQOKX BTC Analysis Email"), "-")

    def test_task_sort_key_uses_slot_suffix(self) -> None:
        self.assertLess(
            task_sort_key("QQOKX BTC Analysis Email 0400"),
            task_sort_key("QQOKX BTC Analysis Email 1600"),
        )

    def test_format_task_result_prefers_known_labels(self) -> None:
        self.assertEqual(format_task_result(0), "0 成功")
        self.assertEqual(format_task_result(267009), "267009 运行中")
        self.assertEqual(format_task_result(42), "42")

    def test_format_event_id_prefers_known_labels(self) -> None:
        self.assertEqual(format_event_id(102), "102 已完成")
        self.assertEqual(format_event_id(999), "999")

    def test_summarize_event_message_compacts_whitespace(self) -> None:
        message = "Task   started\r\nwith   multiple\tspaces"
        self.assertEqual(summarize_event_message(message), "Task started with multiple spaces")

    def test_normalize_event_task_name_uses_message_when_property_missing(self) -> None:
        self.assertEqual(
            normalize_event_task_name("", "Task Scheduler launched \\QQOKX BTC Analysis Email 0000 successfully"),
            "QQOKX BTC Analysis Email 0000",
        )

    def test_load_email_archive_records_reads_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            archive_dir = Path(temp_dir)
            payload = {
                "subject": "Daily digest",
                "delivery_status": "pending_morning_release",
                "scheduled_release_slot": "08:00",
                "analysis_slot": "04:00",
                "generated_at": "2026-06-19T04:00:00Z",
                "archived_at": "2026-06-19T04:01:00Z",
                "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
                "archive_html_path": str(archive_dir / "digest.html"),
                "archive_text_path": str(archive_dir / "digest.txt"),
                "report_path": str(archive_dir / "report.json"),
            }
            (archive_dir / "multi_coin_market_digest_email_20260619T040100000000Z.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            rows = load_email_archive_records(archive_dir=archive_dir)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].subject, "Daily digest")
            self.assertEqual(rows[0].delivery_status, "pending_morning_release")
            self.assertEqual(rows[0].analysis_slot, "04:00")
            self.assertEqual(rows[0].symbols, ("BTC-USDT-SWAP", "ETH-USDT-SWAP"))
