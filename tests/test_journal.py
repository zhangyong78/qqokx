import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.journal import (
    create_journal_entry,
    extract_journal_locally,
    parse_ai_extraction_paste,
)
from okx_quant.persistence import (
    journal_entries_file_path,
    load_journal_entries_snapshot,
    save_journal_entries_snapshot,
)


class JournalTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_local_extractor_handles_btc_trade_hypothesis(self) -> None:
        result = extract_journal_locally(
            "BTC 4H 还是偏多，62500 附近如果回踩不破，1H 有止跌形态我准备轻仓多。"
            "跌破 61500 这个思路作废。"
        )

        self.assertEqual(result.symbol, "BTC")
        self.assertEqual(result.inst_id, "BTC-USDT-SWAP")
        self.assertEqual(result.bias, "long")
        self.assertEqual(result.timeframes, ("4H", "1H"))
        self.assertEqual(result.entry_zone_price, Decimal("62500"))
        self.assertEqual(result.invalidation_price, Decimal("61500"))
        self.assertEqual(result.planned_action, "open_long")
        self.assertEqual(result.position_size_text, "轻仓")
        self.assertTrue(result.needs_review)
        self.assertIn("需要确认关键价位附近的容差范围。", result.review_questions)
        self.assertIn("62500", result.summary)

    def test_parse_ai_extraction_paste_accepts_fenced_json(self) -> None:
        result = parse_ai_extraction_paste(
            """
            ```json
            {
              "symbol": "BTC",
              "bias": "long",
              "timeframes": ["4H", "1H"],
              "entry_zone": 62500,
              "trigger_text": "回踩不破，1H 有止跌形态",
              "invalidation_price": 61500,
              "planned_action": "open_long",
              "position_size_text": "轻仓",
              "summary": "4H 偏多，62500 附近止跌时轻仓做多。",
              "needs_review": true,
              "review_questions": ["止跌形态如何确认？"]
            }
            ```
            """
        )

        self.assertEqual(result.source, "ai_paste")
        self.assertEqual(result.inst_id, "BTC-USDT-SWAP")
        self.assertEqual(result.entry_zone_price, Decimal("62500"))
        self.assertEqual(result.invalidation_price, Decimal("61500"))
        self.assertEqual(result.review_questions, ("止跌形态如何确认？",))

    def test_save_and_load_journal_entries_snapshot(self) -> None:
        temp_path = journal_entries_file_path(self._workspace_temp_dir())
        extraction = extract_journal_locally("BTC 4H 偏多，62500 附近回踩不破准备轻仓多。跌破 61500 作废。")
        entry = create_journal_entry("BTC 4H 偏多，62500 附近回踩不破准备轻仓多。跌破 61500 作废。", extraction=extraction)

        save_journal_entries_snapshot([entry.to_dict()], temp_path)
        snapshot = load_journal_entries_snapshot(temp_path)

        self.assertEqual(len(snapshot["entries"]), 1)
        saved = snapshot["entries"][0]
        self.assertEqual(saved["entry_id"], entry.entry_id)
        self.assertEqual(saved["status"], "review")
        self.assertEqual(saved["extraction"]["inst_id"], "BTC-USDT-SWAP")
