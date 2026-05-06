import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.journal import (
    JournalResearchSample,
    build_research_sample_prompt,
    create_journal_entry,
    extract_journal_locally,
    parse_ai_extraction_paste,
    parse_research_sample_paste,
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

    def test_parse_ai_extraction_paste_accepts_full_research_sample(self) -> None:
        result = parse_ai_extraction_paste(
            """
            {
              "record_type": "market_view",
              "symbol": "BTC-USDT-SWAP",
              "timeframe": "1D",
              "related_timeframes": ["4H"],
              "market_phase": "trend_up",
              "title": "日线看涨三天，出现看跌吞没则失效",
              "raw_summary": "BTC日线看涨三天，近期创出两个月新高；如果日线出现阴线反包阳线则失效。",
              "bias": "long",
              "confidence": 0.64,
              "priority": "medium",
              "hypothesis": {
                "type": "directional_view",
                "statement": "基于日线走强并创出近两个月新高，未来三天行情更偏向延续看涨。"
              },
              "execution_plan": {
                "intended_action": "observe",
                "position_style": "unknown",
                "position_size_note": "",
                "entry_idea": "",
                "trigger_conditions": [],
                "invalidation_conditions": [
                  {
                    "type": "pattern_confirmation",
                    "text": "日线出现看跌吞没（阴线反包阳线）则失效",
                    "value": ""
                  }
                ],
                "targets": [],
                "risk_notes": "当前属于方向判断，不直接等同于具体交易执行。"
              },
              "observation": {
                "key_levels": [],
                "structure_notes": "日线偏强，近期创出两个月新高。",
                "volatility_notes": "",
                "program_disagreement_note": ""
              },
              "verification_plan": {
                "status": "pending",
                "verification_type": "market_outcome",
                "success_criteria": ["未来三天整体走势维持偏强"],
                "failure_criteria": ["未来三天内日线出现看跌吞没"],
                "review_windows": ["3D"]
              },
              "method_tags": ["日线结构", "看跌吞没", "价格行为"],
              "review_questions": ["看跌吞没是否只看实体反包？"],
              "attachments": [],
              "notes_for_me": ""
            }
            """
        )

        self.assertEqual(result.source, "ai_paste")
        self.assertEqual(result.symbol, "BTC")
        self.assertEqual(result.inst_id, "BTC-USDT-SWAP")
        self.assertEqual(result.timeframes, ("1D", "4H"))
        self.assertEqual(result.bias, "long")
        self.assertEqual(result.planned_action, "observe")
        self.assertIn("看跌吞没", result.invalidation_text)
        self.assertEqual(result.raw_payload["record_type"], "market_view")
        self.assertEqual(result.raw_payload["verification_plan"]["review_windows"], ["3D"])

    def test_research_sample_round_trips_structured_fields(self) -> None:
        sample = JournalResearchSample.from_dict(
            {
                "record_type": "market_view",
                "symbol": "BTC-USDT-SWAP",
                "timeframe": "1D",
                "related_timeframes": ["4H"],
                "market_phase": "trend_up",
                "title": "daily bullish view",
                "raw_summary": "daily structure stays bullish for three days",
                "bias": "long",
                "confidence": 0.64,
                "priority": "medium",
                "hypothesis": {"type": "directional_view", "statement": "new two-month high supports upside"},
                "execution_plan": {
                    "intended_action": "observe",
                    "position_style": "unknown",
                    "position_size_note": "",
                    "entry_idea": "",
                    "trigger_conditions": [],
                    "invalidation_conditions": [
                        {"type": "pattern_confirmation", "text": "daily bearish engulfing invalidates", "value": ""}
                    ],
                    "targets": [],
                    "risk_notes": "not a direct trade plan",
                },
                "observation": {"key_levels": [], "structure_notes": "two-month high"},
                "verification_plan": {
                    "status": "pending",
                    "verification_type": "market_outcome",
                    "success_criteria": ["trend remains strong"],
                    "failure_criteria": ["bearish engulfing appears"],
                    "review_windows": ["3D"],
                },
                "method_tags": ["price_action"],
                "review_questions": ["define bearish engulfing body/wick rule"],
                "attachments": [],
                "notes_for_me": "",
            }
        )

        payload = sample.to_dict()

        self.assertEqual(sample.record_type, "market_view")
        self.assertEqual(sample.symbol, "BTC-USDT-SWAP")
        self.assertEqual(sample.execution_plan.intended_action, "observe")
        self.assertEqual(sample.execution_plan.invalidation_conditions[0].text, "daily bearish engulfing invalidates")
        self.assertEqual(payload["verification_plan"]["review_windows"], ["3D"])

    def test_parse_research_sample_paste_returns_full_sample(self) -> None:
        sample = parse_research_sample_paste(
            """
            {
              "record_type": "research_hypothesis",
              "symbol": "BTC",
              "timeframe": "4H",
              "bias": "mixed",
              "hypothesis": {"type": "method_hypothesis", "statement": "volatility expansion changes signal quality"},
              "execution_plan": {"intended_action": "none"},
              "verification_plan": {"status": "pending", "review_windows": ["20 bars"]},
              "method_tags": ["volatility"]
            }
            """
        )

        self.assertEqual(sample.record_type, "research_hypothesis")
        self.assertEqual(sample.symbol, "BTC")
        self.assertEqual(sample.bias, "mixed")
        self.assertEqual(sample.execution_plan.intended_action, "none")
        self.assertEqual(sample.verification_plan.review_windows, ("20 bars",))

    def test_build_research_sample_prompt_uses_full_schema(self) -> None:
        prompt = build_research_sample_prompt("BTC 日线看涨三天")

        self.assertIn("record_type", prompt)
        self.assertIn("verification_plan", prompt)
        self.assertIn("review_questions", prompt)
        self.assertIn("BTC 日线看涨三天", prompt)

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
