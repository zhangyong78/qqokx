from __future__ import annotations

import json
import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.analysis_email_validation import (
    build_recent_email_validation_summary,
    build_email_validation_report_payload,
    load_latest_email_validation_payload,
    load_email_analysis_records,
    save_email_validation_report,
    validate_email_analysis_records,
)
from okx_quant.models import Candle


def _analysis_payload(symbol: str, direction: str, score: int, confidence: str, ts: int, price: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "generated_at": "2026-06-16T12:00:00Z",
        "direction": direction,
        "score": score,
        "confidence": confidence,
        "reason": [f"{symbol} primary reason"],
        "timeframes": [
            {
                "timeframe": "1H",
                "candle_ts": ts,
                "last_close": price,
                "direction": direction,
                "score": score,
                "confidence": confidence,
            },
            {
                "timeframe": "4H",
                "candle_ts": ts,
                "last_close": price,
                "direction": direction,
                "score": score,
                "confidence": confidence,
            },
            {
                "timeframe": "1D",
                "candle_ts": ts,
                "last_close": price,
                "direction": direction,
                "score": score,
                "confidence": confidence,
            },
        ],
    }


class AnalysisEmailValidationTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_load_validate_and_save_email_validation_report(self) -> None:
        temp_dir = self._workspace_temp_dir()
        analysis_dir = temp_dir / "reports" / "analysis"
        archive_dir = analysis_dir / "email_archives"
        archive_dir.mkdir(parents=True, exist_ok=True)

        btc_payload = {
            "generated_at": "2026-06-16T12:00:00Z",
            "symbols": ["BTC-USDT-SWAP"],
            "analyses": [_analysis_payload("BTC-USDT-SWAP", "long", 8, "0.80", 1_000, "100")],
        }
        btc_meta = {
            "subject": "[QQOKX] test 1",
            "generated_at": "2026-06-16T12:00:00Z",
            "viewpoints": [
                {
                    "symbol": "BTC-USDT-SWAP",
                    "asset": "BTC",
                    "stance": "优先做多",
                    "summary": "4H 与 1H 同向偏多。",
                }
            ],
            "digest_payload": btc_payload,
        }
        (archive_dir / "multi_coin_market_digest_email_20260616T120000Z.json").write_text(
            json.dumps(btc_meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        eth_report_path = analysis_dir / "multi_coin_market_digest_20260616T130000Z.json"
        eth_payload = {
            "generated_at": "2026-06-16T13:00:00Z",
            "symbols": ["ETH-USDT-SWAP"],
            "analyses": [_analysis_payload("ETH-USDT-SWAP", "short", -8, "0.75", 2_000, "200")],
        }
        eth_report_path.write_text(json.dumps(eth_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        eth_meta = {
            "subject": "[QQOKX] test 2",
            "generated_at": "2026-06-16T13:00:00Z",
            "report_path": str(eth_report_path),
        }
        (archive_dir / "multi_coin_market_digest_email_20260616T130000Z.json").write_text(
            json.dumps(eth_meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        records = load_email_analysis_records(base_dir=temp_dir)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].symbol, "BTC-USDT-SWAP")
        self.assertEqual(records[0].stance, "优先做多")
        self.assertEqual(records[1].symbol, "ETH-USDT-SWAP")

        class StubClient:
            def get_candles_history_range(self, symbol, timeframe, *, start_ts, end_ts, limit, preload_count):  # noqa: ANN202
                if symbol == "BTC-USDT-SWAP":
                    return [
                        Candle(
                            ts=1_000 + (index + 1) * 3_600_000,
                            open=Decimal("100"),
                            high=Decimal("102") + Decimal(index),
                            low=Decimal("99"),
                            close=Decimal("101.2") + Decimal(index),
                            volume=Decimal("1"),
                            confirmed=True,
                        )
                        for index in range(24)
                    ]
                return [
                    Candle(
                        ts=2_000 + (index + 1) * 3_600_000,
                        open=Decimal("200"),
                        high=Decimal("203") + Decimal(index),
                        low=Decimal("199"),
                        close=Decimal("202") + Decimal(index),
                        volume=Decimal("1"),
                        confirmed=True,
                    )
                    for index in range(24)
                ]

        results = validate_email_analysis_records(records, client=StubClient(), windows_hours=(4, 12, 24))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].validation.verdict, "effective")
        self.assertEqual(results[1].validation.verdict, "invalid")

        payload = build_email_validation_report_payload(results, windows_hours=(4, 12, 24))
        self.assertEqual(payload["overall"]["samples"], 2)
        self.assertEqual(payload["overall"]["completed"], 2)
        self.assertEqual(payload["overall"]["effective"], 1)
        self.assertEqual(payload["overall"]["invalid"], 1)
        self.assertEqual(payload["overall"]["hit_rate_pct"], 50.0)
        self.assertEqual(payload["actionable"]["samples"], 2)
        self.assertIn("BTC-USDT-SWAP", payload["by_symbol"])
        self.assertIn("优先做多", payload["by_stance"])

        saved = save_email_validation_report(payload, base_dir=temp_dir)
        self.assertTrue(saved["json"].exists())
        self.assertTrue(saved["csv"].exists())
        self.assertTrue(saved["md"].exists())
        saved_payload = json.loads(saved["json"].read_text(encoding="utf-8"))
        self.assertEqual(saved_payload["overall"]["samples"], 2)

        latest_payload = load_latest_email_validation_payload(base_dir=temp_dir)
        self.assertIsNotNone(latest_payload)
        assert latest_payload is not None
        recent_summary = build_recent_email_validation_summary(latest_payload, recent_email_limit=1)
        self.assertIsNotNone(recent_summary)
        assert recent_summary is not None
        self.assertEqual(recent_summary["email_count"], 1)
        self.assertEqual(recent_summary["sample_count"], 1)
        self.assertEqual(recent_summary["overall"]["samples"], 1)
        self.assertIn("ETH-USDT-SWAP", recent_summary["by_symbol"])
        self.assertIn("highlights", recent_summary)

    def test_recent_summary_builds_symbol_highlights(self) -> None:
        payload = {
            "generated_at": "2026-06-16T12:00:00Z",
            "details": [
                {
                    "archive_meta_path": "a.json",
                    "generated_at": "2026-06-16T08:00:00Z",
                    "symbol": "BTC-USDT-SWAP",
                    "stance": "优先做多",
                    "return_24h_pct": 1.5,
                    "validation": {"verdict": "effective"},
                },
                {
                    "archive_meta_path": "b.json",
                    "generated_at": "2026-06-16T12:00:00Z",
                    "symbol": "ETH-USDT-SWAP",
                    "stance": "优先做空",
                    "return_24h_pct": -0.8,
                    "validation": {"verdict": "invalid"},
                },
            ],
        }
        summary = build_recent_email_validation_summary(payload, recent_email_limit=20)
        self.assertIsNotNone(summary)
        assert summary is not None
        highlights = summary["highlights"]
        self.assertEqual(highlights["best_symbol"]["symbol"], "BTC-USDT-SWAP")
        self.assertEqual(highlights["worst_symbol"]["symbol"], "ETH-USDT-SWAP")
        self.assertTrue(str(highlights["notable_change"]).strip())
        self.assertIn("BTC", str(highlights["notable_change"]) + str(summary))
