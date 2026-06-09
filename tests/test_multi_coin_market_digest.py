from __future__ import annotations

from decimal import Decimal
from unittest import TestCase

from okx_quant.btc_market_analyzer import BtcMarketAnalysis, PatternFocusEvent, ResonanceAnalysis, TimeframeAnalysis
from okx_quant.multi_coin_market_digest import (
    archive_multi_coin_market_email,
    build_multi_coin_market_email_body,
    build_multi_coin_market_email_html,
    multi_coin_market_digest_payload,
    analyze_multi_coin_market,
)
import tempfile
from pathlib import Path
from unittest.mock import patch


def _timeframe(timeframe: str, direction: str, score: int, focus_labels: tuple[str, ...] = ()) -> TimeframeAnalysis:
    events = tuple(
        PatternFocusEvent(
            timeframe=timeframe,
            ts=1_700_000_000_000 + (index * 3_600_000),
            pattern_id=label,
            label=label,
            direction="long" if "多" in label or "底" in label else "short",
            score=3,
            candle_count=1,
            source="test",
            summary=f"{label} summary",
            reason=f"{label} reason",
        )
        for index, label in enumerate(focus_labels)
    )
    return TimeframeAnalysis(
        symbol="",
        timeframe=timeframe,
        candle_ts=1_700_000_000_000,
        last_close=Decimal("100"),
        direction=direction,
        score=score,
        confidence=Decimal("0.7"),
        trend_context="uptrend",
        signals=(),
        reason=(f"{timeframe} reason",),
        probability={},
        indicators={},
        pattern={},
        focus_events=events,
    )


def _analysis(symbol: str, direction: str, score: int, tf4h_dir: str, tf1h_dir: str, focus4h: tuple[str, ...] = (), focus1h: tuple[str, ...] = ()) -> BtcMarketAnalysis:
    return BtcMarketAnalysis(
        symbol=symbol,
        generated_at="2026-05-29T00:00:00Z",
        direction=direction,
        score=score,
        confidence=Decimal("0.8"),
        resonance=ResonanceAnalysis(direction=direction if direction in {"long", "short"} else "neutral", aligned_timeframes=("4H", "1H"), score=1, confidence=Decimal("0.8"), summary="aligned"),
        signals=(),
        reason=(f"{symbol} reason",),
        timeframes=(
            _timeframe("4H", tf4h_dir, score, focus4h),
            _timeframe("1H", tf1h_dir, score - 1, focus1h),
            _timeframe("1D", direction, score - 2),
        ),
    )


class MultiCoinMarketDigestTest(TestCase):
    def test_analyze_multi_coin_market_ranks_leaders(self) -> None:
        analyses = (
            _analysis("BTC-USDT-SWAP", "long", 8, "long", "long", ("底分型",), ("锤子线",)),
            _analysis("ETH-USDT-SWAP", "short", -9, "short", "short", ("顶分型",), ("双线反转看空",)),
            _analysis("SOL-USDT-SWAP", "neutral", 2, "long", "short", ("大阳线",), ("孕线",)),
        )

        class StubClient:
            def __init__(self) -> None:
                self.index = 0

            def get_candles_history(self, symbol, timeframe, limit=0):
                raise AssertionError("should not be called")

        # Build a digest-like object through the public analyzer function shape
        digest = type("Digest", (), {})()
        # Use payload function expectations by calling internal constructor path indirectly is overkill;
        # build the exact public dataclass via function import pattern is enough for this test.
        from okx_quant.multi_coin_market_digest import MultiCoinMarketDigest, _pick_best_trade_candidate, _pick_strongest_long, _pick_weakest_short

        digest = MultiCoinMarketDigest(
            generated_at="2026-05-29T00:00:00Z",
            symbols=tuple(item.symbol for item in analyses),
            analyses=analyses,
            strongest_long=_pick_strongest_long(analyses),
            weakest_short=_pick_weakest_short(analyses),
            best_trade_candidate=_pick_best_trade_candidate(analyses),
        )

        payload = multi_coin_market_digest_payload(digest)
        body = build_multi_coin_market_email_body(digest)
        html = build_multi_coin_market_email_html(
            digest,
            chart_image_map={
                "BTC-USDT-SWAP": {"1H": "ZmFrZV9pbWFnZQ==", "4H": "ZmFrZV9pbWFnZQ==", "1D": "ZmFrZV9pbWFnZQ==", "1W": "ZmFrZV9pbWFnZQ=="},
                "ETH-USDT-SWAP": {"1H": "ZmFrZV9pbWFnZQ==", "4H": "ZmFrZV9pbWFnZQ==", "1D": "ZmFrZV9pbWFnZQ==", "1W": "ZmFrZV9pbWFnZQ=="},
            },
            overlay_legend_map={
                "BTC-USDT-SWAP": {
                    "1H": "叠加：<span>EMA21</span><span>MA50</span><span>EMA55</span>",
                    "4H": "叠加：<span>EMA21</span><span>EMA55</span>",
                    "1D": "叠加：<span>EMA21</span><span>EMA55</span>",
                    "1W": "叠加：<span>EMA21</span><span>EMA55</span>",
                },
                "ETH-USDT-SWAP": {
                    "1H": "叠加：<span>MA34</span><span>EMA55</span>",
                    "4H": "叠加：<span>EMA21</span><span>EMA55</span>",
                    "1D": "叠加：<span>EMA21</span><span>EMA55</span>",
                    "1W": "叠加：<span>EMA21</span><span>EMA55</span>",
                },
            },
        )

        self.assertEqual(payload["leaders"]["strongest_long"]["symbol"], "BTC-USDT-SWAP")
        self.assertEqual(payload["leaders"]["weakest_short"]["symbol"], "ETH-USDT-SWAP")
        self.assertIn(payload["leaders"]["best_trade_candidate"]["symbol"], {"BTC-USDT-SWAP", "ETH-USDT-SWAP"})
        self.assertIn("简明结论：", body)
        self.assertIn("做多最强", body)
        self.assertIn("分币摘要：", body)
        self.assertIn("<html", html)
        self.assertIn("生成时间：", html)
        self.assertIn("BTC", html)
        self.assertIn("ETH", html)
        self.assertIn("data:image/png;base64", html)
        self.assertIn("1H", html)
        self.assertIn("4H", html)
        self.assertIn("1D", html)
        self.assertIn("1W", html)
        self.assertIn("叠加：", html)
        self.assertIn("background-color:", html)

        with tempfile.TemporaryDirectory() as temp_dir:
            archive_root = Path(temp_dir) / "analysis"
            with patch("okx_quant.multi_coin_market_digest.analysis_report_dir_path", return_value=archive_root):
                archive_path = archive_multi_coin_market_email(
                    digest,
                    subject="[QQOKX] test",
                    body=body,
                    html_body=html,
                    report_path=None,
                )
            self.assertTrue(archive_path.exists())
            self.assertEqual(archive_path.parent.name, "email_archives")
            self.assertTrue(archive_path.with_suffix(".txt").exists())
            self.assertTrue(archive_path.with_suffix(".json").exists())
