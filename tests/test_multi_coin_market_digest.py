from __future__ import annotations

import json
from decimal import Decimal
from unittest import TestCase

from okx_quant.btc_market_analyzer import BtcMarketAnalysis, PatternFocusEvent, ResonanceAnalysis, TimeframeAnalysis
from okx_quant.multi_coin_market_digest import (
    archive_multi_coin_market_email,
    build_multi_coin_market_email_body,
    build_multi_coin_market_email_html,
    build_multi_coin_chart_image_map,
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
        validation_payload = {
            "generated_at": "2026-06-16T12:00:00Z",
            "details": [
                {
                    "archive_meta_path": "a.json",
                    "generated_at": "2026-06-16T08:00:00Z",
                    "symbol": "BTC-USDT-SWAP",
                    "stance": "优先做多",
                    "return_24h_pct": 1.2,
                    "validation": {"verdict": "effective"},
                },
                {
                    "archive_meta_path": "a.json",
                    "generated_at": "2026-06-16T08:00:00Z",
                    "symbol": "ETH-USDT-SWAP",
                    "stance": "优先做空",
                    "return_24h_pct": -0.5,
                    "validation": {"verdict": "invalid"},
                },
                {
                    "archive_meta_path": "b.json",
                    "generated_at": "2026-06-16T12:00:00Z",
                    "symbol": "SOL-USDT-SWAP",
                    "stance": "暂观望",
                    "return_24h_pct": 0.3,
                    "validation": {"verdict": "pending"},
                },
            ],
        }
        with patch("okx_quant.multi_coin_market_digest.load_latest_email_validation_payload", return_value=validation_payload):
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
        self.assertIn("明确观点：", body)
        self.assertIn("最近复盘：", body)
        self.assertIn("覆盖最近 2 封已发送邮件", body)
        self.assertIn("明确观点命中率", body)
        self.assertIn("命中率最高币种", body)
        self.assertIn("命中率最低币种", body)
        self.assertIn("最值得关注的变化", body)
        self.assertIn("今日优先跟踪", body)
        self.assertIn("今日谨慎对待", body)
        self.assertIn("若只做一笔", body)
        self.assertIn("各币种最近命中率简表", body)
        self.assertIn("BTC：命中率", body)
        self.assertIn("做多最强", body)
        self.assertIn("分币摘要：", body)
        self.assertIn("<html", html)
        self.assertIn("生成时间：", html)
        self.assertIn("明确观点", html)
        self.assertIn("最近复盘", html)
        self.assertIn("命中率最高币种", html)
        self.assertIn("命中率最低币种", html)
        self.assertIn("最值得关注的变化", html)
        self.assertIn("今日优先跟踪", html)
        self.assertIn("今日谨慎对待", html)
        self.assertIn("若只做一笔", html)
        self.assertIn("各币种最近命中率简表", html)
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
            metadata = json.loads(archive_path.with_suffix(".json").read_text(encoding="utf-8"))
            self.assertIn("viewpoints", metadata)
            self.assertIn("digest_payload", metadata)
            self.assertEqual(metadata["digest_payload"]["symbols"], list(digest.symbols))
            self.assertEqual(metadata["viewpoints"][0]["symbol"], "BTC-USDT-SWAP")

    def test_build_chart_image_map_reuses_cached_intraday_data_and_fetches_weekly_directly(self) -> None:
        from okx_quant.multi_coin_market_digest import MultiCoinMarketDigest, _pick_best_trade_candidate, _pick_strongest_long, _pick_weakest_short
        from okx_quant.models import Candle

        analysis = _analysis("BTC-USDT-SWAP", "long", 8, "long", "long", ("底分型",), ("锤子线",))
        digest = MultiCoinMarketDigest(
            generated_at="2026-05-29T00:00:00Z",
            symbols=(analysis.symbol,),
            analyses=(analysis,),
            strongest_long=_pick_strongest_long((analysis,)),
            weakest_short=_pick_weakest_short((analysis,)),
            best_trade_candidate=_pick_best_trade_candidate((analysis,)),
        )

        def _candles(step_ms: int, count: int = 140) -> list[Candle]:
            rows: list[Candle] = []
            for index in range(count):
                price = Decimal(str(100 + index))
                rows.append(
                    Candle(
                        ts=index * step_ms,
                        open=price,
                        high=price + Decimal("1"),
                        low=price - Decimal("1"),
                        close=price,
                        volume=Decimal("1"),
                        confirmed=True,
                    )
                )
            return rows

        cached_map = {
            "1H": _candles(3_600_000),
            "4H": _candles(14_400_000),
            "1D": _candles(86_400_000),
        }
        weekly_candles = _candles(604_800_000)

        class StubChartClient:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str, int]] = []

            def get_candles_history(self, symbol: str, timeframe: str, limit: int = 0):  # noqa: ANN202
                self.calls.append((symbol, timeframe, limit))
                raise AssertionError("intraday/daily charts should reuse local cache")

            def get_candles(self, symbol: str, timeframe: str, limit: int = 0):  # noqa: ANN202
                self.calls.append((symbol, timeframe, limit))
                if timeframe != "1W":
                    raise AssertionError(f"unexpected direct fetch timeframe: {timeframe}")
                return weekly_candles[-limit:]

        client = StubChartClient()

        def _fake_load_candle_cache(symbol: str, timeframe: str, *, limit: int | None = None):  # noqa: ANN202
            rows = list(cached_map.get(timeframe, []))
            return rows if limit is None else rows[-limit:]

        with patch("okx_quant.multi_coin_market_digest.load_candle_cache", side_effect=_fake_load_candle_cache):
            with patch("okx_quant.multi_coin_market_digest.render_candles_png_base64", return_value="fake-chart"):
                image_map = build_multi_coin_chart_image_map(digest, client=client)

        self.assertEqual(
            image_map,
            {
                "BTC-USDT-SWAP": {
                    "1H": "fake-chart",
                    "4H": "fake-chart",
                    "1D": "fake-chart",
                    "1W": "fake-chart",
                }
            },
        )
        self.assertEqual(client.calls, [("BTC-USDT-SWAP", "1W", 127)])
