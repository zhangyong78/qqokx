import json
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4
from zoneinfo import ZoneInfo

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalyzerConfig,
    analyze_btc_market_at_time,
    analyze_btc_market_from_candle_map,
    btc_market_analysis_payload,
    build_pattern_focus_events,
    build_btc_market_analysis_email_body,
    save_btc_market_analysis,
)
from okx_quant.models import Candle


def _candles_from_closes(closes: list[Decimal], *, start_ts: int = 1000, step_ms: int = 1000) -> list[Candle]:
    candles: list[Candle] = []
    previous_close: Decimal | None = None
    for index, close_price in enumerate(closes):
        open_price = close_price - Decimal("0.15") if previous_close is None else previous_close
        high = max(open_price, close_price) + Decimal("0.2")
        low = min(open_price, close_price) - Decimal("0.2")
        candles.append(
            Candle(
                ts=start_ts + (index * step_ms),
                open=open_price,
                high=high,
                low=low,
                close=close_price,
                volume=Decimal("1"),
                confirmed=True,
            )
        )
        previous_close = close_price
    return candles


def _bullish_trend_candles(*, start_ts: int = 1000, step_ms: int = 1000) -> list[Candle]:
    closes: list[Decimal] = []
    price = Decimal("100")
    for _ in range(35):
        price += Decimal("0.35")
        closes.append(price)
    price -= Decimal("0.20")
    closes.append(price)
    for _ in range(5):
        price += Decimal("0.30")
        closes.append(price)
    return _candles_from_closes(closes, start_ts=start_ts, step_ms=step_ms)


class BtcMarketAnalyzerTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_analysis_keeps_recent_candles_for_email_chart_snapshot(self) -> None:
        candles = _candles_from_closes([Decimal(100 + index) for index in range(400)])

        analysis = analyze_btc_market_from_candle_map(
            {"1H": candles},
            config=BtcMarketAnalyzerConfig(timeframes=("1H",)),
        )

        chart_map = dict(analysis.chart_candles)
        self.assertEqual(len(chart_map["1H"]), 320)
        self.assertEqual(chart_map["1H"][-1].ts, candles[-1].ts)

    def test_multi_timeframe_analysis_produces_long_resonance(self) -> None:
        candles = _bullish_trend_candles()
        analysis = analyze_btc_market_from_candle_map(
            {"1H": candles, "4H": candles, "1D": candles},
            symbol="BTC-USDT-SWAP",
        )

        self.assertEqual(analysis.direction, "long")
        self.assertEqual(analysis.resonance.direction, "long")
        self.assertEqual(len(analysis.timeframes), 3)
        self.assertTrue(all(item.direction == "long" for item in analysis.timeframes))

        payload = btc_market_analysis_payload(analysis)
        self.assertEqual(payload["symbol"], "BTC-USDT-SWAP")
        self.assertEqual(payload["direction"], "long")
        self.assertEqual(payload["resonance"]["direction"], "long")
        self.assertEqual(payload["mode"], "realtime")
        self.assertIn("focus_events", payload["timeframes"][0])

    def test_build_pattern_focus_events_supports_daily_timeframe(self) -> None:
        candles = [
            Candle(ts=1000, open=Decimal("102"), high=Decimal("103"), low=Decimal("100"), close=Decimal("101"), volume=Decimal("1"), confirmed=True),
            Candle(ts=2000, open=Decimal("101"), high=Decimal("101.5"), low=Decimal("98.5"), close=Decimal("99.5"), volume=Decimal("1"), confirmed=True),
            Candle(ts=3000, open=Decimal("99.5"), high=Decimal("100"), low=Decimal("96.5"), close=Decimal("97"), volume=Decimal("1"), confirmed=True),
            Candle(ts=4000, open=Decimal("97.0"), high=Decimal("97.9"), low=Decimal("92.5"), close=Decimal("97.7"), volume=Decimal("1"), confirmed=True),
        ]

        events = build_pattern_focus_events(candles, timeframe="1D")

        self.assertGreaterEqual(len(events), 1)
        self.assertEqual(events[0].timeframe, "1D")
        self.assertTrue(any(item.label in {"锤子线", "长下影"} for item in events))

    def test_save_and_email_body_are_ready_for_delivery(self) -> None:
        candles = _bullish_trend_candles()
        analysis = analyze_btc_market_from_candle_map(
            {"1H": candles, "4H": candles, "1D": candles},
            symbol="BTC-USDT-SWAP",
        )

        temp_dir = self._workspace_temp_dir()
        output_path = temp_dir / "btc_market_analysis.json"
        saved_path = save_btc_market_analysis(analysis, path=output_path)
        persisted = json.loads(saved_path.read_text(encoding="utf-8"))
        email_body = build_btc_market_analysis_email_body(analysis)

        self.assertEqual(saved_path, output_path)
        self.assertEqual(persisted["symbol"], "BTC-USDT-SWAP")
        self.assertIn("generated_at", persisted)
        self.assertIn("最简结论：", email_body)
        self.assertIn("重点变化：", email_body)
        self.assertIn("当前快照：", email_body)
        self.assertIn("1小时", email_body)
        self.assertIn("4小时", email_body)

    def test_historical_replay_only_uses_candles_before_selected_point(self) -> None:
        analysis_dt = datetime(2026, 5, 7, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        end_ts = int(analysis_dt.astimezone(timezone.utc).timestamp() * 1000)
        history = _bullish_trend_candles(start_ts=end_ts - (39 * 3_600_000), step_ms=3_600_000)

        class StubClient:
            def get_candles_history_range(self, inst_id, bar, *, start_ts, end_ts, limit, preload_count):
                selected = [item for item in history if start_ts <= item.ts <= end_ts]
                selected.append(
                    Candle(
                        ts=end_ts + 3_600_000,
                        open=Decimal("999"),
                        high=Decimal("1000"),
                        low=Decimal("998"),
                        close=Decimal("999"),
                        volume=Decimal("1"),
                        confirmed=True,
                    )
                )
                return selected

        analysis = analyze_btc_market_at_time(StubClient(), symbol="BTC-USDT-SWAP", analysis_dt=analysis_dt)

        self.assertEqual(analysis.mode, "historical_replay")
        self.assertEqual(analysis.analysis_point, "2026-05-07T15:00:00+08:00")
        self.assertEqual(analysis.data_cutoff_rule, "close_time_lte_analysis_point")
        self.assertTrue(all((item.candle_ts or 0) <= end_ts for item in analysis.timeframes))
        self.assertIsNotNone(analysis.validation)
