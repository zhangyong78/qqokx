import json
import shutil
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.btc_market_analyzer import (
    analyze_btc_market_from_candle_map,
    btc_market_analysis_payload,
    build_btc_market_analysis_email_body,
    save_btc_market_analysis,
)
from okx_quant.models import Candle


def _candles_from_closes(closes: list[Decimal]) -> list[Candle]:
    candles: list[Candle] = []
    previous_close: Decimal | None = None
    for index, close_price in enumerate(closes):
        if previous_close is None:
            open_price = close_price - Decimal("0.15")
        else:
            open_price = previous_close
        high = max(open_price, close_price) + Decimal("0.2")
        low = min(open_price, close_price) - Decimal("0.2")
        candles.append(
            Candle(
                ts=(index + 1) * 1000,
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


def _bullish_trend_candles() -> list[Candle]:
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
    return _candles_from_closes(closes)


class BtcMarketAnalyzerTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_multi_timeframe_analysis_produces_long_resonance(self) -> None:
        candles = _bullish_trend_candles()
        analysis = analyze_btc_market_from_candle_map(
            {
                "1H": candles,
                "4H": candles,
                "1D": candles,
            },
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

    def test_save_and_email_body_are_ready_for_delivery(self) -> None:
        candles = _bullish_trend_candles()
        analysis = analyze_btc_market_from_candle_map(
            {
                "1H": candles,
                "4H": candles,
                "1D": candles,
            },
            symbol="BTC-USDT-SWAP",
        )

        temp_dir = self._workspace_temp_dir()
        output_path = temp_dir / "btc_market_analysis.json"
        saved_path = save_btc_market_analysis(analysis, path=output_path)
        persisted = json.loads(saved_path.read_text(encoding="utf-8"))
        email_body = build_btc_market_analysis_email_body(analysis)

        self.assertEqual(saved_path, output_path)
        self.assertEqual(persisted["symbol"], "BTC-USDT-SWAP")
        self.assertIn("综合方向", email_body)
        self.assertIn("[1H]", email_body)
