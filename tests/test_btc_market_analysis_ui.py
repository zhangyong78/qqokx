from decimal import Decimal
from unittest import TestCase

from okx_quant.btc_market_analysis_ui import (
    build_market_analysis_display_payload,
    build_market_analysis_overview_text,
)
from okx_quant.btc_market_analyzer import analyze_btc_market_from_candle_map
from okx_quant.models import Candle


def _candles_from_closes(closes: list[Decimal]) -> list[Candle]:
    candles: list[Candle] = []
    previous_close: Decimal | None = None
    for index, close_price in enumerate(closes):
        open_price = close_price - Decimal("0.15") if previous_close is None else previous_close
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


class BtcMarketAnalysisUiTest(TestCase):
    def test_overview_text_includes_direction_and_timeframes(self) -> None:
        closes = [Decimal("100") + (Decimal(index) * Decimal("0.4")) for index in range(45)]
        candles = _candles_from_closes(closes)
        analysis = analyze_btc_market_from_candle_map(
            {
                "1H": candles,
                "4H": candles,
                "1D": candles,
            },
            symbol="BTC-USDT-SWAP",
        )

        text = build_market_analysis_overview_text(analysis)

        self.assertIn("综合方向", text)
        self.assertIn("[1H]", text)
        self.assertIn("[4H]", text)
        self.assertIn("[1D]", text)
        self.assertNotIn("uptrend", text)
        self.assertNotIn("downtrend", text)
        self.assertNotIn("sideways", text)
        self.assertNotIn("bias=", text)

    def test_display_payload_uses_chinese_labels(self) -> None:
        closes = [Decimal("100") + (Decimal(index) * Decimal("0.4")) for index in range(45)]
        candles = _candles_from_closes(closes)
        analysis = analyze_btc_market_from_candle_map(
            {
                "1H": candles,
                "4H": candles,
                "1D": candles,
            },
            symbol="BTC-USDT-SWAP",
        )

        payload = build_market_analysis_display_payload(analysis)

        self.assertIn("综合方向", payload)
        self.assertIn(payload["综合方向"]["结果"], {"看多", "看空", "中性"})
        self.assertIn("周期分析", payload)
        first_timeframe = payload["周期分析"][0]
        self.assertIn("方向", first_timeframe)
        self.assertIn(first_timeframe["方向"]["结果"], {"看多", "看空", "中性"})
        self.assertIn("趋势语境", first_timeframe)
        self.assertNotIn(first_timeframe["趋势语境"]["结果"], {"uptrend", "downtrend", "sideways"})
