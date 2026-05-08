import json
from decimal import Decimal
from unittest import TestCase

from okx_quant.btc_market_analysis_ui import (
    build_market_analysis_display_payload,
    build_market_analysis_overview_text,
    parse_replay_time_text,
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
    def test_overview_text_includes_candle_time_and_no_utc_label(self) -> None:
        closes = [Decimal("100") + (Decimal(index) * Decimal("0.4")) for index in range(45)]
        candles = _candles_from_closes(closes)
        analysis = analyze_btc_market_from_candle_map(
            {"1H": candles, "4H": candles, "1D": candles},
            symbol="BTC-USDT-SWAP",
        )

        text = build_market_analysis_overview_text(analysis)

        self.assertIn("K", text)
        self.assertIn("[1H]", text)
        self.assertIn("[4H]", text)
        self.assertIn("[1D]", text)
        self.assertNotIn("(UTC)", text)
        self.assertNotIn("uptrend", text)
        self.assertNotIn("downtrend", text)
        self.assertNotIn("sideways", text)
        self.assertNotIn("bias=", text)

    def test_display_payload_uses_local_time_key_instead_of_utc(self) -> None:
        closes = [Decimal("100") + (Decimal(index) * Decimal("0.4")) for index in range(45)]
        candles = _candles_from_closes(closes)
        analysis = analyze_btc_market_from_candle_map(
            {"1H": candles, "4H": candles, "1D": candles},
            symbol="BTC-USDT-SWAP",
        )

        payload = build_market_analysis_display_payload(analysis)

        self.assertNotIn("生成时间(UTC)", payload)
        self.assertNotIn("...Z", json_repr(payload))
        self.assertTrue(any(isinstance(value, str) and value.count(":") >= 2 and "Z" not in value for value in payload.values()))
        self.assertTrue(any(isinstance(value, str) and value.count(":") >= 1 and "Z" not in value for value in payload.values()))
        self.assertIn("周期分析", payload)

    def test_parse_replay_time_text_accepts_expected_format(self) -> None:
        parsed = parse_replay_time_text("2026-05-07 15:00")
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.strftime("%Y-%m-%d %H:%M"), "2026-05-07 15:00")


def json_repr(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False)
