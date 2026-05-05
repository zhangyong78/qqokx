from decimal import Decimal
from unittest import TestCase
from unittest.mock import MagicMock, patch

from okx_quant.candle_cache_verify import verify_and_repair_cached_candles
from okx_quant.models import Candle


def _c(ts: int) -> Candle:
    p = Decimal(str(ts))
    return Candle(ts=ts, open=p, high=p, low=p, close=p, volume=Decimal("1"), confirmed=True)


class CandleCacheVerifyTest(TestCase):
    @patch("okx_quant.candle_cache_verify.load_candle_cache")
    def test_empty_cache_triggers_full_history_then_passes(self, mock_load: MagicMock) -> None:
        merged = [_c(900_000), _c(1_800_000)]
        mock_load.side_effect = [[], merged]

        client = MagicMock()
        client.get_candles_history.return_value = merged

        out = verify_and_repair_cached_candles(client, "BTC-USDT-SWAP", "15m")
        self.assertTrue(out.ok)
        self.assertTrue(out.did_full_history_fetch)
        client.get_candles_history.assert_called_once_with("BTC-USDT-SWAP", "15m", limit=0)
        client.get_candles_history_range.assert_not_called()

    @patch("okx_quant.candle_cache_verify.load_candle_cache")
    def test_gap_triggers_range_fetch(self, mock_load: MagicMock) -> None:
        before = [_c(900_000), _c(2_700_000)]
        after = [_c(900_000), _c(1_800_000), _c(2_700_000)]
        mock_load.side_effect = [before, after]

        client = MagicMock()
        out = verify_and_repair_cached_candles(client, "ETH-USDT-SWAP", "15m")
        self.assertTrue(out.ok)
        self.assertEqual(out.missing_bars_before, 1)
        self.assertEqual(out.missing_bars_after, 0)
        self.assertGreaterEqual(out.range_fetch_calls, 1)
        client.get_candles_history_range.assert_called()
