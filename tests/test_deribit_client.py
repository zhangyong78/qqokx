from decimal import Decimal
from unittest import TestCase

from okx_quant.deribit_client import DeribitApiError, DeribitRestClient


class DummyDeribitClient(DeribitRestClient):
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def _request(self, path: str, *, params: dict[str, str]):
        self.calls.append((path, params))
        if not self.responses:
            raise AssertionError("No more dummy responses")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class DeribitClientTest(TestCase):
    def test_get_volatility_index_candles_parses_history(self) -> None:
        client = DummyDeribitClient(
            [
                {
                    "result": {
                        "data": [
                            [1000, 55.1, 56.2, 54.9, 55.8],
                            [2000, 55.8, 57.0, 55.5, 56.4],
                        ],
                        "continuation": None,
                    }
                }
            ]
        )

        candles = client.get_volatility_index_candles("BTC", "60", start_ts=1000, end_ts=2000)

        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0].ts, 1000)
        self.assertEqual(candles[1].close, Decimal("56.4"))
        self.assertEqual(client.calls[0][0], "/api/v2/public/get_volatility_index_data")
        self.assertEqual(client.calls[0][1]["currency"], "BTC")
        self.assertEqual(client.calls[0][1]["resolution"], "60")

    def test_get_volatility_index_candles_uses_continuation_and_trims_records(self) -> None:
        client = DummyDeribitClient(
            [
                {
                    "result": {
                        "data": [
                            [3000, 58, 59, 57, 58.5],
                            [4000, 58.5, 60, 58, 59],
                        ],
                        "continuation": 2500,
                    }
                },
                {
                    "result": {
                        "data": [
                            [1000, 55, 56, 54, 55.5],
                            [2000, 55.5, 57, 55, 56],
                        ],
                        "continuation": None,
                    }
                },
            ]
        )

        candles = client.get_volatility_index_candles("btc", "3600", start_ts=1000, end_ts=4000, max_records=3)

        self.assertEqual([item.ts for item in candles], [2000, 3000, 4000])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.calls[1][1]["end_timestamp"], "2500")

    def test_get_volatility_index_candles_rejects_invalid_range(self) -> None:
        client = DummyDeribitClient([])
        with self.assertRaises(ValueError):
            client.get_volatility_index_candles("BTC", "60", start_ts=2000, end_ts=1000)

    def test_get_volatility_index_candles_requires_result_dict(self) -> None:
        client = DummyDeribitClient([{"result": None}])
        with self.assertRaises(DeribitApiError):
            client.get_volatility_index_candles("BTC", "60", start_ts=1000, end_ts=2000)
