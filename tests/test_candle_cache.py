import json
from decimal import Decimal
from tempfile import TemporaryDirectory
from unittest import TestCase

import okx_quant.okx_client as okx_client_module
from okx_quant.candle_cache import (
    candle_cache_file_path,
    load_candle_cache,
    load_candle_cache_range,
    merge_candles,
    save_candle_cache,
)
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient


def _build_candle(ts: int, close: str) -> Candle:
    price = Decimal(close)
    return Candle(
        ts=ts,
        open=price,
        high=price + Decimal("1"),
        low=price - Decimal("1"),
        close=price,
        volume=Decimal("1"),
        confirmed=True,
    )


def _build_okx_row(ts: int, close: str) -> list[str]:
    price = Decimal(close)
    return [
        str(ts),
        str(price),
        str(price + Decimal("1")),
        str(price - Decimal("1")),
        str(price),
        "1",
        "0",
        "0",
        "1",
    ]


class DummyHistoryClient(OkxRestClient):
    def __init__(self, pages: dict[str, list[list[str]]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, str]] = []

    def _request(self, method, path, params=None, **kwargs):  # type: ignore[override]
        self.calls.append(dict(params or {}))
        after = (params or {}).get("after") or "__LATEST__"
        return {"code": "0", "data": self.pages.get(after, [])}


class CandleCacheTest(TestCase):
    def test_save_and_load_round_trip(self) -> None:
        candles = [_build_candle(1, "100"), _build_candle(2, "101")]

        with TemporaryDirectory() as temp_dir:
            save_candle_cache("BTC-USDT-SWAP", "15m", candles, base_dir=temp_dir)
            loaded = load_candle_cache("BTC-USDT-SWAP", "15m", base_dir=temp_dir)

        self.assertEqual(loaded, candles)

    def test_load_limit_returns_latest_cached_candles(self) -> None:
        candles = [_build_candle(index, str(100 + index)) for index in range(1, 6)]

        with TemporaryDirectory() as temp_dir:
            save_candle_cache("BTC-USDT-SWAP", "15m", candles, base_dir=temp_dir)
            loaded = load_candle_cache("BTC-USDT-SWAP", "15m", limit=2, base_dir=temp_dir)

        self.assertEqual([candle.ts for candle in loaded], [4, 5])

    def test_load_range_returns_preload_and_limited_selection(self) -> None:
        candles = [_build_candle(index, str(100 + index)) for index in range(1, 8)]

        with TemporaryDirectory() as temp_dir:
            save_candle_cache("BTC-USDT-SWAP", "15m", candles, base_dir=temp_dir)
            loaded = load_candle_cache_range(
                "BTC-USDT-SWAP",
                "15m",
                start_ts=3,
                end_ts=6,
                limit=2,
                preload_count=2,
                base_dir=temp_dir,
            )

        self.assertEqual([candle.ts for candle in loaded], [1, 2, 5, 6])

    def test_legacy_json_cache_migrates_to_sqlite(self) -> None:
        payload = {
            "inst_id": "BTC-USDT-SWAP",
            "bar": "15m",
            "candles": [
                {
                    "ts": 1,
                    "open": "100.123456789",
                    "high": "101.123456789",
                    "low": "99.123456789",
                    "close": "100.223456789",
                    "volume": "1.5",
                    "confirmed": False,
                }
            ],
        }

        with TemporaryDirectory() as temp_dir:
            legacy_path = candle_cache_file_path("BTC-USDT-SWAP", "15m", base_dir=temp_dir)
            legacy_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_path.write_text(json.dumps(payload), encoding="utf-8")

            migrated = load_candle_cache("BTC-USDT-SWAP", "15m", base_dir=temp_dir)
            legacy_path.unlink()
            loaded_from_sqlite = load_candle_cache("BTC-USDT-SWAP", "15m", base_dir=temp_dir)

        self.assertEqual(migrated, loaded_from_sqlite)
        self.assertEqual(migrated[0].close, Decimal("100.223456789"))
        self.assertFalse(migrated[0].confirmed)

    def test_merge_candles_dedupes_and_sorts(self) -> None:
        merged = merge_candles(
            [_build_candle(2, "102"), _build_candle(1, "101")],
            [_build_candle(2, "202"), _build_candle(3, "103")],
        )

        self.assertEqual([candle.ts for candle in merged], [1, 2, 3])
        self.assertEqual(merged[1].close, Decimal("202"))

    def test_history_fetch_uses_cache_and_only_refreshes_latest_page_when_cache_is_enough(self) -> None:
        cached = [_build_candle(index, str(100 + index)) for index in range(1, 1001)]
        latest_page = [_build_okx_row(index, str(200 + index)) for index in range(701, 1001)]
        saved_snapshots: list[list[Candle]] = []
        client = DummyHistoryClient({"__LATEST__": latest_page})

        original_load = okx_client_module.load_candle_cache
        original_save = okx_client_module.save_candle_cache
        okx_client_module.load_candle_cache = lambda inst_id, bar, **kwargs: list(cached)
        okx_client_module.save_candle_cache = (
            lambda inst_id, bar, candles, max_records=None: saved_snapshots.append(list(candles))
        )
        try:
            candles = client.get_candles_history("BTC-USDT-SWAP", "15m", limit=800)
        finally:
            okx_client_module.load_candle_cache = original_load
            okx_client_module.save_candle_cache = original_save

        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("after", client.calls[0])
        self.assertEqual(len(candles), 800)
        self.assertEqual(candles[-1].ts, 1000)
        self.assertTrue(saved_snapshots)
        self.assertEqual(client.last_candle_history_stats["cache_hit_count"], 800)
        self.assertEqual(client.last_candle_history_stats["latest_fetch_count"], 0)
        self.assertEqual(client.last_candle_history_stats["older_fetch_count"], 0)

    def test_history_fetch_zero_limit_downloads_full_history(self) -> None:
        pages = {
            "__LATEST__": [_build_okx_row(index, str(100 + index)) for index in range(701, 1001)],
            "701": [_build_okx_row(index, str(100 + index)) for index in range(401, 701)],
            "401": [_build_okx_row(index, str(100 + index)) for index in range(101, 401)],
            "101": [_build_okx_row(index, str(100 + index)) for index in range(1, 101)],
        }
        saved_snapshots: list[tuple[list[Candle], int | None]] = []
        client = DummyHistoryClient(pages)

        original_load = okx_client_module.load_candle_cache
        original_save = okx_client_module.save_candle_cache
        okx_client_module.load_candle_cache = lambda inst_id, bar, **kwargs: []
        okx_client_module.save_candle_cache = (
            lambda inst_id, bar, candles, max_records=None: saved_snapshots.append((list(candles), max_records))
        )
        try:
            candles = client.get_candles_history("BTC-USDT-SWAP", "15m", limit=0)
        finally:
            okx_client_module.load_candle_cache = original_load
            okx_client_module.save_candle_cache = original_save

        self.assertEqual(len(client.calls), 4)
        self.assertEqual([call.get("after") for call in client.calls], [None, "701", "401", "101"])
        self.assertEqual(len(candles), 1000)
        self.assertEqual(candles[0].ts, 1)
        self.assertEqual(candles[-1].ts, 1000)
        self.assertTrue(saved_snapshots)
        self.assertEqual(len(saved_snapshots[-1][0]), 1000)
        self.assertEqual(saved_snapshots[-1][1], 12000)
        self.assertTrue(client.last_candle_history_stats["full_history"])
        self.assertEqual(client.last_candle_history_stats["requested_count"], 0)
        self.assertEqual(client.last_candle_history_stats["returned_count"], 1000)
        self.assertEqual(client.last_candle_history_stats["latest_fetch_count"], 300)
        self.assertEqual(client.last_candle_history_stats["older_fetch_count"], 700)

    def test_history_fetch_zero_limit_reports_progress_and_saves_checkpoints(self) -> None:
        pages = {
            "__LATEST__": [_build_okx_row(index, str(100 + index)) for index in range(701, 1001)],
            "701": [_build_okx_row(index, str(100 + index)) for index in range(401, 701)],
            "401": [_build_okx_row(index, str(100 + index)) for index in range(101, 401)],
            "101": [_build_okx_row(index, str(100 + index)) for index in range(1, 101)],
        }
        saved_snapshots: list[tuple[list[Candle], int | None]] = []
        progress_records: list[dict[str, object]] = []
        client = DummyHistoryClient(pages)

        original_load = okx_client_module.load_candle_cache
        original_save = okx_client_module.save_candle_cache
        original_interval = okx_client_module.FULL_HISTORY_CHECKPOINT_PAGE_INTERVAL
        okx_client_module.load_candle_cache = lambda inst_id, bar, **kwargs: []
        okx_client_module.save_candle_cache = (
            lambda inst_id, bar, candles, max_records=None: saved_snapshots.append((list(candles), max_records))
        )
        okx_client_module.FULL_HISTORY_CHECKPOINT_PAGE_INTERVAL = 2
        try:
            candles = client.get_candles_history(
                "ETH-USDT-SWAP",
                "5m",
                limit=0,
                progress_callback=lambda payload: progress_records.append(dict(payload)),
            )
        finally:
            okx_client_module.load_candle_cache = original_load
            okx_client_module.save_candle_cache = original_save
            okx_client_module.FULL_HISTORY_CHECKPOINT_PAGE_INTERVAL = original_interval

        self.assertEqual(len(candles), 1000)
        self.assertEqual([record["page_count"] for record in progress_records], [1, 2, 3, 4])
        self.assertEqual(progress_records[-1]["total_count"], 1000)
        self.assertEqual(progress_records[-1]["oldest_ts"], 1)
        self.assertEqual(progress_records[-1]["newest_ts"], 1000)
        self.assertGreaterEqual(len(saved_snapshots), 3)
        self.assertEqual(saved_snapshots[0][1], 12000)
        self.assertEqual(saved_snapshots[-1][1], 12000)

    def test_history_range_merges_local_cache_into_returned_series(self) -> None:
        """区间拉取返回值应合并本地缓存，避免仅 API 子集导致回测缺根。"""
        cached = [
            _build_candle(900_000, "101"),
            _build_candle(2_700_000, "103"),
        ]
        page_after_end = [
            _build_okx_row(2_700_000, "203"),
            _build_okx_row(1_800_000, "202"),
        ]
        client = DummyHistoryClient({"2700001": page_after_end, "1800000": []})
        saved: list[list[Candle]] = []
        original_load = okx_client_module.load_candle_cache
        original_load_range = okx_client_module.load_candle_cache_range
        original_save = okx_client_module.save_candle_cache
        okx_client_module.load_candle_cache = lambda inst_id, bar, **kwargs: list(cached)
        okx_client_module.load_candle_cache_range = (
            lambda inst_id, bar, start_ts, end_ts, limit=None, preload_count=0: list(cached)
        )
        okx_client_module.save_candle_cache = (
            lambda inst_id, bar, candles, max_records=None: saved.append(list(candles))
        )
        try:
            out = client.get_candles_history_range(
                "BTC-USDT-SWAP",
                "15m",
                start_ts=900_000,
                end_ts=2_700_000,
                limit=50,
                preload_count=0,
            )
        finally:
            okx_client_module.load_candle_cache = original_load
            okx_client_module.load_candle_cache_range = original_load_range
            okx_client_module.save_candle_cache = original_save

        self.assertEqual([c.ts for c in out], [900_000, 1_800_000, 2_700_000])
        self.assertTrue(saved)
        merged_ts = {c.ts for c in saved[-1]}
        self.assertEqual(merged_ts, {900_000, 1_800_000, 2_700_000})
