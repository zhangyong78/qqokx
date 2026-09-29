from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.ai_snapshot import (
    AIQuickSnapshotBuilder,
    AISnapshotBuilder,
    _quick_option_contract_fields,
    build_market_rows,
    load_ai_watchlist,
    merge_asset_bases,
    normalize_watch_symbol,
    save_ai_watchlist,
)
from okx_quant.deribit_client import DeribitVolatilityCandle
from okx_quant.models import Candle, Credentials
from okx_quant.okx_client import OkxPosition


def _position(inst_id: str = "BTC-USDT-SWAP") -> OkxPosition:
    return OkxPosition(
        inst_id=inst_id,
        inst_type="SWAP",
        pos_side="net",
        mgn_mode="cross",
        position=Decimal("2"),
        avail_position=Decimal("2"),
        avg_price=Decimal("100"),
        mark_price=Decimal("110"),
        unrealized_pnl=Decimal("20"),
        unrealized_pnl_ratio=Decimal("0.1"),
        liquidation_price=None,
        leverage=Decimal("3"),
        margin_ccy="USDT",
        last_price=Decimal("110"),
        realized_pnl=Decimal("1"),
        margin_ratio=None,
        initial_margin=Decimal("10"),
        maintenance_margin=Decimal("5"),
        delta=Decimal("2"),
        gamma=None,
        vega=None,
        theta=None,
        raw={},
    )


class _FakeVolatility:
    def get_volatility_index_candles(self, *_args, **_kwargs):
        end_ts = int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
        return [
            DeribitVolatilityCandle(
                end_ts - (8000 - index) * 60 * 60 * 1000,
                Decimal("10"),
                Decimal("10"),
                Decimal("10"),
                Decimal("10"),
            )
            for index in range(8000)
        ]


class _FakeClient:
    def get_positions(self, _credentials, *, environment, inst_type, prefer_cache):
        return [_position()] if inst_type == "SWAP" else []

    def get_candles_history(self, _inst_id, _bar, limit=1200):
        return [
            Candle(
                ts=index * 60_000,
                open=Decimal(index),
                high=Decimal(index + 1),
                low=Decimal(index - 1),
                close=Decimal(index),
                volume=Decimal("1"),
                confirmed=True,
            )
            for index in range(300)
        ]

    def get_candles(self, _inst_id, _bar, limit=3):
        ts = int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
        return [Candle(ts, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), False)]

    def get_fills_history(self, *_args, **_kwargs):
        return []


class AISnapshotTest(TestCase):
    def test_watchlist_normalization_and_union(self) -> None:
        self.assertEqual(normalize_watch_symbol("btc-usdt-swap"), "BTC")
        self.assertEqual(merge_asset_bases(["ETH-USD-240927", "BTC-USDT-SWAP"], ["sol", "BTC"]), ["BTC", "ETH", "SOL"])

    def test_watchlist_is_profile_scoped(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "watch.json"
            save_ai_watchlist("159", ["btc", "eth"], path=path)
            save_ai_watchlist("api2", ["sol"], path=path)
            self.assertEqual(load_ai_watchlist("159", path=path), ["BTC", "ETH"])
            self.assertEqual(load_ai_watchlist("api2", path=path), ["SOL"])

    def test_quick_option_contract_fields_are_recovered_from_instrument_id(self) -> None:
        self.assertEqual(
            _quick_option_contract_fields("BTC-USD-270625-105000-C"),
            {"option_type": "call", "expiry": "270625", "strike": 105000},
        )

    def test_indicators_use_warmup_before_last_250_slice(self) -> None:
        candles = [
            Candle(i, Decimal(i), Decimal(i), Decimal(i), Decimal(i), Decimal("1"), True)
            for i in range(300)
        ]
        rows = build_market_rows(candles)
        self.assertEqual(len(rows), 250)
        self.assertNotEqual(rows[0]["ema15"], "50")
        self.assertEqual(rows[-1]["close"], "299")

    def test_unclosed_bar_has_progress_and_closed_bar_views(self) -> None:
        start_ms = int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
        candles = [
            Candle(start_ms, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), True),
            Candle(start_ms + 60 * 60 * 1000, Decimal("2"), Decimal("2"), Decimal("2"), Decimal("2"), Decimal("1"), False),
        ]
        rows = build_market_rows(
            candles,
            limit=250,
            interval="1H",
            snapshot_time_ms=start_ms + 60 * 60 * 1000 + 50 * 60 * 1000,
            fetched_at_ms=start_ms + 60 * 60 * 1000 + 46 * 60 * 1000,
        )
        current = rows[-1]
        self.assertEqual(current["bar_status"], "NEAR_CLOSE")
        self.assertEqual(current["reference_level"], "HIGH")
        self.assertEqual(current["bar_progress_pct"], 83.3)
        self.assertEqual(current["minutes_to_close"], 10.0)
        self.assertEqual(current["market_fetch_age_seconds"], 240)
        self.assertTrue(current["indicator_is_provisional"])

    def test_builder_exports_profile_separated_snapshot_without_spot(self) -> None:
        with TemporaryDirectory() as directory:
            payload = AISnapshotBuilder(
                client=_FakeClient(),
                volatility_client=_FakeVolatility(),
                now=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            ).build(
                credentials=Credentials("key", "secret", "pass", profile_name="159"),
                profile_name="159",
                environment="live",
                watchlist=["ETH"],
                output_root=Path(directory),
            )
            self.assertFalse(payload["account"]["spot_included"])
            self.assertEqual(sorted(payload["assets"]), ["BTC", "ETH"])
            self.assertEqual(payload["portfolio_summary"]["position_count"], 1)
            self.assertTrue(Path(payload["file_path"]).exists())

    def test_quick_snapshot_is_symbol_scoped_and_compact(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            kwargs = {
                "credentials": Credentials("key", "secret", "pass", profile_name="159"),
                "profile_name": "159",
                "environment": "live",
                "now": datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
            }
            full = AISnapshotBuilder(client=_FakeClient(), volatility_client=_FakeVolatility(), now=kwargs["now"]).build(
                credentials=kwargs["credentials"], profile_name="159", environment="live", watchlist=["BTC"], output_root=root / "full"
            )
            quick = AIQuickSnapshotBuilder(client=_FakeClient(), volatility_client=_FakeVolatility(), now=kwargs["now"]).build(
                credentials=kwargs["credentials"], profile_name="159", environment="live", symbols=["BTC"], output_root=root / "quick"
            )
            self.assertEqual(quick["schema"], "qqokx_ai_quick_snapshot")
            self.assertEqual(quick["symbols"], ["BTC"])
            self.assertNotIn("ETH", quick)
            self.assertEqual(len(quick["BTC"]["market"]["1H"]["bars"]), 120)
            self.assertEqual(len(quick["BTC"]["market"]["1W"]["bars"]), 40)
            self.assertEqual(len(quick["BTC"]["volatility"]["1D"]["bars"]), 60)
            self.assertLess(Path(quick["file_path"]).stat().st_size, Path(full["file_path"]).stat().st_size)
