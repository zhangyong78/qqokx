from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.ai_snapshot import (
    AISnapshotBuilder,
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
        ts = int(datetime(2026, 1, 1, 12, tzinfo=timezone.utc).timestamp() * 1000)
        return [DeribitVolatilityCandle(ts, Decimal("10"), Decimal("10"), Decimal("10"), Decimal("10"))]


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

    def test_indicators_use_warmup_before_last_250_slice(self) -> None:
        candles = [
            Candle(i, Decimal(i), Decimal(i), Decimal(i), Decimal(i), Decimal("1"), True)
            for i in range(300)
        ]
        rows = build_market_rows(candles)
        self.assertEqual(len(rows), 250)
        self.assertNotEqual(rows[0]["ema15"], "50")
        self.assertEqual(rows[-1]["close"], "299")

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
