from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import TestCase

from okx_quant.daily_filters import (
    aggregate_candles_to_daily_boundary,
    build_daily_close_vs_ma_bias,
    build_daily_weak_day_flags,
)
from okx_quant.models import Candle, StrategyConfig
from okx_quant.strategy_profiles import (
    STRATEGY_PROFILE_SCHEMA_VERSION,
    StrategyBundle,
    build_strategy_profile_from_config,
    read_strategy_bundle,
    write_strategy_bundle,
)


def _candle(ts: int, open_: str, close: str) -> Candle:
    high = str(max(Decimal(open_), Decimal(close)))
    low = str(min(Decimal(open_), Decimal(close)))
    return Candle(ts, Decimal(open_), Decimal(high), Decimal(low), Decimal(close), Decimal("1"), True)


class StrategyProfilesTest(TestCase):
    def test_aggregate_candles_to_daily_boundary_supports_bjt_00_and_bjt_08(self) -> None:
        hourly = [_candle(index * 3_600_000, "100", str(100 + index)) for index in range(30)]

        bjt_08, bjt_08_audit = aggregate_candles_to_daily_boundary(hourly, boundary="bjt_08")
        bjt_00, bjt_00_audit = aggregate_candles_to_daily_boundary(hourly, boundary="bjt_00")

        self.assertEqual(len(bjt_08), 2)
        self.assertEqual(len(bjt_00), 2)
        self.assertEqual(bjt_08_audit[0].hours_in_bucket, 24)
        self.assertEqual(bjt_08_audit[1].hours_in_bucket, 6)
        self.assertEqual(bjt_00_audit[0].hours_in_bucket, 16)
        self.assertEqual(bjt_00_audit[1].hours_in_bucket, 14)

    def test_daily_filter_helpers_use_latest_closed_daily_candle(self) -> None:
        daily = [
            _candle(0, "100", "110"),
            _candle(86_400_000, "110", "100"),
            _candle(172_800_000, "100", "120"),
        ]
        entries = [
            _candle(1_000, "100", "100"),
            _candle(86_400_000 + 1_000, "100", "100"),
            _candle((2 * 86_400_000) + 1_000, "100", "100"),
            _candle((3 * 86_400_000) + 1_000, "100", "100"),
        ]

        bias = build_daily_close_vs_ma_bias(entries, daily, ma_type="ma", period=2)
        weak = build_daily_weak_day_flags(entries, daily)

        self.assertEqual(bias[0], "neutral")
        self.assertEqual(bias[1], "neutral")
        self.assertEqual(bias[2], "short")
        self.assertEqual(bias[3], "long")
        self.assertEqual(weak, [False, False, True, False])

    def test_strategy_bundle_roundtrip_preserves_daily_filter_spec(self) -> None:
        config = StrategyConfig(
            inst_id="BTC-USDT-SWAP",
            bar="1H",
            ema_period=21,
            trend_ema_period=55,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="net",
            environment="demo",
            tp_sl_trigger_type="mark",
            strategy_id="ema_dynamic_order_long",
            risk_amount=Decimal("10"),
            daily_filter_inst_id="BTC-USDT-SWAP",
            daily_filter_bar="1D",
            daily_filter_boundary="bjt_08",
            daily_filter_enabled=True,
            daily_filter_mode="close_vs_ma",
            daily_filter_scope="long_only",
            daily_filter_ma_type="ema",
            daily_filter_period=5,
        )
        profile = build_strategy_profile_from_config(
            profile_id="btc-long-ema5",
            profile_name="BTC 多头 EMA5",
            strategy_id=config.strategy_id,
            symbol=config.inst_id,
            config=config,
            api_name="moni",
            direction_label="只做多",
            run_mode_label="交易并下单",
            source_report="report.html",
        )
        bundle = StrategyBundle(
            bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
            bundle_name="五币日线过滤组合",
            profiles=(profile,),
            created_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            source_report="report.html",
            auto_start_on_import=False,
        )

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bundle.json"
            write_strategy_bundle(bundle, path)
            restored = read_strategy_bundle(path)

        self.assertEqual(restored.bundle_name, bundle.bundle_name)
        self.assertEqual(len(restored.profiles), 1)
        restored_profile = restored.profiles[0]
        self.assertEqual(restored_profile.profile_id, "btc-long-ema5")
        self.assertTrue(restored_profile.daily_filter.enabled)
        self.assertEqual(restored_profile.daily_filter.boundary, "bjt_08")
        self.assertEqual(restored_profile.daily_filter.mode, "close_vs_ma")
        self.assertEqual(restored_profile.daily_filter.scope, "long_only")
        self.assertEqual(restored_profile.config_snapshot["daily_filter_period"], 5)

    def test_read_strategy_bundle_accepts_utf8_bom(self) -> None:
        payload = {
            "bundle_version": STRATEGY_PROFILE_SCHEMA_VERSION,
            "bundle_name": "UTF8 BOM Bundle",
            "profiles": [
                {
                    "profile_id": "doge-long",
                    "profile_name": "DOGE Long",
                    "strategy_id": "ema_dynamic_order_long",
                    "symbol": "DOGE-USDT-SWAP",
                    "api_name": "moni",
                    "direction_label": "只做多",
                    "run_mode_label": "交易并下单",
                    "enabled": True,
                    "daily_filter": {},
                    "config_snapshot": {
                        "inst_id": "DOGE-USDT-SWAP",
                        "bar": "1H",
                        "strategy_id": "ema_dynamic_order_long",
                    },
                    "tags": [],
                    "notes": "",
                    "created_at": "",
                    "source_report": "",
                }
            ],
        }

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "bundle.json"
            path.write_text(__import__("json").dumps(payload, ensure_ascii=False), encoding="utf-8-sig")
            restored = read_strategy_bundle(path)

        self.assertEqual(restored.bundle_name, "UTF8 BOM Bundle")
        self.assertEqual(restored.profiles[0].symbol, "DOGE-USDT-SWAP")
