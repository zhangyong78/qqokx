import shutil
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.persistence import (
    credentials_file_path,
    load_credentials_snapshot,
    load_option_strategies_snapshot,
    load_smart_order_favorites_snapshot,
    load_strategy_history_snapshot,
    option_strategies_file_path,
    save_credentials_snapshot,
    save_option_strategies_snapshot,
    save_smart_order_favorites_snapshot,
    save_strategy_history_snapshot,
    smart_order_favorites_file_path,
    strategy_history_file_path,
)


class PersistenceTest(TestCase):
    def _workspace_temp_dir(self) -> Path:
        temp_dir = Path("tests_artifacts") / uuid4().hex
        temp_dir.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(temp_dir, ignore_errors=True))
        return temp_dir

    def test_save_and_load_credentials_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = credentials_file_path(temp_dir)
        save_credentials_snapshot("ak", "sk", "pp", temp_path)
        snapshot = load_credentials_snapshot(temp_path)
        self.assertEqual(snapshot["api_key"], "ak")
        self.assertEqual(snapshot["secret_key"], "sk")
        self.assertEqual(snapshot["passphrase"], "pp")

    def test_load_returns_empty_values_when_file_missing(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = credentials_file_path(temp_dir)
        snapshot = load_credentials_snapshot(temp_path)
        self.assertEqual(snapshot["api_key"], "")
        self.assertEqual(snapshot["secret_key"], "")
        self.assertEqual(snapshot["passphrase"], "")

    def test_save_and_load_smart_order_favorites_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = smart_order_favorites_file_path(temp_dir)
        save_smart_order_favorites_snapshot(
            [
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
                {"inst_type": "SWAP", "inst_id": "BTC-USDT-SWAP"},
            ],
            temp_path,
        )

        snapshot = load_smart_order_favorites_snapshot(temp_path)

        self.assertEqual(
            snapshot["favorites"],
            [
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
                {"inst_type": "SWAP", "inst_id": "BTC-USDT-SWAP"},
            ],
        )

    def test_smart_order_favorites_snapshot_deduplicates_items(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = smart_order_favorites_file_path(temp_dir)
        save_smart_order_favorites_snapshot(
            [
                {"inst_type": "OPTION", "inst_id": "btc-usd-260410-66000-p"},
                {"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"},
            ],
            temp_path,
        )

        snapshot = load_smart_order_favorites_snapshot(temp_path)

        self.assertEqual(
            snapshot["favorites"],
            [{"inst_type": "OPTION", "inst_id": "BTC-USD-260410-66000-P"}],
        )

    def test_save_and_load_option_strategy_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = option_strategies_file_path(temp_dir)
        save_option_strategies_snapshot(
            [
                {
                    "name": "Bull Call",
                    "option_family": "BTC-USD",
                    "expiry_code": "260626",
                    "bar": "15m",
                    "candle_limit": "600",
                    "chart_display_ccy": "USDT",
                    "combo_chart_mode": "pnl",
                    "formula": "L1 - L2",
                    "legs": [
                        {
                            "alias": "L1",
                            "inst_id": "BTC-USD-260626-100000-C",
                            "side": "buy",
                            "quantity": "1",
                            "premium": "0.01",
                            "delta": "0.42",
                            "gamma": "0.01",
                            "theta": "-0.002",
                            "vega": "0.12",
                            "enabled": True,
                        },
                        {
                            "alias": "L2",
                            "inst_id": "BTC-USD-260626-120000-C",
                            "side": "sell",
                            "quantity": "1",
                            "premium": "0.003",
                            "enabled": True,
                        },
                    ],
                }
            ],
            temp_path,
        )

        snapshot = load_option_strategies_snapshot(temp_path)
        self.assertEqual(len(snapshot["strategies"]), 1)
        record = snapshot["strategies"][0]
        self.assertEqual(record["name"], "Bull Call")
        self.assertEqual(record["candle_limit"], "1000")
        self.assertEqual(record["chart_display_ccy"], "USDT")
        self.assertEqual(record["combo_chart_mode"], "pnl")
        self.assertEqual(record["formula"], "L1 - L2")
        self.assertEqual(record["legs"][0]["inst_id"], "BTC-USD-260626-100000-C")
        self.assertEqual(record["legs"][0]["delta"], "0.42")
        self.assertEqual(record["legs"][0]["theta"], "-0.002")

    def test_load_option_strategy_snapshot_uses_new_defaults_when_fields_missing(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = option_strategies_file_path(temp_dir)
        save_option_strategies_snapshot(
            [
                {
                    "name": "Defaulted",
                    "option_family": "BTC-USD",
                    "expiry_code": "260626",
                    "formula": "L1",
                    "legs": [
                        {
                            "alias": "L1",
                            "inst_id": "BTC-USD-260626-100000-C",
                            "side": "buy",
                            "quantity": "1",
                            "premium": "0.01",
                            "enabled": True,
                        }
                    ],
                }
            ],
            temp_path,
        )

        snapshot = load_option_strategies_snapshot(temp_path)
        record = snapshot["strategies"][0]
        self.assertEqual(record["bar"], "1H")
        self.assertEqual(record["candle_limit"], "1000")
        self.assertEqual(record["chart_display_ccy"], "结算币")

    def test_load_strategy_history_snapshot_returns_empty_when_missing(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = strategy_history_file_path(temp_dir)

        snapshot = load_strategy_history_snapshot(temp_path)

        self.assertEqual(snapshot, {"records": []})

    def test_save_and_load_strategy_history_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = strategy_history_file_path(temp_dir)
        save_strategy_history_snapshot(
            [
                {
                    "record_id": "20260417170951000000-S02",
                    "session_id": "S02",
                    "api_name": "QQzhangyong",
                    "strategy_id": "ema_dynamic_order_short",
                    "strategy_name": "EMA 动态委托-空头",
                    "symbol": "ETH-USDT-SWAP",
                    "direction_label": "只做空",
                    "run_mode_label": "交易并下单",
                    "status": "已停止",
                    "started_at": "2026-04-17T17:09:51",
                    "stopped_at": "2026-04-17T17:19:51",
                    "ended_reason": "用户手动停止",
                    "log_file_path": r"D:\qqokx\logs\strategy_sessions\2026-04-17\20260417_170951_123456__QQzhangyong__S02__EMA.log",
                    "updated_at": "2026-04-17T17:19:51",
                    "config_snapshot": {
                        "inst_id": "ETH-USDT-SWAP",
                        "trade_inst_id": "ETH-USDT-SWAP",
                        "risk_amount": "10",
                        "atr_stop_multiplier": "2",
                        "take_profit_mode": "dynamic",
                    },
                },
                {
                    "record_id": "20260417165044000000-S01",
                    "session_id": "S01",
                    "api_name": "QQzhangyong",
                    "strategy_id": "ema_dynamic_order_long",
                    "strategy_name": "EMA 动态委托-多头",
                    "symbol": "ETH-USDT-SWAP",
                    "direction_label": "只做多",
                    "run_mode_label": "交易并下单",
                    "status": "运行中",
                    "started_at": "2026-04-17T16:50:44",
                    "config_snapshot": {
                        "inst_id": "ETH-USDT-SWAP",
                        "trade_inst_id": "ETH-USDT-SWAP",
                        "risk_amount": "10",
                    },
                },
            ],
            temp_path,
        )

        snapshot = load_strategy_history_snapshot(temp_path)

        self.assertEqual(len(snapshot["records"]), 2)
        self.assertEqual(snapshot["records"][0]["record_id"], "20260417170951000000-S02")
        self.assertEqual(snapshot["records"][0]["ended_reason"], "用户手动停止")
        self.assertTrue(str(snapshot["records"][0]["log_file_path"]).endswith("__S02__EMA.log"))
        self.assertEqual(snapshot["records"][1]["record_id"], "20260417165044000000-S01")
        self.assertEqual(snapshot["records"][1]["status"], "运行中")

    def test_load_strategy_history_snapshot_skips_invalid_records_and_defaults_optional_fields(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = strategy_history_file_path(temp_dir)
        temp_path.write_text(
            """
{
  "records": [
    {
      "record_id": "valid-1",
      "strategy_name": "EMA 动态委托-多头",
      "started_at": "2026-04-17T16:50:44",
      "config_snapshot": "bad"
    },
    {
      "record_id": "",
      "strategy_name": "bad",
      "started_at": "2026-04-17T16:50:44"
    }
  ]
}
            """.strip(),
            encoding="utf-8",
        )

        snapshot = load_strategy_history_snapshot(temp_path)

        self.assertEqual(len(snapshot["records"]), 1)
        self.assertEqual(snapshot["records"][0]["record_id"], "valid-1")
        self.assertEqual(snapshot["records"][0]["status"], "已停止")
        self.assertEqual(snapshot["records"][0]["config_snapshot"], {})
