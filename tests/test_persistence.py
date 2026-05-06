import shutil
from pathlib import Path
from unittest import TestCase
from uuid import uuid4

from okx_quant.persistence import (
    btc_research_workbench_state_file_path,
    credentials_file_path,
    history_cache_dir_path,
    history_cache_file_path,
    load_btc_research_workbench_state,
    load_history_cache_records,
    load_position_history_view_prefs,
    load_credentials_snapshot,
    load_option_strategies_snapshot,
    load_strategy_parameter_drafts,
    load_smart_order_favorites_snapshot,
    load_strategy_history_snapshot,
    load_strategy_trade_ledger_snapshot,
    option_strategies_file_path,
    save_btc_research_workbench_state,
    save_credentials_snapshot,
    save_history_cache_records,
    save_position_history_view_prefs,
    save_option_strategies_snapshot,
    save_smart_order_favorites_snapshot,
    save_strategy_parameter_drafts,
    save_strategy_history_snapshot,
    save_strategy_trade_ledger_snapshot,
    smart_order_favorites_file_path,
    strategy_parameter_drafts_file_path,
    strategy_history_file_path,
    strategy_trade_ledger_file_path,
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

    def test_save_and_load_btc_research_workbench_state(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = btc_research_workbench_state_file_path(temp_dir)
        save_btc_research_workbench_state(
            {
                "drawings": {
                    "BTC-USDT-SWAP|4H": [
                        {
                            "tool": "trend_line",
                            "start_index": 12,
                            "end_index": 24,
                            "price_a": 62500.0,
                            "price_b": 63880.0,
                        }
                    ]
                },
                "viewports": {
                    "BTC-USDT-SWAP|4H": {"start_index": 180, "visible_count": 220}
                },
            },
            temp_path,
        )

        snapshot = load_btc_research_workbench_state(temp_path)

        self.assertEqual(snapshot["drawings"]["BTC-USDT-SWAP|4H"][0]["tool"], "trend_line")
        self.assertEqual(snapshot["viewports"]["BTC-USDT-SWAP|4H"]["start_index"], 180)

    def test_load_btc_research_workbench_state_returns_defaults_when_missing(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = btc_research_workbench_state_file_path(temp_dir)

        snapshot = load_btc_research_workbench_state(temp_path)

        self.assertEqual(snapshot, {"drawings": {}, "viewports": {}})

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

    def test_save_and_load_strategy_parameter_drafts(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = strategy_parameter_drafts_file_path(temp_dir)
        save_strategy_parameter_drafts(
            {
                "launcher": {
                    "ema_breakout_long": {
                        "bar": "1H",
                        "ema_period": "21",
                        "trend_ema_period": "55",
                    }
                },
                "backtest": {
                    "ema_dynamic_order_short": {
                        "bar": "4小时",
                        "entry_reference_ema_period": "21",
                    }
                },
            },
            temp_path,
        )

        snapshot = load_strategy_parameter_drafts(temp_path)

        self.assertEqual(snapshot["launcher"]["ema_breakout_long"]["bar"], "1H")
        self.assertEqual(snapshot["launcher"]["ema_breakout_long"]["ema_period"], "21")
        self.assertEqual(snapshot["backtest"]["ema_dynamic_order_short"]["entry_reference_ema_period"], "21")
        self.assertEqual(snapshot["observer"], {})

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

    def test_save_and_load_strategy_trade_ledger_snapshot(self) -> None:
        temp_dir = self._workspace_temp_dir()
        temp_path = strategy_trade_ledger_file_path(temp_dir)
        save_strategy_trade_ledger_snapshot(
            [
                {
                    "record_id": "20260423170920000000-S01",
                    "history_record_id": "20260423081513000000-S01",
                    "session_id": "S01",
                    "api_name": "QQzhangyong",
                    "strategy_id": "ema_dynamic_order_long",
                    "strategy_name": "EMA 动态委托-多头",
                    "symbol": "ETH-USDT-SWAP",
                    "direction_label": "只做多",
                    "run_mode_label": "交易并下单",
                    "environment": "demo",
                    "signal_bar_at": "2026-04-23T08:00:00",
                    "opened_at": "2026-04-23T08:15:13",
                    "closed_at": "2026-04-23T17:09:20",
                    "entry_order_id": "1001",
                    "entry_client_order_id": "s01emaent042300000897343",
                    "exit_order_id": "2001",
                    "protective_algo_id": "3001",
                    "protective_algo_cl_ord_id": "s01emaslg042300000897344",
                    "entry_price": "2358.42",
                    "exit_price": "2320.66",
                    "size": "0.1",
                    "entry_fee": "-0.04",
                    "exit_fee": "-0.04",
                    "funding_fee": "-0.01",
                    "gross_pnl": "-3.78",
                    "net_pnl": "-3.87",
                    "close_reason": "OKX止损触发",
                    "reason_confidence": "high",
                    "summary_note": "demo snapshot",
                    "updated_at": "2026-04-23T17:09:21",
                }
            ],
            temp_path,
        )

        snapshot = load_strategy_trade_ledger_snapshot(temp_path)

        self.assertEqual(len(snapshot["records"]), 1)
        record = snapshot["records"][0]
        self.assertEqual(record["record_id"], "20260423170920000000-S01")
        self.assertEqual(record["close_reason"], "OKX止损触发")
        self.assertEqual(record["net_pnl"], "-3.87")

    def test_history_cache_path_isolated_by_profile_and_environment(self) -> None:
        temp_dir = self._workspace_temp_dir()
        cache_dir = history_cache_dir_path("api2", "live", base_dir=temp_dir)
        cache_file = history_cache_file_path("fills", "api2", "live", base_dir=temp_dir)
        self.assertTrue(str(cache_dir).endswith(str(Path("history") / "api2" / "live")))
        self.assertTrue(str(cache_file).endswith(str(Path("history") / "api2" / "live" / "fills_history.json")))

    def test_save_and_load_history_cache_records(self) -> None:
        temp_dir = self._workspace_temp_dir()
        save_history_cache_records(
            "orders",
            "api3",
            "demo",
            [{"order_id": "1001", "inst_id": "BTC-USDT-SWAP"}],
            base_dir=temp_dir,
        )
        records = load_history_cache_records("orders", "api3", "demo", base_dir=temp_dir)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["order_id"], "1001")

    def test_save_and_load_position_history_view_prefs(self) -> None:
        temp_dir = self._workspace_temp_dir()
        target = temp_dir / "position_history_view_prefs.json"
        save_position_history_view_prefs(
            local_range_start="2025-01-01",
            local_range_end="2025-12-31",
            path=target,
        )
        loaded = load_position_history_view_prefs(path=target)
        self.assertEqual(loaded["local_range_start"], "2025-01-01")
        self.assertEqual(loaded["local_range_end"], "2025-12-31")
