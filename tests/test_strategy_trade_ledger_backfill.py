from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from okx_quant.strategy_trade_ledger_backfill import backfill_strategy_trade_ledger, parse_trade_rounds_for_history_record


class StrategyTradeLedgerBackfillTest(unittest.TestCase):
    def test_parse_trade_rounds_collects_events_across_multiple_session_logs(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            state_dir.mkdir(parents=True)
            logs_dir = root / "logs" / "strategy_sessions" / "2026-06-16"
            logs_dir.mkdir(parents=True)
            first_log = logs_dir / "20260616_223929_199293__demoapi__S222__session__SOL-USDT-SWAP.log"
            first_log.write_text(
                "\n".join(
                    [
                        "[06-16 22:40:31] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 2026-06-16 21:00:00 | 准备本地下单 | 信号方向=SHORT",
                        "[06-16 22:41:04] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 本地下单成交 | ordId=E1 | 标的=SOL-USDT-SWAP | 方向=SELL | 成交均价=72.98 | 成交数量=3.84张（折合3.84 SOL）",
                        "[06-16 22:41:05] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 委托追踪 | clOrdId=clE1 | ordId=E1",
                    ]
                ),
                encoding="utf-8",
            )
            second_log = logs_dir / "20260616_223929_200000__demoapi__S222__session__SOL-USDT-SWAP.log"
            second_log.write_text(
                "\n".join(
                    [
                        "[06-17 03:00:16] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 2026-06-17 02:00:00 | MA20 斜率转正平仓 | 斜率比例=0.000156",
                        "[06-17 03:01:08] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 本地斜率转正平仓已成交 | ordId=X1 | 标的=SOL-USDT-SWAP | 方向=BUY | 成交均价=73.82 | 成交数量=3.84张（折合3.84 SOL） | 剩余=0张（折合0 SOL）",
                    ]
                ),
                encoding="utf-8",
            )
            history_record = {
                "record_id": "H1",
                "session_id": "S222",
                "api_name": "demoapi",
                "strategy_id": "ema55_slope_short",
                "strategy_name": "均线斜率做空",
                "symbol": "SOL-USDT-SWAP",
                "direction_label": "只做空",
                "run_mode_label": "交易并下单",
                "status": "已停止",
                "started_at": "2026-06-16T22:39:29",
                "log_file_path": str(second_log),
                "config_snapshot": {"environment": "demo", "signal_mode": "short_only"},
            }

            rounds = parse_trade_rounds_for_history_record(history_record, state_dir=state_dir)

            self.assertEqual(len(rounds), 1)
            self.assertEqual(rounds[0].entry_order_id, "E1")
            self.assertEqual(rounds[0].entry_client_order_id, "clE1")
            self.assertEqual(rounds[0].exit_order_id, "X1")
            self.assertEqual(rounds[0].close_reason, "斜率转正平仓")

    def test_backfill_strategy_trade_ledger_adds_missing_records_and_updates_history(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state_dir = root / "state"
            history_dir = state_dir / "history" / "demoapi" / "demo"
            logs_dir = root / "logs" / "strategy_sessions" / "2026-06-16"
            history_dir.mkdir(parents=True)
            logs_dir.mkdir(parents=True)

            (state_dir / "strategy_trade_ledger.json").write_text(json.dumps({"records": []}, ensure_ascii=False), encoding="utf-8")
            (state_dir / "strategy_history.json").write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "record_id": "H1",
                                "session_id": "S222",
                                "api_name": "demoapi",
                                "strategy_id": "ema55_slope_short",
                                "strategy_name": "均线斜率做空",
                                "symbol": "SOL-USDT-SWAP",
                                "direction_label": "只做空",
                                "run_mode_label": "交易并下单",
                                "status": "已停止",
                                "started_at": "2026-06-16T22:39:29",
                                "log_file_path": str(logs_dir / "20260616_223929_200000__demoapi__S222__session__SOL-USDT-SWAP.log"),
                                "config_snapshot": {"environment": "demo", "signal_mode": "short_only"},
                                "trade_count": 0,
                                "win_count": 0,
                                "gross_pnl_total": "0",
                                "fee_total": "0",
                                "funding_total": "0",
                                "net_pnl_total": "0",
                                "last_close_reason": "",
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (logs_dir / "20260616_223929_199293__demoapi__S222__session__SOL-USDT-SWAP.log").write_text(
                "\n".join(
                    [
                        "[06-16 22:40:31] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 2026-06-16 21:00:00 | 准备本地下单 | 信号方向=SHORT",
                        "[06-16 22:41:04] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 本地下单成交 | ordId=E1 | 标的=SOL-USDT-SWAP | 方向=SELL | 成交均价=72.98 | 成交数量=3.84张（折合3.84 SOL）",
                        "[06-16 22:41:05] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 委托追踪 | clOrdId=clE1 | ordId=E1",
                    ]
                ),
                encoding="utf-8",
            )
            (logs_dir / "20260616_223929_200000__demoapi__S222__session__SOL-USDT-SWAP.log").write_text(
                "\n".join(
                    [
                        "[06-17 03:00:16] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 2026-06-17 02:00:00 | MA20 斜率转正平仓 | 斜率比例=0.000156",
                        "[06-17 03:01:08] [demoapi] [S222 均线斜率做空 SOL-USDT-SWAP] 本地斜率转正平仓已成交 | ordId=X1 | 标的=SOL-USDT-SWAP | 方向=BUY | 成交均价=73.82 | 成交数量=3.84张（折合3.84 SOL） | 剩余=0张（折合0 SOL）",
                    ]
                ),
                encoding="utf-8",
            )
            (history_dir / "fills_history.json").write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "api_name": "demoapi",
                                "order_id": "E1",
                                "inst_id": "SOL-USDT-SWAP",
                                "fill_price": "72.98",
                                "fill_size": "3.84",
                                "fill_fee": "-0.100887552",
                                "fill_time": 1781620854223,
                            },
                            {
                                "api_name": "demoapi",
                                "order_id": "X1",
                                "inst_id": "SOL-USDT-SWAP",
                                "fill_price": "73.82",
                                "fill_size": "3.84",
                                "fill_fee": "-0.102048768",
                                "fill_time": 1781636448428,
                            },
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (history_dir / "order_history.json").write_text(
                json.dumps(
                    {
                        "records": [
                            {
                                "api_name": "demoapi",
                                "order_id": "E1",
                                "client_order_id": "clE1",
                                "avg_price": "72.98",
                                "filled_size": "3.84",
                                "created_time": 1781620854198,
                            },
                            {
                                "api_name": "demoapi",
                                "order_id": "X1",
                                "avg_price": "73.82",
                                "filled_size": "3.84",
                                "created_time": 1781636448403,
                            },
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = backfill_strategy_trade_ledger(state_dir=state_dir, write=True)

            self.assertEqual(result.added_record_count, 1)
            self.assertEqual(result.updated_history_count, 1)
            ledger_payload = json.loads((state_dir / "strategy_trade_ledger.json").read_text(encoding="utf-8"))
            self.assertEqual(len(ledger_payload["records"]), 1)
            ledger_record = ledger_payload["records"][0]
            self.assertEqual(ledger_record["close_reason"], "斜率转正平仓")
            self.assertEqual(ledger_record["entry_order_id"], "E1")
            self.assertEqual(ledger_record["entry_client_order_id"], "clE1")
            self.assertEqual(ledger_record["exit_order_id"], "X1")
            self.assertEqual(ledger_record["gross_pnl"], "-3.2256")
            self.assertEqual(ledger_record["net_pnl"], "-3.428536320")

            history_payload = json.loads((state_dir / "strategy_history.json").read_text(encoding="utf-8"))
            history_record = history_payload["records"][0]
            self.assertEqual(history_record["trade_count"], 1)
            self.assertEqual(history_record["win_count"], 0)
            self.assertEqual(history_record["last_close_reason"], "斜率转正平仓")
            self.assertEqual(history_record["net_pnl_total"], "-3.428536320")

            repeat = backfill_strategy_trade_ledger(state_dir=state_dir, write=False)
            self.assertEqual(repeat.added_record_count, 0)


if __name__ == "__main__":
    unittest.main()
