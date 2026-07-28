from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.semi_auto_desk import SemiAutoTaskRecord
from okx_quant.semi_auto_desk_ui import (
    build_semi_auto_pool_ledger_rows,
    build_semi_auto_task_rows,
)


class SemiAutoDeskUiTest(TestCase):
    def test_pool_ledger_rows_mix_strategies_but_keep_one_pool_only(self) -> None:
        records = [
            SimpleNamespace(
                record_id="L1",
                semi_auto_pool_id="P001",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                opened_at=datetime(2026, 7, 28, 9, 0),
                closed_at=datetime(2026, 7, 28, 10, 0),
                entry_price=Decimal("3600"),
                exit_price=Decimal("3610"),
                net_pnl=Decimal("2"),
                close_reason="止盈",
            ),
            SimpleNamespace(
                record_id="L2",
                semi_auto_pool_id="P001",
                strategy_name="EMA55 斜率做空",
                symbol="BTC-USDT-SWAP",
                direction_label="只做空",
                opened_at=datetime(2026, 7, 28, 11, 0),
                closed_at=datetime(2026, 7, 28, 12, 0),
                entry_price=Decimal("68000"),
                exit_price=Decimal("67900"),
                net_pnl=Decimal("-1"),
                close_reason="止损",
            ),
            SimpleNamespace(record_id="L3", semi_auto_pool_id="P002"),
        ]

        rows = build_semi_auto_pool_ledger_rows("P001", records)

        self.assertEqual([values[1] for _, values in rows], ["EMA55 斜率做空", "EMA 动态委托做多"])
        self.assertEqual([values[7] for _, values in rows], ["-1.00", "+2.00"])

    def test_task_rows_show_one_shot_mode_and_terminal_reason(self) -> None:
        task = SemiAutoTaskRecord(
            task_id="P001-1",
            pool_id="P001",
            template_payload={"strategy_name": "EMA 动态委托做多"},
            symbol="ETH-USDT-SWAP",
            direction_label="只做多",
            bar="1H",
            mode="wait_one",
            status="completed_closed",
            ended_reason="止盈",
        )

        rows = build_semi_auto_task_rows([task])

        self.assertEqual(rows[0][1][4], "等待一单")
        self.assertEqual(rows[0][1][-1], "止盈")
