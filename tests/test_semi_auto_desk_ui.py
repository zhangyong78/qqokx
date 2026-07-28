from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from okx_quant.semi_auto_desk import SemiAutoTaskRecord
from okx_quant.semi_auto_desk_ui import (
    build_semi_auto_pool_performance_rows,
    build_semi_auto_pool_replay_time_markers,
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

    def test_pool_replay_markers_overlay_all_strategies_for_one_symbol(self) -> None:
        records = [
            SimpleNamespace(
                record_id="L1",
                semi_auto_pool_id="P001",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                opened_at=datetime(2026, 7, 17, 20, 0),
                closed_at=datetime(2026, 7, 17, 21, 0),
                entry_price=Decimal("1859.28"),
                exit_price=Decimal("1860.50"),
                net_pnl=Decimal("-0.2767526"),
            ),
            SimpleNamespace(
                record_id="L2",
                semi_auto_pool_id="P001",
                strategy_name="EMA55 斜率做空",
                symbol="ETH-USDT-SWAP",
                direction_label="只做空",
                opened_at=datetime(2026, 7, 18, 8, 0),
                closed_at=datetime(2026, 7, 18, 9, 0),
                entry_price=Decimal("1870"),
                exit_price=Decimal("1860"),
                net_pnl=Decimal("2"),
            ),
            SimpleNamespace(record_id="L3", semi_auto_pool_id="P001", symbol="BTC-USDT-SWAP"),
            SimpleNamespace(record_id="L4", semi_auto_pool_id="P002", symbol="ETH-USDT-SWAP"),
        ]

        markers = build_semi_auto_pool_replay_time_markers("P001", "ETH-USDT-SWAP", records)

        self.assertEqual([marker.key for marker in markers], ["open:L1", "close:L1", "open:L2", "close:L2"])
        self.assertIn("开仓 EMA 动态委托做多", markers[0].label)
        self.assertIn("平仓 EMA 动态委托做多", markers[1].label)
        self.assertIn("价格=1860.50", markers[1].label)
        self.assertIn("本次盈亏=-0.28 USDT", markers[1].label)
        self.assertEqual(markers[0].vertical_anchor, "below")
        self.assertEqual(markers[1].vertical_anchor, "above")

    def test_pool_performance_rows_group_by_strategy_symbol_and_direction(self) -> None:
        records = [
            SimpleNamespace(
                semi_auto_pool_id="P001", strategy_name="EMA 动态委托做多", symbol="ETH-USDT-SWAP", direction_label="只做多", net_pnl=Decimal("3")
            ),
            SimpleNamespace(
                semi_auto_pool_id="P001", strategy_name="EMA 动态委托做多", symbol="ETH-USDT-SWAP", direction_label="只做多", net_pnl=Decimal("-1")
            ),
            SimpleNamespace(
                semi_auto_pool_id="P001", strategy_name="EMA55 斜率做空", symbol="BTC-USDT-SWAP", direction_label="只做空", net_pnl=Decimal("2")
            ),
            SimpleNamespace(semi_auto_pool_id="P002", net_pnl=Decimal("999")),
        ]

        rows = build_semi_auto_pool_performance_rows("P001", records)

        self.assertEqual(len(rows), 2)
        long_row = next(values for _, values in rows if values[0] == "EMA 动态委托做多")
        self.assertEqual(long_row[3], "2")
        self.assertEqual(long_row[4], "50.00%")
        self.assertEqual(long_row[5], "+2.00")
        self.assertEqual(long_row[6], "3.00")
