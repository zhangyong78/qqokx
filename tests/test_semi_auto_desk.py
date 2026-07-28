import shutil
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from uuid import uuid4

from okx_quant.semi_auto_desk import (
    SemiAutoDeskSnapshot,
    SemiAutoPoolRecord,
    SemiAutoTaskRecord,
    build_semi_auto_pool_summary,
    load_semi_auto_desk_snapshot,
    save_semi_auto_desk_snapshot,
)


class SemiAutoDeskTest(TestCase):
    def _temp_path(self) -> Path:
        target = Path("tests_artifacts") / uuid4().hex / "semi_auto_desk.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(target.parent, ignore_errors=True))
        return target

    def test_pool_summary_uses_only_matching_pool_realized_pnl(self) -> None:
        pool = SemiAutoPoolRecord(
            pool_id="P001",
            name="主操盘",
            api_name="real",
            initial_capital=Decimal("1000"),
        )
        records = [
            SimpleNamespace(semi_auto_pool_id="P001", net_pnl=Decimal("20"), closed_at=datetime(2026, 7, 28, 9, 0)),
            SimpleNamespace(semi_auto_pool_id="P001", net_pnl=Decimal("-10"), closed_at=datetime(2026, 7, 28, 10, 0)),
            SimpleNamespace(semi_auto_pool_id="P002", net_pnl=Decimal("999"), closed_at=datetime(2026, 7, 28, 11, 0)),
        ]

        summary = build_semi_auto_pool_summary(pool, [], records)

        self.assertEqual(summary.net_pnl, Decimal("10"))
        self.assertEqual(summary.virtual_equity, Decimal("1010"))
        self.assertEqual(summary.win_rate, Decimal("50"))
        self.assertEqual(summary.profit_loss_ratio, Decimal("2"))

    def test_snapshot_round_trip_keeps_task_mode_and_terminal_reason(self) -> None:
        path = self._temp_path()
        snapshot = SemiAutoDeskSnapshot(
            pools=[
                SemiAutoPoolRecord(
                    pool_id="P001",
                    name="主操盘",
                    api_name="real",
                    initial_capital=Decimal("1000"),
                )
            ],
            tasks=[
                SemiAutoTaskRecord(
                    task_id="P001-1",
                    pool_id="P001",
                    template_payload={"strategy_id": "ema"},
                    mode="wait_one",
                    status="completed_closed",
                    ended_reason="止盈",
                )
            ],
        )

        save_semi_auto_desk_snapshot(snapshot, path)
        restored = load_semi_auto_desk_snapshot(path)

        self.assertEqual(restored.tasks[0].mode, "wait_one")
        self.assertEqual(restored.tasks[0].ended_reason, "止盈")
