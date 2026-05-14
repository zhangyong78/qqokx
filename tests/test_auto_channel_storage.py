from decimal import Decimal
from tempfile import TemporaryDirectory
from pathlib import Path
from unittest import TestCase

from okx_quant.auto_channel_storage import (
    build_auto_channel_snapshot_record,
    deserialize_strategy_live_chart_snapshot,
    load_auto_channel_snapshots,
    save_auto_channel_snapshots,
)
from okx_quant.models import Candle
from okx_quant.strategy_live_chart import StrategyLiveChartLineOverlay, StrategyLiveChartSnapshot
from okx_quant.analysis.structure_models import PriceLine


class AutoChannelStorageTest(TestCase):
    def test_save_and_load_snapshot_roundtrip(self) -> None:
        snapshot = StrategyLiveChartSnapshot(
            session_id="saved",
            candles=(
                Candle(ts=1, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("11"), volume=Decimal("1"), confirmed=True),
                Candle(ts=2, open=Decimal("11"), high=Decimal("13"), low=Decimal("10"), close=Decimal("12"), volume=Decimal("1"), confirmed=True),
            ),
            line_overlays=(
                StrategyLiveChartLineOverlay(
                    key="line",
                    label="line",
                    line=PriceLine(0, Decimal("10"), 12, Decimal("18")),
                    color="#2563eb",
                ),
            ),
            note="saved note",
            right_pad_bars=50,
        )
        record = build_auto_channel_snapshot_record(
            snapshot=snapshot,
            source_mode="real",
            symbol="BTC-USDT-SWAP",
            bar="1H",
            label="btc-test",
            api_profile="api1",
            candle_limit=240,
        )

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "auto_channel_snapshots.json"
            save_auto_channel_snapshots([record], path)
            loaded = load_auto_channel_snapshots(path)

        self.assertEqual(len(loaded), 1)
        loaded_record = loaded[0]
        self.assertEqual(loaded_record["label"], "btc-test")
        restored = deserialize_strategy_live_chart_snapshot(loaded_record["snapshot"])  # type: ignore[arg-type]
        self.assertEqual(len(restored.candles), 2)
        self.assertEqual(restored.line_overlays[0].line.end_index, 12)
        self.assertEqual(restored.right_pad_bars, 50)
        self.assertEqual(restored.note, "saved note")
