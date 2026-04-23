from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.enhanced_backtest import EnhancedBacktestLab, export_lab_report_markdown
from okx_quant.enhanced_models import QuotaSnapshot
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_seed_strategies import register_seed_strategy_package
from okx_quant.models import Candle
from okx_quant.persistence import analysis_report_dir_path


def build_demo_candles(closes: list[str]) -> list[Candle]:
    candles: list[Candle] = []
    previous_close = Decimal(closes[0])
    for index, raw_close in enumerate(closes, start=1):
        close = Decimal(raw_close)
        open_price = previous_close
        high = max(open_price, close)
        low = min(open_price, close)
        candles.append(Candle(index, open_price, high, low, close, Decimal("1"), True))
        previous_close = close
    return candles


def main() -> None:
    registry = EnhancedStrategyRegistry()
    register_seed_strategy_package(registry)
    lab = EnhancedBacktestLab(registry)
    result = lab.run(
        parent_strategy_id="spot_enhancement_36",
        candle_feeds={
            ("SPOT", "BTC-USDT"): build_demo_candles(
                [
                    "100", "101", "102", "103", "104", "102", "104", "106", "108", "107", "109", "111",
                    "110", "108", "106", "104", "103", "101", "100", "98", "99", "101", "103", "105",
                    "107", "106", "104", "102", "100", "99", "101", "104", "106", "108", "107", "109",
                ]
            )
        },
        quota_snapshots={
            "BTC-USD": QuotaSnapshot(
                underlying_family="BTC-USD",
                long_limit_total=Decimal("2"),
                short_limit_total=Decimal("1"),
                protected_long_quota_total=Decimal("2"),
                protected_short_quota_total=Decimal("1"),
            )
        },
    )

    target_dir = analysis_report_dir_path()
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = target_dir / f"enhanced_seed_lab_smoke_report_{timestamp}.md"
    paths = export_lab_report_markdown(result, target)
    print(paths["report"])
    print(paths["json"])


if __name__ == "__main__":
    main()
