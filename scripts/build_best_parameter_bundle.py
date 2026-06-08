from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.models import StrategyConfig
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
from okx_quant.strategy_profiles import (
    STRATEGY_PROFILE_SCHEMA_VERSION,
    StrategyBundle,
    build_strategy_profile_from_config,
    write_strategy_bundle,
)


PACKAGE_DIR = analysis_report_dir_path() / "packages"
PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
TARGET_PATH = PACKAGE_DIR / "最佳参数组合包.json"


@dataclass(frozen=True)
class BundleProfileSpec:
    symbol: str
    profile_id: str
    profile_name: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    notes: str


SPECS = (
    BundleProfileSpec(
        symbol="BTC-USDT-SWAP",
        profile_id="dynamic_long_best_btc",
        profile_name="BTC 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        notes="固定研究口径：EMA21 / MA50 / 挂单 MA50 / SL2 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    BundleProfileSpec(
        symbol="ETH-USDT-SWAP",
        profile_id="dynamic_long_best_eth",
        profile_name="ETH 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ema",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        notes="固定研究口径：MA21 / EMA55 / 挂单 MA55 / SL2 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    BundleProfileSpec(
        symbol="SOL-USDT-SWAP",
        profile_id="dynamic_long_best_sol",
        profile_name="SOL 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        notes="固定研究口径：MA21 / MA55 / 挂单 MA55 / SL1 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
    BundleProfileSpec(
        symbol="BNB-USDT-SWAP",
        profile_id="dynamic_long_best_bnb",
        profile_name="BNB 动态委托做多 最佳参数",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("6"),
        notes="固定研究口径：MA21 / MA55 / 挂单 MA55 / SL1.5 / 动态止盈 / 10U 风险 / 不加日线过滤。",
    ),
)


def build_dynamic_long_config(spec: BundleProfileSpec) -> StrategyConfig:
    return StrategyConfig(
        inst_id=spec.symbol,
        bar="1H",
        ema_period=spec.ema_period,
        ema_type=spec.ema_type,
        trend_ema_period=spec.trend_ema_period,
        trend_ema_type=spec.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=spec.atr_stop_multiplier,
        atr_take_multiplier=spec.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="live",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=spec.entry_reference_ema_period,
        entry_reference_ema_type=spec.entry_reference_ema_type,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def cleanup_old_package_files(target_path: Path) -> list[Path]:
    removed: list[Path] = []
    for path in PACKAGE_DIR.iterdir():
        if path.resolve() == target_path.resolve():
            continue
        if path.is_file():
            path.unlink()
            removed.append(path)
    return removed


def build_bundle() -> StrategyBundle:
    profiles = tuple(
        build_strategy_profile_from_config(
            profile_id=spec.profile_id,
            profile_name=spec.profile_name,
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            symbol=spec.symbol,
            config=build_dynamic_long_config(spec),
            api_name="",
            direction_label="只做多",
            run_mode_label="交易并下单",
            tags=("最佳参数", "EMA动态委托做多", "固定研究"),
            notes=spec.notes,
            source_report="D:/qqokx/reports/ema_dynamic_long_fixed_research_log_4coins_10u.html",
        )
        for spec in SPECS
    )
    return StrategyBundle(
        bundle_version=STRATEGY_PROFILE_SCHEMA_VERSION,
        bundle_name="最佳参数组合包",
        profiles=profiles,
        source_report="D:/qqokx/reports/ema_dynamic_long_fixed_research_log_4coins_10u.html",
        auto_start_on_import=True,
    )


def main() -> None:
    removed = cleanup_old_package_files(TARGET_PATH)
    bundle = build_bundle()
    write_strategy_bundle(bundle, TARGET_PATH)
    print(f"written: {TARGET_PATH}")
    print(f"removed: {len(removed)}")
    for path in removed:
        print(f" - {path.name}")


if __name__ == "__main__":
    main()
