from __future__ import annotations

import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from shutil import copyfile

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import Variant


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"first_batch_3long_daily_ema_filters_10u_{STAMP}"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
PROJECT_JSON_PATH = PROJECT_REPORT_DIR / "first_batch_3long_daily_ema_filters_10u.json"

SYMBOLS = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP")
SYMBOL_LABELS = {
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
    "SOL-USDT-SWAP": "SOL",
}

VARIANTS = (
    Variant("none", "不过滤"),
    Variant("ema_5", "日线 EMA5", "ema", 5),
    Variant("ema_8", "日线 EMA8", "ema", 8),
    Variant("ema_13", "日线 EMA13", "ema", 13),
    Variant("ema_21", "日线 EMA21", "ema", 21),
)


@dataclass(frozen=True)
class OldProfile:
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: str
    atr_take_multiplier: str


PROFILES = {
    "BTC-USDT-SWAP": OldProfile(21, "ema", 50, "ma", 50, "ma", "2", "2"),
    "ETH-USDT-SWAP": OldProfile(21, "ma", 55, "ema", 55, "ma", "2", "2"),
    "SOL-USDT-SWAP": OldProfile(21, "ma", 55, "ma", 55, "ma", "1", "1"),
}


def run_one(symbol: str, variant: Variant) -> dict[str, object]:
    from okx_quant.backtest import _run_backtest_with_loaded_data
    from okx_quant.candle_cache import load_candle_cache
    from okx_quant.models import StrategyConfig
    from okx_quant.okx_client import OkxRestClient
    from scripts.run_btc_daily_ma_direction_filter_research import (
        ENTRY_BAR,
        FILTER_BAR,
        LONG_MAKER_FEE_RATE,
        LONG_TAKER_FEE_RATE,
        build_daily_direction_bias,
        build_metrics,
        build_split_bounds,
        filter_split_trades,
    )

    profile = PROFILES[symbol]
    client = OkxRestClient()
    entry_candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(symbol, FILTER_BAR, limit=None) if candle.confirmed]
    test_bounds = build_split_bounds(len(entry_candles))["test"]
    bias = build_daily_direction_bias(entry_candles, filter_candles, variant) if variant.period else None
    config = StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=profile.ema_period,
        ema_type=profile.ema_type,
        trend_ema_period=profile.trend_ema_period,
        trend_ema_type=profile.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=Decimal(profile.atr_stop_multiplier),
        atr_take_multiplier=Decimal(profile.atr_take_multiplier),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id="ema_dynamic_order_long",
        risk_amount=Decimal("10"),
        backtest_initial_capital=Decimal("10000"),
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=profile.entry_reference_ema_period,
        entry_reference_ema_type=profile.entry_reference_ema_type,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )
    result = _run_backtest_with_loaded_data(
        entry_candles,
        client.get_instrument(symbol),
        config,
        data_source_note=f"{symbol} {variant.label}",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
        direction_filter_bias=bias,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, test_bounds)
    all_metrics = build_metrics(trades)
    test_metrics = build_metrics(test_trades)
    return {
        "variant": variant.label,
        "symbol": SYMBOL_LABELS[symbol],
        "all_pnl_u": float(all_metrics.pnl),
        "all_trades": all_metrics.trades,
        "all_drawdown_u": float(all_metrics.max_drawdown),
        "test_pnl_u": float(test_metrics.pnl),
        "test_trades": test_metrics.trades,
        "test_drawdown_u": float(test_metrics.max_drawdown),
    }


def main() -> None:
    rows: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(run_one, symbol, variant) for variant in VARIANTS for symbol in SYMBOLS]
        for future in as_completed(futures):
            rows.append(future.result())

    rows.sort(key=lambda item: (item["variant"], item["symbol"]))
    frame = pd.DataFrame(rows)
    summary_rows: list[dict[str, object]] = []
    baseline = frame[frame["variant"] == "不过滤"]
    baseline_all = float(baseline["all_pnl_u"].sum())
    baseline_test = float(baseline["test_pnl_u"].sum())
    baseline_all_dd = float(baseline["all_drawdown_u"].max())
    baseline_test_dd = float(baseline["test_drawdown_u"].max())
    for variant in [item.label for item in VARIANTS]:
        group = frame[frame["variant"] == variant]
        summary_rows.append(
            {
                "variant": variant,
                "all_pnl_u": float(group["all_pnl_u"].sum()),
                "all_trades": int(group["all_trades"].sum()),
                "all_drawdown_u": float(group["all_drawdown_u"].max()),
                "test_pnl_u": float(group["test_pnl_u"].sum()),
                "test_trades": int(group["test_trades"].sum()),
                "test_drawdown_u": float(group["test_drawdown_u"].max()),
                "all_delta_vs_no_filter_u": float(group["all_pnl_u"].sum() - baseline_all),
                "test_delta_vs_no_filter_u": float(group["test_pnl_u"].sum() - baseline_test),
                "all_drawdown_delta_u": float(group["all_drawdown_u"].max() - baseline_all_dd),
                "test_drawdown_delta_u": float(group["test_drawdown_u"].max() - baseline_test_dd),
            }
        )

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "scope": "first_batch_3long_old_params_10u",
        "daily_filter_note": "使用 confirmed 1D K，并对每根 1H K 取最近一根已收盘日线，等价于昨天日线状态。",
        "summary": summary_rows,
        "by_coin": rows,
    }
    pd.DataFrame(summary_rows).to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    copyfile(JSON_PATH, PROJECT_JSON_PATH)
    print(JSON_PATH)
    print(PROJECT_JSON_PATH)


if __name__ == "__main__":
    main()
