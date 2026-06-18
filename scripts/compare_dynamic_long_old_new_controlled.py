from __future__ import annotations

import argparse
import subprocess
import sys
import types
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data as run_current_backtest
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from scripts import run_best_parameter_bundle_1h_standard_portfolio as standard_portfolio
from scripts.build_best_parameter_bundle import build_specs


DEFAULT_COINS = ("BTC", "ETH", "SOL", "DOGE")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare dynamic long best profiles with a fixed data/fee/risk protocol. "
            "The base backtest module is loaded from git, while the current module "
            "uses the working tree."
        )
    )
    parser.add_argument("--base-ref", default="HEAD", help="Git ref used as the baseline backtest.py")
    parser.add_argument("--risk-amount", default="100", help="Fixed risk amount per trade")
    parser.add_argument("--initial-capital", default="10000", help="Backtest initial capital")
    parser.add_argument("--coins", nargs="*", default=list(DEFAULT_COINS), help="Coins to compare")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Directory for CSV outputs. Defaults to the analysis report directory.",
    )
    return parser.parse_args()


def load_base_backtest_module(base_ref: str) -> types.ModuleType:
    source = subprocess.check_output(
        ["git", "show", f"{base_ref}:okx_quant/backtest.py"],
        cwd=ROOT,
        text=True,
        encoding="utf-8-sig",
    )
    module = types.ModuleType(f"base_backtest_{base_ref.replace('/', '_').replace('-', '_')}")
    sys.modules[module.__name__] = module
    exec(compile(source, f"{base_ref}:okx_quant/backtest.py", "exec"), module.__dict__)
    return module


def prepare_config(config: Any, *, risk_amount: Decimal, initial_capital: Decimal):
    return replace(
        config,
        bar="1H",
        backtest_initial_capital=initial_capital,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_risk_percent=None,
        risk_amount=risk_amount,
        backtest_entry_slippage_rate=standard_portfolio.FORMAL_SLIPPAGE,
        backtest_exit_slippage_rate=standard_portfolio.FORMAL_SLIPPAGE,
        backtest_slippage_rate=standard_portfolio.FORMAL_SLIPPAGE,
        backtest_funding_rate=standard_portfolio.FORMAL_FUNDING_RATE,
    )


def trade_frame(trades: list[Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for trade_no, trade in enumerate(trades, 1):
        rows.append(
            {
                "trade_no": trade_no,
                "entry_ts": int(trade.entry_ts),
                "exit_ts": int(trade.exit_ts),
                "entry_time_bjt": pd.to_datetime(trade.entry_ts, unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
                .strftime("%Y-%m-%d %H:%M"),
                "exit_time_bjt": pd.to_datetime(trade.exit_ts, unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
                .strftime("%Y-%m-%d %H:%M"),
                "entry_price": float(trade.entry_price),
                "exit_price": float(trade.exit_price),
                "size": float(trade.size),
                "pnl": float(trade.pnl),
                "r_multiple": float(trade.r_multiple),
                "exit_reason": trade.exit_reason,
                "wave_entry_sequence": int(getattr(trade, "wave_entry_sequence", 0)),
                "total_fee": float(trade.total_fee),
                "slippage_cost": float(trade.slippage_cost),
            }
        )
    return pd.DataFrame(rows)


def metrics(frame: pd.DataFrame) -> dict[str, float | int]:
    if frame.empty:
        return {"trades": 0, "pnl": 0.0, "win_rate": 0.0, "profit_factor": 0.0}
    pnl = frame["pnl"].astype(float)
    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = abs(float(pnl[pnl < 0].sum()))
    return {
        "trades": int(len(frame)),
        "pnl": round(float(pnl.sum()), 4),
        "win_rate": round(float((pnl > 0).mean() * 100), 4),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else 999.0,
    }


def exact_match_count(old_frame: pd.DataFrame, current_frame: pd.DataFrame) -> int:
    keys = ["entry_ts", "exit_ts", "entry_price", "exit_price", "exit_reason"]
    if old_frame.empty or current_frame.empty:
        return 0
    return int(len(old_frame[keys].merge(current_frame[keys], on=keys)))


def main() -> None:
    args = parse_args()
    coins = [coin.upper() for coin in args.coins]
    risk_amount = Decimal(str(args.risk_amount))
    initial_capital = Decimal(str(args.initial_capital))
    output_dir = Path(args.output_dir) if args.output_dir else analysis_report_dir_path()
    output_dir.mkdir(parents=True, exist_ok=True)

    base_module = load_base_backtest_module(args.base_ref)
    client = OkxRestClient()
    specs = {spec.profile_id: spec for spec in build_specs()}

    summary_rows: list[dict[str, Any]] = []
    wave_rows: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []

    for coin in coins:
        profile_id = f"dynamic_long_best_{coin.lower()}_v2"
        if profile_id not in specs:
            raise KeyError(f"Unknown dynamic long best profile: {profile_id}")
        spec = specs[profile_id]
        config = prepare_config(spec.config, risk_amount=risk_amount, initial_capital=initial_capital)
        candles = [item for item in load_candle_cache(spec.symbol, "1H", limit=None) if item.confirmed]
        if not candles:
            raise RuntimeError(f"Missing confirmed 1H candles for {spec.symbol}")
        instrument = client.get_instrument(spec.symbol)
        run_kwargs = {
            "data_source_note": f"controlled old/new compare | {profile_id}",
            "maker_fee_rate": standard_portfolio.LONG_MAKER_FEE_RATE,
            "taker_fee_rate": standard_portfolio.LONG_TAKER_FEE_RATE,
        }
        old_result = base_module._run_backtest_with_loaded_data(candles, instrument, config, **run_kwargs)
        current_result = run_current_backtest(candles, instrument, config, **run_kwargs)
        old_frame = trade_frame(old_result.trades)
        current_frame = trade_frame(current_result.trades)
        old_metrics = metrics(old_frame)
        current_metrics = metrics(current_frame)
        common = exact_match_count(old_frame, current_frame)

        summary_rows.append(
            {
                "coin": coin,
                "profile_id": profile_id,
                "max_entries": config.max_entries_per_trend,
                "old_trades": old_metrics["trades"],
                "current_trades": current_metrics["trades"],
                "trade_delta": int(current_metrics["trades"]) - int(old_metrics["trades"]),
                "old_pnl": old_metrics["pnl"],
                "current_pnl": current_metrics["pnl"],
                "pnl_delta": round(float(current_metrics["pnl"]) - float(old_metrics["pnl"]), 4),
                "old_win_rate": old_metrics["win_rate"],
                "current_win_rate": current_metrics["win_rate"],
                "old_profit_factor": old_metrics["profit_factor"],
                "current_profit_factor": current_metrics["profit_factor"],
                "exact_common": common,
                "old_unmatched": int(len(old_frame) - common),
                "current_unmatched": int(len(current_frame) - common),
            }
        )
        for sequence, group in current_frame.groupby("wave_entry_sequence"):
            sequence_metrics = metrics(group)
            wave_rows.append(
                {
                    "coin": coin,
                    "wave_entry_sequence": int(sequence),
                    "trades": sequence_metrics["trades"],
                    "pnl": sequence_metrics["pnl"],
                    "win_rate": sequence_metrics["win_rate"],
                    "profit_factor": sequence_metrics["profit_factor"],
                }
            )
        meta_rows.append(
            {
                "coin": coin,
                "symbol": spec.symbol,
                "candles": len(candles),
                "start_bjt": pd.to_datetime(candles[0].ts, unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
                .strftime("%Y-%m-%d %H:%M"),
                "end_bjt": pd.to_datetime(candles[-1].ts, unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
                .strftime("%Y-%m-%d %H:%M"),
                "risk_amount": str(risk_amount),
                "initial_capital": str(initial_capital),
                "maker_fee_rate": str(standard_portfolio.LONG_MAKER_FEE_RATE),
                "taker_fee_rate": str(standard_portfolio.LONG_TAKER_FEE_RATE),
                "slippage_rate": str(standard_portfolio.FORMAL_SLIPPAGE),
                "base_ref": args.base_ref,
            }
        )

    summary = pd.DataFrame(summary_rows)
    wave = pd.DataFrame(wave_rows)
    meta = pd.DataFrame(meta_rows)
    summary_path = output_dir / "dynamic_long_controlled_old_new_summary.csv"
    wave_path = output_dir / "dynamic_long_controlled_wave_summary.csv"
    meta_path = output_dir / "dynamic_long_controlled_meta.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    wave.to_csv(wave_path, index=False, encoding="utf-8-sig")
    meta.to_csv(meta_path, index=False, encoding="utf-8-sig")

    print(summary.to_string(index=False))
    print()
    print(wave.to_string(index=False))
    print()
    print(summary_path)
    print(wave_path)
    print(meta_path)


if __name__ == "__main__":
    main()
