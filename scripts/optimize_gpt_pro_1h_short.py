from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
import time
from dataclasses import asdict, replace
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from scripts.run_gpt_pro_1h_short_backtest import (
    DEFAULT_REPORT_DIR,
    StrategyConfig,
    candles_to_frame,
    prepare_features,
    run_backtest,
)


OPT_REPORT_DIR = ROOT / "reports" / "gpt_pro_1h_short_optimization"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize the GPT Pro BTC 1H short strategy.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP")
    parser.add_argument("--bar", default="1H")
    parser.add_argument("--recent-limit", type=int, default=10000)
    parser.add_argument("--top-k-full", type=int, default=24)
    parser.add_argument("--initial-equity", type=float, default=10000.0)
    parser.add_argument("--fixed-risk-amount", type=float, default=100.0)
    parser.add_argument("--max-leverage", type=float, default=50.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--data-dir")
    parser.add_argument("--report-dir")
    return parser.parse_args(argv)


def build_param_grid() -> list[dict[str, object]]:
    grid: list[dict[str, object]] = []
    for support_window, breakdown_atr, retest_atr, stop_buffer, watch_bars, rsi_threshold in itertools.product(
        (20, 30, 40),
        (0.10, 0.20, 0.30),
        (0.20, 0.30),
        (0.15, 0.25),
        (6, 8, 12),
        (45.0, 50.0),
    ):
        grid.append(
            {
                "support_window": support_window,
                "breakdown_atr_mult": breakdown_atr,
                "retest_atr_mult": retest_atr,
                "stop_atr_buffer": stop_buffer,
                "max_retest_bars": watch_bars,
                "rsi_short_threshold": rsi_threshold,
                "require_4h_ema_slope_down": True,
                "use_volume_filter": False,
                "volume_mult": 1.10,
            }
        )
    return grid


def make_base_config(args: argparse.Namespace) -> StrategyConfig:
    return StrategyConfig(
        initial_equity=float(args.initial_equity),
        fixed_risk_amount=float(args.fixed_risk_amount),
        fee_rate=float(args.fee_rate),
        max_leverage=float(args.max_leverage),
    )


def score_metrics(metrics: dict[str, object]) -> float:
    total_return_pct = float(metrics["total_return_pct"])
    max_drawdown_pct = abs(float(metrics["max_drawdown_pct"]))
    profit_factor = float(metrics["profit_factor"]) if metrics["profit_factor"] is not None else 0.0
    if not math.isfinite(profit_factor):
        profit_factor = 3.0
    profit_factor = min(profit_factor, 3.0)
    win_rate_pct = float(metrics["win_rate_pct"]) if metrics["win_rate_pct"] is not None else 0.0
    trade_count = float(metrics["trade_count"])
    return (
        total_return_pct
        + (profit_factor - 1.0) * 30.0
        + (win_rate_pct - 50.0) * 0.20
        - max_drawdown_pct * 0.35
        + min(trade_count, 120.0) * 0.03
    )


def score_recent_row(row: dict[str, object]) -> float:
    proxy_metrics = {
        "total_return_pct": row["recent_total_return_pct"],
        "max_drawdown_pct": row["recent_max_drawdown_pct"],
        "profit_factor": row["recent_profit_factor"],
        "win_rate_pct": row["recent_win_rate_pct"],
        "trade_count": row["recent_trade_count"],
    }
    return score_metrics(proxy_metrics)


def combined_score(recent_row: dict[str, object], full_metrics: dict[str, object]) -> float:
    return score_metrics(full_metrics) * 1.25 + score_recent_row(recent_row) * 0.55


def evaluate_dataset(raw: pd.DataFrame, config: StrategyConfig) -> dict[str, object]:
    features = prepare_features(raw, config)
    _, _, metrics = run_backtest(features, config)
    return metrics


def flatten_metrics(prefix: str, metrics: dict[str, object]) -> dict[str, object]:
    return {
        f"{prefix}_final_equity": float(metrics["final_equity"]),
        f"{prefix}_total_return_pct": float(metrics["total_return_pct"]),
        f"{prefix}_max_drawdown_pct": float(metrics["max_drawdown_pct"]),
        f"{prefix}_trade_count": int(metrics["trade_count"]),
        f"{prefix}_win_rate_pct": None if metrics["win_rate_pct"] is None else float(metrics["win_rate_pct"]),
        f"{prefix}_profit_factor": None if metrics["profit_factor"] is None else float(metrics["profit_factor"]),
        f"{prefix}_avg_R": None if metrics["avg_R"] is None else float(metrics["avg_R"]),
        f"{prefix}_data_start_utc": str(metrics["data_start_utc"]),
        f"{prefix}_data_end_utc": str(metrics["data_end_utc"]),
        f"{prefix}_candle_count": int(metrics["candle_count"]),
    }


def config_name(params: dict[str, object]) -> str:
    return (
        f"sw{params['support_window']}_bd{params['breakdown_atr_mult']:.2f}_"
        f"rt{params['retest_atr_mult']:.2f}_sb{params['stop_atr_buffer']:.2f}_"
        f"wb{params['max_retest_bars']}_rsi{params['rsi_short_threshold']:.0f}"
    )


def build_summary_markdown(best_row: dict[str, object], top_frame: pd.DataFrame) -> str:
    lines = [
        "# GPT Pro 1H Short Optimization",
        "",
        "## Best Config",
        "",
        f"- Config: `{best_row['config_name']}`",
        f"- Recent 10000 return: `{best_row['recent_total_return_pct']:.2f}%`",
        f"- Recent 10000 PF: `{best_row['recent_profit_factor']:.4f}`",
        f"- Recent 10000 max DD: `{best_row['recent_max_drawdown_pct']:.2f}%`",
        f"- Full-history return: `{best_row['full_total_return_pct']:.2f}%`",
        f"- Full-history PF: `{best_row['full_profit_factor']:.4f}`",
        f"- Full-history max DD: `{best_row['full_max_drawdown_pct']:.2f}%`",
        "",
        "## Tuned Params",
        "",
        "```json",
        json.dumps(best_row["params"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## Top 10",
        "",
        top_frame.to_markdown(index=False),
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_data_root(Path(args.data_dir).expanduser().resolve() if args.data_dir else None)
    report_dir = Path(args.report_dir).expanduser().resolve() if args.report_dir else OPT_REPORT_DIR
    report_dir.mkdir(parents=True, exist_ok=True)

    if str(args.bar).strip().upper() != "1H":
        raise ValueError("optimizer currently supports only 1H entry candles")

    base_config = make_base_config(args)
    param_grid = build_param_grid()

    recent_raw = candles_to_frame(args.inst_id, args.bar, candle_limit=max(int(args.recent_limit), 0))
    full_raw = candles_to_frame(args.inst_id, args.bar, candle_limit=0)

    recent_rows: list[dict[str, object]] = []
    recent_started = time.time()
    for index, params in enumerate(param_grid, start=1):
        cfg = replace(base_config, **params)
        metrics = evaluate_dataset(recent_raw, cfg)
        row = {
            "config_name": config_name(params),
            "params": params,
            "recent_score": score_metrics(metrics),
            **flatten_metrics("recent", metrics),
        }
        recent_rows.append(row)
        if index % 24 == 0 or index == len(param_grid):
            elapsed = time.time() - recent_started
            print(f"[recent] {index}/{len(param_grid)} combos | elapsed={elapsed:.1f}s")

    recent_frame = pd.DataFrame(recent_rows).sort_values("recent_score", ascending=False).reset_index(drop=True)
    recent_csv = report_dir / "recent_screen_10000bars.csv"
    recent_frame.to_csv(recent_csv, index=False, encoding="utf-8-sig")

    top_candidates = recent_frame.head(max(int(args.top_k_full), 1)).copy()
    full_rows: list[dict[str, object]] = []
    full_started = time.time()
    for index, row in enumerate(top_candidates.to_dict("records"), start=1):
        params = dict(row["params"])
        cfg = replace(base_config, **params)
        full_metrics = evaluate_dataset(full_raw, cfg)
        merged = {
            **row,
            "full_score": score_metrics(full_metrics),
            "combined_score": combined_score(row, full_metrics),
            **flatten_metrics("full", full_metrics),
        }
        full_rows.append(merged)
        print(f"[full] {index}/{len(top_candidates)} combos | elapsed={time.time() - full_started:.1f}s")

    full_frame = pd.DataFrame(full_rows).sort_values("combined_score", ascending=False).reset_index(drop=True)
    full_csv = report_dir / "full_validation.csv"
    full_frame.to_csv(full_csv, index=False, encoding="utf-8-sig")

    best_row = full_frame.iloc[0].to_dict()
    best_json = report_dir / "best_config.json"
    best_json.write_text(json.dumps(best_row, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    top_view = full_frame[
        [
            "config_name",
            "combined_score",
            "recent_total_return_pct",
            "recent_profit_factor",
            "recent_max_drawdown_pct",
            "full_total_return_pct",
            "full_profit_factor",
            "full_max_drawdown_pct",
            "full_trade_count",
        ]
    ].head(10)
    summary_md = report_dir / "summary.md"
    summary_md.write_text(build_summary_markdown(best_row, top_view), encoding="utf-8")

    payload = {
        "data_root": str(data_root()),
        "inst_id": str(args.inst_id).strip().upper(),
        "bar": str(args.bar).strip(),
        "recent_limit": int(args.recent_limit),
        "grid_size": len(param_grid),
        "recent_csv": str(recent_csv),
        "full_csv": str(full_csv),
        "best_json": str(best_json),
        "summary_md": str(summary_md),
        "best_config_name": best_row["config_name"],
        "best_recent_total_return_pct": round(float(best_row["recent_total_return_pct"]), 4),
        "best_recent_profit_factor": round(float(best_row["recent_profit_factor"]), 6),
        "best_full_total_return_pct": round(float(best_row["full_total_return_pct"]), 4),
        "best_full_profit_factor": round(float(best_row["full_profit_factor"]), 6),
        "best_full_max_drawdown_pct": round(float(best_row["full_max_drawdown_pct"]), 4),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
