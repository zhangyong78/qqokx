from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, replace
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.app_paths import configure_data_root, data_root
from scripts.run_gpt_pro_1h_short_backtest import (
    StrategyConfig,
    candles_to_frame,
    prepare_features,
    run_backtest,
)


REPORT_DIR = ROOT / "reports" / "gpt_pro_1h_short_walkforward"
WARMUP_DAYS = 60


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk-forward validation for the GPT Pro BTC 1H short strategy.")
    parser.add_argument("--inst-id", default="BTC-USDT-SWAP")
    parser.add_argument("--bar", default="1H")
    parser.add_argument("--initial-equity", type=float, default=10000.0)
    parser.add_argument("--fixed-risk-amount", type=float, default=100.0)
    parser.add_argument("--max-leverage", type=float, default=50.0)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--train-months", type=int, default=18)
    parser.add_argument("--test-months", type=int, default=6)
    parser.add_argument("--step-months", type=int, default=6)
    parser.add_argument("--loss-streak-stop", type=int, default=0, help="Pause after this many consecutive losing trades. 0 disables.")
    parser.add_argument("--cooldown-days", type=int, default=0, help="Cooldown days after the loss-streak stop triggers.")
    parser.add_argument("--data-dir")
    parser.add_argument("--report-dir")
    return parser.parse_args(argv)


def make_base_config(args: argparse.Namespace) -> StrategyConfig:
    return StrategyConfig(
        initial_equity=float(args.initial_equity),
        fixed_risk_amount=float(args.fixed_risk_amount),
        fee_rate=float(args.fee_rate),
        max_leverage=float(args.max_leverage),
    )


def candidate_params() -> list[dict[str, object]]:
    return [
        {
            "name": "baseline_original",
            "support_window": 20,
            "breakdown_atr_mult": 0.20,
            "retest_atr_mult": 0.30,
            "stop_atr_buffer": 0.25,
            "max_retest_bars": 8,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "opt_full_positive",
            "support_window": 30,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.40,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "opt_recent_best",
            "support_window": 40,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.30,
            "stop_atr_buffer": 0.25,
            "max_retest_bars": 6,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v2",
            "support_window": 30,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.35,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v3",
            "support_window": 30,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.15,
            "stop_atr_buffer": 0.35,
            "max_retest_bars": 6,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v4",
            "support_window": 30,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.15,
            "stop_atr_buffer": 0.35,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v5",
            "support_window": 35,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.40,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v6",
            "support_window": 30,
            "breakdown_atr_mult": 0.28,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.40,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "full_best_v7",
            "support_window": 30,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.40,
            "max_retest_bars": 4,
            "rsi_short_threshold": 40.0,
        },
        {
            "name": "recent_strong_v1",
            "support_window": 40,
            "breakdown_atr_mult": 0.30,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.25,
            "max_retest_bars": 6,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "recent_strong_v2",
            "support_window": 40,
            "breakdown_atr_mult": 0.20,
            "retest_atr_mult": 0.30,
            "stop_atr_buffer": 0.25,
            "max_retest_bars": 6,
            "rsi_short_threshold": 50.0,
        },
        {
            "name": "recent_strong_v3",
            "support_window": 40,
            "breakdown_atr_mult": 0.35,
            "retest_atr_mult": 0.20,
            "stop_atr_buffer": 0.30,
            "max_retest_bars": 4,
            "rsi_short_threshold": 50.0,
        },
    ]


def score_metrics(metrics: dict[str, float]) -> float:
    profit_factor = float(metrics["profit_factor"])
    if not math.isfinite(profit_factor):
        profit_factor = 3.0
    profit_factor = min(profit_factor, 3.0)
    return (
        float(metrics["total_return_pct"])
        + (profit_factor - 1.0) * 25.0
        + (float(metrics["win_rate_pct"]) - 50.0) * 0.15
        - abs(float(metrics["max_drawdown_pct"])) * 0.35
        + min(float(metrics["trade_count"]), 80.0) * 0.04
    )


def metrics_from_trades(trades: pd.DataFrame, *, initial_equity: float) -> dict[str, float]:
    if trades.empty:
        return {
            "final_equity": initial_equity,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "trade_count": 0.0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "avg_pnl": 0.0,
            "avg_R": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
        }
    pnl = trades["pnl"].astype(float)
    pnl_r = trades["pnl_R"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(-losses.sum()) if not losses.empty else 0.0
    equity_curve = initial_equity + pnl.cumsum()
    rolling_peak = equity_curve.cummax()
    drawdown_pct = (equity_curve / rolling_peak - 1.0).min() if len(equity_curve) else 0.0
    final_equity = float(initial_equity + pnl.sum())
    return {
        "final_equity": final_equity,
        "total_return_pct": (final_equity / initial_equity - 1.0) * 100.0 if initial_equity > 0 else 0.0,
        "max_drawdown_pct": float(drawdown_pct) * 100.0,
        "trade_count": float(len(trades)),
        "win_rate_pct": float((pnl > 0).mean() * 100),
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else math.inf,
        "avg_pnl": float(pnl.mean()),
        "avg_R": float(pnl_r.mean()),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
    }


def apply_loss_streak_guard(
    trades: pd.DataFrame,
    *,
    loss_streak_stop: int,
    cooldown_days: int,
) -> pd.DataFrame:
    if trades.empty or loss_streak_stop <= 0 or cooldown_days <= 0:
        return trades.copy()
    frame = trades.copy()
    frame["entry_time_dt"] = pd.to_datetime(frame["entry_time"], utc=True)
    frame["exit_time_dt"] = pd.to_datetime(frame["exit_time"], utc=True)
    frame = frame.sort_values("entry_time_dt").reset_index(drop=True)

    accepted_indices: list[int] = []
    consecutive_losses = 0
    stop_until: pd.Timestamp | None = None

    for index, row in frame.iterrows():
        entry_time = row["entry_time_dt"]
        exit_time = row["exit_time_dt"]
        pnl = float(row["pnl"])
        if stop_until is not None and entry_time < stop_until:
            continue
        accepted_indices.append(index)
        if pnl < 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
        if consecutive_losses >= loss_streak_stop:
            stop_until = exit_time + pd.Timedelta(days=cooldown_days)
            consecutive_losses = 0

    filtered = frame.iloc[accepted_indices].copy()
    return filtered.drop(columns=["entry_time_dt", "exit_time_dt"])


def build_windows(df: pd.DataFrame, *, train_months: int, test_months: int, step_months: int) -> list[dict[str, pd.Timestamp]]:
    first_ts = pd.Timestamp(df["timestamp"].iloc[0]).tz_convert("UTC")
    last_ts = pd.Timestamp(df["timestamp"].iloc[-1]).tz_convert("UTC")
    anchor = pd.Timestamp(year=first_ts.year, month=first_ts.month, day=1, tz="UTC")
    windows: list[dict[str, pd.Timestamp]] = []
    train_delta = pd.DateOffset(months=train_months)
    test_delta = pd.DateOffset(months=test_months)
    step_delta = pd.DateOffset(months=step_months)
    current_train_start = anchor
    while True:
        train_end = current_train_start + train_delta
        test_end = train_end + test_delta
        if test_end > last_ts:
            break
        windows.append(
            {
                "train_start": current_train_start,
                "train_end": train_end,
                "test_start": train_end,
                "test_end": test_end,
            }
        )
        current_train_start = current_train_start + step_delta
    return windows


def run_window_segment(
    raw: pd.DataFrame,
    config: StrategyConfig,
    *,
    segment_start: pd.Timestamp,
    segment_end: pd.Timestamp,
    loss_streak_stop: int = 0,
    cooldown_days: int = 0,
) -> tuple[pd.DataFrame, dict[str, float]]:
    warmup_start = segment_start - pd.Timedelta(days=WARMUP_DAYS)
    subset = raw[(raw["timestamp"] >= warmup_start) & (raw["timestamp"] < segment_end)].copy().reset_index(drop=True)
    if len(subset) < 300:
        return pd.DataFrame(), metrics_from_trades(pd.DataFrame(), initial_equity=float(config.initial_equity))
    features = prepare_features(subset, config)
    trades, _, _ = run_backtest(features, config)
    if trades.empty:
        return trades, metrics_from_trades(trades, initial_equity=float(config.initial_equity))
    trades = trades.copy()
    trades["entry_time_dt"] = pd.to_datetime(trades["entry_time"], utc=True)
    filtered = trades[(trades["entry_time_dt"] >= segment_start) & (trades["entry_time_dt"] < segment_end)].copy()
    filtered = filtered.drop(columns=["entry_time_dt"])
    filtered = apply_loss_streak_guard(
        filtered,
        loss_streak_stop=loss_streak_stop,
        cooldown_days=cooldown_days,
    )
    return filtered, metrics_from_trades(filtered, initial_equity=float(config.initial_equity))


def build_summary_markdown(
    window_frame: pd.DataFrame,
    aggregate_metrics: dict[str, float],
    selected_counts: pd.DataFrame,
    args: argparse.Namespace,
) -> str:
    positive_windows = int((window_frame["test_total_return_pct"] > 0).sum()) if not window_frame.empty else 0
    total_windows = int(len(window_frame))
    pf_text = (
        "inf"
        if math.isinf(aggregate_metrics["profit_factor"])
        else f"{aggregate_metrics['profit_factor']:.4f}"
    )
    lines = [
        "# GPT Pro 1H Short Walk-Forward",
        "",
        f"- Data root: `{data_root()}`",
        f"- Symbol: `{args.inst_id}` `{args.bar}`",
        f"- Train/Test/Step: `{args.train_months}m / {args.test_months}m / {args.step_months}m`",
        f"- Fixed risk per trade: `{args.fixed_risk_amount:.2f}U`",
        f"- Initial equity per window: `{args.initial_equity:.2f}U`",
        f"- Loss guard: `{'off' if args.loss_streak_stop <= 0 or args.cooldown_days <= 0 else f'{args.loss_streak_stop} losses -> pause {args.cooldown_days} days'}`",
        "",
        "## Aggregate Test Result",
        "",
        f"- Test windows: `{total_windows}`",
        f"- Positive windows: `{positive_windows}`",
        f"- Aggregate final equity: `{aggregate_metrics['final_equity']:.2f}`",
        f"- Aggregate total return: `{aggregate_metrics['total_return_pct']:.2f}%`",
        f"- Aggregate max drawdown: `{aggregate_metrics['max_drawdown_pct']:.2f}%`",
        f"- Aggregate trades: `{int(aggregate_metrics['trade_count'])}`",
        f"- Aggregate win rate: `{aggregate_metrics['win_rate_pct']:.2f}%`",
        f"- Aggregate PF: `{pf_text}`",
        "",
        "## Selected Config Frequency",
        "",
        selected_counts.to_markdown(index=False) if not selected_counts.empty else "No window selections.",
        "",
        "## Window Table",
        "",
        window_frame.to_markdown(index=False) if not window_frame.empty else "No walk-forward windows.",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_data_root(Path(args.data_dir).expanduser().resolve() if args.data_dir else None)
    report_dir = Path(args.report_dir).expanduser().resolve() if args.report_dir else REPORT_DIR
    report_dir.mkdir(parents=True, exist_ok=True)

    if str(args.bar).strip().upper() != "1H":
        raise ValueError("walk-forward script currently supports only 1H")

    raw = candles_to_frame(args.inst_id, args.bar, candle_limit=0)
    windows = build_windows(
        raw,
        train_months=max(int(args.train_months), 1),
        test_months=max(int(args.test_months), 1),
        step_months=max(int(args.step_months), 1),
    )
    if not windows:
        raise RuntimeError("no walk-forward windows were generated")

    base_config = make_base_config(args)
    candidates = candidate_params()
    window_rows: list[dict[str, object]] = []
    aggregate_trades: list[pd.DataFrame] = []

    for window_index, window in enumerate(windows, start=1):
        best_train_score = None
        best_candidate_name = ""
        best_params: dict[str, object] | None = None
        best_train_metrics: dict[str, float] | None = None
        best_test_trades = pd.DataFrame()
        best_test_metrics: dict[str, float] | None = None

        for candidate in candidates:
            params = {
                key: value
                for key, value in candidate.items()
                if key != "name"
            }
            config = replace(
                base_config,
                require_4h_ema_slope_down=True,
                use_volume_filter=False,
                volume_mult=1.10,
                **params,
            )
            _, train_metrics = run_window_segment(
                raw,
                config,
                segment_start=window["train_start"],
                segment_end=window["train_end"],
                loss_streak_stop=max(int(args.loss_streak_stop), 0),
                cooldown_days=max(int(args.cooldown_days), 0),
            )
            train_score = score_metrics(train_metrics)
            if best_train_score is None or train_score > best_train_score:
                test_trades, test_metrics = run_window_segment(
                    raw,
                    config,
                    segment_start=window["test_start"],
                    segment_end=window["test_end"],
                    loss_streak_stop=max(int(args.loss_streak_stop), 0),
                    cooldown_days=max(int(args.cooldown_days), 0),
                )
                best_train_score = train_score
                best_candidate_name = str(candidate["name"])
                best_params = dict(params)
                best_train_metrics = train_metrics
                best_test_trades = test_trades
                best_test_metrics = test_metrics

        assert best_params is not None and best_train_metrics is not None and best_test_metrics is not None
        if not best_test_trades.empty:
            tagged = best_test_trades.copy()
            tagged["window_index"] = window_index
            tagged["selected_candidate"] = best_candidate_name
            aggregate_trades.append(tagged)

        window_rows.append(
            {
                "window_index": window_index,
                "train_start_utc": window["train_start"].strftime("%Y-%m-%d"),
                "train_end_utc": (window["train_end"] - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d"),
                "test_start_utc": window["test_start"].strftime("%Y-%m-%d"),
                "test_end_utc": (window["test_end"] - pd.Timedelta(seconds=1)).strftime("%Y-%m-%d"),
                "selected_candidate": best_candidate_name,
                "selected_params": json.dumps(best_params, ensure_ascii=False),
                "loss_guard": (
                    "off"
                    if max(int(args.loss_streak_stop), 0) <= 0 or max(int(args.cooldown_days), 0) <= 0
                    else f"{int(args.loss_streak_stop)} losses -> pause {int(args.cooldown_days)}d"
                ),
                "train_score": best_train_score,
                "train_total_return_pct": best_train_metrics["total_return_pct"],
                "train_profit_factor": best_train_metrics["profit_factor"],
                "train_max_drawdown_pct": best_train_metrics["max_drawdown_pct"],
                "train_trade_count": int(best_train_metrics["trade_count"]),
                "test_total_return_pct": best_test_metrics["total_return_pct"],
                "test_profit_factor": best_test_metrics["profit_factor"],
                "test_max_drawdown_pct": best_test_metrics["max_drawdown_pct"],
                "test_trade_count": int(best_test_metrics["trade_count"]),
                "test_win_rate_pct": best_test_metrics["win_rate_pct"],
            }
        )
        print(
            f"[walk-forward] window {window_index}/{len(windows)} "
            f"| selected={best_candidate_name} "
            f"| train_return={best_train_metrics['total_return_pct']:.2f}% "
            f"| test_return={best_test_metrics['total_return_pct']:.2f}%"
        )

    window_frame = pd.DataFrame(window_rows)
    aggregate_trade_frame = pd.concat(aggregate_trades, ignore_index=True) if aggregate_trades else pd.DataFrame()
    aggregate_metrics = metrics_from_trades(aggregate_trade_frame, initial_equity=float(args.initial_equity))
    selected_counts = (
        window_frame.groupby("selected_candidate", as_index=False)
        .size()
        .rename(columns={"size": "selected_windows"})
        .sort_values(["selected_windows", "selected_candidate"], ascending=[False, True])
        .reset_index(drop=True)
    )

    windows_csv = report_dir / "walkforward_windows.csv"
    trades_csv = report_dir / "walkforward_test_trades.csv"
    summary_md = report_dir / "summary.md"
    summary_json = report_dir / "summary.json"

    window_frame.to_csv(windows_csv, index=False, encoding="utf-8-sig")
    aggregate_trade_frame.to_csv(trades_csv, index=False, encoding="utf-8-sig")
    summary_md.write_text(build_summary_markdown(window_frame, aggregate_metrics, selected_counts, args), encoding="utf-8")
    summary_json.write_text(
        json.dumps(
            {
                "config_pool": candidates,
                "windows": window_rows,
                "aggregate_metrics": aggregate_metrics,
                "selected_counts": selected_counts.to_dict("records"),
                "data_start_utc": str(raw["timestamp"].iloc[0]),
                "data_end_utc": str(raw["timestamp"].iloc[-1]),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "data_root": str(data_root()),
                "windows_csv": str(windows_csv),
                "trades_csv": str(trades_csv),
                "summary_md": str(summary_md),
                "summary_json": str(summary_json),
                "test_windows": int(len(window_frame)),
                "positive_test_windows": int((window_frame["test_total_return_pct"] > 0).sum()),
                "aggregate_final_equity": round(float(aggregate_metrics["final_equity"]), 4),
                "aggregate_total_return_pct": round(float(aggregate_metrics["total_return_pct"]), 4),
                "aggregate_max_drawdown_pct": round(float(aggregate_metrics["max_drawdown_pct"]), 4),
                "aggregate_profit_factor": (
                    "inf" if math.isinf(aggregate_metrics["profit_factor"]) else round(float(aggregate_metrics["profit_factor"]), 6)
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
