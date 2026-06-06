from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    SplitMetrics,
    build_daily_direction_bias,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)
from scripts.run_multi_coin_best_long_daily_gate_report import GATES, LONG_PROFILES, SYMBOL_LABELS


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"multi_coin_dynamic_long_daily_filter_compare_10u_{STAMP}.html"
CSV_PATH = REPORT_DIR / f"multi_coin_dynamic_long_daily_filter_compare_10u_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"multi_coin_dynamic_long_daily_filter_compare_10u_{STAMP}.json"

SYMBOLS = tuple(LONG_PROFILES.keys())
ENTRY_LIMIT = 10000


@dataclass(frozen=True)
class GateRun:
    gate_key: str
    gate_label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


@dataclass(frozen=True)
class CoinStudy:
    symbol: str
    label: str
    profile: object
    entry_count: int
    filter_count: int
    start_ts: int
    end_ts: int
    gate_runs: dict[str, GateRun]


@dataclass(frozen=True)
class ScenarioResult:
    key: str
    label: str
    description: str
    selected_gates: dict[str, str]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


def build_dynamic_long_config(symbol: str) -> StrategyConfig:
    profile = LONG_PROFILES[symbol]
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=profile.ema_period,
        trend_ema_period=profile.trend_ema_period,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=profile.atr_stop_multiplier,
        atr_take_multiplier=profile.atr_stop_multiplier * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        entry_reference_ema_period=profile.entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def build_data_note(symbol: str, entry_count: int, filter_count: int) -> str:
    return (
        f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={entry_count} | "
        f"{FILTER_BAR} candles={filter_count}"
    )


def run_coin_study(client: OkxRestClient, symbol: str) -> CoinStudy:
    entry_candles = [c for c in load_candle_cache(symbol, ENTRY_BAR, limit=ENTRY_LIMIT) if c.confirmed]
    filter_candles = [c for c in load_candle_cache(symbol, FILTER_BAR, limit=None) if c.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}/{FILTER_BAR}")

    instrument = client.get_instrument(symbol)
    test_bounds = build_split_bounds(len(entry_candles))["test"]

    bias_map: dict[str, list[str] | None] = {"none": None}
    for gate in GATES:
        if gate.key == "none":
            continue
        bias_map[gate.key] = build_daily_direction_bias(entry_candles, filter_candles, gate)

    gate_runs: dict[str, GateRun] = {}
    for gate in GATES:
        print(f"run {SYMBOL_LABELS[symbol]} dynamic long gate {gate.label}")
        result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            build_dynamic_long_config(symbol),
            data_source_note=build_data_note(symbol, len(entry_candles), len(filter_candles)),
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
            direction_filter_bias=bias_map[gate.key],
        )
        trades = list(result.trades)
        test_trades = filter_split_trades(trades, test_bounds)
        gate_runs[gate.key] = GateRun(
            gate_key=gate.key,
            gate_label=gate.label,
            trades=trades,
            test_trades=test_trades,
            all_metrics=build_metrics(trades),
            test_metrics=build_metrics(test_trades),
        )

    return CoinStudy(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        profile=LONG_PROFILES[symbol],
        entry_count=len(entry_candles),
        filter_count=len(filter_candles),
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        gate_runs=gate_runs,
    )


def combine_runs(selected_runs: list[GateRun]) -> tuple[list[BacktestTrade], list[BacktestTrade], SplitMetrics, SplitMetrics]:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in selected_runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return trades, test_trades, build_metrics(trades), build_metrics(test_trades)


def build_scenarios(studies: list[CoinStudy]) -> list[ScenarioResult]:
    filtered_gates = [gate for gate in GATES if gate.key != "none"]

    baseline_runs = [study.gate_runs["none"] for study in studies]
    baseline_trades, baseline_test_trades, baseline_all, baseline_test = combine_runs(baseline_runs)
    baseline = ScenarioResult(
        key="baseline",
        label="不过滤",
        description="五币种全部使用各自最优 EMA 动态委托做多参数，不加日线过滤。",
        selected_gates={study.symbol: "none" for study in studies},
        trades=baseline_trades,
        test_trades=baseline_test_trades,
        all_metrics=baseline_all,
        test_metrics=baseline_test,
    )

    common_candidates: list[ScenarioResult] = []
    for gate in filtered_gates:
        selected_runs = [study.gate_runs[gate.key] for study in studies]
        trades, test_trades, all_metrics, test_metrics = combine_runs(selected_runs)
        common_candidates.append(
            ScenarioResult(
                key=f"common_{gate.key}",
                label=f"统一过滤: {gate.label}",
                description=f"五币种统一使用 {gate.label} 作为日线过滤门槛。",
                selected_gates={study.symbol: gate.key for study in studies},
                trades=trades,
                test_trades=test_trades,
                all_metrics=all_metrics,
                test_metrics=test_metrics,
            )
        )
    best_common = max(common_candidates, key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl))

    best_per_coin_runs: list[GateRun] = []
    best_per_coin_keys: dict[str, str] = {}
    for study in studies:
        best_run = max(
            [study.gate_runs[gate.key] for gate in filtered_gates],
            key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl),
        )
        best_per_coin_runs.append(best_run)
        best_per_coin_keys[study.symbol] = best_run.gate_key
    trades, test_trades, all_metrics, test_metrics = combine_runs(best_per_coin_runs)
    best_per_coin = ScenarioResult(
        key="best_per_coin_filtered",
        label="各币种各自最佳过滤",
        description="每个币种都单独选择自己的最佳日线过滤，再汇总五币种结果。",
        selected_gates=best_per_coin_keys,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )

    return [baseline, best_common, best_per_coin]


def aggregate_gate_metrics(studies: list[CoinStudy], gate_key: str) -> GateRun:
    selected_runs = [study.gate_runs[gate_key] for study in studies]
    trades, test_trades, all_metrics, test_metrics = combine_runs(selected_runs)
    gate_label = next(gate.label for gate in GATES if gate.key == gate_key)
    return GateRun(
        gate_key=gate_key,
        gate_label=gate_label,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )


def build_gate_frame(studies: list[CoinStudy]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        baseline = study.gate_runs["none"]
        for gate in GATES:
            run = study.gate_runs[gate.key]
            rows.append(
                {
                    "scope": study.label,
                    "symbol": study.symbol,
                    "gate_key": gate.key,
                    "gate_label": gate.label,
                    "all_pnl": float(run.all_metrics.pnl),
                    "all_trades": run.all_metrics.trades,
                    "all_win_rate": float(run.all_metrics.win_rate),
                    "all_avg_r": float(run.all_metrics.avg_r),
                    "all_profit_factor": None if run.all_metrics.profit_factor is None else float(run.all_metrics.profit_factor),
                    "all_drawdown": float(run.all_metrics.max_drawdown),
                    "test_pnl": float(run.test_metrics.pnl),
                    "test_trades": run.test_metrics.trades,
                    "test_win_rate": float(run.test_metrics.win_rate),
                    "test_avg_r": float(run.test_metrics.avg_r),
                    "test_profit_factor": None if run.test_metrics.profit_factor is None else float(run.test_metrics.profit_factor),
                    "test_drawdown": float(run.test_metrics.max_drawdown),
                    "test_delta_vs_baseline": float(run.test_metrics.pnl - baseline.test_metrics.pnl),
                }
            )

    baseline_aggregate = aggregate_gate_metrics(studies, "none")
    for gate in GATES:
        aggregate = aggregate_gate_metrics(studies, gate.key)
        rows.append(
            {
                "scope": "ALL",
                "symbol": "ALL",
                "gate_key": gate.key,
                "gate_label": gate.label,
                "all_pnl": float(aggregate.all_metrics.pnl),
                "all_trades": aggregate.all_metrics.trades,
                "all_win_rate": float(aggregate.all_metrics.win_rate),
                "all_avg_r": float(aggregate.all_metrics.avg_r),
                "all_profit_factor": None if aggregate.all_metrics.profit_factor is None else float(aggregate.all_metrics.profit_factor),
                "all_drawdown": float(aggregate.all_metrics.max_drawdown),
                "test_pnl": float(aggregate.test_metrics.pnl),
                "test_trades": aggregate.test_metrics.trades,
                "test_win_rate": float(aggregate.test_metrics.win_rate),
                "test_avg_r": float(aggregate.test_metrics.avg_r),
                "test_profit_factor": None if aggregate.test_metrics.profit_factor is None else float(aggregate.test_metrics.profit_factor),
                "test_drawdown": float(aggregate.test_metrics.max_drawdown),
                "test_delta_vs_baseline": float(aggregate.test_metrics.pnl - baseline_aggregate.test_metrics.pnl),
            }
        )
    return pd.DataFrame(rows)


def build_yearly_table(scenarios: list[ScenarioResult]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for scenario in scenarios:
        frame = pd.DataFrame(
            {
                "exit_ts": [int(trade.exit_ts) for trade in scenario.trades],
                "pnl": [float(trade.pnl) for trade in scenario.trades],
            }
        )
        if frame.empty:
            continue
        frame["year"] = pd.to_datetime(frame["exit_ts"], unit="ms", utc=True).dt.strftime("%Y")
        grouped = frame.groupby("year", as_index=False).agg(trades=("pnl", "size"), total_pnl=("pnl", "sum"))
        for row in grouped.to_dict("records"):
            rows.append(
                {
                    "scenario": scenario.label,
                    "year": row["year"],
                    "trades": int(row["trades"]),
                    "total_pnl": float(row["total_pnl"]),
                }
            )
    return pd.DataFrame(rows)


def split_payload(metrics: SplitMetrics) -> dict[str, object]:
    return {
        "pnl": str(metrics.pnl),
        "trades": metrics.trades,
        "win_rate": str(metrics.win_rate),
        "profit_factor": None if metrics.profit_factor is None else str(metrics.profit_factor),
        "avg_r": str(metrics.avg_r),
        "max_drawdown": str(metrics.max_drawdown),
        "return_pct": str(metrics.return_pct),
    }


def scenario_payload(scenario: ScenarioResult, baseline: ScenarioResult) -> dict[str, object]:
    return {
        "key": scenario.key,
        "label": scenario.label,
        "description": scenario.description,
        "selected_gates": scenario.selected_gates,
        "all_metrics": split_payload(scenario.all_metrics),
        "test_metrics": split_payload(scenario.test_metrics),
        "all_delta_vs_baseline": str(scenario.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_delta_vs_baseline": str(scenario.test_metrics.pnl - baseline.test_metrics.pnl),
    }


def build_payload(studies: list[CoinStudy], scenarios: list[ScenarioResult], gate_frame: pd.DataFrame) -> dict[str, object]:
    baseline = next(item for item in scenarios if item.key == "baseline")
    best_common = next(item for item in scenarios if item.key.startswith("common_"))
    best_per_coin = next(item for item in scenarios if item.key == "best_per_coin_filtered")
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "risk_amount": str(RISK_AMOUNT),
        "entry_limit": ENTRY_LIMIT,
        "coins": [study.label for study in studies],
        "best_long_profiles": {
            study.label: {
                "ema_period": study.profile.ema_period,
                "trend_ema_period": study.profile.trend_ema_period,
                "entry_reference_ema_period": study.profile.entry_reference_ema_period,
                "atr_stop_multiplier": str(study.profile.atr_stop_multiplier),
            }
            for study in studies
        },
        "scenarios": [scenario_payload(item, baseline) for item in scenarios],
        "best_common_gate": best_common.label,
        "best_per_coin_gates": {
            study.label: study.gate_runs[best_per_coin.selected_gates[study.symbol]].gate_label for study in studies
        },
        "gate_metrics": gate_frame.to_dict("records"),
    }


def build_summary_chart(scenarios: list[ScenarioResult]):
    labels = [scenario.label for scenario in scenarios]
    test_values = [float(scenario.test_metrics.pnl) for scenario in scenarios]
    all_values = [float(scenario.all_metrics.pnl) for scenario in scenarios]
    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar([item - width / 2 for item in x], test_values, width=width, label="测试段 PnL", color="#1d4ed8")
    ax.bar([item + width / 2 for item in x], all_values, width=width, label="全样本 PnL", color="#0f766e")
    ax.set_title("三种情景总盈亏对比", fontsize=14, pad=12)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def build_coin_delta_chart(studies: list[CoinStudy]):
    labels = []
    baseline_values = []
    filtered_values = []
    for study in studies:
        baseline = study.gate_runs["none"]
        best_filtered = max(
            [study.gate_runs[gate.key] for gate in GATES if gate.key != "none"],
            key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl),
        )
        labels.append(study.label)
        baseline_values.append(float(baseline.test_metrics.pnl))
        filtered_values.append(float(best_filtered.test_metrics.pnl))

    x = range(len(labels))
    width = 0.35
    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar([item - width / 2 for item in x], baseline_values, width=width, label="不过滤", color="#94a3b8")
    ax.bar([item + width / 2 for item in x], filtered_values, width=width, label="最佳过滤", color="#f59e0b")
    ax.set_title("各币种测试段：不过滤 vs 最佳过滤", fontsize=14, pad=12)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def build_yearly_chart(yearly: pd.DataFrame):
    pivot = yearly.pivot(index="year", columns="scenario", values="total_pnl").fillna(0).sort_index()
    fig, ax = plt.subplots(figsize=(10, 5.5))
    pivot.plot(kind="bar", ax=ax, color=["#94a3b8", "#2563eb", "#d97706"], width=0.8)
    ax.set_title("五币种年度总盈亏对比", fontsize=14, pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=0)
    fig.tight_layout()
    return fig


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def fmt(value: Decimal | float | int, digits: int = 4) -> str:
    return format_decimal_fixed(Decimal(str(value)), digits)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


def pct(value: Decimal) -> str:
    return f"{format_decimal_fixed(value, 2)}%"


def build_html(studies: list[CoinStudy], scenarios: list[ScenarioResult], gate_frame: pd.DataFrame, yearly: pd.DataFrame) -> str:
    baseline = next(item for item in scenarios if item.key == "baseline")
    best_common = next(item for item in scenarios if item.key.startswith("common_"))
    best_per_coin = next(item for item in scenarios if item.key == "best_per_coin_filtered")

    summary_chart = fig_to_base64(build_summary_chart(scenarios))
    coin_delta_chart = fig_to_base64(build_coin_delta_chart(studies))
    yearly_chart = fig_to_base64(build_yearly_chart(yearly))

    aggregate = gate_frame[gate_frame["scope"] == "ALL"].copy().sort_values(["test_pnl", "all_pnl"], ascending=False)
    baseline_test_pnl = Decimal(str(float(aggregate[aggregate["gate_key"] == "none"]["test_pnl"].iloc[0])))

    aggregate_rows = []
    for row in aggregate.to_dict("records"):
        delta = Decimal(str(row["test_pnl"])) - baseline_test_pnl
        aggregate_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['gate_label']))}</td>"
            f"<td>{fmt(row['test_pnl'])}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{int(row['test_trades'])}</td>"
            f"<td>{fmt(row['test_avg_r'])}</td>"
            f"<td>{fmt_pf(None if row['test_profit_factor'] is None else Decimal(str(row['test_profit_factor'])))}</td>"
            f"<td>{fmt(row['all_pnl'])}</td>"
            "</tr>"
        )

    profile_rows = []
    best_gate_rows = []
    for study in studies:
        profile = study.profile
        best_filtered = max(
            [study.gate_runs[gate.key] for gate in GATES if gate.key != "none"],
            key=lambda item: (item.test_metrics.pnl, item.all_metrics.pnl),
        )
        baseline_run = study.gate_runs["none"]
        delta = best_filtered.test_metrics.pnl - baseline_run.test_metrics.pnl
        profile_rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>EMA{profile.ema_period}</td>"
            f"<td>EMA{profile.trend_ema_period}</td>"
            f"<td>{html.escape(profile.entry_label)}</td>"
            f"<td>SL x{format_decimal_fixed(profile.atr_stop_multiplier, 1)}</td>"
            "</tr>"
        )
        best_gate_rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>{html.escape(best_filtered.gate_label)}</td>"
            f"<td>{fmt(baseline_run.test_metrics.pnl)}</td>"
            f"<td>{fmt(best_filtered.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{best_filtered.test_metrics.trades}</td>"
            f"<td>{fmt(best_filtered.test_metrics.avg_r)}</td>"
            "</tr>"
        )

    scenario_rows = []
    for scenario in scenarios:
        delta = scenario.test_metrics.pnl - baseline.test_metrics.pnl
        scenario_rows.append(
            "<tr>"
            f"<td>{html.escape(scenario.label)}</td>"
            f"<td>{fmt(scenario.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{scenario.test_metrics.trades}</td>"
            f"<td>{pct(scenario.test_metrics.win_rate)}</td>"
            f"<td>{fmt(scenario.test_metrics.avg_r)}</td>"
            f"<td>{fmt_pf(scenario.test_metrics.profit_factor)}</td>"
            f"<td>{fmt(scenario.all_metrics.pnl)}</td>"
            "</tr>"
        )

    yearly_rows = []
    for row in yearly.sort_values(["year", "scenario"]).to_dict("records"):
        yearly_rows.append(
            "<tr>"
            f"<td>{html.escape(str(row['year']))}</td>"
            f"<td>{html.escape(str(row['scenario']))}</td>"
            f"<td>{int(row['trades'])}</td>"
            f"<td class=\"{'good' if float(row['total_pnl']) >= 0 else 'bad'}\">{fmt(row['total_pnl'])}</td>"
            "</tr>"
        )

    start_ts = min(study.start_ts for study in studies)
    end_ts = max(study.end_ts for study in studies)
    best_common_delta = best_common.test_metrics.pnl - baseline.test_metrics.pnl
    best_per_coin_delta = best_per_coin.test_metrics.pnl - baseline.test_metrics.pnl
    winner_label = best_per_coin.label if best_per_coin.test_metrics.pnl >= best_common.test_metrics.pnl else best_common.label

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种 EMA 动态委托做多 + 日线过滤对比</title>
  <style>
    :root {{
      --bg:#f5f7fb; --panel:#ffffff; --line:#dbe3ec; --ink:#132033; --muted:#667085;
      --blue:#1d4ed8; --teal:#0f766e; --green:#166534; --red:#b42318;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }}
    .wrap {{ max-width:1440px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#184e77 55%,#0f766e 100%);
      color:#fff; border-radius:24px; padding:30px 34px; box-shadow:0 20px 44px rgba(15,23,42,.18);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.75; color:rgba(255,255,255,.92); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{
      background:rgba(255,255,255,.12); border:1px solid rgba(255,255,255,.18);
      border-radius:999px; padding:8px 12px; font-size:13px;
    }}
    .grid4 {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:16px; margin:22px 0; }}
    .grid2 {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; }}
    .card, .section {{
      background:var(--panel); border:1px solid var(--line); border-radius:22px;
      padding:20px; box-shadow:0 10px 24px rgba(15,23,42,.05);
    }}
    .card .k {{ color:var(--muted); font-size:13px; }}
    .card .v {{ margin-top:10px; font-size:28px; font-weight:800; }}
    .card .s {{ margin-top:8px; color:var(--muted); font-size:13px; line-height:1.65; }}
    .section {{ margin-top:18px; }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .note {{ padding:14px 16px; border-left:4px solid var(--blue); background:#eef4ff; border-radius:14px; }}
    .chart {{ background:#fbfdff; border:1px solid var(--line); border-radius:18px; padding:16px; }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th, td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ background:#f8fbff; color:#475467; font-weight:700; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    @media (max-width: 1100px) {{
      .grid4, .grid2 {{ grid-template-columns:1fr; }}
      .wrap {{ padding:16px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种 EMA 动态委托做多：日线过滤 vs 不过滤</h1>
      <p>这次只看做多侧，不再把做空混进来。五个币种统一使用 <strong>10U 风险金</strong>，并且每个币种都套用仓库里已有的 <strong>最佳 EMA 动态委托做多参数</strong>，然后比较三种情景。</p>
      <p>三种情景分别是：<strong>不过滤</strong>、<strong>统一最佳日线过滤</strong>、<strong>各币种各自最佳日线过滤</strong>。这样能直接看清：日线过滤到底有没有增益，增益是统一门槛更好，还是每个币自己挑门槛更好。</p>
      <div class="meta">
        <div class="chip">低周期：{ENTRY_BAR} | 日线过滤：{FILTER_BAR}</div>
        <div class="chip">风险金：{RISK_AMOUNT}U</div>
        <div class="chip">币种：BTC / ETH / SOL / BNB / DOGE</div>
        <div class="chip">1H 样本：最近 {ENTRY_LIMIT:,} 根已确认K线</div>
        <div class="chip">样本区间：{format_ts(start_ts)} -> {format_ts(end_ts)}</div>
      </div>
    </section>

    <section class="grid4">
      <div class="card">
        <div class="k">测试段基线</div>
        <div class="v">{fmt(baseline.test_metrics.pnl)}</div>
        <div class="s">不过滤 | Trades {baseline.test_metrics.trades} | AvgR {fmt(baseline.test_metrics.avg_r)}</div>
      </div>
      <div class="card">
        <div class="k">统一最佳过滤</div>
        <div class="v">{fmt(best_common.test_metrics.pnl)}</div>
        <div class="s">{html.escape(best_common.label)} | 相对基线 {fmt(best_common_delta)}</div>
      </div>
      <div class="card">
        <div class="k">各币种最佳过滤</div>
        <div class="v">{fmt(best_per_coin.test_metrics.pnl)}</div>
        <div class="s">相对基线 {fmt(best_per_coin_delta)} | Trades {best_per_coin.test_metrics.trades}</div>
      </div>
      <div class="card">
        <div class="k">当前样本最优答案</div>
        <div class="v">{html.escape(winner_label)}</div>
        <div class="s">按五币种合并后的测试段 PnL 排序</div>
      </div>
    </section>

    <section class="section">
      <h2>结论</h2>
      <ul>
        <li>五币种做多侧的基线，是 <strong>不过滤</strong> 直接运行各币种最佳 EMA 动态委托参数。</li>
        <li>如果只允许一个统一的日线过滤门槛，当前样本里最强的是 <strong>{html.escape(best_common.label)}</strong>，测试段总盈亏为 <strong>{fmt(best_common.test_metrics.pnl)}</strong>，相对不过滤变化 <strong class="{'good' if best_common_delta >= 0 else 'bad'}">{fmt(best_common_delta)}</strong>。</li>
        <li>如果每个币种都单独选择自己的最佳过滤，五币种合并后的测试段总盈亏为 <strong>{fmt(best_per_coin.test_metrics.pnl)}</strong>，相对不过滤变化 <strong class="{'good' if best_per_coin_delta >= 0 else 'bad'}">{fmt(best_per_coin_delta)}</strong>。</li>
        <li>这份报告最适合回答两个问题：<strong>日线过滤值不值得加</strong>，以及 <strong>统一过滤更好</strong> 还是 <strong>分币种过滤更好</strong>。</li>
      </ul>
      <div class="note">
        这里的“最佳参数”指的是五个币种的低周期做多结构已经先固定为历史最佳模板，然后只在日线过滤这一层做横向比较，不是重新把低周期入场参数全部再扫一遍。
      </div>
    </section>

    <section class="section">
      <h2>做多参数口径</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>快线</th>
            <th>趋势线</th>
            <th>挂单参考</th>
            <th>止损</th>
          </tr>
        </thead>
        <tbody>
          {''.join(profile_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>三种情景总览</h2>
      <table>
        <thead>
          <tr>
            <th>情景</th>
            <th>测试段 PnL</th>
            <th>相对基线</th>
            <th>测试段交易数</th>
            <th>测试段胜率</th>
            <th>测试段 AvgR</th>
            <th>测试段 PF</th>
            <th>全样本 PnL</th>
          </tr>
        </thead>
        <tbody>
          {''.join(scenario_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>图表</h2>
      <div class="grid2">
        <div class="chart"><img src="data:image/png;base64,{summary_chart}" alt="三种情景对比" /></div>
        <div class="chart"><img src="data:image/png;base64,{coin_delta_chart}" alt="各币种不过滤 vs 最佳过滤" /></div>
      </div>
      <div class="chart" style="margin-top:18px;"><img src="data:image/png;base64,{yearly_chart}" alt="年度总盈亏对比" /></div>
    </section>

    <section class="section">
      <h2>统一过滤排行</h2>
      <table>
        <thead>
          <tr>
            <th>过滤门槛</th>
            <th>测试段 PnL</th>
            <th>相对不过滤</th>
            <th>测试段交易数</th>
            <th>测试段 AvgR</th>
            <th>测试段 PF</th>
            <th>全样本 PnL</th>
          </tr>
        </thead>
        <tbody>
          {''.join(aggregate_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>各币种最佳过滤</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>最佳过滤</th>
            <th>不过滤测试段 PnL</th>
            <th>最佳过滤测试段 PnL</th>
            <th>增量</th>
            <th>交易数</th>
            <th>AvgR</th>
          </tr>
        </thead>
        <tbody>
          {''.join(best_gate_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>年度拆分</h2>
      <table>
        <thead>
          <tr>
            <th>年份</th>
            <th>情景</th>
            <th>交易数</th>
            <th>总盈亏</th>
          </tr>
        </thead>
        <tbody>
          {''.join(yearly_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>输出文件</h2>
      <p>HTML：{html.escape(str(HTML_PATH))}</p>
      <p>CSV：{html.escape(str(CSV_PATH))}</p>
      <p>JSON：{html.escape(str(JSON_PATH))}</p>
    </section>
  </div>
</body>
</html>"""


def main() -> None:
    client = OkxRestClient()
    studies = [run_coin_study(client, symbol) for symbol in SYMBOLS]
    scenarios = build_scenarios(studies)
    gate_frame = build_gate_frame(studies)
    yearly = build_yearly_table(scenarios)

    gate_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(
        json.dumps(build_payload(studies, scenarios, gate_frame), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    HTML_PATH.write_text(build_html(studies, scenarios, gate_frame, yearly), encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
