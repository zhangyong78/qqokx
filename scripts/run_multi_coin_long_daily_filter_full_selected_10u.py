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
from shutil import copyfile

import matplotlib.pyplot as plt
import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import BacktestTrade, _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
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
from scripts.run_multi_coin_best_long_daily_gate_report import GATES, SYMBOL_LABELS
from scripts.run_multi_coin_slope_vs_dynamic_long_daily_filter_10u import (
    build_dynamic_long_config,
    build_slope_long_config,
    run_slope_long_with_bias,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"multi_coin_long_daily_filter_full_selected_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
COIN_CSV_PATH = REPORT_DIR / f"{BASENAME}_by_coin.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "multi_coin_long_daily_filter_full_selected_10u.html"

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
GATE_BY_KEY = {gate.key: gate for gate in GATES}

DYNAMIC_FILTER_GATES = {
    "BTC-USDT-SWAP": "ma_8",
    "ETH-USDT-SWAP": "ma_13",
    "SOL-USDT-SWAP": "ma_13",
    "BNB-USDT-SWAP": "ema_8",
    "DOGE-USDT-SWAP": "ema_13",
}
SLOPE_FILTER_GATES = {
    "BTC-USDT-SWAP": "ema_13",
    "ETH-USDT-SWAP": "ma_13",
    "SOL-USDT-SWAP": "ma_13",
    "BNB-USDT-SWAP": "ma_13",
    "DOGE-USDT-SWAP": "ma_5",
}


@dataclass(frozen=True)
class ScenarioSpec:
    key: str
    label: str
    strategy_key: str
    selected_gates: dict[str, str]


@dataclass(frozen=True)
class CoinRun:
    symbol: str
    label: str
    scenario: ScenarioSpec
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    start_ts: int
    end_ts: int
    candle_count: int
    filter_count: int


@dataclass(frozen=True)
class ScenarioRun:
    spec: ScenarioSpec
    coin_runs: list[CoinRun]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


SCENARIOS = (
    ScenarioSpec("dynamic_none", "动态委托做多 / 不过滤", "dynamic_long", {symbol: "none" for symbol in SYMBOLS}),
    ScenarioSpec("dynamic_filtered", "动态委托做多 / 日线过滤", "dynamic_long", DYNAMIC_FILTER_GATES),
    ScenarioSpec("slope_none", "EMA55斜率做多 / 不过滤", "slope_long", {symbol: "none" for symbol in SYMBOLS}),
    ScenarioSpec("slope_filtered", "EMA55斜率做多 / 日线过滤", "slope_long", SLOPE_FILTER_GATES),
)


def main() -> None:
    client = OkxRestClient()
    loaded: dict[str, tuple[list, list, object, object]] = {}
    for symbol in SYMBOLS:
        entry_candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
        filter_candles = [candle for candle in load_candle_cache(symbol, FILTER_BAR, limit=None) if candle.confirmed]
        if not entry_candles or not filter_candles:
            raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}/{FILTER_BAR}")
        loaded[symbol] = (entry_candles, filter_candles, client.get_instrument(symbol), build_split_bounds(len(entry_candles))["test"])

    runs = [run_scenario(loaded, spec) for spec in SCENARIOS]
    summary_frame = build_summary_frame(runs)
    coin_frame = build_coin_frame(runs)
    summary_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(json.dumps(build_payload(runs, summary_frame, coin_frame), ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(runs, summary_frame, coin_frame), encoding="utf-8")
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


def run_scenario(loaded: dict[str, tuple[list, list, object, object]], spec: ScenarioSpec) -> ScenarioRun:
    coin_runs = [run_coin(loaded, spec, symbol) for symbol in SYMBOLS]
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in coin_runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return ScenarioRun(
        spec=spec,
        coin_runs=coin_runs,
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
    )


def run_coin(loaded: dict[str, tuple[list, list, object, object]], spec: ScenarioSpec, symbol: str) -> CoinRun:
    entry_candles, filter_candles, instrument, test_bounds = loaded[symbol]
    gate_key = spec.selected_gates[symbol]
    bias = None
    if gate_key != "none":
        bias = build_daily_direction_bias(entry_candles, filter_candles, GATE_BY_KEY[gate_key])
    print(f"run {SYMBOL_LABELS[symbol]} {spec.label} gate={gate_key} candles={len(entry_candles)}")
    if spec.strategy_key == "dynamic_long":
        result = _run_backtest_with_loaded_data(
            entry_candles,
            instrument,
            build_dynamic_long_config(symbol),
            data_source_note=f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={len(entry_candles)}",
            maker_fee_rate=LONG_MAKER_FEE_RATE,
            taker_fee_rate=LONG_TAKER_FEE_RATE,
            direction_filter_bias=bias,
        )
        trades = list(result.trades)
    elif spec.strategy_key == "slope_long":
        trades = run_slope_long_with_bias(entry_candles, instrument, build_slope_long_config(symbol), bias)
    else:
        raise ValueError(f"unsupported strategy: {spec.strategy_key}")
    test_trades = filter_split_trades(trades, test_bounds)
    return CoinRun(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        scenario=spec,
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        candle_count=len(entry_candles),
        filter_count=len(filter_candles),
    )


def build_summary_frame(runs: list[ScenarioRun]) -> pd.DataFrame:
    baseline_by_strategy = {
        "dynamic_long": next(run for run in runs if run.spec.key == "dynamic_none"),
        "slope_long": next(run for run in runs if run.spec.key == "slope_none"),
    }
    rows: list[dict[str, object]] = []
    for run in runs:
        baseline = baseline_by_strategy[run.spec.strategy_key]
        rows.append(
            {
                "scenario": run.spec.label,
                "strategy_key": run.spec.strategy_key,
                "all_pnl_u": float(run.all_metrics.pnl),
                "all_trades": run.all_metrics.trades,
                "all_win_rate_pct": float(run.all_metrics.win_rate),
                "all_profit_factor": none_or_float(run.all_metrics.profit_factor),
                "all_avg_r": float(run.all_metrics.avg_r),
                "all_drawdown_u": float(run.all_metrics.max_drawdown),
                "test_pnl_u": float(run.test_metrics.pnl),
                "test_trades": run.test_metrics.trades,
                "test_win_rate_pct": float(run.test_metrics.win_rate),
                "test_profit_factor": none_or_float(run.test_metrics.profit_factor),
                "test_avg_r": float(run.test_metrics.avg_r),
                "test_drawdown_u": float(run.test_metrics.max_drawdown),
                "all_delta_vs_no_filter_u": float(run.all_metrics.pnl - baseline.all_metrics.pnl),
                "test_delta_vs_no_filter_u": float(run.test_metrics.pnl - baseline.test_metrics.pnl),
                "all_drawdown_delta_u": float(run.all_metrics.max_drawdown - baseline.all_metrics.max_drawdown),
                "test_drawdown_delta_u": float(run.test_metrics.max_drawdown - baseline.test_metrics.max_drawdown),
                "selected_gates": format_selected_gates(run.spec.selected_gates),
            }
        )
    return pd.DataFrame(rows)


def build_coin_frame(runs: list[ScenarioRun]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for run in runs:
        baseline = next(
            item
            for item in runs
            if item.spec.strategy_key == run.spec.strategy_key and item.spec.selected_gates == {symbol: "none" for symbol in SYMBOLS}
        )
        baseline_by_symbol = {coin.symbol: coin for coin in baseline.coin_runs}
        for coin in run.coin_runs:
            base_coin = baseline_by_symbol[coin.symbol]
            rows.append(
                {
                    "scenario": run.spec.label,
                    "coin": coin.label,
                    "gate": gate_label(run.spec.selected_gates[coin.symbol]),
                    "start": format_ts(coin.start_ts),
                    "end": format_ts(coin.end_ts),
                    "candles": coin.candle_count,
                    "all_pnl_u": float(coin.all_metrics.pnl),
                    "test_pnl_u": float(coin.test_metrics.pnl),
                    "all_delta_vs_no_filter_u": float(coin.all_metrics.pnl - base_coin.all_metrics.pnl),
                    "test_delta_vs_no_filter_u": float(coin.test_metrics.pnl - base_coin.test_metrics.pnl),
                    "all_trades": coin.all_metrics.trades,
                    "test_trades": coin.test_metrics.trades,
                    "test_drawdown_u": float(coin.test_metrics.max_drawdown),
                }
            )
    return pd.DataFrame(rows)


def build_payload(runs: list[ScenarioRun], summary_frame: pd.DataFrame, coin_frame: pd.DataFrame) -> dict[str, object]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "risk_amount": str(RISK_AMOUNT),
        "entry_limit": None,
        "summary": summary_frame.to_dict("records"),
        "by_coin": coin_frame.to_dict("records"),
        "html_path": str(HTML_PATH),
        "project_html_path": str(PROJECT_HTML_PATH),
    }


def build_html(runs: list[ScenarioRun], summary_frame: pd.DataFrame, coin_frame: pd.DataFrame) -> str:
    dynamic_none = next(run for run in runs if run.spec.key == "dynamic_none")
    dynamic_filtered = next(run for run in runs if run.spec.key == "dynamic_filtered")
    slope_none = next(run for run in runs if run.spec.key == "slope_none")
    slope_filtered = next(run for run in runs if run.spec.key == "slope_filtered")
    chart = fig_to_base64(build_chart(summary_frame))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>动态做多 / 斜率做多 日线过滤全历史对比</title>
  <style>
    body {{ margin: 0; font-family: "Microsoft YaHei", "Segoe UI", sans-serif; background: #f6f7f3; color: #1f2937; }}
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 28px 24px 48px; }}
    .hero {{ background: #17202a; color: white; padding: 24px 28px; border-radius: 10px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: white; border: 1px solid #dde3dc; border-radius: 8px; padding: 16px; }}
    .label {{ color: #667085; font-size: 13px; }}
    .value {{ font-size: 26px; font-weight: 700; margin-top: 6px; }}
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 12px; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid #e5e7eb; text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #e9eee8; color: #344054; }}
    img {{ width: 100%; border-radius: 8px; border: 1px solid #dde3dc; background: white; }}
    .note {{ color: #667085; line-height: 1.7; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>动态做多 / EMA55斜率做多：日线过滤全历史对比</h1>
      <p>口径：5 币种，1H 入场，1D 过滤，10U 固定风险，读取本地缓存全历史。</p>
      <p>动态做多使用当前 5 币种各自最佳参数；EMA55 斜率做多使用镜像做多参数：EMA55 单根斜率比例 >= 0.0005 入场，斜率转负出场，ATR14 / 2ATR 止损 / 动态止盈。</p>
    </section>

    <div class="grid">
      <div class="card"><div class="label">动态做多 不过滤 全样本</div><div class="value">{fmt_u(dynamic_none.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">动态做多 日线过滤 全样本</div><div class="value">{fmt_u(dynamic_filtered.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">动态过滤 全样本差值</div><div class="value {class_for(dynamic_filtered.all_metrics.pnl - dynamic_none.all_metrics.pnl)}">{fmt_signed_u(dynamic_filtered.all_metrics.pnl - dynamic_none.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">斜率做多 不过滤 全样本</div><div class="value">{fmt_u(slope_none.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">斜率做多 日线过滤 全样本</div><div class="value">{fmt_u(slope_filtered.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">斜率过滤 全样本差值</div><div class="value {class_for(slope_filtered.all_metrics.pnl - slope_none.all_metrics.pnl)}">{fmt_signed_u(slope_filtered.all_metrics.pnl - slope_none.all_metrics.pnl)}</div></div>
    </div>

    <h2>关键结论</h2>
    <div class="card note">
      <p>动态做多：日线过滤后全样本 {fmt_signed_u(dynamic_filtered.all_metrics.pnl - dynamic_none.all_metrics.pnl)}，测试段 {fmt_signed_u(dynamic_filtered.test_metrics.pnl - dynamic_none.test_metrics.pnl)}。</p>
      <p>EMA55斜率做多：日线过滤后全样本 {fmt_signed_u(slope_filtered.all_metrics.pnl - slope_none.all_metrics.pnl)}，测试段 {fmt_signed_u(slope_filtered.test_metrics.pnl - slope_none.test_metrics.pnl)}。</p>
    </div>

    <h2>汇总表</h2>
    {dataframe_to_html(summary_frame)}

    <h2>图表</h2>
    <img alt="summary chart" src="data:image/png;base64,{chart}">

    <h2>分币种明细</h2>
    {dataframe_to_html(coin_frame)}
  </div>
</body>
</html>"""


def build_chart(summary_frame: pd.DataFrame):
    frame = summary_frame.copy()
    fig, ax = plt.subplots(figsize=(10.8, 5.2))
    x = range(len(frame))
    width = 0.35
    ax.bar([i - width / 2 for i in x], frame["all_pnl_u"], width=width, label="全样本", color="#2563eb")
    ax.bar([i + width / 2 for i in x], frame["test_pnl_u"], width=width, label="测试段", color="#16a34a")
    ax.set_xticks(list(x))
    ax.set_xticklabels(frame["scenario"], rotation=10, ha="right")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475467", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def dataframe_to_html(frame: pd.DataFrame) -> str:
    display = frame.copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(lambda value: "" if pd.isna(value) else f"{value:,.2f}")
    return display.to_html(index=False, escape=False)


def none_or_float(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def gate_label(key: str) -> str:
    if key == "none":
        return "无过滤"
    return GATE_BY_KEY[key].label


def format_selected_gates(selected: dict[str, str]) -> str:
    return " / ".join(f"{SYMBOL_LABELS[symbol]}:{gate_label(key)}" for symbol, key in selected.items())


def fmt_u(value: Decimal) -> str:
    return f"{float(value):,.2f}U"


def fmt_signed_u(value: Decimal) -> str:
    raw = float(value)
    return f"{'+' if raw >= 0 else ''}{raw:,.2f}U"


def class_for(value: Decimal) -> str:
    return "good" if value >= 0 else "bad"


if __name__ == "__main__":
    main()
