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
from okx_quant.strategy_catalog import STRATEGY_EMA55_SLOPE_SHORT_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    INITIAL_CAPITAL,
    RISK_AMOUNT,
    SHORT_TAKER_FEE_RATE,
    SplitMetrics,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"ema55_slope_short_candidate_vs_original_5coins_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
SYMBOL_LABELS = {symbol: symbol.split("-")[0] for symbol in SYMBOLS}
ENTRY_LIMIT = 10000


@dataclass(frozen=True)
class Variant:
    key: str
    label: str
    note: str
    same_bar_block: bool = False
    dynamic_exit_requires_ema_reclaim: bool = False


@dataclass(frozen=True)
class CoinRun:
    symbol: str
    label: str
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    start_ts: int
    end_ts: int
    candle_count: int


@dataclass(frozen=True)
class VariantRun:
    variant: Variant
    coin_runs: list[CoinRun]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


VARIANTS = (
    Variant(
        key="original",
        label="原策略",
        note="当前 EMA55 斜率做空逻辑，动态保护出场后若斜率条件仍满足，可以继续再次开空。",
    ),
    Variant(
        key="candidate",
        label="候选版",
        note="新增两条约束：同根 K 线平仓后禁止再开；若因保本或锁盈类动态保护出场，必须先重新站上 EMA55，再跌回 EMA55 下方才允许重开。",
        same_bar_block=True,
        dynamic_exit_requires_ema_reclaim=True,
    ),
)


def build_config(symbol: str, variant: Variant) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
        big_ema_period=233,
        atr_period=14,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_EMA55_SLOPE_SHORT_ID,
        risk_amount=RISK_AMOUNT,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        take_profit_mode="dynamic",
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        ema55_slope_exit_enabled=False,
        ema55_slope_same_bar_reentry_block=variant.same_bar_block,
        ema55_slope_dynamic_exit_requires_ema_reclaim=variant.dynamic_exit_requires_ema_reclaim,
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
    )


def build_data_note(symbol: str, candle_count: int) -> str:
    return f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={candle_count}"


def run_coin_variant(client: OkxRestClient, symbol: str, variant: Variant) -> CoinRun:
    candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=ENTRY_LIMIT) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}")
    instrument = client.get_instrument(symbol)
    test_bounds = build_split_bounds(len(candles))["test"]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_config(symbol, variant),
        data_source_note=build_data_note(symbol, len(candles)),
        taker_fee_rate=SHORT_TAKER_FEE_RATE,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, test_bounds)
    return CoinRun(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_ts=candles[0].ts,
        end_ts=candles[-1].ts,
        candle_count=len(candles),
    )


def combine_runs(coin_runs: list[CoinRun]) -> tuple[list[BacktestTrade], list[BacktestTrade], SplitMetrics, SplitMetrics]:
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for item in coin_runs:
        trades.extend(item.trades)
        test_trades.extend(item.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return trades, test_trades, build_metrics(trades), build_metrics(test_trades)


def run_variant(client: OkxRestClient, variant: Variant) -> VariantRun:
    coin_runs = [run_coin_variant(client, symbol, variant) for symbol in SYMBOLS]
    trades, test_trades, all_metrics, test_metrics = combine_runs(coin_runs)
    return VariantRun(
        variant=variant,
        coin_runs=coin_runs,
        trades=trades,
        test_trades=test_trades,
        all_metrics=all_metrics,
        test_metrics=test_metrics,
    )


def summary_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    original = next(item for item in runs if item.variant.key == "original")
    rows: list[dict[str, object]] = []
    for run in runs:
        rows.append(
            {
                "variant_key": run.variant.key,
                "variant_label": run.variant.label,
                "all_pnl_u": float(run.all_metrics.pnl),
                "all_trades": run.all_metrics.trades,
                "all_win_rate": float(run.all_metrics.win_rate),
                "all_avg_r": float(run.all_metrics.avg_r),
                "all_profit_factor": None if run.all_metrics.profit_factor is None else float(run.all_metrics.profit_factor),
                "all_drawdown_u": float(run.all_metrics.max_drawdown),
                "test_pnl_u": float(run.test_metrics.pnl),
                "test_trades": run.test_metrics.trades,
                "test_win_rate": float(run.test_metrics.win_rate),
                "test_avg_r": float(run.test_metrics.avg_r),
                "test_profit_factor": None if run.test_metrics.profit_factor is None else float(run.test_metrics.profit_factor),
                "test_drawdown_u": float(run.test_metrics.max_drawdown),
                "test_delta_vs_original_u": float(run.test_metrics.pnl - original.test_metrics.pnl),
                "all_delta_vs_original_u": float(run.all_metrics.pnl - original.all_metrics.pnl),
                "note": run.variant.note,
            }
        )
    return rows


def coin_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    original = next(item for item in runs if item.variant.key == "original")
    original_map = {coin.symbol: coin for coin in original.coin_runs}
    rows: list[dict[str, object]] = []
    for run in runs:
        for coin in run.coin_runs:
            base = original_map[coin.symbol]
            rows.append(
                {
                    "variant_label": run.variant.label,
                    "coin": coin.label,
                    "all_pnl_u": float(coin.all_metrics.pnl),
                    "test_pnl_u": float(coin.test_metrics.pnl),
                    "test_delta_vs_original_u": float(coin.test_metrics.pnl - base.test_metrics.pnl),
                    "all_delta_vs_original_u": float(coin.all_metrics.pnl - base.all_metrics.pnl),
                    "test_trades": coin.test_metrics.trades,
                    "all_trades": coin.all_metrics.trades,
                }
            )
    return rows


def exit_reason_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run in runs:
        frame = pd.DataFrame({"exit_reason": [trade.exit_reason for trade in run.trades]})
        grouped = frame.groupby("exit_reason", as_index=False).size().rename(columns={"size": "count"})
        for record in grouped.to_dict("records"):
            rows.append(
                {
                    "variant_label": run.variant.label,
                    "exit_reason": record["exit_reason"],
                    "count": int(record["count"]),
                }
            )
    return rows


def yearly_rows(runs: list[VariantRun]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run in runs:
        frame = pd.DataFrame(
            {
                "exit_ts": [int(trade.exit_ts) for trade in run.trades],
                "pnl": [float(trade.pnl) for trade in run.trades],
            }
        )
        frame["year"] = pd.to_datetime(frame["exit_ts"], unit="ms", utc=True).dt.strftime("%Y")
        grouped = frame.groupby("year", as_index=False).agg(trades=("pnl", "size"), total_pnl_u=("pnl", "sum"))
        for record in grouped.to_dict("records"):
            rows.append(
                {
                    "variant_label": run.variant.label,
                    "year": record["year"],
                    "trades": int(record["trades"]),
                    "total_pnl_u": float(record["total_pnl_u"]),
                }
            )
    return rows


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def summary_chart(runs: list[VariantRun]):
    labels = [item.variant.label for item in runs]
    test_values = [float(item.test_metrics.pnl) for item in runs]
    all_values = [float(item.all_metrics.pnl) for item in runs]
    width = 0.34
    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    ax.bar([item - width / 2 for item in x], test_values, width=width, label="测试段 PnL", color="#b45309")
    ax.bar([item + width / 2 for item in x], all_values, width=width, label="全样本 PnL", color="#0f766e")
    ax.set_title("原策略 vs 候选版")
    ax.set_ylabel("PnL (U)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.axhline(0, color="#475569", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def coin_delta_chart(coin_frame: pd.DataFrame):
    original = coin_frame[coin_frame["variant_label"] == "原策略"].set_index("coin")
    candidate = coin_frame[coin_frame["variant_label"] == "候选版"].set_index("coin")
    labels = list(original.index)
    baseline_values = [float(original.loc[label, "test_pnl_u"]) for label in labels]
    candidate_values = [float(candidate.loc[label, "test_pnl_u"]) for label in labels]
    width = 0.34
    x = range(len(labels))
    fig, ax = plt.subplots(figsize=(9.6, 5.2))
    ax.bar([item - width / 2 for item in x], baseline_values, width=width, label="原策略", color="#64748b")
    ax.bar([item + width / 2 for item in x], candidate_values, width=width, label="候选版", color="#0f766e")
    ax.set_title("各币种测试段对比")
    ax.set_ylabel("PnL (U)")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels)
    ax.axhline(0, color="#475569", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    ax.legend()
    fig.tight_layout()
    return fig


def yearly_chart(yearly_frame: pd.DataFrame):
    pivot = yearly_frame.pivot(index="year", columns="variant_label", values="total_pnl_u").fillna(0).sort_index()
    fig, ax = plt.subplots(figsize=(9.6, 5.2))
    pivot.plot(kind="bar", ax=ax, width=0.75, color=["#64748b", "#0f766e"])
    ax.set_title("年度表现对比")
    ax.set_xlabel("")
    ax.set_ylabel("PnL (U)")
    ax.axhline(0, color="#475569", linewidth=1)
    ax.grid(axis="y", alpha=0.2)
    plt.xticks(rotation=0)
    fig.tight_layout()
    return fig


def fmt(value: Decimal | float | int, digits: int = 4) -> str:
    return format_decimal_fixed(Decimal(str(value)), digits)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


def build_html(runs: list[VariantRun], summary_frame: pd.DataFrame, coin_frame: pd.DataFrame, yearly_frame: pd.DataFrame) -> str:
    original = next(item for item in runs if item.variant.key == "original")
    candidate = next(item for item in runs if item.variant.key == "candidate")
    summary_plot = fig_to_base64(summary_chart(runs))
    coin_plot = fig_to_base64(coin_delta_chart(coin_frame))
    yearly_plot = fig_to_base64(yearly_chart(yearly_frame))

    summary_rows_html = []
    for row in summary_frame.to_dict("records"):
        summary_rows_html.append(
            "<tr>"
            f"<td>{html.escape(str(row['variant_label']))}</td>"
            f"<td>{fmt(row['test_pnl_u'])}</td>"
            f"<td>{fmt(row['test_delta_vs_original_u'])}</td>"
            f"<td>{int(row['test_trades'])}</td>"
            f"<td>{fmt(row['test_drawdown_u'])}</td>"
            f"<td>{fmt_pf(None if row['test_profit_factor'] is None else Decimal(str(row['test_profit_factor'])))}</td>"
            f"<td>{fmt(row['all_pnl_u'])}</td>"
            f"<td>{fmt(row['all_delta_vs_original_u'])}</td>"
            "</tr>"
        )

    coin_rows_html = []
    for row in coin_frame.sort_values(["coin", "variant_label"]).to_dict("records"):
        coin_rows_html.append(
            "<tr>"
            f"<td>{html.escape(str(row['coin']))}</td>"
            f"<td>{html.escape(str(row['variant_label']))}</td>"
            f"<td>{fmt(row['test_pnl_u'])}</td>"
            f"<td>{fmt(row['test_delta_vs_original_u'])}</td>"
            f"<td>{int(row['test_trades'])}</td>"
            f"<td>{fmt(row['all_pnl_u'])}</td>"
            f"<td>{fmt(row['all_delta_vs_original_u'])}</td>"
            "</tr>"
        )

    data_lines = "".join(
        f"<li><strong>{html.escape(run.label)}</strong>: {html.escape(format_ts(run.start_ts))} -> {html.escape(format_ts(run.end_ts))}, 1H={run.candle_count}</li>"
        for run in original.coin_runs
    )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMA55 斜率做空 候选版 vs 原策略</title>
  <style>
    :root {{
      --bg: #f6f8fb;
      --panel: #ffffff;
      --ink: #172033;
      --muted: #64748b;
      --line: rgba(23,32,51,0.10);
      --accent-a: #64748b;
      --accent-b: #0f766e;
      --shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "Microsoft YaHei", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15,118,110,0.08), transparent 26%),
        radial-gradient(circle at top right, rgba(100,116,139,0.08), transparent 22%),
        linear-gradient(180deg, #fbfcfe 0%, var(--bg) 100%);
    }}
    .wrap {{ width: min(1180px, calc(100vw - 28px)); margin: 0 auto; padding: 28px 0 56px; }}
    .hero {{
      border-radius: 26px;
      padding: 30px;
      color: white;
      background: linear-gradient(135deg, rgba(15,118,110,0.96), rgba(51,65,85,0.95));
      box-shadow: var(--shadow);
    }}
    .hero h1 {{ margin: 10px 0 8px; font-size: 34px; line-height: 1.08; }}
    .hero p {{ margin: 8px 0 0; max-width: 860px; line-height: 1.7; color: rgba(255,255,255,0.90); }}
    .grid {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: 18px; margin-top: 20px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 20px; box-shadow: var(--shadow); padding: 22px; }}
    .stat {{ grid-column: span 3; }}
    .wide {{ grid-column: span 6; }}
    .full {{ grid-column: 1 / -1; }}
    .k {{ color: var(--muted); font-size: 13px; }}
    .v {{ font-size: 30px; font-weight: 700; margin-top: 8px; color: var(--accent-b); }}
    .s {{ margin-top: 8px; color: var(--muted); line-height: 1.6; font-size: 13px; }}
    h2 {{ margin: 0 0 12px; font-size: 20px; }}
    h3 {{ margin: 0 0 8px; font-size: 16px; }}
    p {{ margin: 0; line-height: 1.7; }}
    ul {{ margin: 0; padding-left: 18px; line-height: 1.8; }}
    img {{ width: 100%; border-radius: 16px; border: 1px solid var(--line); background: white; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(23,32,51,0.08); white-space: nowrap; }}
    th {{ color: var(--muted); background: #f7fafc; position: sticky; top: 0; }}
    .scroll {{ overflow: auto; }}
    .note {{ color: var(--muted); font-size: 13px; line-height: 1.7; }}
    @media (max-width: 960px) {{ .stat, .wide {{ grid-column: 1 / -1; }} .hero h1 {{ font-size: 28px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>EMA55 斜率做空：候选版 vs 原策略</h1>
      <p>这次只比较两版：<strong>原策略</strong> 保持不动；<strong>候选版</strong> 在原策略上加两条再入场约束：同根 K 线平仓后禁止再开，以及动态保护出场后必须先重新站上 EMA55，再跌回 EMA55 下方，才允许下一次开空。</p>
    </section>

    <section class="grid">
      <div class="card stat">
        <div class="k">原策略测试段</div>
        <div class="v">{fmt(original.test_metrics.pnl)}U</div>
        <div class="s">全样本 {fmt(original.all_metrics.pnl)}U</div>
      </div>
      <div class="card stat">
        <div class="k">候选版测试段</div>
        <div class="v">{fmt(candidate.test_metrics.pnl)}U</div>
        <div class="s">全样本 {fmt(candidate.all_metrics.pnl)}U</div>
      </div>
      <div class="card stat">
        <div class="k">测试段差值</div>
        <div class="v">{fmt(candidate.test_metrics.pnl - original.test_metrics.pnl)}U</div>
        <div class="s">候选版减原策略</div>
      </div>
      <div class="card stat">
        <div class="k">回撤差值</div>
        <div class="v">{fmt(candidate.test_metrics.max_drawdown - original.test_metrics.max_drawdown)}U</div>
        <div class="s">测试段候选版减原策略</div>
      </div>

      <div class="card wide">
        <h2>原策略</h2>
        <p>{html.escape(original.variant.note)}</p>
      </div>
      <div class="card wide">
        <h2>候选版</h2>
        <p>{html.escape(candidate.variant.note)}</p>
      </div>

      <div class="card full">
        <h2>数据覆盖</h2>
        <ul>{data_lines}</ul>
      </div>

      <div class="card wide">
        <h2>总盈亏对比</h2>
        <img src="data:image/png;base64,{summary_plot}" alt="summary_chart">
      </div>
      <div class="card wide">
        <h2>各币种测试段对比</h2>
        <img src="data:image/png;base64,{coin_plot}" alt="coin_chart">
      </div>

      <div class="card full">
        <h2>总表</h2>
        <div class="note">这里直接看候选版相对原策略的测试段差值和全样本差值，最方便判断这次约束到底换来了什么。</div>
        <div class="scroll" style="margin-top:12px;">
          <table>
            <thead><tr><th>版本</th><th>测试段PnL</th><th>测试段差值</th><th>测试段笔数</th><th>测试段回撤</th><th>测试段PF</th><th>全样本PnL</th><th>全样本差值</th></tr></thead>
            <tbody>{''.join(summary_rows_html)}</tbody>
          </table>
        </div>
      </div>

      <div class="card full">
        <h2>各币种明细</h2>
        <div class="scroll">
          <table>
            <thead><tr><th>币种</th><th>版本</th><th>测试段PnL</th><th>测试段差值</th><th>测试段笔数</th><th>全样本PnL</th><th>全样本差值</th></tr></thead>
            <tbody>{''.join(coin_rows_html)}</tbody>
          </table>
        </div>
      </div>

      <div class="card full">
        <h2>年度表现</h2>
        <img src="data:image/png;base64,{yearly_plot}" alt="yearly_chart">
      </div>
    </section>
  </div>
</body>
</html>"""


def build_payload(runs: list[VariantRun]) -> dict[str, object]:
    summary_frame = pd.DataFrame(summary_rows(runs))
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_bar": ENTRY_BAR,
        "risk_amount": str(RISK_AMOUNT),
        "variants": [variant.__dict__ for variant in VARIANTS],
        "summary_rows": summary_frame.to_dict("records"),
        "coin_rows": coin_rows(runs),
        "exit_reason_rows": exit_reason_rows(runs),
        "yearly_rows": yearly_rows(runs),
    }


def main() -> None:
    client = OkxRestClient()
    runs = [run_variant(client, variant) for variant in VARIANTS]
    summary_frame = pd.DataFrame(summary_rows(runs))
    coin_frame = pd.DataFrame(coin_rows(runs))
    yearly_frame = pd.DataFrame(yearly_rows(runs))

    summary_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(json.dumps(build_payload(runs), ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(runs, summary_frame, coin_frame, yearly_frame), encoding="utf-8")
    print(HTML_PATH)


if __name__ == "__main__":
    main()
