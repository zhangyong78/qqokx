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
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    SplitMetrics,
    Variant,
    build_daily_direction_bias,
    build_metrics,
    build_split_bounds,
    filter_split_trades,
    format_ts,
)


plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


REPORT_DIR = analysis_report_dir_path()
REPORT_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"first_batch_3long_daily_filter_matrix_10u_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
COIN_CSV_PATH = REPORT_DIR / f"{BASENAME}_by_coin.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
PROJECT_HTML_PATH = PROJECT_REPORT_DIR / "first_batch_3long_daily_filter_matrix_10u.html"

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
)
SYMBOL_LABELS = {
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
    "SOL-USDT-SWAP": "SOL",
}

VARIANTS = (
    Variant(key="none", label="不过滤"),
    Variant(key="ema_5", label="日线 EMA5", ma_type="ema", period=5),
    Variant(key="ema_8", label="日线 EMA8", ma_type="ema", period=8),
    Variant(key="ema_13", label="日线 EMA13", ma_type="ema", period=13),
    Variant(key="ema_21", label="日线 EMA21", ma_type="ema", period=21),
    Variant(key="ma_5", label="日线 MA5", ma_type="ma", period=5),
    Variant(key="ma_8", label="日线 MA8", ma_type="ma", period=8),
    Variant(key="ma_13", label="日线 MA13", ma_type="ma", period=13),
    Variant(key="ma_21", label="日线 MA21", ma_type="ma", period=21),
)


@dataclass(frozen=True)
class OldProfile:
    symbol: str
    label: str
    ema_period: int
    ema_type: str
    trend_ema_period: int
    trend_ema_type: str
    entry_reference_ema_period: int
    entry_reference_ema_type: str
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal

    @property
    def profile_label(self) -> str:
        return (
            f"{self.ema_type.upper()}{self.ema_period} / "
            f"{self.trend_ema_type.upper()}{self.trend_ema_period} / "
            f"挂单 {self.entry_reference_ema_type.upper()}{self.entry_reference_ema_period} / "
            f"SLx{self.atr_stop_multiplier}"
        )


@dataclass(frozen=True)
class CoinRun:
    symbol: str
    label: str
    variant: Variant
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    start_ts: int
    end_ts: int
    candle_count: int
    filter_count: int


@dataclass(frozen=True)
class VariantRun:
    variant: Variant
    coin_runs: list[CoinRun]
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics


PROFILES = {
    "BTC-USDT-SWAP": OldProfile(
        symbol="BTC-USDT-SWAP",
        label="BTC",
        ema_period=21,
        ema_type="ema",
        trend_ema_period=50,
        trend_ema_type="ma",
        entry_reference_ema_period=50,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
    ),
    "ETH-USDT-SWAP": OldProfile(
        symbol="ETH-USDT-SWAP",
        label="ETH",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ema",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
    ),
    "SOL-USDT-SWAP": OldProfile(
        symbol="SOL-USDT-SWAP",
        label="SOL",
        ema_period=21,
        ema_type="ma",
        trend_ema_period=55,
        trend_ema_type="ma",
        entry_reference_ema_period=55,
        entry_reference_ema_type="ma",
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
    ),
}


def main() -> None:
    client = OkxRestClient()
    loaded: dict[str, tuple[list, list, object, object]] = {}
    for symbol in SYMBOLS:
        entry_candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=None) if candle.confirmed]
        filter_candles = [candle for candle in load_candle_cache(symbol, FILTER_BAR, limit=None) if candle.confirmed]
        if not entry_candles or not filter_candles:
            raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}/{FILTER_BAR}")
        loaded[symbol] = (
            entry_candles,
            filter_candles,
            client.get_instrument(symbol),
            build_split_bounds(len(entry_candles))["test"],
        )

    runs = [run_variant(loaded, variant) for variant in VARIANTS]
    summary_frame = build_summary_frame(runs)
    coin_frame = build_coin_frame(runs)
    summary_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    coin_frame.to_csv(COIN_CSV_PATH, index=False, encoding="utf-8-sig")
    JSON_PATH.write_text(json.dumps(build_payload(runs, summary_frame, coin_frame), ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(runs, summary_frame, coin_frame), encoding="utf-8")
    copyfile(HTML_PATH, PROJECT_HTML_PATH)
    print(HTML_PATH)
    print(PROJECT_HTML_PATH)


def run_variant(loaded: dict[str, tuple[list, list, object, object]], variant: Variant) -> VariantRun:
    coin_runs = [run_coin(loaded, variant, symbol) for symbol in SYMBOLS]
    trades: list[BacktestTrade] = []
    test_trades: list[BacktestTrade] = []
    for run in coin_runs:
        trades.extend(run.trades)
        test_trades.extend(run.test_trades)
    trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
    return VariantRun(
        variant=variant,
        coin_runs=coin_runs,
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
    )


def run_coin(loaded: dict[str, tuple[list, list, object, object]], variant: Variant, symbol: str) -> CoinRun:
    entry_candles, filter_candles, instrument, test_bounds = loaded[symbol]
    bias = None
    if variant.period:
        bias = build_daily_direction_bias(entry_candles, filter_candles, variant)
    print(f"run {SYMBOL_LABELS[symbol]} {variant.label} candles={len(entry_candles)}")
    result = _run_backtest_with_loaded_data(
        entry_candles,
        instrument,
        build_config(symbol),
        data_source_note=f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={len(entry_candles)}",
        maker_fee_rate=LONG_MAKER_FEE_RATE,
        taker_fee_rate=LONG_TAKER_FEE_RATE,
        direction_filter_bias=bias,
    )
    trades = list(result.trades)
    test_trades = filter_split_trades(trades, test_bounds)
    return CoinRun(
        symbol=symbol,
        label=SYMBOL_LABELS[symbol],
        variant=variant,
        trades=trades,
        test_trades=test_trades,
        all_metrics=build_metrics(trades),
        test_metrics=build_metrics(test_trades),
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        candle_count=len(entry_candles),
        filter_count=len(filter_candles),
    )


def build_config(symbol: str) -> StrategyConfig:
    profile = PROFILES[symbol]
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=profile.ema_period,
        ema_type=profile.ema_type,
        trend_ema_period=profile.trend_ema_period,
        trend_ema_type=profile.trend_ema_type,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=profile.atr_stop_multiplier,
        atr_take_multiplier=profile.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        risk_amount=RISK_AMOUNT,
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


def build_summary_frame(runs: list[VariantRun]) -> pd.DataFrame:
    baseline = next(run for run in runs if run.variant.key == "none")
    rows: list[dict[str, object]] = []
    for run in runs:
        rows.append(
            {
                "variant": run.variant.label,
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
            }
        )
    return pd.DataFrame(rows)


def build_coin_frame(runs: list[VariantRun]) -> pd.DataFrame:
    baseline = next(run for run in runs if run.variant.key == "none")
    baseline_by_symbol = {coin.symbol: coin for coin in baseline.coin_runs}
    rows: list[dict[str, object]] = []
    for run in runs:
        for coin in run.coin_runs:
            base_coin = baseline_by_symbol[coin.symbol]
            rows.append(
                {
                    "variant": run.variant.label,
                    "coin": coin.label,
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


def build_payload(runs: list[VariantRun], summary_frame: pd.DataFrame, coin_frame: pd.DataFrame) -> dict[str, object]:
    return {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "risk_amount": str(RISK_AMOUNT),
        "symbols": list(SYMBOLS),
        "yesterday_daily_note": "方向过滤使用 confirmed 1D K 线，并通过 latest_closed_candle_index 对齐到每根 1H K 当时最近一根已收盘日线，等价于使用昨天已收完的日线状态。",
        "profiles": {
            SYMBOL_LABELS[symbol]: {
                "profile_label": PROFILES[symbol].profile_label,
            }
            for symbol in SYMBOLS
        },
        "summary": summary_frame.to_dict("records"),
        "by_coin": coin_frame.to_dict("records"),
        "html_path": str(HTML_PATH),
        "project_html_path": str(PROJECT_HTML_PATH),
    }


def build_html(runs: list[VariantRun], summary_frame: pd.DataFrame, coin_frame: pd.DataFrame) -> str:
    baseline = next(run for run in runs if run.variant.key == "none")
    ranked_test = sorted(runs[1:], key=lambda item: item.test_metrics.pnl, reverse=True)
    ranked_all = sorted(runs[1:], key=lambda item: item.all_metrics.pnl, reverse=True)
    chart = fig_to_base64(build_chart(summary_frame))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>5月首批三多参数 + 日线过滤矩阵</title>
  <style>
    body {{ margin: 0; font-family: "Microsoft YaHei", "Segoe UI", sans-serif; background: #f6f7f3; color: #1f2937; }}
    .wrap {{ max-width: 1380px; margin: 0 auto; padding: 28px 24px 48px; }}
    .hero {{ background: #17202a; color: white; padding: 24px 28px; border-radius: 12px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; }}
    .card {{ background: white; border: 1px solid #dde3dc; border-radius: 10px; padding: 16px; }}
    .label {{ color: #667085; font-size: 13px; }}
    .value {{ font-size: 26px; font-weight: 700; margin-top: 6px; }}
    .good {{ color: #047857; font-weight: 700; }}
    .bad {{ color: #b42318; font-weight: 700; }}
    h1, h2 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 12px; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid #e5e7eb; text-align: right; white-space: nowrap; font-size: 13px; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #e9eee8; color: #344054; }}
    img {{ width: 100%; border-radius: 8px; border: 1px solid #dde3dc; background: white; }}
    .note {{ color: #667085; line-height: 1.8; }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>5月首批三多参数 + 日线过滤矩阵</h1>
      <p>口径：BTC / ETH / SOL，1H 做多，10U 固定风险，全历史，不混入做空。</p>
      <p>日线过滤用 confirmed 1D K，对每根 1H K 只取当时最近一根已收盘日线，等价于“昨天的日线状态”。</p>
      <p>参数：BTC = EMA21 / MA50 / 挂单MA50 / SL2；ETH = MA21 / EMA55 / 挂单MA55 / SL2；SOL = MA21 / MA55 / 挂单MA55 / SL1。</p>
    </section>

    <div class="grid">
      <div class="card"><div class="label">不过滤全样本</div><div class="value">{fmt_u(baseline.all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">最佳全样本过滤</div><div class="value">{fmt_u(ranked_all[0].all_metrics.pnl)}</div></div>
      <div class="card"><div class="label">最佳测试段过滤</div><div class="value">{fmt_u(ranked_test[0].test_metrics.pnl)}</div></div>
      <div class="card"><div class="label">最佳测试段相对不过滤</div><div class="value {class_for(ranked_test[0].test_metrics.pnl - baseline.test_metrics.pnl)}">{fmt_signed_u(ranked_test[0].test_metrics.pnl - baseline.test_metrics.pnl)}</div></div>
    </div>

    <h2>关键结论</h2>
    <div class="card note">
      <p>全样本最强：{html.escape(ranked_all[0].variant.label)}，全样本 {fmt_u(ranked_all[0].all_metrics.pnl)}，相对不过滤 {fmt_signed_u(ranked_all[0].all_metrics.pnl - baseline.all_metrics.pnl)}。</p>
      <p>测试段最强：{html.escape(ranked_test[0].variant.label)}，测试段 {fmt_u(ranked_test[0].test_metrics.pnl)}，相对不过滤 {fmt_signed_u(ranked_test[0].test_metrics.pnl - baseline.test_metrics.pnl)}。</p>
      <p>“昨天日线”口径说明：过滤信号来自已确认日线，不会读取当天未收盘日线。</p>
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
    fig, ax = plt.subplots(figsize=(11.2, 5.4))
    x = range(len(frame))
    width = 0.35
    ax.bar([i - width / 2 for i in x], frame["all_pnl_u"], width=width, label="全样本", color="#2563eb")
    ax.bar([i + width / 2 for i in x], frame["test_pnl_u"], width=width, label="测试段", color="#16a34a")
    ax.set_xticks(list(x))
    ax.set_xticklabels(frame["variant"], rotation=15, ha="right")
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


def fmt_u(value: Decimal) -> str:
    return f"{float(value):,.2f}U"


def fmt_signed_u(value: Decimal) -> str:
    raw = float(value)
    return f"{'+' if raw >= 0 else ''}{raw:,.2f}U"


def class_for(value: Decimal) -> str:
    return "good" if value >= 0 else "bad"


if __name__ == "__main__":
    main()
