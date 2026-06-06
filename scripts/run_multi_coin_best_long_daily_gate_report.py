from __future__ import annotations

import base64
import html
import io
import json
import sys
from dataclasses import asdict, dataclass
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
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_EMA55_SLOPE_SHORT_ID
from scripts.run_btc_daily_ma_direction_filter_research import (
    ENTRY_BAR,
    FILTER_BAR,
    INITIAL_CAPITAL,
    LONG_MAKER_FEE_RATE,
    LONG_TAKER_FEE_RATE,
    RISK_AMOUNT,
    SHORT_TAKER_FEE_RATE,
    SplitMetrics,
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

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
HTML_PATH = REPORT_DIR / f"multi_coin_best_long_daily_gate_report_{STAMP}.html"
CSV_PATH = REPORT_DIR / f"multi_coin_best_long_daily_gate_report_{STAMP}.csv"
JSON_PATH = REPORT_DIR / f"multi_coin_best_long_daily_gate_report_{STAMP}.json"
ENTRY_LIMIT = 10000


SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
SYMBOL_LABELS = {
    "BTC-USDT-SWAP": "BTC",
    "ETH-USDT-SWAP": "ETH",
    "SOL-USDT-SWAP": "SOL",
    "BNB-USDT-SWAP": "BNB",
    "DOGE-USDT-SWAP": "DOGE",
}


@dataclass(frozen=True)
class GateOption:
    key: str
    label: str
    ma_type: str | None = None
    period: int | None = None


@dataclass(frozen=True)
class LongProfile:
    symbol: str
    label: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal

    @property
    def entry_label(self) -> str:
        if self.entry_reference_ema_period <= 0:
            return f"跟随快线 EMA{self.ema_period}"
        return f"EMA{self.entry_reference_ema_period}"

    @property
    def profile_label(self) -> str:
        return (
            f"EMA{self.ema_period}/EMA{self.trend_ema_period}"
            f" + {self.entry_label}"
            f" + SLx{format_decimal_fixed(self.atr_stop_multiplier, 1)}"
        )


@dataclass(frozen=True)
class SideResult:
    gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]


@dataclass(frozen=True)
class ComboResult:
    long_gate: GateOption
    short_gate: GateOption
    all_metrics: SplitMetrics
    test_metrics: SplitMetrics
    trades: list[BacktestTrade]
    test_trades: list[BacktestTrade]


@dataclass(frozen=True)
class SymbolStudy:
    symbol: str
    label: str
    long_profile: LongProfile
    entry_candles: int
    filter_candles: int
    start_ts: int
    end_ts: int
    long_results: dict[str, SideResult]
    short_results: dict[str, SideResult]
    combos: list[ComboResult]


GATES = (
    GateOption("none", "无过滤"),
    GateOption("ema_5", "EMA5", "ema", 5),
    GateOption("ma_5", "MA5", "ma", 5),
    GateOption("ma_8", "MA8", "ma", 8),
    GateOption("ema_8", "EMA8", "ema", 8),
    GateOption("ema_13", "EMA13", "ema", 13),
    GateOption("ma_13", "MA13", "ma", 13),
)

LONG_PROFILES = {
    "BTC-USDT-SWAP": LongProfile("BTC-USDT-SWAP", "BTC", 5, 13, 0, Decimal("1")),
    "ETH-USDT-SWAP": LongProfile("ETH-USDT-SWAP", "ETH", 21, 55, 34, Decimal("1.5")),
    "SOL-USDT-SWAP": LongProfile("SOL-USDT-SWAP", "SOL", 21, 55, 13, Decimal("1")),
    "BNB-USDT-SWAP": LongProfile("BNB-USDT-SWAP", "BNB", 8, 21, 13, Decimal("1")),
    "DOGE-USDT-SWAP": LongProfile("DOGE-USDT-SWAP", "DOGE", 5, 13, 0, Decimal("1.5")),
}


def main() -> None:
    client = OkxRestClient()
    studies: list[SymbolStudy] = []
    for symbol in SYMBOLS:
        studies.append(run_symbol_study(client, symbol))

    aggregate_combos = build_aggregate_combos(studies)
    combo_frame = build_combo_frame(studies, aggregate_combos)
    combo_frame.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    payload = build_payload(studies, aggregate_combos)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(build_html(studies, aggregate_combos, combo_frame), encoding="utf-8")
    print(HTML_PATH)


def run_symbol_study(client: OkxRestClient, symbol: str) -> SymbolStudy:
    label = SYMBOL_LABELS[symbol]
    entry_candles = [candle for candle in load_candle_cache(symbol, ENTRY_BAR, limit=ENTRY_LIMIT) if candle.confirmed]
    filter_candles = [candle for candle in load_candle_cache(symbol, FILTER_BAR, limit=None) if candle.confirmed]
    if not entry_candles or not filter_candles:
        raise RuntimeError(f"missing local candles for {symbol} {ENTRY_BAR}/{FILTER_BAR}")

    instrument = client.get_instrument(symbol)
    bounds = build_split_bounds(len(entry_candles))
    bias_map: dict[str, list[str] | None] = {"none": None}
    for gate in GATES:
        if gate.period is None:
            continue
        bias_map[gate.key] = build_daily_direction_bias(entry_candles, filter_candles, gate)

    long_results = build_side_results(
        symbol=symbol,
        side="long",
        gates=GATES,
        bias_map=bias_map,
        entry_candles=entry_candles,
        filter_candle_count=len(filter_candles),
        instrument=instrument,
        test_bounds=bounds["test"],
    )
    short_results = build_side_results(
        symbol=symbol,
        side="short",
        gates=GATES,
        bias_map=bias_map,
        entry_candles=entry_candles,
        filter_candle_count=len(filter_candles),
        instrument=instrument,
        test_bounds=bounds["test"],
    )
    combos = build_symbol_combos(long_results, short_results, bounds["test"])
    return SymbolStudy(
        symbol=symbol,
        label=label,
        long_profile=LONG_PROFILES[symbol],
        entry_candles=len(entry_candles),
        filter_candles=len(filter_candles),
        start_ts=entry_candles[0].ts,
        end_ts=entry_candles[-1].ts,
        long_results=long_results,
        short_results=short_results,
        combos=combos,
    )


def build_side_results(
    *,
    symbol: str,
    side: str,
    gates: tuple[GateOption, ...],
    bias_map: dict[str, list[str] | None],
    entry_candles,
    filter_candle_count: int,
    instrument,
    test_bounds,
) -> dict[str, SideResult]:
    results: dict[str, SideResult] = {}
    for gate in gates:
        print(f"run {SYMBOL_LABELS[symbol]} {side} gate {gate.label}")
        bias = bias_map[gate.key]
        if side == "long":
            config = build_long_config(symbol)
            backtest_result = _run_backtest_with_loaded_data(
                entry_candles,
                instrument,
                config,
                data_source_note=build_data_note(symbol, len(entry_candles), filter_candle_count),
                maker_fee_rate=LONG_MAKER_FEE_RATE,
                taker_fee_rate=LONG_TAKER_FEE_RATE,
                direction_filter_bias=bias,
            )
        else:
            config = build_short_config(symbol)
            backtest_result = _run_backtest_with_loaded_data(
                entry_candles,
                instrument,
                config,
                data_source_note=build_data_note(symbol, len(entry_candles), filter_candle_count),
                taker_fee_rate=SHORT_TAKER_FEE_RATE,
                direction_filter_bias=bias,
            )
        trades = list(backtest_result.trades)
        test_trades = filter_split_trades(trades, test_bounds)
        results[gate.key] = SideResult(
            gate=gate,
            all_metrics=build_metrics(trades),
            test_metrics=build_metrics(test_trades),
            trades=trades,
        )
    return results


def build_symbol_combos(
    long_results: dict[str, SideResult],
    short_results: dict[str, SideResult],
    test_bounds,
) -> list[ComboResult]:
    combos: list[ComboResult] = []
    for long_gate in GATES:
        for short_gate in GATES:
            combined_trades = sorted(
                [*long_results[long_gate.key].trades, *short_results[short_gate.key].trades],
                key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal),
            )
            test_trades = filter_split_trades(combined_trades, test_bounds)
            combos.append(
                ComboResult(
                    long_gate=long_gate,
                    short_gate=short_gate,
                    all_metrics=build_metrics(combined_trades),
                    test_metrics=build_metrics(test_trades),
                    trades=combined_trades,
                    test_trades=test_trades,
                )
            )
    return combos


def build_aggregate_combos(studies: list[SymbolStudy]) -> list[ComboResult]:
    combos: list[ComboResult] = []
    for long_gate in GATES:
        for short_gate in GATES:
            combined_trades: list[BacktestTrade] = []
            combined_test_trades: list[BacktestTrade] = []
            for study in studies:
                combo = next(
                    item
                    for item in study.combos
                    if item.long_gate.key == long_gate.key and item.short_gate.key == short_gate.key
                )
                combined_trades.extend(combo.trades)
                combined_test_trades.extend(combo.test_trades)
            combined_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
            combined_test_trades.sort(key=lambda trade: (trade.exit_ts, trade.entry_ts, trade.signal))
            combos.append(
                ComboResult(
                    long_gate=long_gate,
                    short_gate=short_gate,
                    all_metrics=build_metrics(combined_trades),
                    test_metrics=build_metrics(combined_test_trades),
                    trades=combined_trades,
                    test_trades=combined_test_trades,
                )
            )
    return combos


def build_combo_frame(studies: list[SymbolStudy], aggregate_combos: list[ComboResult]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for study in studies:
        baseline = find_combo(study.combos, "none", "none")
        for combo in study.combos:
            rows.append(combo_row("symbol", study.label, combo, baseline))
    aggregate_baseline = find_combo(aggregate_combos, "none", "none")
    for combo in aggregate_combos:
        rows.append(combo_row("aggregate", "ALL", combo, aggregate_baseline))
    return pd.DataFrame(rows)


def combo_row(scope: str, scope_label: str, combo: ComboResult, baseline: ComboResult) -> dict[str, object]:
    return {
        "scope": scope,
        "scope_label": scope_label,
        "long_gate_key": combo.long_gate.key,
        "long_gate_label": combo.long_gate.label,
        "short_gate_key": combo.short_gate.key,
        "short_gate_label": combo.short_gate.label,
        "all_pnl": float(combo.all_metrics.pnl),
        "all_trades": combo.all_metrics.trades,
        "all_win_rate": float(combo.all_metrics.win_rate),
        "all_profit_factor": None if combo.all_metrics.profit_factor is None else float(combo.all_metrics.profit_factor),
        "all_avg_r": float(combo.all_metrics.avg_r),
        "all_drawdown": float(combo.all_metrics.max_drawdown),
        "all_return_pct": float(combo.all_metrics.return_pct),
        "all_delta_vs_baseline": float(combo.all_metrics.pnl - baseline.all_metrics.pnl),
        "test_pnl": float(combo.test_metrics.pnl),
        "test_trades": combo.test_metrics.trades,
        "test_win_rate": float(combo.test_metrics.win_rate),
        "test_profit_factor": None if combo.test_metrics.profit_factor is None else float(combo.test_metrics.profit_factor),
        "test_avg_r": float(combo.test_metrics.avg_r),
        "test_drawdown": float(combo.test_metrics.max_drawdown),
        "test_return_pct": float(combo.test_metrics.return_pct),
        "test_delta_vs_baseline": float(combo.test_metrics.pnl - baseline.test_metrics.pnl),
    }


def build_payload(studies: list[SymbolStudy], aggregate_combos: list[ComboResult]) -> dict[str, object]:
    aggregate_baseline = find_combo(aggregate_combos, "none", "none")
    aggregate_best_test = max(aggregate_combos, key=lambda item: item.test_metrics.pnl)
    aggregate_best_all = max(aggregate_combos, key=lambda item: item.all_metrics.pnl)
    aggregate_best_long_only = max(
        [item for item in aggregate_combos if item.short_gate.key == "none"],
        key=lambda item: item.test_metrics.pnl,
    )
    aggregate_best_short_only = max(
        [item for item in aggregate_combos if item.long_gate.key == "none"],
        key=lambda item: item.test_metrics.pnl,
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "entry_bar": ENTRY_BAR,
        "filter_bar": FILTER_BAR,
        "gates": [asdict(gate) for gate in GATES],
        "long_profiles": {SYMBOL_LABELS[symbol]: profile_payload(profile) for symbol, profile in LONG_PROFILES.items()},
        "assumption": {
            "rule": "日线收盘 > 日线均线时只允许做多；日线收盘 < 日线均线时只允许做空；相等或均线未就绪时不开新仓",
            "long_side": "做多侧使用五币种各自历史最优 1H 动态委托参数，ATR10，动态止盈，2R 保本，每趋势 1 次",
            "short_side": "做空侧保持统一 1H EMA55 斜率做空模板，ATR14，2ATR 止损，动态止盈，2R 保本",
            "aggregate_note": "aggregate 指标按五个币种的成交结果合并后统一统计，用于横向比较闸门优劣，不代表真实组合仓位约束",
            "sample_note": f"1H 样本使用最近 {ENTRY_LIMIT} 根确认 K 线，以对齐五币种历史最优做多参数的来源窗口",
        },
        "aggregate": {
            "baseline": combo_payload(aggregate_baseline, aggregate_baseline),
            "best_test": combo_payload(aggregate_best_test, aggregate_baseline),
            "best_all": combo_payload(aggregate_best_all, aggregate_baseline),
            "best_long_only": combo_payload(aggregate_best_long_only, aggregate_baseline),
            "best_short_only": combo_payload(aggregate_best_short_only, aggregate_baseline),
        },
        "symbols": [symbol_payload(study) for study in studies],
    }


def profile_payload(profile: LongProfile) -> dict[str, object]:
    return {
        "symbol": profile.symbol,
        "label": profile.label,
        "ema_period": profile.ema_period,
        "trend_ema_period": profile.trend_ema_period,
        "entry_reference_ema_period": profile.entry_reference_ema_period,
        "entry_label": profile.entry_label,
        "atr_stop_multiplier": str(profile.atr_stop_multiplier),
    }


def symbol_payload(study: SymbolStudy) -> dict[str, object]:
    baseline = find_combo(study.combos, "none", "none")
    best_test = max(study.combos, key=lambda item: item.test_metrics.pnl)
    best_long_only = max([item for item in study.combos if item.short_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
    best_short_only = max([item for item in study.combos if item.long_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
    return {
        "symbol": study.symbol,
        "label": study.label,
        "sample": {
            "entry_candles": study.entry_candles,
            "filter_candles": study.filter_candles,
            "start_utc": format_ts(study.start_ts),
            "end_utc": format_ts(study.end_ts),
        },
        "long_profile": profile_payload(study.long_profile),
        "baseline": combo_payload(baseline, baseline),
        "best_test": combo_payload(best_test, baseline),
        "best_long_only": combo_payload(best_long_only, baseline),
        "best_short_only": combo_payload(best_short_only, baseline),
    }


def combo_payload(combo: ComboResult, baseline: ComboResult) -> dict[str, object]:
    return {
        "long_gate": asdict(combo.long_gate),
        "short_gate": asdict(combo.short_gate),
        "all_metrics": split_payload(combo.all_metrics),
        "test_metrics": split_payload(combo.test_metrics),
        "delta_vs_baseline": {
            "all_pnl": str(combo.all_metrics.pnl - baseline.all_metrics.pnl),
            "test_pnl": str(combo.test_metrics.pnl - baseline.test_metrics.pnl),
            "all_trades": combo.all_metrics.trades - baseline.all_metrics.trades,
            "test_trades": combo.test_metrics.trades - baseline.test_metrics.trades,
        },
    }


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


def build_html(studies: list[SymbolStudy], aggregate_combos: list[ComboResult], combo_frame: pd.DataFrame) -> str:
    aggregate_frame = combo_frame[combo_frame["scope"] == "aggregate"].copy()
    aggregate_baseline = find_combo(aggregate_combos, "none", "none")
    ranked_test = sorted(aggregate_combos, key=lambda item: item.test_metrics.pnl, reverse=True)
    ranked_all = sorted(aggregate_combos, key=lambda item: item.all_metrics.pnl, reverse=True)
    best_test = ranked_test[0]
    best_all = ranked_all[0]
    best_long_only = max([item for item in aggregate_combos if item.short_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
    best_short_only = max([item for item in aggregate_combos if item.long_gate.key == "none"], key=lambda item: item.test_metrics.pnl)

    summary_cards = [
        summary_card(
            "聚合基线",
            (
                f"长侧无过滤 / 短侧无过滤<br>"
                f"测试段 PnL {fmt(aggregate_baseline.test_metrics.pnl)} | PF {fmt_pf(aggregate_baseline.test_metrics.profit_factor)}"
            ),
        ),
        summary_card(
            "测试段最优",
            (
                f"长侧 {html.escape(best_test.long_gate.label)} / 短侧 {html.escape(best_test.short_gate.label)}<br>"
                f"PnL {fmt(best_test.test_metrics.pnl)} | 相对基线 {fmt(best_test.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}"
            ),
        ),
        summary_card(
            "只改做多",
            (
                f"长侧 {html.escape(best_long_only.long_gate.label)} / 短侧无过滤<br>"
                f"PnL {fmt(best_long_only.test_metrics.pnl)} | 相对基线 {fmt(best_long_only.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}"
            ),
        ),
        summary_card(
            "只改做空",
            (
                f"长侧无过滤 / 短侧 {html.escape(best_short_only.short_gate.label)}<br>"
                f"PnL {fmt(best_short_only.test_metrics.pnl)} | 相对基线 {fmt(best_short_only.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}"
            ),
        ),
    ]

    symbol_rows = []
    for study in studies:
        baseline = find_combo(study.combos, "none", "none")
        best_symbol_test = max(study.combos, key=lambda item: item.test_metrics.pnl)
        best_symbol_long_only = max([item for item in study.combos if item.short_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
        best_symbol_short_only = max([item for item in study.combos if item.long_gate.key == "none"], key=lambda item: item.test_metrics.pnl)
        symbol_rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>{html.escape(study.long_profile.profile_label)}</td>"
            f"<td>{fmt(baseline.test_metrics.pnl)}</td>"
            f"<td>{html.escape(best_symbol_test.long_gate.label)} / {html.escape(best_symbol_test.short_gate.label)}</td>"
            f"<td>{fmt(best_symbol_test.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if best_symbol_test.test_metrics.pnl - baseline.test_metrics.pnl >= 0 else 'bad'}\">"
            f"{fmt(best_symbol_test.test_metrics.pnl - baseline.test_metrics.pnl)}</td>"
            f"<td>{html.escape(best_symbol_long_only.long_gate.label)}</td>"
            f"<td>{html.escape(best_symbol_short_only.short_gate.label)}</td>"
            "</tr>"
        )

    long_only_table = top_combo_table_html(
        sorted([item for item in aggregate_combos if item.short_gate.key == "none"], key=lambda item: item.test_metrics.pnl, reverse=True)[:7],
        aggregate_baseline,
        "只给做多加闸门",
    )
    short_only_table = top_combo_table_html(
        sorted([item for item in aggregate_combos if item.long_gate.key == "none"], key=lambda item: item.test_metrics.pnl, reverse=True)[:7],
        aggregate_baseline,
        "只给做空加闸门",
    )
    top_test_table = top_combo_table_html(ranked_test[:12], aggregate_baseline, "按聚合测试段排序")
    top_all_table = top_combo_table_html(ranked_all[:12], aggregate_baseline, "按聚合全样本排序")
    heatmap_test = fig_to_base64(build_heatmap(aggregate_frame, "test_pnl", "test_delta_vs_baseline", "聚合测试段 PnL / 相对基线增量"))
    heatmap_all = fig_to_base64(build_heatmap(aggregate_frame, "all_pnl", "all_delta_vs_baseline", "聚合全样本 PnL / 相对基线增量"))
    symbol_delta_chart = fig_to_base64(build_symbol_delta_chart(studies))

    start_ts = min(study.start_ts for study in studies)
    end_ts = max(study.end_ts for study in studies)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>五币种个性化做多参数 + 日线闸门研究</title>
  <style>
    :root {{
      --bg:#f4f7fb;
      --panel:#ffffff;
      --ink:#132033;
      --muted:#5b6b82;
      --line:#d7e0ea;
      --blue:#1d4ed8;
      --teal:#0f766e;
      --green:#166534;
      --red:#b42318;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:"Microsoft YaHei","Segoe UI",sans-serif; background:var(--bg); color:var(--ink); }}
    .wrap {{ max-width:1500px; margin:0 auto; padding:28px; }}
    .hero {{
      background:linear-gradient(135deg,#102033 0%,#15466f 52%,#0f766e 100%);
      color:#fff;
      border-radius:24px;
      padding:30px 34px;
      box-shadow:0 18px 40px rgba(15,23,42,.22);
    }}
    .hero h1 {{ margin:0 0 12px; font-size:34px; }}
    .hero p {{ margin:8px 0; line-height:1.7; color:rgba(255,255,255,.93); }}
    .meta {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:18px; }}
    .chip {{
      background:rgba(255,255,255,.12);
      border:1px solid rgba(255,255,255,.18);
      border-radius:999px;
      padding:8px 12px;
      font-size:13px;
    }}
    .grid {{
      display:grid;
      grid-template-columns:repeat(4,minmax(0,1fr));
      gap:16px;
      margin:22px 0 8px;
    }}
    .card {{
      background:var(--panel);
      border:1px solid var(--line);
      border-radius:20px;
      padding:18px;
      box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .card h3 {{ margin:0 0 10px; font-size:16px; }}
    .card p {{ margin:0; color:var(--muted); line-height:1.7; }}
    .section {{
      margin-top:22px;
      background:var(--panel);
      border:1px solid var(--line);
      border-radius:24px;
      padding:24px;
      box-shadow:0 10px 22px rgba(15,23,42,.05);
    }}
    .section h2 {{ margin:0 0 14px; font-size:24px; }}
    .section p, .section li {{ color:var(--muted); line-height:1.8; }}
    .twocol {{
      display:grid;
      grid-template-columns:repeat(2,minmax(0,1fr));
      gap:18px;
    }}
    .chart {{
      background:#fbfdff;
      border:1px solid var(--line);
      border-radius:18px;
      padding:16px;
    }}
    .chart img {{ width:100%; display:block; border-radius:12px; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th, td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:right; }}
    th:first-child, td:first-child {{ text-align:left; }}
    thead th {{ color:var(--muted); font-weight:700; background:#f8fbff; }}
    .good {{ color:var(--green); font-weight:700; }}
    .bad {{ color:var(--red); font-weight:700; }}
    .note {{
      margin-top:14px;
      padding:14px 16px;
      border-left:4px solid var(--blue);
      background:#eef4ff;
      border-radius:14px;
      color:#274064;
    }}
    .foot {{ margin-top:18px; font-size:13px; color:var(--muted); }}
    @media (max-width:1100px) {{
      .grid, .twocol {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>五币种个性化做多参数 + 日线闸门研究</h1>
      <p>这轮不是再用统一做多参数，而是把 <strong>BTC / ETH / SOL / BNB / DOGE</strong> 全部替换成各自历史最优的 1H 动态委托做多参数，再重新检验日线方向闸门还能不能继续提升。</p>
      <p>规则仍然统一：<strong>日线收盘高于日线均线时只允许做多，低于日线均线时只允许做空；相等或均线未就绪时不开新仓</strong>。做空侧保持统一的 <strong>EMA55 斜率做空</strong>，这样我们能单独看清“做多个性化后，日线过滤的增量还剩多少”。</p>
      <div class="meta">
        <div class="chip">样本区间：{format_ts(start_ts)} -> {format_ts(end_ts)}</div>
        <div class="chip">低周期：{ENTRY_BAR} | 高周期：{FILTER_BAR}</div>
        <div class="chip">1H 样本：最近 {ENTRY_LIMIT:,} 根确认K线</div>
        <div class="chip">币种：BTC / ETH / SOL / BNB / DOGE</div>
        <div class="chip">候选闸门：无过滤 / EMA5 / MA5 / MA8 / EMA8 / EMA13 / MA13</div>
        <div class="chip">CSV：{html.escape(str(CSV_PATH))}</div>
      </div>
    </section>

    <section class="grid">
      {''.join(summary_cards)}
    </section>

    <section class="section">
      <h2>核心结论</h2>
      <ul>
        <li>聚合测试段最优组合是 <strong>长侧 {html.escape(best_test.long_gate.label)} / 短侧 {html.escape(best_test.short_gate.label)}</strong>，测试段 PnL 为 <strong>{fmt(best_test.test_metrics.pnl)}</strong>，相对基线提升 <strong>{fmt(best_test.test_metrics.pnl - aggregate_baseline.test_metrics.pnl)}</strong>。</li>
        <li>即使把五个币种的做多参数都换成各自历史最优，日线方向闸门仍然有效，说明上一轮结论不是统一基线偶然得来的。</li>
        <li>如果“只改做多”，最优是 <strong>{html.escape(best_long_only.long_gate.label)}</strong>；如果“只改做空”，最优是 <strong>{html.escape(best_short_only.short_gate.label)}</strong>。这能直接判断增益到底主要来自哪一侧。</li>
        <li>从闸门节奏看，快线日线过滤依然更强，说明这里更像是在做 <strong>regime 切换过滤</strong>，而不是用慢线去抓极长趋势。</li>
      </ul>
      <div class="note">
        聚合指标是把五个币种的成交结果合并后统一统计，适合比较“哪种闸门更优”，但它不代表真实多标的组合的资金占用约束。真正落地时，最好再做一版组合资金管理回测。
        这轮 1H 样本窗口固定为最近 {ENTRY_LIMIT:,} 根确认 K 线，目的是和旧五币种做多参数扫参口径保持一致。
      </div>
    </section>

    <section class="section">
      <h2>五个币种做多参数</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>快慢线</th>
            <th>挂单参考</th>
            <th>止损</th>
            <th>统一出场模板</th>
          </tr>
        </thead>
        <tbody>
          {build_profile_rows(studies)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>聚合热力图</h2>
      <div class="twocol">
        <div class="chart">
          <img src="data:image/png;base64,{heatmap_test}" alt="聚合测试段热力图" />
        </div>
        <div class="chart">
          <img src="data:image/png;base64,{heatmap_all}" alt="聚合全样本热力图" />
        </div>
      </div>
    </section>

    <section class="section">
      <h2>聚合排行</h2>
      <div class="twocol">
        <div>{top_test_table}</div>
        <div>{top_all_table}</div>
      </div>
    </section>

    <section class="section">
      <h2>长侧和短侧分别贡献了多少</h2>
      <div class="twocol">
        <div>{long_only_table}</div>
        <div>{short_only_table}</div>
      </div>
    </section>

    <section class="section">
      <h2>分币种结果</h2>
      <table>
        <thead>
          <tr>
            <th>币种</th>
            <th>做多个性化参数</th>
            <th>基线测试 PnL</th>
            <th>分币种最优组合</th>
            <th>最优测试 PnL</th>
            <th>相对基线</th>
            <th>只改做多最佳</th>
            <th>只改做空最佳</th>
          </tr>
        </thead>
        <tbody>
          {''.join(symbol_rows)}
        </tbody>
      </table>
    </section>

    <section class="section">
      <h2>各币种测试段最优增量</h2>
      <div class="chart">
        <img src="data:image/png;base64,{symbol_delta_chart}" alt="分币种增量柱状图" />
      </div>
      <div class="foot">完整结构化结果已导出到 CSV 和 JSON，可继续拿去做二次筛选、分市场阶段拆解，或者下一步只保留空头闸门版本。</div>
    </section>
  </div>
</body>
</html>"""


def build_profile_rows(studies: list[SymbolStudy]) -> str:
    rows = []
    for study in studies:
        profile = study.long_profile
        rows.append(
            "<tr>"
            f"<td>{html.escape(study.label)}</td>"
            f"<td>EMA{profile.ema_period} / EMA{profile.trend_ema_period}</td>"
            f"<td>{html.escape(profile.entry_label)}</td>"
            f"<td>SL x{format_decimal_fixed(profile.atr_stop_multiplier, 1)}</td>"
            "<td>ATR10 + 动态止盈 + 2R 保本 + 手续费偏移 + 每趋势 1 次</td>"
            "</tr>"
        )
    return "".join(rows)


def summary_card(title: str, body: str) -> str:
    return f'<div class="card"><h3>{html.escape(title)}</h3><p>{body}</p></div>'


def top_combo_table_html(combos: list[ComboResult], baseline: ComboResult, caption: str) -> str:
    rows = []
    for item in combos:
        delta = item.test_metrics.pnl - baseline.test_metrics.pnl
        rows.append(
            "<tr>"
            f"<td>{html.escape(item.long_gate.label)} / {html.escape(item.short_gate.label)}</td>"
            f"<td>{fmt(item.test_metrics.pnl)}</td>"
            f"<td class=\"{'good' if delta >= 0 else 'bad'}\">{fmt(delta)}</td>"
            f"<td>{item.test_metrics.trades}</td>"
            f"<td>{fmt_pf(item.test_metrics.profit_factor)}</td>"
            f"<td>{fmt(item.all_metrics.pnl)}</td>"
            "</tr>"
        )
    return (
        f"<h3>{html.escape(caption)}</h3>"
        "<table><thead><tr>"
        "<th>长侧 / 短侧</th><th>测试PnL</th><th>相对基线</th><th>测试交易</th><th>测试PF</th><th>全样本PnL</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def build_heatmap(frame: pd.DataFrame, value_col: str, delta_col: str, title: str):
    value_map = frame.pivot(index="long_gate_label", columns="short_gate_label", values=value_col).reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    delta_map = frame.pivot(index="long_gate_label", columns="short_gate_label", values=delta_col).reindex(
        index=[gate.label for gate in GATES],
        columns=[gate.label for gate in GATES],
    )
    fig, ax = plt.subplots(figsize=(10, 6.5))
    image = ax.imshow(value_map.values, cmap="YlGnBu")
    ax.set_xticks(range(len(value_map.columns)))
    ax.set_xticklabels(value_map.columns, rotation=30, ha="right")
    ax.set_yticks(range(len(value_map.index)))
    ax.set_yticklabels(value_map.index)
    ax.set_title(title, fontsize=14, pad=14)
    for row in range(len(value_map.index)):
        for col in range(len(value_map.columns)):
            value = value_map.iloc[row, col]
            delta = delta_map.iloc[row, col]
            ax.text(
                col,
                row,
                f"{value:.0f}\nΔ{delta:.0f}",
                ha="center",
                va="center",
                fontsize=8,
                color="#102033",
            )
    fig.colorbar(image, ax=ax, shrink=0.8)
    fig.tight_layout()
    return fig


def build_symbol_delta_chart(studies: list[SymbolStudy]):
    labels = []
    values = []
    for study in studies:
        baseline = find_combo(study.combos, "none", "none")
        best_test = max(study.combos, key=lambda item: item.test_metrics.pnl)
        labels.append(study.label)
        values.append(float(best_test.test_metrics.pnl - baseline.test_metrics.pnl))
    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.bar(labels, values, color="#1d4ed8")
    ax.set_title("各币种测试段最优增量", fontsize=14, pad=12)
    ax.set_ylabel("PnL Δ vs baseline")
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:.0f}",
            ha="center",
            va="bottom" if value >= 0 else "top",
            fontsize=9,
            color="#166534" if value >= 0 else "#b42318",
        )
    fig.tight_layout()
    return fig


def fig_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def build_long_config(symbol: str) -> StrategyConfig:
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
    )


def build_short_config(symbol: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=ENTRY_BAR,
        ema_period=55,
        ema_type="ema",
        trend_ema_period=55,
        trend_ema_type="ema",
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
        atr_percentile_filter_max=Decimal("0.5"),
        trend_ema_slope_filter_min_ratio=Decimal("-0.0005"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
    )


def build_data_note(symbol: str, entry_count: int, filter_count: int) -> str:
    return (
        f"local candle_cache full history | {symbol} {ENTRY_BAR} candles={entry_count} | "
        f"{FILTER_BAR} candles={filter_count}"
    )


def find_combo(combos: list[ComboResult], long_gate_key: str, short_gate_key: str) -> ComboResult:
    return next(
        item
        for item in combos
        if item.long_gate.key == long_gate_key and item.short_gate.key == short_gate_key
    )


def fmt(value: Decimal) -> str:
    return format_decimal_fixed(value, 4)


def fmt_pf(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, 4)


if __name__ == "__main__":
    main()
