from __future__ import annotations

import csv
import html
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data, format_trade_exit_reason
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import Instrument, StrategyConfig
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed


CONFIG_PATH = Path(r"C:\Users\Windows\Desktop\EMA 动态委托做多_BTC-USDT-SWAP.json")
REPORT_DIR = analysis_report_dir_path()
PROJECT_REPORT_DIR = ROOT / "reports"
PROJECT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
BASENAME = f"btc_dynamic_long_ma50_vs_ema55_matrix_{STAMP}"
HTML_PATH = REPORT_DIR / f"{BASENAME}.html"
CSV_PATH = REPORT_DIR / f"{BASENAME}.csv"
JSON_PATH = REPORT_DIR / f"{BASENAME}.json"
MD_PATH = REPORT_DIR / f"{BASENAME}.md"
LATEST_HTML_PATH = PROJECT_REPORT_DIR / "btc_dynamic_long_ma50_vs_ema55_matrix_latest.html"
LATEST_CSV_PATH = PROJECT_REPORT_DIR / "btc_dynamic_long_ma50_vs_ema55_matrix_latest.csv"
LATEST_JSON_PATH = PROJECT_REPORT_DIR / "btc_dynamic_long_ma50_vs_ema55_matrix_latest.json"
LATEST_MD_PATH = PROJECT_REPORT_DIR / "btc_dynamic_long_ma50_vs_ema55_matrix_latest.md"

MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")
ENTRY_OPTIONS = (1, 2, 3)
STOP_OPTIONS = (Decimal("1"), Decimal("1.5"), Decimal("2"))

BTC_SWAP_INSTRUMENT = Instrument(
    inst_id="BTC-USDT-SWAP",
    inst_type="SWAP",
    tick_size=Decimal("0.1"),
    lot_size=Decimal("0.01"),
    min_size=Decimal("0.01"),
    state="live",
    settle_ccy="USDT",
    ct_val=Decimal("0.01"),
    ct_mult=Decimal("1"),
    ct_val_ccy="BTC",
    uly="BTC-USDT",
    inst_family="BTC-USDT",
)


@dataclass(frozen=True)
class MatrixSpec:
    group_key: str
    group_label: str
    trend_ema_type: str
    trend_ema_period: int
    entry_reference_ema_type: str
    entry_reference_ema_period: int


@dataclass(frozen=True)
class MatrixRow:
    group_key: str
    group_label: str
    trend_line: str
    entry_line: str
    max_entries_per_trend: int
    atr_stop_multiplier: str
    total_pnl: Decimal
    total_return_pct: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    win_rate: Decimal
    total_trades: int
    average_r_multiple: Decimal
    profit_loss_ratio: Decimal | None
    total_fees: Decimal
    maker_fees: Decimal
    taker_fees: Decimal
    pnl_to_drawdown: Decimal
    exit_reason_top: str


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def int_value(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    return int(value)


def str_value(value: Any, default: str = "") -> str:
    text = str(value if value is not None else default).strip()
    return text or default


def bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def fmt(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def moving_average_label(ma_type: str, period: int) -> str:
    prefix = "MA" if ma_type.lower() == "ma" else "EMA"
    return f"{prefix}{period}"


def load_template_payload() -> dict[str, Any]:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("strategy template JSON must be an object")
    return payload


def load_template_snapshot() -> dict[str, Any]:
    payload = load_template_payload()
    snapshot = payload.get("config_snapshot")
    if not isinstance(snapshot, dict):
        raise KeyError("config_snapshot missing in template JSON")
    return snapshot


def build_base_config(snapshot: dict[str, Any]) -> StrategyConfig:
    return StrategyConfig(
        inst_id=str_value(snapshot.get("inst_id"), "BTC-USDT-SWAP"),
        bar=str_value(snapshot.get("bar"), "1H"),
        ema_period=int_value(snapshot.get("ema_period"), 21),
        ema_type=str_value(snapshot.get("ema_type"), "ema"),
        trend_ema_period=int_value(snapshot.get("trend_ema_period"), 50),
        trend_ema_type=str_value(snapshot.get("trend_ema_type"), "ma"),
        big_ema_period=int_value(snapshot.get("big_ema_period"), 233),
        atr_period=int_value(snapshot.get("atr_period"), 10),
        atr_stop_multiplier=dec(snapshot.get("atr_stop_multiplier"), "2"),
        atr_take_multiplier=dec(snapshot.get("atr_take_multiplier"), "2"),
        order_size=dec(snapshot.get("order_size"), "0"),
        trade_mode=str_value(snapshot.get("trade_mode"), "cross"),
        signal_mode=str_value(snapshot.get("signal_mode"), "long_only"),
        position_mode=str_value(snapshot.get("position_mode"), "net"),
        environment="demo",
        tp_sl_trigger_type=str_value(snapshot.get("tp_sl_trigger_type"), "mark"),
        strategy_id=str_value(snapshot.get("strategy_id"), "ema_dynamic_order_long"),
        poll_seconds=float(snapshot.get("poll_seconds") or 10.0),
        risk_amount=dec(snapshot.get("risk_amount"), "20"),
        trade_inst_id=snapshot.get("trade_inst_id"),
        tp_sl_mode=str_value(snapshot.get("tp_sl_mode"), "exchange"),
        local_tp_sl_inst_id=snapshot.get("local_tp_sl_inst_id"),
        entry_side_mode=str_value(snapshot.get("entry_side_mode"), "follow_signal"),
        run_mode=str_value(snapshot.get("run_mode"), "trade"),
        backtest_initial_capital=dec(snapshot.get("backtest_initial_capital"), "10000"),
        backtest_sizing_mode=str_value(snapshot.get("backtest_sizing_mode"), "fixed_risk"),
        backtest_risk_percent=dec(snapshot.get("backtest_risk_percent")) if snapshot.get("backtest_risk_percent") not in (None, "") else None,
        backtest_compounding=bool_value(snapshot.get("backtest_compounding"), False),
        backtest_entry_slippage_rate=dec(snapshot.get("backtest_entry_slippage_rate"), "0"),
        backtest_exit_slippage_rate=dec(snapshot.get("backtest_exit_slippage_rate"), "0"),
        backtest_slippage_rate=dec(snapshot.get("backtest_slippage_rate"), "0"),
        backtest_funding_rate=dec(snapshot.get("backtest_funding_rate"), "0"),
        take_profit_mode=str_value(snapshot.get("take_profit_mode"), "dynamic"),
        max_entries_per_trend=int_value(snapshot.get("max_entries_per_trend"), 1),
        entry_reference_ema_period=int_value(snapshot.get("entry_reference_ema_period"), 50),
        entry_reference_ema_type=str_value(snapshot.get("entry_reference_ema_type"), "ma"),
        dynamic_two_r_break_even=bool_value(snapshot.get("dynamic_two_r_break_even"), True),
        dynamic_fee_offset_enabled=bool_value(snapshot.get("dynamic_fee_offset_enabled"), True),
        ema55_slope_exit_enabled=bool_value(snapshot.get("ema55_slope_exit_enabled"), True),
        ema55_slope_lock_profit_enabled=bool_value(snapshot.get("ema55_slope_lock_profit_enabled"), False),
        ema55_slope_lock_profit_trigger_r=int_value(snapshot.get("ema55_slope_lock_profit_trigger_r"), 5),
        ema55_slope_negative_entry_bars=int_value(snapshot.get("ema55_slope_negative_entry_bars"), 1),
        ema55_slope_same_bar_reentry_block=bool_value(snapshot.get("ema55_slope_same_bar_reentry_block"), False),
        ema55_slope_dynamic_exit_requires_bear_reentry=bool_value(
            snapshot.get("ema55_slope_dynamic_exit_requires_bear_reentry"), False
        ),
        ema55_slope_dynamic_exit_bear_reentry_break_prev_low=bool_value(
            snapshot.get("ema55_slope_dynamic_exit_bear_reentry_break_prev_low"), False
        ),
        ema55_slope_dynamic_exit_requires_ema_reclaim=bool_value(
            snapshot.get("ema55_slope_dynamic_exit_requires_ema_reclaim"), False
        ),
        ema55_slope_locked_reentry_requires_ema21_near=bool_value(
            snapshot.get("ema55_slope_locked_reentry_requires_ema21_near"), False
        ),
        ema55_slope_locked_reentry_min_r=int_value(snapshot.get("ema55_slope_locked_reentry_min_r"), 0),
        ema55_slope_locked_reentry_max_r=int_value(snapshot.get("ema55_slope_locked_reentry_max_r"), 0),
        ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry=bool_value(
            snapshot.get("ema55_slope_dynamic_exit_bull_bar_requires_bear_reentry"), False
        ),
        ema55_slope_dynamic_exit_bull_bar_reentry_min_r=int_value(
            snapshot.get("ema55_slope_dynamic_exit_bull_bar_reentry_min_r"), 0
        ),
        ema55_slope_dynamic_exit_bull_bar_reentry_max_r=int_value(
            snapshot.get("ema55_slope_dynamic_exit_bull_bar_reentry_max_r"), 0
        ),
        atr_percentile_filter_max=dec(snapshot.get("atr_percentile_filter_max"), "0"),
        trend_ema_slope_filter_enabled=bool_value(snapshot.get("trend_ema_slope_filter_enabled"), False),
        trend_ema_slope_filter_lookback_bars=int_value(snapshot.get("trend_ema_slope_filter_lookback_bars"), 5),
        trend_ema_slope_filter_min_ratio=dec(snapshot.get("trend_ema_slope_filter_min_ratio"), "0"),
        startup_chase_window_seconds=int_value(snapshot.get("startup_chase_window_seconds"), 0),
        time_stop_break_even_enabled=bool_value(snapshot.get("time_stop_break_even_enabled"), False),
        time_stop_break_even_bars=int_value(snapshot.get("time_stop_break_even_bars"), 0),
        hold_close_exit_bars=int_value(snapshot.get("hold_close_exit_bars"), 0),
        trader_virtual_stop_loss=bool_value(snapshot.get("trader_virtual_stop_loss"), False),
        backtest_profile_id=str_value(snapshot.get("backtest_profile_id"), ""),
        backtest_profile_name=str_value(snapshot.get("backtest_profile_name"), ""),
        backtest_profile_summary=str_value(snapshot.get("backtest_profile_summary"), ""),
        cross_higher_tf_inst_id=snapshot.get("cross_higher_tf_inst_id"),
        cross_higher_tf_bar=snapshot.get("cross_higher_tf_bar"),
        cross_higher_tf_ref_ema_period=int_value(snapshot.get("cross_higher_tf_ref_ema_period"), 0),
        mtf_filter_inst_id=snapshot.get("mtf_filter_inst_id"),
        mtf_filter_bar=snapshot.get("mtf_filter_bar"),
        mtf_filter_fast_ema_period=int_value(snapshot.get("mtf_filter_fast_ema_period"), 21),
        mtf_filter_slow_ema_period=int_value(snapshot.get("mtf_filter_slow_ema_period"), 55),
        mtf_reversal_mode=str_value(snapshot.get("mtf_reversal_mode"), "block_new_entries"),
        rail_candidate_ema_periods=tuple(int(item) for item in snapshot.get("rail_candidate_ema_periods", (21, 34, 55, 89))),
        rail_touch_atr_ratio=dec(snapshot.get("rail_touch_atr_ratio"), "0.2"),
        rail_bounce_atr_ratio=dec(snapshot.get("rail_bounce_atr_ratio"), "0.6"),
        rail_bounce_confirm_bars=int_value(snapshot.get("rail_bounce_confirm_bars"), 3),
        rail_break_atr_ratio=dec(snapshot.get("rail_break_atr_ratio"), "1.0"),
        rail_reclaim_bars=int_value(snapshot.get("rail_reclaim_bars"), 2),
        rail_score_lookback_bars=int_value(snapshot.get("rail_score_lookback_bars"), 60),
        rail_switch_min_score_delta=dec(snapshot.get("rail_switch_min_score_delta"), "8"),
        rail_min_touches=int_value(snapshot.get("rail_min_touches"), 2),
        rail_min_bounces=int_value(snapshot.get("rail_min_bounces"), 1),
    )


def build_matrix_specs(snapshot: dict[str, Any]) -> tuple[MatrixSpec, ...]:
    original_trend_type = str_value(snapshot.get("trend_ema_type"), "ma")
    original_trend_period = int_value(snapshot.get("trend_ema_period"), 50)
    original_entry_type = str_value(snapshot.get("entry_reference_ema_type"), original_trend_type)
    original_entry_period = int_value(snapshot.get("entry_reference_ema_period"), original_trend_period)
    return (
        MatrixSpec(
            group_key="baseline_ma50",
            group_label="原最佳参数矩阵（MA50）",
            trend_ema_type=original_trend_type,
            trend_ema_period=original_trend_period,
            entry_reference_ema_type=original_entry_type,
            entry_reference_ema_period=original_entry_period,
        ),
        MatrixSpec(
            group_key="ema55_variant",
            group_label="替换矩阵（EMA55）",
            trend_ema_type="ema",
            trend_ema_period=55,
            entry_reference_ema_type="ema",
            entry_reference_ema_period=55,
        ),
    )


def load_candles(symbol: str, bar: str) -> list:
    candles = [candle for candle in load_candle_cache(symbol, bar, limit=None) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"missing local candles for {symbol} {bar}")
    return candles


def summarize_exit_reasons(trades: list, limit: int = 4) -> str:
    counts: dict[str, int] = {}
    for trade in trades:
        label = format_trade_exit_reason(trade.exit_reason)
        counts[label] = counts.get(label, 0) + 1
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return " | ".join(f"{label} {count}" for label, count in ordered[:limit])


def run_matrix(base_config: StrategyConfig, candles: list, spec: MatrixSpec) -> list[MatrixRow]:
    rows: list[MatrixRow] = []
    total = len(ENTRY_OPTIONS) * len(STOP_OPTIONS)
    step = 0
    for max_entries in ENTRY_OPTIONS:
        for stop_atr in STOP_OPTIONS:
            step += 1
            config = StrategyConfig(
                **{
                    **asdict(base_config),
                    "trend_ema_type": spec.trend_ema_type,
                    "trend_ema_period": spec.trend_ema_period,
                    "entry_reference_ema_type": spec.entry_reference_ema_type,
                    "entry_reference_ema_period": spec.entry_reference_ema_period,
                    "max_entries_per_trend": max_entries,
                    "atr_stop_multiplier": stop_atr,
                }
            )
            print(
                f"[{spec.group_label} {step}/{total}] "
                f"{moving_average_label(spec.trend_ema_type, spec.trend_ema_period)} / "
                f"{moving_average_label(spec.entry_reference_ema_type, spec.entry_reference_ema_period)} / "
                f"每波{max_entries} / SLx{stop_atr}",
                flush=True,
            )
            result = _run_backtest_with_loaded_data(
                candles,
                BTC_SWAP_INSTRUMENT,
                config,
                data_source_note=f"local candle_cache full history | {spec.group_key}",
                maker_fee_rate=MAKER_FEE_RATE,
                taker_fee_rate=TAKER_FEE_RATE,
            )
            report = result.report
            pnl_to_drawdown = report.total_pnl / report.max_drawdown if report.max_drawdown != 0 else Decimal("0")
            rows.append(
                MatrixRow(
                    group_key=spec.group_key,
                    group_label=spec.group_label,
                    trend_line=moving_average_label(spec.trend_ema_type, spec.trend_ema_period),
                    entry_line=moving_average_label(spec.entry_reference_ema_type, spec.entry_reference_ema_period),
                    max_entries_per_trend=max_entries,
                    atr_stop_multiplier=str(stop_atr),
                    total_pnl=report.total_pnl,
                    total_return_pct=report.total_return_pct,
                    max_drawdown=report.max_drawdown,
                    max_drawdown_pct=report.max_drawdown_pct,
                    profit_factor=report.profit_factor,
                    win_rate=report.win_rate,
                    total_trades=report.total_trades,
                    average_r_multiple=report.average_r_multiple,
                    profit_loss_ratio=report.profit_loss_ratio,
                    total_fees=report.total_fees,
                    maker_fees=report.maker_fees,
                    taker_fees=report.taker_fees,
                    pnl_to_drawdown=pnl_to_drawdown,
                    exit_reason_top=summarize_exit_reasons(result.trades),
                )
            )
    return rows


def export_csv(path: Path, rows: list[MatrixRow]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "group_key",
                "group_label",
                "trend_line",
                "entry_line",
                "max_entries_per_trend",
                "atr_stop_multiplier",
                "total_pnl",
                "total_return_pct",
                "max_drawdown",
                "max_drawdown_pct",
                "profit_factor",
                "win_rate",
                "total_trades",
                "average_r_multiple",
                "profit_loss_ratio",
                "total_fees",
                "maker_fees",
                "taker_fees",
                "pnl_to_drawdown",
                "exit_reason_top",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.group_key,
                    row.group_label,
                    row.trend_line,
                    row.entry_line,
                    row.max_entries_per_trend,
                    row.atr_stop_multiplier,
                    fmt(row.total_pnl, 4),
                    fmt(row.total_return_pct, 2),
                    fmt(row.max_drawdown, 4),
                    fmt(row.max_drawdown_pct, 2),
                    fmt(row.profit_factor, 4) if row.profit_factor is not None else "",
                    fmt(row.win_rate, 2),
                    row.total_trades,
                    fmt(row.average_r_multiple, 4),
                    fmt(row.profit_loss_ratio, 4) if row.profit_loss_ratio is not None else "",
                    fmt(row.total_fees, 4),
                    fmt(row.maker_fees, 4),
                    fmt(row.taker_fees, 4),
                    fmt(row.pnl_to_drawdown, 4),
                    row.exit_reason_top,
                ]
            )


def pick_best(rows: list[MatrixRow], key: str, reverse: bool = True) -> MatrixRow:
    return sorted(rows, key=lambda row: getattr(row, key), reverse=reverse)[0]


def best_pf(rows: list[MatrixRow]) -> MatrixRow:
    return sorted(rows, key=lambda row: (row.profit_factor if row.profit_factor is not None else Decimal("-1"), row.total_pnl), reverse=True)[0]


def build_summary(groups: dict[str, list[MatrixRow]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, rows in groups.items():
        best_pnl = pick_best(rows, "total_pnl", True)
        best_quality = pick_best(rows, "pnl_to_drawdown", True)
        lowest_dd = pick_best(rows, "max_drawdown", False)
        best_pf_row = best_pf(rows)
        payload[key] = {
            "best_pnl": row_to_payload(best_pnl),
            "best_quality": row_to_payload(best_quality),
            "lowest_drawdown": row_to_payload(lowest_dd),
            "best_profit_factor": row_to_payload(best_pf_row),
        }
    baseline = payload["baseline_ma50"]["best_pnl"]
    variant = payload["ema55_variant"]["best_pnl"]
    payload["head_to_head"] = {
        "best_pnl_delta_u": float(Decimal(str(variant["total_pnl"])) - Decimal(str(baseline["total_pnl"]))),
        "best_pnl_delta_return_pct": float(
            Decimal(str(variant["total_return_pct"])) - Decimal(str(baseline["total_return_pct"]))
        ),
        "best_pnl_delta_drawdown_u": float(
            Decimal(str(variant["max_drawdown"])) - Decimal(str(baseline["max_drawdown"]))
        ),
        "best_pnl_delta_trades": int(variant["total_trades"]) - int(baseline["total_trades"]),
    }
    return payload


def row_to_payload(row: MatrixRow) -> dict[str, Any]:
    return {
        "group_label": row.group_label,
        "trend_line": row.trend_line,
        "entry_line": row.entry_line,
        "max_entries_per_trend": row.max_entries_per_trend,
        "atr_stop_multiplier": row.atr_stop_multiplier,
        "total_pnl": float(row.total_pnl),
        "total_return_pct": float(row.total_return_pct),
        "max_drawdown": float(row.max_drawdown),
        "max_drawdown_pct": float(row.max_drawdown_pct),
        "profit_factor": None if row.profit_factor is None else float(row.profit_factor),
        "win_rate": float(row.win_rate),
        "total_trades": row.total_trades,
        "average_r_multiple": float(row.average_r_multiple),
        "profit_loss_ratio": None if row.profit_loss_ratio is None else float(row.profit_loss_ratio),
        "total_fees": float(row.total_fees),
        "pnl_to_drawdown": float(row.pnl_to_drawdown),
        "exit_reason_top": row.exit_reason_top,
    }


def build_markdown(snapshot: dict[str, Any], rows: list[MatrixRow], summary: dict[str, Any]) -> str:
    lines = [
        "# BTC 动态委托做多：MA50 vs EMA55 双矩阵报告",
        "",
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"定稿模板：`{CONFIG_PATH}`",
        f"标的：`{snapshot.get('inst_id', 'BTC-USDT-SWAP')}`",
        f"周期：`{snapshot.get('bar', '1H')}`",
        "",
        "## 本轮固定口径",
        "",
        f"- 快线：`{moving_average_label(str_value(snapshot.get('ema_type'), 'ema'), int_value(snapshot.get('ema_period'), 21))}`",
        f"- 大周期趋势线基线：`{moving_average_label(str_value(snapshot.get('trend_ema_type'), 'ma'), int_value(snapshot.get('trend_ema_period'), 50))}`",
        f"- 挂单参考线基线：`{moving_average_label(str_value(snapshot.get('entry_reference_ema_type'), 'ma'), int_value(snapshot.get('entry_reference_ema_period'), 50))}`",
        f"- ATR周期：`{int_value(snapshot.get('atr_period'), 10)}`",
        f"- 动态止盈：`{str_value(snapshot.get('take_profit_mode'), 'dynamic')}`",
        f"- 2R保本：`{'开' if bool_value(snapshot.get('dynamic_two_r_break_even'), True) else '关'}`",
        f"- 手续费偏移：`{'开' if bool_value(snapshot.get('dynamic_fee_offset_enabled'), True) else '关'}`",
        f"- 风险金：`{dec(snapshot.get('risk_amount'), '20')}` U",
        f"- 本轮矩阵变量：`每波开仓次数 = 1/2/3`，`ATR止损 = 1/1.5/2`",
        "",
        "## 结论先看",
        "",
    ]
    base_best = summary["baseline_ma50"]["best_pnl"]
    ema_best = summary["ema55_variant"]["best_pnl"]
    delta = summary["head_to_head"]
    lines.extend(
        [
            f"- 原 MA50 矩阵里，收益最高的是 `每波{base_best['max_entries_per_trend']} / SLx{base_best['atr_stop_multiplier']}`，总盈亏 `{base_best['total_pnl']:.4f}` U，回撤 `{base_best['max_drawdown']:.4f}` U，PF `{base_best['profit_factor'] if base_best['profit_factor'] is not None else '-'} `。",
            f"- 改 EMA55 矩阵里，收益最高的是 `每波{ema_best['max_entries_per_trend']} / SLx{ema_best['atr_stop_multiplier']}`，总盈亏 `{ema_best['total_pnl']:.4f}` U，回撤 `{ema_best['max_drawdown']:.4f}` U，PF `{ema_best['profit_factor'] if ema_best['profit_factor'] is not None else '-'} `。",
            f"- 以各自最佳收益组合对打，`EMA55 - MA50` 的盈亏差是 `{delta['best_pnl_delta_u']:+.4f}` U，收益率差 `{delta['best_pnl_delta_return_pct']:+.2f}` pct，回撤差 `{delta['best_pnl_delta_drawdown_u']:+.4f}` U，交易数差 `{delta['best_pnl_delta_trades']:+d}`。",
            "",
            "## 全量结果",
            "",
            "| 组别 | 趋势线 | 挂单线 | 每波开仓 | ATR止损 | 总盈亏U | 收益率% | 最大回撤U | 回撤% | PF | 胜率% | 交易数 | 平均R | 盈亏/回撤 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row.group_label} | {row.trend_line} | {row.entry_line} | {row.max_entries_per_trend} | {row.atr_stop_multiplier} | "
            f"{fmt(row.total_pnl, 4)} | {fmt(row.total_return_pct, 2)} | {fmt(row.max_drawdown, 4)} | {fmt(row.max_drawdown_pct, 2)} | "
            f"{fmt(row.profit_factor, 4) if row.profit_factor is not None else '-'} | {fmt(row.win_rate, 2)} | {row.total_trades} | "
            f"{fmt(row.average_r_multiple, 4)} | {fmt(row.pnl_to_drawdown, 4)} |"
        )
    lines.extend(
        [
            "",
            "## 分析",
            "",
        ]
    )
    for group_key in ("baseline_ma50", "ema55_variant"):
        title = "原最佳参数矩阵（MA50）" if group_key == "baseline_ma50" else "替换矩阵（EMA55）"
        group_rows = [row for row in rows if row.group_key == group_key]
        best_pnl_row = pick_best(group_rows, "total_pnl", True)
        best_quality_row = pick_best(group_rows, "pnl_to_drawdown", True)
        lowest_dd_row = pick_best(group_rows, "max_drawdown", False)
        lines.append(f"### {title}")
        lines.append("")
        lines.append(
            f"- 收益最高：`每波{best_pnl_row.max_entries_per_trend} / SLx{best_pnl_row.atr_stop_multiplier}`，"
            f"总盈亏 `{fmt(best_pnl_row.total_pnl, 4)}` U，PF `{fmt(best_pnl_row.profit_factor, 4) if best_pnl_row.profit_factor is not None else '-'}`，"
            f"交易 `{best_pnl_row.total_trades}` 笔。"
        )
        lines.append(
            f"- 风险收益比最好：`每波{best_quality_row.max_entries_per_trend} / SLx{best_quality_row.atr_stop_multiplier}`，"
            f"盈亏/回撤 `{fmt(best_quality_row.pnl_to_drawdown, 4)}`，回撤 `{fmt(best_quality_row.max_drawdown, 4)}` U。"
        )
        lines.append(
            f"- 回撤最低：`每波{lowest_dd_row.max_entries_per_trend} / SLx{lowest_dd_row.atr_stop_multiplier}`，"
            f"最大回撤 `{fmt(lowest_dd_row.max_drawdown, 4)}` U，但对应总盈亏 `{fmt(lowest_dd_row.total_pnl, 4)}` U。"
        )
        lines.append(
            f"- 最优收益组合的主要出场结构：`{best_pnl_row.exit_reason_top}`。"
        )
        lines.append("")
    lines.extend(
        [
            "## 备注",
            "",
            f"- 这轮没有改快线，仍然是 `{moving_average_label(str_value(snapshot.get('ema_type'), 'ema'), int_value(snapshot.get('ema_period'), 21))}`。",
            "- 这轮只改两件事：趋势/挂单参考线从 `MA50` 替换成 `EMA55`，以及矩阵扫描 `每波开仓次数` 和 `ATR止损`。",
            f"- 手续费口径沿用仓库里 BTC 做多研究脚本的常用设置：Maker `{MAKER_FEE_RATE}`，Taker `{TAKER_FEE_RATE}`。",
        ]
    )
    return "\n".join(lines) + "\n"


def build_html(snapshot: dict[str, Any], rows: list[MatrixRow], summary: dict[str, Any]) -> str:
    style = """
body{font-family:"Microsoft YaHei UI",Arial,sans-serif;background:#f6f2e8;color:#182018;margin:0;padding:28px}
h1,h2,h3{margin:0 0 10px}
.sub{color:#5b675d;margin-bottom:22px}
.cards{display:grid;grid-template-columns:repeat(4,minmax(180px,1fr));gap:14px;margin:18px 0 24px}
.card{background:#fffdf7;border:1px solid #dfd4bd;border-radius:16px;padding:16px;box-shadow:0 10px 24px rgba(0,0,0,.05)}
.card .k{font-size:13px;color:#667065}
.card .v{font-size:24px;font-weight:700;color:#0f6b52;margin-top:8px}
.section{background:#fffdf7;border:1px solid #dfd4bd;border-radius:18px;padding:18px;margin:16px 0;box-shadow:0 10px 24px rgba(0,0,0,.05)}
table{width:100%;border-collapse:collapse;background:#fff}
th,td{padding:10px 12px;border-bottom:1px solid #ece6d8;text-align:right;white-space:nowrap}
th{background:#113b34;color:#fff}
th:first-child,td:first-child,td:nth-child(2),td:nth-child(3){text-align:left}
.good{color:#0f7a4f;font-weight:700}
.bad{color:#b44935;font-weight:700}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
ul{margin:0;padding-left:20px;line-height:1.75}
@media(max-width:980px){body{padding:16px}.cards,.grid{grid-template-columns:1fr}table{display:block;overflow:auto}}
"""
    base_best = summary["baseline_ma50"]["best_pnl"]
    ema_best = summary["ema55_variant"]["best_pnl"]
    delta = summary["head_to_head"]
    card_rows = [
        ("原 MA50 最佳", f"每波{base_best['max_entries_per_trend']} / SLx{base_best['atr_stop_multiplier']}", f"{base_best['total_pnl']:.4f}U"),
        ("EMA55 最佳", f"每波{ema_best['max_entries_per_trend']} / SLx{ema_best['atr_stop_multiplier']}", f"{ema_best['total_pnl']:.4f}U"),
        ("最佳收益差", "EMA55 - MA50", f"{delta['best_pnl_delta_u']:+.4f}U"),
        ("最佳回撤差", "EMA55 - MA50", f"{delta['best_pnl_delta_drawdown_u']:+.4f}U"),
    ]
    table_rows: list[str] = []
    for row in rows:
        pnl_cls = "good" if row.total_pnl > 0 else "bad"
        quality_cls = "good" if row.pnl_to_drawdown >= Decimal("1") else ""
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(row.group_label)}</td>"
            f"<td>{row.trend_line}</td>"
            f"<td>{row.entry_line}</td>"
            f"<td>{row.max_entries_per_trend}</td>"
            f"<td>{row.atr_stop_multiplier}</td>"
            f"<td class=\"{pnl_cls}\">{fmt(row.total_pnl, 4)}</td>"
            f"<td>{fmt(row.total_return_pct, 2)}%</td>"
            f"<td>{fmt(row.max_drawdown, 4)}</td>"
            f"<td>{fmt(row.max_drawdown_pct, 2)}%</td>"
            f"<td>{fmt(row.profit_factor, 4) if row.profit_factor is not None else '-'}</td>"
            f"<td>{fmt(row.win_rate, 2)}%</td>"
            f"<td>{row.total_trades}</td>"
            f"<td>{fmt(row.average_r_multiple, 4)}</td>"
            f"<td class=\"{quality_cls}\">{fmt(row.pnl_to_drawdown, 4)}</td>"
            "</tr>"
        )
    analysis_blocks: list[str] = []
    for group_key in ("baseline_ma50", "ema55_variant"):
        title = "原最佳参数矩阵（MA50）" if group_key == "baseline_ma50" else "替换矩阵（EMA55）"
        group_rows = [row for row in rows if row.group_key == group_key]
        best_pnl_row = pick_best(group_rows, "total_pnl", True)
        best_quality_row = pick_best(group_rows, "pnl_to_drawdown", True)
        lowest_dd_row = pick_best(group_rows, "max_drawdown", False)
        analysis_blocks.append(
            f"""
<div class="section">
  <h3>{html.escape(title)}</h3>
  <ul>
    <li>收益最高：{html.escape(best_pnl_row.trend_line)} / {html.escape(best_pnl_row.entry_line)} / 每波{best_pnl_row.max_entries_per_trend} / SLx{best_pnl_row.atr_stop_multiplier}，
    总盈亏 {fmt(best_pnl_row.total_pnl, 4)}U，回撤 {fmt(best_pnl_row.max_drawdown, 4)}U，PF {fmt(best_pnl_row.profit_factor, 4) if best_pnl_row.profit_factor is not None else '-'}</li>
    <li>风险收益比最好：每波{best_quality_row.max_entries_per_trend} / SLx{best_quality_row.atr_stop_multiplier}，
    盈亏/回撤 {fmt(best_quality_row.pnl_to_drawdown, 4)}，交易 {best_quality_row.total_trades} 笔</li>
    <li>回撤最低：每波{lowest_dd_row.max_entries_per_trend} / SLx{lowest_dd_row.atr_stop_multiplier}，
    最大回撤 {fmt(lowest_dd_row.max_drawdown, 4)}U，但对应总盈亏 {fmt(lowest_dd_row.total_pnl, 4)}U</li>
    <li>最优收益组合主要出场：{html.escape(best_pnl_row.exit_reason_top)}</li>
  </ul>
</div>
"""
        )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>BTC 动态委托做多 MA50 vs EMA55 双矩阵报告</title>
<style>{style}</style>
</head>
<body>
<h1>BTC 动态委托做多：MA50 vs EMA55 双矩阵报告</h1>
<div class="sub">模板来源：{html.escape(str(CONFIG_PATH))} | 标的：{html.escape(str(snapshot.get('inst_id', 'BTC-USDT-SWAP')))} | 周期：{html.escape(str(snapshot.get('bar', '1H')))}</div>
<div class="cards">
  {''.join(f'<div class="card"><div class="k">{html.escape(label)}</div><div>{html.escape(sub)}</div><div class="v">{html.escape(value)}</div></div>' for label, sub, value in card_rows)}
</div>
<div class="section">
  <h2>固定口径</h2>
  <ul>
    <li>快线：{moving_average_label(str_value(snapshot.get('ema_type'), 'ema'), int_value(snapshot.get('ema_period'), 21))}</li>
    <li>基线趋势/挂单参考：{moving_average_label(str_value(snapshot.get('trend_ema_type'), 'ma'), int_value(snapshot.get('trend_ema_period'), 50))} / {moving_average_label(str_value(snapshot.get('entry_reference_ema_type'), 'ma'), int_value(snapshot.get('entry_reference_ema_period'), 50))}</li>
    <li>替换版本趋势/挂单参考：EMA55 / EMA55</li>
    <li>矩阵变量：每波开仓 1 / 2 / 3；ATR 止损 1 / 1.5 / 2</li>
    <li>风险金：{html.escape(str(dec(snapshot.get('risk_amount'), '20')))}U；动态止盈：{html.escape(str(snapshot.get('take_profit_mode', 'dynamic')))}</li>
    <li>手续费口径：Maker {html.escape(str(MAKER_FEE_RATE))}；Taker {html.escape(str(TAKER_FEE_RATE))}</li>
  </ul>
</div>
<div class="section">
  <h2>矩阵总表</h2>
  <table>
    <thead>
      <tr>
        <th>组别</th><th>趋势线</th><th>挂单线</th><th>每波开仓</th><th>ATR止损</th><th>总盈亏U</th><th>收益率%</th><th>最大回撤U</th><th>回撤%</th><th>PF</th><th>胜率%</th><th>交易数</th><th>平均R</th><th>盈亏/回撤</th>
      </tr>
    </thead>
    <tbody>{''.join(table_rows)}</tbody>
  </table>
</div>
<div class="grid">
  {''.join(analysis_blocks)}
</div>
<div class="section">
  <h2>对打结论</h2>
  <ul>
    <li>各自最佳收益组合对比：EMA55 相对 MA50 的总盈亏差 {delta['best_pnl_delta_u']:+.4f}U</li>
    <li>收益率差：{delta['best_pnl_delta_return_pct']:+.2f} pct</li>
    <li>回撤差：{delta['best_pnl_delta_drawdown_u']:+.4f}U</li>
    <li>交易数差：{delta['best_pnl_delta_trades']:+d}</li>
  </ul>
</div>
</body>
</html>
"""


def write_outputs(snapshot: dict[str, Any], rows: list[MatrixRow], summary: dict[str, Any]) -> None:
    html_doc = build_html(snapshot, rows, summary)
    md_doc = build_markdown(snapshot, rows, summary)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "config_path": str(CONFIG_PATH),
        "assumptions": {
            "maker_fee_rate": str(MAKER_FEE_RATE),
            "taker_fee_rate": str(TAKER_FEE_RATE),
            "entry_options": list(ENTRY_OPTIONS),
            "stop_options": [str(item) for item in STOP_OPTIONS],
        },
        "template_snapshot": snapshot,
        "summary": summary,
        "rows": [row_to_payload(row) | {
            "group_key": row.group_key,
            "group_label": row.group_label,
            "trend_line": row.trend_line,
            "entry_line": row.entry_line,
        } for row in rows],
    }
    HTML_PATH.write_text(html_doc, encoding="utf-8")
    CSV_PATH.write_text("", encoding="utf-8")
    export_csv(CSV_PATH, rows)
    JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    MD_PATH.write_text(md_doc, encoding="utf-8")
    LATEST_HTML_PATH.write_text(html_doc, encoding="utf-8")
    LATEST_CSV_PATH.write_text(CSV_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    LATEST_JSON_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LATEST_MD_PATH.write_text(md_doc, encoding="utf-8")


def main() -> None:
    snapshot = load_template_snapshot()
    base_config = build_base_config(snapshot)
    candles = load_candles(base_config.inst_id, base_config.bar)
    rows: list[MatrixRow] = []
    for spec in build_matrix_specs(snapshot):
        rows.extend(run_matrix(base_config, candles, spec))
    rows.sort(key=lambda row: (row.group_key, row.max_entries_per_trend, Decimal(row.atr_stop_multiplier)))
    grouped = {
        "baseline_ma50": [row for row in rows if row.group_key == "baseline_ma50"],
        "ema55_variant": [row for row in rows if row.group_key == "ema55_variant"],
    }
    summary = build_summary(grouped)
    write_outputs(snapshot, rows, summary)
    print(HTML_PATH)
    print(CSV_PATH)
    print(JSON_PATH)
    print(MD_PATH)
    print(LATEST_HTML_PATH)


if __name__ == "__main__":
    main()
