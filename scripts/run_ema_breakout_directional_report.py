from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import (
    _build_backtest_data_source_note,
    _load_backtest_candles,
    _required_backtest_preload_candles,
    _run_backtest_with_loaded_data,
    run_backtest,
)
from okx_quant.backtest_export import export_single_backtest_report
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_EMA_BREAKDOWN_SHORT_ID, STRATEGY_EMA_BREAKOUT_LONG_ID

SYMBOL = "ETH-USDT-SWAP"
BARS: tuple[str, ...] = ("15m", "1H", "4H")
BAR_LABELS = {"15m": "15分钟", "1H": "1小时", "4H": "4小时"}
ENTRY_REFERENCE_EMAS: tuple[int, ...] = (21, 55)
STOP_ATRS: tuple[Decimal, ...] = (Decimal("1"), Decimal("1.5"), Decimal("2"))
TAKE_ATRS: tuple[Decimal, ...] = (Decimal("2"), Decimal("3"), Decimal("4"))
ATR_PERIOD = 10
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
CANDLE_LIMIT = 10_000
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")
MAIN_SLIPPAGE = Decimal("0.0003")
STRESS_SLIPPAGE = Decimal("0.0005")
FULL_HISTORY_START_TS = 0
STRATEGY_LABELS = {
    STRATEGY_EMA_BREAKOUT_LONG_ID: "EMA 突破做多策略",
    STRATEGY_EMA_BREAKDOWN_SHORT_ID: "EMA 跌破做空策略",
}
SIGNAL_LABELS = {
    STRATEGY_EMA_BREAKOUT_LONG_ID: "只做多",
    STRATEGY_EMA_BREAKDOWN_SHORT_ID: "只做空",
}


@dataclass(frozen=True)
class Scenario:
    key: str
    label: str
    entry_slippage: Decimal
    exit_slippage: Decimal


@dataclass(frozen=True)
class MatrixRow:
    scenario: str
    scenario_label: str
    strategy_id: str
    strategy_label: str
    bar: str
    bar_label: str
    entry_reference_ema: int
    atr_stop_multiplier: str
    atr_take_multiplier: str
    candle_limit: int
    total_trades: int
    win_rate: str
    total_pnl: str
    total_return_pct: str
    average_r_multiple: str
    max_drawdown: str
    max_drawdown_pct: str
    profit_factor: str
    profit_loss_ratio: str
    ending_equity: str
    total_fees: str
    slippage_costs: str
    funding_costs: str
    take_profit_hits: int
    stop_loss_hits: int
    data_source_note: str
    standard_report_path: str | None = None
    full_history_report_path: str | None = None


def build_strategy_config(
    *,
    strategy_id: str,
    bar: str,
    entry_reference_ema_period: int,
    atr_stop_multiplier: Decimal,
    atr_take_multiplier: Decimal,
    entry_slippage_rate: Decimal,
    exit_slippage_rate: Decimal,
) -> StrategyConfig:
    signal_mode = "long_only" if strategy_id == STRATEGY_EMA_BREAKOUT_LONG_ID else "short_only"
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=atr_stop_multiplier,
        atr_take_multiplier=atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=entry_slippage_rate,
        backtest_exit_slippage_rate=exit_slippage_rate,
        backtest_slippage_rate=exit_slippage_rate,
        backtest_funding_rate=Decimal("0"),
    )


def build_matrix_configs(scenario: Scenario) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for strategy_id in STRATEGY_LABELS:
        for bar in BARS:
            for entry_reference_ema in ENTRY_REFERENCE_EMAS:
                for stop_atr in STOP_ATRS:
                    for take_atr in TAKE_ATRS:
                        configs.append(
                            build_strategy_config(
                                strategy_id=strategy_id,
                                bar=bar,
                                entry_reference_ema_period=entry_reference_ema,
                                atr_stop_multiplier=stop_atr,
                                atr_take_multiplier=take_atr,
                                entry_slippage_rate=scenario.entry_slippage,
                                exit_slippage_rate=scenario.exit_slippage,
                            )
                        )
    return configs


def metric_key(config: StrategyConfig) -> tuple[str, str, int, str, str]:
    return (
        config.strategy_id,
        config.bar,
        config.entry_reference_ema_period,
        str(config.atr_stop_multiplier),
        str(config.atr_take_multiplier),
    )


def row_sort_key(row: MatrixRow) -> tuple[str, str, Decimal, Decimal, Decimal]:
    return (
        row.strategy_id,
        row.bar,
        -Decimal(row.total_pnl),
        -(Decimal(row.profit_factor) if row.profit_factor != "-" else Decimal("0")),
        Decimal(row.max_drawdown_pct),
    )


def winner_sort_key(row: MatrixRow) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    profit_factor = Decimal(row.profit_factor) if row.profit_factor != "-" else Decimal("0")
    return (
        Decimal(row.total_pnl),
        profit_factor,
        -Decimal(row.max_drawdown_pct),
        Decimal(row.average_r_multiple),
    )


def format_decimal(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def build_matrix_row(
    *,
    scenario: Scenario,
    config: StrategyConfig,
    result,
    data_source_note: str,
) -> MatrixRow:
    report = result.report
    return MatrixRow(
        scenario=scenario.key,
        scenario_label=scenario.label,
        strategy_id=config.strategy_id,
        strategy_label=STRATEGY_LABELS[config.strategy_id],
        bar=config.bar,
        bar_label=BAR_LABELS[config.bar],
        entry_reference_ema=config.entry_reference_ema_period,
        atr_stop_multiplier=str(config.atr_stop_multiplier),
        atr_take_multiplier=str(config.atr_take_multiplier),
        candle_limit=CANDLE_LIMIT,
        total_trades=report.total_trades,
        win_rate=format_decimal(report.win_rate, 2),
        total_pnl=format_decimal(report.total_pnl, 4),
        total_return_pct=format_decimal(report.total_return_pct, 2),
        average_r_multiple=format_decimal(report.average_r_multiple, 4),
        max_drawdown=format_decimal(report.max_drawdown, 4),
        max_drawdown_pct=format_decimal(report.max_drawdown_pct, 2),
        profit_factor=format_decimal(report.profit_factor, 4) if report.profit_factor is not None else "-",
        profit_loss_ratio=format_decimal(report.profit_loss_ratio, 4) if report.profit_loss_ratio is not None else "-",
        ending_equity=format_decimal(report.ending_equity, 4),
        total_fees=format_decimal(report.total_fees, 4),
        slippage_costs=format_decimal(report.slippage_costs, 4),
        funding_costs=format_decimal(report.funding_costs, 4),
        take_profit_hits=report.take_profit_hits,
        stop_loss_hits=report.stop_loss_hits,
        data_source_note=data_source_note,
    )


def summarise_rows(rows: list[MatrixRow]) -> dict[str, Any]:
    grouped: dict[tuple[str, str], list[MatrixRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.strategy_id, row.bar)].append(row)

    summary_rows: list[dict[str, Any]] = []
    for (strategy_id, bar), bucket in sorted(grouped.items()):
        sorted_bucket = sorted(bucket, key=winner_sort_key, reverse=True)
        best = sorted_bucket[0]
        positive_count = sum(1 for item in bucket if Decimal(item.total_pnl) > 0)
        summary_rows.append(
            {
                "strategy_id": strategy_id,
                "strategy_label": STRATEGY_LABELS[strategy_id],
                "bar": bar,
                "bar_label": BAR_LABELS[bar],
                "sample_count": len(bucket),
                "positive_count": positive_count,
                "positive_ratio_pct": format_decimal(Decimal(positive_count * 100) / Decimal(len(bucket)), 2),
                "best_total_pnl": best.total_pnl,
                "best_profit_factor": best.profit_factor,
                "best_max_drawdown_pct": best.max_drawdown_pct,
                "best_entry_reference_ema": best.entry_reference_ema,
                "best_atr_stop_multiplier": best.atr_stop_multiplier,
                "best_atr_take_multiplier": best.atr_take_multiplier,
            }
        )
    return {"groups": summary_rows}


def build_text_report(
    *,
    generated_at: str,
    scenarios: list[Scenario],
    rows_by_scenario: dict[str, list[MatrixRow]],
    winners: dict[tuple[str, str], MatrixRow],
    full_history_rows: list[MatrixRow],
) -> str:
    lines = [
        "EMA 突破做多 / EMA 跌破做空 回测报告",
        "=" * 72,
        f"生成时间(UTC)：{generated_at}",
        f"标的：{SYMBOL}",
        "正式测试范围：15分钟 / 1小时 / 4小时；5分钟本轮不纳入正式结论。",
        f"标准样本：每组 {CANDLE_LIMIT} 根 K 线；前 200 根用于预热，不参与回测统计。",
        f"本金：{INITIAL_CAPITAL}；仓位：固定风险金 {RISK_AMOUNT}；不复利。",
        f"手续费：Maker {MAKER_FEE * Decimal('100'):.4f}% / Taker {TAKER_FEE * Decimal('100'):.4f}%。",
        f"主滑点：开 {MAIN_SLIPPAGE * Decimal('100'):.4f}% / 平 {MAIN_SLIPPAGE * Decimal('100'):.4f}%。",
        f"压力滑点：开 {STRESS_SLIPPAGE * Decimal('100'):.4f}% / 平 {STRESS_SLIPPAGE * Decimal('100'):.4f}%。",
        "参数矩阵：参考EMA=21/55；止损ATR=1/1.5/2；止盈ATR=2/3/4；动态止盈；2R保本开启；手续费偏移开启。",
        "",
    ]

    for scenario in scenarios:
        lines.append(f"[{scenario.label}]")
        scenario_rows = rows_by_scenario[scenario.key]
        grouped = defaultdict(list)
        for row in scenario_rows:
            grouped[(row.strategy_id, row.bar)].append(row)
        for strategy_id in STRATEGY_LABELS:
            for bar in BARS:
                bucket = grouped[(strategy_id, bar)]
                bucket_sorted = sorted(bucket, key=winner_sort_key, reverse=True)
                best = bucket_sorted[0]
                positive_count = sum(1 for item in bucket if Decimal(item.total_pnl) > 0)
                lines.append(
                    (
                        f"- {STRATEGY_LABELS[strategy_id]} | {BAR_LABELS[bar]} | 正收益 {positive_count}/{len(bucket)} | "
                        f"最优：EMA{best.entry_reference_ema} / SLx{best.atr_stop_multiplier} / TPx{best.atr_take_multiplier} | "
                        f"总盈亏 {best.total_pnl} | PF {best.profit_factor} | 回撤 {best.max_drawdown_pct}%"
                    )
                )
        lines.append("")

    lines.extend(["长样本确认（主滑点口径）", "-" * 72])
    for row in full_history_rows:
        lines.append(
            (
                f"- {row.strategy_label} | {row.bar_label} | EMA{row.entry_reference_ema} / "
                f"SLx{row.atr_stop_multiplier} / TPx{row.atr_take_multiplier} | "
                f"全量总盈亏 {row.total_pnl} | 收益率 {row.total_return_pct}% | PF {row.profit_factor} | "
                f"回撤 {row.max_drawdown_pct}% | 报告 {row.full_history_report_path or '-'}"
            )
        )

    lines.extend(["", "本轮选择逻辑", "-" * 72])
    lines.append("1. 先在主滑点口径下，对每个“策略 × 周期”分组挑出 10000 根样本的最优参数。")
    lines.append("2. 再把这 6 组胜出参数拿去做全量历史确认，检查是否只在最近阶段有效。")
    lines.append("3. 压力滑点结果保留在矩阵汇总中，用来观察 edge 对执行摩擦的脆弱程度。")
    lines.append("")
    lines.append("胜出参数（主滑点、10000根样本）")
    lines.append("-" * 72)
    for key in sorted(winners):
        item = winners[key]
        lines.append(
            (
                f"- {item.strategy_label} | {item.bar_label} | EMA{item.entry_reference_ema} / "
                f"SLx{item.atr_stop_multiplier} / TPx{item.atr_take_multiplier} | "
                f"总盈亏 {item.total_pnl} | PF {item.profit_factor} | 回撤 {item.max_drawdown_pct}% | "
                f"标准报告 {item.standard_report_path or '-'}"
            )
        )
    return "\n".join(lines) + "\n"


def to_jsonable(rows: list[MatrixRow]) -> list[dict[str, Any]]:
    return [asdict(item) for item in rows]


def export_csv(path: Path, rows: list[MatrixRow]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys())
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def main() -> None:
    client = OkxRestClient()
    generated_at = datetime.now(timezone.utc)
    stamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    report_dir = analysis_report_dir_path()
    report_dir.mkdir(parents=True, exist_ok=True)

    main_scenario = Scenario("main", "主滑点 0.03%/0.03%", MAIN_SLIPPAGE, MAIN_SLIPPAGE)
    stress_scenario = Scenario("stress", "压力滑点 0.05%/0.05%", STRESS_SLIPPAGE, STRESS_SLIPPAGE)
    scenarios = [main_scenario, stress_scenario]
    data_source_note = _build_backtest_data_source_note(client)
    instrument = client.get_instrument(SYMBOL)

    candle_cache: dict[str, list[Any]] = {}
    rows_by_scenario: dict[str, list[MatrixRow]] = {}
    results_by_scenario: dict[str, dict[tuple[str, str, int, str, str], tuple[StrategyConfig, Any]]] = {}

    max_preload = _required_backtest_preload_candles(
        build_strategy_config(
            strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
            bar="1H",
            entry_reference_ema_period=max(ENTRY_REFERENCE_EMAS),
            atr_stop_multiplier=max(STOP_ATRS),
            atr_take_multiplier=max(TAKE_ATRS),
            entry_slippage_rate=MAIN_SLIPPAGE,
            exit_slippage_rate=MAIN_SLIPPAGE,
        )
    )

    for bar in BARS:
        print(f"loading {SYMBOL} {bar} {CANDLE_LIMIT} candles", flush=True)
        candle_cache[bar] = _load_backtest_candles(
            client,
            SYMBOL,
            bar,
            CANDLE_LIMIT,
            preload_count=max_preload,
        )

    print(f"running scenario: {main_scenario.label}", flush=True)
    main_rows: list[MatrixRow] = []
    main_results: dict[tuple[str, str, int, str, str], tuple[StrategyConfig, Any]] = {}
    main_configs = build_matrix_configs(main_scenario)
    for index, config in enumerate(main_configs, start=1):
        if index % 20 == 1 or index == len(main_configs):
            print(
                f"  [{index}/{len(main_configs)}] {STRATEGY_LABELS[config.strategy_id]} "
                f"{config.bar} EMA{config.entry_reference_ema_period} SLx{config.atr_stop_multiplier} TPx{config.atr_take_multiplier}",
                flush=True,
            )
        result = _run_backtest_with_loaded_data(
            candle_cache[config.bar],
            instrument,
            config,
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        row = build_matrix_row(
            scenario=main_scenario,
            config=config,
            result=result,
            data_source_note=data_source_note,
        )
        main_rows.append(row)
        main_results[metric_key(config)] = (config, result)
    rows_by_scenario[main_scenario.key] = sorted(main_rows, key=row_sort_key)
    results_by_scenario[main_scenario.key] = main_results

    winners: dict[tuple[str, str], MatrixRow] = {}
    grouped_main: dict[tuple[str, str], list[MatrixRow]] = defaultdict(list)
    for row in rows_by_scenario["main"]:
        grouped_main[(row.strategy_id, row.bar)].append(row)
    for key, bucket in grouped_main.items():
        winners[key] = sorted(bucket, key=winner_sort_key, reverse=True)[0]

    print(f"running scenario: {stress_scenario.label}（仅复核主滑点胜出组）", flush=True)
    stress_rows: list[MatrixRow] = []
    stress_results: dict[tuple[str, str, int, str, str], tuple[StrategyConfig, Any]] = {}
    for key in sorted(winners):
        item = winners[key]
        config = build_strategy_config(
            strategy_id=item.strategy_id,
            bar=item.bar,
            entry_reference_ema_period=item.entry_reference_ema,
            atr_stop_multiplier=Decimal(item.atr_stop_multiplier),
            atr_take_multiplier=Decimal(item.atr_take_multiplier),
            entry_slippage_rate=stress_scenario.entry_slippage,
            exit_slippage_rate=stress_scenario.exit_slippage,
        )
        result = _run_backtest_with_loaded_data(
            candle_cache[config.bar],
            instrument,
            config,
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        row = build_matrix_row(
            scenario=stress_scenario,
            config=config,
            result=result,
            data_source_note=data_source_note,
        )
        stress_rows.append(row)
        stress_results[metric_key(config)] = (config, result)
    rows_by_scenario[stress_scenario.key] = sorted(stress_rows, key=row_sort_key)
    results_by_scenario[stress_scenario.key] = stress_results

    exported_at = datetime.now()
    full_history_rows: list[MatrixRow] = []
    updated_main_rows: list[MatrixRow] = []
    standard_path_map: dict[tuple[str, str], str] = {}
    full_path_map: dict[tuple[str, str], str] = {}

    for key in sorted(winners):
        winner_row = winners[key]
        config, result = results_by_scenario["main"][
            (winner_row.strategy_id, winner_row.bar, winner_row.entry_reference_ema, winner_row.atr_stop_multiplier, winner_row.atr_take_multiplier)
        ]
        standard_report_path = export_single_backtest_report(
            result,
            config,
            CANDLE_LIMIT,
            exported_at=exported_at,
        )
        standard_path_map[key] = str(standard_report_path)
        winners[key] = MatrixRow(**{**asdict(winner_row), "standard_report_path": str(standard_report_path)})

        print(f"full-history confirm: {winner_row.strategy_label} {winner_row.bar_label} EMA{winner_row.entry_reference_ema} SLx{winner_row.atr_stop_multiplier} TPx{winner_row.atr_take_multiplier}", flush=True)
        full_result = run_backtest(
            client,
            config,
            candle_limit=0,
            start_ts=FULL_HISTORY_START_TS,
            end_ts=int(datetime.now(timezone.utc).timestamp() * 1000),
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        full_report_path = export_single_backtest_report(
            full_result,
            config,
            0,
            exported_at=exported_at,
        )
        full_path_map[key] = str(full_report_path)
        full_history_rows.append(
            MatrixRow(
                **{
                    **asdict(
                        build_matrix_row(
                            scenario=scenarios[0],
                            config=config,
                            result=full_result,
                            data_source_note=full_result.data_source_note,
                        )
                    ),
                    "scenario": "full_history_main",
                    "scenario_label": "全量历史确认（主滑点）",
                    "candle_limit": len(full_result.candles),
                    "standard_report_path": str(standard_report_path),
                    "full_history_report_path": str(full_report_path),
                }
            )
        )

    for row in main_rows:
        key = (row.strategy_id, row.bar)
        if key in winners and row.entry_reference_ema == winners[key].entry_reference_ema and row.atr_stop_multiplier == winners[key].atr_stop_multiplier and row.atr_take_multiplier == winners[key].atr_take_multiplier:
            updated_main_rows.append(
                MatrixRow(
                    **{
                        **asdict(row),
                        "standard_report_path": standard_path_map.get(key),
                        "full_history_report_path": full_path_map.get(key),
                    }
                )
            )
        else:
            updated_main_rows.append(row)
    rows_by_scenario["main"] = updated_main_rows

    flat_rows = rows_by_scenario["main"] + rows_by_scenario["stress"]
    csv_path = report_dir / f"ema_breakout_directional_matrix_{stamp}_full.csv"
    json_path = report_dir / f"ema_breakout_directional_matrix_{stamp}_summary.json"
    report_path = report_dir / f"ema_breakout_directional_matrix_{stamp}_report.txt"
    leader_path = report_dir / f"ema_breakout_directional_matrix_{stamp}_leader_onepage.txt"

    export_csv(csv_path, flat_rows)

    summary_payload = {
        "generated_at": generated_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "bars": list(BARS),
        "candle_limit": CANDLE_LIMIT,
        "initial_capital": str(INITIAL_CAPITAL),
        "risk_amount": str(RISK_AMOUNT),
        "maker_fee": str(MAKER_FEE),
        "taker_fee": str(TAKER_FEE),
        "scenarios": [
            {
                "key": item.key,
                "label": item.label,
                "entry_slippage": str(item.entry_slippage),
                "exit_slippage": str(item.exit_slippage),
                "summary": summarise_rows(rows_by_scenario[item.key]),
            }
            for item in scenarios
        ],
        "standard_rows": to_jsonable(flat_rows),
        "winners_main_10000": [asdict(item) for item in winners.values()],
        "full_history_rows": [asdict(item) for item in full_history_rows],
        "notes": [
            "5分钟周期未纳入本轮正式结论，只在后续需要时作为执行层压力测试补充。",
            "主结论基于 0.03%/0.03% 双边滑点。",
            "0.05%/0.05% 压力滑点不再跑全矩阵，只复核主滑点下每个策略 × 周期的胜出参数组。",
            "全量历史确认仅针对主滑点口径下，每个策略 × 周期的胜出参数组执行。",
        ],
    }
    json_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    report_text = build_text_report(
        generated_at=generated_at.isoformat(timespec="seconds").replace("+00:00", "Z"),
        scenarios=scenarios,
        rows_by_scenario=rows_by_scenario,
        winners=winners,
        full_history_rows=full_history_rows,
    )
    report_path.write_text(report_text, encoding="utf-8-sig")

    leader_lines = [
        "EMA 突破/跌破 回测一页结论",
        "=" * 60,
        f"生成时间(UTC)：{generated_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"标的：{SYMBOL}",
        "范围：15m / 1H / 4H；10000根标准样本 + 胜出组全量历史确认。",
        "摩擦口径：主滑点 0.03%/0.03%；压力滑点 0.05%/0.05%；M/T 手续费 0.015% / 0.036%。",
        "",
        "主滑点胜出参数：",
    ]
    for key in sorted(winners):
        item = winners[key]
        leader_lines.append(
            (
                f"- {item.strategy_label} | {item.bar_label} | EMA{item.entry_reference_ema} / "
                f"SLx{item.atr_stop_multiplier} / TPx{item.atr_take_multiplier} | "
                f"10000根总盈亏 {item.total_pnl} | PF {item.profit_factor} | 回撤 {item.max_drawdown_pct}%"
            )
        )
    leader_lines.extend(["", "全量历史确认："])
    for item in full_history_rows:
        leader_lines.append(
            (
                f"- {item.strategy_label} | {item.bar_label} | EMA{item.entry_reference_ema} / "
                f"SLx{item.atr_stop_multiplier} / TPx{item.atr_take_multiplier} | "
                f"全量总盈亏 {item.total_pnl} | 收益率 {item.total_return_pct}% | PF {item.profit_factor}"
            )
        )
    leader_lines.append("")
    leader_lines.append(f"完整汇总：{report_path}")
    leader_lines.append(f"CSV 明细：{csv_path}")
    leader_lines.append(f"JSON 汇总：{json_path}")
    leader_path.write_text("\n".join(leader_lines) + "\n", encoding="utf-8-sig")

    print("done", flush=True)
    print(f"report_txt={report_path}", flush=True)
    print(f"leader_txt={leader_path}", flush=True)
    print(f"summary_json={json_path}", flush=True)
    print(f"full_csv={csv_path}", flush=True)


if __name__ == "__main__":
    main()
