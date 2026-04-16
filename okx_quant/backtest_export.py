from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.backtest import BacktestResult, format_backtest_report
from okx_quant.backtest_audit import (
    batch_backtest_artifact_paths,
    export_batch_backtest_manifest,
    export_single_backtest_audit_files,
    single_backtest_artifact_paths,
)
from okx_quant.backtest_strategy_pool import is_strategy_pool_config, strategy_pool_profile_name
from okx_quant.models import StrategyConfig
from okx_quant.persistence import backtest_report_export_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import BACKTEST_STRATEGY_DEFINITIONS, is_dynamic_strategy_id, is_slot_handoff_strategy_id


STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in BACKTEST_STRATEGY_DEFINITIONS}
BAR_VALUE_TO_LABEL = {
    "5m": "5分钟",
    "15m": "15分钟",
    "1H": "1小时",
    "4H": "4小时",
}
SIGNAL_VALUE_TO_LABEL = {
    "both": "双向",
    "long_only": "只做多",
    "short_only": "只做空",
}
EXIT_REASON_TO_LABEL = {
    "take_profit": "止盈",
    "stop_loss": "止损",
}
FEE_TYPE_TO_LABEL = {
    "maker": "Maker",
    "taker": "Taker",
    "none": "-",
}


def export_single_backtest_report(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    *,
    exported_at: datetime | None = None,
    base_dir: Path | None = None,
) -> Path:
    exported_at = exported_at or datetime.now()
    report_dir = backtest_report_export_dir_path(base_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    file_name = (
        f"single_{exported_at.strftime('%Y%m%d_%H%M%S')}_"
        f"{_sanitize_filename_part(config.strategy_id)}_"
        f"{_sanitize_filename_part(config.inst_id)}_"
        f"{_sanitize_filename_part(config.bar)}_"
        f"{_sanitize_filename_part(config.signal_mode)}.txt"
    )
    target = report_dir / file_name
    artifact_paths = single_backtest_artifact_paths(target)
    target.write_text(
        _build_single_backtest_report_text(
            result,
            config,
            candle_limit,
            exported_at,
            artifact_paths=artifact_paths,
        ),
        encoding="utf-8-sig",
    )
    export_single_backtest_audit_files(
        result,
        config,
        candle_limit,
        exported_at=exported_at,
        report_path=target,
    )
    return target


def export_batch_backtest_report(
    results: list[tuple[StrategyConfig, BacktestResult]],
    candle_limit: int,
    *,
    batch_label: str | None = None,
    exported_at: datetime | None = None,
    base_dir: Path | None = None,
) -> Path:
    if not results:
        raise ValueError("批量回测结果为空，无法导出报告。")
    exported_at = exported_at or datetime.now()
    first_config = results[0][0]
    report_dir = backtest_report_export_dir_path(base_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    batch_name = batch_label or "batch"
    batch_mode = _resolve_batch_mode(first_config)
    sorted_results = sorted(results, key=lambda item: _batch_result_sort_key(item[0], batch_mode))
    file_name = (
        f"batch_{exported_at.strftime('%Y%m%d_%H%M%S')}_"
        f"{_sanitize_filename_part(batch_name)}_"
        f"{_sanitize_filename_part(first_config.strategy_id)}_"
        f"{_sanitize_filename_part(first_config.inst_id)}_"
        f"{_sanitize_filename_part(first_config.bar)}_"
        f"{_sanitize_filename_part(first_config.signal_mode)}.txt"
    )
    target = report_dir / file_name
    artifact_paths = batch_backtest_artifact_paths(target)
    target.write_text(
        _build_batch_backtest_report_text(
            sorted_results,
            candle_limit,
            batch_name,
            exported_at,
            artifact_paths=artifact_paths,
        ),
        encoding="utf-8-sig",
    )
    detail_dir = artifact_paths["detail_dir"]
    detail_dir.mkdir(parents=True, exist_ok=True)
    detail_records: list[dict[str, object]] = []
    for index, (config, result) in enumerate(sorted_results, start=1):
        detail_path = detail_dir / _build_batch_detail_file_name(index, config, batch_mode)
        detail_artifact_paths = single_backtest_artifact_paths(detail_path)
        detail_path.write_text(
            _build_single_backtest_report_text(
                result,
                config,
                candle_limit,
                exported_at,
                artifact_paths=detail_artifact_paths,
            ),
            encoding="utf-8-sig",
        )
        export_single_backtest_audit_files(
            result,
            config,
            candle_limit,
            exported_at=exported_at,
            report_path=detail_path,
        )
        detail_records.append(
            {
                "index": index,
                "label": _build_batch_result_title(index, config, batch_mode),
                "config": config,
                "result": result,
                "report_path": detail_path,
            }
        )
    export_batch_backtest_manifest(
        report_path=target,
        batch_label=batch_name,
        candle_limit=candle_limit,
        exported_at=exported_at,
        results=detail_records,
    )
    return target


def _build_single_backtest_report_text(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    exported_at: datetime,
    *,
    artifact_paths: dict[str, Path] | None = None,
) -> str:
    strategy_name = STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id)
    lines = [
        "策略回测报告",
        "=" * 72,
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"策略：{strategy_name}",
        f"交易对：{config.inst_id}",
        f"周期：{BAR_VALUE_TO_LABEL.get(config.bar, config.bar)}",
        f"信号方向：{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}",
        f"回测K线数：{candle_limit}",
        f"开始时间：{_format_result_start(result)}",
        f"结束时间：{_format_result_end(result)}",
        f"参数摘要：{_build_param_summary(config, result)}",
    ]
    if result.data_source_note:
        lines.append(f"数据来源：{result.data_source_note}")
    if artifact_paths is not None:
        lines.extend(
            [
                f"资金审计文件：{artifact_paths['capital']}",
                f"操作日志文件：{artifact_paths['operations']}",
                f"审计清单：{artifact_paths['manifest']}",
            ]
        )
    lines.extend(
        [
            "",
            "回测报告",
            "-" * 72,
            format_backtest_report(result),
            "",
            "交易明细",
            "-" * 72,
            _build_trade_lines(result),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _build_batch_backtest_report_text(
    results: list[tuple[StrategyConfig, BacktestResult]],
    candle_limit: int,
    batch_label: str,
    exported_at: datetime,
    *,
    artifact_paths: dict[str, Path] | None = None,
) -> str:
    first_config = results[0][0]
    strategy_name = STRATEGY_ID_TO_NAME.get(first_config.strategy_id, first_config.strategy_id)
    batch_mode = _resolve_batch_mode(first_config)
    sorted_results = sorted(results, key=lambda item: _batch_result_sort_key(item[0], batch_mode))
    starts = [result.candles[0].ts for _, result in sorted_results if result.candles]
    ends = [result.candles[-1].ts for _, result in sorted_results if result.candles]
    lines = [
        "策略批量回测报告",
        "=" * 72,
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"批次：{batch_label}",
        f"策略：{strategy_name}",
        f"交易对：{first_config.inst_id}",
        f"周期：{BAR_VALUE_TO_LABEL.get(first_config.bar, first_config.bar)}",
        f"信号方向：{SIGNAL_VALUE_TO_LABEL.get(first_config.signal_mode, first_config.signal_mode)}",
        f"回测K线数：{candle_limit}",
        f"开始时间：{_format_timestamp(min(starts)) if starts else '-'}",
        f"结束时间：{_format_timestamp(max(ends)) if ends else '-'}",
        _build_batch_scope_line(sorted_results, batch_mode),
    ]
    notes = {result.data_source_note for _, result in sorted_results if result.data_source_note}
    if len(notes) == 1:
        lines.append(f"数据来源：{next(iter(notes))}")
    if artifact_paths is not None:
        lines.extend(
            [
                f"批量明细目录：{artifact_paths['detail_dir']}",
                f"批量审计清单：{artifact_paths['manifest']}",
            ]
        )
    lines.extend(
        [
            "",
            "矩阵对比",
            "-" * 72,
            _build_batch_matrix_lines(sorted_results, batch_mode),
            "",
            "分项报告",
            "-" * 72,
        ]
    )
    for index, (config, result) in enumerate(sorted_results, start=1):
        title = _build_batch_result_title(index, config, batch_mode)
        lines.extend([title, format_backtest_report(result), ""])
    return "\n".join(lines).rstrip() + "\n"


def _build_trade_lines(result: BacktestResult) -> str:
    if not result.trades:
        return "本次回测没有成交交易。"
    lines = [
        "方向 | 进场时间 | 进场价格 | 出场时间 | 出场价格 | 原因 | 毛盈亏 | 手续费 | 净盈亏 | R倍数",
    ]
    for trade in result.trades:
        direction = "做多" if trade.signal == "long" else "做空"
        reason = EXIT_REASON_TO_LABEL.get(trade.exit_reason, trade.exit_reason)
        fee_note = (
            f"{format_decimal_fixed(trade.total_fee, 4)}"
            f" ({FEE_TYPE_TO_LABEL.get(trade.entry_fee_type, trade.entry_fee_type)}/"
            f"{FEE_TYPE_TO_LABEL.get(trade.exit_fee_type, trade.exit_fee_type)})"
        )
        lines.append(
            " | ".join(
                [
                    direction,
                    _format_timestamp(trade.entry_ts),
                    format_decimal_fixed(trade.entry_price, 4),
                    _format_timestamp(trade.exit_ts),
                    format_decimal_fixed(trade.exit_price, 4),
                    reason,
                    format_decimal_fixed(trade.gross_pnl, 4),
                    fee_note,
                    format_decimal_fixed(trade.pnl, 4),
                    format_decimal_fixed(trade.r_multiple, 4),
                ]
            )
        )
    return "\n".join(lines)


def _resolve_batch_mode(config: StrategyConfig) -> str:
    if is_strategy_pool_config(config):
        return "strategy_pool"
    if is_dynamic_strategy_id(config.strategy_id):
        if config.take_profit_mode == "dynamic":
            return "dynamic_entries"
        return "fixed_entries"
    return "atr_matrix"


def _batch_result_sort_key(config: StrategyConfig, batch_mode: str) -> tuple[object, ...]:
    if batch_mode == "strategy_pool":
        return (
            config.backtest_profile_id or config.backtest_profile_name,
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.atr_stop_multiplier,
            config.atr_take_multiplier,
        )
    if batch_mode == "dynamic_entries":
        return (config.atr_stop_multiplier, config.max_entries_per_trend)
    if batch_mode == "fixed_entries":
        return (
            config.max_entries_per_trend,
            config.atr_stop_multiplier,
            config.atr_take_multiplier,
        )
    return (config.atr_stop_multiplier, config.atr_take_multiplier)


def _format_max_entries_label(value: int) -> str:
    return "不限(0)" if value <= 0 else f"{value}次"


def _build_batch_result_title(index: int, config: StrategyConfig, batch_mode: str) -> str:
    if batch_mode == "strategy_pool":
        return (
            f"[{index}] {strategy_pool_profile_name(config)} | "
            f"EMA{config.ema_period}/{config.trend_ema_period} | "
            f"ATR{config.atr_period} | "
            f"SL x{format_decimal(config.atr_stop_multiplier)} | "
            f"TP x{format_decimal(config.atr_take_multiplier)}"
        )
    if batch_mode == "dynamic_entries":
        return (
            f"[{index}] 动态止盈 | SL x{format_decimal(config.atr_stop_multiplier)} | "
            f"每波最多开仓次数：{_format_max_entries_label(config.max_entries_per_trend)}"
        )
    if batch_mode == "fixed_entries":
        return (
            f"[{index}] 每波最多开仓次数：{_format_max_entries_label(config.max_entries_per_trend)} | "
            f"SL x{format_decimal(config.atr_stop_multiplier)} | "
            f"TP x{format_decimal(config.atr_take_multiplier)}"
        )
    return (
        f"[{index}] SL x{format_decimal(config.atr_stop_multiplier)} | "
        f"TP x{format_decimal(config.atr_take_multiplier)}"
    )


def _build_batch_detail_file_name(index: int, config: StrategyConfig, batch_mode: str) -> str:
    if batch_mode == "strategy_pool":
        suffix = (
            f"{config.backtest_profile_id or 'candidate'}_"
            f"ema_{config.ema_period}_{config.trend_ema_period}_"
            f"atr_{config.atr_period}"
        )
    elif batch_mode == "dynamic_entries":
        suffix = (
            f"sl_x{format_decimal(config.atr_stop_multiplier)}_"
            f"entries_{config.max_entries_per_trend}"
        )
    elif batch_mode == "fixed_entries":
        suffix = (
            f"entries_{config.max_entries_per_trend}_"
            f"sl_x{format_decimal(config.atr_stop_multiplier)}_"
            f"tp_x{format_decimal(config.atr_take_multiplier)}"
        )
    else:
        suffix = (
            f"sl_x{format_decimal(config.atr_stop_multiplier)}_"
            f"tp_x{format_decimal(config.atr_take_multiplier)}"
        )
    return f"{index:02d}_{_sanitize_filename_part(suffix)}.txt"


def _build_batch_scope_line(
    results: list[tuple[StrategyConfig, BacktestResult]],
    batch_mode: str,
) -> str:
    maker_fee = _format_percent(results[0][1].maker_fee_rate)
    taker_fee = _format_percent(results[0][1].taker_fee_rate)
    if batch_mode == "strategy_pool":
        first_config = results[0][0]
        return (
            "参数范围：5m 槽位接管候选策略池；"
            f"候选数 = {len(results)}；"
            f"最大槽位 = {first_config.max_entries_per_trend}；"
            f"单槽位数量 = {format_decimal(first_config.order_size)}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "dynamic_entries":
        return (
            "参数范围：动态止盈；"
            f"挂单参考EMA = EMA{results[0][0].resolved_entry_reference_ema_period()}；"
            "SL = 1/1.5/2 ATR；"
            "每波最多开仓次数 = 0/1/2/3；"
            f"2R保本 = {results[0][0].dynamic_two_r_break_even_label()}；"
            f"手续费偏移 = {results[0][0].dynamic_fee_offset_enabled_label()}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "fixed_entries":
        return (
            f"参数范围：挂单参考EMA = EMA{results[0][0].resolved_entry_reference_ema_period()}；"
            "每波最多开仓次数 = 0/1/2/3；"
            "SL = 1/1.5/2 ATR；"
            "TP = SL x1/x2/x3；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    return f"参数范围：SL = 1/1.5/2 ATR；TP = SL x1/x2/x3；手续费 M/T = {maker_fee} / {taker_fee}"


def _build_batch_matrix_lines(results: list[tuple[StrategyConfig, BacktestResult]], batch_mode: str) -> str:
    if batch_mode == "strategy_pool":
        lines = ["候选策略 | 参数 | 总盈亏 | 胜率 | 交易数 | 期末人工池 | 峰值占槽"]
        for config, result in results:
            lines.append(
                " | ".join(
                    [
                        strategy_pool_profile_name(config),
                        (
                            f"EMA{config.ema_period}/{config.trend_ema_period} "
                            f"ATR{config.atr_period} "
                            f"SLx{format_decimal(config.atr_stop_multiplier)} "
                            f"TPx{format_decimal(config.atr_take_multiplier)}"
                        ),
                        format_decimal_fixed(result.report.total_pnl, 4),
                        f"{format_decimal_fixed(result.report.win_rate, 2)}%",
                        f"{result.report.total_trades}笔",
                        f"{result.report.manual_open_positions}笔",
                        str(result.report.max_total_occupied_slots),
                    ]
                )
            )
        return "\n".join(lines)

    if batch_mode == "dynamic_entries":
        header = ["SL \\\\ 每波最多开仓次数", "不限(0)", "1次", "2次", "3次"]
        rows = [" | ".join(header)]
        result_map = {
            (config.atr_stop_multiplier, config.max_entries_per_trend): result
            for config, result in results
        }
        stop_values = sorted({config.atr_stop_multiplier for config, _ in results})
        for stop_value in stop_values:
            cells = [f"SL x{format_decimal(stop_value)}"]
            for entry_limit in (0, 1, 2, 3):
                matched = result_map.get((stop_value, entry_limit))
                cells.append("-" if matched is None else _build_matrix_cell_text(matched))
            rows.append(" | ".join(cells))
        return "\n".join(rows)

    if batch_mode == "fixed_entries":
        groups: dict[int, list[tuple[StrategyConfig, BacktestResult]]] = {}
        for config, result in results:
            groups.setdefault(config.max_entries_per_trend, []).append((config, result))
        lines: list[str] = []
        for entry_limit in (0, 1, 2, 3):
            group = groups.get(entry_limit)
            if not group:
                continue
            lines.append(f"【每波最多开仓次数：{_format_max_entries_label(entry_limit)}】")
            lines.append(_build_matrix_lines(group))
            lines.append("")
        return "\n".join(lines).rstrip()

    return _build_matrix_lines(results)


def _build_matrix_lines(results: list[tuple[StrategyConfig, BacktestResult]]) -> str:
    header = ["SL \\\\ TP", "TP = SL x1", "TP = SL x2", "TP = SL x3"]
    rows = [" | ".join(header)]
    stop_values = sorted({config.atr_stop_multiplier for config, _ in results})
    for stop_value in stop_values:
        cells = [f"SL x{format_decimal(stop_value)}"]
        for take_ratio in (1, 2, 3):
            target_take = stop_value * take_ratio
            matched = next(
                (
                    result
                    for config, result in results
                    if config.atr_stop_multiplier == stop_value and config.atr_take_multiplier == target_take
                ),
                None,
            )
            cells.append("-" if matched is None else _build_matrix_cell_text(matched))
        rows.append(" | ".join(cells))
    return "\n".join(rows)


def _build_matrix_cell_text(result: BacktestResult) -> str:
    return (
        f"{format_decimal_fixed(result.report.total_pnl, 4)} | "
        f"{format_decimal_fixed(result.report.win_rate, 2)}% | "
        f"{result.report.total_trades}笔"
    )


def _build_param_summary(config: StrategyConfig, result: BacktestResult) -> str:
    if config.backtest_sizing_mode == "risk_percent":
        sizing_text = f"风险百分比{format_decimal(config.backtest_risk_percent or Decimal('0'))}%"
    elif config.backtest_sizing_mode == "fixed_size":
        sizing_text = f"固定数量{format_decimal(config.order_size)}"
    else:
        sizing_text = f"固定风险金{format_decimal(config.risk_amount or Decimal('0'))}"
    parts = [
        f"EMA{config.ema_period}",
        f"趋势EMA{config.trend_ema_period}",
        f"ATR{config.atr_period}",
        f"SL x{format_decimal(config.atr_stop_multiplier)}",
        f"TP x{format_decimal(config.atr_take_multiplier)}",
    ]
    if config.backtest_profile_name:
        parts.insert(0, f"候选{config.backtest_profile_name}")
    if is_dynamic_strategy_id(config.strategy_id):
        parts.insert(2, f"挂单EMA{config.resolved_entry_reference_ema_period()}")
        parts.append(f"止盈方式{'动态止盈' if config.take_profit_mode == 'dynamic' else '固定止盈'}")
        if config.take_profit_mode == "dynamic":
            parts.append(f"2R保本{config.dynamic_two_r_break_even_label()}")
            parts.append(f"手续费偏移{config.dynamic_fee_offset_enabled_label()}")
        parts.append(f"每波最多开仓次数{_format_max_entries_label(config.max_entries_per_trend)}")
    elif is_slot_handoff_strategy_id(config.strategy_id):
        parts.append(f"最大槽位{config.max_entries_per_trend}")
        parts.append(f"单槽位{format_decimal(config.order_size)}")
    parts.extend(
        [
            f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}",
            f"仓位{sizing_text}",
            f"本金{format_decimal_fixed(config.backtest_initial_capital, 2)}",
            "复利" if config.backtest_compounding else "不复利",
            f"M费{_format_percent(result.maker_fee_rate)}",
            f"T费{_format_percent(result.taker_fee_rate)}",
            f"开滑{_format_percent(config.resolved_backtest_entry_slippage_rate())}",
            f"平滑{_format_percent(config.resolved_backtest_exit_slippage_rate())}",
            f"资金费{_format_percent(config.backtest_funding_rate)}",
        ]
    )
    return " / ".join(parts)


def _format_result_start(result: BacktestResult) -> str:
    if not result.candles:
        return "-"
    return _format_timestamp(result.candles[0].ts)


def _format_result_end(result: BacktestResult) -> str:
    if not result.candles:
        return "-"
    return _format_timestamp(result.candles[-1].ts)


def _format_timestamp(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    return str(ts)


def _format_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * 100, 4)}%"


def _sanitize_filename_part(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned or "report"
