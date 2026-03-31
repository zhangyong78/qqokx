from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.backtest import BacktestResult, format_backtest_report
from okx_quant.models import StrategyConfig
from okx_quant.persistence import backtest_report_export_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DEFINITIONS


STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in STRATEGY_DEFINITIONS}
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
    target.write_text(
        _build_single_backtest_report_text(result, config, candle_limit, exported_at),
        encoding="utf-8-sig",
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
    file_name = (
        f"batch_{exported_at.strftime('%Y%m%d_%H%M%S')}_"
        f"{_sanitize_filename_part(batch_name)}_"
        f"{_sanitize_filename_part(first_config.strategy_id)}_"
        f"{_sanitize_filename_part(first_config.inst_id)}_"
        f"{_sanitize_filename_part(first_config.bar)}_"
        f"{_sanitize_filename_part(first_config.signal_mode)}.txt"
    )
    target = report_dir / file_name
    target.write_text(
        _build_batch_backtest_report_text(results, candle_limit, batch_name, exported_at),
        encoding="utf-8-sig",
    )
    return target


def _build_single_backtest_report_text(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    exported_at: datetime,
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
) -> str:
    first_config = results[0][0]
    strategy_name = STRATEGY_ID_TO_NAME.get(first_config.strategy_id, first_config.strategy_id)
    sorted_results = sorted(
        results,
        key=lambda item: (item[0].atr_stop_multiplier, item[0].atr_take_multiplier),
    )
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
        f"参数范围：SL = 1/1.5/2 ATR；TP = SL x1/x2/x3；手续费 M/T = "
        f"{_format_percent(sorted_results[0][1].maker_fee_rate)} / {_format_percent(sorted_results[0][1].taker_fee_rate)}",
    ]
    notes = {result.data_source_note for _, result in sorted_results if result.data_source_note}
    if len(notes) == 1:
        lines.append(f"数据来源：{next(iter(notes))}")
    lines.extend(
        [
            "",
            "矩阵对比",
            "-" * 72,
            _build_matrix_lines(sorted_results),
            "",
            "分项报告",
            "-" * 72,
        ]
    )
    for index, (config, result) in enumerate(sorted_results, start=1):
        lines.extend(
            [
                f"[{index}] SL x{format_decimal(config.atr_stop_multiplier)} | "
                f"TP x{format_decimal(config.atr_take_multiplier)}",
                format_backtest_report(result),
                "",
            ]
        )
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
            if matched is None:
                cells.append("-")
                continue
            cells.append(
                f"{format_decimal_fixed(matched.report.total_pnl, 4)} | "
                f"{format_decimal_fixed(matched.report.win_rate, 2)}% | "
                f"{matched.report.total_trades}笔"
            )
        rows.append(" | ".join(cells))
    return "\n".join(rows)


def _build_param_summary(config: StrategyConfig, result: BacktestResult) -> str:
    if config.backtest_sizing_mode == "risk_percent":
        sizing_text = f"风险百分比{format_decimal(config.backtest_risk_percent or Decimal('0'))}%"
    elif config.backtest_sizing_mode == "fixed_size":
        sizing_text = f"固定数量{format_decimal(config.order_size)}"
    else:
        sizing_text = f"固定风险金{format_decimal(config.risk_amount or Decimal('0'))}"
    return (
        f"EMA{config.ema_period} / 趋势EMA{config.trend_ema_period} / ATR{config.atr_period} / "
        f"SLx{format_decimal(config.atr_stop_multiplier)} / TPx{format_decimal(config.atr_take_multiplier)} / "
        f"方向{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)} / 仓位{sizing_text} / "
        f"本金{format_decimal_fixed(config.backtest_initial_capital, 2)} / "
        f"{'复利' if config.backtest_compounding else '不复利'} / "
        f"M费{_format_percent(result.maker_fee_rate)} / T费{_format_percent(result.taker_fee_rate)} / "
        f"滑点{_format_percent(config.backtest_slippage_rate)} / 资金费{_format_percent(config.backtest_funding_rate)}"
    )


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


def _format_percent(rate) -> str:
    return f"{format_decimal_fixed(rate * 100, 4)}%"


def _sanitize_filename_part(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned or "report"
