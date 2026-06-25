from __future__ import annotations

import csv
import html
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.backtest import (
    BACKTEST_RESERVED_CANDLES,
    BacktestResult,
    format_backtest_report,
    format_trade_exit_reason,
    summarize_trade_exit_reasons,
)
from okx_quant.backtest_audit import (
    batch_backtest_artifact_paths,
    export_batch_backtest_manifest,
    export_single_backtest_audit_files,
    single_backtest_artifact_paths,
)
from okx_quant.backtest_strategy_pool import is_strategy_pool_config, strategy_pool_profile_name
from okx_quant.minimum_risk_recommendations import format_risk_recommendation, recommended_minimum_risk_amount_for_config
from okx_quant.models import StrategyConfig, describe_dynamic_protection_rules, moving_average_display_label
from okx_quant.persistence import backtest_report_export_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import (
    BACKTEST_STRATEGY_DEFINITIONS,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
    STRATEGY_EMA55_SLOPE_SHORT_ID,
    is_dynamic_strategy_id,
)
from okx_quant.strategy_parameters import strategy_uses_parameter
from okx_quant.strategy_runtime_registry import get_strategy_runtime_profile


STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in BACKTEST_STRATEGY_DEFINITIONS}
BTC_EMA15_MA50_PULLBACK_STRATEGY_IDS = {
    STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
    STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID,
}
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
FEE_TYPE_TO_LABEL = {
    "maker": "Maker",
    "taker": "Taker",
    "none": "-",
}


def _format_candle_limit_text(candle_limit: int) -> str:
    return "全量" if candle_limit <= 0 else str(candle_limit)


def _config_fast_label(config: StrategyConfig) -> str:
    return moving_average_display_label(config.resolved_ema_type(), config.ema_period)


def _config_trend_label(config: StrategyConfig) -> str:
    return moving_average_display_label(config.resolved_trend_ema_type(), config.trend_ema_period)


def _config_reference_label(config: StrategyConfig) -> str:
    return moving_average_display_label(
        config.resolved_entry_reference_ema_type(),
        config.resolved_entry_reference_ema_period(),
    )


def _uses_dynamic_break_even_trigger_r(config: StrategyConfig) -> bool:
    return config.take_profit_mode == "dynamic" and strategy_uses_parameter(
        config.strategy_id,
        "dynamic_break_even_trigger_r",
    )


def _dynamic_protection_summary_parts(config: StrategyConfig) -> tuple[str, ...]:
    rules = config.resolved_dynamic_protection_rules()
    parts: list[str] = []
    if rules:
        for rule in rules:
            trigger_r = rule.resolved_trigger_r()
            if rule.resolved_action() == "break_even":
                parts.append(f"{trigger_r}R保本")
                continue
            lock_r = rule.resolved_lock_r()
            if rule.trailing_enabled():
                parts.append(f"{trigger_r}R锁{lock_r}R后每{rule.resolved_trail_every_r()}R移{rule.resolved_trail_add_r()}R")
            else:
                parts.append(f"{trigger_r}R锁{lock_r}R")
    elif _uses_dynamic_break_even_trigger_r(config):
        first_lock_r = max(int(config.dynamic_first_lock_r), 0)
        trailing_start_r = max(int(config.ema55_slope_lock_profit_trigger_r), 2)
        trailing_step_r = max(int(config.dynamic_trailing_step_r), 1)
        effective_first_lock_r = first_lock_r if first_lock_r > 0 else max(trailing_start_r - trailing_step_r, 0)
        parts.extend(
            (
                f"{max(int(config.dynamic_break_even_trigger_r), 1)}R保本",
                f"{trailing_start_r}R锁{effective_first_lock_r}R后每{trailing_step_r}R移{trailing_step_r}R",
            )
        )
    else:
        parts.append(f"2R保本{config.dynamic_two_r_break_even_label()}")
    if bool(config.trend_ema_close_exit_after_trigger_r_enabled):
        parts.append(f"{config.resolved_trend_ema_close_exit_after_trigger_r()}R破趋势EMA平仓")
    return tuple(parts)


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
            report_path=target,
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
    if config.strategy_id in BTC_EMA15_MA50_PULLBACK_STRATEGY_IDS:
        export_btc_ema15_ma50_pullback_research_bundle(
            [(config, result)],
            exported_at=exported_at,
            base_dir=base_dir,
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
                report_path=detail_path,
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
    if first_config.strategy_id in BTC_EMA15_MA50_PULLBACK_STRATEGY_IDS:
        export_btc_ema15_ma50_pullback_research_bundle(
            sorted_results,
            exported_at=exported_at,
            base_dir=base_dir,
        )
    return target


def _build_single_backtest_report_text(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    exported_at: datetime,
    *,
    report_path: Path | None = None,
    artifact_paths: dict[str, Path] | None = None,
) -> str:
    lines = [
        "交易员速览",
        "=" * 72,
        *build_backtest_focus_lines(
            result,
            config,
            candle_limit,
            exported_at=exported_at,
            report_path=report_path,
            artifact_paths=artifact_paths,
        ),
    ]
    lines.extend(
        [
            "",
            "完整明细",
            "-" * 72,
            format_backtest_report(result),
            "",
            "交易明细",
            "-" * 72,
            _build_trade_lines(result),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def build_backtest_focus_lines(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    *,
    exported_at: datetime | None = None,
    snapshot_id: str | None = None,
    report_path: Path | None = None,
    artifact_paths: dict[str, Path] | None = None,
) -> list[str]:
    report = result.report
    strategy_name = STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id)
    start_text = _format_result_start(result)
    end_text = _format_result_end(result)
    warmup_count = min(BACKTEST_RESERVED_CANDLES, len(result.candles))
    fee_to_net_pct = None if report.total_pnl == 0 else (report.total_fees / abs(report.total_pnl)) * Decimal("100")
    recommendation = recommended_minimum_risk_amount_for_config(config)
    exit_reason_summary = _build_exit_reason_summary_text(result)
    if report_path is not None and artifact_paths is None:
        artifact_paths = single_backtest_artifact_paths(report_path)
    identity_parts = []
    if snapshot_id:
        identity_parts.append(f"编号：{snapshot_id}")
    if exported_at is not None:
        identity_parts.append(f"时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}")
    identity_parts.extend(
        [
            f"策略：{strategy_name}",
            f"交易对：{config.inst_id}",
            f"K线：{BAR_VALUE_TO_LABEL.get(config.bar, config.bar)}",
            f"方向：{SIGNAL_VALUE_TO_LABEL.get(config.signal_mode, config.signal_mode)}",
        ]
    )
    lines = [
        " | ".join(identity_parts),
        (
            f"区间：{start_text} -> {end_text} | 样本：{len(result.candles):,}根 | "
            f"设置：{_format_candle_limit_text(candle_limit)} | 预热：前 {warmup_count} 根 | "
            f"交易：{report.total_trades}笔 | 胜率：{format_decimal_fixed(report.win_rate, 2)}%"
        ),
        (
            f"结果：总盈亏 {format_decimal_fixed(report.total_pnl, 4)} | "
            f"收益率 {format_decimal_fixed(report.total_return_pct, 2)}% | "
            f"最大回撤 {format_decimal_fixed(report.max_drawdown, 4)} "
            f"({format_decimal_fixed(report.max_drawdown_pct, 2)}%) | "
            f"PF {_format_optional_ratio(report.profit_factor)} | "
            f"盈亏比 {_format_optional_ratio(report.profit_loss_ratio)}"
        ),
        f"参数摘要：{_build_param_summary(config, result)}",
        (
            f"费用：Maker {_format_percent(result.maker_fee_rate)} | "
            f"Taker {_format_percent(result.taker_fee_rate)} | "
            f"开滑{_format_percent(result.entry_slippage_rate)} | "
            f"平滑{_format_percent(result.exit_slippage_rate)} | "
            f"手续费合计 {format_decimal_fixed(report.total_fees, 4)} | "
            f"手续费占净盈亏 {_format_optional_pct(fee_to_net_pct)}"
        ),
    ]
    if exit_reason_summary:
        lines.append(f"平仓统计：{exit_reason_summary}")
    if recommendation is not None:
        lines.append(f"历史推荐：{config.inst_id} {format_risk_recommendation(recommendation)}。")
    if result.data_source_note:
        lines.append(f"数据来源：{result.data_source_note}")
    if report_path is not None:
        lines.append(f"报告文件：{report_path}")
    if artifact_paths is not None:
        lines.extend(
            [
                f"资本审计：{artifact_paths['capital']}",
                f"操作日志：{artifact_paths['operations']}",
                f"审计清单：{artifact_paths['manifest']}",
            ]
        )
    return lines


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
        f"回测K线数：{_format_candle_limit_text(candle_limit)}",
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
        reason = format_trade_exit_reason(trade.exit_reason)
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
    if get_strategy_runtime_profile(config.strategy_id).family == "ema55_slope_short":
        return "atr_period_matrix"
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
    if batch_mode == "atr_period_matrix":
        return (config.atr_stop_multiplier, config.atr_period)
    return (config.atr_stop_multiplier, config.atr_take_multiplier)


def _format_max_entries_label(value: int) -> str:
    return "不限(0)" if value <= 0 else f"{value}次"


def _build_batch_result_title(index: int, config: StrategyConfig, batch_mode: str) -> str:
    if batch_mode == "strategy_pool":
        return (
            f"[{index}] {strategy_pool_profile_name(config)} | "
            f"{_config_fast_label(config)}/{_config_trend_label(config)} | "
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
    if batch_mode == "atr_period_matrix":
        return f"[{index}] ATR{config.atr_period} | SL x{format_decimal(config.atr_stop_multiplier)}"
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
    elif batch_mode == "atr_period_matrix":
        suffix = f"atr_{config.atr_period}_sl_x{format_decimal(config.atr_stop_multiplier)}"
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
            "参数范围：5m 候选策略池；"
            f"候选数 = {len(results)}；"
            f"最大槽位 = {first_config.max_entries_per_trend}；"
            f"单槽位数量 = {format_decimal(first_config.order_size)}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "dynamic_entries":
        config = results[0][0]
        break_even_text = "；".join(_dynamic_protection_summary_parts(config)) + "；"
        return (
            "参数范围：动态止盈；"
            f"挂单参考线 = {_config_reference_label(results[0][0])}；"
            "SL = 1/1.5/2 ATR；"
            "每波最多开仓次数 = 0/1/2/3；"
            f"{break_even_text}"
            f"手续费偏移 = {results[0][0].dynamic_fee_offset_enabled_label()}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "fixed_entries":
        return (
            f"参数范围：挂单参考线 = {_config_reference_label(results[0][0])}；"
            "每波最多开仓次数 = 0/1/2/3；"
            "SL = 1/1.5/2 ATR；"
            "TP = SL x1/x2/x3；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    return f"参数范围：SL = 1/1.5/2 ATR；TP = SL x1/x2/x3；手续费 M/T = {maker_fee} / {taker_fee}"


    if batch_mode == "atr_period_matrix":
        return (
            "鍙傛暟鑼冨洿锛欰TR 鍛ㄦ湡 = 10 / 14锛?"
            "SL = 1/1.5/2 ATR锛?"
            "ATR 止盈鍙綔淇濇姢浠峰悎娉曞崰浣嶏紝绛栫暐浠呯敤 ATR 姝㈡崯 + 鏂滅巼骞冲潶绂诲満锛?"
            f"鎵嬬画璐?M/T = {maker_fee} / {taker_fee}"
        )
    return f"鍙傛暟鑼冨洿锛歋L = 1/1.5/2 ATR锛汿P = SL x1/x2/x3锛涙墜缁垂 M/T = {maker_fee} / {taker_fee}"


def _build_batch_scope_line(
    results: list[tuple[StrategyConfig, BacktestResult]],
    batch_mode: str,
) -> str:
    maker_fee = _format_percent(results[0][1].maker_fee_rate)
    taker_fee = _format_percent(results[0][1].taker_fee_rate)
    if batch_mode == "strategy_pool":
        first_config = results[0][0]
        return (
            "参数范围：5m 候选策略池；"
            f"候选数 = {len(results)}；"
            f"最大加仓 = {first_config.max_entries_per_trend}；"
            f"单次下单量 = {format_decimal(first_config.order_size)}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "dynamic_entries":
        return (
            "参数范围：动态止盈；"
            f"挂单参考线 = {_config_reference_label(results[0][0])}；"
            "SL = 1/1.5/2 ATR；"
            "每波最大开仓次数 = 0/1/2/3；"
            f"2R 保本 = {results[0][0].dynamic_two_r_break_even_label()}；"
            f"手续费偏移 = {results[0][0].dynamic_fee_offset_enabled_label()}；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "fixed_entries":
        return (
            f"参数范围：挂单参考线 = {_config_reference_label(results[0][0])}；"
            "每波最大开仓次数 = 0/1/2/3；"
            "SL = 1/1.5/2 ATR；"
            "TP = SL x1/x2/x3；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    if batch_mode == "atr_period_matrix":
        return (
            "参数范围：ATR 周期 = 10 / 14；"
            "SL = 1/1.5/2 ATR；"
            "ATR 止盈仅作保护价格占位，策略实际只使用 ATR 止损 + EMA55 斜率走平离场；"
            f"手续费 M/T = {maker_fee} / {taker_fee}"
        )
    return f"参数范围：SL = 1/1.5/2 ATR；TP = SL x1/x2/x3；手续费 M/T = {maker_fee} / {taker_fee}"


def _build_batch_matrix_lines(results: list[tuple[StrategyConfig, BacktestResult]], batch_mode: str) -> str:
    if batch_mode == "strategy_pool":
        lines = ["候选策略 | 参数 | 总盈亏 | 胜率 | 交易数 | PF | 平均R"]
        for config, result in results:
            lines.append(
                " | ".join(
                    [
                        strategy_pool_profile_name(config),
                        (
                            f"{_config_fast_label(config)}/{_config_trend_label(config)} "
                            f"ATR{config.atr_period} "
                            f"SLx{format_decimal(config.atr_stop_multiplier)} "
                            f"TPx{format_decimal(config.atr_take_multiplier)}"
                        ),
                        format_decimal_fixed(result.report.total_pnl, 4),
                        f"{format_decimal_fixed(result.report.win_rate, 2)}%",
                        f"{result.report.total_trades}笔",
                        format_decimal_fixed(result.report.profit_factor or Decimal("0"), 2),
                        format_decimal_fixed(result.report.average_r_multiple, 2),
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
    if batch_mode == "atr_period_matrix":
        atr_periods = sorted({config.atr_period for config, _ in results})
        header = ["SL \\\\ ATR", *[f"ATR{period}" for period in atr_periods]]
        rows = [" | ".join(header)]
        stop_values = sorted({config.atr_stop_multiplier for config, _ in results})
        for stop_value in stop_values:
            cells = [f"SL x{format_decimal(stop_value)}"]
            for atr_period in atr_periods:
                matched = next(
                    (
                        result
                        for config, result in results
                        if config.atr_stop_multiplier == stop_value and config.atr_period == atr_period
                    ),
                    None,
                )
                cells.append("-" if matched is None else _build_matrix_cell_text(matched))
            rows.append(" | ".join(cells))
        return "\n".join(rows)

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


def _build_exit_reason_summary_text(result: BacktestResult) -> str:
    ordered = summarize_trade_exit_reasons(result.trades)
    if not ordered:
        return ""
    return " | ".join(f"{label} {count}" for label, count in ordered)


def _build_param_summary(config: StrategyConfig, result: BacktestResult) -> str:
    parts = [
        f"EMA{config.ema_period}",
        f"趋势线{_config_trend_label(config)}",
        f"ATR{config.atr_period}",
        f"SL x{format_decimal(config.atr_stop_multiplier)}",
    ]
    if config.backtest_profile_name:
        parts.insert(0, f"候选{config.backtest_profile_name}")
    if is_dynamic_strategy_id(config.strategy_id):
        parts.insert(2, f"挂单参考线{_config_reference_label(config)}")
        if config.take_profit_mode == "dynamic":
            parts.append("动态止盈")
            parts.extend(_dynamic_protection_summary_parts(config))
            if config.time_stop_break_even_enabled and config.resolved_time_stop_break_even_bars() > 0:
                parts.append(f"时间保本{config.resolved_time_stop_break_even_bars()}根")
        else:
            parts.append(f"TP x{format_decimal(config.atr_take_multiplier)}")
        parts.append(f"每波最多开仓次数{_format_max_entries_label(config.max_entries_per_trend)}")
    if config.strategy_id == STRATEGY_EMA55_SLOPE_SHORT_ID:
        if config.take_profit_mode == "dynamic":
            parts.append("动态止盈")
            parts.append(f"动态止盈首档{config.resolved_dynamic_trailing_start_r()}R")
            parts.extend(_dynamic_protection_summary_parts(config))
            if config.time_stop_break_even_enabled and config.resolved_time_stop_break_even_bars() > 0:
                parts.append(f"时间保本{config.resolved_time_stop_break_even_bars()}根")
        else:
            parts.append(f"TP x{format_decimal(config.atr_take_multiplier)}")
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


def _format_optional_ratio(value: Decimal | None) -> str:
    if value is None:
        return "无亏损交易"
    return format_decimal_fixed(value, 4)


def _format_optional_pct(value: Decimal | None) -> str:
    if value is None:
        return "无"
    return f"{format_decimal_fixed(value, 2)}%"


def _sanitize_filename_part(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("._")
    return cleaned or "report"


def btc_ema15_ma50_pullback_report_dir(
    base_dir: Path | None = None,
    *,
    strategy_id: str = STRATEGY_BTC_EMA15_MA50_PULLBACK_LONG_ID,
) -> Path:
    root = Path(base_dir) if base_dir is not None else Path("reports")
    return root / _btc_ema15_ma50_pullback_study_slug(strategy_id) / "latest"


def _btc_ema15_ma50_pullback_study_slug(strategy_id: str) -> str:
    if strategy_id == STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID:
        return "btc_ema15_ma50_short"
    return "btc_ema15_ma50_long"


def _btc_ema15_ma50_pullback_direction(strategy_id: str) -> str:
    return "short" if strategy_id == STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID else "long"


def _btc_ema15_ma50_pullback_title(strategy_id: str) -> str:
    return "BTC EMA15/MA50 回踩做空" if strategy_id == STRATEGY_BTC_EMA15_MA50_PULLBACK_SHORT_ID else "BTC EMA15/MA50 回踩做多"


def _btc_ema15_ma50_pullback_report_dir_for_strategy(strategy_id: str, base_dir: Path | None = None) -> Path:
    root = Path(base_dir) if base_dir is not None else Path("reports")
    return root / _btc_ema15_ma50_pullback_study_slug(strategy_id) / "latest"


def export_btc_ema15_ma50_pullback_research_bundle(
    results: list[tuple[StrategyConfig, BacktestResult]],
    *,
    exported_at: datetime | None = None,
    base_dir: Path | None = None,
) -> Path:
    if not results:
        raise ValueError("BTC EMA15/MA50 回踩研究结果为空，无法导出。")
    exported_at = exported_at or datetime.now()
    strategy_id = results[0][0].strategy_id
    output_dir = _btc_ema15_ma50_pullback_report_dir_for_strategy(strategy_id, base_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ordered = sorted(results, key=lambda item: _btc_ema15_ma50_rank_sort_key(item[0], item[1]), reverse=True)
    best_config, best_result = ordered[0]
    comparison_rows = [
        _btc_ema15_ma50_comparison_row(index, config, result)
        for index, (config, result) in enumerate(ordered, start=1)
    ]
    summary_row = _btc_ema15_ma50_summary_row(best_config, best_result, exported_at=exported_at)
    trades_rows = _btc_ema15_ma50_trade_rows(best_config, best_result)

    _write_csv_rows(output_dir / "summary.csv", list(summary_row.keys()), [summary_row])
    _write_csv_rows(output_dir / "strategy_comparison.csv", list(comparison_rows[0].keys()), comparison_rows)
    _write_csv_rows(output_dir / "trades.csv", list(trades_rows[0].keys()) if trades_rows else _btc_ema15_trade_csv_headers(), trades_rows)
    _write_csv_rows(
        output_dir / "equity_curve.csv",
        ["ts", "time", "equity", "net_value", "drawdown", "drawdown_pct"],
        _btc_ema15_ma50_equity_rows(best_result),
    )
    _write_csv_rows(
        output_dir / "monthly_returns.csv",
        ["period", "trades", "win_rate", "total_pnl", "return_pct", "start_equity", "end_equity", "max_drawdown", "max_drawdown_pct"],
        _btc_ema15_ma50_period_rows(best_result.monthly_stats),
    )
    _write_csv_rows(
        output_dir / "yearly_returns.csv",
        ["period", "trades", "win_rate", "total_pnl", "return_pct", "start_equity", "end_equity", "max_drawdown", "max_drawdown_pct"],
        _btc_ema15_ma50_period_rows(best_result.yearly_stats),
    )
    trade_chart_dir = output_dir / "trade_charts"
    trade_chart_dir.mkdir(parents=True, exist_ok=True)
    trade_chart_links = _btc_ema15_ma50_write_trade_charts(trade_chart_dir, best_config, best_result)
    report_html = _btc_ema15_ma50_build_report_html(
        ordered,
        best_config,
        best_result,
        comparison_rows,
        trades_rows,
        trade_chart_links,
        exported_at=exported_at,
    )
    (output_dir / "report.html").write_text(report_html, encoding="utf-8")
    return output_dir


def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _csv_value(value: object) -> object:
    if isinstance(value, Decimal):
        return format_decimal(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return value


def _btc_ema15_ma50_rank_sort_key(config: StrategyConfig, result: BacktestResult) -> tuple[Decimal, Decimal, Decimal]:
    return (
        result.report.total_pnl,
        sum((trade.r_multiple for trade in result.trades), Decimal("0")),
        -result.report.max_drawdown,
    )


def _btc_ema15_ma50_summary_row(
    config: StrategyConfig,
    result: BacktestResult,
    *,
    exported_at: datetime,
) -> dict[str, object]:
    return {
        "exported_at": exported_at.strftime("%Y-%m-%d %H:%M:%S"),
        "strategy_id": config.strategy_id,
        "strategy_name": STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id),
        "symbol": config.inst_id,
        "timeframe": config.bar,
        "atr_period": config.atr_period,
        "atr_stop_multiplier": config.atr_stop_multiplier,
        "cross_window_bars": config.cross_window_bars,
        "max_pullback_index": config.max_pullback_index,
        "daily_filter": _btc_ema15_ma50_daily_filter_text(config),
        "exit_mode": config.exit_mode,
        "rr": config.rr,
        "break_even_trigger_r": config.dynamic_break_even_trigger_r,
        "trailing_start_r": config.ema55_slope_lock_profit_trigger_r,
        "first_lock_r": config.dynamic_first_lock_r,
        "trailing_step_r": config.dynamic_trailing_step_r,
        "trades": result.report.total_trades,
        "win_rate": result.report.win_rate,
        "profit_factor": result.report.profit_factor,
        "total_r": sum((trade.r_multiple for trade in result.trades), Decimal("0")),
        "net_profit": result.report.total_pnl,
        "max_drawdown": result.report.max_drawdown,
        "max_drawdown_pct": result.report.max_drawdown_pct,
        "avg_r": result.report.average_r_multiple,
        "median_r": _btc_ema15_ma50_median_r(result),
        "max_consecutive_losses": _btc_ema15_ma50_max_consecutive_losses(result),
    }


def _btc_ema15_ma50_comparison_row(rank: int, config: StrategyConfig, result: BacktestResult) -> dict[str, object]:
    return {
        "rank": rank,
        "symbol": config.inst_id,
        "timeframe": config.bar,
        "atr_period": config.atr_period,
        "atr_stop_multiplier": config.atr_stop_multiplier,
        "cross_window_bars": config.cross_window_bars,
        "max_pullback_index": config.max_pullback_index,
        "daily_filter": _btc_ema15_ma50_daily_filter_text(config),
        "exit_mode": config.exit_mode,
        "rr": config.rr,
        "break_even_trigger_r": config.dynamic_break_even_trigger_r,
        "trailing_start_r": config.ema55_slope_lock_profit_trigger_r,
        "first_lock_r": config.dynamic_first_lock_r,
        "trailing_step_r": config.dynamic_trailing_step_r,
        "trades": result.report.total_trades,
        "win_rate": result.report.win_rate,
        "profit_factor": result.report.profit_factor,
        "total_r": sum((trade.r_multiple for trade in result.trades), Decimal("0")),
        "net_profit": result.report.total_pnl,
        "max_drawdown": result.report.max_drawdown,
        "max_drawdown_pct": result.report.max_drawdown_pct,
        "avg_r": result.report.average_r_multiple,
        "median_r": _btc_ema15_ma50_median_r(result),
        "max_consecutive_losses": _btc_ema15_ma50_max_consecutive_losses(result),
        "maker_fee_rate": result.maker_fee_rate,
        "taker_fee_rate": result.taker_fee_rate,
        "entry_slippage_rate": result.entry_slippage_rate,
        "exit_slippage_rate": result.exit_slippage_rate,
    }


def _btc_ema15_trade_csv_headers() -> list[str]:
    return [
        "trade_id",
        "symbol",
        "timeframe",
        "direction",
        "cross_time",
        "entry_time",
        "exit_time",
        "entry_price",
        "exit_price",
        "ema15_at_entry",
        "ma50_at_entry",
        "atr_at_entry",
        "stop_price",
        "qty",
        "gross_pnl",
        "fee",
        "slippage",
        "net_pnl",
        "r_multiple",
        "exit_reason",
        "bars_after_cross",
        "pullback_index",
        "pullback_depth_pct",
        "ema15_slope_5",
        "ema15_slope_10",
        "ma50_slope_10",
        "daily_filter_pass",
        "max_r_before_exit",
        "max_drawdown_r",
        "holding_bars",
    ]


def _btc_ema15_ma50_trade_rows(config: StrategyConfig, result: BacktestResult) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    direction = _btc_ema15_ma50_pullback_direction(config.strategy_id)
    for index, trade in enumerate(result.trades, start=1):
        metadata = trade.metadata or {}
        rows.append(
            {
                "trade_id": f"T{index:04d}",
                "symbol": config.inst_id,
                "timeframe": config.bar,
                "direction": direction,
                "cross_time": _format_timestamp(int(metadata.get("cross_ts", trade.entry_ts))),
                "entry_time": _format_timestamp(trade.entry_ts),
                "exit_time": _format_timestamp(trade.exit_ts),
                "entry_price": trade.entry_price,
                "exit_price": trade.exit_price,
                "ema15_at_entry": metadata.get("ema15_at_entry", ""),
                "ma50_at_entry": metadata.get("ma50_at_entry", ""),
                "atr_at_entry": metadata.get("atr_at_entry", trade.atr_value),
                "stop_price": metadata.get("stop_price", trade.stop_loss),
                "qty": trade.size,
                "gross_pnl": trade.gross_pnl,
                "fee": trade.total_fee,
                "slippage": trade.slippage_cost,
                "net_pnl": trade.pnl,
                "r_multiple": trade.r_multiple,
                "exit_reason": format_trade_exit_reason(trade.exit_reason),
                "bars_after_cross": metadata.get("bars_after_cross", ""),
                "pullback_index": metadata.get("pullback_index", ""),
                "pullback_depth_pct": metadata.get("pullback_depth_pct", ""),
                "ema15_slope_5": metadata.get("ema15_slope_5", ""),
                "ema15_slope_10": metadata.get("ema15_slope_10", ""),
                "ma50_slope_10": metadata.get("ma50_slope_10", ""),
                "daily_filter_pass": bool(metadata.get("daily_filter_pass", False)),
                "max_r_before_exit": metadata.get("max_r_before_exit", ""),
                "max_drawdown_r": metadata.get("max_drawdown_r", ""),
                "holding_bars": max(trade.exit_index - trade.entry_index, 0),
            }
        )
    return rows


def _btc_ema15_ma50_equity_rows(result: BacktestResult) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, candle in enumerate(result.candles):
        rows.append(
            {
                "ts": candle.ts,
                "time": _format_timestamp(candle.ts),
                "equity": result.equity_curve[index] if index < len(result.equity_curve) else Decimal("0"),
                "net_value": result.net_value_curve[index] if index < len(result.net_value_curve) else Decimal("0"),
                "drawdown": result.drawdown_curve[index] if index < len(result.drawdown_curve) else Decimal("0"),
                "drawdown_pct": result.drawdown_pct_curve[index] if index < len(result.drawdown_pct_curve) else Decimal("0"),
            }
        )
    return rows


def _btc_ema15_ma50_period_rows(stats: list[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in stats:
        rows.append(
            {
                "period": item.period_label,
                "trades": item.trades,
                "win_rate": item.win_rate,
                "total_pnl": item.total_pnl,
                "return_pct": item.return_pct,
                "start_equity": item.start_equity,
                "end_equity": item.end_equity,
                "max_drawdown": item.max_drawdown,
                "max_drawdown_pct": item.max_drawdown_pct,
            }
        )
    return rows


def _btc_ema15_ma50_median_r(result: BacktestResult) -> Decimal:
    values = sorted(trade.r_multiple for trade in result.trades)
    if not values:
        return Decimal("0")
    middle = len(values) // 2
    if len(values) % 2 == 1:
        return values[middle]
    return (values[middle - 1] + values[middle]) / Decimal("2")


def _btc_ema15_ma50_max_consecutive_losses(result: BacktestResult) -> int:
    streak = 0
    best = 0
    for trade in result.trades:
        if trade.pnl < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _btc_ema15_ma50_daily_filter_text(config: StrategyConfig) -> str:
    if not config.uses_daily_filter():
        return "disabled"
    return f"{config.daily_filter_mode}/{config.daily_filter_scope}/{config.daily_filter_period}"


def _btc_ema15_ma50_write_trade_charts(
    output_dir: Path,
    config: StrategyConfig,
    result: BacktestResult,
) -> dict[str, str]:
    links: dict[str, str] = {}
    for index, trade in enumerate(result.trades, start=1):
        trade_id = f"T{index:04d}"
        file_name = f"{trade_id}.html"
        (output_dir / file_name).write_text(
            _btc_ema15_ma50_build_trade_chart_html(config, result, trade, trade_id),
            encoding="utf-8",
        )
        links[trade_id] = f"trade_charts/{file_name}"
    return links


def _btc_ema15_ma50_build_trade_chart_html(
    config: StrategyConfig,
    result: BacktestResult,
    trade,
    trade_id: str,
) -> str:
    metadata = trade.metadata or {}
    strategy_title = _btc_ema15_ma50_pullback_title(config.strategy_id)
    direction = _btc_ema15_ma50_pullback_direction(config.strategy_id)
    cross_label = "CrossDown" if direction == "short" else "CrossUp"
    cross_index = int(metadata.get("cross_index", max(trade.entry_index - 1, 0)))
    start_index = max(min(cross_index, trade.entry_index) - 12, 0)
    end_index = min(trade.exit_index + 12, len(result.candles) - 1)
    window = result.candles[start_index : end_index + 1]
    x_values = [_format_timestamp(item.ts) for item in window]
    candle_payload = {
        "x": x_values,
        "open": [float(item.open) for item in window],
        "high": [float(item.high) for item in window],
        "low": [float(item.low) for item in window],
        "close": [float(item.close) for item in window],
    }
    ema_trace = [
        None if value is None else float(value)
        for value in result.ema_values[start_index : end_index + 1]
    ]
    ma_trace = [
        None if value is None else float(value)
        for value in result.trend_ema_values[start_index : end_index + 1]
    ]
    stop_history = []
    for item in metadata.get("stop_history", []):
        ts = int(item.get("ts", trade.entry_ts))
        if result.candles[start_index].ts <= ts <= result.candles[end_index].ts:
            stop_history.append(
                {
                    "x": _format_timestamp(ts),
                    "y": float(Decimal(str(item.get("price", trade.stop_loss)))),
                }
            )
    if not stop_history:
        stop_history = [
            {"x": _format_timestamp(trade.entry_ts), "y": float(trade.stop_loss)},
            {"x": _format_timestamp(trade.exit_ts), "y": float(trade.stop_loss)},
        ]
    chart_payload = {
        "candles": candle_payload,
        "ema": {"x": x_values, "y": ema_trace},
        "ma": {"x": x_values, "y": ma_trace},
        "cross": {
            "x": [_format_timestamp(int(metadata.get("cross_ts", trade.entry_ts)))],
            "y": [float(metadata.get("ema15_at_entry", trade.entry_price))],
        },
        "entry": {"x": [_format_timestamp(trade.entry_ts)], "y": [float(trade.entry_price)]},
        "exit": {"x": [_format_timestamp(trade.exit_ts)], "y": [float(trade.exit_price)]},
        "initial_stop": {
            "x": [_format_timestamp(trade.entry_ts), _format_timestamp(trade.exit_ts)],
            "y": [float(trade.stop_loss), float(trade.stop_loss)],
        },
        "stop_history": {
            "x": [item["x"] for item in stop_history],
            "y": [item["y"] for item in stop_history],
        },
        "max_favorable": {
            "x": [_format_timestamp(int(metadata.get("max_favorable_ts", trade.exit_ts)))],
            "y": [float(Decimal(str(metadata.get("max_favorable_price", trade.exit_price))))],
        },
        "max_adverse": {
            "x": [_format_timestamp(int(metadata.get("max_adverse_ts", trade.entry_ts)))],
            "y": [float(Decimal(str(metadata.get("max_adverse_price", trade.entry_price))))],
        },
    }
    summary = (
        f"交易ID：{trade_id} | {cross_label}：{_format_timestamp(int(metadata.get('cross_ts', trade.entry_ts)))} | "
        f"入场：{_format_timestamp(trade.entry_ts)} @ {format_decimal_fixed(trade.entry_price, 4)} | "
        f"离场：{_format_timestamp(trade.exit_ts)} @ {format_decimal_fixed(trade.exit_price, 4)} | "
        f"原因：{format_trade_exit_reason(trade.exit_reason)} | R={format_decimal_fixed(trade.r_multiple, 4)}"
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(trade_id)} 交易图</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: "Microsoft YaHei", sans-serif; margin: 24px; background: #f7f5ef; color: #1f2933; }}
    h1 {{ margin-bottom: 8px; }}
    .summary {{ margin-bottom: 16px; }}
    #chart {{ width: 100%; height: 760px; }}
  </style>
</head>
<body>
  <h1>{html.escape(strategy_title)} | {html.escape(trade_id)}</h1>
  <div class="summary">{html.escape(summary)}</div>
  <div id="chart"></div>
  <script>
    const payload = {json.dumps(chart_payload, ensure_ascii=False)};
    const traces = [
      {{
        x: payload.candles.x,
        open: payload.candles.open,
        high: payload.candles.high,
        low: payload.candles.low,
        close: payload.candles.close,
        type: "candlestick",
        name: "4H K线"
      }},
      {{ x: payload.ema.x, y: payload.ema.y, type: "scatter", mode: "lines", name: "EMA15", line: {{ color: "#e76f51", width: 2 }} }},
      {{ x: payload.ma.x, y: payload.ma.y, type: "scatter", mode: "lines", name: "MA50", line: {{ color: "#264653", width: 2 }} }},
      {{ x: payload.cross.x, y: payload.cross.y, type: "scatter", mode: "markers", name: "{cross_label}", marker: {{ color: "#2a9d8f", size: 12, symbol: "diamond" }} }},
      {{ x: payload.entry.x, y: payload.entry.y, type: "scatter", mode: "markers+text", text: ["Entry"], textposition: "top center", name: "Entry", marker: {{ color: "#1d4ed8", size: 12 }} }},
      {{ x: payload.exit.x, y: payload.exit.y, type: "scatter", mode: "markers+text", text: ["Exit"], textposition: "top center", name: "Exit", marker: {{ color: "#b91c1c", size: 12 }} }},
      {{ x: payload.initial_stop.x, y: payload.initial_stop.y, type: "scatter", mode: "lines", name: "初始止损", line: {{ color: "#ef4444", dash: "dot" }} }},
      {{ x: payload.stop_history.x, y: payload.stop_history.y, type: "scatter", mode: "lines+markers", name: "动态止损轨迹", line: {{ color: "#8b5cf6", width: 2 }} }},
      {{ x: payload.max_favorable.x, y: payload.max_favorable.y, type: "scatter", mode: "markers+text", text: ["最高浮盈"], textposition: "top right", name: "最高浮盈", marker: {{ color: "#15803d", size: 11 }} }},
      {{ x: payload.max_adverse.x, y: payload.max_adverse.y, type: "scatter", mode: "markers+text", text: ["最大浮亏"], textposition: "bottom right", name: "最大浮亏", marker: {{ color: "#dc2626", size: 11 }} }}
    ];
    Plotly.newPlot("chart", traces, {{
      title: "{html.escape(trade_id)} | {html.escape(format_trade_exit_reason(trade.exit_reason))} | R={format_decimal_fixed(trade.r_multiple, 4)}",
      xaxis: {{ rangeslider: {{ visible: false }} }},
      yaxis: {{ title: "价格" }},
      legend: {{ orientation: "h" }},
      margin: {{ l: 60, r: 20, t: 60, b: 40 }},
      paper_bgcolor: "#f7f5ef",
      plot_bgcolor: "#fffdf8"
    }}, {{ responsive: true }});
  </script>
</body>
</html>
"""


def _btc_ema15_ma50_build_report_html(
    ordered: list[tuple[StrategyConfig, BacktestResult]],
    best_config: StrategyConfig,
    best_result: BacktestResult,
    comparison_rows: list[dict[str, object]],
    trades_rows: list[dict[str, object]],
    trade_chart_links: dict[str, str],
    *,
    exported_at: datetime,
) -> str:
    strategy_title = _btc_ema15_ma50_pullback_title(best_config.strategy_id)
    direction = _btc_ema15_ma50_pullback_direction(best_config.strategy_id)
    slug = _btc_ema15_ma50_pullback_study_slug(best_config.strategy_id)
    description = (
        "本研究模块固定品种为 BTC-USDT-SWAP、周期为 4H、方向只做空。先要求 EMA15 从上向下穿越 MA50，再在限定窗口内等待 high 回抽 EMA15 且 close 收回 EMA15 下方，信号在收盘确认后统一于下一根K线开盘成交。止损使用 ATR 倍数，离场可选择固定RR、动态保护，以及 EMA15 收盘重新站回上方后的下一根开盘离场。"
        if direction == "short"
        else "本研究模块固定品种为 BTC-USDT-SWAP、周期为 4H、方向只做多。先要求 EMA15 从下向上穿越 MA50，再在限定窗口内等待 low 回踩 EMA15 且 close 收回 EMA15 上方，信号在收盘确认后统一于下一根K线开盘成交。止损使用 ATR 倍数，离场可选择固定RR、动态保护，以及 EMA15 收盘跌破后的下一根开盘离场。"
    )
    top_rows = comparison_rows[:20]
    best_total_r = sum((trade.r_multiple for trade in best_result.trades), Decimal("0"))
    equity_payload = {
        "x": [_format_timestamp(candle.ts) for candle in best_result.candles],
        "net": [float(value) for value in best_result.net_value_curve],
        "drawdown": [float(value) for value in best_result.drawdown_curve],
    }
    comparison_table = "".join(
        "<tr>"
        f"<td>{row['rank']}</td>"
        f"<td>{html.escape(str(row['atr_period']))}</td>"
        f"<td>{html.escape(str(row['atr_stop_multiplier']))}</td>"
        f"<td>{html.escape(str(row['cross_window_bars']))}</td>"
        f"<td>{html.escape(str(row['max_pullback_index']))}</td>"
        f"<td>{html.escape(str(row['daily_filter']))}</td>"
        f"<td>{html.escape(str(row['exit_mode']))}</td>"
        f"<td>{html.escape(str(row['rr']))}</td>"
        f"<td>{html.escape(str(row['net_profit']))}</td>"
        f"<td>{html.escape(str(row['total_r']))}</td>"
        f"<td>{html.escape(str(row['profit_factor']))}</td>"
        f"<td>{html.escape(str(row['max_drawdown']))}</td>"
        "</tr>"
        for row in top_rows
    )
    trades_table = "".join(
        "<tr>"
        f"<td>{html.escape(str(row['trade_id']))}</td>"
        f"<td>{html.escape(str(row['entry_time']))}</td>"
        f"<td>{html.escape(str(row['exit_time']))}</td>"
        f"<td>{html.escape(str(row['entry_price']))}</td>"
        f"<td>{html.escape(str(row['exit_price']))}</td>"
        f"<td>{html.escape(str(row['net_pnl']))}</td>"
        f"<td>{html.escape(str(row['r_multiple']))}</td>"
        f"<td>{html.escape(str(row['exit_reason']))}</td>"
        f"<td><a href=\"{html.escape(trade_chart_links.get(str(row['trade_id']), ''))}\">查看图表</a></td>"
        "</tr>"
        for row in trades_rows
    )
    monthly_table = "".join(
        "<tr>"
        f"<td>{html.escape(item.period_label)}</td>"
        f"<td>{item.trades}</td>"
        f"<td>{format_decimal_fixed(item.return_pct, 2)}%</td>"
        f"<td>{format_decimal_fixed(item.total_pnl, 4)}</td>"
        "</tr>"
        for item in best_result.monthly_stats
    )
    yearly_table = "".join(
        "<tr>"
        f"<td>{html.escape(item.period_label)}</td>"
        f"<td>{item.trades}</td>"
        f"<td>{format_decimal_fixed(item.return_pct, 2)}%</td>"
        f"<td>{format_decimal_fixed(item.total_pnl, 4)}</td>"
        "</tr>"
        for item in best_result.yearly_stats
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{html.escape(strategy_title)}研究报告</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: "Microsoft YaHei", sans-serif; margin: 24px; background: #f4efe6; color: #1f2933; }}
    h1, h2 {{ color: #8f3f2b; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 24px; }}
    .card {{ background: #fffdf8; border: 1px solid #eadfcd; border-radius: 12px; padding: 14px; }}
    table {{ width: 100%; border-collapse: collapse; margin: 12px 0 24px; background: #fffdf8; }}
    th, td {{ border: 1px solid #eadfcd; padding: 8px 10px; text-align: left; font-size: 14px; }}
    th {{ background: #f7e8d0; }}
    .muted {{ color: #52606d; }}
    #equityChart {{ width: 100%; height: 460px; }}
  </style>
</head>
<body>
  <h1>{html.escape(strategy_title)}研究报告</h1>
  <div class="muted">导出时间：{exported_at.strftime("%Y-%m-%d %H:%M:%S")} | 输出目录：reports/{slug}/latest/</div>
  <h2>策略说明</h2>
  <p>{html.escape(description)}</p>
  <div class="cards">
    <div class="card"><strong>参数排名第1</strong><br>ATR={best_config.atr_period} / stop={format_decimal(best_config.atr_stop_multiplier)} / window={best_config.cross_window_bars}</div>
    <div class="card"><strong>胜率</strong><br>{format_decimal_fixed(best_result.report.win_rate, 2)}%</div>
    <div class="card"><strong>Profit Factor</strong><br>{_format_optional_ratio(best_result.report.profit_factor)}</div>
    <div class="card"><strong>总R</strong><br>{format_decimal_fixed(best_total_r, 4)}</div>
    <div class="card"><strong>净利润</strong><br>{format_decimal_fixed(best_result.report.total_pnl, 4)}</div>
    <div class="card"><strong>最大回撤</strong><br>{format_decimal_fixed(best_result.report.max_drawdown, 4)}</div>
    <div class="card"><strong>连续亏损</strong><br>{_btc_ema15_ma50_max_consecutive_losses(best_result)}</div>
    <div class="card"><strong>平均R / 中位数R</strong><br>{format_decimal_fixed(best_result.report.average_r_multiple, 4)} / {format_decimal_fixed(_btc_ema15_ma50_median_r(best_result), 4)}</div>
  </div>
  <h2>参数组合排名</h2>
  <table>
    <thead>
      <tr><th>排名</th><th>ATR周期</th><th>ATR止损倍数</th><th>Cross窗口</th><th>最大回踩序号</th><th>日线过滤</th><th>Exit模式</th><th>RR</th><th>净利润</th><th>总R</th><th>PF</th><th>最大回撤</th></tr>
    </thead>
    <tbody>{comparison_table}</tbody>
  </table>
  <h2>资金曲线与回撤曲线</h2>
  <div id="equityChart"></div>
  <h2>月度收益</h2>
  <table><thead><tr><th>月份</th><th>交易数</th><th>收益率</th><th>净利润</th></tr></thead><tbody>{monthly_table}</tbody></table>
  <h2>年度收益</h2>
  <table><thead><tr><th>年份</th><th>交易数</th><th>收益率</th><th>净利润</th></tr></thead><tbody>{yearly_table}</tbody></table>
  <h2>每笔交易列表</h2>
  <table>
    <thead><tr><th>ID</th><th>入场时间</th><th>离场时间</th><th>入场价</th><th>离场价</th><th>净利润</th><th>R</th><th>离场原因</th><th>K线图</th></tr></thead>
    <tbody>{trades_table}</tbody>
  </table>
  <script>
    const payload = {json.dumps(equity_payload, ensure_ascii=False)};
    Plotly.newPlot("equityChart", [
      {{ x: payload.x, y: payload.net, type: "scatter", mode: "lines", name: "资金曲线", line: {{ color: "#8f3f2b", width: 2 }} }},
      {{ x: payload.x, y: payload.drawdown, type: "scatter", mode: "lines", name: "回撤曲线", yaxis: "y2", line: {{ color: "#1d3557", width: 2 }} }}
    ], {{
      margin: {{ l: 50, r: 50, t: 30, b: 40 }},
      paper_bgcolor: "#f4efe6",
      plot_bgcolor: "#fffdf8",
      xaxis: {{ title: "时间" }},
      yaxis: {{ title: "资金曲线" }},
      yaxis2: {{ title: "回撤", overlaying: "y", side: "right" }},
      legend: {{ orientation: "h" }}
    }}, {{ responsive: true }});
  </script>
</body>
</html>
"""
