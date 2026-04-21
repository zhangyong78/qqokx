from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.backtest import (
    BacktestResult,
    _apply_slippage_price,
)
from okx_quant.models import StrategyConfig
from okx_quant.strategy_catalog import BACKTEST_STRATEGY_DEFINITIONS, is_dynamic_strategy_id


STRATEGY_ID_TO_NAME = {item.strategy_id: item.name for item in BACKTEST_STRATEGY_DEFINITIONS}
FUNDING_INTERVAL_MS = Decimal("28800000")
ZERO = Decimal("0")


@dataclass(frozen=True)
class _CapitalExposure:
    position_id: str
    pool: str
    signal: str
    entry_index: int
    end_index_exclusive: int
    entry_ts: int
    entry_price: Decimal
    size: Decimal
    entry_fee: Decimal
    estimated_exit_fee_rate: Decimal
    slippage_rate: Decimal
    funding_rate: Decimal
    stop_loss: Decimal
    take_profit: Decimal


@dataclass(frozen=True)
class _MarkToMarketEstimate:
    exit_price_estimated: Decimal
    exit_fee_estimated: Decimal
    exit_slippage_cost_estimated: Decimal
    funding_cost: Decimal
    gross_pnl_report_basis: Decimal
    pnl_report_basis: Decimal
    gross_pnl_liquidation_basis: Decimal
    pnl_liquidation_basis: Decimal


@dataclass(frozen=True)
class _OperationEvent:
    ts: int
    candle_index: int
    sort_order: int
    event_seq_hint: str
    position_id: str
    group: str
    action: str
    signal: str
    price: Decimal
    size: Decimal
    entry_fee: Decimal | None = None
    exit_fee: Decimal | None = None
    total_fee: Decimal | None = None
    funding_cost: Decimal | None = None
    gross_pnl: Decimal | None = None
    pnl: Decimal | None = None
    floating_pnl_report_basis: Decimal | None = None
    floating_pnl_liquidation_basis: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    break_even_price: Decimal | None = None
    realized_pnl_change: Decimal = ZERO
    reason: str = ""
    note: str = ""
    auto_delta: int = 0
    manual_delta: int = 0


def single_backtest_artifact_paths(report_path: Path | str) -> dict[str, Path]:
    target = Path(report_path)
    return {
        "report": target,
        "capital": target.with_suffix(".capital.csv"),
        "operations": target.with_suffix(".operations.csv"),
        "manifest": target.with_suffix(".audit.json"),
    }


def batch_backtest_artifact_paths(report_path: Path | str) -> dict[str, Path]:
    target = Path(report_path)
    return {
        "report": target,
        "manifest": target.with_suffix(".audit.json"),
        "detail_dir": target.parent / f"{target.stem}_details",
    }


def describe_backtest_export_artifacts(report_path: Path | str) -> list[str]:
    target = Path(report_path)
    lines = [f"报告文件：{target}"]
    single_paths = single_backtest_artifact_paths(target)
    batch_paths = batch_backtest_artifact_paths(target)
    if single_paths["capital"].exists() or single_paths["operations"].exists():
        if single_paths["capital"].exists():
            lines.append(f"资金审计：{single_paths['capital']}")
        if single_paths["operations"].exists():
            lines.append(f"操作日志：{single_paths['operations']}")
        if single_paths["manifest"].exists():
            lines.append(f"审计清单：{single_paths['manifest']}")
        return lines
    if batch_paths["detail_dir"].exists():
        lines.append(f"批量明细目录：{batch_paths['detail_dir']}")
    if batch_paths["manifest"].exists():
        lines.append(f"批量审计清单：{batch_paths['manifest']}")
    return lines


def export_single_backtest_audit_files(
    result: BacktestResult,
    config: StrategyConfig,
    candle_limit: int,
    *,
    exported_at: datetime,
    report_path: Path,
) -> dict[str, Path]:
    paths = single_backtest_artifact_paths(report_path)
    capital_rows = _build_capital_audit_rows(result)
    operation_rows = _build_operation_rows(result)
    _write_csv(paths["capital"], capital_rows)
    _write_csv(paths["operations"], operation_rows)
    manifest_payload = {
        "schema_version": 1,
        "export_scope": "single",
        "exported_at": exported_at.isoformat(timespec="seconds"),
        "strategy": _strategy_config_snapshot(config),
        "time_range": _time_range_snapshot(result),
        "counts": {
            "candle_count": len(result.candles),
            "trade_count": len(result.trades),
            "manual_position_count": len(result.manual_positions),
            "terminal_open_position_count": 1 if result.open_position is not None else 0,
            "capital_row_count": len(capital_rows),
            "operation_event_count": len(operation_rows),
            "candle_limit": candle_limit,
        },
        "report_summary": _report_snapshot(result),
        "methodology": {
            "capital_curve": [
                "realized_equity = initial_capital + cumulative realized pnl from closed trades",
                "report_basis floating pnl = close-based mark-to-market minus entry fee and accrued funding",
                "liquidation_basis floating pnl = report_basis plus estimated exit slippage and exit fee impact",
                "marked_equity_liquidation_basis is the strictest capital view and is used for marked drawdown columns",
            ],
            "operation_log": [
                "entry / exit / handoff / snapshot events are ordered by time, then by event type",
                "auto_positions_after / manual_positions_after are reconstructed from event deltas",
                "manual snapshot rows preserve end-of-backtest positions that still need human handling",
            ],
        },
        "files": {
            "report": _file_metadata(paths["report"]),
            "capital": _file_metadata(paths["capital"]),
            "operations": _file_metadata(paths["operations"]),
        },
    }
    _write_json(paths["manifest"], manifest_payload)
    return paths


def export_batch_backtest_manifest(
    *,
    report_path: Path,
    batch_label: str,
    candle_limit: int,
    exported_at: datetime,
    results: list[dict[str, object]],
) -> Path:
    paths = batch_backtest_artifact_paths(report_path)
    payload = {
        "schema_version": 1,
        "export_scope": "batch",
        "exported_at": exported_at.isoformat(timespec="seconds"),
        "batch_label": batch_label,
        "candle_limit": candle_limit,
        "result_count": len(results),
        "detail_dir": str(paths["detail_dir"]),
        "files": {
            "report": _file_metadata(paths["report"]),
        },
        "results": [],
        "methodology": {
            "detail_reports": "Each result in detail_dir has its own full txt report plus capital / operations / audit sidecars.",
            "matrix_report": "The root batch txt remains the quick comparison entry point; detailed audit lives in the detail directory.",
        },
    }
    for item in results:
        detail_report = Path(str(item["report_path"]))
        single_paths = single_backtest_artifact_paths(detail_report)
        config = item["config"]
        result = item["result"]
        if not isinstance(config, StrategyConfig) or not isinstance(result, BacktestResult):
            continue
        payload["results"].append(
            {
                "index": int(item["index"]),
                "label": str(item.get("label", "")),
                "strategy": _strategy_config_snapshot(config),
                "time_range": _time_range_snapshot(result),
                "report_summary": _report_snapshot(result),
                "files": {
                    "report": _file_metadata(detail_report),
                    "capital": _file_metadata(single_paths["capital"]),
                    "operations": _file_metadata(single_paths["operations"]),
                    "manifest": _file_metadata(single_paths["manifest"]),
                },
            }
        )
    _write_json(paths["manifest"], payload)
    return paths["manifest"]

def _build_capital_audit_rows(result: BacktestResult) -> list[dict[str, str]]:
    exposures = _build_capital_exposures(result)
    realized_changes = [ZERO for _ in result.candles]
    last_index = max(len(result.candles) - 1, 0)
    for trade in result.trades:
        if not result.candles:
            continue
        exit_index = max(0, min(trade.exit_index, last_index))
        realized_changes[exit_index] += trade.pnl
    rows: list[dict[str, str]] = []
    realized_running = ZERO
    marked_peak_liquidation = result.initial_capital
    for index, candle in enumerate(result.candles):
        realized_change = realized_changes[index]
        realized_running += realized_change
        auto_report = ZERO
        manual_report = ZERO
        auto_liquidation = ZERO
        manual_liquidation = ZERO
        auto_positions = 0
        manual_positions = 0
        for exposure in exposures:
            if index < exposure.entry_index or index >= exposure.end_index_exclusive:
                continue
            estimate = _estimate_mark_to_market(
                signal=exposure.signal,
                entry_price=exposure.entry_price,
                current_price=candle.close,
                size=exposure.size,
                entry_fee=exposure.entry_fee,
                estimated_exit_fee_rate=exposure.estimated_exit_fee_rate,
                slippage_rate=exposure.slippage_rate,
                tick_size=result.instrument.tick_size,
                funding_rate=exposure.funding_rate,
                entry_ts=exposure.entry_ts,
                current_ts=candle.ts,
            )
            if exposure.pool == "manual":
                manual_positions += 1
                manual_report += estimate.pnl_report_basis
                manual_liquidation += estimate.pnl_liquidation_basis
            else:
                auto_positions += 1
                auto_report += estimate.pnl_report_basis
                auto_liquidation += estimate.pnl_liquidation_basis
        floating_report = auto_report + manual_report
        floating_liquidation = auto_liquidation + manual_liquidation
        realized_equity = result.initial_capital + realized_running
        marked_equity_report = realized_equity + floating_report
        marked_equity_liquidation = realized_equity + floating_liquidation
        if marked_equity_liquidation > marked_peak_liquidation:
            marked_peak_liquidation = marked_equity_liquidation
        marked_drawdown_liquidation = marked_peak_liquidation - marked_equity_liquidation
        marked_drawdown_liquidation_pct = (
            ZERO
            if marked_peak_liquidation <= 0
            else (marked_drawdown_liquidation / marked_peak_liquidation) * Decimal("100")
        )
        realized_drawdown = result.drawdown_curve[index] if index < len(result.drawdown_curve) else ZERO
        realized_drawdown_pct = result.drawdown_pct_curve[index] if index < len(result.drawdown_pct_curve) else ZERO
        rows.append(
            {
                "candle_index": str(index),
                "ts": str(candle.ts),
                "datetime": _format_timestamp(candle.ts),
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "realized_pnl_change": str(realized_change),
                "realized_pnl_cumulative": str(realized_running),
                "realized_equity": str(realized_equity),
                "auto_floating_pnl_report_basis": str(auto_report),
                "manual_floating_pnl_report_basis": str(manual_report),
                "floating_pnl_report_basis": str(floating_report),
                "auto_floating_pnl_liquidation_basis": str(auto_liquidation),
                "manual_floating_pnl_liquidation_basis": str(manual_liquidation),
                "floating_pnl_liquidation_basis": str(floating_liquidation),
                "marked_equity_report_basis": str(marked_equity_report),
                "marked_equity_liquidation_basis": str(marked_equity_liquidation),
                "realized_drawdown": str(realized_drawdown),
                "realized_drawdown_pct": str(realized_drawdown_pct),
                "marked_drawdown_liquidation": str(marked_drawdown_liquidation),
                "marked_drawdown_liquidation_pct": str(marked_drawdown_liquidation_pct),
                "auto_positions": str(auto_positions),
                "manual_positions": str(manual_positions),
                "occupied_slots": str(auto_positions + manual_positions),
            }
        )
    return rows


def _build_operation_rows(result: BacktestResult) -> list[dict[str, str]]:
    events = _build_operation_events(result)
    rows: list[dict[str, str]] = []
    auto_positions = 0
    manual_positions = 0
    for index, event in enumerate(events, start=1):
        auto_positions += event.auto_delta
        manual_positions += event.manual_delta
        rows.append(
            {
                "event_seq": str(index),
                "event_hint": event.event_seq_hint,
                "position_id": event.position_id,
                "group": event.group,
                "action": event.action,
                "ts": str(event.ts),
                "datetime": _format_timestamp(event.ts),
                "candle_index": str(event.candle_index),
                "direction": event.signal,
                "price": _decimal_text(event.price),
                "size": _decimal_text(event.size),
                "entry_fee": _decimal_text(event.entry_fee),
                "exit_fee": _decimal_text(event.exit_fee),
                "total_fee": _decimal_text(event.total_fee),
                "funding_cost": _decimal_text(event.funding_cost),
                "gross_pnl": _decimal_text(event.gross_pnl),
                "pnl": _decimal_text(event.pnl),
                "floating_pnl_report_basis": _decimal_text(event.floating_pnl_report_basis),
                "floating_pnl_liquidation_basis": _decimal_text(event.floating_pnl_liquidation_basis),
                "stop_loss": _decimal_text(event.stop_loss),
                "take_profit": _decimal_text(event.take_profit),
                "break_even_price": _decimal_text(event.break_even_price),
                "realized_pnl_change": _decimal_text(event.realized_pnl_change),
                "auto_positions_after": str(auto_positions),
                "manual_positions_after": str(manual_positions),
                "occupied_slots_after": str(auto_positions + manual_positions),
                "reason": event.reason,
                "note": event.note,
            }
        )
    return rows


def _build_capital_exposures(result: BacktestResult) -> list[_CapitalExposure]:
    exposures: list[_CapitalExposure] = []
    for index, trade in enumerate(result.trades, start=1):
        exposures.append(
            _CapitalExposure(
                position_id=f"trade_{index:03d}",
                pool="auto",
                signal=trade.signal,
                entry_index=trade.entry_index,
                end_index_exclusive=max(trade.entry_index, trade.exit_index),
                entry_ts=trade.entry_ts,
                entry_price=trade.entry_price,
                size=trade.size,
                entry_fee=trade.entry_fee,
                estimated_exit_fee_rate=_fee_rate_from_type(result, trade.exit_fee_type),
                slippage_rate=result.slippage_rate,
                funding_rate=result.funding_rate,
                stop_loss=trade.stop_loss,
                take_profit=trade.take_profit,
            )
        )
    for index, manual_position in enumerate(result.manual_positions, start=1):
        exposures.append(
            _CapitalExposure(
                position_id=f"manual_{index:03d}",
                pool="manual",
                signal=manual_position.signal,
                entry_index=manual_position.entry_index,
                end_index_exclusive=len(result.candles),
                entry_ts=manual_position.entry_ts,
                entry_price=manual_position.entry_price,
                size=manual_position.size,
                entry_fee=manual_position.entry_fee,
                estimated_exit_fee_rate=result.taker_fee_rate,
                slippage_rate=result.slippage_rate,
                funding_rate=result.funding_rate,
                stop_loss=manual_position.stop_loss,
                take_profit=manual_position.take_profit,
            )
        )
    if result.open_position is not None:
        open_position = result.open_position
        exposures.append(
            _CapitalExposure(
                position_id="open_terminal",
                pool="auto",
                signal=open_position.signal,
                entry_index=open_position.entry_index,
                end_index_exclusive=len(result.candles),
                entry_ts=open_position.entry_ts,
                entry_price=open_position.entry_price,
                size=open_position.size,
                entry_fee=open_position.entry_fee,
                estimated_exit_fee_rate=result.taker_fee_rate,
                slippage_rate=result.slippage_rate,
                funding_rate=result.funding_rate,
                stop_loss=open_position.stop_loss,
                take_profit=open_position.take_profit,
            )
        )
    return exposures

def _build_operation_events(result: BacktestResult) -> list[_OperationEvent]:
    events: list[_OperationEvent] = []
    for index, trade in enumerate(result.trades, start=1):
        position_id = f"trade_{index:03d}"
        events.append(
            _OperationEvent(
                ts=trade.entry_ts,
                candle_index=trade.entry_index,
                sort_order=0,
                event_seq_hint=f"T{index:03d}-ENTRY",
                position_id=position_id,
                group="trade",
                action="entry",
                signal=trade.signal,
                price=trade.entry_price,
                size=trade.size,
                entry_fee=trade.entry_fee,
                stop_loss=trade.stop_loss,
                take_profit=trade.take_profit,
                note="strategy_entry",
                auto_delta=1,
            )
        )
        events.append(
            _OperationEvent(
                ts=trade.exit_ts,
                candle_index=trade.exit_index,
                sort_order=1,
                event_seq_hint=f"T{index:03d}-EXIT",
                position_id=position_id,
                group="trade",
                action="exit",
                signal=trade.signal,
                price=trade.exit_price,
                size=trade.size,
                entry_fee=trade.entry_fee,
                exit_fee=trade.exit_fee,
                total_fee=trade.total_fee,
                funding_cost=trade.funding_cost,
                gross_pnl=trade.gross_pnl,
                pnl=trade.pnl,
                realized_pnl_change=trade.pnl,
                stop_loss=trade.stop_loss,
                take_profit=trade.take_profit,
                reason=trade.exit_reason,
                note="closed_trade",
                auto_delta=-1,
            )
        )
    for index, manual_position in enumerate(result.manual_positions, start=1):
        position_id = f"manual_{index:03d}"
        handoff_estimate = _estimate_mark_to_market(
            signal=manual_position.signal,
            entry_price=manual_position.entry_price,
            current_price=manual_position.handoff_price,
            size=manual_position.size,
            entry_fee=manual_position.entry_fee,
            estimated_exit_fee_rate=result.taker_fee_rate,
            slippage_rate=result.slippage_rate,
            tick_size=result.instrument.tick_size,
            funding_rate=result.funding_rate,
            entry_ts=manual_position.entry_ts,
            current_ts=manual_position.handoff_ts,
        )
        snapshot_estimate = _estimate_mark_to_market(
            signal=manual_position.signal,
            entry_price=manual_position.entry_price,
            current_price=manual_position.current_price,
            size=manual_position.size,
            entry_fee=manual_position.entry_fee,
            estimated_exit_fee_rate=result.taker_fee_rate,
            slippage_rate=result.slippage_rate,
            tick_size=result.instrument.tick_size,
            funding_rate=result.funding_rate,
            entry_ts=manual_position.entry_ts,
            current_ts=manual_position.current_ts,
        )
        events.append(
            _OperationEvent(
                ts=manual_position.entry_ts,
                candle_index=manual_position.entry_index,
                sort_order=0,
                event_seq_hint=f"M{index:03d}-ENTRY",
                position_id=position_id,
                group="manual",
                action="entry",
                signal=manual_position.signal,
                price=manual_position.entry_price,
                size=manual_position.size,
                entry_fee=manual_position.entry_fee,
                stop_loss=manual_position.stop_loss,
                take_profit=manual_position.take_profit,
                note="strategy_entry",
                auto_delta=1,
            )
        )
        events.append(
            _OperationEvent(
                ts=manual_position.handoff_ts,
                candle_index=manual_position.handoff_index,
                sort_order=2,
                event_seq_hint=f"M{index:03d}-HANDOFF",
                position_id=position_id,
                group="manual",
                action="handoff_to_manual",
                signal=manual_position.signal,
                price=manual_position.handoff_price,
                size=manual_position.size,
                entry_fee=manual_position.entry_fee,
                exit_fee=handoff_estimate.exit_fee_estimated,
                total_fee=manual_position.entry_fee + handoff_estimate.exit_fee_estimated,
                funding_cost=handoff_estimate.funding_cost,
                gross_pnl=handoff_estimate.gross_pnl_report_basis,
                pnl=handoff_estimate.pnl_report_basis,
                floating_pnl_report_basis=handoff_estimate.pnl_report_basis,
                floating_pnl_liquidation_basis=handoff_estimate.pnl_liquidation_basis,
                stop_loss=manual_position.stop_loss,
                take_profit=manual_position.take_profit,
                break_even_price=manual_position.break_even_price,
                reason=manual_position.handoff_reason,
                note="signal_invalidated_then_handed_to_manual_pool",
                auto_delta=-1,
                manual_delta=1,
            )
        )
        events.append(
            _OperationEvent(
                ts=manual_position.current_ts,
                candle_index=len(result.candles) - 1 if result.candles else manual_position.handoff_index,
                sort_order=3,
                event_seq_hint=f"M{index:03d}-SNAPSHOT",
                position_id=position_id,
                group="manual",
                action="manual_snapshot",
                signal=manual_position.signal,
                price=manual_position.current_price,
                size=manual_position.size,
                entry_fee=manual_position.entry_fee,
                exit_fee=snapshot_estimate.exit_fee_estimated,
                total_fee=manual_position.entry_fee + snapshot_estimate.exit_fee_estimated,
                funding_cost=manual_position.funding_cost,
                gross_pnl=manual_position.gross_pnl,
                pnl=manual_position.pnl,
                floating_pnl_report_basis=manual_position.pnl,
                floating_pnl_liquidation_basis=snapshot_estimate.pnl_liquidation_basis,
                stop_loss=manual_position.stop_loss,
                take_profit=manual_position.take_profit,
                break_even_price=manual_position.break_even_price,
                reason=manual_position.handoff_reason,
                note="backtest_end_manual_pool_snapshot",
            )
        )
    if result.open_position is not None:
        open_position = result.open_position
        snapshot_estimate = _estimate_mark_to_market(
            signal=open_position.signal,
            entry_price=open_position.entry_price,
            current_price=open_position.current_price,
            size=open_position.size,
            entry_fee=open_position.entry_fee,
            estimated_exit_fee_rate=result.taker_fee_rate,
            slippage_rate=result.slippage_rate,
            tick_size=result.instrument.tick_size,
            funding_rate=result.funding_rate,
            entry_ts=open_position.entry_ts,
            current_ts=open_position.current_ts,
        )
        events.append(
            _OperationEvent(
                ts=open_position.entry_ts,
                candle_index=open_position.entry_index,
                sort_order=0,
                event_seq_hint="O001-ENTRY",
                position_id="open_terminal",
                group="open",
                action="entry",
                signal=open_position.signal,
                price=open_position.entry_price,
                size=open_position.size,
                entry_fee=open_position.entry_fee,
                stop_loss=open_position.initial_stop_loss,
                take_profit=open_position.initial_take_profit,
                note="strategy_entry",
                auto_delta=1,
            )
        )
        events.append(
            _OperationEvent(
                ts=open_position.current_ts,
                candle_index=len(result.candles) - 1 if result.candles else open_position.entry_index,
                sort_order=3,
                event_seq_hint="O001-SNAPSHOT",
                position_id="open_terminal",
                group="open",
                action="open_position_snapshot",
                signal=open_position.signal,
                price=open_position.current_price,
                size=open_position.size,
                entry_fee=open_position.entry_fee,
                exit_fee=snapshot_estimate.exit_fee_estimated,
                total_fee=open_position.entry_fee + snapshot_estimate.exit_fee_estimated,
                funding_cost=open_position.funding_cost,
                gross_pnl=open_position.gross_pnl,
                pnl=open_position.pnl,
                floating_pnl_report_basis=open_position.pnl,
                floating_pnl_liquidation_basis=snapshot_estimate.pnl_liquidation_basis,
                stop_loss=open_position.stop_loss,
                take_profit=open_position.take_profit,
                note="backtest_end_open_position_snapshot",
            )
        )
    events.sort(key=lambda item: (item.ts, item.candle_index, item.sort_order, item.position_id, item.event_seq_hint))
    return events


def _estimate_mark_to_market(
    *,
    signal: str,
    entry_price: Decimal,
    current_price: Decimal,
    size: Decimal,
    entry_fee: Decimal,
    estimated_exit_fee_rate: Decimal,
    slippage_rate: Decimal,
    tick_size: Decimal,
    funding_rate: Decimal,
    entry_ts: int,
    current_ts: int,
) -> _MarkToMarketEstimate:
    if signal == "long":
        gross_pnl_report_basis = (current_price - entry_price) * size
    else:
        gross_pnl_report_basis = (entry_price - current_price) * size
    funding_periods = Decimal(str(max(current_ts - entry_ts, 0))) / FUNDING_INTERVAL_MS
    funding_cost = abs(entry_price * size) * funding_rate * funding_periods
    pnl_report_basis = gross_pnl_report_basis - entry_fee - funding_cost
    exit_price_estimated = _apply_slippage_price(
        current_price,
        signal=signal,
        tick_size=tick_size,
        slippage_rate=slippage_rate,
        is_entry=False,
    )
    if signal == "long":
        gross_pnl_liquidation_basis = (exit_price_estimated - entry_price) * size
    else:
        gross_pnl_liquidation_basis = (entry_price - exit_price_estimated) * size
    exit_fee_estimated = abs(exit_price_estimated * size) * estimated_exit_fee_rate
    pnl_liquidation_basis = gross_pnl_liquidation_basis - entry_fee - exit_fee_estimated - funding_cost
    exit_slippage_cost_estimated = abs(exit_price_estimated - current_price) * abs(size)
    return _MarkToMarketEstimate(
        exit_price_estimated=exit_price_estimated,
        exit_fee_estimated=exit_fee_estimated,
        exit_slippage_cost_estimated=exit_slippage_cost_estimated,
        funding_cost=funding_cost,
        gross_pnl_report_basis=gross_pnl_report_basis,
        pnl_report_basis=pnl_report_basis,
        gross_pnl_liquidation_basis=gross_pnl_liquidation_basis,
        pnl_liquidation_basis=pnl_liquidation_basis,
    )

def _strategy_config_snapshot(config: StrategyConfig) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": config.strategy_id,
        "strategy_name": STRATEGY_ID_TO_NAME.get(config.strategy_id, config.strategy_id),
        "backtest_profile_id": config.backtest_profile_id or None,
        "backtest_profile_name": config.backtest_profile_name or None,
        "backtest_profile_summary": config.backtest_profile_summary or None,
        "inst_id": config.inst_id,
        "bar": config.bar,
        "signal_mode": config.signal_mode,
        "ema_period": config.ema_period,
        "trend_ema_period": config.trend_ema_period,
        "entry_reference_ema_period": config.resolved_entry_reference_ema_period(),
        "big_ema_period": config.big_ema_period,
        "atr_period": config.atr_period,
        "atr_stop_multiplier": str(config.atr_stop_multiplier),
        "atr_take_multiplier": str(config.atr_take_multiplier),
        "order_size": str(config.order_size),
        "risk_amount": str(config.risk_amount) if config.risk_amount is not None else None,
        "take_profit_mode": config.take_profit_mode,
        "max_entries_per_trend": config.max_entries_per_trend,
        "backtest_initial_capital": str(config.backtest_initial_capital),
        "backtest_sizing_mode": config.backtest_sizing_mode,
        "backtest_compounding": config.backtest_compounding,
        "backtest_entry_slippage_rate": str(config.resolved_backtest_entry_slippage_rate()),
        "backtest_exit_slippage_rate": str(config.resolved_backtest_exit_slippage_rate()),
        "backtest_slippage_rate": str(config.backtest_slippage_rate),
        "backtest_funding_rate": str(config.backtest_funding_rate),
    }
    if is_dynamic_strategy_id(config.strategy_id):
        payload["dynamic_two_r_break_even"] = config.dynamic_two_r_break_even
        payload["dynamic_fee_offset_enabled"] = config.dynamic_fee_offset_enabled
        payload["time_stop_break_even_enabled"] = config.time_stop_break_even_enabled
        payload["time_stop_break_even_bars"] = config.resolved_time_stop_break_even_bars()
    return payload


def _time_range_snapshot(result: BacktestResult) -> dict[str, object]:
    if not result.candles:
        return {"start_ts": None, "end_ts": None, "start": None, "end": None}
    return {
        "start_ts": result.candles[0].ts,
        "end_ts": result.candles[-1].ts,
        "start": _format_timestamp(result.candles[0].ts),
        "end": _format_timestamp(result.candles[-1].ts),
    }


def _report_snapshot(result: BacktestResult) -> dict[str, object]:
    report = result.report
    return {
        "total_trades": report.total_trades,
        "win_trades": report.win_trades,
        "loss_trades": report.loss_trades,
        "breakeven_trades": report.breakeven_trades,
        "win_rate": str(report.win_rate),
        "total_pnl": str(report.total_pnl),
        "average_pnl": str(report.average_pnl),
        "gross_profit": str(report.gross_profit),
        "gross_loss": str(report.gross_loss),
        "profit_factor": None if report.profit_factor is None else str(report.profit_factor),
        "average_r_multiple": str(report.average_r_multiple),
        "max_drawdown": str(report.max_drawdown),
        "max_drawdown_pct": str(report.max_drawdown_pct),
        "ending_equity": str(report.ending_equity),
        "total_return_pct": str(report.total_return_pct),
        "maker_fees": str(report.maker_fees),
        "taker_fees": str(report.taker_fees),
        "total_fees": str(report.total_fees),
        "slippage_costs": str(report.slippage_costs),
        "funding_costs": str(report.funding_costs),
        "manual_handoffs": report.manual_handoffs,
        "manual_open_positions": report.manual_open_positions,
        "manual_open_size": str(report.manual_open_size),
        "manual_open_pnl": str(report.manual_open_pnl),
        "max_manual_positions": report.max_manual_positions,
        "max_total_occupied_slots": report.max_total_occupied_slots,
    }


def _fee_rate_from_type(result: BacktestResult, fee_type: str) -> Decimal:
    if fee_type == "maker":
        return result.maker_fee_rate
    if fee_type == "taker":
        return result.taker_fee_rate
    return ZERO


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    if not fieldnames:
        if ".capital." in path.name:
            fieldnames = [
                "candle_index",
                "ts",
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "realized_pnl_change",
                "realized_pnl_cumulative",
                "realized_equity",
                "auto_floating_pnl_report_basis",
                "manual_floating_pnl_report_basis",
                "floating_pnl_report_basis",
                "auto_floating_pnl_liquidation_basis",
                "manual_floating_pnl_liquidation_basis",
                "floating_pnl_liquidation_basis",
                "marked_equity_report_basis",
                "marked_equity_liquidation_basis",
                "realized_drawdown",
                "realized_drawdown_pct",
                "marked_drawdown_liquidation",
                "marked_drawdown_liquidation_pct",
                "auto_positions",
                "manual_positions",
                "occupied_slots",
            ]
        else:
            fieldnames = [
                "event_seq",
                "event_hint",
                "position_id",
                "group",
                "action",
                "ts",
                "datetime",
                "candle_index",
                "direction",
                "price",
                "size",
                "entry_fee",
                "exit_fee",
                "total_fee",
                "funding_cost",
                "gross_pnl",
                "pnl",
                "floating_pnl_report_basis",
                "floating_pnl_liquidation_basis",
                "stop_loss",
                "take_profit",
                "break_even_price",
                "realized_pnl_change",
                "auto_positions_after",
                "manual_positions_after",
                "occupied_slots_after",
                "reason",
                "note",
            ]
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    temp_path.replace(path)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _file_metadata(path: Path) -> dict[str, object]:
    payload: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
    }
    if not path.exists():
        return payload
    content = path.read_bytes()
    payload["size_bytes"] = len(content)
    payload["sha256"] = hashlib.sha256(content).hexdigest()
    return payload


def _format_timestamp(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


def _decimal_text(value: Decimal | None) -> str:
    if value is None:
        return ""
    return str(value)
