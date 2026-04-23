from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta
from decimal import Decimal
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

from okx_quant.backtest import _load_backtest_candles
from okx_quant.enhanced_backtest import EnhancedBacktestLab, export_lab_report_markdown, format_lab_report_markdown
from okx_quant.enhanced_evidence_chart import export_evidence_chart_bundle
from okx_quant.enhanced_history_loader import load_segmented_history_candles
from okx_quant.enhanced_models import LabSimulationConfig, QuotaSnapshot
from okx_quant.enhanced_position_ledger import (
    TARGET_REDUCE_NET_PNL_PCT,
    TARGET_SMALL_PROFIT_NET_PNL_PCT,
    TOTAL_BUCKET_MODE_CHOICES,
    TOTAL_BUCKET_MODE_DEFAULT,
    TOTAL_BUCKET_MODE_LABELS,
    build_active_position_ledger_rows,
    build_active_position_summary_rows,
    build_total_position_summary_rows,
)
from okx_quant.enhanced_registry import EnhancedStrategyRegistry
from okx_quant.enhanced_review_pages import (
    build_manual_review_rows,
    write_manual_review_gallery_html,
    write_position_management_html,
    write_total_position_management_html,
)
from okx_quant.enhanced_runtime_config import (
    apply_strategy_runtime_payload,
    build_strategy_runtime_payload,
    get_strategy_runtime_payload,
    load_runtime_store,
    normalize_profile_payload,
    serialize_lab_profile,
    write_strategy_runtime_payload,
)
from okx_quant.enhanced_seed_strategies import register_seed_strategy_package
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path, enhanced_strategy_runtime_file_path


SHANGHAI = ZoneInfo("Asia/Shanghai")
DEFAULT_RUNTIME_CONFIG_FILE = str(enhanced_strategy_runtime_file_path())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a real-data seed lab report for 现货增强三十六计.")
    parser.add_argument("--inst-id", default="BTC-USDT")
    parser.add_argument("--underlying-family", default="BTC-USD")
    parser.add_argument("--base-bar", default="5m")
    parser.add_argument("--signal-bar", default="5m")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--segment-days", type=int, default=7)
    parser.add_argument("--long-limit", default="50")
    parser.add_argument("--short-limit", default="50")
    parser.add_argument("--protected-long", default="50")
    parser.add_argument("--protected-short", default="50")
    parser.add_argument("--exit-mode", choices=("fixed_hold", "tp_sl_handoff"), default="tp_sl_handoff")
    parser.add_argument("--fixed-hold-bars", type=int, default=0)
    parser.add_argument("--max-hold-bars", type=int, default=24)
    parser.add_argument("--stop-loss-pct", default="0.005")
    parser.add_argument("--take-profit-pct", default="0.010")
    parser.add_argument("--fee-rate", default="0.0005")
    parser.add_argument("--slippage-rate", default="0.0002")
    parser.add_argument("--funding-rate-8h", default="0")
    parser.add_argument("--stop-hit-mode", choices=("close_loss", "handoff_manual"), default="handoff_manual")
    parser.add_argument(
        "--total-bucket-mode",
        choices=TOTAL_BUCKET_MODE_CHOICES,
        default=TOTAL_BUCKET_MODE_DEFAULT,
    )
    parser.add_argument("--enable-signals", default="")
    parser.add_argument("--disable-signals", default="")
    parser.add_argument("--clear-lab-profiles", default="")
    parser.add_argument("--profile-overrides-file", default="")
    parser.add_argument("--runtime-config-file", default=DEFAULT_RUNTIME_CONFIG_FILE)
    parser.add_argument("--ignore-runtime-config", action="store_true")
    parser.add_argument("--save-runtime-config", action="store_true")
    return parser.parse_args()


def resolve_range(args: argparse.Namespace) -> tuple[datetime, datetime]:
    now = datetime.now(SHANGHAI)
    end_dt = now.replace(second=0, microsecond=0)
    if args.end_date:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=SHANGHAI)
        end_dt = end_dt.replace(hour=23, minute=59)
    if args.start_date:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=SHANGHAI)
    else:
        start_dt = end_dt - timedelta(days=max(args.days, 1))
    return start_dt, end_dt


def build_simulation_config(args: argparse.Namespace) -> LabSimulationConfig:
    return LabSimulationConfig(
        exit_mode=args.exit_mode,
        fixed_hold_bars=max(int(args.fixed_hold_bars), 0),
        max_hold_bars=max(int(args.max_hold_bars), 0),
        stop_loss_pct=Decimal(args.stop_loss_pct),
        take_profit_pct=Decimal(args.take_profit_pct),
        fee_rate=Decimal(args.fee_rate),
        slippage_rate=Decimal(args.slippage_rate),
        funding_rate_per_8h=Decimal(args.funding_rate_8h),
        stop_hit_mode=args.stop_hit_mode,
        close_on_timeout_if_profitable=True,
    ).normalized()


def parse_csv_items(raw: str) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for part in raw.split(","):
        item = part.strip()
        if not item or item in seen:
            continue
        items.append(item)
        seen.add(item)
    return items


def serialize_simulation_config(config: LabSimulationConfig) -> dict[str, object]:
    return {
        "exit_mode": config.exit_mode,
        "fixed_hold_bars": config.fixed_hold_bars,
        "max_hold_bars": config.max_hold_bars,
        "stop_loss_pct": str(config.stop_loss_pct),
        "take_profit_pct": str(config.take_profit_pct),
        "fee_rate": str(config.fee_rate),
        "slippage_rate": str(config.slippage_rate),
        "funding_rate_per_8h": str(config.funding_rate_per_8h),
        "stop_hit_mode": config.stop_hit_mode,
        "close_on_timeout_if_profitable": config.close_on_timeout_if_profitable,
    }


def load_profile_overrides(path: Path) -> dict[str, dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("profile overrides file must be a JSON object keyed by signal_id")
    normalized: dict[str, dict[str, object]] = {}
    for signal_id, raw_changes in payload.items():
        if not isinstance(signal_id, str) or not signal_id.strip():
            raise ValueError("profile override signal_id must be a non-empty string")
        if not isinstance(raw_changes, dict):
            raise TypeError(f"profile override for {signal_id!r} must be a JSON object")
        normalized[signal_id.strip()] = normalize_profile_payload(raw_changes)
    return normalized


def apply_runtime_overrides(registry: EnhancedStrategyRegistry, args: argparse.Namespace) -> dict[str, object]:
    disabled_signals = tuple(parse_csv_items(args.disable_signals))
    enabled_signals = tuple(parse_csv_items(args.enable_signals))
    cleared_lab_profiles = tuple(parse_csv_items(args.clear_lab_profiles))
    profile_overrides_path = Path(args.profile_overrides_file).expanduser() if args.profile_overrides_file else None
    profile_overrides = {} if profile_overrides_path is None else load_profile_overrides(profile_overrides_path)

    for signal_id in disabled_signals:
        registry.set_child_signal_enabled(signal_id, False)
    for signal_id in enabled_signals:
        registry.set_child_signal_enabled(signal_id, True)
    for signal_id in cleared_lab_profiles:
        registry.clear_signal_lab_profile(signal_id)

    patched_profiles: dict[str, dict[str, object]] = {}
    for signal_id, changes in profile_overrides.items():
        patched_profiles[signal_id] = serialize_lab_profile(
            registry.patch_signal_lab_profile(signal_id, **changes)
        )

    return {
        "disabled_signals": list(disabled_signals),
        "enabled_signals": list(enabled_signals),
        "cleared_lab_profiles": list(cleared_lab_profiles),
        "profile_overrides_file": "" if profile_overrides_path is None else str(profile_overrides_path),
        "patched_profiles": patched_profiles,
    }


def resolve_runtime_config_path(args: argparse.Namespace) -> Path:
    return Path(args.runtime_config_file).expanduser()


def apply_saved_runtime_config(
    registry: EnhancedStrategyRegistry,
    *,
    parent_strategy_id: str,
    runtime_config_path: Path,
    ignore_runtime_config: bool,
) -> tuple[dict[str, object], bool]:
    if ignore_runtime_config:
        return {
            "loaded": False,
            "reason": "ignored_by_flag",
            "runtime_config_file": str(runtime_config_path),
        }, False
    store = load_runtime_store(runtime_config_path)
    strategy_payload = get_strategy_runtime_payload(store, parent_strategy_id)
    if strategy_payload is None:
        return {
            "loaded": False,
            "reason": "strategy_not_found",
            "runtime_config_file": str(runtime_config_path),
        }, False
    applied = apply_strategy_runtime_payload(
        registry,
        parent_strategy_id=parent_strategy_id,
        strategy_payload=strategy_payload,
    )
    return {
        "loaded": True,
        "reason": "loaded_from_file",
        "runtime_config_file": str(runtime_config_path),
        **applied,
    }, True


def persist_runtime_config(
    registry: EnhancedStrategyRegistry,
    *,
    parent_strategy_id: str,
    runtime_config_path: Path,
    save_runtime_config: bool,
    ignore_runtime_config: bool,
    had_saved_strategy_config: bool,
) -> tuple[dict[str, object], dict[str, object]]:
    strategy_payload = build_strategy_runtime_payload(
        registry,
        parent_strategy_id=parent_strategy_id,
    )
    should_save = save_runtime_config or (not ignore_runtime_config and not had_saved_strategy_config)
    if not should_save:
        return {
            "saved": False,
            "reason": "not_requested",
            "runtime_config_file": str(runtime_config_path),
        }, strategy_payload
    written = write_strategy_runtime_payload(
        runtime_config_path,
        strategy_payload=strategy_payload,
    )
    return {
        "saved": True,
        "reason": "explicit_save" if save_runtime_config else "bootstrap_missing_strategy_config",
        "runtime_config_file": str(written),
    }, strategy_payload


def write_summary_csv(path: Path, result) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "signal_id",
                "signal_name",
                "underlying_family",
                "lab_profile_name",
                "exit_mode",
                "fixed_hold_bars",
                "max_hold_bars",
                "stop_loss_pct",
                "take_profit_pct",
                "fee_rate",
                "slippage_rate",
                "funding_rate_per_8h",
                "stop_hit_mode",
                "total_signal_events",
                "routed_events",
                "accepted_events",
                "rejected_events",
                "gate_rejected_events",
                "quota_rejected_events",
                "realized_outcomes",
                "take_profit_closed_events",
                "stop_loss_closed_events",
                "timeout_profit_closed_events",
                "manual_handoff_events",
                "unresolved_events",
                "win_events",
                "loss_events",
                "breakeven_events",
                "win_rate_pct",
                "total_pnl_value",
                "average_pnl_value",
                "profit_factor",
                "average_pnl_pct",
                "average_win_pct",
                "average_loss_pct",
                "profit_loss_ratio",
                "playbook_accept_counts",
                "rejection_reasons",
            ]
        )
        for item in result.summaries:
            writer.writerow(
                [
                    item.signal_id,
                    item.signal_name,
                    item.underlying_family,
                    item.lab_profile_name,
                    item.applied_simulation_config.exit_mode,
                    item.applied_simulation_config.fixed_hold_bars,
                    item.applied_simulation_config.max_hold_bars,
                    str(item.applied_simulation_config.stop_loss_pct),
                    str(item.applied_simulation_config.take_profit_pct),
                    str(item.applied_simulation_config.fee_rate),
                    str(item.applied_simulation_config.slippage_rate),
                    str(item.applied_simulation_config.funding_rate_per_8h),
                    item.applied_simulation_config.stop_hit_mode,
                    item.total_signal_events,
                    item.routed_events,
                    item.accepted_events,
                    item.rejected_events,
                    item.gate_rejected_events,
                    item.quota_rejected_events,
                    item.realized_outcomes,
                    item.take_profit_closed_events,
                    item.stop_loss_closed_events,
                    item.timeout_profit_closed_events,
                    item.manual_handoff_events,
                    item.unresolved_events,
                    item.win_events,
                    item.loss_events,
                    item.breakeven_events,
                    str(item.win_rate_pct),
                    str(item.total_pnl_value),
                    str(item.average_pnl_value),
                    "" if item.profit_factor is None else str(item.profit_factor),
                    str(item.average_pnl_pct),
                    str(item.average_win_pct),
                    str(item.average_loss_pct),
                    "" if item.profit_loss_ratio is None else str(item.profit_loss_ratio),
                    "; ".join(f"{name}={count}" for name, count in item.playbook_accept_counts),
                    "; ".join(f"{reason}={count}" for reason, count in item.rejection_reasons),
                ]
            )
    return path


def write_profiles_csv(path: Path, result) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "signal_id",
                "signal_name",
                "underlying_family",
                "lab_profile_name",
                "exit_mode",
                "fixed_hold_bars",
                "max_hold_bars",
                "stop_loss_pct",
                "take_profit_pct",
                "fee_rate",
                "slippage_rate",
                "funding_rate_per_8h",
                "stop_hit_mode",
                "close_on_timeout_if_profitable",
            ]
        )
        for item in result.summaries:
            config = item.applied_simulation_config
            writer.writerow(
                [
                    item.signal_id,
                    item.signal_name,
                    item.underlying_family,
                    item.lab_profile_name,
                    config.exit_mode,
                    config.fixed_hold_bars,
                    config.max_hold_bars,
                    str(config.stop_loss_pct),
                    str(config.take_profit_pct),
                    str(config.fee_rate),
                    str(config.slippage_rate),
                    str(config.funding_rate_per_8h),
                    config.stop_hit_mode,
                    config.close_on_timeout_if_profitable,
                ]
            )
    return path


def write_signal_states_csv(
    path: Path,
    *,
    registry: EnhancedStrategyRegistry,
    parent_strategy_id: str,
    base_simulation: LabSimulationConfig,
) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "signal_id",
                "signal_name",
                "enabled",
                "source_market",
                "source_inst_id",
                "source_bar",
                "underlying_family",
                "direction_bias",
                "evidence_template_id",
                "playbooks",
                "lab_profile_name",
                "exit_mode",
                "fixed_hold_bars",
                "max_hold_bars",
                "stop_loss_pct",
                "take_profit_pct",
                "fee_rate",
                "slippage_rate",
                "funding_rate_per_8h",
                "stop_hit_mode",
                "close_on_timeout_if_profitable",
                "signal_notes",
                "profile_notes",
            ]
        )
        for signal in registry.list_child_signals(parent_strategy_id, enabled_only=False):
            profile = registry.get_signal_lab_profile(signal.signal_id)
            resolved = base_simulation if profile is None else profile.resolve(base_simulation)
            playbooks = registry.list_playbooks_for_signal(signal.signal_id, enabled_only=False)
            writer.writerow(
                [
                    signal.signal_id,
                    signal.signal_name,
                    signal.enabled,
                    signal.source.market,
                    signal.source.inst_id,
                    signal.source.bar,
                    signal.underlying_family,
                    signal.direction_bias,
                    signal.evidence_template_id,
                    "; ".join(f"{item.playbook_id}:{item.playbook_name}" for item in playbooks),
                    "" if profile is None else profile.profile_name,
                    resolved.exit_mode,
                    resolved.fixed_hold_bars,
                    resolved.max_hold_bars,
                    str(resolved.stop_loss_pct),
                    str(resolved.take_profit_pct),
                    str(resolved.fee_rate),
                    str(resolved.slippage_rate),
                    str(resolved.funding_rate_per_8h),
                    resolved.stop_hit_mode,
                    resolved.close_on_timeout_if_profitable,
                    signal.notes,
                    "" if profile is None else profile.notes,
                ]
            )
    return path


def write_events_csv(path: Path, result) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "signal_id",
                "signal_name",
                "playbook_id",
                "playbook_name",
                "evidence_id",
                "evidence_template_id",
                "evidence_summary",
                "underlying_family",
                "candle_ts",
                "signal_price",
                "position_size",
                "accepted",
                "trigger_reason",
                "gate_reason",
                "quota_reason",
                "lifecycle_status",
                "entry_price",
                "stop_loss_price",
                "take_profit_price",
                "holding_bars",
                "exit_ts",
                "exit_price",
                "exit_reason",
                "gross_pnl_value",
                "fee_cost_value",
                "slippage_cost_value",
                "funding_cost_value",
                "realized_pnl_points",
                "realized_pnl_pct",
                "realized_pnl_value",
                "quota_allocations",
            ]
        )
        for item in result.events:
            writer.writerow(
                [
                    item.signal_id,
                    item.signal_name,
                    item.playbook_id,
                    item.playbook_name,
                    item.evidence_id,
                    item.evidence_template_id,
                    item.evidence_summary,
                    item.underlying_family,
                    item.candle_ts,
                    str(item.signal_price),
                    str(item.position_size),
                    item.accepted,
                    item.trigger_reason,
                    item.gate_reason,
                    item.quota_reason,
                    item.lifecycle_status,
                    "" if item.entry_price is None else str(item.entry_price),
                    "" if item.stop_loss_price is None else str(item.stop_loss_price),
                    "" if item.take_profit_price is None else str(item.take_profit_price),
                    item.holding_bars,
                    "" if item.exit_ts is None else item.exit_ts,
                    "" if item.exit_price is None else str(item.exit_price),
                    item.exit_reason,
                    "" if item.gross_pnl_value is None else str(item.gross_pnl_value),
                    "" if item.fee_cost_value is None else str(item.fee_cost_value),
                    "" if item.slippage_cost_value is None else str(item.slippage_cost_value),
                    "" if item.funding_cost_value is None else str(item.funding_cost_value),
                    "" if item.realized_pnl_points is None else str(item.realized_pnl_points),
                    "" if item.realized_pnl_pct is None else str(item.realized_pnl_pct),
                    "" if item.realized_pnl_value is None else str(item.realized_pnl_value),
                    "; ".join(f"{source}={quantity}" for source, quantity in item.quota_allocations),
                ]
            )
    return path


def write_evidence_index_csv(
    path: Path,
    result,
    *,
    chart_paths_by_evidence: dict[str, str] | None = None,
) -> Path:
    chart_paths_by_evidence = chart_paths_by_evidence or {}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "evidence_id",
                "signal_id",
                "signal_name",
                "playbook_id",
                "playbook_name",
                "evidence_template_id",
                "underlying_family",
                "source_market",
                "source_inst_id",
                "source_bar",
                "direction_bias",
                "candle_ts",
                "signal_price",
                "trigger_reason",
                "evidence_summary",
                "setup_start_ts",
                "setup_end_ts",
                "trigger_ts",
                "followthrough_end_ts",
                "setup_candle_count",
                "followthrough_candle_count",
                "trigger_open",
                "trigger_high",
                "trigger_low",
                "trigger_close",
                "chart_path",
                "note",
            ]
        )
        for item in result.evidences:
            trigger_candle = item.trigger_candle
            writer.writerow(
                [
                    item.evidence_id,
                    item.signal_id,
                    item.signal_name,
                    item.playbook_id,
                    item.playbook_name,
                    item.evidence_template_id,
                    item.underlying_family,
                    item.source_market,
                    item.source_inst_id,
                    item.source_bar,
                    item.direction_bias,
                    item.candle_ts,
                    str(item.signal_price),
                    item.trigger_reason,
                    item.evidence_summary,
                    "" if not item.setup_candles else item.setup_candles[0].ts,
                    "" if not item.setup_candles else item.setup_candles[-1].ts,
                    "" if trigger_candle is None else trigger_candle.ts,
                    "" if not item.followthrough_candles else item.followthrough_candles[-1].ts,
                    len(item.setup_candles),
                    len(item.followthrough_candles),
                    "" if trigger_candle is None else str(trigger_candle.open),
                    "" if trigger_candle is None else str(trigger_candle.high),
                    "" if trigger_candle is None else str(trigger_candle.low),
                    "" if trigger_candle is None else str(trigger_candle.close),
                    chart_paths_by_evidence.get(item.evidence_id, ""),
                    item.note,
                ]
            )
    return path


def write_evidence_chart_manifest_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    return _write_dict_rows_csv(
        path,
        [
            "evidence_id",
            "signal_id",
            "signal_name",
            "playbook_id",
            "playbook_name",
            "lifecycle_status",
            "accepted",
            "chart_file_name",
            "chart_path",
            "trigger_reason",
            "evidence_summary",
            "source_market",
            "source_inst_id",
            "source_bar",
        ],
        rows,
    )


def write_manual_review_manifest_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    return _write_dict_rows_csv(
        path,
        [
            "ledger_id",
            "manual_pool",
            "lifecycle_status",
            "signal_id",
            "signal_name",
            "playbook_id",
            "playbook_name",
            "position_side",
            "entry_ts",
            "handoff_ts",
            "entry_price",
            "position_size",
            "latest_reference_price",
            "break_even_reference_price",
            "group_net_break_even_reference_price",
            "group_unified_close_reference_price",
            "group_buffer_to_unified_close_pct",
            "group_target_reduce_reference_price",
            "group_buffer_to_target_reduce_pct",
            "group_target_small_profit_reference_price",
            "group_buffer_to_target_small_profit_pct",
            "group_risk_priority_rank",
            "group_risk_priority_label",
            "group_risk_priority_note",
            "group_suggested_action",
            "estimated_net_pnl_value_if_closed_now",
            "bars_since_entry_to_reference",
            "trigger_reason",
            "evidence_summary",
            "evidence_id",
            "chart_path",
        ],
        rows,
    )


def _serialize_evidence_record(item) -> dict[str, object]:
    return {
        "evidence_id": item.evidence_id,
        "signal_id": item.signal_id,
        "signal_name": item.signal_name,
        "playbook_id": item.playbook_id,
        "playbook_name": item.playbook_name,
        "evidence_template_id": item.evidence_template_id,
        "underlying_family": item.underlying_family,
        "source_market": item.source_market,
        "source_inst_id": item.source_inst_id,
        "source_bar": item.source_bar,
        "direction_bias": item.direction_bias,
        "candle_ts": item.candle_ts,
        "signal_price": str(item.signal_price),
        "trigger_reason": item.trigger_reason,
        "evidence_summary": item.evidence_summary,
        "setup_candles": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
            }
            for candle in item.setup_candles
        ],
        "trigger_candle": (
            None
            if item.trigger_candle is None
            else {
                "ts": item.trigger_candle.ts,
                "open": str(item.trigger_candle.open),
                "high": str(item.trigger_candle.high),
                "low": str(item.trigger_candle.low),
                "close": str(item.trigger_candle.close),
                "volume": str(item.trigger_candle.volume),
            }
        ),
        "followthrough_candles": [
            {
                "ts": candle.ts,
                "open": str(candle.open),
                "high": str(candle.high),
                "low": str(candle.low),
                "close": str(candle.close),
                "volume": str(candle.volume),
            }
            for candle in item.followthrough_candles
        ],
        "note": item.note,
    }


def write_evidences_json(path: Path, result) -> Path:
    payload = {
        "count": len(result.evidences),
        "evidences": [_serialize_evidence_record(item) for item in result.evidences],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def write_runtime_config_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _serialize_csv_value(value: object) -> object:
    if value is None:
        return ""
    return str(value) if isinstance(value, Decimal) else value


def _write_dict_rows_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _serialize_csv_value(row.get(field)) for field in fieldnames})
    return path


def write_position_ledger_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    return _write_dict_rows_csv(
        path,
        [
            "ledger_id",
            "parent_strategy_id",
            "parent_strategy_name",
            "manual_pool",
            "lifecycle_status",
            "signal_id",
            "signal_name",
            "playbook_id",
            "playbook_name",
            "playbook_action",
            "position_side",
            "underlying_family",
            "source_market",
            "source_inst_id",
            "source_bar",
            "entry_ts",
            "handoff_ts",
            "reference_ts",
            "signal_price",
            "entry_price",
            "position_size",
            "position_cost_value",
            "stop_loss_price",
            "take_profit_price",
            "latest_reference_price",
            "estimated_exit_price_if_closed_now",
            "break_even_reference_price",
            "fee_rate",
            "slippage_rate",
            "funding_rate_per_8h",
            "reference_line_capacity_value",
            "reference_line_constant_value",
            "estimated_gross_pnl_value_if_closed_now",
            "entry_fee_cost_value",
            "estimated_exit_fee_cost_value_if_closed_now",
            "estimated_fee_cost_value_if_closed_now",
            "entry_slippage_cost_value",
            "estimated_exit_slippage_cost_value_if_closed_now",
            "estimated_slippage_cost_value_if_closed_now",
            "estimated_funding_cost_value_to_reference",
            "estimated_net_pnl_value_if_closed_now",
            "estimated_net_pnl_pct_if_closed_now",
            "auto_holding_bars",
            "bars_in_manual_pool",
            "bars_since_entry_to_reference",
            "handoff_reason",
            "quota_allocations",
            "trigger_reason",
            "evidence_summary",
            "evidence_id",
            "evidence_template_id",
            "chart_path",
            "setup_candle_count",
            "followthrough_candle_count",
            "evidence_note",
        ],
        rows,
    )


def write_position_ledger_summary_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    return _write_dict_rows_csv(
        path,
        [
            "parent_strategy_id",
            "parent_strategy_name",
            "manual_pool",
            "underlying_family",
            "source_market",
            "source_inst_id",
            "source_bar",
            "playbook_id",
            "playbook_name",
            "playbook_action",
            "position_side",
            "signal_id",
            "signal_name",
            "active_position_count",
            "total_position_size",
            "total_position_cost_value",
            "weighted_avg_entry_price",
            "latest_reference_price",
            "weighted_break_even_reference_price",
            "reference_line_capacity_value",
            "reference_line_constant_value",
            "net_break_even_reference_price",
            "unified_close_reference_price",
            "buffer_to_unified_close_pct",
            "buffer_to_unified_close_value",
            "target_reduce_net_pnl_pct",
            "target_reduce_net_pnl_value",
            "target_reduce_reference_price",
            "buffer_to_target_reduce_pct",
            "buffer_to_target_reduce_value",
            "target_small_profit_net_pnl_pct",
            "target_small_profit_net_pnl_value",
            "target_small_profit_reference_price",
            "buffer_to_target_small_profit_pct",
            "buffer_to_target_small_profit_value",
            "risk_priority_rank",
            "risk_priority_label",
            "risk_priority_note",
            "suggested_action",
            "estimated_gross_pnl_value_if_closed_now",
            "entry_fee_cost_value",
            "estimated_exit_fee_cost_value_if_closed_now",
            "estimated_fee_cost_value_if_closed_now",
            "entry_slippage_cost_value",
            "estimated_exit_slippage_cost_value_if_closed_now",
            "estimated_slippage_cost_value_if_closed_now",
            "estimated_funding_cost_value_to_reference",
            "estimated_net_pnl_value_if_closed_now",
            "estimated_net_pnl_pct_if_closed_now",
            "reference_ts",
            "first_entry_ts",
            "last_entry_ts",
            "evidence_ids",
        ],
        rows,
    )


def write_total_position_summary_csv(path: Path, rows: list[dict[str, object]]) -> Path:
    return _write_dict_rows_csv(
        path,
        [
            "parent_strategy_id",
            "parent_strategy_name",
            "bucket_mode",
            "bucket_mode_label",
            "bucket_key",
            "bucket_label",
            "manual_pool",
            "pool_state",
            "underlying_family",
            "price_domain_key",
            "price_domain_label",
            "source_market",
            "source_inst_id",
            "source_bar",
            "source_count",
            "source_labels",
            "playbook_action",
            "playbook_action_count",
            "playbook_actions",
            "playbook_name",
            "playbook_ids",
            "playbook_names",
            "position_side",
            "summary_group_count",
            "signal_coverage_count",
            "signal_ids",
            "signal_names",
            "active_position_count",
            "manual_position_count",
            "auto_position_count",
            "manual_group_count",
            "auto_group_count",
            "total_position_size",
            "total_position_cost_value",
            "weighted_avg_entry_price",
            "latest_reference_price",
            "weighted_break_even_reference_price",
            "reference_line_capacity_value",
            "reference_line_constant_value",
            "net_break_even_reference_price",
            "unified_close_reference_price",
            "buffer_to_unified_close_pct",
            "buffer_to_unified_close_value",
            "target_reduce_net_pnl_pct",
            "target_reduce_net_pnl_value",
            "target_reduce_reference_price",
            "buffer_to_target_reduce_pct",
            "buffer_to_target_reduce_value",
            "target_small_profit_net_pnl_pct",
            "target_small_profit_net_pnl_value",
            "target_small_profit_reference_price",
            "buffer_to_target_small_profit_pct",
            "buffer_to_target_small_profit_value",
            "risk_priority_rank",
            "risk_priority_label",
            "risk_priority_note",
            "suggested_action",
            "estimated_gross_pnl_value_if_closed_now",
            "entry_fee_cost_value",
            "estimated_exit_fee_cost_value_if_closed_now",
            "estimated_fee_cost_value_if_closed_now",
            "entry_slippage_cost_value",
            "estimated_exit_slippage_cost_value_if_closed_now",
            "estimated_slippage_cost_value_if_closed_now",
            "estimated_funding_cost_value_to_reference",
            "estimated_net_pnl_value_if_closed_now",
            "estimated_net_pnl_pct_if_closed_now",
            "reference_ts",
            "first_entry_ts",
            "last_entry_ts",
        ],
        rows,
    )


def load_realdata_candles(
    *,
    client: OkxRestClient,
    inst_id: str,
    bar: str,
    start_ts: int,
    end_ts: int,
    segment_days: int,
) -> tuple[list, list[dict[str, object]], str]:
    load_range = partial(_load_backtest_candles, client)
    segment_ms = max(int(segment_days), 1) * 24 * 60 * 60 * 1000
    notes: list[dict[str, object]] = []
    result = load_segmented_history_candles(
        load_range,
        inst_id=inst_id,
        bar=bar,
        start_ts=start_ts,
        end_ts=end_ts,
        preload_count=64,
        segment_ms=segment_ms,
    )
    for item in result.segments:
        notes.append(
            {
                "index": item.index,
                "start_ts": item.start_ts,
                "end_ts": item.end_ts,
                "start_time": datetime.fromtimestamp(item.start_ts / 1000, SHANGHAI).isoformat(),
                "end_time": datetime.fromtimestamp(item.end_ts / 1000, SHANGHAI).isoformat(),
                "requested_limit": item.requested_limit,
                "preload_count": item.preload_count,
                "returned_count": item.returned_count,
            }
        )
    summary_note = f"segmented_range_fetch: {len(result.segments)} segments, {len(result.candles)} candles"
    return list(result.candles), notes, summary_note


def main() -> None:
    args = parse_args()
    parent_strategy_id = "spot_enhancement_36"
    start_dt, end_dt = resolve_range(args)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    client = OkxRestClient()
    candles, segment_notes, data_source_note = load_realdata_candles(
        client=client,
        inst_id=args.inst_id,
        bar=args.base_bar,
        start_ts=start_ts,
        end_ts=end_ts,
        segment_days=args.segment_days,
    )
    simulation = build_simulation_config(args)

    registry = EnhancedStrategyRegistry()
    register_seed_strategy_package(
        registry,
        underlying_family=args.underlying_family,
        spot_inst_id=args.inst_id,
        signal_bar=args.signal_bar,
    )
    runtime_config_path = resolve_runtime_config_path(args)
    loaded_runtime_config, had_saved_strategy_config = apply_saved_runtime_config(
        registry,
        parent_strategy_id=parent_strategy_id,
        runtime_config_path=runtime_config_path,
        ignore_runtime_config=args.ignore_runtime_config,
    )
    runtime_overrides = apply_runtime_overrides(registry, args)
    saved_runtime_config, effective_runtime_config = persist_runtime_config(
        registry,
        parent_strategy_id=parent_strategy_id,
        runtime_config_path=runtime_config_path,
        save_runtime_config=args.save_runtime_config,
        ignore_runtime_config=args.ignore_runtime_config,
        had_saved_strategy_config=had_saved_strategy_config,
    )
    result = EnhancedBacktestLab(registry).run(
        parent_strategy_id=parent_strategy_id,
        candle_feeds={
            ("SPOT", args.inst_id): candles,
        },
        base_bar=args.base_bar,
        quota_snapshots={
            args.underlying_family: QuotaSnapshot(
                underlying_family=args.underlying_family,
                long_limit_total=Decimal(args.long_limit),
                short_limit_total=Decimal(args.short_limit),
                protected_long_quota_total=Decimal(args.protected_long),
                protected_short_quota_total=Decimal(args.protected_short),
            )
        },
        simulation_config=simulation,
    )
    playbook_actions = {
        item.playbook_id: item.action
        for signal in registry.list_child_signals(parent_strategy_id, enabled_only=False)
        for item in registry.list_playbooks_for_signal(signal.signal_id, enabled_only=False)
    }
    latest_reference_candles = {}
    if candles:
        latest_reference_candles[("SPOT", args.inst_id)] = candles[-1]
    position_ledger_rows = build_active_position_ledger_rows(
        result=result,
        playbook_actions=playbook_actions,
        latest_reference_candles=latest_reference_candles,
    )
    target_dir = analysis_report_dir_path()
    target_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"enhanced_seed_lab_realdata_{args.inst_id.replace('-', '_')}_{args.base_bar}_{timestamp}"
    evidence_chart_bundle = export_evidence_chart_bundle(
        result=result,
        target_dir=target_dir / f"{stem}.evidence_charts",
        latest_reference_candles=latest_reference_candles,
    )
    chart_paths_by_evidence = evidence_chart_bundle["chart_paths_by_evidence"]
    for row in position_ledger_rows:
        row["chart_path"] = chart_paths_by_evidence.get(str(row["evidence_id"]), "")
    position_ledger_summary_rows = build_active_position_summary_rows(position_ledger_rows)
    total_position_summary_rows = build_total_position_summary_rows(
        position_ledger_summary_rows,
        bucket_mode=args.total_bucket_mode,
    )
    total_bucket_mode_label = TOTAL_BUCKET_MODE_LABELS[args.total_bucket_mode]
    manual_review_rows = build_manual_review_rows(
        position_ledger_rows,
        summary_rows=position_ledger_summary_rows,
    )
    manual_position_count = sum(1 for item in position_ledger_rows if item["manual_pool"] == "manual")
    high_priority_group_count = sum(
        1 for item in position_ledger_summary_rows if int(item.get("risk_priority_rank", 99)) <= 2
    )
    total_position_bucket_count = len(total_position_summary_rows)
    report_path = target_dir / f"{stem}.md"
    exported = export_lab_report_markdown(result, report_path)

    markdown_body = format_lab_report_markdown(result)
    header_lines = [
        "# 现货增强三十六计真实数据验收报告",
        "",
        f"- 标的：`{args.inst_id}`",
        f"- 标的家族：`{args.underlying_family}`",
        f"- 基础周期：`{args.base_bar}`",
        f"- 子策略周期：`{args.signal_bar}`",
        f"- 起始时间：`{start_dt.strftime('%Y-%m-%d %H:%M %Z')}`",
        f"- 结束时间：`{end_dt.strftime('%Y-%m-%d %H:%M %Z')}`",
        f"- 实际 K 线数量：`{len(candles)}`",
        f"- 数据说明：`{data_source_note}`",
        f"- 额度假设：`long={args.long_limit}, short={args.short_limit}, protected_long={args.protected_long}, protected_short={args.protected_short}`",
        f"- 期末活动仓位：`{len(position_ledger_rows)}`",
        f"- 其中人工接管仓：`{manual_position_count}`",
        f"- 常驻配置文件：`{runtime_config_path}`",
        f"- 证据图数量：`{len(evidence_chart_bundle['manifest_rows'])}`",
        f"- 人工接管专用图卡：`{len(manual_review_rows)}`",
        f"- 高优先级分组：`{high_priority_group_count}`",
        f"- 总持仓管理桶：`{total_position_bucket_count}`",
        f"- 总持仓合并模式：`{args.total_bucket_mode}` / `{total_bucket_mode_label}`",
        "",
        "## 说明",
        "- 本报告使用真实历史数据，并且改成了分段拉数，便于稳定跑更长区间。",
        "- 当前实验口径已经支持手续费、滑点、资金费、止盈止损、超时盈利平仓、止损触发人工接管。",
        "- `handoff_manual` 会保留额度占用，用来逼近实际人工兜底流程。",
        "- 新增期末持仓管理台账，便于按子策略、方向、成本和证据进行人工接管处理。",
        f"- 活动仓现已给出三层参考线：`净保本线`、`目标减仓线({TARGET_REDUCE_NET_PNL_PCT}%)`、`目标小赚线({TARGET_SMALL_PROFIT_NET_PNL_PCT}%)`。",
        "- 子策略启停与参数档现已支持常驻配置文件，便于长期保存与复现。",
        "- 新增证据图 SVG 与 HTML 画廊，方便人工按图扫单复核。",
        "- 新增人工接管专用画廊和按子策略分组的人工处理清单 HTML，方便你直接管理活动仓。",
        "- 新增总持仓总览页，把自动仓与人工接管仓按总成本统一汇总，便于整组处理。",
        "- 宽桶模式会自动按价格域兜底拆桶，避免把期权权利金与现货/永续价格直接混算。",
        "",
    ]
    exported["report"].write_text("\n".join(header_lines) + markdown_body, encoding="utf-8")

    summary_csv = write_summary_csv(target_dir / f"{stem}.summary.csv", result)
    profiles_csv = write_profiles_csv(target_dir / f"{stem}.profiles.csv", result)
    signal_states_csv = write_signal_states_csv(
        target_dir / f"{stem}.signal_states.csv",
        registry=registry,
        parent_strategy_id=parent_strategy_id,
        base_simulation=simulation,
    )
    events_csv = write_events_csv(target_dir / f"{stem}.events.csv", result)
    evidence_index_csv = write_evidence_index_csv(
        target_dir / f"{stem}.evidence_index.csv",
        result,
        chart_paths_by_evidence=chart_paths_by_evidence,
    )
    evidences_json = write_evidences_json(target_dir / f"{stem}.evidences.json", result)
    evidence_chart_manifest_csv = write_evidence_chart_manifest_csv(
        target_dir / f"{stem}.evidence_chart_manifest.csv",
        evidence_chart_bundle["manifest_rows"],
    )
    manual_review_manifest_csv = write_manual_review_manifest_csv(
        target_dir / f"{stem}.manual_review_manifest.csv",
        manual_review_rows,
    )
    manual_review_gallery_html = write_manual_review_gallery_html(
        target_dir / f"{stem}.manual_review_gallery.html",
        parent_strategy_name=result.parent_strategy_name,
        rows=manual_review_rows,
    )
    position_management_html = write_position_management_html(
        target_dir / f"{stem}.position_management.html",
        parent_strategy_name=result.parent_strategy_name,
        summary_rows=position_ledger_summary_rows,
        ledger_rows=manual_review_rows,
    )
    total_position_management_html = write_total_position_management_html(
        target_dir / f"{stem}.total_position_management.html",
        parent_strategy_name=result.parent_strategy_name,
        total_summary_rows=total_position_summary_rows,
        summary_rows=position_ledger_summary_rows,
        group_detail_path=target_dir / f"{stem}.position_management.html",
    )
    runtime_config_effective_json = write_runtime_config_json(
        target_dir / f"{stem}.runtime_config.effective.json",
        effective_runtime_config,
    )
    position_ledger_csv = write_position_ledger_csv(target_dir / f"{stem}.position_ledger.csv", position_ledger_rows)
    position_ledger_summary_csv = write_position_ledger_summary_csv(
        target_dir / f"{stem}.position_ledger_summary.csv",
        position_ledger_summary_rows,
    )
    total_position_summary_csv = write_total_position_summary_csv(
        target_dir / f"{stem}.total_position_summary.csv",
        total_position_summary_rows,
    )
    meta_json = target_dir / f"{stem}.meta.json"
    meta_payload = {
        "inst_id": args.inst_id,
        "underlying_family": args.underlying_family,
        "base_bar": args.base_bar,
        "signal_bar": args.signal_bar,
        "start_time": start_dt.isoformat(),
        "end_time": end_dt.isoformat(),
        "candle_count": len(candles),
        "data_source_note": data_source_note,
        "segment_notes": segment_notes,
        "assumptions": {
            "long_limit": args.long_limit,
            "short_limit": args.short_limit,
            "protected_long": args.protected_long,
            "protected_short": args.protected_short,
            "simulation": serialize_simulation_config(simulation),
            "loaded_runtime_config": loaded_runtime_config,
            "runtime_overrides": runtime_overrides,
            "saved_runtime_config": saved_runtime_config,
            "active_position_count": len(position_ledger_rows),
            "manual_position_count": manual_position_count,
            "evidence_chart_count": len(evidence_chart_bundle["manifest_rows"]),
            "manual_review_count": len(manual_review_rows),
            "high_priority_group_count": high_priority_group_count,
            "total_position_bucket_count": total_position_bucket_count,
            "total_bucket_mode": args.total_bucket_mode,
            "total_bucket_mode_label": total_bucket_mode_label,
            "target_reduce_net_pnl_pct": str(TARGET_REDUCE_NET_PNL_PCT),
            "target_small_profit_net_pnl_pct": str(TARGET_SMALL_PROFIT_NET_PNL_PCT),
        },
        "files": {
            "report": str(exported["report"]),
            "json": str(exported["json"]),
            "summary_csv": str(summary_csv),
            "profiles_csv": str(profiles_csv),
            "signal_states_csv": str(signal_states_csv),
            "events_csv": str(events_csv),
            "evidence_index_csv": str(evidence_index_csv),
            "evidences_json": str(evidences_json),
            "evidence_chart_manifest_csv": str(evidence_chart_manifest_csv),
            "evidence_chart_gallery_html": str(evidence_chart_bundle["gallery_html"]),
            "evidence_chart_dir": str(evidence_chart_bundle["chart_dir"]),
            "manual_review_manifest_csv": str(manual_review_manifest_csv),
            "manual_review_gallery_html": str(manual_review_gallery_html),
            "position_management_html": str(position_management_html),
            "total_position_management_html": str(total_position_management_html),
            "runtime_config_effective_json": str(runtime_config_effective_json),
            "runtime_config_file": str(runtime_config_path),
            "position_ledger_csv": str(position_ledger_csv),
            "position_ledger_summary_csv": str(position_ledger_summary_csv),
            "total_position_summary_csv": str(total_position_summary_csv),
        },
    }
    meta_json.write_text(json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(exported["report"])
    print(exported["json"])
    print(summary_csv)
    print(profiles_csv)
    print(signal_states_csv)
    print(events_csv)
    print(evidence_index_csv)
    print(evidences_json)
    print(evidence_chart_manifest_csv)
    print(evidence_chart_bundle["gallery_html"])
    print(manual_review_manifest_csv)
    print(manual_review_gallery_html)
    print(position_management_html)
    print(total_position_management_html)
    print(runtime_config_effective_json)
    print(position_ledger_csv)
    print(position_ledger_summary_csv)
    print(total_position_summary_csv)
    print(meta_json)


if __name__ == "__main__":
    main()
