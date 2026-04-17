from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Mapping

from okx_quant.enhanced_models import EnhancedBacktestLabResult, LabSimulationConfig
from okx_quant.models import Candle


ZERO = Decimal("0")
EIGHT_HOURS_MS = Decimal("28800000")
TARGET_REDUCE_NET_PNL_PCT = Decimal("0.5")
TARGET_SMALL_PROFIT_NET_PNL_PCT = Decimal("1.0")
TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE = "playbook_direction_source"
TOTAL_BUCKET_MODE_PLAYBOOK = "playbook_direction"
TOTAL_BUCKET_MODE_UNDERLYING = "underlying_direction"
TOTAL_BUCKET_MODE_CHOICES = (
    TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE,
    TOTAL_BUCKET_MODE_PLAYBOOK,
    TOTAL_BUCKET_MODE_UNDERLYING,
)
TOTAL_BUCKET_MODE_DEFAULT = TOTAL_BUCKET_MODE_UNDERLYING
TOTAL_BUCKET_MODE_LABELS = {
    TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE: "按动作+方向+来源",
    TOTAL_BUCKET_MODE_PLAYBOOK: "按动作+方向",
    TOTAL_BUCKET_MODE_UNDERLYING: "按标的家族+方向",
}
LONG_ACTIONS = {"SPOT_BUY", "SWAP_LONG", "OPTION_LONG_CALL", "OPTION_LONG_PUT"}
SHORT_ACTIONS = {"SPOT_SELL", "SWAP_SHORT", "OPTION_SHORT_CALL", "OPTION_SHORT_PUT"}
ACTIVE_LIFECYCLE_TO_POOL = {
    "handoff_manual": "manual",
    "open_unresolved": "auto",
}


def build_active_position_ledger_rows(
    *,
    result: EnhancedBacktestLabResult,
    playbook_actions: Mapping[str, str],
    latest_reference_candles: Mapping[tuple[str, str], Candle],
) -> list[dict[str, object]]:
    evidence_by_id = {item.evidence_id: item for item in result.evidences}
    simulation_by_signal = {
        item.signal_id: item.applied_simulation_config
        for item in result.summaries
    }
    rows: list[dict[str, object]] = []
    for event in result.events:
        manual_pool = ACTIVE_LIFECYCLE_TO_POOL.get(event.lifecycle_status)
        if manual_pool is None or not event.accepted:
            continue

        evidence = evidence_by_id.get(event.evidence_id)
        if evidence is None:
            continue

        action = playbook_actions.get(event.playbook_id, "")
        if action not in LONG_ACTIONS and action not in SHORT_ACTIONS:
            continue

        simulation = simulation_by_signal.get(event.signal_id, result.simulation_config)
        reference_candle = latest_reference_candles.get((evidence.source_market, evidence.source_inst_id))
        rows.append(
            _build_ledger_row(
                result=result,
                event=event,
                evidence=evidence,
                simulation=simulation,
                playbook_action=action,
                manual_pool=manual_pool,
                reference_candle=reference_candle,
            )
        )

    rows.sort(
        key=lambda item: (
            str(item["manual_pool"]),
            str(item["underlying_family"]),
            str(item["signal_id"]),
            int(item["entry_ts"]),
            str(item["ledger_id"]),
        )
    )
    return rows


def build_active_position_summary_rows(ledger_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], dict[str, object]] = {}
    weighted_entry_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    weighted_break_even_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    weighted_reference_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    reference_line_capacity_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    reference_line_constant_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)

    for row in ledger_rows:
        key = (
            row["parent_strategy_id"],
            row["parent_strategy_name"],
            row["manual_pool"],
            row["underlying_family"],
            row["source_market"],
            row["source_inst_id"],
            row["source_bar"],
            row["playbook_id"],
            row["playbook_name"],
            row["playbook_action"],
            row["position_side"],
            row["signal_id"],
            row["signal_name"],
        )
        size = _decimal(row["position_size"])
        entry_price = _decimal(row["entry_price"])
        latest_reference_price = _decimal(row["latest_reference_price"])
        break_even_price = _optional_decimal(row["break_even_reference_price"])
        reference_line_capacity_value = _optional_decimal(row.get("reference_line_capacity_value"))
        reference_line_constant_value = _optional_decimal(row.get("reference_line_constant_value"))
        entry_ts = int(row["entry_ts"])
        current = grouped.get(key)
        if current is None:
            current = {
                "parent_strategy_id": row["parent_strategy_id"],
                "parent_strategy_name": row["parent_strategy_name"],
                "manual_pool": row["manual_pool"],
                "underlying_family": row["underlying_family"],
                "source_market": row["source_market"],
                "source_inst_id": row["source_inst_id"],
                "source_bar": row["source_bar"],
                "playbook_id": row["playbook_id"],
                "playbook_name": row["playbook_name"],
                "playbook_action": row["playbook_action"],
                "position_side": row["position_side"],
                "signal_id": row["signal_id"],
                "signal_name": row["signal_name"],
                "active_position_count": 0,
                "total_position_size": ZERO,
                "total_position_cost_value": ZERO,
                "estimated_gross_pnl_value_if_closed_now": ZERO,
                "entry_fee_cost_value": ZERO,
                "estimated_exit_fee_cost_value_if_closed_now": ZERO,
                "estimated_fee_cost_value_if_closed_now": ZERO,
                "entry_slippage_cost_value": ZERO,
                "estimated_exit_slippage_cost_value_if_closed_now": ZERO,
                "estimated_slippage_cost_value_if_closed_now": ZERO,
                "estimated_funding_cost_value_to_reference": ZERO,
                "estimated_net_pnl_value_if_closed_now": ZERO,
                "reference_ts": row["reference_ts"],
                "first_entry_ts": entry_ts,
                "last_entry_ts": entry_ts,
                "evidence_ids": [],
            }
            grouped[key] = current

        current["active_position_count"] = int(current["active_position_count"]) + 1
        current["total_position_size"] = _decimal(current["total_position_size"]) + size
        current["total_position_cost_value"] = _decimal(current["total_position_cost_value"]) + _decimal(
            row["position_cost_value"]
        )
        current["estimated_gross_pnl_value_if_closed_now"] = _decimal(
            current["estimated_gross_pnl_value_if_closed_now"]
        ) + _decimal(row["estimated_gross_pnl_value_if_closed_now"])
        current["entry_fee_cost_value"] = _decimal(current["entry_fee_cost_value"]) + _decimal(
            row["entry_fee_cost_value"]
        )
        current["estimated_exit_fee_cost_value_if_closed_now"] = _decimal(
            current["estimated_exit_fee_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_exit_fee_cost_value_if_closed_now"])
        current["estimated_fee_cost_value_if_closed_now"] = _decimal(
            current["estimated_fee_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_fee_cost_value_if_closed_now"])
        current["entry_slippage_cost_value"] = _decimal(current["entry_slippage_cost_value"]) + _decimal(
            row["entry_slippage_cost_value"]
        )
        current["estimated_exit_slippage_cost_value_if_closed_now"] = _decimal(
            current["estimated_exit_slippage_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_exit_slippage_cost_value_if_closed_now"])
        current["estimated_slippage_cost_value_if_closed_now"] = _decimal(
            current["estimated_slippage_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_slippage_cost_value_if_closed_now"])
        current["estimated_funding_cost_value_to_reference"] = _decimal(
            current["estimated_funding_cost_value_to_reference"]
        ) + _decimal(row["estimated_funding_cost_value_to_reference"])
        current["estimated_net_pnl_value_if_closed_now"] = _decimal(
            current["estimated_net_pnl_value_if_closed_now"]
        ) + _decimal(row["estimated_net_pnl_value_if_closed_now"])
        current["reference_ts"] = max(int(current["reference_ts"]), int(row["reference_ts"]))
        current["first_entry_ts"] = min(int(current["first_entry_ts"]), entry_ts)
        current["last_entry_ts"] = max(int(current["last_entry_ts"]), entry_ts)
        current["evidence_ids"].append(str(row["evidence_id"]))

        weighted_entry_numerator[key] += entry_price * size
        weighted_reference_numerator[key] += latest_reference_price * size
        if break_even_price is not None:
            weighted_break_even_numerator[key] += break_even_price * size
        if reference_line_capacity_value is not None and reference_line_constant_value is not None:
            reference_line_capacity_numerator[key] += reference_line_capacity_value
            reference_line_constant_numerator[key] += reference_line_constant_value

    summary_rows: list[dict[str, object]] = []
    for key, item in grouped.items():
        total_size = _decimal(item["total_position_size"])
        weighted_avg_entry = ZERO if total_size == 0 else weighted_entry_numerator[key] / total_size
        weighted_reference = ZERO if total_size == 0 else weighted_reference_numerator[key] / total_size
        weighted_break_even = None
        if total_size > 0 and weighted_break_even_numerator[key] != ZERO:
            weighted_break_even = weighted_break_even_numerator[key] / total_size
        estimated_net = _decimal(item["estimated_net_pnl_value_if_closed_now"])
        total_cost = _decimal(item["total_position_cost_value"])
        estimated_net_pct = ZERO if total_cost == 0 else (estimated_net / total_cost) * Decimal("100")
        reference_line_capacity_value = reference_line_capacity_numerator[key]
        reference_line_constant_value = reference_line_constant_numerator[key]
        net_break_even_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=ZERO,
        )
        target_reduce_net_pnl_value = (total_cost * TARGET_REDUCE_NET_PNL_PCT) / Decimal("100")
        target_small_profit_net_pnl_value = (total_cost * TARGET_SMALL_PROFIT_NET_PNL_PCT) / Decimal("100")
        target_reduce_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=target_reduce_net_pnl_value,
        )
        target_small_profit_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=target_small_profit_net_pnl_value,
        )
        unified_close_reference_price = net_break_even_reference_price
        buffer_to_unified_close_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=unified_close_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_unified_close_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=unified_close_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_reduce_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=target_reduce_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_reduce_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=target_reduce_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_small_profit_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=target_small_profit_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_small_profit_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=target_small_profit_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        (
            risk_priority_rank,
            risk_priority_label,
            risk_priority_note,
            suggested_action,
        ) = _classify_group_priority(
            manual_pool=str(item["manual_pool"]),
            estimated_net_value=estimated_net,
            buffer_to_unified_close_pct=buffer_to_unified_close_pct,
            buffer_to_target_reduce_pct=buffer_to_target_reduce_pct,
            buffer_to_target_small_profit_pct=buffer_to_target_small_profit_pct,
        )
        summary_rows.append(
            {
                **item,
                "weighted_avg_entry_price": weighted_avg_entry,
                "latest_reference_price": weighted_reference,
                "weighted_break_even_reference_price": weighted_break_even,
                "reference_line_capacity_value": reference_line_capacity_value,
                "reference_line_constant_value": reference_line_constant_value,
                "net_break_even_reference_price": net_break_even_reference_price,
                "unified_close_reference_price": unified_close_reference_price,
                "buffer_to_unified_close_pct": buffer_to_unified_close_pct,
                "buffer_to_unified_close_value": buffer_to_unified_close_value,
                "target_reduce_net_pnl_pct": TARGET_REDUCE_NET_PNL_PCT,
                "target_reduce_net_pnl_value": target_reduce_net_pnl_value,
                "target_reduce_reference_price": target_reduce_reference_price,
                "buffer_to_target_reduce_pct": buffer_to_target_reduce_pct,
                "buffer_to_target_reduce_value": buffer_to_target_reduce_value,
                "target_small_profit_net_pnl_pct": TARGET_SMALL_PROFIT_NET_PNL_PCT,
                "target_small_profit_net_pnl_value": target_small_profit_net_pnl_value,
                "target_small_profit_reference_price": target_small_profit_reference_price,
                "buffer_to_target_small_profit_pct": buffer_to_target_small_profit_pct,
                "buffer_to_target_small_profit_value": buffer_to_target_small_profit_value,
                "risk_priority_rank": risk_priority_rank,
                "risk_priority_label": risk_priority_label,
                "risk_priority_note": risk_priority_note,
                "suggested_action": suggested_action,
                "estimated_net_pnl_pct_if_closed_now": estimated_net_pct,
                "evidence_ids": "; ".join(item["evidence_ids"]),
            }
        )

    summary_rows.sort(key=_summary_sort_key)
    return summary_rows


def build_total_position_summary_rows(
    summary_rows: list[dict[str, object]],
    *,
    bucket_mode: str = TOTAL_BUCKET_MODE_DEFAULT,
) -> list[dict[str, object]]:
    _validate_total_bucket_mode(bucket_mode)
    grouped: dict[tuple[object, ...], dict[str, object]] = {}
    weighted_entry_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    weighted_break_even_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)
    weighted_reference_numerator: defaultdict[tuple[object, ...], Decimal] = defaultdict(lambda: ZERO)

    for row in summary_rows:
        key = _build_total_bucket_key(row, bucket_mode=bucket_mode)
        size = _decimal(row["total_position_size"])
        active_position_count = int(row.get("active_position_count", 0))
        manual_pool = str(row.get("manual_pool", ""))
        playbook_action = str(row.get("playbook_action", ""))
        signal_id = str(row.get("signal_id", ""))
        signal_name = str(row.get("signal_name", ""))
        playbook_id = str(row.get("playbook_id", ""))
        playbook_name = str(row.get("playbook_name", ""))
        source_market = str(row.get("source_market", ""))
        source_inst_id = str(row.get("source_inst_id", ""))
        source_bar = str(row.get("source_bar", ""))
        source_label = _build_total_bucket_source_label(row)
        price_domain_key = _build_total_bucket_price_domain_key(row)
        price_domain_label = _build_total_bucket_price_domain_label(row)
        current = grouped.get(key)
        if current is None:
            current = {
                "parent_strategy_id": row["parent_strategy_id"],
                "parent_strategy_name": row["parent_strategy_name"],
                "bucket_mode": bucket_mode,
                "bucket_mode_label": TOTAL_BUCKET_MODE_LABELS[bucket_mode],
                "bucket_key": _serialize_total_bucket_key(key),
                "underlying_family": row["underlying_family"],
                "source_market": "",
                "source_inst_id": "",
                "source_bar": "",
                "playbook_action": "",
                "position_side": row["position_side"],
                "price_domain_key": price_domain_key,
                "price_domain_label": price_domain_label,
                "summary_group_count": 0,
                "signal_coverage_count": 0,
                "active_position_count": 0,
                "manual_position_count": 0,
                "auto_position_count": 0,
                "manual_group_count": 0,
                "auto_group_count": 0,
                "total_position_size": ZERO,
                "total_position_cost_value": ZERO,
                "estimated_gross_pnl_value_if_closed_now": ZERO,
                "entry_fee_cost_value": ZERO,
                "estimated_exit_fee_cost_value_if_closed_now": ZERO,
                "estimated_fee_cost_value_if_closed_now": ZERO,
                "entry_slippage_cost_value": ZERO,
                "estimated_exit_slippage_cost_value_if_closed_now": ZERO,
                "estimated_slippage_cost_value_if_closed_now": ZERO,
                "estimated_funding_cost_value_to_reference": ZERO,
                "estimated_net_pnl_value_if_closed_now": ZERO,
                "reference_line_capacity_value": ZERO,
                "reference_line_constant_value": ZERO,
                "reference_ts": int(row.get("reference_ts", 0)),
                "first_entry_ts": int(row.get("first_entry_ts", 0)),
                "last_entry_ts": int(row.get("last_entry_ts", 0)),
                "_signal_ids": [],
                "_signal_names": [],
                "_playbook_actions": [],
                "_playbook_ids": [],
                "_playbook_names": [],
                "_source_markets": [],
                "_source_inst_ids": [],
                "_source_bars": [],
                "_source_labels": [],
            }
            grouped[key] = current

        current["summary_group_count"] = int(current["summary_group_count"]) + 1
        current["active_position_count"] = int(current["active_position_count"]) + active_position_count
        current["total_position_size"] = _decimal(current["total_position_size"]) + size
        current["total_position_cost_value"] = _decimal(current["total_position_cost_value"]) + _decimal(
            row["total_position_cost_value"]
        )
        current["estimated_gross_pnl_value_if_closed_now"] = _decimal(
            current["estimated_gross_pnl_value_if_closed_now"]
        ) + _decimal(row["estimated_gross_pnl_value_if_closed_now"])
        current["entry_fee_cost_value"] = _decimal(current["entry_fee_cost_value"]) + _decimal(
            row["entry_fee_cost_value"]
        )
        current["estimated_exit_fee_cost_value_if_closed_now"] = _decimal(
            current["estimated_exit_fee_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_exit_fee_cost_value_if_closed_now"])
        current["estimated_fee_cost_value_if_closed_now"] = _decimal(
            current["estimated_fee_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_fee_cost_value_if_closed_now"])
        current["entry_slippage_cost_value"] = _decimal(current["entry_slippage_cost_value"]) + _decimal(
            row["entry_slippage_cost_value"]
        )
        current["estimated_exit_slippage_cost_value_if_closed_now"] = _decimal(
            current["estimated_exit_slippage_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_exit_slippage_cost_value_if_closed_now"])
        current["estimated_slippage_cost_value_if_closed_now"] = _decimal(
            current["estimated_slippage_cost_value_if_closed_now"]
        ) + _decimal(row["estimated_slippage_cost_value_if_closed_now"])
        current["estimated_funding_cost_value_to_reference"] = _decimal(
            current["estimated_funding_cost_value_to_reference"]
        ) + _decimal(row["estimated_funding_cost_value_to_reference"])
        current["estimated_net_pnl_value_if_closed_now"] = _decimal(
            current["estimated_net_pnl_value_if_closed_now"]
        ) + _decimal(row["estimated_net_pnl_value_if_closed_now"])
        current["reference_line_capacity_value"] = _decimal(current["reference_line_capacity_value"]) + _decimal(
            row.get("reference_line_capacity_value", ZERO)
        )
        current["reference_line_constant_value"] = _decimal(current["reference_line_constant_value"]) + _decimal(
            row.get("reference_line_constant_value", ZERO)
        )
        current["reference_ts"] = max(int(current["reference_ts"]), int(row.get("reference_ts", 0)))
        first_entry_ts = int(row.get("first_entry_ts", 0))
        last_entry_ts = int(row.get("last_entry_ts", 0))
        current["first_entry_ts"] = (
            first_entry_ts
            if int(current["first_entry_ts"]) == 0
            else min(int(current["first_entry_ts"]), first_entry_ts)
        )
        current["last_entry_ts"] = max(int(current["last_entry_ts"]), last_entry_ts)

        if manual_pool == "manual":
            current["manual_position_count"] = int(current["manual_position_count"]) + active_position_count
            current["manual_group_count"] = int(current["manual_group_count"]) + 1
        else:
            current["auto_position_count"] = int(current["auto_position_count"]) + active_position_count
            current["auto_group_count"] = int(current["auto_group_count"]) + 1

        _append_unique_text(current["_signal_ids"], signal_id)
        _append_unique_text(current["_signal_names"], signal_name)
        _append_unique_text(current["_playbook_actions"], playbook_action)
        _append_unique_text(current["_playbook_ids"], playbook_id)
        _append_unique_text(current["_playbook_names"], playbook_name)
        _append_unique_text(current["_source_markets"], source_market)
        _append_unique_text(current["_source_inst_ids"], source_inst_id)
        _append_unique_text(current["_source_bars"], source_bar)
        _append_unique_text(current["_source_labels"], source_label)

        weighted_entry_numerator[key] += _decimal(row["weighted_avg_entry_price"]) * size
        weighted_reference_numerator[key] += _decimal(row["latest_reference_price"]) * size
        weighted_break_even_price = _optional_decimal(row.get("weighted_break_even_reference_price"))
        if weighted_break_even_price is not None:
            weighted_break_even_numerator[key] += weighted_break_even_price * size

    total_rows: list[dict[str, object]] = []
    for key, item in grouped.items():
        total_size = _decimal(item["total_position_size"])
        total_cost = _decimal(item["total_position_cost_value"])
        weighted_avg_entry = ZERO if total_size == 0 else weighted_entry_numerator[key] / total_size
        weighted_reference = ZERO if total_size == 0 else weighted_reference_numerator[key] / total_size
        weighted_break_even = None
        if total_size > 0 and weighted_break_even_numerator[key] != ZERO:
            weighted_break_even = weighted_break_even_numerator[key] / total_size

        estimated_net = _decimal(item["estimated_net_pnl_value_if_closed_now"])
        estimated_net_pct = ZERO if total_cost == 0 else (estimated_net / total_cost) * Decimal("100")
        reference_line_capacity_value = _decimal(item["reference_line_capacity_value"])
        reference_line_constant_value = _decimal(item["reference_line_constant_value"])
        net_break_even_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=ZERO,
        )
        target_reduce_net_pnl_value = (total_cost * TARGET_REDUCE_NET_PNL_PCT) / Decimal("100")
        target_small_profit_net_pnl_value = (total_cost * TARGET_SMALL_PROFIT_NET_PNL_PCT) / Decimal("100")
        target_reduce_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=target_reduce_net_pnl_value,
        )
        target_small_profit_reference_price = _solve_group_target_reference_price(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            position_side=str(item["position_side"]),
            target_net_pnl_value=target_small_profit_net_pnl_value,
        )
        buffer_to_unified_close_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=net_break_even_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_unified_close_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=net_break_even_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_reduce_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=target_reduce_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_reduce_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=target_reduce_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_small_profit_pct = _directional_buffer_pct(
            reference_price=weighted_reference,
            unified_close_reference_price=target_small_profit_reference_price,
            position_side=str(item["position_side"]),
        )
        buffer_to_target_small_profit_value = _directional_buffer_value(
            reference_price=weighted_reference,
            unified_close_reference_price=target_small_profit_reference_price,
            total_size=total_size,
            position_side=str(item["position_side"]),
        )
        has_manual_positions = int(item["manual_position_count"]) > 0
        has_auto_positions = int(item["auto_position_count"]) > 0
        management_pool = "manual" if has_manual_positions else "auto"
        if has_manual_positions and has_auto_positions:
            pool_state = "mixed"
        elif has_manual_positions:
            pool_state = "manual_only"
        else:
            pool_state = "auto_only"
        playbook_actions_text = "; ".join(item["_playbook_actions"])
        playbook_ids_text = "; ".join(item["_playbook_ids"])
        playbook_names_text = "; ".join(item["_playbook_names"])
        source_markets_text = "; ".join(item["_source_markets"])
        source_inst_ids_text = "; ".join(item["_source_inst_ids"])
        source_bars_text = "; ".join(item["_source_bars"])
        source_labels_text = "; ".join(item["_source_labels"])
        (
            risk_priority_rank,
            risk_priority_label,
            risk_priority_note,
            suggested_action,
        ) = _classify_group_priority(
            manual_pool=management_pool,
            estimated_net_value=estimated_net,
            buffer_to_unified_close_pct=buffer_to_unified_close_pct,
            buffer_to_target_reduce_pct=buffer_to_target_reduce_pct,
            buffer_to_target_small_profit_pct=buffer_to_target_small_profit_pct,
        )
        total_rows.append(
            {
                **item,
                "manual_pool": management_pool,
                "pool_state": pool_state,
                "signal_coverage_count": len(item["_signal_ids"]),
                "signal_ids": "; ".join(item["_signal_ids"]),
                "signal_names": "; ".join(item["_signal_names"]),
                "playbook_action_count": len(item["_playbook_actions"]),
                "playbook_actions": playbook_actions_text,
                "playbook_action": playbook_actions_text,
                "playbook_ids": playbook_ids_text,
                "playbook_names": playbook_names_text,
                "playbook_name": playbook_names_text,
                "source_count": len(item["_source_labels"]),
                "source_labels": source_labels_text,
                "source_market": source_markets_text,
                "source_inst_id": source_inst_ids_text,
                "source_bar": source_bars_text,
                "bucket_label": _build_total_bucket_label(
                    bucket_mode=bucket_mode,
                    playbook_actions_text=playbook_actions_text,
                    position_side=str(item["position_side"]),
                    underlying_family=str(item["underlying_family"]),
                    source_inst_ids_text=source_inst_ids_text,
                    source_bars_text=source_bars_text,
                    price_domain_label=str(item["price_domain_label"]),
                ),
                "weighted_avg_entry_price": weighted_avg_entry,
                "latest_reference_price": weighted_reference,
                "weighted_break_even_reference_price": weighted_break_even,
                "net_break_even_reference_price": net_break_even_reference_price,
                "unified_close_reference_price": net_break_even_reference_price,
                "buffer_to_unified_close_pct": buffer_to_unified_close_pct,
                "buffer_to_unified_close_value": buffer_to_unified_close_value,
                "target_reduce_net_pnl_pct": TARGET_REDUCE_NET_PNL_PCT,
                "target_reduce_net_pnl_value": target_reduce_net_pnl_value,
                "target_reduce_reference_price": target_reduce_reference_price,
                "buffer_to_target_reduce_pct": buffer_to_target_reduce_pct,
                "buffer_to_target_reduce_value": buffer_to_target_reduce_value,
                "target_small_profit_net_pnl_pct": TARGET_SMALL_PROFIT_NET_PNL_PCT,
                "target_small_profit_net_pnl_value": target_small_profit_net_pnl_value,
                "target_small_profit_reference_price": target_small_profit_reference_price,
                "buffer_to_target_small_profit_pct": buffer_to_target_small_profit_pct,
                "buffer_to_target_small_profit_value": buffer_to_target_small_profit_value,
                "risk_priority_rank": risk_priority_rank,
                "risk_priority_label": risk_priority_label,
                "risk_priority_note": risk_priority_note,
                "suggested_action": suggested_action,
                "estimated_net_pnl_pct_if_closed_now": estimated_net_pct,
            }
        )

    total_rows.sort(key=_summary_sort_key)
    return total_rows


def _validate_total_bucket_mode(bucket_mode: str) -> None:
    if bucket_mode not in TOTAL_BUCKET_MODE_CHOICES:
        raise ValueError(f"unsupported total bucket mode: {bucket_mode}")


def _build_total_bucket_key(row: dict[str, object], *, bucket_mode: str) -> tuple[object, ...]:
    parent_strategy_id = str(row.get("parent_strategy_id", ""))
    parent_strategy_name = str(row.get("parent_strategy_name", ""))
    position_side = str(row.get("position_side", ""))
    playbook_action = str(row.get("playbook_action", ""))
    source_market = str(row.get("source_market", ""))
    source_inst_id = str(row.get("source_inst_id", ""))
    source_bar = str(row.get("source_bar", ""))
    price_domain_key = _build_total_bucket_price_domain_key(row)
    if bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE:
        return (
            bucket_mode,
            parent_strategy_id,
            parent_strategy_name,
            playbook_action,
            position_side,
            source_market,
            source_inst_id,
            source_bar,
        )
    if bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK:
        return (
            bucket_mode,
            parent_strategy_id,
            parent_strategy_name,
            playbook_action,
            position_side,
            price_domain_key,
        )
    return (
        bucket_mode,
        parent_strategy_id,
        parent_strategy_name,
        position_side,
        price_domain_key,
    )


def _build_total_bucket_price_domain_key(row: dict[str, object]) -> str:
    action = str(row.get("playbook_action", ""))
    underlying_family = str(row.get("underlying_family", ""))
    source_market = str(row.get("source_market", ""))
    source_inst_id = str(row.get("source_inst_id", ""))
    if action in {"SPOT_BUY", "SPOT_SELL", "SWAP_LONG", "SWAP_SHORT"}:
        return f"underlying::{underlying_family}"
    return f"instrument::{source_market}::{source_inst_id}"


def _build_total_bucket_price_domain_label(row: dict[str, object]) -> str:
    action = str(row.get("playbook_action", ""))
    if action in {"SPOT_BUY", "SPOT_SELL", "SWAP_LONG", "SWAP_SHORT"}:
        return str(row.get("underlying_family", ""))
    return str(row.get("source_inst_id", ""))


def _build_total_bucket_source_label(row: dict[str, object]) -> str:
    return " / ".join(
        part
        for part in (
            str(row.get("source_market", "")),
            str(row.get("source_inst_id", "")),
            str(row.get("source_bar", "")),
        )
        if part
    )


def _build_total_bucket_label(
    *,
    bucket_mode: str,
    playbook_actions_text: str,
    position_side: str,
    underlying_family: str,
    source_inst_ids_text: str,
    source_bars_text: str,
    price_domain_label: str,
) -> str:
    if bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK_SOURCE:
        return " / ".join(
            part
            for part in (
                playbook_actions_text or "-",
                position_side or "-",
                source_inst_ids_text or price_domain_label or "-",
                source_bars_text or "-",
            )
            if part
        )
    if bucket_mode == TOTAL_BUCKET_MODE_PLAYBOOK:
        return " / ".join(
            part
            for part in (
                playbook_actions_text or "-",
                position_side or "-",
                price_domain_label or underlying_family or "-",
            )
            if part
        )
    return " / ".join(
        part
        for part in (
            underlying_family or price_domain_label or "-",
            position_side or "-",
        )
        if part
    )


def _serialize_total_bucket_key(key: tuple[object, ...]) -> str:
    return " | ".join(str(part) for part in key)


def _append_unique_text(values: list[str], value: str) -> None:
    normalized = value.strip()
    if normalized and normalized not in values:
        values.append(normalized)


def _build_ledger_row(
    *,
    result: EnhancedBacktestLabResult,
    event,
    evidence,
    simulation: LabSimulationConfig,
    playbook_action: str,
    manual_pool: str,
    reference_candle: Candle | None,
) -> dict[str, object]:
    is_long = _is_long_action(playbook_action)
    entry_price = _optional_decimal(event.entry_price)
    entry_price = event.signal_price if entry_price is None else entry_price
    position_size = _decimal(event.position_size)
    signal_price = _decimal(event.signal_price)
    position_cost_value = entry_price * position_size
    entry_fee_value = abs(entry_price) * simulation.fee_rate * position_size
    entry_slippage_value = abs(entry_price - signal_price) * position_size

    reference_ts = event.exit_ts if event.exit_ts is not None else event.candle_ts
    latest_reference_price = None
    estimated_exit_price = None
    estimated_gross_value = ZERO
    estimated_exit_fee_value = ZERO
    estimated_exit_slippage_value = ZERO
    estimated_funding_value = ZERO
    estimated_net_value = ZERO
    estimated_net_pct = ZERO
    break_even_reference_price = None
    reference_line_capacity_value = None
    reference_line_constant_value = None

    if reference_candle is not None:
        reference_ts = reference_candle.ts
        latest_reference_price = reference_candle.close
        estimated_exit_price = _apply_slippage_price(
            reference_candle.close,
            is_long=is_long,
            slippage_rate=simulation.slippage_rate,
            is_entry=False,
        )
        gross_points = _directional_points(entry_price, estimated_exit_price, is_long=is_long)
        estimated_gross_value = gross_points * position_size
        estimated_exit_fee_value = abs(estimated_exit_price) * simulation.fee_rate * position_size
        estimated_exit_slippage_value = abs(estimated_exit_price - reference_candle.close) * position_size
        estimated_funding_value = _estimate_funding_cost_value(
            entry_price=entry_price,
            position_size=position_size,
            funding_rate_per_8h=simulation.funding_rate_per_8h,
            entry_ts=event.candle_ts,
            reference_ts=reference_ts,
        )
        estimated_net_value = estimated_gross_value - entry_fee_value - estimated_exit_fee_value - estimated_funding_value
        estimated_net_pct = ZERO if entry_price == 0 else (estimated_net_value / position_cost_value) * Decimal("100")
        reference_line_capacity_value, reference_line_constant_value = _estimate_reference_line_components(
            entry_price=entry_price,
            fee_rate=simulation.fee_rate,
            slippage_rate=simulation.slippage_rate,
            funding_cost_value=estimated_funding_value,
            position_size=position_size,
            is_long=is_long,
        )
        break_even_reference_price = _solve_reference_price_from_components(
            reference_line_capacity_value=reference_line_capacity_value,
            reference_line_constant_value=reference_line_constant_value,
            is_long=is_long,
            target_net_pnl_value=ZERO,
        )

    quota_allocations = "; ".join(f"{source}={quantity}" for source, quantity in event.quota_allocations)
    total_bars = _estimate_bar_count(event.candle_ts, reference_ts, evidence.source_bar)
    bars_in_manual_pool = 0
    if manual_pool == "manual" and event.exit_ts is not None:
        bars_in_manual_pool = _estimate_bar_count(event.exit_ts, reference_ts, evidence.source_bar)

    return {
        "ledger_id": event.evidence_id,
        "parent_strategy_id": result.parent_strategy_id,
        "parent_strategy_name": result.parent_strategy_name,
        "manual_pool": manual_pool,
        "lifecycle_status": event.lifecycle_status,
        "signal_id": event.signal_id,
        "signal_name": event.signal_name,
        "playbook_id": event.playbook_id,
        "playbook_name": event.playbook_name,
        "playbook_action": playbook_action,
        "position_side": "long" if is_long else "short",
        "underlying_family": event.underlying_family,
        "source_market": evidence.source_market,
        "source_inst_id": evidence.source_inst_id,
        "source_bar": evidence.source_bar,
        "entry_ts": event.candle_ts,
        "handoff_ts": event.exit_ts,
        "reference_ts": reference_ts,
        "signal_price": signal_price,
        "entry_price": entry_price,
        "position_size": position_size,
        "position_cost_value": position_cost_value,
        "stop_loss_price": event.stop_loss_price,
        "take_profit_price": event.take_profit_price,
        "latest_reference_price": latest_reference_price,
        "estimated_exit_price_if_closed_now": estimated_exit_price,
        "break_even_reference_price": break_even_reference_price,
        "fee_rate": simulation.fee_rate,
        "slippage_rate": simulation.slippage_rate,
        "funding_rate_per_8h": simulation.funding_rate_per_8h,
        "reference_line_capacity_value": reference_line_capacity_value,
        "reference_line_constant_value": reference_line_constant_value,
        "estimated_gross_pnl_value_if_closed_now": estimated_gross_value,
        "entry_fee_cost_value": entry_fee_value,
        "estimated_exit_fee_cost_value_if_closed_now": estimated_exit_fee_value,
        "estimated_fee_cost_value_if_closed_now": entry_fee_value + estimated_exit_fee_value,
        "entry_slippage_cost_value": entry_slippage_value,
        "estimated_exit_slippage_cost_value_if_closed_now": estimated_exit_slippage_value,
        "estimated_slippage_cost_value_if_closed_now": entry_slippage_value + estimated_exit_slippage_value,
        "estimated_funding_cost_value_to_reference": estimated_funding_value,
        "estimated_net_pnl_value_if_closed_now": estimated_net_value,
        "estimated_net_pnl_pct_if_closed_now": estimated_net_pct,
        "auto_holding_bars": event.holding_bars,
        "bars_in_manual_pool": bars_in_manual_pool,
        "bars_since_entry_to_reference": total_bars,
        "handoff_reason": event.exit_reason,
        "quota_allocations": quota_allocations,
        "trigger_reason": event.trigger_reason,
        "evidence_summary": event.evidence_summary,
        "evidence_id": event.evidence_id,
        "evidence_template_id": event.evidence_template_id,
        "setup_candle_count": len(evidence.setup_candles),
        "followthrough_candle_count": len(evidence.followthrough_candles),
        "evidence_note": evidence.note,
    }


def _estimate_funding_cost_value(
    *,
    entry_price: Decimal,
    position_size: Decimal,
    funding_rate_per_8h: Decimal,
    entry_ts: int,
    reference_ts: int,
) -> Decimal:
    if funding_rate_per_8h <= 0 or reference_ts <= entry_ts or entry_price <= 0 or position_size <= 0:
        return ZERO
    funding_periods = Decimal(str(reference_ts - entry_ts)) / EIGHT_HOURS_MS
    return abs(entry_price) * funding_rate_per_8h * funding_periods * position_size


def _estimate_reference_line_components(
    *,
    entry_price: Decimal,
    fee_rate: Decimal,
    slippage_rate: Decimal,
    funding_cost_value: Decimal,
    position_size: Decimal,
    is_long: bool,
) -> tuple[Decimal | None, Decimal | None]:
    if entry_price <= 0 or position_size <= 0:
        return None, None
    if is_long:
        reference_line_capacity_value = position_size * (Decimal("1") - slippage_rate) * (Decimal("1") - fee_rate)
        reference_line_constant_value = (entry_price * position_size * (Decimal("1") + fee_rate)) + funding_cost_value
        if reference_line_capacity_value <= 0:
            return None, None
        return reference_line_capacity_value, reference_line_constant_value
    reference_line_capacity_value = position_size * (Decimal("1") + slippage_rate) * (Decimal("1") + fee_rate)
    reference_line_constant_value = (entry_price * position_size * (Decimal("1") - fee_rate)) - funding_cost_value
    if reference_line_capacity_value <= 0 or reference_line_constant_value <= 0:
        return None, None
    return reference_line_capacity_value, reference_line_constant_value


def _solve_reference_price_from_components(
    *,
    reference_line_capacity_value: Decimal | None,
    reference_line_constant_value: Decimal | None,
    is_long: bool,
    target_net_pnl_value: Decimal,
) -> Decimal | None:
    if reference_line_capacity_value is None or reference_line_constant_value is None:
        return None
    if reference_line_capacity_value <= 0:
        return None
    if is_long:
        return (reference_line_constant_value + target_net_pnl_value) / reference_line_capacity_value
    numerator = reference_line_constant_value - target_net_pnl_value
    if numerator <= 0:
        return None
    return numerator / reference_line_capacity_value


def _solve_group_target_reference_price(
    *,
    reference_line_capacity_value: Decimal,
    reference_line_constant_value: Decimal,
    position_side: str,
    target_net_pnl_value: Decimal,
) -> Decimal | None:
    if reference_line_capacity_value <= 0:
        return None
    if position_side == "long":
        return (reference_line_constant_value + target_net_pnl_value) / reference_line_capacity_value
    if position_side == "short":
        numerator = reference_line_constant_value - target_net_pnl_value
        if numerator <= 0:
            return None
        return numerator / reference_line_capacity_value
    return None


def _estimate_bar_count(start_ts: int, end_ts: int, bar: str) -> int:
    bar_ms = _bar_to_ms(bar)
    if bar_ms <= 0 or end_ts <= start_ts:
        return 0
    return int((end_ts - start_ts) // bar_ms)


def _bar_to_ms(bar: str) -> int:
    raw = bar.strip()
    if len(raw) < 2:
        return 0
    try:
        size = int(raw[:-1])
    except ValueError:
        return 0
    unit = raw[-1].lower()
    unit_ms = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }.get(unit)
    if unit_ms is None:
        return 0
    return size * unit_ms


def _directional_points(entry_price: Decimal, exit_price: Decimal, *, is_long: bool) -> Decimal:
    return exit_price - entry_price if is_long else entry_price - exit_price


def _apply_slippage_price(
    price: Decimal,
    *,
    is_long: bool,
    slippage_rate: Decimal,
    is_entry: bool,
) -> Decimal:
    if price <= 0 or slippage_rate <= 0:
        return price
    if is_long:
        multiplier = Decimal("1") + slippage_rate if is_entry else Decimal("1") - slippage_rate
    else:
        multiplier = Decimal("1") - slippage_rate if is_entry else Decimal("1") + slippage_rate
    return price * multiplier


def _is_long_action(action: str) -> bool:
    if action in LONG_ACTIONS:
        return True
    if action in SHORT_ACTIONS:
        return False
    raise ValueError(f"unsupported playbook action for position ledger: {action}")


def _directional_buffer_pct(
    *,
    reference_price: Decimal,
    unified_close_reference_price: Decimal | None,
    position_side: str,
) -> Decimal | None:
    if unified_close_reference_price is None or unified_close_reference_price <= 0:
        return None
    if position_side == "long":
        return ((reference_price - unified_close_reference_price) / unified_close_reference_price) * Decimal("100")
    if position_side == "short":
        return ((unified_close_reference_price - reference_price) / unified_close_reference_price) * Decimal("100")
    return None


def _directional_buffer_value(
    *,
    reference_price: Decimal,
    unified_close_reference_price: Decimal | None,
    total_size: Decimal,
    position_side: str,
) -> Decimal | None:
    if unified_close_reference_price is None:
        return None
    if position_side == "long":
        return (reference_price - unified_close_reference_price) * total_size
    if position_side == "short":
        return (unified_close_reference_price - reference_price) * total_size
    return None


def _classify_group_priority(
    *,
    manual_pool: str,
    estimated_net_value: Decimal,
    buffer_to_unified_close_pct: Decimal | None,
    buffer_to_target_reduce_pct: Decimal | None,
    buffer_to_target_small_profit_pct: Decimal | None,
) -> tuple[int, str, str, str]:
    if buffer_to_unified_close_pct is None:
        return (
            9,
            "P9_缺参考线",
            "当前没有统一平仓参考线，先补足参考价再判断。",
            "等待参考价",
        )

    abs_buffer = abs(buffer_to_unified_close_pct)
    if manual_pool == "manual" and buffer_to_unified_close_pct < 0 and abs_buffer <= Decimal("1.5"):
        return (
            1,
            "P1_临近统一平仓线",
            "已经接近整组保本线，适合优先盯盘，随时准备统一处理。",
            "优先盯保本出",
        )
    if manual_pool == "manual" and buffer_to_unified_close_pct < 0:
        return (
            2,
            "P2_浮亏风险组",
            "当前仍在统一平仓线下方，属于需要重点防守的人工接管仓。",
            "重点控风险",
        )
    if buffer_to_target_small_profit_pct is not None and buffer_to_target_small_profit_pct >= 0:
        return (
            4,
            "P4_达到小赚区",
            "已经达到目标小赚线，可以优先考虑统一锁盈或择机整组处理。",
            "可择机统一锁盈",
        )
    if buffer_to_target_reduce_pct is not None and buffer_to_target_reduce_pct >= 0:
        return (
            5,
            "P5_达到减仓区",
            "已经越过目标减仓线，但尚未达到目标小赚线，适合分批减压。",
            "可分批减压",
        )
    if manual_pool == "manual" and buffer_to_unified_close_pct >= 0:
        return (
            3,
            "P3_盈利保护组",
            "已经回到净保本线上方，但还没到目标减仓线，适合先保护利润、观察回抽。",
            "优先保护利润",
        )
    if estimated_net_value >= 0:
        return (
            6,
            "P6_顺势观察组",
            "已经回到正收益区，但离目标减仓线还有距离，可以继续观察。",
            "继续观察",
        )
    return (
        7,
        "P7_远离平仓线",
        "距离净保本线仍有明显距离，短时间内不适合硬处理。",
        "等待回拉",
    )


def _summary_sort_key(item: dict[str, object]) -> tuple[object, ...]:
    rank = int(item.get("risk_priority_rank", 99))
    buffer_pct = item.get("buffer_to_unified_close_pct")
    buffer_abs = Decimal("999999") if buffer_pct is None else abs(_decimal(buffer_pct))
    reduce_buffer_pct = item.get("buffer_to_target_reduce_pct")
    reduce_buffer_abs = Decimal("999999") if reduce_buffer_pct is None else abs(_decimal(reduce_buffer_pct))
    small_profit_buffer_pct = item.get("buffer_to_target_small_profit_pct")
    small_profit_buffer_abs = (
        Decimal("999999") if small_profit_buffer_pct is None else abs(_decimal(small_profit_buffer_pct))
    )
    estimated_net = _decimal(item.get("estimated_net_pnl_value_if_closed_now", ZERO))
    if rank == 1:
        detail = buffer_abs
    elif rank == 2:
        detail = -buffer_abs
    elif rank == 3:
        detail = reduce_buffer_abs
    elif rank == 4:
        detail = -estimated_net
    elif rank == 5:
        detail = small_profit_buffer_abs
    elif rank == 6:
        detail = -estimated_net
    else:
        detail = int(item.get("first_entry_ts", 0))
    return (
        rank,
        str(item.get("manual_pool", "")),
        detail,
        str(item.get("signal_id", "")),
        int(item.get("first_entry_ts", 0)),
    )


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _optional_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    return _decimal(value)
