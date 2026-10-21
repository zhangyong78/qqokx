"""Fail-closed OKX execution primitives for area-shape plans.

Execution sessions bind one explicit demo or live runtime. The module keeps
execution records separate from local paper-plan state and stores no credentials.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from okx_quant.models import Instrument
from okx_quant.pricing import snap_to_increment
from okx_quant.client_order_id import with_custom_order_id_prefix


AREA_SHAPE_EXECUTION_RULE_VERSION = 2
_CL_ORD_ID_RE = re.compile(r"^[A-Za-z0-9-]{1,32}$")


def credential_fingerprint(api_key: str) -> str:
    """One-way key binding for restore checks; never persist the key itself."""
    if not api_key:
        raise ValueError("API key为空，不能冻结执行身份")
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _d(value: object, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"{label}规格无效") from exc
    if not result.is_finite():
        raise ValueError(f"{label}规格不是有限数")
    return result


def validate_area_shape_runtime(*, plan: Any, runtime: Any, unlocked: bool, environment: str) -> tuple[str, str]:
    """Require explicit unlock and exact profile/environment binding."""
    if not unlocked:
        raise PermissionError("必须先在全局账户栏解锁计划绑定的API profile")
    environment = str(environment).lower()
    if environment not in {"demo", "live"}:
        raise PermissionError("区域形态执行环境无效")
    if str(getattr(plan, "environment", "")).lower() != environment:
        raise PermissionError("计划冻结环境与执行环境不一致")
    if runtime is None or str(getattr(runtime, "environment", "")).lower() != environment:
        raise PermissionError("当前profile环境与计划冻结环境不一致")
    credentials = getattr(runtime, "credentials", None)
    runtime_profile = str(getattr(runtime, "credential_profile_name", "") or getattr(credentials, "profile_name", ""))
    if runtime_profile != str(plan.profile_name):
        raise PermissionError("运行时profile与计划冻结账户不一致")
    if str(getattr(credentials, "profile_name", "") or runtime_profile) != str(plan.profile_name):
        raise PermissionError("凭证profile与计划不一致")
    key_hash = credential_fingerprint(str(getattr(credentials, "api_key", "")))
    return runtime_profile, key_hash


def validate_demo_runtime(*, plan: Any, runtime: Any, unlocked: bool) -> tuple[str, str]:
    """Compatibility entry point that can never authorize live execution."""
    if str(getattr(plan, "environment", "")).lower() != "demo":
        raise PermissionError("demo执行入口只接受demo计划")
    return validate_area_shape_runtime(plan=plan, runtime=runtime, unlocked=unlocked, environment="demo")


def validate_usdt_linear_swap(instrument: Instrument, symbol: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    inst_id = str(instrument.inst_id).upper()
    expected_base = inst_id.removesuffix("-USDT-SWAP")
    if inst_id != str(symbol).upper() or not re.fullmatch(r"[A-Z0-9]+-USDT-SWAP", inst_id):
        raise ValueError("仅支持精确匹配的USDT线性永续合约")
    if str(instrument.inst_type).upper() != "SWAP" or str(instrument.state).lower() != "live":
        raise ValueError("合约类型或状态不支持")
    if str(instrument.settle_ccy or "").upper() != "USDT":
        raise ValueError("仅支持USDT结算的线性永续合约")
    if str(instrument.ct_val_ccy or "").upper() != expected_base:
        raise ValueError("合约ctVal币种与基础币不匹配")
    tick = _d(instrument.tick_size, "tickSz")
    lot = _d(instrument.lot_size, "lotSz")
    minimum = _d(instrument.min_size, "minSz")
    ct_val = _d(instrument.ct_val, "ctVal")
    ct_mult = _d(instrument.ct_mult, "ctMult")
    if min(tick, lot, minimum, ct_val, ct_mult) <= 0:
        raise ValueError("合约tick/lot/min/ctVal/ctMult缺失或非正数")
    return tick, lot, minimum, ct_val * ct_mult


def size_usdt_linear_contracts(
    *, risk_amount: Decimal, entry: Decimal, stop: Decimal, fee_rate: Decimal,
    slippage_rate: Decimal, lot_size: Decimal, min_size: Decimal, contract_multiplier: Decimal,
) -> tuple[Decimal, Decimal]:
    values = (risk_amount, entry, stop, fee_rate, slippage_rate, lot_size, min_size, contract_multiplier)
    if any(not value.is_finite() for value in values) or min(risk_amount, entry, stop, lot_size, min_size, contract_multiplier) <= 0:
        raise ValueError("风险、价格或合约参数无效")
    if entry == stop or fee_rate < 0 or slippage_rate < 0:
        raise ValueError("入场/止损距离及费用/滑点参数无效")
    if fee_rate + slippage_rate >= 1:
        raise ValueError("费用加滑点预算达到100%，拒绝定仓")
    # Price-difference PnL plus entry/stop taker fee and slippage budgets.
    risk_per_contract = abs(entry - stop) * contract_multiplier + (entry + stop) * contract_multiplier * (fee_rate + slippage_rate)
    raw_contracts = risk_amount / risk_per_contract
    contracts = (raw_contracts / lot_size).to_integral_value(rounding=ROUND_FLOOR) * lot_size
    if contracts < min_size:
        raise ValueError("按风险金额计算的张数低于minSz；未扩大风险预算")
    return contracts, risk_per_contract


def price_for_net_r(
    *, entry: Decimal, stop: Decimal, direction: str, target_r: Decimal,
    fee_rate: Decimal, slippage_rate: Decimal, contract_multiplier: Decimal,
    tick_size: Decimal,
) -> Decimal:
    values = (entry, stop, target_r, fee_rate, slippage_rate, contract_multiplier, tick_size)
    if any(not value.is_finite() for value in values):
        raise ValueError("目标价输入必须为有限数")
    if (direction not in {"long", "short"} or target_r < 0 or entry <= 0 or stop <= 0
            or contract_multiplier <= 0 or tick_size <= 0):
        raise ValueError("目标R或方向无效")
    total_cost_rate = fee_rate + slippage_rate
    if total_cost_rate < 0 or total_cost_rate >= 1:
        raise ValueError("费用与滑点比例不安全")
    r_distance = abs(entry - stop)
    if r_distance <= 0 or contract_multiplier <= 0:
        raise ValueError("初始R或合约乘数无效")
    if direction == "long":
        raw = (entry * (Decimal("1") + total_cost_rate) + target_r * r_distance) / (Decimal("1") - total_cost_rate)
    else:
        raw = (entry * (Decimal("1") - total_cost_rate) - target_r * r_distance) / (Decimal("1") + total_cost_rate)
    if raw <= 0:
        raise ValueError("目标R对应价格非正，拒绝生成止盈价")
    rounding = ROUND_CEILING if direction == "long" else ROUND_FLOOR
    return (raw / tick_size).to_integral_value(rounding=rounding) * tick_size


def new_execution_client_order_id(plan_id: str, signal_key: str, leg: str) -> str:
    """Stable deterministic id; same signal cannot mint a second id after crash."""
    digest = hashlib.sha256(f"{plan_id}|{signal_key}|{leg}".encode("utf-8")).hexdigest()[:20]
    value = with_custom_order_id_prefix(f"area{leg[:2]}{digest}")
    if not _CL_ORD_ID_RE.fullmatch(value):
        raise ValueError("生成的OKX客户端订单编号无效")
    return value


def build_area_shape_execution_session(
    *, plan: Any, runtime: Any, account_uid: str, instrument: Instrument,
    fee_rate: Decimal, slippage_rate: Decimal, baseline_candle_ts: int,
    confirmed_at_ms: int, unlocked: bool, environment: str,
) -> dict[str, Any]:
    environment = str(environment).lower()
    profile, key_hash = validate_area_shape_runtime(plan=plan, runtime=runtime, unlocked=unlocked, environment=environment)
    if str(plan.status) not in {"monitoring", "touched"}:
        raise ValueError("需先启动区域监控；不能把历史纸面待入场/持仓直接转换为demo仓位")
    if plan.paper_position is not None or str(plan.status).startswith("paper_"):
        raise ValueError("计划已有纸面持仓/待入场状态；需新建计划后才能启用demo执行")
    if int(baseline_candle_ts) <= 0 or int(confirmed_at_ms) <= 0:
        raise ValueError("缺少启用时的最新已收盘K线基线")
    validate_usdt_linear_swap(instrument, plan.symbol)
    if str(plan.config.trigger_price_type) != "last":
        raise ValueError("当前K线与保护触发仅使用last，其他触发价源拒绝")
    frozen_account_uid = str(account_uid or "").strip()
    if not frozen_account_uid:
        raise ValueError("OKX账户身份UID缺失，无法隔离同账户profile")
    trade_mode = str(getattr(runtime, "trade_mode", "") or "").lower()
    position_mode = str(getattr(runtime, "position_mode", "") or "").lower()
    if environment == "live" and (trade_mode not in {"cross", "isolated"}
                                   or position_mode not in {"net", "long_short"}):
        raise ValueError("live session必须冻结有效的trade_mode与position_mode")
    if not fee_rate.is_finite() or fee_rate < 0 or not slippage_rate.is_finite() or slippage_rate < 0:
        raise ValueError("有效taker成本或滑点预算缺失")
    # Store a complete frozen plan/config snapshot and signal baseline. Startup
    # never restores the write-enabled state; it always becomes manual review.
    return {
        "version": AREA_SHAPE_EXECUTION_RULE_VERSION,
        "mode": f"{environment}_exchange",
        "session_id": f"{plan.plan_id}-{secrets.token_hex(4)}",
        "plan_id": plan.plan_id,
        "plan_snapshot": plan.to_dict(),
        "profile_name": profile,
        "environment": environment,
        "account_uid": frozen_account_uid,
        "api_key_sha256": key_hash,
        "symbol": plan.symbol,
        "direction": plan.config.direction,
        "trade_mode": trade_mode,
        "position_mode": position_mode,
        "trigger_price_type": "last",
        "instrument": {
            "ct_val": str(instrument.ct_val), "ct_mult": str(instrument.ct_mult),
            "ct_val_ccy": instrument.ct_val_ccy, "settle_ccy": instrument.settle_ccy,
            "tick_size": str(instrument.tick_size), "lot_size": str(instrument.lot_size),
            "min_size": str(instrument.min_size),
        },
        "fee_rate": str(fee_rate),
        "slippage_rate": str(slippage_rate),
        "confirmed_at_ms": int(confirmed_at_ms),
        "signal_cursor_ts": int(baseline_candle_ts),
        "baseline_candle_ts": int(baseline_candle_ts),
        "expires_at_candle_ts": int(plan.expires_at_candle_ts),
        "last_minute_cursor_ts": 0,
        "favorable_price": "",
        "status": "armed",
        "rule_version": AREA_SHAPE_EXECUTION_RULE_VERSION,
        "resting_entry_intent": None,
        "pending_intent": None,
        "position": None,
        "events": [{"kind": "armed", "ts": int(confirmed_at_ms), "message": f"{environment}已启用；仅消费启用后的新信号，重启后必须人工重新启用"}],
    }


def build_demo_execution_session(**kwargs: Any) -> dict[str, Any]:
    """Compatibility entry point that cannot silently upgrade a demo plan."""
    if str(getattr(kwargs.get("plan"), "environment", "")).lower() != "demo":
        raise PermissionError("demo session builder只接受demo计划")
    return build_area_shape_execution_session(**kwargs, environment="demo")


def restore_execution_session_for_readonly(session: dict[str, Any]) -> dict[str, Any]:
    """A process restart is never permission to continue order writes."""
    restored = dict(session)
    expected_mode = f"{str(restored.get('environment') or 'demo').lower()}_exchange"
    if (restored.get("mode") not in {"demo_exchange", "live_exchange"}
            or restored.get("mode") != expected_mode
            or restored.get("version") != AREA_SHAPE_EXECUTION_RULE_VERSION):
        restored["status"] = "manual_review"
        restored["write_disabled"] = True
        return restored
    if restored.get("status") not in {"closed", "cancelled", "expired"}:
        restored["status"] = "needs_manual_rearm"
        restored["write_disabled"] = True
        restored.setdefault("events", []).append({"kind": "restart_readonly", "message": "进程重启后仅只读对账；未人工重新启用前禁止写入"})
    return restored


def check_area_shape_execution_conflicts(client: Any, credentials: Any, *, environment: str, inst_id: str, own_ids: set[str] | None = None) -> None:
    if environment not in {"demo", "live"}:
        raise PermissionError("冲突检查环境无效")
    own_ids = own_ids or set()
    positions = client.get_positions(credentials, environment=environment, inst_type="SWAP", prefer_cache=False)
    if any(str(item.inst_id).upper() == inst_id.upper() and item.position != 0 for item in positions):
        raise RuntimeError("同账户同合约已有仓位（包括手工/其他profile/EMA）；拒绝区域开仓")
    # Must be an exact instId query covering all entry-capable normal/algo orders.
    orders = client.get_area_shape_pending_orders(credentials, environment=environment, inst_id=inst_id)
    if any((str(getattr(item, "client_order_id", "") or "") not in own_ids) for item in orders):
        raise RuntimeError("同账户同合约已有活动普通/算法委托；拒绝区域开仓")


def check_demo_execution_conflicts(client: Any, credentials: Any, *, environment: str, inst_id: str, own_ids: set[str] | None = None) -> None:
    if environment != "demo":
        raise PermissionError("demo冲突检查入口只允许demo环境")
    check_area_shape_execution_conflicts(client, credentials, environment="demo", inst_id=inst_id, own_ids=own_ids)


def create_persisted_entry_intent(session: dict[str, Any], *, signal_key: str, side: str, size: Decimal, price: Decimal, stop: Decimal) -> dict[str, Any]:
    if session.get("status") not in {"armed", "signal_pending"} or session.get("pending_intent"):
        raise RuntimeError("执行会话状态不允许创建新入场意图")
    if (not signal_key or side not in {"buy", "sell"}
            or any(not value.is_finite() or value <= 0 for value in (size, price, stop))):
        raise ValueError("入场意图参数无效")
    client_id = new_execution_client_order_id(str(session["plan_id"]), signal_key, "entry")
    return {
        "intent_id": f"{session['plan_id']}|{signal_key}|entry",
        "signal_key": signal_key,
        "cl_ord_id": client_id,
        "side": side,
        "size": str(size),
        "price": str(price),
        "stop": str(stop),
        "state": "prepared",
        "created_at_ms": __import__("time").time_ns() // 1_000_000,
    }

