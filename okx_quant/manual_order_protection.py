"""Explicit, user initiated edits of an existing exchange protection order."""
from decimal import Decimal

from okx_quant.area_shape_execution import validate_usdt_linear_swap
from okx_quant.arbitrage.arbitrage_executor import _build_strategy_config
from okx_quant.pricing import snap_to_increment

_PENDING_EDITS = {}
_FEE_CACHE = {}


def _fee_cache_key(runtime, inst_id):
    credentials = runtime.credentials
    identity = str(getattr(credentials, "api_key", "") or getattr(credentials, "profile_name", "") or getattr(runtime, "credential_profile_name", ""))
    return (identity, runtime.environment, inst_id)


def cached_fee(runtime, inst_id):
    key = _fee_cache_key(runtime, inst_id)
    return _FEE_CACHE.get(key) if key[0] else None


def read_order(client, runtime, item):
    if runtime.environment not in {"live", "demo"}:
        raise ValueError("账户环境无效")
    if item.source_kind == "algo":
        raw = client.get_order_algo(runtime.credentials, environment=runtime.environment,
            inst_id=item.inst_id, algo_id=item.algo_id, algo_cl_ord_id=item.algo_client_order_id)
        if not raw or raw.get("state") != "live":
            raise ValueError("该保护委托已结束或状态无法确认")
        current = client._parse_algo_order_item(raw, default_inst_type=item.inst_type)
    else:
        from roll_terminal_qt.order_service import OrderFeedThread, order_view_to_trade_order_item
        status = client.get_order(runtime.credentials, _build_strategy_config(item.inst_id, runtime),
            inst_id=item.inst_id, ord_id=item.order_id, cl_ord_id=item.client_order_id, request_timeout=5)
        if status.state not in {"live", "partially_filled"}:
            raise ValueError("该委托已结束")
        feed = OrderFeedThread(None)
        try:
            current = order_view_to_trade_order_item(feed._to_view(status))
        finally:
            feed.stop()
    if current.inst_id != item.inst_id or current.side != item.side or current.pos_side != item.pos_side:
        raise ValueError("委托身份或方向已变化，请刷新后重试")
    if (item.source_kind == "algo" and item.algo_id and current.algo_id != item.algo_id) or (
        item.source_kind != "algo" and item.order_id and current.order_id != item.order_id):
        raise ValueError("交易所返回的委托ID不匹配")
    return current


def matched_position(positions, item):
    matches = [p for p in positions if p.inst_id == item.inst_id and Decimal(str(p.position)) != 0
               and (item.pos_side not in {"long", "short"} or p.pos_side == item.pos_side)]
    if len(matches) != 1:
        raise ValueError("无法唯一匹配真实持仓，请先核对多空方向")
    p = matches[0]
    direction = p.pos_side if p.pos_side in {"long", "short"} else ("long" if p.position > 0 else "short")
    return p, direction


def break_even_price(entry, fee, tick, direction):
    entry, fee, tick = Decimal(str(entry)), Decimal(str(fee)), Decimal(str(tick))
    if not all(v.is_finite() for v in (entry, fee, tick)) or entry <= 0 or tick <= 0 or not 0 <= fee < 1:
        raise ValueError("开仓价、手续费或报价单位无效")
    if direction not in {"long", "short"}:
        raise ValueError("持仓方向无效")
    price = entry * (1 + fee) / (1 - fee) if direction == "long" else entry * (1 - fee) / (1 + fee)
    return snap_to_increment(price, tick, "up" if direction == "long" else "down")


def prepare_break_even(client, runtime, item):
    current = read_order(client, runtime, item)
    if current.source_kind != "algo" or current.ord_type not in {"oco", "conditional"}:
        raise ValueError("设置保本需要选中持仓对应的 OCO/条件止损保护单")
    instrument = client.get_instrument(item.inst_id, prefer_cached=False)
    tick, _lot, _min, _multiplier = validate_usdt_linear_swap(instrument, item.inst_id)
    positions = client.get_positions(runtime.credentials, environment=runtime.environment, prefer_cache=False)
    position, direction = matched_position(positions, current)
    if current.side != ("sell" if direction == "long" else "buy"):
        raise ValueError("保护单平仓方向与真实持仓不符")
    if current.size is not None and current.size > abs(position.position):
        raise ValueError("保护单数量大于真实持仓，请先核对")
    if current.stop_loss_order_price != Decimal("-1"):
        raise ValueError("设置保本暂只支持市价止损保护单")
    fee = client.get_area_shape_fee_rate(runtime.credentials, environment=runtime.environment, inst_family=instrument.inst_family)
    fee_key = _fee_cache_key(runtime, item.inst_id)
    if fee_key[0]:
        _FEE_CACHE[fee_key] = fee
    target = break_even_price(position.avg_price, fee, tick, direction)
    old = current.stop_loss_trigger_price
    if old is None or old <= 0:
        raise ValueError("原委托没有可核实的止损价")
    if (direction == "long" and old >= target) or (direction == "short" and old <= target):
        raise ValueError("现有止损已经达到保本或更有利，无需修改")
    price_type = current.stop_loss_trigger_price_type or "last"
    latest = client.get_trigger_price(item.inst_id, price_type, environment=runtime.environment, max_cached_age_seconds=0)
    if (direction == "long" and latest <= target) or (direction == "short" and latest >= target):
        raise ValueError("当前触发价尚未越过含双向手续费的保本价，暂不能设置")
    return current, target, direction, fee


def amend_protection(client, runtime, item, tp, sl, *, break_even=False):
    from okx_quant.pending_order_chase import get_pending_order_chase_service, order_reference
    service = get_pending_order_chase_service()
    key = service.key(runtime, item.inst_id, order_reference(item))
    with service._operation_lock(key):
        if key in _PENDING_EDITS:
            current = read_order(client, runtime, item)
            pending_tp, pending_sl = _PENDING_EDITS[key]
            if ((pending_tp is None or current.take_profit_trigger_price == pending_tp)
                    and (pending_sl is None or current.stop_loss_trigger_price == pending_sl)):
                del _PENDING_EDITS[key]
                return current
            raise ValueError("上一笔改单结果尚未确认，禁止重复提交；请先核对交易所状态")
        return _amend_locked(client, runtime, item, tp, sl, break_even, service.snapshot(key), key)


def _amend_locked(client, runtime, item, tp, sl, break_even, task, key):
    if task.get("active"):
        raise ValueError("该委托正在自动追单，请先停止追单")
    current = read_order(client, runtime, item)
    if break_even:
        displayed_stop = sl
        current, sl, _direction, _fee = prepare_break_even(client, runtime, current)
        if displayed_stop is not None and displayed_stop != sl:
            raise ValueError(f"交易所核对后的保本价为 {sl}，与预览不同；请重新打开窗口确认")
        # This action changes only the stop, even if the dialog's TP field is populated.
        tp = None
        if current.stop_loss_order_price != Decimal("-1"):
            raise ValueError("保本暂只支持市价止损；限价止损无法保证按保本价成交")
        fresh = read_order(client, runtime, current)
        if fresh.stop_loss_trigger_price != current.stop_loss_trigger_price:
            raise ValueError("计算期间止损被修改，请重新核对")
    from okx_quant.log_utils import append_log_line
    append_log_line(f"[manual_protection] request env={runtime.environment} inst={item.inst_id} algo={item.algo_id} tp={tp} sl={sl} breakeven={break_even}")
    common = {}
    for name, value, original_order, kind in (
        ("take_profit", tp, current.take_profit_order_price, current.take_profit_trigger_price_type),
        ("stop_loss", sl, current.stop_loss_order_price, current.stop_loss_trigger_price_type),
    ):
        if value is not None:
            if not value.is_finite() or value <= 0:
                raise ValueError("触发价必须是正数")
            common[f"new_{name}_trigger_price"] = value
            common[f"new_{name}_order_price"] = original_order
            common[f"new_{name}_trigger_price_type"] = kind
    if not common:
        raise ValueError("没有需要修改的价格")
    _PENDING_EDITS[key] = (tp, sl)
    if current.source_kind == "algo":
        result = client.amend_algo_order(runtime.credentials, environment=runtime.environment, inst_id=item.inst_id,
            algo_id=current.algo_id, algo_cl_ord_id=current.algo_client_order_id, **common)
    else:
        result = client.amend_order(runtime.credentials, environment=runtime.environment, inst_id=item.inst_id,
            ord_id=current.order_id, cl_ord_id=current.client_order_id, **common)
    if str(result.s_code or "0") != "0":
        raise ValueError(f"交易所拒绝改单：{result.s_msg}")
    # No automatic retry of a write: an uncertain read must be reconciled first.
    verified = read_order(client, runtime, current)
    if ((tp is not None and verified.take_profit_trigger_price != tp)
            or (sl is not None and verified.stop_loss_trigger_price != sl)):
        raise ValueError("改单已请求，但回读尚未确认生效；请刷新核对，勿重复提交")
    append_log_line(f"[manual_protection] verified env={runtime.environment} inst={item.inst_id} algo={item.algo_id} tp={verified.take_profit_trigger_price} sl={verified.stop_loss_trigger_price}")
    _PENDING_EDITS.pop(key, None)
    return verified
