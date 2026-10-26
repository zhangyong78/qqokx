"""User-started, same-order best-quote amendments. No cancel/recreate entry loop."""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from okx_quant.app_paths import state_dir_path
from okx_quant.area_shape_execution import validate_usdt_linear_swap
from okx_quant.area_shape_trade import area_shape_target_space_r
from okx_quant.persistence import _write_json_atomic
from okx_quant.pricing import snap_to_increment


_WORKING = {"live", "partially_filled"}
_TERMINAL = {"filled", "canceled", "partially_canceled", "mmp_canceled", "order_failed"}


def number(value: Any, default: str = "0") -> Decimal:
    result = Decimal(str(value if value not in (None, "") else default))
    if not result.is_finite():
        raise ValueError("订单价格或数量不是有效数字")
    return result


def runtime_fingerprint(runtime: Any) -> str:
    if not getattr(runtime.credentials, "api_key", ""):
        raise ValueError("API身份缺失，不能绑定追单")
    return hashlib.sha256(str(runtime.credentials.api_key).encode()).hexdigest()


def order_reference(order: Any) -> str:
    return str(getattr(order, "order_id", "") or getattr(order, "ord_id", "") or "")


def chase_unavailable_reason(order: Any) -> str:
    kind = str(getattr(order, "ord_type", "") or "").lower()
    if kind not in {"limit", "post_only"} or not order_reference(order):
        if kind in {"oco", "conditional", "trigger", "move_order_stop"}:
            return "这是尚未触发的条件/止盈止损单；不追改触发价。触发为普通限价子单后可追价，市价退出无需追单。"
        return "仅普通限价委托支持追单；市价、FOK和算法委托不适用。"
    if str(getattr(order, "state", "") or "").lower() not in _WORKING:
        return "委托尚未确认活动或已经结束。"
    return ""


def attached_stop(raw: dict[str, Any]) -> Decimal | None:
    rows = [raw, *(item for item in raw.get("attachAlgoOrds", []) if isinstance(item, dict))]
    stops = {number(row["slTriggerPx"]) for row in rows if row.get("slTriggerPx") not in (None, "", "0")}
    if len(stops) > 1:
        raise ValueError("该开仓单有多组不同止损，不能自动确定风险基准")
    return next(iter(stops), None)


def attached_targets(raw: dict[str, Any]) -> list[Decimal]:
    rows = [raw, *(item for item in raw.get("attachAlgoOrds", []) if isinstance(item, dict))]
    return sorted({number(row["tpTriggerPx"]) for row in rows if row.get("tpTriggerPx") not in (None, "", "0")})


def classify_order(raw: dict[str, Any], *, position_mode: str, positions: list[Any]) -> str:
    side, pos_side = str(raw.get("side", "")), str(raw.get("posSide", ""))
    if side not in {"buy", "sell"}:
        raise ValueError("无法确定订单买卖方向")
    if position_mode == "long_short_mode":
        if pos_side not in {"long", "short"}:
            raise ValueError("双向持仓订单缺少明确的持仓方向")
        return "close" if (pos_side, side) in {("long", "sell"), ("short", "buy")} else "open"
    if position_mode != "net_mode":
        raise ValueError("当前账户持仓模式不支持自动识别开平仓")
    reduce_only = str(raw.get("reduceOnly", "")).lower()
    if reduce_only in {"true", "1"}:
        return "close"
    for position in positions:
        if str(position.inst_id).upper() != str(raw.get("instId", "")).upper():
            continue
        qty = number(position.position)
        if (qty > 0 and side == "sell") or (qty < 0 and side == "buy"):
            raise ValueError("订单可能同时减仓和反向开仓；请先使用明确的只减仓平仓单")
    if reduce_only not in {"false", "0"}:
        raise ValueError("净持仓订单未明确标记只减仓属性，无法可靠识别开平仓")
    return "open"


def remaining_risk_size(*, budget: Decimal, stop: Decimal, price: Decimal, filled: Decimal,
                        average: Decimal, multiplier: Decimal, cost_rate: Decimal,
                        lot: Decimal) -> Decimal:
    """Return total target size (OKX newSz includes all cumulative fills)."""
    filled_risk = filled * (abs(average-stop) + (average+stop)*cost_rate) * multiplier
    remaining_budget = max(Decimal(0), budget-filled_risk)
    unit_risk = (abs(price-stop) + (price+stop)*cost_rate) * multiplier
    if unit_risk <= 0:
        raise ValueError("委托价到止损的风险距离无效")
    remaining = snap_to_increment(remaining_budget/unit_risk, lot, "down")
    return filled + remaining


class PendingOrderChaseService:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or state_dir_path()/"pending_order_chase.json"
        self.lock = threading.RLock()
        self.operation_locks: dict[str, threading.RLock] = {}
        self.tasks: dict[str, dict[str, Any]] = {}
        self.error = ""
        if self.path.exists():
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                if payload.get("version") != 1 or not isinstance(payload.get("tasks"), dict):
                    raise ValueError("追单记录格式未知")
                self.tasks = payload["tasks"]
                for task in self.tasks.values():
                    task["active"] = False
                    task["message"] = "程序重启，自动追单已暂停；重新启用前核对原订单"
            except Exception as exc:
                self.tasks = {}
                self.error = f"追单记录读取失败，已禁止改单：{exc}"

    @staticmethod
    def key(runtime: Any, inst_id: str, order_id: str) -> str:
        return f"{runtime_fingerprint(runtime)}|{runtime.environment}|{inst_id.upper()}|{order_id}"

    def _save(self) -> None:
        with self.lock:
            if self.error:
                raise RuntimeError(self.error)
            try:
                _write_json_atomic(self.path, {"version": 1, "tasks": self.tasks})
            except Exception as exc:
                self.error = f"追单记录保存失败，停止后续改单：{exc}"
                raise RuntimeError(self.error) from exc

    def _operation_lock(self, key: str) -> threading.RLock:
        with self.lock:
            return self.operation_locks.setdefault(key, threading.RLock())

    def snapshot(self, key: str) -> dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.tasks.get(key, {}))

    def stop(self, key: str, message: str = "自动追单已停止，保留交易所原委托") -> None:
        with self.lock:
            if key in self.tasks:
                self.tasks[key].update(active=False, message=message)
                self._save()

    def _read(self, client: Any, runtime: Any, task: dict[str, Any]) -> Any:
        status = client.get_order(runtime.credentials, SimpleNamespace(environment=runtime.environment),
                                  inst_id=task["inst_id"], ord_id=task["order_id"])
        raw = status.raw or {}
        if (str(status.ord_id) != task["order_id"] or str(raw.get("instId", "")).upper() != task["inst_id"]
                or str(raw.get("side", "")) != task["side"] or str(raw.get("posSide", "")) != task["pos_side"]
                or str(raw.get("ordType", "")) not in {"limit", "post_only"}
                or (task.get("client_order_id") and str(raw.get("clOrdId", "")) != task["client_order_id"])):
            raise ValueError("订单身份、方向或类型已变化；停止追单")
        return status

    def start(self, *, client: Any, runtime: Any, order: Any, context: dict[str, Any] | None = None) -> str:
        reason = chase_unavailable_reason(order)
        if reason:
            raise ValueError(reason)
        if runtime.environment not in {"demo", "live"}:
            raise ValueError("账户环境无效")
        context = context or {}
        key = self.key(runtime, order.inst_id, order_reference(order))
        with self._operation_lock(key):
            if self.error:
                raise RuntimeError(self.error)
            old = self.tasks.get(key)
            if old and old.get("active"):
                raise ValueError("该委托已在自动追单")
            task = dict(inst_id=str(order.inst_id).upper(), order_id=order_reference(order),
                        client_order_id=str(getattr(order, "client_order_id", "") or ""),
                        side=str(order.side), pos_side=str(getattr(order, "pos_side", "") or ""))
            status = self._read(client, runtime, task)
            if str(status.state) not in _WORKING:
                raise ValueError("该委托已结束，无需追单")
            if (number(status.price) <= 0 or number(status.size) <= 0
                    or not Decimal(0) <= number(status.filled_size) <= number(status.size)):
                raise ValueError("原委托价格、数量或累计成交量无效")
            if old and old.get("pending"):
                self._resolve_pending(old, status)
                if old.get("pending"):
                    self._save()
                    raise ValueError("上一次改单尚未核实，已暂停；不重复发出改单")
            account = client.get_account_config(runtime.credentials, environment=runtime.environment)
            uid = str((account.raw or {}).get("uid", ""))
            if not uid:
                raise ValueError("未取得账户UID，不能绑定追单")
            positions = client.get_positions(runtime.credentials, environment=runtime.environment, prefer_cache=False)
            mode = classify_order(status.raw, position_mode=str((account.raw or {}).get("posMode", "")), positions=positions)
            instrument = client.get_instrument(task["inst_id"], prefer_cached=True)
            if instrument is None or number(instrument.tick_size) <= 0:
                raise ValueError("合约报价单位不可用")
            task.update(mode=mode, account_uid=uid, environment=runtime.environment,
                        fingerprint=runtime_fingerprint(runtime), profile_name=str(getattr(runtime, "credential_profile_name", "")),
                        position_mode=str((account.raw or {}).get("posMode", "")), tick=str(instrument.tick_size),
                        active=True, pending=None, owner=str(context.get("owner", "")),
                        stop_on_fill=bool(context.get("stop_on_fill", False)),
                        constraints={name: str(context[name]) for name in ("target", "min_space_r", "take_profit", "take_profit_r") if context.get(name) is not None},
                        approved=list(old.get("approved", [])) if old else [],
                        initial=list(old["initial"]) if old else [str(status.price), str(status.size)],
                        last_filled=str(status.filled_size or 0), message="自动追单已启用")
            if mode == "open":
                tick, lot, minimum, multiplier = validate_usdt_linear_swap(instrument, task["inst_id"])
                stop = attached_stop(status.raw)
                if stop is None or stop <= 0:
                    raise ValueError("开仓单缺少可核实的附带止损；请先设置止损再启用追单")
                if context.get("stop") is not None and stop != number(context["stop"]):
                    raise ValueError("交易所止损与原计划不一致，不能追单")
                if (task["side"] == "buy" and stop >= number(status.price)) or (task["side"] == "sell" and stop <= number(status.price)):
                    raise ValueError("开仓委托价与止损方向不正确")
                targets = attached_targets(status.raw)
                if any(target <= 0 for target in targets):
                    raise ValueError("附带止盈价无效")
                if targets and context.get("take_profit") and number(context["take_profit"]) not in targets:
                    raise ValueError("交易所止盈与原计划不一致，不能追单")
                fee = client.get_area_shape_fee_rate(runtime.credentials, environment=runtime.environment,
                                                     inst_family=instrument.inst_family)
                cost = number(fee) + number(context.get("slippage_rate"))
                if number(fee) < 0 or number(context.get("slippage_rate")) < 0 or cost >= 1:
                    raise ValueError("手续费或滑点参数无效")
                filled, total = number(status.filled_size), number(status.size)
                average = number(status.avg_price) if filled else number(status.price)
                if filled and average <= 0:
                    raise ValueError("部分成交均价未确认，不能计算剩余风险")
                budget = number(context.get("risk_amount"))
                if budget <= 0:
                    budget = ((total-filled)*(abs(number(status.price)-stop)+(number(status.price)+stop)*cost)
                              + filled*(abs(average-stop)+(average+stop)*cost))*multiplier
                if budget <= 0:
                    raise ValueError("开仓风险预算无效")
                task.update(stop=str(stop), budget=str(budget), cost_rate=str(cost), lot=str(lot),
                            minimum=str(minimum), multiplier=str(multiplier), take_profits=[str(p) for p in targets])
            with self.lock:
                self.tasks[key] = task
                self._save()
            return key

    @staticmethod
    def _pair(status: Any) -> list[str]:
        return [str(number(status.price)), str(number(status.size))]

    @staticmethod
    def _same_pair(a: list[str], b: list[str]) -> bool:
        return len(a) == len(b) == 2 and all(number(x) == number(y) for x, y in zip(a, b))

    def _resolve_pending(self, task: dict[str, Any], status: Any) -> None:
        pending = task.get("pending")
        if not pending:
            return
        if self._same_pair(self._pair(status), pending["new"]):
            task.update(pending=None, message="改单已回读确认")
        elif (str((status.raw or {}).get("reqId", "")) == pending["req_id"]
              and str((status.raw or {}).get("amendResult", "")) not in {"", "0"}):
            task.update(pending=None, active=False, message="交易所拒绝改单，原委托保留")
        else:
            task.update(active=False, message="改单结果尚未核实，已暂停；仅核对原订单，不重发")

    def step(self, *, client: Any, runtime: Any, key: str) -> dict[str, Any]:
        with self._operation_lock(key):
            task = self.tasks[key]
            if self.error:
                raise RuntimeError(self.error)
            if task["fingerprint"] != runtime_fingerprint(runtime) or task["environment"] != runtime.environment:
                raise ValueError("追单绑定的账户或环境已改变")
            status = self._read(client, runtime, task)
            if task.get("pending"):
                self._resolve_pending(task, status)
                self._save()
                return copy.deepcopy(task)
            if not task.get("active"):
                return copy.deepcopy(task)
            if str(status.state) in _TERMINAL:
                task.update(active=False, message=f"委托已{status.state}，追单结束")
                self._save()
                return copy.deepcopy(task)
            if str(status.state) not in _WORKING:
                raise ValueError("委托状态未确认，停止追单")
            pair = self._pair(status)
            known = [task["initial"], *task["approved"]]
            if not any(self._same_pair(pair, item) for item in known):
                raise ValueError("委托被其他来源修改；自动追单已停止")
            filled = number(status.filled_size)
            if filled < number(task["last_filled"]):
                raise ValueError("累计成交数量回退，停止追单并核对订单")
            task["last_filled"] = str(filled)
            if filled > 0 and task["stop_on_fill"]:
                task.update(active=False, message="已检测到成交，追单停止并交回原策略保护")
                self._save()
                return copy.deepcopy(task)
            positions = client.get_positions(runtime.credentials, environment=runtime.environment, prefer_cache=False)
            if classify_order(status.raw, position_mode=task["position_mode"], positions=positions) != task["mode"]:
                raise ValueError("委托的开平仓性质发生变化，停止追单")
            book = client.get_order_book(task["inst_id"], depth=1)
            book_ts = int((book.raw or {}).get("ts") or 0)
            if (abs(int(time.time()*1000)-book_ts) > 5000 or not book.bids or not book.asks
                    or number(book.bids[0][0]) <= 0 or number(book.asks[0][0]) <= 0
                    or book.bids[0][0] > book.asks[0][0]):
                raise ValueError("买一/卖一盘口缺失、过期或倒挂，暂停追单")
            price = number(book.bids[0][0] if task["side"] == "buy" else book.asks[0][0])
            price = snap_to_increment(price, number(task["tick"]), "down" if task["side"] == "buy" else "up")
            total = number(status.size)
            new_total = total
            if task["mode"] == "open":
                stop = attached_stop(status.raw)
                if stop is None or stop != number(task["stop"]):
                    raise ValueError("附带止损已变化或无法核实；停止追单，不覆盖人工保护")
                if (task["side"] == "buy" and stop >= price) or (task["side"] == "sell" and stop <= price):
                    raise ValueError("最新盘口已越过止损，不追价")
                direction = "long" if task["side"] == "buy" else "short"
                targets = attached_targets(status.raw)
                if targets != [number(p) for p in task.get("take_profits", [])]:
                    raise ValueError("附带止盈已变化；停止追单，不覆盖人工保护")
                if any(area_shape_target_space_r(entry=price, stop=stop, target=target, direction=direction,
                        cost_rate=number(task["cost_rate"])) <= 0 for target in targets):
                    raise ValueError("最新盘口已接近或越过附带止盈，没有有效收益空间")
                constraints = task["constraints"]
                for target_name, r_name in (("target", "min_space_r"), ("take_profit", "take_profit_r")):
                    if constraints.get(target_name):
                        space = area_shape_target_space_r(entry=price, stop=stop, target=number(constraints[target_name]),
                            direction=direction, cost_rate=number(task["cost_rate"]))
                        if space < number(constraints.get(r_name)):
                            raise ValueError("最新盘口已不满足原计划的目标空间/盈亏比，暂停追单")
                average = number(status.avg_price) if filled else price
                if filled and average <= 0:
                    raise ValueError("部分成交均价不可用，暂停追单")
                # A fill may race this amendment at the OLD price. Size for the
                # worse of old/new quotes; a later confirmed cycle can enlarge
                # the remaining quantity after the better quote is effective.
                sizing_price = (max(price, number(status.price)) if task["side"] == "buy"
                                else min(price, number(status.price)))
                new_total = remaining_risk_size(budget=number(task["budget"]), stop=stop, price=sizing_price,
                    filled=filled, average=average, multiplier=number(task["multiplier"]),
                    cost_rate=number(task["cost_rate"]), lot=number(task["lot"]))
                if new_total-filled < number(task["minimum"]):
                    raise ValueError("剩余风险预算不足最小下单量；已暂停追单，请核对剩余委托")
                instrument = client.get_instrument(task["inst_id"], prefer_cached=True)
                client._enforce_order_size_safety_limit(instrument=instrument, size=new_total, reference_price=price)
            if price == number(status.price) and new_total == total:
                task["message"] = "已在买一/卖一，等待成交"
                return copy.deepcopy(task)
            if not task.get("active"):
                return copy.deepcopy(task)
            amendment = dict(req_id=uuid.uuid4().hex, old=pair, new=[str(price), str(new_total)], at_ms=int(time.time()*1000))
            task["pending"] = amendment
            task["approved"] = [*task["approved"], amendment["new"]][-256:]
            task["message"] = "改单已记录，等待交易所回读"
            self._save()  # Durable authorization before any order write.
            try:
                client.amend_order(runtime.credentials, environment=runtime.environment,
                    inst_id=task["inst_id"], ord_id=task["order_id"], req_id=amendment["req_id"],
                    new_price=price, **({"new_size": new_total} if task["mode"] == "open" else {}))
            except Exception as exc:
                # The exchange may have accepted the request; query this order,
                # never cancel/recreate or blindly resend the amendment.
                task["last_request_error"] = str(exc)
            try:
                updated = self._read(client, runtime, task)
                self._resolve_pending(task, updated)
                if task.get("pending") and task.get("last_request_error"):
                    task["message"] += f"；请求提示：{task['last_request_error']}"
                if str(updated.state) in _TERMINAL:
                    task.update(active=False, message=f"委托已{updated.state}，追单结束")
            except Exception as exc:
                task.update(active=False, message=f"改单后回读失败，已暂停：{exc}")
            self._save()
            return copy.deepcopy(task)

    def authorized_update(self, *, runtime: Any, inst_id: str, status: Any,
                          old_price: Any, old_size: Any, stop: Any, account_uid: str = "") -> dict[str, Any] | None:
        """Let the original strategy recognize only this feature's durable amendments."""
        if not getattr(runtime.credentials, "api_key", ""):
            return None
        with self.lock:
            task = self.tasks.get(self.key(runtime, inst_id, str(status.ord_id)))
            if not task or task.get("mode") != "open" or not task.get("owner"):
                return None
            raw = status.raw or {}
            if (str(raw.get("instId", "")).upper() != task["inst_id"]
                    or str(raw.get("side", "")) != task["side"]
                    or str(raw.get("posSide", "")) != task["pos_side"]
                    or (task["client_order_id"] and str(raw.get("clOrdId", "")) != task["client_order_id"])):
                return None
            if account_uid and task["account_uid"] != account_uid:
                return None
            if number(task["stop"]) != number(stop):
                return None
            known = [task["initial"], *task["approved"]]
            if (any(self._same_pair([str(old_price), str(old_size)], p) for p in known)
                    and any(self._same_pair(self._pair(status), p) for p in known)):
                return copy.deepcopy(task)
            return None


_SERVICE: PendingOrderChaseService | None = None
_SERVICE_LOCK = threading.Lock()


def get_pending_order_chase_service() -> PendingOrderChaseService:
    global _SERVICE
    with _SERVICE_LOCK:
        if _SERVICE is None:
            _SERVICE = PendingOrderChaseService()
        return _SERVICE
