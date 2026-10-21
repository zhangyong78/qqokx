"""Explicitly armed OKX demo/live executor for area-shape plans."""

from __future__ import annotations

import threading
import time
import copy
from decimal import Decimal
from typing import Any, Callable

from PySide6.QtCore import QObject, Signal

from okx_quant.area_shape_execution import (
    AREA_SHAPE_EXECUTION_RULE_VERSION,
    build_area_shape_execution_session,
    check_area_shape_execution_conflicts,
    create_persisted_entry_intent,
    new_execution_client_order_id,
    price_for_net_r,
    restore_execution_session_for_readonly,
    size_usdt_linear_contracts,
    validate_area_shape_runtime,
    validate_usdt_linear_swap,
)
from okx_quant.area_shape_execution_store import load_area_shape_execution_records, save_area_shape_execution_records
from okx_quant.area_shape_trade import (
    AREA_DYNAMIC_TAKE_PROFIT_R, AreaShapePlan, area_shape_lock_profit_r,
    area_shape_net_progress_r, calculate_initial_stop,
)
from okx_quant.engine import StrategyEngine
from okx_quant.indicators import atr
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import snap_to_increment


_ACTIVE = {"arming", "armed", "signal_pending", "entry_intent", "position_unprotected", "tp_pending", "protected", "closing_pending", "manual_control"}
_TERMINAL = {"closed", "cancelled", "expired"}


class AreaShapeDemoExecutionMonitor(QObject):
    """Runs only sessions explicitly enabled in the current process.

    Persisted sessions are downgraded to read-only on construction. This class
    never loads profiles or unlocks credentials on startup.
    """

    status_changed = Signal(str)
    session_updated = Signal(object)

    def __init__(
        self, parent: QObject | None = None, *, client_factory: Callable[[], Any] = OkxRestClient,
        store_load: Callable[[], list[dict[str, Any]]] = load_area_shape_execution_records,
        store_save: Callable[[list[dict[str, Any]]], Any] = save_area_shape_execution_records,
        poll_seconds: float = 10.0,
    ) -> None:
        super().__init__(parent)
        self._lock = threading.RLock()
        self._client_factory = client_factory
        self._store_save = store_save
        self._poll_seconds = max(1.0, float(poll_seconds))
        self._sessions: dict[str, dict[str, Any]] = {}
        self._runtimes: dict[str, Any] = {}
        self._clients: dict[str, Any] = {}
        self._reservations: set[tuple[str, str, str]] = set()
        self._store_write_blocked = False
        self._load_errors: list[str] = []
        try:
            records = store_load()
        except Exception as exc:
            records = []
            self._store_write_blocked = True
            self._load_errors.append(str(exc))
        for record in records:
            restored = restore_execution_session_for_readonly(record)
            session_id = str(restored.get("session_id", ""))
            if not session_id:
                self._store_write_blocked = True
                self._load_errors.append("执行记录缺少session_id")
                continue
            self._sessions[session_id] = restored
            if restored.get("status") not in _TERMINAL:
                self._reservations.add(self._reservation_key(restored))
        if records and not self._store_write_blocked:
            self._persist_locked()
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self._arm_workers: set[threading.Thread] = set()

    @staticmethod
    def _reservation_key(session: dict[str, Any]) -> tuple[str, str, str]:
        return (str(session.get("account_uid", "")), str(session.get("environment", "")), str(session.get("symbol", "")).upper())

    def start(self) -> None:
        with self._lock:
            if self._worker is not None and self._worker.is_alive():
                return
            self._stop.clear()
            self._worker = threading.Thread(target=self._run, daemon=True, name="qqokx-area-shape-demo-execution")
            self._worker.start()

    def stop(self) -> None:
        self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=3.0)
        if self.is_running():
            # Do not race an active tick or arm worker with another REST
            # cancel/reconcile sequence. The owner keeps this QObject alive
            # until these workers finish; restart still restores read-only.
            with self._lock:
                for session in self._sessions.values():
                    if session.get("status") in _ACTIVE:
                        session.setdefault("events", []).append({
                            "kind": "shutdown_worker_busy", "ts": int(time.time() * 1000),
                            "message": "监控或启用线程尚未退出；为避免并发订单操作未重复撤单，重启后只读核对原委托/成交",
                        })
                self._persist_locked()
            return
        with self._lock:
            resting = [key for key, session in self._sessions.items()
                       if isinstance(session.get("resting_entry_intent"), dict)]
        for session_id in resting:
            try:
                self._reconcile_resting_entry(session_id, force_cancel=True, cancel_mode="shutdown")
            except Exception as exc:
                with self._lock:
                    session = self._sessions.get(session_id)
                    if session is not None:
                        session["status"] = "needs_manual_rearm"
                        session["write_disabled"] = True
                        session.setdefault("events", []).append({"kind": "shutdown_gtc_unverified",
                                                                  "ts": int(time.time() * 1000),
                                                                  "message": f"退出时GTC撤单/成交未确认；重启只读核对：{exc}"})
                        self._persist_locked()
        with self._lock:
            for client in self._clients.values():
                try:
                    client.close()
                except Exception:
                    pass
            self._clients.clear()
            # Open demo positions remain exchange-protected; execution is not
            # silently resumed after process restart, which becomes read-only.
            for session in self._sessions.values():
                if session.get("status") in _ACTIVE:
                    session["status"] = "needs_manual_rearm"
                    session.setdefault("events", []).append({"kind": "shutdown_readonly", "message": "进程退出；OKX活动入场委托/保护单仍按交易所状态存在，重启后先只读对账"})
            self._persist_locked()

    def is_running(self) -> bool:
        with self._lock:
            return bool(self._arm_workers) or (self._worker is not None and self._worker.is_alive())

    def pause_session(self, session_id: str) -> None:
        """Stop future entries while keeping exchange-hosted protection managed."""
        with self._lock:
            session = self._sessions[session_id]
            if session.get("status") not in {"armed", "signal_pending", "entry_intent", "protected", "position_unprotected", "tp_pending", "closing_pending"}:
                raise ValueError("当前会话不能暂停")
            session["paused"] = True
            if isinstance(session.get("resting_entry_intent"), dict) and not isinstance(session.get("position"), dict):
                session["cancel_requested"] = "pause"
                session["signal_pending"] = None
                # Route the next monitor tick through GTC cancellation rather
                # than allowing a queued signal_pending tick to submit FOK.
                session["status"] = "armed"
            elif session.get("status") == "entry_intent":
                session["cancel_requested"] = "pause"
            elif session.get("status") in {"armed", "signal_pending"}:
                session["status"] = "paused"
                session["signal_pending"] = None
            session.setdefault("events", []).append({"kind": "paused", "ts": int(time.time() * 1000), "message": "停止新入场；已有保护继续运行"})
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(session))

    def cancel_session(self, session_id: str) -> None:
        with self._lock:
            session = self._sessions[session_id]
            if isinstance(session.get("position"), dict):
                raise ValueError("已有成交仓位；取消只停止新入场，需先人工处置仓位")
            if isinstance(session.get("resting_entry_intent"), dict):
                session["paused"] = True
                session["cancel_requested"] = "cancel"
                session["signal_pending"] = None
                session["status"] = "armed"
                session.setdefault("events", []).append({"kind": "cancel_requested", "ts": int(time.time() * 1000),
                                                          "message": "已请求撤销GTC入场限价单；等待交易所确认终态/成交量"})
                self._persist_locked()
                result = copy.deepcopy(session)
                self.session_updated.emit(result)
                return
            if session.get("status") == "entry_intent" and isinstance(session.get("pending_intent"), dict):
                session["paused"] = True
                session["cancel_requested"] = "cancel"
                session.setdefault("events", []).append({"kind": "cancel_requested", "ts": int(time.time() * 1000),
                                                          "message": "已请求撤销OKX触发委托；等待交易所确认终态"})
                self._persist_locked()
                result = copy.deepcopy(session)
                self.session_updated.emit(result)
                return
            if session.get("pending_intent") and not isinstance(session.get("position"), dict):
                raise ValueError("入场意图结果未核实；需先按原client id对账")
            session["status"] = "cancelled"
            session["paused"] = True
            session["signal_pending"] = None
            self._reservations.discard(self._reservation_key(session))
            session.setdefault("events", []).append({"kind": "cancelled", "ts": int(time.time() * 1000), "message": "计划已取消，不再新开仓"})
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(session))

    def resume_session(self, session_id: str, *, runtime: Any, unlocked: bool) -> dict[str, Any]:
        """Resume an in-process pause after rechecking the bound demo account."""
        with self._lock:
            session = self._sessions[session_id]
            if session.get("status") not in {"paused", "protected", "position_unprotected", "tp_pending"} or not session.get("paused"):
                raise ValueError("会话不处于可恢复暂停状态")
            self._verify_runtime_identity(session, runtime, unlocked)
        created_client = None
        client = self._clients.get(session_id)
        if client is None:
            created_client = self._client_factory()
            client = created_client
        account = client.get_account_config(runtime.credentials, environment=_session_environment(session))
        if str((account.raw or {}).get("uid") or "") != str(session.get("account_uid")):
            if created_client is not None:
                created_client.close()
            raise PermissionError("当前OKX UID与暂停会话不一致")
        with self._lock:
            self._runtimes[session_id] = runtime
            if created_client is not None:
                self._clients[session_id] = created_client
        if isinstance(session.get("resting_entry_intent"), dict):
            # A prior pause may have timed out or raced an entry tick. Never
            # overwrite its client ID with a new GTC before the exchange has
            # confirmed cancellation and zero fills.
            result = self._reconcile_resting_entry(
                session_id, force_cancel=True, cancel_mode="pause",
            )
            if result != "clear" or self._sessions[session_id].get("status") != "paused":
                raise RuntimeError("遗留GTC未确认终态零成交；暂停恢复保持停止并转只读核对")
            session = copy.deepcopy(self._sessions[session_id])
        has_position = isinstance(session.get("position"), dict)
        baseline = 0
        baseline_candles: list[Any] = []
        if not has_position:
            expires = int(session.get("expires_at_candle_ts") or 0)
            if expires > 0 and int(time.time() * 1000) >= expires + 3_600_000:
                self._finish_entry_cancel(session_id, "expire")
                return copy.deepcopy(self._sessions[session_id])
            check_area_shape_execution_conflicts(
                client, runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
            )
            baseline_candles = [item for item in client.get_candles_history(session["symbol"], "1H", limit=240) if item.confirmed]
            if not baseline_candles:
                if created_client is not None:
                    created_client.close()
                raise RuntimeError("恢复新入场监控需要最新已收盘1H基线")
            baseline = max(int(item.ts) for item in baseline_candles)
        with self._lock:
            if self._stop.is_set() or self._sessions[session_id].get("status") != session.get("status"):
                if created_client is not None:
                    created_client.close()
                raise RuntimeError("会话在恢复期间已变化或应用正在关闭")
            self._runtimes[session_id] = runtime
            if created_client is not None:
                self._clients[session_id] = created_client
            current = self._sessions[session_id]
            current["paused"] = False
            if has_position:
                current["status"] = "protected" if current.get("position", {}).get("stop_verified_at_ms") else "position_unprotected"
            else:
                current["status"] = "armed"
                current["signal_cursor_ts"] = baseline
                current["baseline_candle_ts"] = baseline
                plan = AreaShapePlan.from_dict(current["plan_snapshot"])
                if plan.config.resting_entry_enabled or plan.config.entry_trigger_mode == "direct_touch":
                    current["resting_entry_intent"], reason = self._prepare_resting_entry(current, plan, baseline_candles)
                    if reason:
                        current.setdefault("events", []).append({"kind": "resting_entry_skipped", "ts": int(time.time() * 1000),
                                                                  "message": reason})
            current.setdefault("events", []).append({"kind": "resumed", "ts": int(time.time() * 1000),
                                                       "message": "人工恢复；持仓继续保护管理，新入场从当前已收盘K线之后开始"})
            self._persist_locked()
            result = copy.deepcopy(current)
        if not has_position and result.get("resting_entry_intent"):
            self._submit_resting_entry(session_id)
            result = copy.deepcopy(self._sessions[session_id])
        self.session_updated.emit(result)
        return result

    def sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(list(self._sessions.values()))

    def request_arm(self, *, plan: AreaShapePlan, runtime: Any, unlocked: bool, paper_monitor: Any, environment: str | None = None) -> None:
        """Preflight in a worker so REST/credential verification never blocks Qt."""
        if self._stop.is_set():
            self.status_changed.emit("应用正在关闭；拒绝启用demo执行")
            return
        runtime_environment = str(getattr(runtime, "environment", "")).lower()
        if runtime_environment == "live" and str(environment or "").lower() != "live":
            self.status_changed.emit("live执行必须显式传入 environment='live'；未启动计划")
            return
        worker = threading.Thread(
            target=self._arm_worker, kwargs={"plan": plan, "runtime": runtime, "unlocked": unlocked,
                                             "paper_monitor": paper_monitor,
                                             "environment": environment or str(getattr(runtime, "environment", ""))},
            daemon=True, name=f"qqokx-area-demo-arm-{plan.plan_id[:8]}",
        )
        with self._lock:
            if self._stop.is_set():
                return
            self._arm_workers.add(worker)
            try:
                worker.start()
            except Exception:
                self._arm_workers.discard(worker)
                raise

    def _arm_worker(self, **kwargs: Any) -> None:
        environment = str(kwargs.get("environment") or getattr(kwargs.get("runtime"), "environment", "demo")).lower()
        try:
            self.arm_plan(**kwargs)
        except Exception as exc:
            self.status_changed.emit(f"{environment}执行启用失败：{exc}")
        finally:
            with self._lock:
                self._arm_workers.discard(threading.current_thread())

    def arm_plan(self, *, plan: AreaShapePlan, runtime: Any, unlocked: bool, paper_monitor: Any, environment: str | None = None) -> dict[str, Any]:
        if str(getattr(runtime, "environment", "")).lower() == "live" and str(environment or "").lower() != "live":
            raise PermissionError("live执行必须显式传入 environment='live'")
        environment = str(environment or getattr(runtime, "environment", "")).lower()
        profile, key_hash = validate_area_shape_runtime(plan=plan, runtime=runtime, unlocked=unlocked, environment=environment)
        credentials = runtime.credentials
        client = self._client_factory()
        account = client.get_account_config(credentials, environment=environment)
        account_uid = str((account.raw or {}).get("uid") or "").strip()
        if not account_uid:
            raise RuntimeError("OKX账户配置未返回uid；无法按账户隔离，拒绝启用")
        reservation = (account_uid, environment, plan.symbol.upper())
        with self._lock:
            self._require_writable_store()
            if reservation in self._reservations:
                try:
                    client.close()
                except Exception:
                    pass
                raise RuntimeError("同一OKX账户的该demo合约已有区域执行会话或待人工对账会话")
            self._reservations.add(reservation)
        session_id = ""
        try:
            instrument = client.get_area_shape_instrument(plan.symbol)
            validate_usdt_linear_swap(instrument, plan.symbol)
            family = str(instrument.inst_family or "").upper()
            if not family:
                raise RuntimeError("合约instFamily缺失，无法读取手续费")
            fee_rate = client.get_area_shape_fee_rate(credentials, environment=environment, inst_family=family)
            # Strategy preference must match the account's real position mode.
            actual_mode = str((account.raw or {}).get("posMode") or "")
            runtime_mode = str(getattr(runtime, "position_mode", ""))
            if actual_mode not in {"net_mode", "long_short_mode"}:
                raise RuntimeError("账户持仓模式未知，拒绝执行")
            if runtime_mode not in {"net", "long_short"} or (actual_mode == "net_mode") != (runtime_mode == "net"):
                raise RuntimeError("runtime持仓模式与OKX账户posMode不一致")
            check_area_shape_execution_conflicts(client, credentials, environment=environment, inst_id=plan.symbol)
            if self._stop.is_set():
                raise RuntimeError("应用正在关闭；取消晚到的demo启用")
            candles = [item for item in client.get_candles_history(plan.symbol, "1H", limit=240) if item.confirmed]
            if not candles:
                raise RuntimeError("缺少确认的1H基线K线")
            session = build_area_shape_execution_session(
                plan=plan, runtime=runtime, account_uid=account_uid, instrument=instrument,
                fee_rate=fee_rate, slippage_rate=plan.config.slippage_rate,
                baseline_candle_ts=max(item.ts for item in candles), confirmed_at_ms=int(time.time() * 1000),
                unlocked=unlocked, environment=environment,
            )
            session["api_key_sha256"] = key_hash
            session["client_id_fingerprint"] = key_hash
            session["status"] = "arming"
            session_id = str(session["session_id"])
            if self._stop.is_set():
                raise RuntimeError("应用正在关闭；取消晚到的demo启用")
            with self._lock:
                self._sessions[session_id] = session
                self._persist_locked()
            # After this marker the public paper worker will not mutate this plan.
            mark_execution = getattr(paper_monitor, "mark_area_shape_execution", None)
            if callable(mark_execution):
                mark_execution(plan.plan_id, f"{environment}_exchange")
            elif environment == "demo" and callable(getattr(paper_monitor, "mark_demo_execution", None)):
                paper_monitor.mark_demo_execution(plan.plan_id)
            else:
                raise RuntimeError("区域计划监控器不支持冻结环境的执行隔离")
            resting_reason = ""
            place_line_order = plan.config.entry_trigger_mode == "direct_touch" or plan.config.resting_entry_enabled
            if place_line_order:
                session["resting_entry_intent"], resting_reason = self._prepare_resting_entry(session, plan, candles)
                if plan.config.entry_trigger_mode == "direct_touch" and not session["resting_entry_intent"]:
                    raise RuntimeError(resting_reason or "直接入场线GTC挂单未能准备")
            with self._lock:
                session["status"] = "armed"
                if resting_reason:
                    session["events"].append({"kind": "resting_entry_skipped", "ts": int(time.time() * 1000),
                                              "message": resting_reason})
                armed_message = (
                    f"直接价格触发已启用；正在向OKX提交{environment}入场线GTC限价单，成交后转入仓位保护"
                    if plan.config.entry_trigger_mode == "direct_touch"
                    else f"{environment}执行已启用；GTC入场线挂单与形态确认入场并行，成交互斥"
                )
                session["events"].append({"kind": "armed", "ts": int(time.time() * 1000),
                                          "message": armed_message})
                self._runtimes[session_id] = runtime
                self._clients[session_id] = client
                self._persist_locked()
            if session.get("resting_entry_intent"):
                self._submit_resting_entry(session_id)
            self.session_updated.emit(copy.deepcopy(session))
            current = self._sessions[session_id]
            if plan.config.entry_trigger_mode == "direct_touch":
                status_message = (
                    f"{plan.symbol}直接入场已启用；OKX入场线GTC已提交，等待委托成交"
                    if current.get("status") == "armed" and current.get("resting_entry_intent")
                    else f"{plan.symbol}直接入场状态：{current.get('status')}；请核对区域交易事件"
                )
            else:
                status_message = f"{plan.symbol} {environment}执行已启用，账户uid已绑定；等待新信号"
            self.status_changed.emit(status_message)
            return dict(session)
        except Exception:
            with self._lock:
                if session_id and session_id in self._sessions:
                    self._sessions[session_id]["status"] = "manual_review"
                    self._sessions[session_id].setdefault("events", []).append({"kind": "arm_failed", "message": "启用预检/写入失败；需人工核对"})
                    self._persist_locked()
                else:
                    # No persisted session or order intent exists, so REST
                    # writes could not have been attempted and the reservation
                    # is safe to release.
                    self._reservations.discard(reservation)
                    try:
                        client.close()
                    except Exception:
                        pass
            raise

    def _prepare_resting_entry(
        self, session: dict[str, Any], plan: AreaShapePlan, candles: list[Any],
    ) -> tuple[dict[str, Any] | None, str]:
        expires = int(session.get("expires_at_candle_ts") or 0)
        if expires > 0 and int(time.time() * 1000) >= expires + 3_600_000:
            return None, "计划已到期；禁止准备新的入场线GTC"
        if (plan.config.entry_trigger_mode != "direct_touch"
                and plan.config.entry_mode != "next_open"):
            return None, "普通GTC与旧版OKX条件触发模式不并行；本次保留条件触发路径"
        needs_atr = (plan.config.stop_basis != "fixed_distance" and plan.config.stop_offset_atr > 0) or plan.config.target_buffer_atr > 0
        current_atr = Decimal("0")
        if needs_atr:
            values = atr(candles, plan.config.atr_period)
            current_atr = values[-1] if values else None
            if current_atr is None or current_atr <= 0:
                return None, "最新已收盘1H ATR不可用；无法计算入场保护止损或目标缓冲"
        if plan.config.stop_basis != "fixed_distance" and plan.config.stop_offset_atr <= 0:
            return None, "止损ATR偏移必须大于0才能计算入场保护止损"
        instrument = session["instrument"]
        tick = Decimal(str(instrument["tick_size"]))
        side = "buy" if plan.config.direction == "long" else "sell"
        raw_entry = plan.support_price if side == "buy" else plan.pressure_price
        entry = snap_to_increment(raw_entry, tick, "down" if side == "buy" else "up")
        if not candles:
            return None, "缺少已收盘K线，无法计算入场保护止损"
        if plan.config.stop_basis == "three_candle" and len(candles) < 3:
            return None, "最近三根已收盘K线不足；无法计算入场保护止损"
        raw_stop = calculate_initial_stop(
            plan.config, plan.config.direction, entry_reference=entry,
            signal_candle=candles[-1], recent_candles=candles[-3:], atr_value=current_atr,
        )
        stop = snap_to_increment(raw_stop, tick, "up" if side == "buy" else "down")
        if entry <= 0 or stop <= 0 or (side == "buy" and stop >= entry) or (side == "sell" and stop <= entry):
            return None, "GTC线价/ATR止损关系无效；本次未挂GTC"
        target = plan.pressure_price if side == "buy" else plan.support_price
        target_buffer = current_atr * plan.config.target_buffer_atr
        total_cost = Decimal(session["fee_rate"]) + Decimal(session["slippage_rate"])
        available = (target - entry - target_buffer) if side == "buy" else (entry - target - target_buffer)
        space_after_cost = available - (entry + target) * total_cost
        if space_after_cost / abs(entry - stop) < plan.config.min_space_r:
            return None, f"GTC线价估算空间不足最低{plan.config.min_space_r}R；本次未挂GTC"
        multiplier = Decimal(str(instrument["ct_val"])) * Decimal(str(instrument["ct_mult"]))
        try:
            size, _ = size_usdt_linear_contracts(
                risk_amount=plan.config.risk_amount, entry=entry, stop=stop,
                fee_rate=Decimal(session["fee_rate"]), slippage_rate=Decimal(session["slippage_rate"]),
                lot_size=Decimal(str(instrument["lot_size"])), min_size=Decimal(str(instrument["min_size"])),
                contract_multiplier=multiplier,
            )
        except ValueError as exc:
            return None, f"GTC风险定仓不可用（{exc}）；本次未挂GTC"
        signal_key = f"resting:{session['baseline_candle_ts']}:{session.get('last_signal_key') or 'initial'}"
        stop_algo_cl_ord_id = new_execution_client_order_id(session["plan_id"], signal_key, "stop")
        try:
            take_profit_price = price_for_net_r(
                entry=entry, stop=stop, direction=plan.config.direction,
                target_r=_take_profit_r(plan), fee_rate=Decimal(session["fee_rate"]),
                slippage_rate=Decimal(session["slippage_rate"]), contract_multiplier=multiplier,
                tick_size=tick,
            )
        except ValueError as exc:
            return None, f"止盈保护价不可用；本次未挂GTC：{exc}"
        take_profit_algo_cl_ord_id = stop_algo_cl_ord_id
        return ({
            "entry_kind": "gtc", "signal_key": signal_key,
            "cl_ord_id": new_execution_client_order_id(session["plan_id"], signal_key, "gtc"),
            "stop_algo_cl_ord_id": stop_algo_cl_ord_id,
            "take_profit_algo_cl_ord_id": take_profit_algo_cl_ord_id,
            "take_profit_price": "" if take_profit_price is None else str(take_profit_price),
            "take_profit_target_r": str(_take_profit_r(plan)), "entry_reference": str(entry),
            "side": side, "size": str(size), "price": str(entry), "stop": str(stop),
            "state": "prepared", "created_at_ms": int(time.time() * 1000),
            "expires_at_ms": int(session["expires_at_candle_ts"]) + 3_600_000,
            "atr_at_arm": str(current_atr),
        }, "")

    def _submit_resting_entry(self, session_id: str) -> None:
        session = self._sessions[session_id]
        intent = session.get("resting_entry_intent")
        if not isinstance(intent, dict):
            return
        runtime, client = self._runtime_client(session_id)
        if int(intent.get("expires_at_ms") or 0) > 0 and int(time.time() * 1000) >= int(intent["expires_at_ms"]):
            # The durable ID might already have been submitted before a crash;
            # reconcile/cancel it rather than placing a late new order.
            self._reconcile_resting_entry(session_id, force_cancel=True, cancel_mode="expire")
            return
        self._assert_write_enabled(session_id)
        self._persist_session(session_id)  # stable clOrdId is durable before POST
        try:
            _place_area_shape_order(client, "limit",
                runtime.credentials, _strategy_config(session, runtime), inst_id=session["symbol"],
                side=intent["side"], size=Decimal(intent["size"]), limit_price=Decimal(intent["price"]),
                stop_price=Decimal(intent["stop"]), client_order_id=intent["cl_ord_id"],
                stop_algo_client_order_id=intent["stop_algo_cl_ord_id"],
                take_profit_price=(Decimal(intent["take_profit_price"])
                                   if intent.get("take_profit_price") else None),
            )
        except Exception:
            # The POST may have succeeded; only query this original clOrdId.
            pass
        self._reconcile_resting_entry(session_id, force_cancel=False)

    def _reconcile_resting_entry(
        self, session_id: str, *, force_cancel: bool = False, cancel_mode: str = "",
    ) -> str:
        session = self._sessions[session_id]
        intent = session.get("resting_entry_intent")
        if not isinstance(intent, dict):
            return "clear"
        runtime, client = self._runtime_client(session_id, protective=True)
        config = _strategy_config(session, runtime)
        try:
            status = client.get_order(runtime.credentials, config, inst_id=session["symbol"], cl_ord_id=intent["cl_ord_id"])
        except Exception as exc:
            self._set_status(session_id, "manual_review", f"GTC原订单回查未确认；禁止提交第二种入场：{exc}")
            return "error"
        raw = status.raw if isinstance(status.raw, dict) else {}
        if (str(raw.get("instId") or "").upper() != str(session["symbol"]).upper()
                or str(raw.get("clOrdId") or "") != str(intent["cl_ord_id"])
                or str(raw.get("side") or "").lower() != str(intent["side"]).lower()
                or str(raw.get("ordType") or "").lower() != "limit"
                or Decimal(str(raw.get("sz") or "0")) != Decimal(str(intent["size"]))
                or Decimal(str(raw.get("px") or "0")) != Decimal(str(intent["price"]))):
            self._set_status(session_id, "manual_review", "GTC订单回读与冻结合约/方向/类型/数量/价格不符")
            return "error"
        state = str(status.state or "").lower()
        now_ms = int(time.time() * 1000)
        if not cancel_mode and not force_cancel and now_ms >= int(intent.get("expires_at_ms") or 0):
            cancel_mode = "expire"
        if state in {"live", "partially_filled"} and (force_cancel or cancel_mode or Decimal(str(status.filled_size or 0)) > 0):
            verified_before_cancel = copy.deepcopy(status)
            try:
                client.cancel_order(runtime.credentials, config, inst_id=session["symbol"], cl_ord_id=intent["cl_ord_id"])
                status = client.get_order(runtime.credentials, config, inst_id=session["symbol"], cl_ord_id=intent["cl_ord_id"])
                state = str(status.state or "").lower()
            except Exception as exc:
                if self._protect_uncertain_entry_cancel(session_id, verified_before_cancel, intent):
                    return "filled"
                self._set_status(session_id, "manual_review", f"GTC撤单后成交/终态未确认；停止FOK并需人工核对：{exc}")
                return "error"
            raw = status.raw if isinstance(status.raw, dict) else {}
            if (str(raw.get("instId") or "").upper() != str(session["symbol"]).upper()
                    or str(raw.get("clOrdId") or "") != str(intent["cl_ord_id"])
                    or str(raw.get("side") or "").lower() != str(intent["side"]).lower()
                    or str(raw.get("ordType") or "").lower() != "limit"
                    or Decimal(str(raw.get("sz") or "0")) != Decimal(str(intent["size"]))
                    or Decimal(str(raw.get("px") or "0")) != Decimal(str(intent["price"]))):
                if self._protect_uncertain_entry_cancel(session_id, verified_before_cancel, intent):
                    return "filled"
                self._set_status(session_id, "manual_review", "GTC撤单后订单身份回读不符；停止FOK并需人工核对")
                return "error"
            if state in {"live", "partially_filled"}:
                if self._protect_uncertain_entry_cancel(session_id, status, intent):
                    return "filled"
                self._set_status(session_id, "manual_review", "GTC撤单后仍显示活动；停止FOK并需人工核对")
                return "error"
        filled = Decimal(str(status.filled_size or 0))
        if filled > 0:
            if state not in {"filled", "canceled", "partially_canceled"} or not status.avg_price or Decimal(str(status.avg_price)) <= 0:
                self._set_status(session_id, "manual_review", "GTC已报告成交但终态/均价未核实；停止FOK并需人工核对")
                return "error"
            fill_time = int((status.raw or {}).get("fillTime") or 0)
            if not fill_time and status.ord_id:
                try:
                    fills = client.get_fills_history(runtime.credentials, environment=_session_environment(session), inst_types=("SWAP",), limit=100)
                    fill_time = max((int(item.fill_time or 0) for item in fills
                                     if item.order_id == status.ord_id and item.inst_id.upper() == session["symbol"].upper()), default=0)
                except Exception:
                    fill_time = 0  # Protect first; unknown entry time disables stage management.
            self._protect_fill(session_id, status, fill_time, intent=intent)
            return "filled"
        if state in {"live", "partially_filled"}:
            return "live"
        if state in {"canceled", "partially_canceled", "order_failed"}:
            with self._lock:
                current = self._sessions[session_id]
                current["resting_entry_intent"] = None
                current.setdefault("events", []).append({"kind": "gtc_terminal", "ts": now_ms,
                                                           "message": "GTC已终结且无成交；互斥入场可继续"})
                self._persist_locked()
            if cancel_mode:
                self._finish_resting_cancel(session_id, cancel_mode)
            return "clear"
        self._set_status(session_id, "manual_review", f"GTC订单状态{state or 'unknown'}无法确认；停止新入场")
        return "error"

    def _finish_resting_cancel(self, session_id: str, cancel_mode: str) -> None:
        with self._lock:
            current = self._sessions[session_id]
            current["resting_entry_intent"] = None
            current["signal_pending"] = None
            current.pop("cancel_requested", None)
            current["status"] = (
                "cancelled" if cancel_mode == "cancel" else
                "paused" if cancel_mode == "pause" else
                "expired" if cancel_mode == "expire" else
                "needs_manual_rearm" if cancel_mode == "shutdown" else "armed"
            )
            current["paused"] = current["status"] != "armed"
            if current["status"] in {"cancelled", "expired"}:
                self._reservations.discard(self._reservation_key(current))
            current.setdefault("events", []).append({"kind": current["status"], "ts": int(time.time() * 1000),
                                                       "message": "GTC已确认撤销且无成交"})
            self._persist_locked()
            result = copy.deepcopy(current)
        self.session_updated.emit(result)

    def reconcile_readonly(self, session_id: str, *, runtime: Any, unlocked: bool) -> dict[str, Any]:
        """Read back exchange facts, but never restore write permission."""
        with self._lock:
            session = self._sessions[session_id]
            if session.get("status") not in {"needs_manual_rearm", "manual_review"}:
                raise ValueError("会话不处于只读恢复状态")
            self._verify_runtime_identity(session, runtime, unlocked)
        client = self._client_factory()
        try:
            account = client.get_account_config(runtime.credentials, environment=_session_environment(session))
            uid = str((account.raw or {}).get("uid") or "")
            if uid != str(session.get("account_uid")):
                raise PermissionError("当前OKX UID与原会话不一致")
            report: dict[str, Any] = {"entry": None, "resting_entry": None, "position": [], "stop": None, "take_profit": None}
            resting = session.get("resting_entry_intent") or {}
            if resting.get("cl_ord_id"):
                status = client.get_order(runtime.credentials, _strategy_config(session, runtime),
                                          inst_id=session["symbol"], cl_ord_id=resting["cl_ord_id"])
                report["resting_entry"] = {
                    "state": status.state, "order_id": status.ord_id,
                    "filled_size": str(status.filled_size or 0), "average_price": str(status.avg_price or 0),
                }
            intent = session.get("pending_intent") or {}
            if intent.get("cl_ord_id"):
                if str(intent.get("entry_kind") or "trigger") in {"fok", "gtc"}:
                    status = client.get_order(runtime.credentials, _strategy_config(session, runtime),
                                              inst_id=session["symbol"], cl_ord_id=intent["cl_ord_id"])
                    report["entry"] = {
                        "state": status.state, "order_id": status.ord_id,
                        "ord_type": str((status.raw or {}).get("ordType") or ""),
                        "filled_size": str(status.filled_size or 0), "average_price": str(status.avg_price or 0),
                    }
                else:
                    algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                                 algo_cl_ord_id=intent["cl_ord_id"])
                    report["entry"] = None if algo is None else {
                        "state": algo.get("state"), "trigger": algo.get("triggerPx"),
                        "trigger_type": algo.get("triggerPxType"), "order_ids": list(algo.get("ordIdList") or []),
                    }
            positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
            report["position"] = [
                {"inst_id": p.inst_id, "position": str(p.position), "pos_side": str(p.pos_side)}
                for p in positions if p.inst_id.upper() == str(session["symbol"]).upper()
            ]
            position = session.get("position") or {}
            for leg, id_key, kind, trigger_key in (
                ("stop", "stop_algo_cl_ord_id", "sl", "slTriggerPx"),
                ("take_profit", "take_profit_algo_cl_ord_id", "tp", "tpTriggerPx"),
            ):
                client_id = position.get(id_key)
                if client_id:
                    raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=client_id)
                    report[leg] = None if raw is None else {
                        "state": raw.get("state"), "inst_id": raw.get("instId"), "trigger": raw.get(trigger_key),
                        "trigger_type": raw.get("slTriggerPxType" if kind == "sl" else "tpTriggerPxType"),
                        "size": raw.get("sz"), "side": raw.get("side"),
                    }
        finally:
            try:
                client.close()
            except Exception:
                pass
        with self._lock:
            session["readonly_reconciliation"] = report
            session["readonly_reconciled_at_ms"] = int(time.time() * 1000)
            session.setdefault("events", []).append({"kind": "readonly_reconciled", "ts": int(time.time() * 1000),
                                                       "message": "已回读UID、原入场、仓位及保护单；仍只读"})
            self._persist_locked()
            result = dict(session)
            result["status"] = "needs_manual_rearm"
            return result

    def rearm_session(self, session_id: str, *, runtime: Any, unlocked: bool) -> dict[str, Any]:
        """Explicitly restore management only after a successful read-only reconciliation."""
        with self._lock:
            session = self._sessions[session_id]
            if session.get("status") not in {"needs_manual_rearm", "manual_review"}:
                raise ValueError("会话不处于只读恢复状态")
            if not session.get("readonly_reconciled_at_ms") or not isinstance(session.get("readonly_reconciliation"), dict):
                raise ValueError("必须先完成只读交易所对账")
            self._verify_runtime_identity(session, runtime, unlocked)
        client = self._client_factory()
        try:
            account = client.get_account_config(runtime.credentials, environment=_session_environment(session))
            if str((account.raw or {}).get("uid") or "") != str(session.get("account_uid")):
                raise PermissionError("当前OKX UID与原会话不一致")
            positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
            matching = [p for p in positions if p.inst_id.upper() == str(session["symbol"]).upper() and Decimal(p.position) != 0]
            intent = session.get("pending_intent")
            intent_status = None
            resting = session.get("resting_entry_intent")
            if isinstance(resting, dict):
                config = _strategy_config(session, runtime)
                resting_status = client.get_order(runtime.credentials, config, inst_id=session["symbol"],
                                                 cl_ord_id=resting["cl_ord_id"])
                if not _verified_entry_fill_order(resting_status, session, resting, order_type="limit"):
                    raise RuntimeError("原GTC身份或冻结参数不符；保持只读")
                state = str(resting_status.state or "").lower()
                if state in {"live", "partially_filled"}:
                    client.cancel_order(runtime.credentials, config, inst_id=session["symbol"],
                                        cl_ord_id=resting["cl_ord_id"])
                    resting_status = client.get_order(runtime.credentials, config, inst_id=session["symbol"],
                                                     cl_ord_id=resting["cl_ord_id"])
                    state = str(resting_status.state or "").lower()
                if not _verified_entry_fill_order(resting_status, session, resting, order_type="limit"):
                    raise RuntimeError("撤单后原GTC身份或冻结参数不符；保持只读")
                raw = resting_status.raw if isinstance(resting_status.raw, dict) else {}
                if (state not in {"filled", "canceled", "partially_canceled", "order_failed"}
                        or str(raw.get("clOrdId") or "") != str(resting["cl_ord_id"])
                        or str(raw.get("ordType") or "").lower() != "limit"
                        or str(raw.get("instId") or "").upper() != str(session["symbol"]).upper()):
                    raise RuntimeError("原GTC终态或订单身份未核实；保持只读")
                if Decimal(str(resting_status.filled_size or 0)) > 0:
                    intent = dict(resting)
                    session["pending_intent"] = intent
                    intent_status = resting_status
                    if not matching:
                        raise RuntimeError("重启后发现GTC成交但当前仓位为空；保持只读并人工核对")
                else:
                    session["resting_entry_intent"] = None
            if intent:
                config = _strategy_config(session, runtime)
                if str(intent.get("entry_kind") or "trigger") in {"fok", "gtc"}:
                    expected_ord_type = "limit" if str(intent.get("entry_kind")) == "gtc" else "fok"
                    intent_status = client.get_order(runtime.credentials, config, inst_id=session["symbol"],
                                                     cl_ord_id=intent["cl_ord_id"])
                    if not _verified_entry_fill_order(intent_status, session, intent, order_type=expected_ord_type):
                        raise RuntimeError("原入场订单身份或冻结参数不符；保持只读")
                    entry_state = str(intent_status.state or "").lower()
                    if entry_state in {"live", "partially_filled"}:
                        client.cancel_order(runtime.credentials, config, inst_id=session["symbol"],
                                            cl_ord_id=intent["cl_ord_id"])
                        intent_status = client.get_order(runtime.credentials, config, inst_id=session["symbol"],
                                                         cl_ord_id=intent["cl_ord_id"])
                        entry_state = str(intent_status.state or "").lower()
                    if entry_state not in {"filled", "canceled", "partially_canceled"}:
                        raise RuntimeError("原FOK订单终态未核实；保持只读且不重发")
                    if not _verified_entry_fill_order(intent_status, session, intent, order_type=expected_ord_type):
                        raise RuntimeError("撤单后原入场订单身份或冻结参数不符；保持只读")
                    raw = intent_status.raw if isinstance(intent_status.raw, dict) else {}
                    if (str(raw.get("instId") or "").upper() != str(session["symbol"]).upper()
                            or str(raw.get("clOrdId") or "") != str(intent["cl_ord_id"])
                            or str(raw.get("side") or "").lower() != str(intent.get("side") or "").lower()
                            or str(raw.get("ordType") or "").lower() != expected_ord_type
                            or Decimal(str(raw.get("sz") or "0")) != Decimal(str(intent.get("size") or "0"))):
                        raise RuntimeError("原FOK订单回读与持久化意图不符；保持只读人工核对")
                    if Decimal(str(intent_status.filled_size or 0)) <= 0:
                        intent_status = None
                        session["pending_intent"] = None
                else:
                    entry_algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                                       algo_cl_ord_id=intent["cl_ord_id"])
                    if entry_algo is None:
                        raise RuntimeError("原触发委托不存在；保持只读且不重发")
                    if not _verified_trigger_entry(entry_algo, session, intent):
                        raise RuntimeError("原触发委托身份或冻结参数不符；保持只读")
                    entry_state = str(entry_algo.get("state") or "").lower()
                    if entry_state == "live":
                        client.cancel_algo_order(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                                 algo_cl_ord_id=intent["cl_ord_id"])
                        entry_algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                                           algo_cl_ord_id=intent["cl_ord_id"])
                        entry_state = str((entry_algo or {}).get("state") or "").lower()
                        if not _verified_trigger_entry(entry_algo, session, intent):
                            raise RuntimeError("撤单后原触发委托身份或冻结参数不符；保持只读")
                    child_ids = (entry_algo or {}).get("ordIdList") if isinstance((entry_algo or {}).get("ordIdList"), list) else []
                    if not child_ids and (entry_algo or {}).get("ordId"):
                        child_ids = [(entry_algo or {}).get("ordId")]
                    if entry_state == "effective" or (entry_state == "canceled" and child_ids):
                        if len(child_ids) != 1:
                            raise RuntimeError("原触发委托已触发但成交子委托无法唯一确认；保持只读")
                        intent_status = client.get_order(runtime.credentials, config,
                                                         inst_id=session["symbol"], ord_id=str(child_ids[0]))
                        if not _verified_entry_fill_order(intent_status, session, intent, order_id=str(child_ids[0])):
                            raise RuntimeError("原触发成交子单身份或冻结参数不符；保持只读")
                        if str(intent_status.state).lower() in {"live", "partially_filled"}:
                            client.cancel_order(runtime.credentials, config, inst_id=session["symbol"], ord_id=str(child_ids[0]))
                            intent_status = client.get_order(runtime.credentials, config,
                                                             inst_id=session["symbol"], ord_id=str(child_ids[0]))
                            if (not _verified_entry_fill_order(intent_status, session, intent, order_id=str(child_ids[0]))
                                    or str(intent_status.state).lower() in {"live", "partially_filled"}):
                                raise RuntimeError("触发子单余量未确认终结；保持只读")
                    elif entry_state == "canceled" and not child_ids:
                        intent_status = None
                        session["pending_intent"] = None
                    else:
                        raise RuntimeError("原触发委托终态未核实；保持只读且不重发")
            if matching:
                position = session.get("position")
                if not isinstance(position, dict) and intent_status is not None:
                    # Crash window: the exchange accepted/finalized the entry,
                    # but the process stopped before persisting the fill snapshot.
                    # Rebuild only from the original durable intent and exact
                    # exchange fill/position facts; never submit another entry.
                    filled = Decimal(str(intent_status.filled_size or "0"))
                    avg_price = Decimal(str(intent_status.avg_price or "0"))
                    requested = Decimal(str(intent.get("size") or "0"))
                    fill_time = int((intent_status.raw or {}).get("fillTime") or 0)
                    if (intent_status.state not in {"filled", "canceled", "partially_canceled"} or filled <= 0 or filled > requested
                            or avg_price <= 0 or len(matching) != 1
                            or not _position_matches(matching[0], str(session["direction"]), filled, _session_position_mode(session, runtime))):
                        raise RuntimeError("成交快照缺失且原委托/当前仓位无法精确归属；保持只读人工核对")
                    if fill_time <= 0:
                        fills = client.get_fills_history(runtime.credentials, environment=_session_environment(session), inst_types=("SWAP",), limit=100)
                        fill_time = max((int(item.fill_time or 0) for item in fills
                                         if item.order_id == intent_status.ord_id
                                         and item.inst_id.upper() == session["symbol"].upper()), default=0)
                    if fill_time <= 0:
                        raise RuntimeError("原入场成交时间无法核实；保持只读人工核对")
                    plan = AreaShapePlan.from_dict(session["plan_snapshot"])
                    stop = Decimal(str(intent.get("stop") or "0"))
                    entry_is_safe = (stop > 0 and (stop < avg_price if plan.config.direction == "long" else stop > avg_price))
                    if not entry_is_safe:
                        raise RuntimeError("成交快照重建时冻结止损与真实均价关系无效；保持只读")
                    instrument = session.get("instrument") or {}
                    multiplier = Decimal(str(instrument.get("ct_val") or "0")) * Decimal(str(instrument.get("ct_mult") or "0"))
                    if multiplier <= 0 or int(session.get("rule_version") or 0) != AREA_SHAPE_EXECUTION_RULE_VERSION:
                        raise RuntimeError("成交快照重建缺少有效冻结合约规格/规则版本；保持只读")
                    position = {
                        "entry": str(avg_price), "initial_stop": str(stop), "stop": str(stop),
                        "risk_distance": str(abs(avg_price - stop)), "size": str(filled),
                        "requested_size": str(requested), "direction": plan.config.direction,
                        "pos_side": _position_side(plan.config.direction, _session_position_mode(session, runtime)),
                        "entry_ts": fill_time,
                        "entry_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(fill_time / 1000)),
                        "stop_algo_cl_ord_id": str(intent.get("stop_algo_cl_ord_id") or ""),
                        "stop_algo_id": "", "stop_verified_at_ms": 0, "stage": "initial",
                        "favorable_price": str(avg_price),
                        "last_minute_cursor_ts": max(0, (fill_time + 59_999) // 60_000 * 60_000 - 60_000),
                        "rule_version": session["rule_version"], "contract_multiplier": str(multiplier),
                        "fee_rate": session["fee_rate"], "slippage_rate": session["slippage_rate"],
                        "partial_fill_requires_review": filled != requested, "account_position_mismatch": False,
                        "reconstructed_after_crash": True,
                    }
                    with self._lock:
                        session["position"] = position
                        session["actual_fill_recorded_at_ms"] = int(time.time() * 1000)
                        session["status"] = "needs_manual_rearm"
                        self._persist_locked()
                if not isinstance(position, dict) or len(matching) != 1 or not _position_matches(
                    matching[0], str(session["direction"]), Decimal(position["size"]), _session_position_mode(session, runtime)
                ):
                    raise RuntimeError("只读对账发现未能归属的活动仓位；禁止自动接管")
                reconstructed_partial = bool(position.get("reconstructed_after_crash")
                                             and position.get("partial_fill_requires_review"))
                if ((position.get("partial_fill_requires_review") and not reconstructed_partial)
                        or position.get("account_position_mismatch")
                        or not int(position.get("entry_ts") or 0)
                        or int(position.get("rule_version") or 0) != AREA_SHAPE_EXECUTION_RULE_VERSION):
                    raise RuntimeError("部分成交/账户聚合/成交时间/规则版本需要人工核对；保持只读")
                if intent_status is not None:
                    if Decimal(str(intent_status.filled_size or 0)) != Decimal(position["size"]):
                        raise RuntimeError("原入场累计成交与当前可归属仓位不同；保持只读")
                    session["pending_intent"] = None
                stop_id = str(position.get("stop_algo_cl_ord_id") or (session.get("protection_intent") or {}).get("cl_ord_id") or "")
                if not stop_id:
                    raise RuntimeError("活动仓位缺少原保护编号；无法安全恢复")
                raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
                pending_amend = position.get("pending_amend_intent") or {}
                expected_stop = Decimal(str(pending_amend.get("trigger") or position["stop"]))
                expected_side = "sell" if session["direction"] == "long" else "buy"
                # An interrupted amend may not have reached OKX. A verified old
                # stop remains valid protection; keep the pending upgrade for
                # the regular current-price checks instead of rejecting recovery.
                amend_confirmed = bool(pending_amend)
                if pending_amend and _verified_protection(
                    raw, inst_id=session["symbol"], side=expected_side,
                    size=Decimal(position["size"]), trigger=Decimal(position["stop"]), kind="sl",
                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime),
                ):
                    expected_stop = Decimal(position["stop"])
                    amend_confirmed = False
                valid_stop = _verified_protection(raw, inst_id=session["symbol"], side=expected_side,
                                                  size=Decimal(position["size"]), trigger=expected_stop,
                                                  kind="sl", pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime))
                if not valid_stop:
                    state = str((raw or {}).get("state", "")).lower()
                    if state not in {"", "canceled", "order_failed", "partially_failed"}:
                        raise RuntimeError("活动仓位止损参数异常或已触发；保持只读并人工核对")
                    fallback_id = str((session.get("protection_intent") or {}).get("cl_ord_id") or
                                      new_execution_client_order_id(session["plan_id"], str(intent.get("signal_key") or "recovery"), "stopfallback"))
                    with self._lock:
                        session["protection_intent"] = {"kind": "sl", "cl_ord_id": fallback_id,
                                                         "trigger": str(expected_stop), "size": str(position["size"]),
                                                         "side": expected_side}
                        position["stop_algo_cl_ord_id"] = fallback_id
                        self._persist_locked()
                    last = client.get_trigger_price(session["symbol"], "last", environment=_session_environment(session), max_cached_age_seconds=5)
                    if not (expected_stop < last if session["direction"] == "long" else expected_stop > last):
                        raise RuntimeError("当前last已越过冻结止损；不挂可能立即触发的保护，保持人工接管")
                    fallback = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=fallback_id)
                    if fallback is None or str(fallback.get("state", "")).lower() in {"canceled", "order_failed", "partially_failed"}:
                        _place_area_shape_order(client, "protection",
                            runtime.credentials, _strategy_config(session, runtime), inst_id=session["symbol"],
                            side=expected_side, size=Decimal(position["size"]), pos_side=position.get("pos_side"),
                            trigger_price=expected_stop, client_order_id=fallback_id, protection_kind="sl",
                        )
                    raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=fallback_id)
                    if not _verified_protection(raw, inst_id=session["symbol"], side=expected_side,
                                                size=Decimal(position["size"]), trigger=expected_stop, kind="sl",
                                                pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
                        raise RuntimeError("原止损不存在，fallback保护未能回读验证；保持未保护人工处置")
                    stop_id = fallback_id
                    position["stop_algo_cl_ord_id"] = fallback_id
                if amend_confirmed:
                    position["stop"] = str(expected_stop)
                    position["stage"] = str(pending_amend.get("stage") or position.get("stage"))
                    position.pop("pending_amend_intent", None)
                    position.pop("pending_stop_price", None)
                    position.pop("pending_stage", None)
                position["stop_algo_id"] = str(raw.get("algoId") or "")
                position["stop_verified_at_ms"] = int(time.time() * 1000)
                plan = AreaShapePlan.from_dict(session["plan_snapshot"])
                if reconstructed_partial:
                    new_status = "manual_control"
                    session.setdefault("events", []).append({"kind": "recovered_partial_fill_protected",
                                                               "ts": int(time.time() * 1000),
                                                               "message": "恢复到原订单部分成交；已核验已成交数量止损保护，余量/账户竞态需人工接管"})
                else:
                    tp_id = str(position.get("take_profit_algo_cl_ord_id") or "")
                    if not tp_id:
                        raise RuntimeError("止盈保护编号缺失；活动仓位保持只读，不能恢复为受保护")
                    tp_raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session),
                                                   inst_id=session["symbol"], algo_cl_ord_id=tp_id)
                    if str((tp_raw or {}).get("state", "")).lower() == "effective":
                        raise RuntimeError("止盈保护已触发/终态但账户仍有活动仓位；保持只读并人工核对成交")
                    instrument = session.get("instrument") or {}
                    multiplier = Decimal(str(instrument.get("ct_val", "0"))) * Decimal(str(instrument.get("ct_mult", "0")))
                    frozen_tp = (position.get("take_profit") or (position.get("take_profit_intent") or {}).get("trigger")
                                 or intent.get("take_profit_price"))
                    if not frozen_tp:
                        raise RuntimeError("恢复缺少原始冻结止盈价；保持只读，禁止套用新规则")
                    target = Decimal(str(frozen_tp))
                    if not _verified_protection(
                        tp_raw, inst_id=session["symbol"],
                        side="sell" if plan.config.direction == "long" else "buy",
                        size=Decimal(position["size"]), trigger=target, kind="tp",
                        pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime),
                    ):
                        raise RuntimeError("止盈保护未回读为live且参数一致；活动仓位保持只读")
                    position["take_profit"] = str(target)
                    position["take_profit_verified_at_ms"] = int(time.time() * 1000)
                if not reconstructed_partial:
                    new_status = "protected"
            else:
                if intent_status is not None and Decimal(str(intent_status.filled_size or 0)) > 0:
                    session["recovery_unresolved_fill"] = {
                        "entry_order_id": intent_status.ord_id, "filled_size": str(intent_status.filled_size),
                        "avg_price": str(intent_status.avg_price or ""), "state": intent_status.state,
                    }
                    session["status"] = "manual_review"
                    session.setdefault("events", []).append({"kind": "recovery_unresolved_fill", "ts": int(time.time() * 1000),
                                                               "message": "原入场曾成交但当前净仓位为空；不重挂保护、不重新开仓，需人工对账成交/平仓"})
                    self._persist_locked()
                    raise RuntimeError("已发现原入场成交但当前仓位为空；未发现可归属保护平仓事实，保持只读人工核对")
                if isinstance(session.get("position"), dict):
                    if self._confirm_exchange_closed(session_id, runtime, client, session, session["position"]):
                        try:
                            client.close()
                        except Exception:
                            pass
                        return copy.deepcopy(self._sessions[session_id])
                    raise RuntimeError("原会话曾有成交仓位但当前为空，缺少关联保护成交证据；不把旧计划重置为新入场")
                candles = [item for item in client.get_candles_history(session["symbol"], "1H", limit=240) if item.confirmed]
                if not candles:
                    raise RuntimeError("恢复新入场监控缺少最新已收盘1H基线")
                baseline = max(int(item.ts) for item in candles)
                check_area_shape_execution_conflicts(client, runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"])
                if _session_environment(session) == "live":
                    # Recovery is for reconciling and protecting an existing
                    # position. It must never create a fresh live entry epoch.
                    session["resting_entry_intent"] = None
                    session["pending_intent"] = None
                    session["signal_pending"] = None
                    session["status"] = "cancelled"
                    session.setdefault("events", []).append({
                        "kind": "live_recovery_ended", "ts": int(time.time() * 1000),
                        "message": "只读对账确认无可归属仓位/未成交委托；实盘旧会话安全结束，不恢复新开仓，请新建计划",
                    })
                    self._reservations.discard(self._reservation_key(session))
                    new_status = "cancelled"
                else:
                    session["signal_cursor_ts"] = baseline
                    session["baseline_candle_ts"] = baseline
                    session["pending_intent"] = None
                    session["signal_pending"] = None
                    plan = AreaShapePlan.from_dict(session["plan_snapshot"])
                    if plan.config.resting_entry_enabled or plan.config.entry_trigger_mode == "direct_touch":
                        session["resting_entry_intent"], reason = self._prepare_resting_entry(session, plan, candles)
                        if reason:
                            session.setdefault("events", []).append({"kind": "resting_entry_skipped", "ts": int(time.time() * 1000),
                                                                      "message": reason})
                    new_status = "armed"
        except Exception:
            try:
                client.close()
            except Exception:
                pass
            raise
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("应用正在关闭；恢复保持只读")
            session = self._sessions[session_id]
            session["status"] = new_status
            session.pop("write_disabled", None)
            session["rearmed_at_ms"] = int(time.time() * 1000)
            session.setdefault("events", []).append({"kind": "rearmed", "ts": session["rearmed_at_ms"],
                                                       "message": "人工确认只读对账后恢复；活动仓位仅管理，不把纸面或旧信号转成新仓"})
            self._runtimes[session_id] = runtime
            self._clients[session_id] = client
            self._persist_locked()
            result = copy.deepcopy(session)
        if new_status == "armed" and result.get("resting_entry_intent"):
            self._submit_resting_entry(session_id)
            result = copy.deepcopy(self._sessions[session_id])
        self.session_updated.emit(result)
        return result

    @staticmethod
    def _verify_runtime_identity(session: dict[str, Any], runtime: Any, unlocked: bool) -> None:
        environment = _session_environment(session)
        if not unlocked or runtime is None or str(getattr(runtime, "environment", "")).lower() != environment:
            raise PermissionError("操作必须显式解锁且绑定冻结的执行环境")
        if environment == "live":
            if not str(session.get("account_uid") or "").strip():
                raise PermissionError("live session缺少冻结account_uid；禁止执行")
            if str(session.get("mode") or "") != "live_exchange":
                raise PermissionError("live session缺少或包含无效mode；禁止执行")
            if str(session.get("trade_mode") or "").lower() not in {"cross", "isolated"}:
                raise PermissionError("live session缺少有效冻结trade_mode；禁止执行")
            if str(session.get("position_mode") or "").lower() not in {"net", "long_short"}:
                raise PermissionError("live session缺少有效冻结position_mode；禁止执行")
        runtime_profile = str(getattr(runtime, "credential_profile_name", "")
                              or getattr(getattr(runtime, "credentials", None), "profile_name", ""))
        credential_profile = str(getattr(getattr(runtime, "credentials", None), "profile_name", "") or runtime_profile)
        if runtime_profile != str(session.get("profile_name")) or credential_profile != str(session.get("profile_name")):
            raise PermissionError("当前profile与原会话不一致")
        if credential_hash(runtime.credentials.api_key) != session.get("api_key_sha256"):
            raise PermissionError("API key指纹与原会话不同，须人工重新核对")
        snapshot = session.get("plan_snapshot")
        if not isinstance(snapshot, dict):
            raise PermissionError("冻结计划快照缺失；禁止执行")
        config = snapshot.get("config") if isinstance(snapshot.get("config"), dict) else {}
        frozen_direction = str(session.get("direction") or (config.get("direction") if environment == "demo" else ""))
        frozen_trigger = str(session.get("trigger_price_type") or ("last" if environment == "demo" else ""))
        if (str(snapshot.get("plan_id", "")) != str(session.get("plan_id", ""))
                or str(snapshot.get("profile_name", "")) != str(session.get("profile_name", ""))
                or str(snapshot.get("environment", "")).lower() != environment
                or str(snapshot.get("symbol", "")).upper() != str(session.get("symbol", "")).upper()
                or str(config.get("direction", "")).lower() != frozen_direction.lower()
                or str(config.get("trigger_price_type", "last" if environment == "demo" else "")).lower() != frozen_trigger.lower()
                or frozen_trigger.lower() != "last"):
            raise PermissionError("冻结session与plan快照的环境/产品/方向/触发价不一致")
        if session.get("trade_mode") and str(getattr(runtime, "trade_mode", "")) != str(session["trade_mode"]):
            raise PermissionError("运行时交易模式与冻结会话不同")
        if session.get("position_mode") and str(getattr(runtime, "position_mode", "")) != str(session["position_mode"]):
            raise PermissionError("运行时持仓模式与冻结会话不同")

    def _run(self) -> None:
        while not self._stop.wait(self._poll_seconds):
            with self._lock:
                active = [(key, dict(session)) for key, session in self._sessions.items() if session.get("status") in _ACTIVE]
            for session_id, snapshot in active:
                try:
                    self._tick(session_id, snapshot)
                except Exception as exc:
                    with self._lock:
                        current = self._sessions.get(session_id)
                        if current is not None:
                            current["last_error"] = str(exc)
                            current["last_error_at_ms"] = int(time.time() * 1000)
                            current["retry_count"] = int(current.get("retry_count", 0)) + 1
                            if not self._store_write_blocked:
                                try:
                                    self._persist_locked()
                                except Exception:
                                    # _persist_locked latched write blocking and
                                    # emitted the storage failure separately.
                                    pass
                    self.status_changed.emit(f"demo执行暂时失败，已保留状态并重试：{exc}")

    def _tick(self, session_id: str, session: dict[str, Any]) -> None:
        with self._lock:
            current = self._sessions.get(session_id)
            if current is None:
                return
            session = copy.deepcopy(current)
        status = str(session.get("status"))
        if status == "armed":
            cancel_mode = str(session.get("cancel_requested") or "")
            if cancel_mode and not isinstance(session.get("resting_entry_intent"), dict):
                self._finish_resting_cancel(session_id, cancel_mode)
                return
            if session.get("resting_entry_intent") or cancel_mode:
                result = self._reconcile_resting_entry(
                    session_id, force_cancel=bool(cancel_mode), cancel_mode=cancel_mode,
                )
                if result in {"filled", "error"} or self._sessions[session_id].get("status") != "armed":
                    return
            self._check_new_signals(session_id)
        elif status == "signal_pending":
            self._try_live_entry(session_id)
        elif status == "entry_intent":
            # A crash/exception after intent persistence may already have sent
            # the order. Query the original id only; never issue a replacement.
            self._reconcile_entry_intent(session_id)
        elif status == "position_unprotected":
            self._retry_initial_protection(session_id)
        elif status == "tp_pending":
            self._ensure_take_profit(session_id)
        elif status == "protected":
            self._manage_filled_position(session_id)
        elif status == "closing_pending":
            runtime, client = self._runtime_client(session_id)
            position = self._sessions[session_id].get("position")
            if isinstance(position, dict):
                self._confirm_if_position_closed(session_id, runtime, client, self._sessions[session_id], position)
            else:
                self._set_status(session_id, "manual_review", "平仓对账缺少本会话持仓快照")

    def _retry_initial_protection(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id, protective=True)
        position = session.get("position")
        if not isinstance(position, dict):
            self._set_status(session_id, "manual_review", "已确认成交但缺少仓位快照；人工核对")
            return
        if position.get("unsafe_initial_stop"):
            self._set_status(session_id, "manual_control", "成交价越过冻结初始止损；禁止自动重挂该止损，需人工立即处置")
            return
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        intent = session.get("protection_intent") or {}
        signal_key = str((session.get("pending_intent") or {}).get("signal_key") or session.get("last_signal_key") or "")
        if not signal_key:
            self._set_status(session_id, "manual_review", "保护恢复缺少原信号编号；禁止换ID")
            return
        stop_id = str(intent.get("cl_ord_id") or position.get("stop_algo_cl_ord_id") or "")
        stop = Decimal(position["stop"])
        size = Decimal(position["size"])
        closing_side = "sell" if plan.config.direction == "long" else "buy"
        pos_side = _position_side(plan.config.direction, _session_position_mode(session, runtime))
        if stop_id:
            raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
            if _verified_protection(raw, inst_id=session["symbol"], side=closing_side, size=size,
                                    trigger=stop, kind="sl", pos_side=pos_side, position_mode=_session_position_mode(session, runtime)):
                position["stop_algo_cl_ord_id"] = stop_id
                position["stop_algo_id"] = str(raw.get("algoId") or "")
                position["stop_verified_at_ms"] = int(time.time() * 1000)
                self._ensure_take_profit(session_id)
                if session.get("status") in {"tp_pending", "manual_review", "manual_control", "closed", "closing_pending"}:
                    return
                if position.get("partial_fill_requires_review") or position.get("account_position_mismatch"):
                    self._set_status(session_id, "manual_control", "已保护已知成交量，但部分成交/账户聚合不一致需人工接管；停止后续自动交易管理")
                    return
                self._set_status(session_id, "protected", "原client id保护单已回读live验证；恢复持仓管理")
                return
            state = str((raw or {}).get("state", "")).lower()
            if state == "effective":
                if self._confirm_if_position_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                    return
                self._set_status(session_id, "manual_control", "保护单已触发但成交/平仓证据不完整；停止写入并需人工核对")
                return
            if raw is not None and state not in {"canceled", "order_failed", "partially_failed"}:
                self._set_status(session_id, "position_unprotected", f"原保护单状态{state or '未知'}；只读等待，禁止另建保护单")
                return
        else:
            stop_id = new_execution_client_order_id(session["plan_id"], signal_key, "stopfallback")
            intent = {"kind": "sl", "cl_ord_id": stop_id, "trigger": str(stop), "size": str(size), "side": closing_side}
            with self._lock:
                session["protection_intent"] = intent
                self._persist_locked()
        # On timeout, this stable id remains persisted and is queried before any
        # subsequent same-id retry. Never mint another protection identifier.
        fallback_id = new_execution_client_order_id(session["plan_id"], signal_key, "stopfallback")
        if stop_id != fallback_id:
            stop_id = fallback_id
            intent = {"kind": "sl", "cl_ord_id": stop_id, "trigger": str(stop), "size": str(size), "side": closing_side}
            with self._lock:
                session["protection_intent"] = intent
                self._persist_locked()
        existing = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
        state = str((existing or {}).get("state", "")).lower()
        if existing is None or state in {"canceled", "order_failed", "partially_failed"}:
            _place_area_shape_order(client, "protection",
                runtime.credentials, _strategy_config(session, runtime), inst_id=session["symbol"], side=closing_side,
                size=size, pos_side=pos_side, trigger_price=stop, client_order_id=stop_id, protection_kind="sl",
            )
            existing = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
        if not _verified_protection(existing, inst_id=session["symbol"], side=closing_side, size=size,
                                    trigger=stop, kind="sl", pos_side=pos_side, position_mode=_session_position_mode(session, runtime)):
            self._set_status(session_id, "position_unprotected", "同一fallback保护编号回读未验证；保留成交和intent，持续重试/人工处置")
            return
        position["stop_algo_cl_ord_id"] = stop_id
        position["stop_algo_id"] = str(existing.get("algoId") or "")
        position["stop_verified_at_ms"] = int(time.time() * 1000)
        self._ensure_take_profit(session_id)
        if session.get("status") in {"tp_pending", "manual_review", "closed", "closing_pending"}:
            return
        if position.get("partial_fill_requires_review") or position.get("account_position_mismatch"):
            with self._lock:
                session.pop("cancel_requested", None)
                session["position"] = position
                session["status"] = "manual_control"
                self._persist_locked()
            self.session_updated.emit(copy.deepcopy(session))
            self.status_changed.emit("已保护已知成交量，但部分成交/账户聚合不一致需人工接管；停止后续自动交易管理")
            return
        with self._lock:
            session.pop("cancel_requested", None)
            session["position"] = position
            session["status"] = "protected"
            session["protection_intent"] = intent
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(session))
        self._manage_filled_position(session_id)

    def _check_new_signals(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        snapshot = AreaShapePlan.from_dict(session["plan_snapshot"])
        candles = sorted((item for item in client.get_candles_history(session["symbol"], "1H", limit=240) if item.confirmed), key=lambda item: item.ts)
        cursor = int(session.get("signal_cursor_ts", 0))
        if snapshot.config.entry_trigger_mode == "direct_touch":
            # Direct-touch plans use the exchange-hosted GTC order submitted
            # at arm time. Do not create a second local FOK entry.
            return
        if not candles or candles[-1].ts <= cursor:
            return
        fresh = [item for item in candles if item.ts > cursor]
        if not fresh or fresh[0].ts != cursor + 3_600_000 or any(b.ts - a.ts != 3_600_000 for a, b in zip(fresh, fresh[1:])):
            self._set_status(session_id, "manual_review", "1H确认历史缺口/超出可用页；停止新入场并需人工核对")
            return
        if candles[-1].ts > int(snapshot.expires_at_candle_ts):
            if session.get("resting_entry_intent"):
                with self._lock:
                    current = self._sessions[session_id]
                    current["paused"] = True
                    current["cancel_requested"] = "expire"
                    self._persist_locked()
                self._reconcile_resting_entry(session_id, force_cancel=True, cancel_mode="expire")
            else:
                self._set_status(session_id, "expired", "demo信号计划已过期")
            return
        snapshot.execution_mode = "paper"
        snapshot.paper_position = None
        snapshot.status = "monitoring"
        snapshot.last_candle_ts = cursor
        snapshot.activation_candle_ts = int(session["baseline_candle_ts"])
        event = snapshot.observe(candles)
        with self._lock:
            current = self._sessions[session_id]
            if self._stop.is_set() or current.get("status") != "armed" or current.get("paused"):
                return
            current["signal_cursor_ts"] = max(cursor, snapshot.last_candle_ts)
            if event and event.get("kind") == "paper_pending":
                pending = event.get("paper_position")
                signal_ts = int(event.get("candle_ts", 0) or 0)
                now_ms = int(time.time() * 1000)
                if (not isinstance(pending, dict) or signal_ts <= int(current.get("baseline_candle_ts", 0))
                        or signal_ts > int(current.get("expires_at_candle_ts", 0))
                        or now_ms - (signal_ts + 3_600_000) > 5 * 60_000):
                    current["status"] = "manual_review"
                    current["last_error"] = "信号已过启用基线/计划有效期/实时确认窗口；禁止历史信号开仓"
                else:
                    current["signal_event"] = dict(event)
                    current["signal_pending"] = dict(pending)
                    current["status"] = "signal_pending"
                    current["last_signal_key"] = str(pending.get("signal_key", ""))
            self._persist_locked()
            emitted = dict(current)
        self.session_updated.emit(emitted)

    def _try_live_entry(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        signal = dict(session["signal_pending"])
        if session.get("resting_entry_intent"):
            result = self._reconcile_resting_entry(session_id, force_cancel=True)
            if result != "clear":
                return
            session = self._sessions[session_id]
            runtime, client = self._runtime_client(session_id)
        signal_ts = int(signal.get("signal_candle_ts", 0))
        now_ms = int(time.time() * 1000)
        if signal_ts <= int(session.get("baseline_candle_ts", 0)) or signal_ts > int(session.get("expires_at_candle_ts", 0)):
            self._set_status(session_id, "manual_review", "信号不在启用后计划有效期内；禁止执行")
            return
        if now_ms - (signal_ts + 3_600_000) > 5 * 60_000:
            self._consume_signal(session_id, "信号已超出实时确认窗口；不使用历史K线追单")
            return
        if int(session.get("signal_cursor_ts", 0)) < signal_ts:
            self._set_status(session_id, "manual_review", "信号游标落后于信号K线；停止执行")
            return
        entry_kind = "fok" if plan.config.entry_mode == "next_open" else "trigger"
        if entry_kind == "trigger" and now_ms > signal_ts + 2 * 3_600_000:
            self._consume_signal(session_id, "形态突破等待超过默认60分钟；已放弃该信号")
            return
        trigger = Decimal(str(signal.get("entry_trigger") or "0"))
        if entry_kind == "trigger":
            current_price = client.get_trigger_price(session["symbol"], "last", environment=_session_environment(session), max_cached_age_seconds=5)
            if trigger <= 0:
                self._consume_signal(session_id, "形态信号缺少有效突破触发价；跳过")
                return
            crossed = current_price >= trigger if plan.config.direction == "long" else current_price <= trigger
            if crossed:
                self._consume_signal(session_id, "价格已越过条件单触发位；不追价")
                return
        instrument = client.get_area_shape_instrument(session["symbol"])
        tick, lot, minimum, multiplier = validate_usdt_linear_swap(instrument, session["symbol"])
        book = client.get_order_book(session["symbol"], depth=1)
        try:
            book_ts = int(book.raw.get("ts") or 0)
        except (TypeError, ValueError):
            book_ts = 0
        if (book_ts <= 0 or abs(int(time.time() * 1000) - book_ts) > 5_000
                or not book.bids or not book.asks or book.bids[0][0] <= 0
                or book.asks[0][0] <= 0 or book.bids[0][0] > book.asks[0][0]):
            self._consume_signal(session_id, "实时盘口缺失、过期或价差结构异常；跳过入场")
            return
        side = "buy" if plan.config.direction == "long" else "sell"
        if side == "buy":
            reference_quote = book.asks[0][0]
            if entry_kind == "trigger":
                if reference_quote >= trigger:
                    self._consume_signal(session_id, "ask已触及条件单触发区；不追价挂单")
                    return
                limit = trigger * (Decimal("1") + Decimal(session["slippage_rate"]))
            else:
                # FOK follows the local line+pattern confirmation immediately;
                # the best ask plus configured slippage is a strict price cap.
                limit = reference_quote * (Decimal("1") + Decimal(session["slippage_rate"]))
        else:
            reference_quote = book.bids[0][0]
            if entry_kind == "trigger":
                if reference_quote <= trigger:
                    self._consume_signal(session_id, "bid已触及条件单触发区；不追价挂单")
                    return
                limit = trigger * (Decimal("1") - Decimal(session["slippage_rate"]))
            else:
                limit = reference_quote * (Decimal("1") - Decimal(session["slippage_rate"]))
        stop = Decimal(str(signal["stop"]))
        limit = snap_to_increment(limit, tick, "down" if side == "buy" else "up")
        normalized_stop = snap_to_increment(stop, tick, "nearest")
        if normalized_stop != stop:
            self._consume_signal(session_id, "冻结初始止损价不符合tickSz；拒绝扩大风险后下单")
            return
        if (entry_kind == "trigger" and
                ((side == "buy" and not stop < trigger <= limit) or (side == "sell" and not limit <= trigger < stop))):
            self._consume_signal(session_id, "触发价/限价/冻结止损关系不安全；跳过该信号")
            return
        if (entry_kind == "fok" and ((side == "buy" and stop >= limit) or (side == "sell" and stop <= limit))):
            self._consume_signal(session_id, "当前盘口价格与冻结初始止损关系不安全；拒绝提交FOK")
            return
        costs = Decimal(session["fee_rate"]) + Decimal(session["slippage_rate"])
        risk_per_contract = abs(limit - stop) * multiplier + (limit + stop) * multiplier * costs
        target = plan.pressure_price if plan.config.direction == "long" else plan.support_price
        buffer = Decimal(str(signal.get("target_buffer_atr") or "0"))
        available = (target - limit - buffer) if plan.config.direction == "long" else (limit - target - buffer)
        space_after_cost = available - (limit + target) * costs
        if space_after_cost / abs(limit - stop) < plan.config.min_space_r:
            self._consume_signal(session_id, "实时last/盘口计算后可用空间不足最低R门槛；未下单")
            return
        size, _ = size_usdt_linear_contracts(
            risk_amount=plan.config.risk_amount, entry=limit, stop=stop,
            fee_rate=Decimal(session["fee_rate"]), slippage_rate=Decimal(session["slippage_rate"]),
            lot_size=lot, min_size=minimum, contract_multiplier=multiplier,
        )
        check_area_shape_execution_conflicts(client, runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"])
        intent = create_persisted_entry_intent(
            {**session, "plan_id": session["plan_id"]}, signal_key=str(signal["signal_key"]),
            side=side, size=size, price=limit, stop=stop,
        )
        intent["entry_kind"] = entry_kind
        if entry_kind == "trigger":
            intent["trigger_px"] = str(snap_to_increment(trigger, tick, "up" if side == "buy" else "down"))
            intent["trigger_expires_at_ms"] = signal_ts + 2 * 3_600_000
        intent["stop_algo_cl_ord_id"] = new_execution_client_order_id(session["plan_id"], str(signal["signal_key"]), "stop")
        try:
            frozen_tp = price_for_net_r(
                entry=limit, stop=stop, direction=plan.config.direction,
                target_r=_take_profit_r(plan), fee_rate=Decimal(session["fee_rate"]),
                slippage_rate=Decimal(session["slippage_rate"]),
                contract_multiplier=multiplier, tick_size=tick,
            )
        except ValueError as exc:
            self._consume_signal(session_id, f"附带止盈保护价无效；未提交入场：{exc}")
            return
        intent.update({"take_profit_price": str(frozen_tp),
                       "take_profit_algo_cl_ord_id": intent["stop_algo_cl_ord_id"],
                       "take_profit_target_r": str(_take_profit_r(plan)), "entry_reference": str(limit)})
        with self._lock:
            current = self._sessions[session_id]
            if (self._stop.is_set() or current.get("status") != "signal_pending"
                    or current.get("pending_intent") or current.get("paused")
                    or current.get("cancel_requested")):
                return
            current["pending_intent"] = intent
            current["status"] = "entry_intent"
            self._persist_locked()  # stable client id is durable before POST
        self._submit_or_reconcile_entry(session_id)

    def _submit_or_reconcile_entry(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        intent = session["pending_intent"]
        config = _strategy_config(session, runtime)
        self._assert_write_enabled(session_id)
        try:
            if str(intent.get("entry_kind") or "trigger") == "fok":
                _place_area_shape_order(client, "fok",
                    runtime.credentials, config, inst_id=session["symbol"], side=intent["side"],
                    size=Decimal(intent["size"]), limit_price=Decimal(intent["price"]),
                    stop_price=Decimal(intent["stop"]), client_order_id=intent["cl_ord_id"],
                    stop_algo_client_order_id=intent["stop_algo_cl_ord_id"],
                    take_profit_price=Decimal(intent["take_profit_price"]) if intent.get("take_profit_price") else None,
                )
            else:
                _place_area_shape_order(client, "trigger",
                    runtime.credentials, config, inst_id=session["symbol"], side=intent["side"],
                    size=Decimal(intent["size"]), trigger_price=Decimal(intent["trigger_px"]),
                    limit_price=Decimal(intent["price"]), stop_price=Decimal(intent["stop"]),
                    take_profit_price=Decimal(intent["take_profit_price"]) if intent.get("take_profit_price") else None,
                    client_order_id=intent["cl_ord_id"], stop_algo_client_order_id=intent["stop_algo_cl_ord_id"],
                )
        except Exception:
            # The POST may have succeeded. Reconcile this same algoClOrdId only.
            pass
        self._reconcile_entry_intent(session_id)

    def _protect_fill(self, session_id: str, order_status: Any, fill_time: int, *, intent: dict[str, Any] | None = None) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id, protective=True)
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        intent = intent or session["pending_intent"]
        entry = Decimal(order_status.avg_price)
        size = Decimal(order_status.filled_size)
        stop = Decimal(intent["stop"])
        # Persist every confirmed fill before protection/API follow-up. This
        # allows retries to reconcile protection without losing actual exposure.
        frozen_instrument = session["instrument"]
        frozen_multiplier = Decimal(frozen_instrument["ct_val"]) * Decimal(frozen_instrument["ct_mult"])
        position = {
            "entry": str(entry), "initial_stop": str(stop), "stop": str(stop),
            "risk_distance": str(abs(entry - stop)), "size": str(size),
            "requested_size": str(intent["size"]), "direction": plan.config.direction,
            "pos_side": _position_side(plan.config.direction, _session_position_mode(session, runtime)),
            "entry_ts": int(fill_time or 0), "entry_time": "" if not fill_time else time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(fill_time / 1000)),
            "stop_algo_cl_ord_id": str(intent["stop_algo_cl_ord_id"]), "stop_algo_id": "",
            "stop_verified_at_ms": 0, "stage": "initial", "favorable_price": str(entry),
            "last_minute_cursor_ts": 0 if not fill_time else max(0, (fill_time + 59_999) // 60_000 * 60_000 - 60_000),
            "rule_version": session["rule_version"], "contract_multiplier": str(frozen_multiplier),
            "fee_rate": session["fee_rate"], "slippage_rate": session["slippage_rate"],
            "partial_fill_requires_review": size != Decimal(intent["size"]) or bool(intent.get("entry_cancel_unverified")),
        }
        if intent.get("take_profit_price") and intent.get("take_profit_algo_cl_ord_id"):
            position["take_profit_algo_cl_ord_id"] = str(intent["take_profit_algo_cl_ord_id"])
            position["take_profit_intent"] = {
                "trigger": str(intent["take_profit_price"]), "size": str(size),
                "side": "sell" if plan.config.direction == "long" else "buy",
                "rule_version": session["rule_version"],
            }
            position["take_profit_target_r"] = str(intent.get("take_profit_target_r") or _take_profit_r(plan))
            position["entry_reference"] = str(intent.get("entry_reference") or intent["price"])
        with self._lock:
            current = self._sessions[session_id]
            current["position"] = position
            current["status"] = "position_unprotected"
            current["actual_fill_recorded_at_ms"] = int(time.time() * 1000)
            current["pending_intent"] = dict(intent)
            if str(intent.get("entry_kind")) == "gtc":
                current["resting_entry_intent"] = None
                current.pop("cancel_requested", None)
            self._persist_locked()
        unsafe_stop = (plan.config.direction == "long" and stop >= entry) or (plan.config.direction == "short" and stop <= entry)
        positions_flat = False
        try:
            positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
            matching = [item for item in positions if item.inst_id.upper() == session["symbol"].upper()]
            positions_flat = not matching
            position_mismatch = (len(matching) != 1 or not _position_matches(
                matching[0], plan.config.direction, size, _session_position_mode(session, runtime)))
        except Exception:
            # A failed exposure read cannot erase a confirmed fill. Attempt
            # protection for the known fill, then require manual reconciliation.
            position_mismatch = True
        position["account_position_mismatch"] = bool(position_mismatch)
        self._persist_session(session_id)
        config = _strategy_config(session, runtime)
        closing_side = "sell" if plan.config.direction == "long" else "buy"
        stop_id = str(intent["stop_algo_cl_ord_id"])
        algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
        if unsafe_stop:
            position["unsafe_initial_stop"] = True
            self._persist_session(session_id)
            if str((algo or {}).get("state", "")).lower() == "effective":
                if self._confirm_if_position_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                    return
            self._set_status(session_id, "manual_control", "真实成交价已越过冻结初始止损；不重挂可能立即触发的错误保护，需人工立即处置")
            return
        if str((algo or {}).get("state", "")).lower() == "effective":
            if self._confirm_if_position_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                return
            self._set_status(session_id, "manual_control", "初始保护已触发；成交/仓位尚未完整核实，需人工接管")
            return
        if not _verified_protection(algo, inst_id=session["symbol"], side=closing_side, size=size,
                                    trigger=stop, kind="sl", pos_side=_position_side(plan.config.direction, _session_position_mode(session, runtime)),
                                    position_mode=_session_position_mode(session, runtime)):
            if algo is None or str(algo.get("state", "")).lower() in {"canceled", "order_failed", "partially_failed"}:
                fallback_id = new_execution_client_order_id(session["plan_id"], str(intent["signal_key"]), "stopfallback")
                with self._lock:
                    current = self._sessions[session_id]
                    current["protection_intent"] = {"kind": "sl", "cl_ord_id": fallback_id,
                                                     "trigger": str(stop), "size": str(size), "side": closing_side}
                    self._persist_locked()
                prior = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=fallback_id)
                if prior is None or str(prior.get("state", "")).lower() in {"canceled", "order_failed", "partially_failed"}:
                    try:
                        _place_area_shape_order(client, "protection",
                            runtime.credentials, config, inst_id=session["symbol"], side=closing_side,
                            size=size, pos_side=_position_side(plan.config.direction, _session_position_mode(session, runtime)),
                            trigger_price=stop, client_order_id=fallback_id, protection_kind="sl",
                        )
                    except Exception:
                        # Keep the intent and only inspect that same identifier on retry.
                        raise
                stop_id = fallback_id
                algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=stop_id)
            if not _verified_protection(algo, inst_id=session["symbol"], side=closing_side, size=size,
                                        trigger=stop, kind="sl", pos_side=_position_side(plan.config.direction, _session_position_mode(session, runtime)),
                                        position_mode=_session_position_mode(session, runtime)):
                self._set_status(session_id, "position_unprotected", "初始止损未被交易所回读验证；停止新开仓并需人工立即处置")
                return
        position["stop_algo_cl_ord_id"] = stop_id
        position["stop_algo_id"] = str(algo.get("algoId") or "")
        position["stop_verified_at_ms"] = int(time.time() * 1000)
        with self._lock:
            current = self._sessions[session_id]
            current["position"] = position
            current["status"] = "protected"
            current["pending_intent"] = None if order_status.state == "filled" and size == Decimal(intent["size"]) else dict(intent)
            if size != Decimal(intent["size"]):
                position["partial_fill_requires_review"] = True
            self._persist_locked()
        actual_cost_rate = Decimal(session["fee_rate"]) + Decimal(session["slippage_rate"])
        actual_risk = size * (abs(entry - stop) * Decimal(position["contract_multiplier"])
                              + (entry + stop) * Decimal(position["contract_multiplier"]) * actual_cost_rate)
        position["actual_risk_amount"] = str(actual_risk)
        position["risk_budget_amount"] = str(plan.config.risk_amount)
        if actual_risk > plan.config.risk_amount:
            position["risk_budget_exceeded"] = True
        self._persist_session(session_id)
        if not fill_time:
            self._set_status(session_id, "manual_review", "成交事实与初始保护已保存，但交易所成交时间未能核实；保护保留，停止阶段管理需人工核对")
            return
        self._ensure_take_profit(session_id)
        if session.get("status") in {"tp_pending", "manual_review", "closed", "closing_pending"}:
            return
        if size != Decimal(intent["size"]):
            self._set_status(session_id, "manual_control", "入场单出现部分成交；已先保护已知成交数量，余量/竞态需人工核对")
            return
        if position_mismatch:
            self._set_status(session_id, "manual_control", "已保护本订单确认成交数量；账户聚合仓位与本单不一致，停止自动管理并需人工接管")
            return
        if actual_risk > plan.config.risk_amount:
            self._set_status(session_id, "manual_control", f"实际成交含费用风险{actual_risk}超过预算{plan.config.risk_amount}；保护单保留，需人工减仓/接管")
        self.session_updated.emit(copy.deepcopy(self._sessions[session_id]))

    def _ensure_take_profit(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        position = session["position"]
        entry, stop = Decimal(position["entry"]), Decimal(position["initial_stop"])
        frozen = session.get("instrument") or {}
        tick = Decimal(str(frozen.get("tick_size", "0")))
        multiplier = Decimal(str(position.get("contract_multiplier", "0")))
        intent = position.get("take_profit_intent") or {}
        if intent:
            if (Decimal(str(intent.get("size", "0"))) != Decimal(position["size"])
                    or str(intent.get("side", "")) != ("sell" if plan.config.direction == "long" else "buy")):
                self._set_status(session_id, "manual_review", "止盈保护持久化意图与仓位不一致；禁止更换请求参数")
                return
            target = Decimal(str(intent.get("trigger", "0")))
        elif position.get("take_profit"):
            # Reconciliation must retain the original frozen target even when
            # the actual fill improved on the entry reference.
            target = Decimal(str(position["take_profit"]))
        else:
            target = price_for_net_r(entry=entry, stop=stop, direction=plan.config.direction,
                                     target_r=_take_profit_r(plan), fee_rate=Decimal(session["fee_rate"]),
                                     slippage_rate=Decimal(session["slippage_rate"]), contract_multiplier=multiplier,
                                     tick_size=tick)
        if target <= 0:
            self._set_status(session_id, "manual_review", "固定目标价无效；保留交易所止损并需人工核对")
            return
        closing_side = "sell" if plan.config.direction == "long" else "buy"
        signal_key = str(session.get("last_signal_key") or session.get("signal_pending", {}).get("signal_key") or "unknown")
        tp_id = str(position.get("take_profit_algo_cl_ord_id") or new_execution_client_order_id(session["plan_id"], signal_key, "takeprofit"))
        config = _strategy_config(session, runtime)
        try:
            with self._lock:
                position["take_profit_algo_cl_ord_id"] = tp_id
                position["take_profit_intent"] = {"trigger": str(target), "size": str(position["size"]),
                                                   "side": closing_side, "rule_version": session["rule_version"]}
                self._persist_locked()
            existing = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=tp_id)
            if str((existing or {}).get("state", "")).lower() == "effective":
                if self._confirm_if_position_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                    return
                self._set_status(session_id, "manual_control", "止盈单已触发但成交/平仓证据不完整；停止写入并需人工核对")
                return
            if (str((existing or {}).get("state", "")).lower() == "live"
                    and position.get("stop_algo_cl_ord_id") == tp_id
                    and not _verified_protection(existing, inst_id=session["symbol"], side=closing_side,
                                                 size=Decimal(position["size"]), trigger=target, kind="tp",
                                                 pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime))):
                # A partially filled parent may expose only its SL child. Keep
                # that protection and give the standalone TP its own stable ID.
                tp_id = new_execution_client_order_id(session["plan_id"], tp_id, "tpfallback")
                with self._lock:
                    position["take_profit_algo_cl_ord_id"] = tp_id
                    self._persist_locked()
                existing = client.get_order_algo(runtime.credentials, environment=_session_environment(session),
                                                 inst_id=session["symbol"], algo_cl_ord_id=tp_id)
            if not _verified_protection(existing, inst_id=session["symbol"], side=closing_side,
                                        size=Decimal(position["size"]), trigger=target, kind="tp",
                                        pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
                if existing is None or str(existing.get("state", "")).lower() in {"canceled", "order_failed", "partially_failed"}:
                    self._assert_write_enabled(session_id)
                    _place_area_shape_order(client, "protection",
                        runtime.credentials, config, inst_id=session["symbol"], side=closing_side,
                        size=Decimal(position["size"]), pos_side=position.get("pos_side"),
                        trigger_price=target, client_order_id=tp_id, protection_kind="tp",
                    )
            algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=tp_id)
        except Exception as exc:
            self._set_status(session_id, "tp_pending", f"止盈保护请求/回读暂未确认；已保留止损，将按原ID核对：{exc}")
            return
        if not _verified_protection(algo, inst_id=session["symbol"], side=closing_side,
                                    size=Decimal(position["size"]), trigger=target, kind="tp",
                                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
            self._set_status(session_id, "tp_pending", "止盈保护单未回读到live/精确参数；已保留止损，将继续按原ID核对")
            return
        position["take_profit"] = str(target)
        position["take_profit_algo_cl_ord_id"] = tp_id
        position["take_profit_verified_at_ms"] = int(time.time() * 1000)
        position.pop("take_profit_intent", None)
        with self._lock:
            needs_review = (position.get("partial_fill_requires_review") or position.get("account_position_mismatch")
                            or position.get("risk_budget_exceeded"))
            self._sessions[session_id]["status"] = "manual_control" if needs_review else "protected"
            session.setdefault("events", []).append({"kind": "take_profit_verified", "ts": int(time.time() * 1000),
                "trigger": str(target), "algo_cl_ord_id": tp_id, "source": "last",
                "target_r": str(position.get("take_profit_target_r") or _take_profit_r(plan)),
                "entry_reference": str(position.get("entry_reference") or entry), "actual_entry": str(entry)})
        self._persist_session(session_id)

    def _manage_filled_position(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        position = session.get("position")
        if not isinstance(position, dict):
            self._set_status(session_id, "manual_review", "活动保护状态缺少成交仓位记录")
            return
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
        matching = [item for item in positions if item.inst_id.upper() == session["symbol"].upper() and Decimal(item.position) != 0]
        if not matching:
            if self._confirm_exchange_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                return
        if len(matching) != 1 or not _position_matches(matching[0], plan.config.direction, Decimal(position["size"]), _session_position_mode(session, runtime)):
            self._set_status(session_id, "manual_control", "账户仓位数量/方向已变化；暂停自动改单，不自动平仓")
            return
        algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                     algo_id=position.get("stop_algo_id") or None,
                                     algo_cl_ord_id=position.get("stop_algo_cl_ord_id") or None)
        if str((algo or {}).get("state", "")).lower() == "effective":
            if self._confirm_exchange_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                return
        if algo is None and position.get("pending_amend_intent"):
            # A delayed read after an amend is not proof that the old stop has
            # disappeared. Keep the same identifiers and retry; never fall back
            # to creating an initial stop from this path.
            self.status_changed.emit("动态止损改单待回读；保留原编号，继续核对交易所状态")
            return
        current_stop = _decimal_or_none((algo or {}).get("slTriggerPx"))
        if current_stop is None or not _verified_protection(
                algo, inst_id=session["symbol"], side="sell" if plan.config.direction == "long" else "buy",
                size=Decimal(position["size"]), trigger=current_stop, kind="sl",
                pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
            self._set_status(session_id, "manual_control", "已核验的交易所止损被撤销、变更或已触发；停止自动覆盖，需核对仓位与保护")
            return
        if current_stop != Decimal(position["stop"]):
            pending_price = _decimal_or_none(position.get("pending_stop_price"))
            if pending_price is not None and current_stop == pending_price:
                position["stop"] = str(pending_price)
                position["stage"] = str(position.get("pending_stage") or position.get("stage"))
                position.pop("pending_stop_price", None)
                position.pop("pending_stage", None)
                position.pop("pending_amend_intent", None)
                session.setdefault("events", []).append({"kind": "stop_amend_reconciled", "ts": int(time.time()*1000),
                    "stop": str(pending_price), "stage": position["stage"], "source": "last"})
                self._persist_session(session_id)
            else:
                self._set_status(session_id, "manual_control", "交易所止损价与本会话最近核验值不同；视为人工接管，停止覆盖")
                return
        tp_id = position.get("take_profit_algo_cl_ord_id")
        if not tp_id or not position.get("take_profit_verified_at_ms"):
            self._set_status(session_id, "tp_pending", "止盈保护尚未完成回读核验；保留止损并按原ID恢复")
            return
        tp = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=tp_id)
        if str((tp or {}).get("state", "")).lower() == "effective":
            if self._confirm_exchange_closed(session_id, runtime, client, session, position) or session.get("status") == "closing_pending":
                return
        if not _verified_protection(tp, inst_id=session["symbol"], side="sell" if plan.config.direction == "long" else "buy",
                                    size=Decimal(position["size"]), trigger=Decimal(position["take_profit"]), kind="tp",
                                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
            self._set_status(session_id, "manual_control", "已核验止盈单发生变化/终态；视为手动接管，保留现有止损")
            return
        if plan.config.exit_template == "fixed_1r" and not plan.config.break_even_enabled:
            return
        price = client.get_trigger_price(session["symbol"], "last", environment=_session_environment(session), max_cached_age_seconds=5)
        engine = StrategyEngine(client, lambda _message: None)
        first_minute = (int(position["entry_ts"]) + 59_999) // 60_000 * 60_000
        cursor = int(position.get("last_minute_cursor_ts", 0)) or first_minute - 60_000
        scan_error = ""
        try:
            favorable, new_cursor = engine._scan_dynamic_stop_minute_candles(
                inst_id=session["symbol"], trigger_type="last", last_candle_ts=cursor,
                minimum_candle_ts=first_minute,
                direction=plan.config.direction,
            )
            if new_cursor is None or (cursor is not None and new_cursor < cursor):
                raise RuntimeError("分钟补查游标无效")
            now_minute = int(time.time() * 1000) // 60_000 * 60_000
            expected_closed_minute = now_minute - 60_000
            if new_cursor < expected_closed_minute - 60_000:
                # The helper returns only a contiguous confirmed cursor, but
                # favorable prices elsewhere in the valid holding interval
                # are still evidence of a reached stage. Retain both facts.
                scan_error = "分钟历史存在未补齐区间，将从连续确认游标继续补查"
        except Exception as exc:
            favorable, new_cursor = None, cursor
            scan_error = str(exc)
        prior_favorable = Decimal(position["favorable_price"])
        if favorable is not None:
            prior_favorable = max(prior_favorable, favorable) if plan.config.direction == "long" else min(prior_favorable, favorable)
        # The same live last-price source catches intraminute trigger levels;
        # closed-minute history remains the catch-up source after missed polls.
        prior_favorable = max(prior_favorable, price) if plan.config.direction == "long" else min(prior_favorable, price)
        position["favorable_price"] = str(prior_favorable)
        position["last_minute_cursor_ts"] = int(new_cursor or cursor or 0)
        if scan_error:
            self.status_changed.emit(f"分钟补查未完成，继续保留已确认档位和交易所保护：{scan_error}")
        entry, stop0 = Decimal(position["entry"]), Decimal(position["initial_stop"])
        protection_fee = Decimal(session["fee_rate"]) if plan.config.fee_offset_enabled else Decimal("0")
        progress = area_shape_net_progress_r(
            plan.config, direction=plan.config.direction, entry=entry, initial_stop=stop0,
            price=prior_favorable, fee_rate=protection_fee,
        )
        elapsed_bars = max(0, (int(time.time() * 1000) - int(position["entry_ts"])) // 3_600_000)
        current_net_progress = area_shape_net_progress_r(
            plan.config, direction=plan.config.direction, entry=entry, initial_stop=stop0,
            price=price, fee_rate=protection_fee,
        )
        time_be_ready = (
            plan.config.break_even_enabled and plan.config.time_break_even_enabled
            and elapsed_bars >= plan.config.time_break_even_bars and current_net_progress >= 0
        )
        pending_stage = str(position.get("pending_stage", ""))
        if (plan.config.exit_template != "fixed_1r" and
                (progress >= plan.config.lock_trigger_r or position.get("stage") == "locked" or pending_stage == "locked")):
            stage, locked_r = "locked", area_shape_lock_profit_r(plan.config, progress)
        elif plan.config.break_even_enabled and (
                progress >= plan.config.be_trigger_r or position.get("stage") == "break_even"
                or pending_stage == "break_even" or time_be_ready):
            stage, locked_r = "break_even", Decimal("0")
        else:
            self._persist_session(session_id)
            return
        candidate = price_for_net_r(entry=entry, stop=stop0, direction=plan.config.direction,
                                    target_r=locked_r, fee_rate=protection_fee,
                                    slippage_rate=Decimal("0"),
                                    contract_multiplier=Decimal(position["contract_multiplier"]),
                                    tick_size=Decimal(session["instrument"]["tick_size"]))
        old_stop = Decimal(position["stop"])
        tightens = candidate > old_stop if plan.config.direction == "long" else candidate < old_stop
        allows_now = candidate < price if plan.config.direction == "long" else candidate > price
        if not tightens:
            self._persist_session(session_id)
            return
        if not allows_now:
            position["pending_stage"] = stage
            position["pending_stop_price"] = str(candidate)
            self._persist_session(session_id)
            return
        position["pending_stage"] = stage
        position["pending_stop_price"] = str(candidate)
        position["pending_amend_intent"] = {"trigger": str(candidate), "stage": stage,
                                             "previous_trigger": str(old_stop), "created_at_ms": int(time.time() * 1000)}
        self._persist_session(session_id)
        # Read current state and current last immediately before amend; manual
        # price changes above stop this branch before overwrite.
        latest = client.get_trigger_price(session["symbol"], "last", environment=_session_environment(session))
        if not (candidate < latest if plan.config.direction == "long" else candidate > latest):
            position["pending_stage"] = stage
            position["pending_stop_price"] = str(candidate)
            self._persist_session(session_id)
            return
        final_positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
        final_matching = [item for item in final_positions
                          if item.inst_id.upper() == session["symbol"].upper() and Decimal(item.position) != 0]
        if len(final_matching) != 1 or not _position_matches(final_matching[0], plan.config.direction,
                                                              Decimal(position["size"]), _session_position_mode(session, runtime)):
            self._set_status(session_id, "manual_control", "改单前账户仓位发生变化；保留交易所止损并停止覆盖")
            return
        final_algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                           algo_id=position.get("stop_algo_id") or None,
                                           algo_cl_ord_id=position.get("stop_algo_cl_ord_id") or None)
        if not _verified_protection(final_algo, inst_id=session["symbol"],
                                    side="sell" if plan.config.direction == "long" else "buy",
                                    size=Decimal(position["size"]), trigger=old_stop, kind="sl",
                                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
            self._set_status(session_id, "manual_control", "改单前保护单回读与上次确认值不同；视为人工接管")
            return
        latest = client.get_trigger_price(session["symbol"], "last", environment=_session_environment(session), max_cached_age_seconds=5)
        if not (candidate < latest if plan.config.direction == "long" else candidate > latest):
            position["pending_stage"] = stage
            position["pending_stop_price"] = str(candidate)
            self._persist_session(session_id)
            return
        self._assert_write_enabled(session_id)
        event = {"kind": "stop_amend_requested", "ts": int(time.time() * 1000), "source": "last",
                 "favorable_price": str(prior_favorable), "current_price": str(latest),
                 "progress_r": str(progress), "locked_r": str(locked_r), "old_stop": str(old_stop),
                 "new_stop": str(candidate), "frozen_tp": position.get("take_profit"),
                 "rule_version": session["rule_version"]}
        session.setdefault("events", []).append(event)
        self._persist_session(session_id)
        try:
            client.amend_algo_order(
                runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                algo_id=position.get("stop_algo_id") or None, algo_cl_ord_id=position.get("stop_algo_cl_ord_id") or None,
                new_stop_loss_trigger_price=candidate, new_stop_loss_order_price=Decimal("-1"),
                new_stop_loss_trigger_price_type="last",
            )
        except Exception as exc:
            session.setdefault("events", []).append({"kind": "stop_amend_unconfirmed", "ts": int(time.time()*1000),
                                                      "new_stop": str(candidate), "error": str(exc)})
            self._persist_session(session_id)
            raise
        updated = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                        algo_id=position.get("stop_algo_id") or None,
                                        algo_cl_ord_id=position.get("stop_algo_cl_ord_id") or None)
        if not _verified_protection(updated, inst_id=session["symbol"], side="sell" if plan.config.direction == "long" else "buy",
                                    size=Decimal(position["size"]), trigger=candidate, kind="sl",
                                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
            # ACK is not effective-state confirmation. Reconcile this same
            # pending amend on subsequent ticks, never use initial SL fallback.
            session.setdefault("events", []).append({"kind": "stop_amend_readback_pending",
                "ts": int(time.time()*1000), "new_stop": str(candidate)})
            self._persist_session(session_id)
            self.status_changed.emit("动态止损改单待核验；保留原保护编号，下一轮继续对账")
            return
        position["stop"] = str(candidate)
        position["stage"] = stage
        position.pop("pending_stop_price", None)
        position.pop("pending_stage", None)
        position.pop("pending_amend_intent", None)
        self._persist_session(session_id)
        if position.get("stop_algo_cl_ord_id") == position.get("take_profit_algo_cl_ord_id"):
            if not _verified_protection(updated, inst_id=session["symbol"],
                                        side="sell" if plan.config.direction == "long" else "buy",
                                        size=Decimal(position["size"]), trigger=Decimal(position["take_profit"]),
                                        kind="tp", pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
                self._set_status(session_id, "manual_control", "止损已收紧，但共用附单的冻结止盈回读不符；停止覆盖并需人工核对")
                return
        session.setdefault("events", []).append({"kind": "stop_amend_verified", "ts": int(time.time()*1000),
                                                  "stop": str(candidate), "unchanged_tp": position.get("take_profit"),
                                                  "stage": stage, "source": "last"})
        self._persist_session(session_id)
        if scan_error:
            self.status_changed.emit(f"分钟补查失败，已按实时last记录有利档位；游标不前移并将重试：{scan_error}")

    def _confirm_exchange_closed(self, session_id: str, runtime: Any, client: Any,
                                 session: dict[str, Any], position: dict[str, Any]) -> bool:
        """Confirm real closing fills, then retire this session's remaining algos."""
        target_order_ids: set[str] = set()
        owned_algos: dict[str, tuple[str, Decimal]] = {}
        direction = str(position.get("direction") or session["direction"])
        expected_side = "sell" if direction == "long" else "buy"
        expected_size = Decimal(position["size"])
        terminal_seen = False
        for key, kind, price_key in (("stop_algo_cl_ord_id", "sl", "stop"),
                                     ("take_profit_algo_cl_ord_id", "tp", "take_profit")):
            order_id = position.get(key)
            if not order_id:
                continue
            trigger = _decimal_or_none(position.get(price_key))
            if trigger is None and kind == "tp":
                trigger = _decimal_or_none((position.get("take_profit_intent") or {}).get("trigger"))
            if trigger is None:
                continue
            owned_algos[str(order_id)] = (kind, trigger)
            raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"], algo_cl_ord_id=order_id)
            if str((raw or {}).get("state", "")).lower() != "effective":
                continue
            actual_trigger = _decimal_or_none(raw.get("slTriggerPx" if kind == "sl" else "tpTriggerPx"))
            allowed_triggers = {trigger}
            if kind == "sl" and position.get("pending_stop_price"):
                allowed_triggers.add(Decimal(position["pending_stop_price"]))
            if actual_trigger not in allowed_triggers or not _verified_protection(
                    {**raw, "state": "live"}, inst_id=session["symbol"], side=expected_side,
                    size=expected_size, trigger=actual_trigger, kind=kind,
                    pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
                self._set_status(session_id, "manual_control", "保护单终态身份或冻结参数不符；不采用其平仓记录")
                return False
            terminal_seen = True
            for field in ("ordId", "actualOrdId"):
                if raw.get(field):
                    target_order_ids.add(str(raw[field]))
            for value in raw.get("ordIdList", []) if isinstance(raw.get("ordIdList"), list) else []:
                if isinstance(value, dict) and value.get("ordId"):
                    target_order_ids.add(str(value["ordId"]))
                elif value:
                    target_order_ids.add(str(value))
        if not terminal_seen:
            return False
        # Fills and flat positions can be visible on different API polls. Keep
        # the reservation and continue reconciling without creating protection.
        with self._lock:
            current = self._sessions[session_id]
            if not current.get("write_disabled"):
                current["status"] = "closing_pending"
            current["closing_order_ids"] = sorted(target_order_ids)
            if not current.get("close_reconciliation_started_at_ms"):
                current["close_reconciliation_started_at_ms"] = int(time.time() * 1000)
                current.setdefault("events", []).append({"kind": "close_reconciliation_pending",
                    "ts": current["close_reconciliation_started_at_ms"],
                    "message": "保护单已触发，等待真实平仓成交与残留保护单核对"})
            self._persist_locked()
        if not target_order_ids:
            return False
        positions = client.get_positions(runtime.credentials, environment=_session_environment(session), inst_type="SWAP", prefer_cache=False)
        if any(item.inst_id.upper() == str(session["symbol"]).upper() and Decimal(item.position) != 0 for item in positions):
            return False
        entry_ts = int(position.get("entry_ts") or 0)
        try:
            fills = client.get_fills_history(runtime.credentials, environment=_session_environment(session), inst_types=("SWAP",), limit=100)
        except Exception:
            return False
        matched_fills = [item for item in fills if str(item.order_id or "") in target_order_ids
                         and item.inst_id.upper() == str(session["symbol"]).upper()
                         and str(item.side or "").lower() == expected_side
                         and int(item.fill_time or 0) >= entry_ts
                         and (not position.get("pos_side") or str(item.pos_side or "").lower() == str(position["pos_side"]).lower())]
        if not matched_fills:
            return False
        total_closed = sum((Decimal(item.fill_size or 0) for item in matched_fills), Decimal("0"))
        if total_closed != expected_size:
            return False
        close_px_notional = sum((Decimal(item.fill_size or 0) * Decimal(item.fill_price or 0) for item in matched_fills), Decimal("0"))
        avg_close = close_px_notional / total_closed if total_closed else Decimal("0")
        fees = sum((Decimal(item.fill_fee or 0) for item in matched_fills), Decimal("0"))
        # Standalone SL/TP fallbacks are not OCO siblings. A surviving order
        # must not remain armed against a later position after this one closes.
        for client_id, (kind, trigger) in owned_algos.items():
            raw = client.get_order_algo(runtime.credentials, environment=_session_environment(session),
                                        inst_id=session["symbol"], algo_cl_ord_id=client_id)
            state = str((raw or {}).get("state", "")).lower()
            if raw is None or state in {"canceled", "order_failed", "partially_failed", "effective"}:
                continue
            if not _verified_protection(raw, inst_id=session["symbol"], side=expected_side,
                                        size=expected_size, trigger=trigger, kind=kind,
                                        pos_side=position.get("pos_side"), position_mode=_session_position_mode(session, runtime)):
                self._set_status(session_id, "manual_control", "平仓后本会话残留保护参数发生变化；停止自动覆盖并人工核对")
                return False
            if self._stop.is_set():
                return False
            self._require_writable_store()
            session["closing_cleanup_intent"] = {"algo_cl_ord_id": client_id, "kind": kind}
            self._persist_session(session_id)
            client.cancel_algo_order(runtime.credentials, environment=_session_environment(session),
                                     inst_id=session["symbol"], algo_cl_ord_id=client_id)
            updated = client.get_order_algo(runtime.credentials, environment=_session_environment(session),
                                            inst_id=session["symbol"], algo_cl_ord_id=client_id)
            if updated is not None and str(updated.get("state") or "").lower() not in {
                    "canceled", "order_failed", "partially_failed", "effective"}:
                return False
            session.setdefault("events", []).append({"kind": "closed_protection_cleanup_verified",
                "ts": int(time.time() * 1000), "algo_cl_ord_id": client_id})
        with self._lock:
            current = self._sessions[session_id]
            current["status"] = "closed"
            current["closed_at_ms"] = int(time.time() * 1000)
            position["actual_close_avg_price"] = str(avg_close)
            position["actual_close_fees"] = str(fees)
            position["actual_close_filled_size"] = str(total_closed)
            current["closed_evidence"] = "terminal_protective_algo+matching_order_fills"
            current.pop("closing_cleanup_intent", None)
            current.setdefault("events", []).append({"kind": "closed", "ts": current["closed_at_ms"],
                                                       "message": "保护单终态且交易所仓位历史确认已平；释放合约会话占用"})
            self._reservations.discard(self._reservation_key(current))
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(current))
        return True

    def _confirm_if_position_closed(self, session_id: str, runtime: Any, client: Any,
                                    session: dict[str, Any], position: dict[str, Any]) -> bool:
        return self._confirm_exchange_closed(session_id, runtime, client, session, position)

    def _reconcile_entry_intent(self, session_id: str) -> None:
        session = self._sessions[session_id]
        runtime, client = self._runtime_client(session_id)
        intent = session.get("pending_intent")
        if not isinstance(intent, dict):
            self._set_status(session_id, "manual_review", "入场intent丢失")
            return
        if str(intent.get("entry_kind") or "trigger") == "fok":
            self._reconcile_fok_entry_intent(session_id, session, runtime, client, intent)
            return
        try:
            algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                         algo_cl_ord_id=intent["cl_ord_id"])
        except Exception as exc:
            self._set_status(session_id, "manual_review", f"原algoClOrdId回查未确认；禁止重新提交：{exc}")
            return
        if algo is None:
            self._set_status(session_id, "manual_review", "OKX未返回原触发委托；结果不明，禁止重新提交")
            return
        if not _verified_trigger_entry(algo, session, intent):
            self._set_status(session_id, "manual_review", "触发委托身份、方向、数量或冻结触发/限价回读不符；禁止采用其成交记录")
            return
        state = str(algo.get("state") or "").lower()
        cancel_mode = str(session.get("cancel_requested") or "")
        if state == "live" and not cancel_mode and int(intent.get("trigger_expires_at_ms") or 0) > 0:
            if int(time.time() * 1000) >= int(intent["trigger_expires_at_ms"]):
                cancel_mode = "expire"
                with self._lock:
                    self._sessions[session_id]["cancel_requested"] = cancel_mode
                    self._persist_locked()
        if state == "live" and cancel_mode:
            try:
                client.cancel_algo_order(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                         algo_cl_ord_id=intent["cl_ord_id"])
                algo = client.get_order_algo(runtime.credentials, environment=_session_environment(session), inst_id=session["symbol"],
                                             algo_cl_ord_id=intent["cl_ord_id"])
            except Exception as exc:
                self._set_status(session_id, "manual_review", f"撤销OKX触发委托后状态未确认：{exc}")
                return
            if not _verified_trigger_entry(algo, session, intent):
                self._set_status(session_id, "manual_review", "撤单后触发委托身份或冻结参数回读不符；保持人工核对")
                return
            state = str((algo or {}).get("state") or "").lower()
            if state == "live":
                self._set_status(session_id, "manual_review", "撤单请求已发出但委托仍显示live；需人工核对")
                return
        if state == "live":
            self._persist_session(session_id)
            return
        spawned = algo.get("ordIdList") if isinstance(algo.get("ordIdList"), list) else []
        if not spawned and algo.get("ordId"):
            spawned = [algo.get("ordId")]
        if state == "canceled" and not spawned:
            self._finish_entry_cancel(session_id, cancel_mode)
            return
        if state not in {"effective", "canceled"} or not spawned:
            self._set_status(session_id, "manual_review", f"触发委托终态为{state or 'unknown'}，未取得唯一成交委托；人工核对")
            return
        if len(spawned) != 1:
            self._set_status(session_id, "manual_review", "触发委托返回多个成交子委托；需人工核对")
            return
        # A cancelled parent can still have spawned a filled/partial child.
        # Reuse the regular FOK reconciliation, targeting that exact child ID.
        self._reconcile_fok_entry_intent(session_id, session, runtime, client, intent, order_id=str(spawned[0]))

    def _protect_uncertain_entry_cancel(self, session_id: str, status: Any, intent: dict[str, Any]) -> bool:
        """A failed remainder cancel cannot discard a previously verified fill."""
        filled, average = _decimal_or_none(status.filled_size), _decimal_or_none(status.avg_price)
        if filled is None or filled <= 0 or average is None or average <= 0:
            return False
        snapshot = {**intent, "entry_cancel_unverified": True}
        self._protect_fill(session_id, status, int((status.raw or {}).get("fillTime") or 0), intent=snapshot)
        return True

    def _reconcile_fok_entry_intent(
        self, session_id: str, session: dict[str, Any], runtime: Any, client: Any,
        intent: dict[str, Any], *, order_id: str | None = None,
    ) -> None:
        """Reconcile the same durable regular-order client ID; never resend a FOK."""
        config = _strategy_config(session, runtime)
        order_lookup = {"ord_id": order_id} if order_id else {"cl_ord_id": intent["cl_ord_id"]}
        try:
            status = client.get_order(
                runtime.credentials, config, inst_id=session["symbol"], **order_lookup,
            )
        except Exception as exc:
            self._set_status(session_id, "manual_review", f"原FOK clOrdId回查未确认；禁止重发：{exc}")
            return
        if not _verified_entry_fill_order(status, session, intent, order_id=order_id):
            self._set_status(session_id, "manual_review", "FOK成交委托身份、方向、类型、数量或冻结限价回读不符")
            return
        order_state = str(status.state or "").strip().lower()
        cancel_mode = str(session.get("cancel_requested") or "")
        if order_state in {"live", "partially_filled"}:
            verified_before_cancel = copy.deepcopy(status)
            try:
                client.cancel_order(runtime.credentials, config, inst_id=session["symbol"],
                                    **order_lookup)
                status = client.get_order(
                    runtime.credentials, config, inst_id=session["symbol"], **order_lookup,
                )
                order_state = str(status.state or "").strip().lower()
            except Exception as exc:
                if self._protect_uncertain_entry_cancel(session_id, verified_before_cancel, intent):
                    return
                self._set_status(session_id, "manual_review", f"FOK撤单后状态未确认：{exc}")
                return
            if not _verified_entry_fill_order(status, session, intent, order_id=order_id):
                if self._protect_uncertain_entry_cancel(session_id, verified_before_cancel, intent):
                    return
                self._set_status(session_id, "manual_review", "FOK撤单后身份回读不符；停止采用该记录")
                return
            if order_state in {"live", "partially_filled"}:
                if self._protect_uncertain_entry_cancel(session_id, status, intent):
                    return
                self._set_status(session_id, "manual_review", "FOK订单余量撤单后仍活动；需人工核对")
                return
        if not _verified_entry_fill_order(status, session, intent, order_id=order_id):
            self._set_status(session_id, "manual_review", "FOK订单回读合约、编号、方向、类型或数量与持久化意图不符")
            return
        filled = Decimal(str(status.filled_size or 0))
        if filled > 0 and status.avg_price and Decimal(str(status.avg_price)) > 0:
            fill_time = int(status.raw.get("fillTime") or 0)
            if not fill_time and status.ord_id:
                try:
                    fills = client.get_fills_history(runtime.credentials, environment=_session_environment(session), inst_types=("SWAP",), limit=100)
                    fill_time = max((int(item.fill_time or 0) for item in fills
                                     if item.order_id == status.ord_id and item.inst_id.upper() == session["symbol"].upper()), default=0)
                except Exception:
                    fill_time = 0
            self._protect_fill(session_id, status, fill_time)
            return
        if order_state in {"canceled", "filled", "partially_canceled"} and filled == 0:
            if cancel_mode:
                self._finish_entry_cancel(session_id, cancel_mode)
            else:
                self._consume_signal(session_id, "触线形态信号已提交FOK但未成交；本次信号已消费，不重复追单")
            return
        self._set_status(session_id, "manual_review", f"原FOK订单状态为{order_state or 'unknown'}，成交事实未核实")

    def _finish_entry_cancel(self, session_id: str, cancel_mode: str) -> None:
        with self._lock:
            current = self._sessions[session_id]
            current["pending_intent"] = None
            current["signal_pending"] = None
            current.pop("cancel_requested", None)
            current["status"] = ("cancelled" if cancel_mode == "cancel" else
                                  "paused" if cancel_mode == "pause" else
                                  "expired" if cancel_mode == "expire" else "armed")
            if current["status"] in {"cancelled", "expired"}:
                self._reservations.discard(self._reservation_key(current))
            current.setdefault("events", []).append({"kind": current["status"], "ts": int(time.time() * 1000),
                                                       "message": "OKX已确认条件委托撤销且无触发成交"})
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(self._sessions[session_id]))

    def _consume_signal(self, session_id: str, message: str) -> None:
        with self._lock:
            session = self._sessions[session_id]
            session["status"] = "armed"
            session["pending_intent"] = None
            session.setdefault("events", []).append({"kind": "signal_skipped", "ts": int(time.time() * 1000), "message": message})
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(session))
        self._maybe_rearm_resting_entry(session_id)

    def _maybe_rearm_resting_entry(self, session_id: str) -> None:
        session = self._sessions[session_id]
        plan = AreaShapePlan.from_dict(session["plan_snapshot"])
        if (not (plan.config.resting_entry_enabled or plan.config.entry_trigger_mode == "direct_touch")
                or session.get("status") != "armed"
                or session.get("paused") or session.get("position") or session.get("resting_entry_intent")):
            return
        if int(time.time() * 1000) >= int(session.get("expires_at_candle_ts", 0)) + 3_600_000:
            self._set_status(session_id, "expired", "GTC未成交且计划已到期")
            return
        runtime, client = self._runtime_client(session_id)
        candles = [item for item in client.get_candles_history(session["symbol"], "1H", limit=240) if item.confirmed]
        intent, reason = self._prepare_resting_entry(session, plan, candles)
        with self._lock:
            current = self._sessions[session_id]
            if current.get("status") != "armed" or current.get("resting_entry_intent"):
                return
            current["resting_entry_intent"] = intent
            if reason:
                current.setdefault("events", []).append({"kind": "resting_entry_skipped", "ts": int(time.time() * 1000),
                                                          "message": reason})
            self._persist_locked()
        if intent:
            self._submit_resting_entry(session_id)

    def _set_status(self, session_id: str, status: str, message: str) -> None:
        with self._lock:
            session = self._sessions[session_id]
            session["status"] = status
            session.setdefault("events", []).append({"kind": status, "ts": int(time.time() * 1000), "message": message})
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(session))
        self.status_changed.emit(message)

    def _persist_session(self, session_id: str) -> None:
        with self._lock:
            self._persist_locked()
        self.session_updated.emit(copy.deepcopy(self._sessions[session_id]))

    def _persist_locked(self) -> None:
        self._require_writable_store()
        try:
            self._store_save([dict(item) for item in self._sessions.values()])
        except Exception as exc:
            # A durable intent is a prerequisite for every later order write.
            # In-memory mutations after a failed save cannot authorize trading.
            self._store_write_blocked = True
            self._load_errors.append(f"执行状态保存失败：{exc}")
            self.status_changed.emit(f"执行状态保存失败，已禁止交易写入并保留交易所现有保护：{exc}")
            raise

    def _require_writable_store(self) -> None:
        if self._store_write_blocked:
            raise RuntimeError("demo执行存储损坏或schema未知；为保护原文件禁止写入")

    def _runtime_client(self, session_id: str, *, protective: bool = False) -> tuple[Any, Any]:
        self._require_writable_store()
        runtime = self._runtimes.get(session_id)
        client = self._clients.get(session_id)
        if runtime is None or client is None:
            raise RuntimeError("本进程没有显式启用的demo runtime；只读恢复状态禁止写入")
        session = self._sessions[session_id]
        self._verify_runtime_identity(session, runtime, True)
        if int(session.get("rule_version") or 0) != AREA_SHAPE_EXECUTION_RULE_VERSION:
            raise PermissionError("执行规则版本已变化；旧会话保持只读，禁止套用新保护规则")
        if str(getattr(runtime, "environment", "")).lower() != _session_environment(session):
            raise PermissionError("运行时环境已变化；拒绝跨环境写入")
        blocked = {"closed", "cancelled", "expired"}
        if not protective:
            blocked.update({"paused", "needs_manual_rearm", "manual_review"})
        if session.get("write_disabled") and not protective or session.get("status") in blocked:
            raise PermissionError("会话只读/终止，禁止交易写入")
        return runtime, client

    def _assert_write_enabled(self, session_id: str) -> None:
        self._require_writable_store()
        if self._stop.is_set():
            raise PermissionError("应用关闭中；禁止新的demo交易写入")
        session = self._sessions[session_id]
        if session.get("write_disabled") or session.get("status") in {"needs_manual_rearm", "manual_review", "closed", "cancelled", "expired", "paused"}:
            raise PermissionError("会话只读/终止，禁止交易写入")


def _place_area_shape_order(client: Any, kind: str, credentials: Any, config: StrategyConfig, **kwargs: Any) -> Any:
    method_name = {
        "limit": "place_area_shape_limit_entry",
        "fok": "place_area_shape_fok_entry",
        "trigger": "place_area_shape_trigger_entry",
        "protection": "place_area_shape_protection_algo",
    }.get(kind)
    if method_name is None:
        raise ValueError("区域执行订单类型无效")
    method = getattr(client, method_name, None)
    if not callable(method) and config.environment == "demo":
        # Older demo-only test doubles and external adapters remain compatible.
        method = getattr(client, f"place_area_shape_demo_{kind}_entry" if kind != "protection"
                         else "place_area_shape_demo_protection_algo", None)
    if not callable(method):
        raise RuntimeError(f"client缺少{config.environment}区域{kind}执行适配器")
    if str(config.environment).lower() not in {"demo", "live"}:
        raise PermissionError("区域执行配置环境无效")
    return method(credentials, config, **kwargs)


def _take_profit_r(plan: AreaShapePlan) -> Decimal:
    return plan.config.fixed_tp_r if plan.config.exit_template == "fixed_1r" else AREA_DYNAMIC_TAKE_PROFIT_R


def _strategy_config(session: dict[str, Any], runtime: Any) -> StrategyConfig:
    return StrategyConfig(
        inst_id=str(session["symbol"]), bar="1H", ema_period=21, atr_period=14,
        atr_stop_multiplier=Decimal("1"), atr_take_multiplier=Decimal("1"), order_size=Decimal("1"),
        trade_mode=str(session.get("trade_mode") or runtime.trade_mode), signal_mode="both",
        position_mode=_session_position_mode(session, runtime),
        environment=_session_environment(session), tp_sl_trigger_type="last", strategy_id="area_shape_execution",
    )


def _session_environment(session: dict[str, Any]) -> str:
    environment = str(session.get("environment") or "demo").lower()
    if environment not in {"demo", "live"}:
        raise PermissionError("session环境无效；拒绝REST请求")
    if str(session.get("mode") or f"{environment}_exchange") != f"{environment}_exchange":
        raise PermissionError("session mode与环境不一致；拒绝REST请求")
    return environment


def _session_position_mode(session: dict[str, Any], runtime: Any) -> str:
    frozen = str(session.get("position_mode") or getattr(runtime, "position_mode", ""))
    if frozen not in {"net", "long_short"}:
        raise PermissionError("冻结持仓模式无效；拒绝REST请求")
    return frozen


def _position_side(direction: str, position_mode: str) -> str | None:
    return direction if position_mode == "long_short" else None


def _position_matches(position: Any, direction: str, size: Decimal, position_mode: str) -> bool:
    raw = getattr(position, "position", Decimal("0"))
    if position_mode == "long_short":
        return str(getattr(position, "pos_side", "")).lower() == direction and abs(raw) == size
    actual_direction = "long" if raw > 0 else "short" if raw < 0 else ""
    return actual_direction == direction and abs(raw) == size


def _verified_trigger_entry(raw: Any, session: dict[str, Any], intent: dict[str, Any]) -> bool:
    if not isinstance(raw, dict):
        return False
    size, cap, trigger = (_decimal_or_none(intent.get(field)) for field in ("size", "price", "trigger_px"))
    if any(value is None or value <= 0 for value in (size, cap, trigger)):
        return False
    direction = str(session.get("direction") or session["plan_snapshot"]["config"]["direction"])
    if intent.get("side") != ("buy" if direction == "long" else "sell"):
        return False
    return (
        str(raw.get("instId") or "").upper() == str(session["symbol"]).upper()
        and str(raw.get("algoClOrdId") or "") == str(intent.get("cl_ord_id") or "")
        and str(raw.get("side") or "").lower() == str(intent.get("side") or "").lower()
        and str(raw.get("ordType") or "").lower() == "trigger"
        and str(raw.get("triggerPxType") or "").lower() == "last"
        and _decimal_or_none(raw.get("sz")) == _decimal_or_none(intent.get("size"))
        and _decimal_or_none(raw.get("triggerPx")) == _decimal_or_none(intent.get("trigger_px"))
        and _decimal_or_none(raw.get("ordPx") or raw.get("orderPx")) == _decimal_or_none(intent.get("price"))
        and str(raw.get("advanceOrdType") or "fok").lower() == "fok"
    )


def _verified_entry_fill_order(status: Any, session: dict[str, Any], intent: dict[str, Any], *, order_id: str | None = None, order_type: str = "fok") -> bool:
    raw = status.raw if isinstance(status.raw, dict) else {}
    expected_size = _decimal_or_none(intent.get("size"))
    cap = _decimal_or_none(intent.get("price"))
    direction = str(session.get("direction") or session["plan_snapshot"]["config"]["direction"])
    if cap is None or cap <= 0 or intent.get("side") != ("buy" if direction == "long" else "sell"):
        return False
    filled = _decimal_or_none(status.filled_size)
    return (
        str(raw.get("instId") or "").upper() == str(session["symbol"]).upper()
        and (str(raw.get("ordId") or "") == order_id and str(status.ord_id or "") == order_id if order_id
             else str(raw.get("clOrdId") or "") == str(intent.get("cl_ord_id") or ""))
        and str(raw.get("side") or "").lower() == str(intent.get("side") or "").lower()
        and str(raw.get("ordType") or "").lower() == order_type
        and expected_size is not None and expected_size > 0
        and _decimal_or_none(raw.get("sz")) == expected_size
        and _decimal_or_none(raw.get("px")) == _decimal_or_none(intent.get("price"))
        and filled is not None and 0 <= filled <= expected_size
    )


def _verified_protection(
    raw: dict[str, Any] | None, *, inst_id: str, side: str, size: Decimal,
    trigger: Decimal, kind: str, pos_side: str | None, position_mode: str = "net",
) -> bool:
    if not isinstance(raw, dict) or str(raw.get("state", "")).lower() != "live":
        return False
    if str(raw.get("instId", "")).upper() != inst_id.upper() or str(raw.get("side", "")).lower() != side:
        return False
    if position_mode == "net" and str(raw.get("reduceOnly", "")).lower() not in {"true", "1"}:
        return False
    if position_mode == "net" and str(raw.get("posSide", "")).lower() not in {"", "net"}:
        return False
    if Decimal(str(raw.get("sz") or "0")) != size:
        return False
    if pos_side and str(raw.get("posSide", "")).lower() != pos_side:
        return False
    key = "slTriggerPx" if kind == "sl" else "tpTriggerPx"
    type_key = "slTriggerPxType" if kind == "sl" else "tpTriggerPxType"
    order_price_key = "slOrdPx" if kind == "sl" else "tpOrdPx"
    if str(raw.get(type_key, "")).lower() != "last":
        return False
    if str(raw.get(order_price_key, "")) != "-1":
        return False
    actual = _decimal_or_none(raw.get(key))
    return actual == trigger


def _decimal_or_none(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def credential_hash(api_key: str) -> str:
    from okx_quant.area_shape_execution import credential_fingerprint
    return credential_fingerprint(api_key)
