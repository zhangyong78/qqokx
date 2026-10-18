"""Explicitly authorized, minimum-size OKX demo protection integration matrix.

Signals are synthetic: no strategy waits for a market pattern. Exchange order
responses, fills, protection reads and cancellation results are real demo data.
Only the test's own orders and newly confirmed fills may be cleaned up.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from okx_quant.area_shape_execution import build_demo_execution_session, new_execution_client_order_id, price_for_net_r
from okx_quant.area_shape_execution_monitor import AreaShapeDemoExecutionMonitor, _strategy_config, _verified_protection
from okx_quant.area_shape_trade import AreaShapeConfig, AreaShapePlan, area_shape_net_progress_r
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import credential_profile_has_switch_password, load_credentials_profiles_snapshot
from okx_quant.pricing import format_decimal, snap_to_increment
from roll_terminal_qt.runtime import load_runtime


class Journal:
    def __init__(self, folder: Path):
        folder.mkdir(parents=True, exist_ok=True)
        self.path = folder / "exchange.jsonl"
        self.rows = []

    def emit(self, kind, **fields):
        row = {"time_utc": datetime.now(timezone.utc).isoformat(), "kind": kind, **fields}
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        self.rows.append(row)
        if kind not in {"request", "response"}:
            print(json.dumps(row, ensure_ascii=False, default=str), flush=True)


class DemoClient(OkxRestClient):
    def __init__(self, journal):
        self.journal = journal
        super().__init__()

    def _request(self, method, path, **kwargs):
        if kwargs.get("auth") and kwargs.get("simulated") is not True:
            raise PermissionError("Test runner rejects all authenticated non-demo requests")
        # Never record credentials, request headers or authentication signatures.
        body = kwargs.get("body")
        record = path.startswith("/api/v5/trade/")
        if record:
            self.journal.emit("request", method=method, endpoint=path, body=body, params=kwargs.get("params"), simulated=True)
        try:
            result = super()._request(method, path, **kwargs)
        except Exception as exc:
            if record:
                expected_missing = method == "GET" and path == "/api/v5/trade/order-algo" and str(getattr(exc, "code", "")) == "51603"
                self.journal.emit("expected_missing" if expected_missing else "api_error", endpoint=path,
                                  error=str(exc), code=getattr(exc, "code", None))
            raise
        if record:
            self.journal.emit("response", endpoint=path, payload=result)
        return result


def wait_read(read, done, attempts=6):
    last = None
    for _ in range(attempts):
        last = read()
        if done(last):
            return last
        time.sleep(0.5)
    raise RuntimeError(f"Exchange state did not settle: {last}")


def positions(client, runtime, symbol):
    return [p for p in client.get_positions(runtime.credentials, environment="demo", inst_type="SWAP", prefer_cache=False)
            if p.inst_id == symbol and p.position != 0]


def run_case(client, runtime, symbol, instrument, fee, baseline, route, direction, dynamic, be, journal):
    name = f"{route}_{direction}_{'dynamic' if dynamic else 'fixed'}_be{int(be)}"
    journal.emit("case_start", case=name, environment="demo", signal_source="synthetic", size=instrument.min_size)
    assert not positions(client, runtime, symbol), "Pre-existing position; test refused"
    assert not client.get_area_shape_pending_orders(runtime.credentials, environment="demo", inst_id=symbol), "Pre-existing order; test refused"
    tick = instrument.tick_size
    sign = Decimal(1) if direction == "long" else Decimal(-1)
    side = "buy" if direction == "long" else "sell"
    book = client.get_order_book(symbol, depth=1)
    quote = book.asks[0][0] if side == "buy" else book.bids[0][0]
    trigger = snap_to_increment(quote * (1 + sign * Decimal("0.002")), tick, "up" if side == "buy" else "down")
    reference = trigger if route == "trigger" else quote
    entry = snap_to_increment(reference * (1 + sign * Decimal("0.0008")), tick, "down" if side == "buy" else "up")
    stop = snap_to_increment(entry * (1 - sign * Decimal("0.005")), tick, "nearest")
    target_r = Decimal(15) if dynamic else Decimal(2)
    target = price_for_net_r(entry=entry, stop=stop, direction=direction, target_r=target_r,
                             fee_rate=fee, slippage_rate=Decimal(0), contract_multiplier=instrument.ct_val * instrument.ct_mult,
                             tick_size=tick)
    support = entry if direction == "long" else entry - abs(entry - stop) * 20
    pressure = entry + abs(entry - stop) * 20 if direction == "long" else entry
    plan = AreaShapePlan.create(profile_name=runtime.credential_profile_name, environment="demo", symbol=symbol,
        support_price=support, pressure_price=pressure,
        config=AreaShapeConfig(direction=direction, risk_amount=Decimal("1000"), stop_basis="fixed_distance",
            fixed_stop_distance=abs(entry-stop), exit_template="lock_2r_at_3r" if dynamic else "fixed_1r",
            fixed_tp_r=target_r if not dynamic else Decimal(2), break_even_enabled=be,
            resting_entry_enabled=False, entry_trigger_mode="direct_touch" if route == "gtc" else "pattern",
            entry_mode="breakout" if route == "trigger" else "next_open"))
    plan.activate(last_closed_candle_ts=baseline, profile_name=plan.profile_name, environment="demo")
    session = build_demo_execution_session(plan=plan, runtime=runtime, account_uid="isolated-demo-test",
        instrument=instrument, fee_rate=fee, slippage_rate=Decimal(0), baseline_candle_ts=baseline,
        confirmed_at_ms=int(time.time()*1000), unlocked=True)
    client_id = new_execution_client_order_id(plan.plan_id, name, "testentry")
    attachment_id = new_execution_client_order_id(plan.plan_id, name, "testprotect")
    intent = {"entry_kind": route, "signal_key": name, "cl_ord_id": client_id,
        "stop_algo_cl_ord_id": attachment_id, "take_profit_algo_cl_ord_id": attachment_id,
        "take_profit_price": str(target), "take_profit_target_r": str(target_r), "entry_reference": str(entry),
        "side": side, "size": str(instrument.min_size), "price": str(entry), "stop": str(stop)}
    session.update(status="entry_intent", pending_intent=intent, last_signal_key=name)
    monitor = AreaShapeDemoExecutionMonitor(store_load=lambda: [], store_save=lambda _: None)
    monitor._sessions[session["session_id"]] = session
    monitor._runtimes[session["session_id"]] = runtime
    monitor._clients[session["session_id"]] = client
    config = _strategy_config(session, runtime)
    submitted = False
    confirmed_size = Decimal(0)
    succeeded = False
    own_algos = {attachment_id}
    try:
        common = dict(inst_id=symbol, side=side, size=instrument.min_size, limit_price=entry,
                      stop_price=stop, client_order_id=client_id, stop_algo_client_order_id=attachment_id,
                      take_profit_price=target)
        submitted = True  # A timeout may still have accepted the request.
        if route == "trigger":
            client.place_area_shape_demo_trigger_entry(runtime.credentials, config, trigger_price=trigger, **common)
            raw = client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=client_id)
            assert raw and raw.get("state") == "live", f"Trigger entry is not live: {raw}"
            attached = raw.get("attachAlgoOrds") or []
            assert any(Decimal(x.get("tpTriggerPx") or 0) == target and Decimal(x.get("slTriggerPx") or 0) == stop for x in attached), "Trigger parent lost TP/SL attachment"
            journal.emit("trigger_parent_verified", case=name, entry_id=client_id, tp=target, sl=stop, fills_tested=False)
        else:
            method = client.place_area_shape_demo_limit_entry if route == "gtc" else client.place_area_shape_demo_fok_entry
            method(runtime.credentials, config, **common)
            order = wait_read(lambda: client.get_order(runtime.credentials, config, inst_id=symbol, cl_ord_id=client_id),
                              lambda row: row.state in {"filled", "canceled"})
            assert order.state == "filled", f"Immediate-fill test did not fill: {order.state}"
            confirmed_size = Decimal(str(order.filled_size))
            assert confirmed_size == instrument.min_size, "Unexpected partial fill"
            wait_read(lambda: client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=attachment_id),
                      lambda raw: bool(raw and raw.get("state") == "live"))
            fill_time = int(order.raw.get("fillTime") or 0)
            if not fill_time:
                fills = client.get_fills_history(runtime.credentials, environment="demo", inst_types=("SWAP",), limit=100)
                fill_time = max((x.fill_time for x in fills if x.order_id == order.ord_id and x.inst_id == symbol), default=0)
            assert fill_time, "Real exchange fill timestamp unavailable"
            monitor._protect_fill(session["session_id"], order, fill_time)
            position = session["position"]
            own_algos.update(str(position[k]) for k in ("stop_algo_cl_ord_id", "take_profit_algo_cl_ord_id") if position.get(k))
            assert session["status"] == "protected", f"Application did not verify protection: {session['status']}"
            assert position.get("stop_verified_at_ms") and position.get("take_profit_verified_at_ms")
            assert position["stop_algo_cl_ord_id"] == attachment_id and position["take_profit_algo_cl_ord_id"] == attachment_id, "Unexpected fallback/duplicate protection"
            actual_tp_r = area_shape_net_progress_r(plan.config, direction=direction, entry=Decimal(position["entry"]),
                initial_stop=stop, price=target, fee_rate=fee)
            journal.emit("filled_protection_verified", case=name, entry_id=client_id, protection_id=attachment_id,
                         actual_entry=position["entry"], frozen_entry=entry, sl=stop, tp=target, actual_tp_net_r=actual_tp_r)
            if dynamic or be:
                latest = client.get_trigger_price(symbol, "last", environment="demo", max_cached_age_seconds=0)
                candidate = snap_to_increment(stop + sign * abs(Decimal(position["entry"])-stop) / 4, tick, "nearest")
                assert candidate < latest if direction == "long" else candidate > latest
                client.amend_algo_order(runtime.credentials, environment="demo", inst_id=symbol,
                    algo_cl_ord_id=attachment_id, new_stop_loss_trigger_price=candidate,
                    new_stop_loss_order_price=Decimal(-1), new_stop_loss_trigger_price_type="last")
                updated = client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=attachment_id)
                pos_side = direction if runtime.position_mode == "long_short" else None
                for kind, expected in (("sl", candidate), ("tp", target)):
                    assert _verified_protection(updated, inst_id=symbol, side="sell" if direction == "long" else "buy",
                        size=confirmed_size, trigger=expected, kind=kind, pos_side=pos_side, position_mode=runtime.position_mode), f"{kind} protection failed after SL-only amend"
                journal.emit("sl_only_amend_verified", case=name, old_sl=stop, new_sl=candidate, unchanged_tp=target,
                             market_r_trigger_tested=False)
        succeeded = True
    finally:
        if submitted:
            # Cancel the parent first and reconcile its fill; never cancel an
            # unknown order or close a position whose size/side cannot be owned.
            if route == "trigger":
                raw = client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=client_id)
                if raw and raw.get("state") == "live":
                    client.cancel_algo_order(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=client_id)
                    wait_read(lambda: client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=client_id),
                              lambda row: bool(row and row.get("state") == "canceled"))
                elif raw and raw.get("state") == "effective":
                    child_id = raw.get("ordId") or raw.get("actualOrdId")
                    if child_id:
                        child = client.get_order(runtime.credentials, config, inst_id=symbol, ord_id=child_id)
                        confirmed_size = Decimal(str(child.filled_size))
            else:
                order = client.get_order(runtime.credentials, config, inst_id=symbol, cl_ord_id=client_id)
                if order.state in {"live", "partially_filled"}:
                    client.cancel_order(runtime.credentials, config, inst_id=symbol, cl_ord_id=client_id)
                    order = wait_read(lambda: client.get_order(runtime.credentials, config, inst_id=symbol, cl_ord_id=client_id),
                                      lambda row: row.state in {"filled", "canceled"})
                confirmed_size = Decimal(str(order.filled_size))
            actual_positions = positions(client, runtime, symbol)
            if actual_positions:
                assert len(actual_positions) == 1 and abs(actual_positions[0].position) == confirmed_size > 0, "Cleanup cannot attribute position; leave its protection and stop"
                assert actual_positions[0].pos_side == direction if runtime.position_mode == "long_short" else (actual_positions[0].position > 0) == (direction == "long")
                close_id = new_execution_client_order_id(plan.plan_id, name, "testclose")
                body = {"instId": symbol, "tdMode": runtime.trade_mode, "side": "sell" if direction == "long" else "buy",
                        "ordType": "market", "sz": format_decimal(confirmed_size), "clOrdId": close_id}
                if runtime.position_mode == "long_short":
                    body["posSide"] = direction
                else:
                    body["reduceOnly"] = True
                client._request("POST", "/api/v5/trade/order", body=body, auth=True,
                                credentials=runtime.credentials, simulated=True)
                wait_read(lambda: positions(client, runtime, symbol), lambda rows: not rows)
            for algo_id in own_algos:
                raw = client.get_order_algo(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=algo_id)
                if raw and raw.get("state") == "live":
                    client.cancel_algo_order(runtime.credentials, environment="demo", inst_id=symbol, algo_cl_ord_id=algo_id)
            assert not positions(client, runtime, symbol), "Residual test position"
            assert not client.get_area_shape_pending_orders(runtime.credentials, environment="demo", inst_id=symbol), "Residual/foreign order; stop testing"
            journal.emit("cleanup_verified", case=name, remaining_positions=0, remaining_orders=0)
    journal.emit("case_pass" if succeeded else "case_failed", case=name)
    return {"case": name, "passed": succeeded, "trigger_fill_tested": route != "trigger"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="moni")
    parser.add_argument("--symbol", default="BTC-USDT-SWAP")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--limit", type=int, default=24)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not args.execute:
        raise SystemExit("No orders submitted: supply --execute only with explicit human authorization")
    profile = load_credentials_profiles_snapshot().get("profiles", {}).get(args.profile, {})
    assert not credential_profile_has_switch_password(profile), "Unlock the protected demo profile through the application first"
    runtime = load_runtime(args.profile)
    assert runtime and runtime.environment == "demo", "Only an explicit existing demo profile is permitted"
    journal = Journal(Path(args.output))
    results = []
    with_client = DemoClient(journal)
    try:
        account = with_client.get_account_config(runtime.credentials, environment="demo")
        runtime = replace(runtime, position_mode="long_short" if account.raw.get("posMode") == "long_short_mode" else "net")
        instrument = with_client.get_area_shape_instrument(args.symbol)
        fee = with_client.get_area_shape_fee_rate(runtime.credentials, environment="demo", inst_family=instrument.inst_family)
        candles = with_client.get_candles_history(args.symbol, "1H", limit=240)
        baseline = max(c.ts for c in candles if c.confirmed)
        journal.emit("preflight", profile=args.profile, environment="demo", symbol=args.symbol,
                     position_mode=runtime.position_mode, min_size=instrument.min_size, fee_rate=fee)
        cases = [(r,d,dy,be) for r in ("gtc","fok","trigger") for d in ("long","short") for dy in (False,True) for be in (False,True)]
        for case in cases[args.start:args.start + args.limit]:
            results.append(run_case(with_client, runtime, args.symbol, instrument, fee, baseline, *case, journal))
    except Exception as exc:
        journal.emit("run_failed", error=str(exc))
        raise
    finally:
        (Path(args.output)/"summary.json").write_text(json.dumps({"results": results,
            "passed": sum(x["passed"] for x in results), "scope": "real OKX demo responses; synthetic signals; no natural market R trigger claim"},
            ensure_ascii=False, indent=2), encoding="utf-8")
        with_client.close()


if __name__ == "__main__":
    main()
