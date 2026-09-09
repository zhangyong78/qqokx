"""Isolated August replay of the saved best-parameter bundle.

Uses the existing engine for strategy signals, sizing and dynamic protection.
Exports both the original execution and the project's next-open standard.
The production engine and live parameters are never modified.
"""
from __future__ import annotations

import ast
import difflib
import inspect
import json
import sys
from dataclasses import asdict, replace
from decimal import Decimal

from review_reapai_august_2026 import ROOT, OUT, START, END, HOUR, SYMBOLS, read, save, date, metrics
import pandas as pd
import okx_quant.backtest as bt
from okx_quant.models import Candle, Instrument, StrategyConfig, normalize_dynamic_protection_rules

D = Decimal
MAKER, TAKER, SLIP = D("0.00015"), D("0.00036"), D("0.0003")


def config_loader():
    # Reuse the existing deserializer without importing its plotting module or
    # triggering its module-level report-directory creation.
    path = ROOT / "scripts/run_best_parameter_bundle_1h_standard_portfolio.py"
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "deserialize_strategy_config")
    env = dict(Decimal=D, StrategyConfig=StrategyConfig, normalize_dynamic_protection_rules=normalize_dynamic_protection_rules)
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), env)
    return env[node.name]


def market_data(symbol):
    raw = read(OUT / f"candles_{symbol}.json")
    candles = [Candle(int(r[0]), *(D(str(v)) for v in r[1:6]), bool(r[6])) for r in raw]
    assert len({c.ts for c in candles}) == len(candles)
    assert all(b.ts-a.ts == HOUR for a,b in zip(candles,candles[1:]))
    assert len([c for c in candles if START <= c.ts < END]) == 744
    r = read(OUT / f"instrument_{symbol}.json")[0]
    instrument = Instrument(inst_id=symbol, inst_type="SWAP", tick_size=D(r["tickSz"]), lot_size=D(r["lotSz"]), min_size=D(r["minSz"]), state=r["state"], settle_ccy=r["settleCcy"], ct_val=D(r["ctVal"]), ct_mult=D(r["ctMult"] or "1"), ct_val_ccy=r["ctValCcy"], uly=r["uly"], inst_family=r["instFamily"])
    return candles, instrument


def replace_once(source, old, new):
    assert source.count(old) == 1, f"Engine source changed: expected one occurrence of {old!r}"
    return source.replace(old, new, 1)


def next_open_functions(config):
    """Clone only two engine functions; all changes are exported for audit."""
    env = dict(bt.__dict__)
    originals = {}
    modified = {}
    name = "_run_ema55_slope_short_backtest"
    original = inspect.getsource(getattr(bt, name))
    source = replace_once(original,
        '        try:\n            protection = build_protection_plan(',
        '        if index + 1 >= len(candles):\n            continue\n        try:\n            protection = build_protection_plan(')
    source = replace_once(source,
        '        entry_price_raw = protection.entry_reference',
        '        entry_price_raw = candles[index + 1].open\n        if entry_price_raw >= protection.stop_loss or (take_profit_enabled and entry_price_raw <= protection.take_profit):\n            continue')
    source = replace_once(source, '            entry_price=protection.entry_reference,', '            entry_price=entry_price_raw,')
    source = replace_once(source, '            entry_index=index,\n            entry_ts=candle.ts,', '            entry_index=index + 1,\n            entry_ts=candles[index + 1].ts,')
    source = replace_once(source, '            apply_entry_slippage=True,', '            apply_entry_slippage=True,\n            metadata={"signal_ts": candle.ts, "execution": "next_open"},')
    source = replace_once(source, '        if open_position is not None and slope_exit_enabled', '        if open_position is not None and index + 1 < len(candles) and slope_exit_enabled')
    source = replace_once(source, '                exit_fee_rate=taker_fee_rate,\n                exit_fee_type="taker",\n            )',
                          '                exit_fee_rate=taker_fee_rate,\n                exit_fee_type="taker",\n                allow_same_candle=True,\n            )')
    originals[name], modified[name] = original, source

    name = "_run_dynamic_backtest"
    original = inspect.getsource(getattr(bt, name))
    source = replace_once(original, '            elif (\n                bool(config.trend_ema_close_exit_after_trigger_r_enabled)',
                          '            elif (\n                index + 1 < len(candles)\n                and bool(config.trend_ema_close_exit_after_trigger_r_enabled)')
    originals[name], modified[name] = original, source

    def fill_at_next_open(instrument, plan, candle, candle_index, **kwargs):
        assert candle.ts > plan.candle_ts
        price = candle.open
        if (plan.signal == "long" and price <= plan.stop_loss) or (plan.signal == "short" and price >= plan.stop_loss):
            return None
        kwargs.pop("immediate_entry_fee_rate", None)
        kwargs.pop("immediate_entry_fee_type", None)
        size = bt._determine_backtest_order_size(instrument=instrument, config=config, entry_price=price, stop_loss=plan.stop_loss, risk_price_compatible=True)
        return bt._create_open_position(instrument=instrument, signal=plan.signal, entry_index=candle_index,
            entry_ts=candle.ts, entry_price_raw=price, entry_path_price=price, stop_loss=plan.stop_loss,
            take_profit=plan.take_profit, atr_value=plan.atr_value, size=size,
            exit_fee_rate=kwargs["dynamic_exit_fee_rate"], apply_entry_slippage=True,
            metadata={"signal_ts": plan.candle_ts, "execution": "next_open"}, **kwargs)

    def run(config_candles, instrument, cfg, side):
        def close_on_next_open(position, candle, index, **kwargs):
            # Only direct close-confirmed signal exits call this clone binding;
            # intrabar stop/target helpers retain their original implementation.
            assert kwargs["exit_reason"] in {"slope_turn_positive", "trend_ema_close_exit"}
            next_candle = config_candles[index + 1]
            kwargs["exit_price_raw"] = next_candle.open
            kwargs["exit_price"] = bt._apply_slippage_price(next_candle.open, signal=position.signal,
                tick_size=position.tick_size, slippage_rate=position.exit_slippage_rate, is_entry=False)
            return bt._build_closed_trade(position, next_candle, index + 1, **kwargs)
        env["_build_closed_trade"] = close_on_next_open
        env["_try_fill_dynamic_order"] = fill_at_next_open
        if side == "long":
            return env["_run_dynamic_backtest"](config_candles, instrument, cfg, maker_fee_rate=MAKER, taker_fee_rate=TAKER)
        return env["_run_ema55_slope_short_backtest"](config_candles, instrument, cfg, taker_fee_rate=TAKER)

    for name,source in modified.items():
        exec(compile(source, f"<isolated_august_{name}>", "exec"), env)
    audit_diff = "\n".join("".join(difflib.unified_diff(originals[n].splitlines(True), modified[n].splitlines(True), fromfile=n+":original", tofile=n+":next_open")) for n in originals)
    return run, audit_diff


def funding_for(symbol, side, entry_ts, exit_ts, size, candles):
    # Settlement marks are approximated by the last completed 1H trade close.
    # Actual live funding is reconciled separately from account bills.
    closes = {c.ts + HOUR: c.close for c in candles}
    total = D("0")
    for r in read(OUT / f"funding_{symbol}.json"):
        t = int(r["fundingTime"])
        if entry_ts < t <= exit_ts and t in closes:
            rate = D(r.get("realizedRate") or r["fundingRate"])
            total += D("-1" if side == "long" else "1") * abs(size) * closes[t] * rate
    return total


def trade_row(t, profile, risk, candles):
    fund = funding_for(profile["symbol"], t.signal, t.entry_ts, t.exit_ts, t.size, candles)
    metadata = dict(t.metadata or {})
    return {"symbol": profile["symbol"], "strategy": profile["profile_name"], "profile_id": profile["profile_id"], "side": t.signal,
            "entry_ts": t.entry_ts, "exit_ts": t.exit_ts, "entry_time": date(t.entry_ts), "exit_time": date(t.exit_ts),
            "entry_price": float(t.entry_price), "exit_price": float(t.exit_price), "base_size": float(t.size), "risk_target": float(risk), "risk_value": float(t.risk_value),
            "gross_pnl": float(t.gross_pnl), "fee": -float(t.total_fee), "funding": float(fund), "net_pnl": float(t.pnl + fund),
            "net_before_funding": float(t.pnl), "slippage_cost": float(t.slippage_cost), "entry_fee": float(t.entry_fee), "exit_fee": float(t.exit_fee),
            "entry_fee_type": t.entry_fee_type, "exit_reason": t.exit_reason, "exit_reason_cn": bt.format_trade_exit_reason(t.exit_reason),
            "holding_hours": (t.exit_ts-t.entry_ts)/HOUR, "stop_loss": float(t.stop_loss), "take_profit": float(t.take_profit),
            "signal_ts": metadata.get("signal_ts"), "wave_entry_sequence": t.wave_entry_sequence}


def run_all():
    loader = config_loader()
    bundle = read(OUT / "parameter_bundle.json")
    markets = {s: market_data(s) for s in SYMBOLS}
    all_summaries = {}
    for mode in ("original_100u", "original_profile_risk", "calibrated_original_100u", "calibrated_original_profile_risk", "next_open_100u", "next_open_profile_risk"):
        rows, open_rows, checks = [], [], []
        for p in bundle["profiles"]:
            candles, instrument = markets[p["symbol"]]
            if mode.startswith(("calibrated_", "next_open")):
                # Engine PnL and fixed-risk sizing use base-coin quantities,
                # whereas OKX instrument lot/min fields are contract counts.
                multiplier = instrument.ct_val * instrument.ct_mult
                instrument = replace(instrument, lot_size=instrument.lot_size*multiplier,
                    min_size=instrument.min_size*multiplier, ct_val=D("1"), ct_mult=D("1"))
            cfg = loader(p["config_snapshot"])
            risk = D("100") if mode.endswith("100u") else cfg.risk_amount
            cfg = replace(cfg, bar="1H", risk_amount=risk, order_size=D("0"), backtest_sizing_mode="fixed_risk",
                backtest_compounding=False, backtest_risk_percent=None, backtest_initial_capital=D("10000"),
                backtest_entry_slippage_rate=SLIP, backtest_exit_slippage_rate=SLIP, backtest_slippage_rate=SLIP, backtest_funding_rate=D("0"))
            side = "long" if cfg.signal_mode == "long_only" else "short"
            if mode.startswith("next_open"):
                run, audit_diff = next_open_functions(cfg)
                trades, terminal = run(candles, instrument, cfg, side)
                save("execution_adapter_audit.json", {"diff": audit_diff, "long_fill": "next candle open, risk resized to retained signal stop, 3bps entry slippage, maker assumed", "signal_exits": "next candle open", "intrabar_exits": "original engine OHLC path"})
            elif side == "long":
                trades, terminal = bt._run_dynamic_backtest(candles, instrument, cfg, maker_fee_rate=MAKER, taker_fee_rate=TAKER)
            else:
                trades, terminal = bt._run_ema55_slope_short_backtest(candles, instrument, cfg, taker_fee_rate=TAKER)
            aug = [t for t in trades if START <= t.exit_ts < END]
            for t in aug:
                assert t.entry_ts <= t.exit_ts
                assert abs(t.pnl - (t.gross_pnl-t.total_fee-t.funding_cost)) < D("0.00000001")
                if mode.startswith("next_open"):
                    assert t.entry_ts == t.metadata["signal_ts"] + HOUR
                    expected = bt._apply_slippage_price(candles[t.entry_index].open, signal=side, tick_size=instrument.tick_size, slippage_rate=SLIP, is_entry=True)
                    assert t.entry_price == expected
                if mode.startswith(("calibrated_", "next_open")):
                    assert t.size % instrument.lot_size == 0
                    assert t.risk_value <= risk
                rows.append(trade_row(t,p,risk,candles))
            if terminal:
                op = asdict(terminal)
                op.update(symbol=p["symbol"], strategy=p["profile_name"])
                op["funding_estimate"] = funding_for(p["symbol"],side,terminal.entry_ts,END-1,terminal.size,candles)
                open_rows.append(op)
            # June-vs-May prehistory stability check is directly relevant to a
            # single-month replay and detects warm-up-induced signal changes.
            if mode in ("original_100u", "calibrated_original_100u"):
                shorter = [c for c in candles if c.ts >= START-61*24*HOUR]
                if side == "long":
                    check_trades,_ = bt._run_dynamic_backtest(shorter,instrument,cfg,maker_fee_rate=MAKER,taker_fee_rate=TAKER)
                else:
                    check_trades,_ = bt._run_ema55_slope_short_backtest(shorter,instrument,cfg,taker_fee_rate=TAKER)
                signature = lambda ts: [(t.entry_ts,t.exit_ts,str(t.pnl)) for t in ts if START <= t.exit_ts < END]
                checks.append({"symbol":p["symbol"],"side":side,"prehistory_stable":signature(trades)==signature(check_trades)})
            if mode == "next_open_100u":
                cutoff=START+19*24*HOUR
                prefix=[c for c in candles if c.ts<cutoff]
                prefix_trades,_=run(prefix,instrument,cfg,side)
                sig=lambda ts:[(t.entry_ts,t.exit_ts,str(t.pnl)) for t in ts if START<=t.exit_ts<cutoff-HOUR]
                assert sig(trades)==sig(prefix_trades), "Future-data prefix stability failed"
                checks.append({"symbol":p["symbol"],"side":side,"future_prefix_stable":True})
            print(mode,p["symbol"],side,len(aug),round(sum(float(t.pnl) for t in aug),2), flush=True)
        df = pd.DataFrame(rows).sort_values(["exit_ts","symbol","side"]).reset_index(drop=True)
        df.to_csv(OUT / f"backtest_{mode}.csv",index=False,encoding="utf-8-sig")
        save(f"open_positions_{mode}.json",open_rows)
        summary = {"overall":metrics(df), "directions": {k:metrics(s) for k,s in df.groupby("side")},
                   "symbols":{k:metrics(s) for k,s in df.groupby("symbol")},
                   "symbol_sides":{f"{k[0]} {k[1]}":metrics(s) for k,s in df.groupby(["symbol","side"])},
                   "net_before_funding":float(df.net_before_funding.sum()), "slippage_cost":float(df.slippage_cost.sum()), "open_positions":len(open_rows), "checks":checks}
        save(f"summary_{mode}.json",summary)
        all_summaries[mode] = summary["overall"]
    save("backtest_comparison.json",all_summaries)
    print(json.dumps(all_summaries,ensure_ascii=False,indent=2),flush=True)


if __name__ == "__main__":
    run_all()
