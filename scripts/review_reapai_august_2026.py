"""ReapAI August review: read-only account queries, isolated research outputs.

Never writes application state or sends trading requests. Credentials are loaded
only for the named profile and never included in output artifacts or logs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from decimal import Decimal
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "reapai_august_2026_full"
DATA = Path("D:/qqokx_data")
sys.path.insert(0, str(ROOT))
sys.path.append(str(ROOT / ".venv/Lib/site-packages"))
os.environ["QQOKX_DATA_DIR"] = str(OUT / "runtime_data")
BJT = timezone(timedelta(hours=8))
SYMBOLS = tuple(f"{c}-USDT-SWAP" for c in ("BTC", "ETH", "SOL", "DOGE"))
HOUR = 3_600_000


def ms(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=BJT).timestamp() * 1000)


START, END = ms("2026-08-01"), ms("2026-09-01")
PRELOAD = ms("2026-05-01")


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def date(t):
    return datetime.fromtimestamp(int(t) / 1000, BJT).isoformat()


def inspect_local():
    manifest = {}
    for kind in ("position", "fills", "order"):
        p = DATA / f"state/history/ReapAI/live/{kind}_history.json"
        data = read(p)
        records = data["records"]
        key = "fill_time" if kind == "fills" else "update_time"
        aug = [r for r in records if START <= int(r[key]) < END]
        manifest[kind] = {"updated_at": data.get("updated_at"), "all": len(records), "aug": len(aug),
                          "first": date(min(int(r[key]) for r in records)),
                          "last": date(max(int(r[key]) for r in records)),
                          "symbols": dict(Counter(r["inst_id"] for r in aug))}
    db = DATA / "cache/candle_store.db"
    if Path(str(db) + "-wal").exists():
        raise RuntimeError("Candle store has an active WAL; immutable read would miss changes")
    with sqlite3.connect(f"file:{db.as_posix()}?mode=ro&immutable=1", uri=True) as conn:
        candles = {}
        for symbol in SYMBOLS:
            rows = conn.execute("SELECT ts,open,high,low,close,volume,confirmed FROM candles WHERE inst_id=? AND bar='1H' AND ts>=? AND ts<? ORDER BY ts", (symbol, PRELOAD, END)).fetchall()
            save(f"candles_{symbol}_local.json", rows)
            present = {r[0] for r in rows if r[6]}
            candles[symbol] = {"rows": len(rows), "first": date(rows[0][0]), "last": date(rows[-1][0]),
                               "aug_confirmed": sum(START <= t < END for t in present),
                               "aug_missing": sum(t not in present for t in range(START, END, HOUR))}
        manifest["candles"] = candles
    histories = read(DATA / "state/strategy_history.json")["records"]
    # Inspect only nonsecret metadata; account credentials never enter this file.
    manifest["strategy_record_keys"] = list(histories[0]) if histories else []
    reap = [r for r in histories if any(str(r.get(k, "")).lower() == "reapai" for k in ("profile_name", "api_name", "api_profile", "account_name"))]
    save("reapai_strategy_history.json", reap)
    manifest["reap_strategy_records"] = len(reap)
    bundle_path = DATA / "reports/analysis/packages/最佳参数组合包.json"
    bundle = read(bundle_path)
    save("parameter_bundle.json", bundle)
    manifest["bundle_sha256"] = hashlib.sha256(bundle_path.read_bytes()).hexdigest()
    manifest["bundle_created"] = bundle.get("created_at")
    save("local_inventory.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)


def sync():
    from okx_quant.okx_client import OkxRestClient
    from okx_quant.models import Credentials
    from okx_quant.persistence import load_credentials_profiles_snapshot
    profiles = load_credentials_profiles_snapshot(DATA / "config/credentials.json")["profiles"]
    if "ReapAI" not in profiles:
        raise RuntimeError("Exact ReapAI profile missing")
    profile = profiles["ReapAI"]
    if not all(profile.get(k) for k in ("api_key", "secret_key", "passphrase")):
        raise RuntimeError("ReapAI credentials could not be decrypted")
    creds = Credentials(api_key=profile["api_key"], secret_key=profile["secret_key"], passphrase=profile["passphrase"], profile_name="ReapAI")
    if profile.get("environment") != "live":
        raise RuntimeError("ReapAI profile is not marked live")
    client = OkxRestClient()
    allowed = {"/api/v5/account/positions-history", "/api/v5/trade/fills-history", "/api/v5/trade/orders-history-archive", "/api/v5/account/bills-archive", "/api/v5/public/instruments", "/api/v5/market/history-candles", "/api/v5/public/funding-rate-history", "/api/v5/public/time"}

    def get(path, params=None, auth=False):
        assert path in allowed
        for attempt in range(3):
            try:
                return client._request("GET", path, params=params, auth=auth, credentials=creds if auth else None, timeout_seconds=15).get("data", [])
            except Exception as e:
                if attempt == 2:
                    raise RuntimeError(f"GET {path} failed: {type(e).__name__}: {e}") from None
                time.sleep(1 + attempt)

    print("ReapAI live: read-only history synchronization", flush=True)
    print("server_time", get("/api/v5/public/time"), flush=True)
    audit = {"fetched_at": datetime.now(BJT).isoformat(), "profile": "ReapAI", "environment": "live",
             "start": date(START), "end_exclusive": date(END), "configured_fees": {k: profile.get(k) for k in ("futures_maker_fee_rate", "futures_taker_fee_rate")}}

    def paged(name, path, params, cursor_key, dedup_keys, time_key=None, floor=None, limit=100):
        target = OUT / (name + ".json")
        if target.exists():
            rows = read(target)
            print(name, "reuse", len(rows), flush=True)
            return rows
        rows, seen, cursor = [], set(), None
        for page in range(150):
            query = {**params, "limit": str(limit)}
            if cursor is not None:
                query["after"] = str(cursor)
            batch = get(path, query, auth=path.startswith(("/api/v5/account/", "/api/v5/trade/")))
            if not batch:
                break
            added = 0
            for r in batch:
                key = tuple(r.get(k) for k in dedup_keys)
                if key not in seen:
                    seen.add(key)
                    rows.append(r)
                    added += 1
            next_cursor = batch[-1].get(cursor_key)
            print(name, "page", page + 1, "rows", len(rows), flush=True)
            if floor is not None and min(int(r[time_key]) for r in batch) < floor:
                break
            if len(batch) < limit:
                break
            if not next_cursor or next_cursor == cursor or added == 0:
                raise RuntimeError(f"Pagination stalled: {name}")
            cursor = next_cursor
            time.sleep(0.3)
        else:
            raise RuntimeError(f"Pagination cap reached: {name}")
        save(name + ".json", rows)
        return rows

    paged("positions_raw", "/api/v5/account/positions-history", {"instType": "SWAP"}, "uTime", ("instId", "posId", "cTime", "uTime"), "uTime", ms("2026-07-01"))
    window = {"instType": "SWAP", "begin": str(ms("2026-07-01")), "end": str(END - 1)}
    paged("fills_raw", "/api/v5/trade/fills-history", window, "billId", ("billId",))
    paged("orders_raw", "/api/v5/trade/orders-history-archive", window, "ordId", ("ordId",))
    paged("bills_raw", "/api/v5/account/bills-archive", {"begin": str(START), "end": str(END - 1)}, "billId", ("billId",))
    for symbol in SYMBOLS:
        instrument_path = OUT / f"instrument_{symbol}.json"
        if not instrument_path.exists():
            save(instrument_path.name, get("/api/v5/public/instruments", {"instType": "SWAP", "instId": symbol}))
        target = OUT / f"candles_{symbol}.json"
        if not target.exists():
            local = read(OUT / f"candles_{symbol}_local.json")
            by_ts = {int(r[0]): r for r in local if r[6]}
            missing = [t for t in range(PRELOAD, END, HOUR) if t not in by_ts]
            while missing:
                latest = missing[-1]
                batch = get("/api/v5/market/history-candles", {"instId": symbol, "bar": "1H", "after": str(latest + 1), "limit": "300"})
                added = 0
                for r in batch:
                    t = int(r[0])
                    if PRELOAD <= t < END and str(r[8]) == "1":
                        added += t not in by_ts
                        by_ts[t] = [t, *r[1:6], 1]
                if not added:
                    raise RuntimeError(f"Could not fill candle gap {symbol} {date(latest)}")
                missing = [t for t in missing if t not in by_ts]
                print(symbol, "remaining_missing_candles", len(missing), flush=True)
                time.sleep(0.25)
            save(target.name, [by_ts[t] for t in sorted(by_ts)])
        paged(f"funding_{symbol}", "/api/v5/public/funding-rate-history", {"instId": symbol, "after": str(END)}, "fundingTime", ("fundingTime",), "fundingTime", ms("2026-07-01"))
    save("sync_manifest.json", audit)
    print("SYNC_COMPLETE", flush=True)


def analyze_live():
    import pandas as pd
    D = lambda x: Decimal(str(x or "0"))
    positions = read(OUT / "positions_raw.json")
    latest = {}
    for r in positions:
        key = (r["instId"], r["posId"], r["cTime"])
        if key not in latest or int(r["uTime"]) > int(latest[key]["uTime"]):
            latest[key] = r
    selected = [r for r in latest.values() if START <= int(r["uTime"]) < END]
    rows = []
    for r in sorted(selected, key=lambda x: int(x["uTime"])):
        gross, fee, funding = map(D, (r.get("pnl"), r.get("fee"), r.get("fundingFee")))
        net = D(r.get("realizedPnl"))
        rows.append(dict(symbol=r["instId"], side=r.get("direction") or r["posSide"], entry_ts=int(r["cTime"]), exit_ts=int(r["uTime"]),
                         entry_time=date(r["cTime"]), exit_time=date(r["uTime"]), entry_price=float(D(r["openAvgPx"])), exit_price=float(D(r["closeAvgPx"])),
                         contracts=float(D(r.get("closeTotalPos"))), gross_pnl=float(gross), fee=float(fee), funding=float(funding), net_pnl=float(net),
                         reconciliation_residual=float(net-gross-fee-funding), holding_hours=(int(r["uTime"])-int(r["cTime"]))/HOUR,
                         position_type=r.get("type"), lifecycle=f'{r["instId"]}|{r["posId"]}|{r["cTime"]}'))
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "live_trades.csv", index=False, encoding="utf-8-sig")
    fills = [r for r in read(OUT / "fills_raw.json") if START <= int(r.get("fillTime") or r["ts"]) < END]
    bills = [r for r in read(OUT / "bills_raw.json") if START <= int(r["ts"]) < END]
    fill_rows = []
    for r in fills:
        ct = D(read(OUT / f'instrument_{r["instId"]}.json')[0]["ctVal"])
        notional = D(r["fillPx"]) * D(r["fillSz"]) * ct
        fill_rows.append({**r, "time_bjt": date(r.get("fillTime") or r["ts"]), "notional": float(notional), "fee_rate": float(-D(r.get("fee"))/notional) if notional else None})
    pd.DataFrame(fill_rows).to_csv(OUT / "live_fills.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(bills).to_csv(OUT / "live_bills.csv", index=False, encoding="utf-8-sig")
    summary = {"live": metrics(df), "directions": {k:metrics(s) for k,s in df.groupby("side")}, "symbols": {k:metrics(s) for k,s in df.groupby("symbol")},
               "symbol_sides": {f"{k[0]} {k[1]}":metrics(s) for k,s in df.groupby(["symbol","side"])},
               "position_types": dict(Counter(r.get("type") for r in selected)), "raw_position_count": len(positions), "unique_lifecycles": len(latest),
               "fills_count": len(fills), "fills_pnl": str(sum(D(r.get("fillPnl")) for r in fills)), "fills_fee": str(sum(D(r.get("fee")) for r in fills)),
               "bills_count": len(bills), "bills_types": dict(Counter(r.get("type") for r in bills)),
               "bills_subtypes": dict(Counter(r.get("subType") for r in bills)),
               "bills_balance_change": str(sum(D(r.get("balChg")) for r in bills)),
               "bills_pnl": str(sum(D(r.get("pnl")) for r in bills)), "bills_fee": str(sum(D(r.get("fee")) for r in bills)),
               "fee_rates": {k: sorted(set(round(x["fee_rate"],8) for x in fill_rows if x["execType"] == k)) for k in ("M","T")},
               "max_position_reconciliation_residual": float(df.reconciliation_residual.abs().max()),
               "cross_month_closes": int((df.entry_ts < START).sum())}
    summary["daily"] = {k: metrics(s) for k,s in df.groupby(df.exit_time.str[:10])}
    save("live_summary.json", summary)
    print(json.dumps({k:v for k,v in summary.items() if k != "daily"}, ensure_ascii=False, indent=2), flush=True)


def metrics(df):
    if df.empty:
        return {"trades": 0, "net_pnl": 0.0}
    x = df.sort_values("exit_ts")
    pnls = x.net_pnl.astype(float)
    wins, losses = pnls[pnls > 0], pnls[pnls < 0]
    cumulative = pnls.cumsum()
    peak = cumulative.cummax().clip(lower=0)
    streak = max_streak = 0
    for p in pnls:
        streak = streak + 1 if p < 0 else 0
        max_streak = max(streak, max_streak)
    return {"trades": len(x), "wins": len(wins), "losses": len(losses), "win_rate": len(wins)/len(x)*100,
            "gross_pnl": float(x.gross_pnl.sum()), "fee": float(x.fee.sum()), "funding": float(x.funding.sum()), "net_pnl": float(pnls.sum()),
            "profit_factor": float(wins.sum()/-losses.sum()) if len(losses) else None,
            "average_win": float(wins.mean()) if len(wins) else 0, "average_loss": float(losses.mean()) if len(losses) else 0,
            "payoff_ratio": float(wins.mean()/-losses.mean()) if len(wins) and len(losses) else None,
            "max_realized_drawdown": float((peak-cumulative).max()), "max_loss_streak": max_streak,
            "max_win": float(pnls.max()), "max_loss": float(pnls.min()), "median_holding_hours": float(x.holding_hours.median())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("inspect", "sync", "live"))
    args = parser.parse_args()
    {"inspect": inspect_local, "sync": sync, "live": analyze_live}[args.mode]()
