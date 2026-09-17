import csv
import hashlib
import json
import os
import glob
import shutil
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone, timedelta

CSV_FILES = glob.glob(r"C:\Users\31612\Desktop\*.csv")
DATA_ROOT = r"D:\qqokx_data\state\history"
SRC_PATH = os.path.join(DATA_ROOT, "159", "live", "position_history.json")
DST_PATH = os.path.join(DATA_ROOT, "2211", "live", "position_history.json")
BACKUP_DIR = r"D:\qqokx\backups"


def clean(value):
    return str(value or "").replace("\ufeff", "").strip()


def dec(value):
    value = clean(value)
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def numstr(value):
    value = dec(value) if not isinstance(value, Decimal) else value
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def key_num(value):
    value = dec(value) if not isinstance(value, Decimal) else value
    if value is None:
        return None
    return numstr(value.quantize(Decimal("0.00000001")))


def time_ms(value):
    text = clean(value)
    dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return int(dt.timestamp() * 1000)


def csv_key(row, headers):
    direction = "long" if clean(row[headers[6]]) == "做多" else "short"
    return (
        clean(row[headers[4]]),
        direction,
        key_num(row[headers[9]]),
        key_num(row[headers[10]]),
        key_num(row[headers[11]]),
    )


def local_key(record):
    return (
        record.get("inst_id"),
        record.get("direction"),
        key_num(record.get("close_size")),
        key_num(record.get("open_avg_price")),
        key_num(record.get("close_avg_price")),
    )


def make_record(row, headers):
    inst_id = clean(row[headers[4]])
    direction = "long" if clean(row[headers[6]]) == "做多" else "short"
    open_time = time_ms(row[headers[0]])
    update_time = time_ms(row[headers[1]])
    max_size = numstr(row[headers[8]])
    close_size = numstr(row[headers[9]])
    open_px = numstr(row[headers[10]])
    close_px = numstr(row[headers[11]])
    ccy = clean(row[headers[12]]) or None
    pnl = numstr(row[headers[13]]) or "0"
    fee_raw = clean(row[headers[15]])
    fee = numstr(fee_raw) if fee_raw else None
    realized = str(Decimal(pnl) + (Decimal(fee) if fee is not None else Decimal("0")))
    if "." in realized:
        realized = realized.rstrip("0").rstrip(".")
    mgn_mode = "isolated" if "逐仓" in clean(row[headers[5]]) else "cross"
    ratio = numstr(row[headers[14]]) or "0"
    stable = "|".join([inst_id, direction, close_size, open_px, close_px, str(open_time), str(update_time)])
    pos_id = "csv-import-" + hashlib.sha256(stable.encode("utf-8")).hexdigest()[:24]
    meta = {
        "lifecycleKey": "CSVIMPORT|" + stable,
        "snapshotSeq": 1,
        "snapshotCount": 1,
        "isLatestSnapshot": True,
        "isPartialClose": False,
        "prevCloseTotal": "",
        "incrementalCloseTotal": close_size,
        "openTotal": max_size,
        "closeTotal": close_size,
    }
    raw = {
        "cTime": str(open_time),
        "ccy": ccy,
        "closeAvgPx": close_px,
        "closeTotalPos": close_size,
        "direction": direction,
        "fee": fee if fee is not None else "0",
        "fundingFee": "0",
        "instId": inst_id,
        "instType": "OPTION",
        "lever": "0.0",
        "liqPenalty": "0",
        "mgnMode": mgn_mode,
        "nonSettleAvgPx": "",
        "openAvgPx": open_px,
        "openMaxPos": max_size,
        "pnl": pnl,
        "pnlRatio": ratio,
        "posId": pos_id,
        "posSide": "net",
        "realizedPnl": realized,
        "settledPnl": "",
        "triggerPx": "",
        "type": "2",
        "uTime": str(update_time),
        "uly": clean(row[headers[3]]),
        "qqokxHistoryMeta": meta,
    }
    record = {
        "update_time": update_time,
        "inst_id": inst_id,
        "inst_type": "OPTION",
        "mgn_mode": mgn_mode,
        "pos_side": "net",
        "direction": direction,
        "open_avg_price": open_px,
        "close_avg_price": close_px,
        "close_size": close_size,
        "pnl": pnl,
        "realized_pnl": realized,
        "settle_pnl": None,
        "raw": raw,
        "fee": fee,
        "fee_currency": ccy,
        "funding_fee": "0",
        "qqokx_history_meta": meta,
    }
    if fee is None:
        record.pop("fee")
    return record


def load_csv_rows():
    with open(CSV_PATH, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    headers = rows[1]
    return headers, [dict(zip(headers, row)) for row in rows[2:] if any(row)]


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path, value):
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    os.replace(temp, path)


def main():
    if len(CSV_FILES) != 1:
        raise RuntimeError(f"expected one Desktop CSV, found {len(CSV_FILES)}")
    csv_path = CSV_FILES[0]
    src = load_json(SRC_PATH)
    dst = load_json(DST_PATH)
    global CSV_PATH
    CSV_PATH = csv_path
    headers, rows = load_csv_rows()
    csv_rows = [row for row in rows if clean(row.get("业务线")) == "期权"]
    local = src.get("records", [])
    local_keys = {local_key(record) for record in local if record.get("inst_type") == "OPTION"}
    missing_rows = [row for row in csv_rows if csv_key(row, headers) not in local_keys]
    imported = [make_record(row, headers) for row in missing_rows]
    if len(imported) != 62:
        raise RuntimeError(f"expected 62 missing CSV rows, found {len(imported)}")
    now_tag = "20260916"
    os.makedirs(BACKUP_DIR, exist_ok=True)
    src_backup = os.path.join(BACKUP_DIR, "position_history_159_before_csv_import_" + now_tag + ".json")
    dst_backup = os.path.join(BACKUP_DIR, "position_history_2211_before_csv_import_" + now_tag + ".json")
    shutil.copy2(SRC_PATH, src_backup)
    shutil.copy2(DST_PATH, dst_backup)
    src["records"] = local + imported
    save_json(SRC_PATH, src)
    save_json(DST_PATH, src)
    print(json.dumps({
        "imported": len(imported),
        "source_records": len(src["records"]),
        "source_option_records": sum(r.get("inst_type") == "OPTION" for r in src["records"]),
        "destination_records": len(load_json(DST_PATH)["records"]),
        "src_backup": src_backup,
        "dst_backup": dst_backup,
        "first_imported": imported[0]["inst_id"],
        "last_imported": imported[-1]["inst_id"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
