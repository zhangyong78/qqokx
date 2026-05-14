from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator

from okx_quant.app_paths import cache_dir_path
from okx_quant.models import Candle


_DB_FILE_NAME = "candle_store.db"
_THREAD_LOCAL = threading.local()


def candle_store_db_path(base_dir: Path | str | None = None) -> Path:
    root = Path(base_dir) if base_dir is not None else cache_dir_path()
    return root / _DB_FILE_NAME


def _normalize_inst_id(inst_id: str) -> str:
    return inst_id.strip().upper()


def _normalize_bar(bar: str) -> str:
    return bar.strip()


def _connection_cache() -> dict[str, sqlite3.Connection]:
    cache = getattr(_THREAD_LOCAL, "connections", None)
    if cache is None:
        cache = {}
        _THREAD_LOCAL.connections = cache
    return cache


def _open_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            inst_id TEXT NOT NULL,
            bar TEXT NOT NULL,
            ts INTEGER NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume TEXT NOT NULL,
            confirmed INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (inst_id, bar, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candle_cache_migrations (
            inst_id TEXT NOT NULL,
            bar TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_size INTEGER NOT NULL,
            source_mtime_ns INTEGER NOT NULL,
            migrated_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            PRIMARY KEY (inst_id, bar, source_path)
        )
        """
    )
    conn.commit()
    return conn


@contextmanager
def _connection(base_dir: Path | str | None = None) -> Iterator[sqlite3.Connection]:
    db_path = candle_store_db_path(base_dir)
    if base_dir is None:
        cache = _connection_cache()
        key = str(db_path)
        conn = cache.get(key)
        if conn is None:
            conn = _open_connection(db_path)
            cache[key] = conn
        yield conn
        return

    conn = _open_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


def _candle_to_row(inst_id: str, bar: str, candle: Candle) -> tuple[str, str, int, str, str, str, str, str, int]:
    return (
        inst_id,
        bar,
        int(candle.ts),
        str(candle.open),
        str(candle.high),
        str(candle.low),
        str(candle.close),
        str(candle.volume),
        1 if candle.confirmed else 0,
    )


def _row_to_candle(row: tuple[object, ...]) -> Candle:
    return Candle(
        ts=int(row[0]),
        open=Decimal(str(row[1])),
        high=Decimal(str(row[2])),
        low=Decimal(str(row[3])),
        close=Decimal(str(row[4])),
        volume=Decimal(str(row[5])),
        confirmed=bool(row[6]),
    )


def upsert_candles(
    inst_id: str,
    bar: str,
    candles: list[Candle],
    *,
    base_dir: Path | str | None = None,
) -> None:
    if not candles:
        return
    normalized_inst_id = _normalize_inst_id(inst_id)
    normalized_bar = _normalize_bar(bar)
    with _connection(base_dir) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO candles
                (inst_id, bar, ts, open, high, low, close, volume, confirmed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [_candle_to_row(normalized_inst_id, normalized_bar, candle) for candle in candles],
        )
        conn.commit()


def get_candles(
    inst_id: str,
    bar: str,
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    limit: int | None = None,
    base_dir: Path | str | None = None,
) -> list[Candle]:
    normalized_inst_id = _normalize_inst_id(inst_id)
    normalized_bar = _normalize_bar(bar)
    params: list[object] = [normalized_inst_id, normalized_bar]
    where = "WHERE inst_id = ? AND bar = ?"
    if start_ts is not None:
        where += " AND ts >= ?"
        params.append(int(start_ts))
    if end_ts is not None:
        where += " AND ts <= ?"
        params.append(int(end_ts))

    if limit is not None:
        sql = (
            "SELECT ts, open, high, low, close, volume, confirmed "
            f"FROM candles {where} ORDER BY ts DESC LIMIT ?"
        )
        params.append(max(0, int(limit)))
        with _connection(base_dir) as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_candle(row) for row in reversed(rows)]

    sql = f"SELECT ts, open, high, low, close, volume, confirmed FROM candles {where} ORDER BY ts ASC"
    with _connection(base_dir) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_candle(row) for row in rows]


def get_candles_before(
    inst_id: str,
    bar: str,
    *,
    before_ts: int,
    limit: int,
    base_dir: Path | str | None = None,
) -> list[Candle]:
    if limit <= 0:
        return []
    normalized_inst_id = _normalize_inst_id(inst_id)
    normalized_bar = _normalize_bar(bar)
    with _connection(base_dir) as conn:
        rows = conn.execute(
            """
            SELECT ts, open, high, low, close, volume, confirmed
            FROM candles
            WHERE inst_id = ? AND bar = ? AND ts < ?
            ORDER BY ts DESC
            LIMIT ?
            """,
            (normalized_inst_id, normalized_bar, int(before_ts), int(limit)),
        ).fetchall()
    return [_row_to_candle(row) for row in reversed(rows)]


def get_candle_count(inst_id: str, bar: str, *, base_dir: Path | str | None = None) -> int:
    normalized_inst_id = _normalize_inst_id(inst_id)
    normalized_bar = _normalize_bar(bar)
    with _connection(base_dir) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM candles WHERE inst_id = ? AND bar = ?",
            (normalized_inst_id, normalized_bar),
        ).fetchone()
    return int(row[0]) if row else 0


def _migration_is_current(
    conn: sqlite3.Connection,
    *,
    inst_id: str,
    bar: str,
    json_path: Path,
) -> bool:
    stat = json_path.stat()
    row = conn.execute(
        """
        SELECT source_size, source_mtime_ns
        FROM candle_cache_migrations
        WHERE inst_id = ? AND bar = ? AND source_path = ?
        """,
        (inst_id, bar, str(json_path)),
    ).fetchone()
    return bool(row and int(row[0]) == stat.st_size and int(row[1]) == stat.st_mtime_ns)


def migrate_json_cache_file(
    inst_id: str,
    bar: str,
    json_path: Path,
    *,
    base_dir: Path | str | None = None,
) -> int:
    if not json_path.exists():
        return 0
    normalized_inst_id = _normalize_inst_id(inst_id)
    normalized_bar = _normalize_bar(bar)
    with _connection(base_dir) as conn:
        if _migration_is_current(conn, inst_id=normalized_inst_id, bar=normalized_bar, json_path=json_path):
            row = conn.execute(
                "SELECT COUNT(*) FROM candles WHERE inst_id = ? AND bar = ?",
                (normalized_inst_id, normalized_bar),
            ).fetchone()
            return int(row[0]) if row else 0

        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception:
            return 0
        rows = payload.get("candles") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return 0

        candles: list[tuple[str, str, int, str, str, str, str, str, int]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            try:
                candles.append(
                    (
                        normalized_inst_id,
                        normalized_bar,
                        int(item["ts"]),
                        str(item["open"]),
                        str(item["high"]),
                        str(item["low"]),
                        str(item["close"]),
                        str(item.get("volume", "0")),
                        1 if bool(item.get("confirmed", True)) else 0,
                    )
                )
            except Exception:
                continue

        if candles:
            conn.executemany(
                """
                INSERT OR REPLACE INTO candles
                    (inst_id, bar, ts, open, high, low, close, volume, confirmed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                candles,
            )
        stat = json_path.stat()
        conn.execute(
            """
            INSERT OR REPLACE INTO candle_cache_migrations
                (inst_id, bar, source_path, source_size, source_mtime_ns, migrated_at, row_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_inst_id,
                normalized_bar,
                str(json_path),
                stat.st_size,
                stat.st_mtime_ns,
                datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                len(candles),
            ),
        )
        conn.commit()
        return len(candles)
