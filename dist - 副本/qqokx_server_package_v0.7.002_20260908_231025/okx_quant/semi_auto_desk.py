from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from okx_quant.persistence import semi_auto_desk_file_path


SEMI_AUTO_TASK_MODE_VALUES: tuple[str, ...] = ("evaluate_once", "wait_one")
SEMI_AUTO_TASK_STATUS_VALUES: tuple[str, ...] = (
    "queued",
    "running",
    "opened",
    "settling",
    "completed_no_signal",
    "completed_closed",
    "blocked_conflict",
    "cancelled",
    "failed",
)


@dataclass
class SemiAutoPoolRecord:
    pool_id: str
    name: str
    api_name: str
    initial_capital: Decimal
    status: str = "waiting_selection"
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class SemiAutoTaskRecord:
    task_id: str
    pool_id: str
    template_payload: dict[str, object]
    mode: str = "wait_one"
    status: str = "queued"
    session_id: str = ""
    symbol: str = ""
    direction_label: str = ""
    bar: str = ""
    started_at: datetime | None = None
    ended_at: datetime | None = None
    ended_reason: str = ""
    ledger_record_id: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class SemiAutoDeskSnapshot:
    pools: list[SemiAutoPoolRecord] = field(default_factory=list)
    tasks: list[SemiAutoTaskRecord] = field(default_factory=list)


@dataclass(frozen=True)
class SemiAutoPoolSummary:
    realized_count: int
    win_count: int
    loss_count: int
    net_pnl: Decimal
    virtual_equity: Decimal
    win_rate: Decimal
    average_win: Decimal | None
    average_loss: Decimal | None
    profit_loss_ratio: Decimal | None


def _parse_decimal(value: object, default: Decimal | None = None) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return default


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _pool_to_payload(pool: SemiAutoPoolRecord) -> dict[str, object]:
    return {
        "pool_id": pool.pool_id,
        "name": pool.name,
        "api_name": pool.api_name,
        "initial_capital": _decimal_text(pool.initial_capital),
        "status": pool.status,
        "created_at": pool.created_at.isoformat(timespec="seconds"),
        "updated_at": pool.updated_at.isoformat(timespec="seconds"),
    }


def _pool_from_payload(payload: object) -> SemiAutoPoolRecord | None:
    if not isinstance(payload, dict):
        return None
    pool_id = str(payload.get("pool_id") or "").strip()
    initial_capital = _parse_decimal(payload.get("initial_capital"))
    if not pool_id or initial_capital is None:
        return None
    now = datetime.now()
    return SemiAutoPoolRecord(
        pool_id=pool_id,
        name=str(payload.get("name") or pool_id).strip() or pool_id,
        api_name=str(payload.get("api_name") or "").strip(),
        initial_capital=initial_capital,
        status=str(payload.get("status") or "waiting_selection").strip() or "waiting_selection",
        created_at=_parse_datetime(payload.get("created_at")) or now,
        updated_at=_parse_datetime(payload.get("updated_at")) or now,
    )


def _task_to_payload(task: SemiAutoTaskRecord) -> dict[str, object]:
    return {
        "task_id": task.task_id,
        "pool_id": task.pool_id,
        "template_payload": task.template_payload,
        "mode": task.mode,
        "status": task.status,
        "session_id": task.session_id,
        "symbol": task.symbol,
        "direction_label": task.direction_label,
        "bar": task.bar,
        "started_at": task.started_at.isoformat(timespec="seconds") if task.started_at is not None else None,
        "ended_at": task.ended_at.isoformat(timespec="seconds") if task.ended_at is not None else None,
        "ended_reason": task.ended_reason,
        "ledger_record_id": task.ledger_record_id,
        "created_at": task.created_at.isoformat(timespec="seconds"),
        "updated_at": task.updated_at.isoformat(timespec="seconds"),
    }


def _task_from_payload(payload: object) -> SemiAutoTaskRecord | None:
    if not isinstance(payload, dict):
        return None
    task_id = str(payload.get("task_id") or "").strip()
    pool_id = str(payload.get("pool_id") or "").strip()
    template_payload = payload.get("template_payload")
    if not task_id or not pool_id or not isinstance(template_payload, dict):
        return None
    mode = str(payload.get("mode") or "wait_one").strip()
    status = str(payload.get("status") or "queued").strip()
    if mode not in SEMI_AUTO_TASK_MODE_VALUES:
        mode = "wait_one"
    if status not in SEMI_AUTO_TASK_STATUS_VALUES:
        status = "failed"
    now = datetime.now()
    return SemiAutoTaskRecord(
        task_id=task_id,
        pool_id=pool_id,
        template_payload=dict(template_payload),
        mode=mode,
        status=status,
        session_id=str(payload.get("session_id") or "").strip(),
        symbol=str(payload.get("symbol") or "").strip(),
        direction_label=str(payload.get("direction_label") or "").strip(),
        bar=str(payload.get("bar") or "").strip(),
        started_at=_parse_datetime(payload.get("started_at")),
        ended_at=_parse_datetime(payload.get("ended_at")),
        ended_reason=str(payload.get("ended_reason") or "").strip(),
        ledger_record_id=str(payload.get("ledger_record_id") or "").strip(),
        created_at=_parse_datetime(payload.get("created_at")) or now,
        updated_at=_parse_datetime(payload.get("updated_at")) or now,
    )


def save_semi_auto_desk_snapshot(snapshot: SemiAutoDeskSnapshot, path: Path | None = None) -> Path:
    target = path or semi_auto_desk_file_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "pools": [_pool_to_payload(pool) for pool in snapshot.pools],
                "tasks": [_task_to_payload(task) for task in snapshot.tasks],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def load_semi_auto_desk_snapshot(path: Path | None = None) -> SemiAutoDeskSnapshot:
    target = path or semi_auto_desk_file_path()
    if not target.exists():
        return SemiAutoDeskSnapshot()
    payload = json.loads(target.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return SemiAutoDeskSnapshot()
    pools = [pool for item in payload.get("pools", []) if (pool := _pool_from_payload(item)) is not None]
    tasks = [task for item in payload.get("tasks", []) if (task := _task_from_payload(item)) is not None]
    return SemiAutoDeskSnapshot(pools=pools, tasks=tasks)


def semi_auto_pool_ledger_records(pool_id: str, ledger_records: list[object]) -> list[object]:
    normalized_pool_id = str(pool_id or "").strip()
    matched = [
        record
        for record in ledger_records
        if str(getattr(record, "semi_auto_pool_id", "") or "").strip() == normalized_pool_id
    ]
    matched.sort(
        key=lambda record: (
            getattr(record, "closed_at", None) or datetime.min,
            str(getattr(record, "record_id", "") or ""),
        )
    )
    return matched


def build_semi_auto_pool_summary(
    pool: SemiAutoPoolRecord,
    tasks: list[SemiAutoTaskRecord],
    ledger_records: list[object],
) -> SemiAutoPoolSummary:
    del tasks
    records = semi_auto_pool_ledger_records(pool.pool_id, ledger_records)
    pnl_values = [Decimal(str(getattr(record, "net_pnl", None))) for record in records if getattr(record, "net_pnl", None) is not None]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    net_pnl = sum(pnl_values, Decimal("0"))
    average_win = sum(wins, Decimal("0")) / Decimal(len(wins)) if wins else None
    average_loss = sum(losses, Decimal("0")) / Decimal(len(losses)) if losses else None
    profit_loss_ratio = average_win / abs(average_loss) if average_win is not None and average_loss not in {None, Decimal("0")} else None
    realized_count = len(pnl_values)
    win_rate = Decimal(len(wins)) * Decimal("100") / Decimal(realized_count) if realized_count else Decimal("0")
    return SemiAutoPoolSummary(
        realized_count=realized_count,
        win_count=len(wins),
        loss_count=len(losses),
        net_pnl=net_pnl,
        virtual_equity=pool.initial_capital + net_pnl,
        win_rate=win_rate,
        average_win=average_win,
        average_loss=average_loss,
        profit_loss_ratio=profit_loss_ratio,
    )
