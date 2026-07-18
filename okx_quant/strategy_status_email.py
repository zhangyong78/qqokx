from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable

from okx_quant.app_paths import state_dir_path


STATUS_EMAIL_TIMES: tuple[time, ...] = (
    time(8, 0),
    time(12, 0),
    time(16, 0),
    time(20, 0),
)
STATUS_EMAIL_STATE_FILE_NAME = "strategy_status_email_state.json"
STATUS_EMAIL_STATE_RETENTION_DAYS = 14


def latest_due_status_email_slot(
    previous_check: datetime | None,
    now: datetime,
) -> datetime | None:
    if previous_check is None or now <= previous_check:
        return None
    candidates: list[datetime] = []
    current_date = previous_check.date()
    while current_date <= now.date():
        for slot_time in STATUS_EMAIL_TIMES:
            candidate = datetime.combine(current_date, slot_time)
            if previous_check < candidate <= now:
                candidates.append(candidate)
        current_date += timedelta(days=1)
    return max(candidates) if candidates else None


def status_email_slot_key(slot: datetime) -> str:
    return slot.strftime("%Y-%m-%dT%H:%M")


def strategy_status_email_state_file_path(base_dir: Path | None = None) -> Path:
    root = Path(base_dir) if base_dir is not None else state_dir_path()
    return root / STATUS_EMAIL_STATE_FILE_NAME


def load_sent_status_email_slots(
    path: Path | None = None,
    logger: Callable[[str], None] | None = None,
) -> set[str]:
    target = path or strategy_status_email_state_file_path()
    if not target.exists():
        return set()
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        raw_slots = payload.get("sent_slots", []) if isinstance(payload, dict) else []
        if not isinstance(raw_slots, list):
            raise ValueError("sent_slots 不是列表")
        return {str(item).strip() for item in raw_slots if str(item).strip()}
    except Exception as exc:
        if logger is not None:
            logger(f"读取策略状态邮件记录失败：{exc}")
        return set()


def _slot_key_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M").date()
    except ValueError:
        return None


def claim_status_email_slot(
    slot: datetime,
    *,
    path: Path | None = None,
    now: datetime | None = None,
    logger: Callable[[str], None] | None = None,
) -> bool:
    target = path or strategy_status_email_state_file_path()
    key = status_email_slot_key(slot)
    sent_slots = load_sent_status_email_slots(target, logger=logger)
    if key in sent_slots:
        return False
    reference_date = (now or datetime.now()).date()
    oldest_kept_date = reference_date - timedelta(days=STATUS_EMAIL_STATE_RETENTION_DAYS - 1)
    retained = {
        item
        for item in sent_slots
        if (parsed := _slot_key_date(item)) is not None and parsed >= oldest_kept_date
    }
    retained.add(key)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "sent_slots": sorted(retained),
        "updated_at": (now or datetime.now()).isoformat(timespec="seconds"),
    }
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)
    return True
