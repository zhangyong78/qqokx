from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from html import escape
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


@dataclass(frozen=True, slots=True)
class StrategyStatusEmailRow:
    session: str
    api: str
    account_equity: str
    strategy: str
    symbol: str
    direction: str
    open_qty: str
    entry_price: str
    stop_price: str
    take_profit: str
    live_pnl: str
    net_pnl: str
    last_net_pnl: str
    status: str
    started: str
    risk_amount: str


@dataclass(frozen=True, slots=True)
class StrategyStatusEmailContent:
    subject: str
    body: str
    html_body: str


_COLUMN_LABELS: tuple[tuple[str, str], ...] = (
    ("api", "API"),
    ("session", "会话"),
    ("account_equity", "账户总权益"),
    ("strategy", "策略"),
    ("symbol", "标的"),
    ("direction", "方向"),
    ("open_qty", "开仓数量"),
    ("entry_price", "开仓价"),
    ("stop_price", "止损价"),
    ("take_profit", "止盈价"),
    ("live_pnl", "实时浮盈亏"),
    ("net_pnl", "净盈亏"),
    ("last_net_pnl", "上次净盈亏"),
    ("status", "状态"),
    ("started", "启动时间"),
    ("risk_amount", "风险金"),
)


def _clean_cell(value: object) -> str:
    text = str(value or "").strip()
    return text or "-"


def build_strategy_status_email(
    rows: list[StrategyStatusEmailRow],
    *,
    scheduled_for: datetime,
    generated_at: datetime,
    is_test: bool = False,
) -> StrategyStatusEmailContent:
    ordered = sorted(rows, key=lambda row: (_clean_cell(row.api).casefold(), _clean_cell(row.session)))
    api_counts = Counter(_clean_cell(row.api) for row in ordered)
    title = "策略运行状态测试" if is_test else "策略运行状态"
    subject_time = generated_at if is_test else scheduled_for
    subject = f"[OKXQQ] {title} | {subject_time:%Y-%m-%d %H:%M} | 活跃 {len(ordered)}"

    intro_lines = [
        f"计划时段：{scheduled_for:%Y-%m-%d %H:%M}",
        f"生成时间：{generated_at:%Y-%m-%d %H:%M:%S}",
        f"活跃会话：{len(ordered)}",
        f"涉及 API：{len(api_counts)}",
    ]
    if is_test:
        intro_lines.insert(0, "本邮件由手动测试触发，不占用定时发送时段。")
    intro_lines.extend(f"{api}：{count}" for api, count in sorted(api_counts.items()))

    header = "\t".join(label for _name, label in _COLUMN_LABELS)
    if ordered:
        detail_lines = [
            "\t".join(_clean_cell(getattr(row, name)) for name, _label in _COLUMN_LABELS)
            for row in ordered
        ]
        body = "\n".join([*intro_lines, "", header, *detail_lines])
    else:
        body = "\n".join([*intro_lines, "", "当前无活跃策略"])

    summary_html = "".join(f"<li>{escape(line)}</li>" for line in intro_lines)
    if ordered:
        heading_html = "".join(f"<th>{escape(label)}</th>" for _name, label in _COLUMN_LABELS)
        row_html = "".join(
            "<tr>"
            + "".join(
                f"<td>{escape(_clean_cell(getattr(row, name)))}</td>"
                for name, _label in _COLUMN_LABELS
            )
            + "</tr>"
            for row in ordered
        )
        detail_html = f"<table><thead><tr>{heading_html}</tr></thead><tbody>{row_html}</tbody></table>"
    else:
        detail_html = "<p><strong>当前无活跃策略</strong></p>"
    html_body = (
        "<!doctype html><html><head><meta charset=\"utf-8\"><style>"
        "body{font-family:Arial,'Microsoft YaHei',sans-serif;color:#1f2328}"
        "table{border-collapse:collapse;font-size:12px;width:100%}"
        "th,td{border:1px solid #d0d7de;padding:6px 8px;text-align:left;white-space:nowrap}"
        "th{background:#f6f8fa}tbody tr:nth-child(even){background:#fbfcfd}"
        "</style></head><body>"
        f"<h2>{escape(title)}</h2><ul>{summary_html}</ul>{detail_html}</body></html>"
    )
    return StrategyStatusEmailContent(subject=subject, body=body, html_body=html_body)


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
