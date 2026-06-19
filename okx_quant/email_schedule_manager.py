from __future__ import annotations

import base64
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okx_quant.persistence import analysis_report_dir_path


EMAIL_SCHEDULE_TASK_PREFIX = "QQOKX BTC Analysis Email"
_TASK_SLOT_RE = re.compile(r"(\d{4})$")
_TASK_NAME_IN_MESSAGE_RE = re.compile(r"\\QQOKX BTC Analysis Email \d{4}")
_TASK_RESULT_LABELS = {
    0: "成功",
    1: "失败",
    267009: "运行中",
    267011: "尚未运行",
}
_EVENT_ID_LABELS = {
    100: "已启动",
    101: "启动失败",
    102: "已完成",
    107: "已触发",
    108: "错过触发",
    110: "被用户运行",
    111: "终止中",
    118: "已结束",
    129: "创建进程",
    200: "动作已启动",
    201: "动作已完成",
    203: "动作失败",
}


@dataclass(slots=True)
class EmailScheduledTaskSnapshot:
    task_name: str
    state: str
    next_run_time: str
    last_run_time: str
    last_result: int
    missed_runs: int
    command_line: str
    author: str
    logon_type: str
    start_when_available: bool
    disallow_start_if_on_batteries: bool
    stop_if_going_on_batteries: bool


@dataclass(slots=True)
class EmailScheduledTaskEvent:
    time_created: str
    event_id: int
    level: str
    task_name: str
    provider: str
    message: str


@dataclass(slots=True)
class EmailArchiveRecord:
    subject: str
    delivery_status: str
    scheduled_release_slot: str
    analysis_slot: str
    generated_at: str
    archived_at: str
    symbols: tuple[str, ...]
    meta_path: Path
    archive_html_path: Path | None
    archive_text_path: Path | None
    report_path: Path | None


@dataclass(slots=True)
class EmailScheduleSnapshotBundle:
    tasks: list[EmailScheduledTaskSnapshot]
    events: list[EmailScheduledTaskEvent]
    archives: list[EmailArchiveRecord]


def collect_email_schedule_snapshot(*, event_limit: int = 200, archive_limit: int = 120) -> EmailScheduleSnapshotBundle:
    return EmailScheduleSnapshotBundle(
        tasks=load_email_schedule_tasks(),
        events=load_email_schedule_history(limit=event_limit),
        archives=load_email_archive_records(limit=archive_limit),
    )


def load_email_schedule_tasks() -> list[EmailScheduledTaskSnapshot]:
    command = """
$ErrorActionPreference = 'Stop'
$prefix = '__TASK_PREFIX__'

function Format-TaskTime([datetime]$value) {
    if ($null -eq $value -or $value -eq [datetime]::MinValue) {
        return ''
    }
    return $value.ToString('yyyy-MM-dd HH:mm:ss')
}

$rows = @()
$tasks = Get-ScheduledTask | Where-Object { $_.TaskName -like "$prefix *" } | Sort-Object TaskName
foreach ($task in $tasks) {
    $info = $task | Get-ScheduledTaskInfo
    $action = $task.Actions | Select-Object -First 1
    $execute = ''
    $arguments = ''
    if ($null -ne $action) {
        $execute = [string]$action.Execute
        $arguments = [string]$action.Arguments
    }
    $rows += [PSCustomObject]@{
        task_name = [string]$task.TaskName
        state = [string]$task.State
        next_run_time = Format-TaskTime $info.NextRunTime
        last_run_time = Format-TaskTime $info.LastRunTime
        last_result = [int]$info.LastTaskResult
        missed_runs = [int]$info.NumberOfMissedRuns
        command_line = "$execute $arguments".Trim()
        author = [string]$task.Author
        logon_type = [string]$task.Principal.LogonType
        start_when_available = [bool]$task.Settings.StartWhenAvailable
        disallow_start_if_on_batteries = [bool]$task.Settings.DisallowStartIfOnBatteries
        stop_if_going_on_batteries = [bool]$task.Settings.StopIfGoingOnBatteries
    }
}

@($rows) | ConvertTo-Json -Depth 5 -Compress
""".replace("__TASK_PREFIX__", EMAIL_SCHEDULE_TASK_PREFIX)
    rows = _coerce_rows(_run_powershell_json(command))
    items = [
        EmailScheduledTaskSnapshot(
            task_name=str(row.get("task_name", "") or "").strip(),
            state=str(row.get("state", "") or "").strip(),
            next_run_time=str(row.get("next_run_time", "") or "").strip(),
            last_run_time=str(row.get("last_run_time", "") or "").strip(),
            last_result=_to_int(row.get("last_result")),
            missed_runs=_to_int(row.get("missed_runs")),
            command_line=str(row.get("command_line", "") or "").strip(),
            author=str(row.get("author", "") or "").strip(),
            logon_type=str(row.get("logon_type", "") or "").strip(),
            start_when_available=bool(row.get("start_when_available")),
            disallow_start_if_on_batteries=bool(row.get("disallow_start_if_on_batteries")),
            stop_if_going_on_batteries=bool(row.get("stop_if_going_on_batteries")),
        )
        for row in rows
        if str(row.get("task_name", "") or "").strip()
    ]
    items.sort(key=lambda item: task_sort_key(item.task_name))
    return items


def load_email_schedule_history(*, limit: int = 200) -> list[EmailScheduledTaskEvent]:
    safe_limit = max(1, min(limit, 500))
    fetch_limit = max(safe_limit * 6, 400)
    command = f"""
$ErrorActionPreference = 'Stop'
$prefix = '{EMAIL_SCHEDULE_TASK_PREFIX}'
$maxEvents = {fetch_limit}
$limit = {safe_limit}

function Resolve-TaskName($event) {{
    foreach ($prop in $event.Properties) {{
        if ($prop.Value -is [string] -and $prop.Value -like "\\$prefix *") {{
            return [string]$prop.Value
        }}
    }}
    if ($event.Message -match '\\\\QQOKX BTC Analysis Email \\d{{4}}') {{
        return $Matches[0]
    }}
    return ''
}}

$rows = @()
$events = Get-WinEvent -LogName 'Microsoft-Windows-TaskScheduler/Operational' -MaxEvents $maxEvents |
    Where-Object {{ $_.Message -like "*$prefix*" }} |
    Select-Object -First $limit
foreach ($event in $events) {{
    $rows += [PSCustomObject]@{{
        time_created = $event.TimeCreated.ToString('yyyy-MM-dd HH:mm:ss')
        event_id = [int]$event.Id
        level = [string]$event.LevelDisplayName
        task_name = Resolve-TaskName $event
        provider = [string]$event.ProviderName
        message = [string]$event.Message
    }}
}}

@($rows) | ConvertTo-Json -Depth 4 -Compress
"""
    rows = _coerce_rows(_run_powershell_json(command))
    items = [
        EmailScheduledTaskEvent(
            time_created=str(row.get("time_created", "") or "").strip(),
            event_id=_to_int(row.get("event_id")),
            level=str(row.get("level", "") or "").strip(),
            task_name=str(row.get("task_name", "") or "").strip(),
            provider=str(row.get("provider", "") or "").strip(),
            message=str(row.get("message", "") or "").strip(),
        )
        for row in rows
    ]
    return items


def load_email_archive_records(
    *,
    limit: int = 120,
    archive_dir: Path | None = None,
) -> list[EmailArchiveRecord]:
    target_dir = archive_dir or (analysis_report_dir_path() / "email_archives")
    if not target_dir.exists():
        return []
    records: list[EmailArchiveRecord] = []
    meta_paths = sorted(target_dir.glob("multi_coin_market_digest_email_*.json"), reverse=True)
    for meta_path in meta_paths:
        payload = _load_json_file(meta_path)
        if not isinstance(payload, dict):
            continue
        symbols = payload.get("symbols")
        archive_html_path = _optional_path(payload.get("archive_html_path"))
        archive_text_path = _optional_path(payload.get("archive_text_path"))
        report_path = _optional_path(payload.get("report_path"))
        records.append(
            EmailArchiveRecord(
                subject=str(payload.get("subject", "") or "").strip(),
                delivery_status=str(payload.get("delivery_status", "") or "").strip(),
                scheduled_release_slot=str(payload.get("scheduled_release_slot", "") or "").strip(),
                analysis_slot=str(payload.get("analysis_slot", "") or "").strip(),
                generated_at=str(payload.get("generated_at", "") or "").strip(),
                archived_at=str(payload.get("archived_at", "") or "").strip(),
                symbols=tuple(str(item).strip() for item in symbols if str(item).strip()) if isinstance(symbols, list) else (),
                meta_path=meta_path,
                archive_html_path=archive_html_path,
                archive_text_path=archive_text_path,
                report_path=report_path,
            )
        )
        if len(records) >= max(1, limit):
            break
    records.sort(key=lambda item: item.archived_at or item.generated_at or item.meta_path.name, reverse=True)
    return records


def start_email_schedule_task(task_name: str) -> None:
    target = str(task_name or "").strip()
    if not target:
        raise ValueError("task_name is required")
    command = f"""
$ErrorActionPreference = 'Stop'
Start-ScheduledTask -TaskName '{target.replace("'", "''")}'
"""
    _run_powershell_text(command)


def task_slot_label(task_name: str) -> str:
    match = _TASK_SLOT_RE.search(str(task_name or "").strip())
    if not match:
        return "-"
    raw = match.group(1)
    return f"{raw[:2]}:{raw[2:]}"


def task_sort_key(task_name: str) -> tuple[int, str]:
    match = _TASK_SLOT_RE.search(str(task_name or "").strip())
    if not match:
        return (99_999, str(task_name or ""))
    return (int(match.group(1)), str(task_name or ""))


def format_task_result(task_result: int) -> str:
    label = _TASK_RESULT_LABELS.get(int(task_result))
    if label:
        return f"{int(task_result)} {label}"
    return str(int(task_result))


def format_event_id(event_id: int) -> str:
    label = _EVENT_ID_LABELS.get(int(event_id))
    if label:
        return f"{int(event_id)} {label}"
    return str(int(event_id))


def summarize_event_message(message: str, *, max_length: int = 120) -> str:
    compact = " ".join(str(message or "").split())
    if len(compact) <= max_length:
        return compact
    if max_length <= 1:
        return compact[:max_length]
    return compact[: max_length - 1] + "…"


def normalize_event_task_name(task_name: str, message: str = "") -> str:
    raw = str(task_name or "").strip()
    if raw:
        return raw.lstrip("\\")
    match = _TASK_NAME_IN_MESSAGE_RE.search(str(message or ""))
    if not match:
        return ""
    return match.group(0).lstrip("\\")


def open_path(path: Path) -> None:
    target = Path(path).expanduser()
    if not target.exists():
        raise FileNotFoundError(str(target))
    if hasattr(__import__("os"), "startfile"):
        __import__("os").startfile(str(target))  # type: ignore[attr-defined]
        return
    subprocess.run(["xdg-open", str(target)], check=False)


def _run_powershell_json(command: str) -> Any:
    output = _run_powershell_text(command)
    text = output.strip()
    if not text:
        return []
    return json.loads(text)


def _run_powershell_text(command: str) -> str:
    encoded = base64.b64encode(command.encode("utf-16le")).decode("ascii")
    process = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if process.returncode != 0:
        error_text = (process.stderr or process.stdout or "").strip()
        raise RuntimeError(error_text or f"PowerShell exited with code {process.returncode}")
    return process.stdout


def _coerce_rows(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _optional_path(value: object) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    return Path(text)


def _to_int(value: object) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return 0
