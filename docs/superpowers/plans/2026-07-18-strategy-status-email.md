# OKXQQ Strategy Status Email Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four daily all-API active-strategy status emails and a matching manual test-email button to the running OKXQQ desktop application.

**Architecture:** Put schedule selection, persistent slot claiming, and text/HTML rendering in a new pure-Python module. Keep the Tk integration in `UiStrategySessionsMixin`: it adapts existing in-memory session/cache values into immutable rows, queues email through the existing `EmailNotifier`, and owns the lightweight `root.after` scheduler. The settings window adds one button that uses the same content pipeline without touching scheduled-send state.

**Tech Stack:** Python 3.11+, standard-library `dataclasses`, `datetime`, `html`, `json`, `pathlib`, Tkinter, existing `EmailNotifier`, `unittest`-style pytest tests.

## Global Constraints

- Fixed local send times are exactly `08:00`, `12:00`, `16:00`, and `20:00`.
- One email combines every API profile and includes only active sessions; stopped history is excluded.
- The status email always uses the global sender and global recipients, never the currently selected API sender override.
- The email must not make new OKX API requests; it uses the values and caches already available to the session table.
- SMTP work remains asynchronous and must not block strategy execution or Tk.
- A first application check establishes a baseline and does not backfill pre-start slots; wake-up catch-up sends only the most recent crossed slot.
- A scheduled `date+slot` is claimed before queueing and is not retried in the same slot after failure.
- Manual test email uses real current active sessions, has a test-marked subject, and never reads or writes scheduled-send deduplication state.
- Do not add dependencies or a second credential store.
- Preserve all unrelated pre-existing worktree changes; stage only files named by the current task.

---

### Task 1: Schedule Selection and Persistent Slot Claims

**Files:**
- Create: `okx_quant/strategy_status_email.py`
- Create: `tests/test_strategy_status_email.py`

**Interfaces:**
- Produces: `STATUS_EMAIL_TIMES: tuple[datetime.time, ...]`
- Produces: `latest_due_status_email_slot(previous_check: datetime | None, now: datetime) -> datetime | None`
- Produces: `status_email_slot_key(slot: datetime) -> str`
- Produces: `strategy_status_email_state_file_path(base_dir: Path | None = None) -> Path`
- Produces: `load_sent_status_email_slots(path: Path | None = None, logger: Callable[[str], None] | None = None) -> set[str]`
- Produces: `claim_status_email_slot(slot: datetime, *, path: Path | None = None, now: datetime | None = None, logger: Callable[[str], None] | None = None) -> bool`

- [ ] **Step 1: Write failing schedule and persistence tests**

Create `tests/test_strategy_status_email.py` with these initial tests:

```python
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from okx_quant.strategy_status_email import (
    STATUS_EMAIL_TIMES,
    claim_status_email_slot,
    latest_due_status_email_slot,
    load_sent_status_email_slots,
    status_email_slot_key,
    strategy_status_email_state_file_path,
)


class StrategyStatusEmailScheduleTest(TestCase):
    def test_fixed_times_are_0800_1200_1600_2000(self) -> None:
        self.assertEqual(
            tuple(item.strftime("%H:%M") for item in STATUS_EMAIL_TIMES),
            ("08:00", "12:00", "16:00", "20:00"),
        )

    def test_first_check_only_establishes_baseline(self) -> None:
        self.assertIsNone(
            latest_due_status_email_slot(None, datetime(2026, 7, 18, 12, 30))
        )

    def test_crossing_one_slot_returns_that_slot(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 18, 7, 59, 50),
                datetime(2026, 7, 18, 8, 0, 10),
            ),
            datetime(2026, 7, 18, 8, 0),
        )

    def test_crossing_multiple_slots_returns_only_latest(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 18, 7, 30),
                datetime(2026, 7, 18, 16, 5),
            ),
            datetime(2026, 7, 18, 16, 0),
        )

    def test_crossing_midnight_can_return_previous_day_2000(self) -> None:
        self.assertEqual(
            latest_due_status_email_slot(
                datetime(2026, 7, 17, 19, 30),
                datetime(2026, 7, 18, 7, 0),
            ),
            datetime(2026, 7, 17, 20, 0),
        )

    def test_slot_key_is_stable(self) -> None:
        self.assertEqual(
            status_email_slot_key(datetime(2026, 7, 18, 8, 0)),
            "2026-07-18T08:00",
        )

    def test_claim_persists_and_rejects_duplicate_after_reload(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            slot = datetime(2026, 7, 18, 8, 0)
            self.assertTrue(claim_status_email_slot(slot, path=path, now=slot))
            self.assertFalse(claim_status_email_slot(slot, path=path, now=slot))
            self.assertEqual(load_sent_status_email_slots(path), {"2026-07-18T08:00"})

    def test_claim_prunes_keys_older_than_14_days(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            path.write_text(
                json.dumps({"sent_slots": ["2026-06-01T08:00", "2026-07-17T20:00"]}),
                encoding="utf-8",
            )
            claim_status_email_slot(
                datetime(2026, 7, 18, 8, 0),
                path=path,
                now=datetime(2026, 7, 18, 8, 0),
            )
            self.assertEqual(
                load_sent_status_email_slots(path),
                {"2026-07-17T20:00", "2026-07-18T08:00"},
            )

    def test_corrupt_state_logs_and_falls_back_to_empty(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            path.write_text("not-json", encoding="utf-8")
            logs: list[str] = []
            self.assertEqual(load_sent_status_email_slots(path, logger=logs.append), set())
            self.assertEqual(len(logs), 1)
            self.assertIn("读取策略状态邮件记录失败", logs[0])

    def test_default_state_path_uses_expected_name(self) -> None:
        with TemporaryDirectory() as temp_dir:
            self.assertEqual(
                strategy_status_email_state_file_path(Path(temp_dir)).name,
                "strategy_status_email_state.json",
            )
```

- [ ] **Step 2: Run the tests and verify the module is missing**

Run:

```powershell
python -m pytest tests/test_strategy_status_email.py -v
```

Expected: collection fails with `ModuleNotFoundError: No module named 'okx_quant.strategy_status_email'`.

- [ ] **Step 3: Implement the minimal scheduler and state store**

Create `okx_quant/strategy_status_email.py` with:

```python
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
        if (_slot_key_date(item) is not None and _slot_key_date(item) >= oldest_kept_date)
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
```

- [ ] **Step 4: Run the focused tests**

Run:

```powershell
python -m pytest tests/test_strategy_status_email.py -v
```

Expected: `10 passed`.

- [ ] **Step 5: Commit only Task 1 files**

```powershell
git add -- okx_quant/strategy_status_email.py tests/test_strategy_status_email.py
git commit -m "feat: add strategy status email schedule state"
```

---

### Task 2: Pure Text and HTML Status Email Rendering

**Files:**
- Modify: `okx_quant/strategy_status_email.py`
- Modify: `tests/test_strategy_status_email.py`

**Interfaces:**
- Produces: immutable `StrategyStatusEmailRow` with the 16 user-visible status columns.
- Produces: immutable `StrategyStatusEmailContent(subject: str, body: str, html_body: str)`.
- Produces: `build_strategy_status_email(rows: list[StrategyStatusEmailRow], *, scheduled_for: datetime, generated_at: datetime, is_test: bool = False) -> StrategyStatusEmailContent`.

- [ ] **Step 1: Add failing rendering tests**

Append to `tests/test_strategy_status_email.py`:

```python
from okx_quant.strategy_status_email import (
    StrategyStatusEmailRow,
    build_strategy_status_email,
)


def _row(**overrides: str) -> StrategyStatusEmailRow:
    values = {
        "session": "S01",
        "api": "api-b",
        "account_equity": "2734.05",
        "strategy": "EMA 动态委托做多",
        "symbol": "DOGE-USDT-SWAP",
        "direction": "只做多",
        "open_qty": "7140 DOGE",
        "entry_price": "0.0724",
        "stop_price": "0.07184",
        "take_profit": "-",
        "live_pnl": "+0.93",
        "net_pnl": "-0.67",
        "last_net_pnl": "-0.67",
        "status": "持仓监控中",
        "started": "07-13 17:39:52",
        "risk_amount": "4",
    }
    values.update(overrides)
    return StrategyStatusEmailRow(**values)


class StrategyStatusEmailRenderingTest(TestCase):
    def test_formal_email_contains_subject_summary_and_all_columns(self) -> None:
        content = build_strategy_status_email(
            [_row()],
            scheduled_for=datetime(2026, 7, 18, 8, 0),
            generated_at=datetime(2026, 7, 18, 8, 0, 8),
        )
        self.assertEqual(
            content.subject,
            "[OKXQQ] 策略运行状态 | 2026-07-18 08:00 | 活跃 1",
        )
        self.assertIn("涉及 API：1", content.body)
        self.assertIn("api-b：1", content.body)
        self.assertIn("7140 DOGE", content.body)
        self.assertIn("账户总权益", content.html_body)
        self.assertIn("上次净盈亏", content.html_body)
        self.assertIn("风险金", content.html_body)

    def test_rows_are_sorted_by_api_then_session(self) -> None:
        content = build_strategy_status_email(
            [
                _row(session="S02", api="api-b"),
                _row(session="S03", api="api-a"),
                _row(session="S01", api="api-b"),
            ],
            scheduled_for=datetime(2026, 7, 18, 12, 0),
            generated_at=datetime(2026, 7, 18, 12, 0, 3),
        )
        self.assertLess(content.body.index("api-a\tS03"), content.body.index("api-b\tS01"))
        self.assertLess(content.body.index("api-b\tS01"), content.body.index("api-b\tS02"))

    def test_html_escapes_dynamic_values(self) -> None:
        content = build_strategy_status_email(
            [_row(strategy="EMA <script>&")],
            scheduled_for=datetime(2026, 7, 18, 16, 0),
            generated_at=datetime(2026, 7, 18, 16, 0),
        )
        self.assertIn("EMA &lt;script&gt;&amp;", content.html_body)
        self.assertNotIn("EMA <script>", content.html_body)

    def test_empty_email_still_reports_healthy_empty_state(self) -> None:
        content = build_strategy_status_email(
            [],
            scheduled_for=datetime(2026, 7, 18, 20, 0),
            generated_at=datetime(2026, 7, 18, 20, 0, 1),
        )
        self.assertIn("活跃 0", content.subject)
        self.assertIn("当前无活跃策略", content.body)
        self.assertIn("当前无活跃策略", content.html_body)

    def test_manual_test_email_is_marked_and_explains_no_slot_consumption(self) -> None:
        content = build_strategy_status_email(
            [_row()],
            scheduled_for=datetime(2026, 7, 18, 10, 23),
            generated_at=datetime(2026, 7, 18, 10, 23),
            is_test=True,
        )
        self.assertIn("策略运行状态测试", content.subject)
        self.assertIn("手动测试触发，不占用定时发送时段", content.body)
        self.assertIn("手动测试触发，不占用定时发送时段", content.html_body)
```

- [ ] **Step 2: Run only the new rendering tests and verify failure**

Run:

```powershell
python -m pytest tests/test_strategy_status_email.py::StrategyStatusEmailRenderingTest -v
```

Expected: collection fails because `StrategyStatusEmailRow` and `build_strategy_status_email` do not exist.

- [ ] **Step 3: Add immutable row/content types and the renderer**

Add imports to `okx_quant/strategy_status_email.py`:

```python
from collections import Counter
from dataclasses import dataclass
from html import escape
```

Add below the constants:

```python
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
```

- [ ] **Step 4: Run the complete pure-module suite**

Run:

```powershell
python -m pytest tests/test_strategy_status_email.py -v
```

Expected: `15 passed`.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- okx_quant/strategy_status_email.py tests/test_strategy_status_email.py
git commit -m "feat: render strategy status emails"
```

---

### Task 3: Adapt All Active UI Sessions and Force Global Mail Identity

**Files:**
- Modify: `okx_quant/ui_strategy_sessions.py:1-220`
- Modify: `okx_quant/ui_strategy_sessions.py:5907-5957`
- Modify: `okx_quant/ui_strategy_sessions.py:7140-7197`
- Modify: `tests/test_ui.py:8897-8923`

**Interfaces:**
- Consumes: `StrategyStatusEmailRow` from Task 2.
- Produces: `UiStrategySessionsMixin._strategy_status_email_rows() -> list[StrategyStatusEmailRow]`.
- Extends: `_collect_notification_config(..., use_global_sender: bool = False) -> EmailNotificationConfig`.
- Produces: `_build_strategy_status_email_notifier() -> EmailNotifier | None`.

- [ ] **Step 1: Add failing adapter/config tests to `tests/test_ui.py`**

Add `from okx_quant.strategy_status_email import StrategyStatusEmailRow` to the imports, then add:

```python
    def test_collect_notification_config_can_force_global_sender(self) -> None:
        app = SimpleNamespace(
            smtp_port=_Var("465"),
            recipient_emails=_Var("ops@example.com"),
            notify_enabled=_Var(True),
            smtp_host=_Var("smtp.example.com"),
            smtp_username=_Var("smtp-user"),
            smtp_password=_Var("secret"),
            sender_email=_Var("global@example.com"),
            use_ssl=_Var(True),
            notify_trade_fills=_Var(True),
            notify_signals=_Var(True),
            notify_errors=_Var(True),
            _api_sender_email_overrides={"api2": "api2@example.com"},
        )
        app._parse_optional_port = lambda raw: QuantApp._parse_optional_port(app, raw)
        app._split_recipients = lambda raw: QuantApp._split_recipients(app, raw)
        app._normalized_api_sender_email_overrides = lambda: QuantApp._normalized_api_sender_email_overrides(app)
        app._resolved_api_sender_email_override = lambda profile_name=None: QuantApp._resolved_api_sender_email_override(
            app, profile_name
        )

        config = QuantApp._collect_notification_config(
            app,
            validate_if_enabled=True,
            api_profile_name="api2",
            use_global_sender=True,
        )

        self.assertEqual(config.sender_email, "global@example.com")

    def test_strategy_status_email_rows_include_all_api_active_sessions_only(self) -> None:
        def session(session_id: str, api_name: str, status: str, running: bool) -> SimpleNamespace:
            return SimpleNamespace(
                session_id=session_id,
                api_name=api_name,
                strategy_id="ema_dynamic_long",
                strategy_name="EMA 动态委托做多",
                symbol="ETH-USDT-SWAP",
                direction_label="只做多",
                display_status="等待信号" if running else status,
                status=status,
                engine=SimpleNamespace(is_running=running),
                stop_cleanup_in_progress=False,
                config=SimpleNamespace(risk_amount=Decimal("12")),
                net_pnl_total=Decimal("-1.2"),
                last_net_pnl=Decimal("0.5"),
                started_at=datetime(2026, 7, 18, 7, 0),
            )

        active_a = session("S02", "api-a", "运行中", True)
        active_b = session("S01", "api-b", "待恢复", False)
        stopped = session("S03", "api-c", "已停止", False)
        app = SimpleNamespace(sessions={item.session_id: item for item in (active_a, active_b, stopped)})
        app._session_live_pnl_snapshot = lambda item: (Decimal("1.25") if item.session_id == "S02" else None, None)
        app._session_open_position_amount_text = lambda item: "2 ETH" if item.session_id == "S02" else "-"
        app._session_account_total_equity_text = lambda item: "1000.00" if item.api_name == "api-a" else "500.00"
        app._session_runtime_entry_price_text = lambda item: "3000" if item.session_id == "S02" else "-"
        app._session_runtime_stop_price_text = lambda item: "2900" if item.session_id == "S02" else "-"
        app._session_runtime_take_profit_text = lambda item: "3300" if item.session_id == "S02" else "-"
        app._session_recovery_reason_summary = lambda item: "等待接管" if item.status == "待恢复" else ""
        app._format_session_started_at = lambda value: value.strftime("%m-%d %H:%M:%S")

        rows = QuantApp._strategy_status_email_rows(app)

        self.assertEqual([(row.api, row.session) for row in rows], [("api-a", "S02"), ("api-b", "S01")])
        self.assertEqual(rows[0].account_equity, "1000.00")
        self.assertEqual(rows[0].open_qty, "2 ETH")
        self.assertEqual(rows[1].status, "待恢复:等待接管")
```

- [ ] **Step 2: Run the two tests and verify the missing keyword/method failures**

Run:

```powershell
python -m pytest tests/test_ui.py -k "force_global_sender or strategy_status_email_rows" -v
```

Expected: failures for unexpected `use_global_sender` and missing `_strategy_status_email_rows`.

- [ ] **Step 3: Extend config collection without changing existing callers**

In `okx_quant/ui_strategy_sessions.py`, import:

```python
from okx_quant.strategy_status_email import StrategyStatusEmailRow
```

Change the signature and sender selection inside `_collect_notification_config` to:

```python
    def _collect_notification_config(
        self,
        *,
        validate_if_enabled: bool,
        api_profile_name: str | None = None,
        use_global_sender: bool = False,
    ) -> EmailNotificationConfig:
        smtp_port = self._parse_optional_port(self.smtp_port.get())
        recipients = tuple(self._split_recipients(self.recipient_emails.get()))
        sender_email = self.sender_email.get().strip()
        if not use_global_sender:
            sender_email = self._resolved_api_sender_email_override(api_profile_name) or sender_email
        config = EmailNotificationConfig(
            enabled=self.notify_enabled.get(),
            smtp_host=self.smtp_host.get().strip(),
            smtp_port=smtp_port,
            smtp_username=self.smtp_username.get().strip(),
            smtp_password=self.smtp_password.get(),
            sender_email=sender_email,
            recipient_emails=recipients,
            use_ssl=self.use_ssl.get(),
            notify_trade_fills=self.notify_trade_fills.get(),
            notify_signals=self.notify_signals.get(),
            notify_errors=self.notify_errors.get(),
        )
        if validate_if_enabled and config.enabled:
            if not config.smtp_host:
                raise ValueError("已启用邮件通知，请填写 SMTP 主机")
            if not recipients:
                raise ValueError("已启用邮件通知，请填写至少一个收件邮箱")
            if not (config.sender_email or config.smtp_username):
                raise ValueError("已启用邮件通知，请填写发件邮箱或 SMTP 用户名")
        return config
```

Add after `_build_signal_monitor_notifier`:

```python
    def _build_strategy_status_email_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(
            validate_if_enabled=True,
            use_global_sender=True,
        )
        if not notification_config.enabled:
            return None
        return EmailNotifier(
            notification_config,
            logger=self._make_system_logger("邮件 策略运行状态"),
        )
```

- [ ] **Step 4: Add the UI-to-row adapter using existing table helpers**

Add near `_upsert_session_row` in `okx_quant/ui_strategy_sessions.py`:

```python
    def _strategy_status_email_rows(self) -> list[StrategyStatusEmailRow]:
        rows: list[StrategyStatusEmailRow] = []
        for session in self.sessions.values():
            if not QuantApp._session_counts_toward_running_summary(session):
                continue
            live_pnl, _refreshed_at = self._session_live_pnl_snapshot(session)
            risk_amount = UiStrategySessionsMixin._format_optional_positive_entry_decimal(
                getattr(getattr(session, "config", None), "risk_amount", None)
            ) or "-"
            recovery_summary = self._session_recovery_reason_summary(session)
            status_text = (
                f"{session.status}:{recovery_summary}"
                if session.status in {"待恢复", "恢复中"} and recovery_summary
                else session.display_status
            )
            rows.append(
                StrategyStatusEmailRow(
                    session=session.session_id or "-",
                    api=session.api_name or "-",
                    account_equity=self._session_account_total_equity_text(session),
                    strategy=session.strategy_name or "-",
                    symbol=session.symbol or "-",
                    direction=_normalize_strategy_direction_label(
                        getattr(session, "strategy_id", getattr(getattr(session, "config", None), "strategy_id", "")),
                        getattr(session, "config", None),
                        fallback=session.direction_label,
                    ),
                    open_qty=self._session_open_position_amount_text(session),
                    entry_price=self._session_runtime_entry_price_text(session),
                    stop_price=self._session_runtime_stop_price_text(session),
                    take_profit=self._session_runtime_take_profit_text(session),
                    live_pnl=_format_optional_usdt_precise(live_pnl, places=2),
                    net_pnl=_format_optional_usdt_precise(session.net_pnl_total, places=2),
                    last_net_pnl=_format_optional_usdt_precise(session.last_net_pnl, places=2),
                    status=status_text or "-",
                    started=self._format_session_started_at(session.started_at),
                    risk_amount=risk_amount,
                )
            )
        rows.sort(key=lambda row: (row.api.casefold(), row.session))
        return rows
```

- [ ] **Step 5: Run adapter tests and existing notification tests**

Run:

```powershell
python -m pytest tests/test_ui.py -k "notification_config or strategy_status_email_rows" -v
```

Expected: all selected tests pass, including the existing API-specific override test.

- [ ] **Step 6: Commit Task 3**

```powershell
git add -- okx_quant/ui_strategy_sessions.py tests/test_ui.py
git commit -m "feat: collect all-api strategy status rows"
```

---

### Task 4: Manual Strategy Status Test Email Button

**Files:**
- Modify: `okx_quant/ui_shell.py:7464-7469`
- Modify: `okx_quant/ui_strategy_sessions.py:5959-6005`
- Modify: `tests/test_ui.py`

**Interfaces:**
- Consumes: `build_strategy_status_email` and `_strategy_status_email_rows()`.
- Produces: `_queue_strategy_status_email(scheduled_for: datetime, generated_at: datetime, is_test: bool) -> bool`.
- Produces: `send_strategy_status_test_email() -> None` as the Tk button callback.

- [ ] **Step 1: Add failing manual-send tests**

Add `import inspect` to `tests/test_ui.py`, import `StrategyStatusEmailContent`, then add:

```python
    def test_queue_strategy_status_email_uses_raw_html_notifier(self) -> None:
        notifier = SimpleNamespace(notify_async=MagicMock())
        rows = [MagicMock(spec=StrategyStatusEmailRow)]
        content = StrategyStatusEmailContent("subject", "body", "<b>html</b>")
        app = SimpleNamespace(
            _build_strategy_status_email_notifier=lambda: notifier,
            _strategy_status_email_rows=lambda: rows,
            _enqueue_log=MagicMock(),
        )
        with patch("okx_quant.ui_strategy_sessions.build_strategy_status_email", return_value=content) as build:
            queued = QuantApp._queue_strategy_status_email(
                app,
                scheduled_for=datetime(2026, 7, 18, 10, 23),
                generated_at=datetime(2026, 7, 18, 10, 23),
                is_test=True,
            )

        self.assertTrue(queued)
        build.assert_called_once()
        notifier.notify_async.assert_called_once_with("subject", "body", html_body="<b>html</b>")

    def test_manual_strategy_status_test_email_does_not_claim_schedule_slot(self) -> None:
        app = SimpleNamespace(
            _settings_window=None,
            root=object(),
            _queue_strategy_status_email=MagicMock(return_value=True),
        )
        with patch("okx_quant.strategy_status_email.claim_status_email_slot") as claim, patch(
            "okx_quant.ui_strategy_sessions.messagebox.showinfo"
        ) as showinfo:
            QuantApp.send_strategy_status_test_email(app)

        claim.assert_not_called()
        app._queue_strategy_status_email.assert_called_once()
        self.assertTrue(app._queue_strategy_status_email.call_args.kwargs["is_test"])
        showinfo.assert_called_once()

    def test_settings_window_wires_strategy_status_test_button(self) -> None:
        source = inspect.getsource(QuantApp.open_settings_window)
        self.assertIn('text="发送策略状态测试邮件"', source)
        self.assertIn("command=self.send_strategy_status_test_email", source)
```

- [ ] **Step 2: Run the manual-email tests and verify failures**

Run:

```powershell
python -m pytest tests/test_ui.py -k "strategy_status_test or queue_strategy_status" -v
```

Expected: failures for missing callback/helper/button wiring.

- [ ] **Step 3: Implement shared queueing and manual callback**

Expand the strategy-status import in `okx_quant/ui_strategy_sessions.py`:

```python
from okx_quant.strategy_status_email import (
    StrategyStatusEmailRow,
    build_strategy_status_email,
)
```

Add after `_build_strategy_status_email_notifier`:

```python
    def _queue_strategy_status_email(
        self,
        *,
        scheduled_for: datetime,
        generated_at: datetime,
        is_test: bool,
    ) -> bool:
        notifier = self._build_strategy_status_email_notifier()
        if notifier is None:
            self._enqueue_log("策略状态邮件已跳过：当前未启用邮件通知。")
            return False
        content = build_strategy_status_email(
            self._strategy_status_email_rows(),
            scheduled_for=scheduled_for,
            generated_at=generated_at,
            is_test=is_test,
        )
        notifier.notify_async(content.subject, content.body, html_body=content.html_body)
        kind = "测试" if is_test else "定时"
        self._enqueue_log(f"已提交策略状态{kind}邮件发送请求：{content.subject}")
        return True

    def send_strategy_status_test_email(self) -> None:
        now = datetime.now()
        try:
            queued = self._queue_strategy_status_email(
                scheduled_for=now,
                generated_at=now,
                is_test=True,
            )
        except Exception as exc:
            messagebox.showerror(
                "策略状态测试邮件失败",
                str(exc),
                parent=self._settings_window or self.root,
            )
            return
        if not queued:
            messagebox.showinfo(
                "提示",
                "当前未启用邮件通知。",
                parent=self._settings_window or self.root,
            )
            return
        messagebox.showinfo(
            "提示",
            "策略状态测试邮件已提交，请检查收件箱。",
            parent=self._settings_window or self.root,
        )
```

- [ ] **Step 4: Wire the new button beside the existing generic test**

Replace the two footer button lines in `okx_quant/ui_shell.py` with:

```python
        ttk.Button(footer, text="发送测试邮件", command=self.send_test_email).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(
            footer,
            text="发送策略状态测试邮件",
            command=self.send_strategy_status_test_email,
        ).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(footer, text="关闭", command=self._close_settings_window).grid(row=0, column=2)
```

- [ ] **Step 5: Run manual-send and notification regression tests**

Run:

```powershell
python -m pytest tests/test_ui.py -k "strategy_status_test or queue_strategy_status or notification_config" -v
python -m pytest tests/test_notifications.py -v
```

Expected: all selected UI tests and all notification tests pass.

- [ ] **Step 6: Commit Task 4**

```powershell
git add -- okx_quant/ui_shell.py okx_quant/ui_strategy_sessions.py tests/test_ui.py
git commit -m "feat: add strategy status test email button"
```

---

### Task 5: In-App Daily Scheduler and Shutdown Safety

**Files:**
- Modify: `okx_quant/ui_shell.py:3369-4040`
- Modify: `okx_quant/ui_shell.py:14087-14115`
- Modify: `okx_quant/ui_strategy_sessions.py`
- Modify: `tests/test_ui.py`

**Interfaces:**
- Consumes: `latest_due_status_email_slot` and `claim_status_email_slot` from Task 1.
- Produces: `_start_strategy_status_email_scheduler() -> None`.
- Produces: `_run_strategy_status_email_tick(now: datetime | None = None) -> None`.
- Produces: `_stop_strategy_status_email_scheduler() -> None`.

- [ ] **Step 1: Add failing scheduler integration tests**

Extend the strategy-status imports in `tests/test_ui.py` with `status_email_slot_key`, then add:

```python
    def test_status_email_tick_claims_and_queues_crossed_slot(self) -> None:
        root = SimpleNamespace(after=MagicMock(return_value="next-job"))
        app = SimpleNamespace(
            root=root,
            _strategy_status_email_last_check_at=datetime(2026, 7, 18, 7, 59, 50),
            _strategy_status_email_closing=False,
            _strategy_status_email_job="old-job",
            _queue_strategy_status_email=MagicMock(return_value=True),
            _enqueue_log=MagicMock(),
        )
        app._run_strategy_status_email_tick = lambda: QuantApp._run_strategy_status_email_tick(app)
        with patch("okx_quant.ui_strategy_sessions.claim_status_email_slot", return_value=True) as claim:
            QuantApp._run_strategy_status_email_tick(app, now=datetime(2026, 7, 18, 8, 0, 10))

        claim.assert_called_once()
        self.assertEqual(status_email_slot_key(claim.call_args.args[0]), "2026-07-18T08:00")
        app._queue_strategy_status_email.assert_called_once_with(
            scheduled_for=datetime(2026, 7, 18, 8, 0),
            generated_at=datetime(2026, 7, 18, 8, 0, 10),
            is_test=False,
        )
        root.after.assert_called_once_with(30_000, app._run_strategy_status_email_tick)
        self.assertEqual(app._strategy_status_email_job, "next-job")

    def test_status_email_tick_does_not_queue_duplicate_claim(self) -> None:
        app = SimpleNamespace(
            root=SimpleNamespace(after=MagicMock(return_value="next-job")),
            _strategy_status_email_last_check_at=datetime(2026, 7, 18, 11, 59, 50),
            _strategy_status_email_closing=False,
            _strategy_status_email_job=None,
            _queue_strategy_status_email=MagicMock(),
            _enqueue_log=MagicMock(),
        )
        app._run_strategy_status_email_tick = lambda: QuantApp._run_strategy_status_email_tick(app)
        with patch("okx_quant.ui_strategy_sessions.claim_status_email_slot", return_value=False):
            QuantApp._run_strategy_status_email_tick(app, now=datetime(2026, 7, 18, 12, 0, 5))
        app._queue_strategy_status_email.assert_not_called()

    def test_status_email_tick_logs_state_write_failure_and_reschedules(self) -> None:
        root = SimpleNamespace(after=MagicMock(return_value="next-job"))
        app = SimpleNamespace(
            root=root,
            _strategy_status_email_last_check_at=datetime(2026, 7, 18, 15, 59, 50),
            _strategy_status_email_closing=False,
            _strategy_status_email_job=None,
            _queue_strategy_status_email=MagicMock(),
            _enqueue_log=MagicMock(),
        )
        app._run_strategy_status_email_tick = lambda: QuantApp._run_strategy_status_email_tick(app)
        with patch(
            "okx_quant.ui_strategy_sessions.claim_status_email_slot",
            side_effect=PermissionError("denied"),
        ):
            QuantApp._run_strategy_status_email_tick(app, now=datetime(2026, 7, 18, 16, 0, 5))
        app._queue_strategy_status_email.assert_not_called()
        self.assertIn("策略状态定时邮件触发失败", app._enqueue_log.call_args.args[0])
        root.after.assert_called_once()

    def test_start_scheduler_sets_baseline_without_backfill(self) -> None:
        root = SimpleNamespace(after=MagicMock(return_value="job-1"))
        app = SimpleNamespace(root=root)
        app._run_strategy_status_email_tick = lambda: QuantApp._run_strategy_status_email_tick(app)
        with patch("okx_quant.ui_strategy_sessions.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(2026, 7, 18, 12, 30)
            QuantApp._start_strategy_status_email_scheduler(app)
        self.assertEqual(app._strategy_status_email_last_check_at, datetime(2026, 7, 18, 12, 30))
        self.assertFalse(app._strategy_status_email_closing)
        root.after.assert_called_once_with(30_000, app._run_strategy_status_email_tick)

    def test_stop_scheduler_cancels_pending_job(self) -> None:
        root = SimpleNamespace(after_cancel=MagicMock())
        app = SimpleNamespace(root=root, _strategy_status_email_job="job-1")
        QuantApp._stop_strategy_status_email_scheduler(app)
        self.assertTrue(app._strategy_status_email_closing)
        self.assertIsNone(app._strategy_status_email_job)
        root.after_cancel.assert_called_once_with("job-1")
```

- [ ] **Step 2: Run scheduler tests and verify missing methods**

Run:

```powershell
python -m pytest tests/test_ui.py -k "status_email_tick or start_scheduler or stop_scheduler" -v
```

Expected: failures for the three missing scheduler methods.

- [ ] **Step 3: Implement the UI-thread scheduler in the mixin**

Expand the import in `okx_quant/ui_strategy_sessions.py`:

```python
from okx_quant.strategy_status_email import (
    StrategyStatusEmailRow,
    build_strategy_status_email,
    claim_status_email_slot,
    latest_due_status_email_slot,
)
```

Add a module constant:

```python
_STRATEGY_STATUS_EMAIL_CHECK_INTERVAL_MS = 30_000
```

Add these methods after `send_strategy_status_test_email`:

```python
    def _start_strategy_status_email_scheduler(self) -> None:
        self._strategy_status_email_last_check_at = datetime.now()
        self._strategy_status_email_closing = False
        self._strategy_status_email_job = self.root.after(
            _STRATEGY_STATUS_EMAIL_CHECK_INTERVAL_MS,
            self._run_strategy_status_email_tick,
        )

    def _run_strategy_status_email_tick(self, now: datetime | None = None) -> None:
        current = now or datetime.now()
        previous = getattr(self, "_strategy_status_email_last_check_at", None)
        self._strategy_status_email_last_check_at = current
        try:
            due_slot = latest_due_status_email_slot(previous, current)
            if due_slot is not None and claim_status_email_slot(
                due_slot,
                now=current,
                logger=self._enqueue_log,
            ):
                self._queue_strategy_status_email(
                    scheduled_for=due_slot,
                    generated_at=current,
                    is_test=False,
                )
        except Exception as exc:
            self._enqueue_log(f"策略状态定时邮件触发失败：{exc}")
        finally:
            self._strategy_status_email_job = None
            if not getattr(self, "_strategy_status_email_closing", False):
                self._strategy_status_email_job = self.root.after(
                    _STRATEGY_STATUS_EMAIL_CHECK_INTERVAL_MS,
                    self._run_strategy_status_email_tick,
                )

    def _stop_strategy_status_email_scheduler(self) -> None:
        self._strategy_status_email_closing = True
        job = getattr(self, "_strategy_status_email_job", None)
        self._strategy_status_email_job = None
        if job is None:
            return
        try:
            self.root.after_cancel(job)
        except Exception:
            pass
```

- [ ] **Step 4: Start and stop the scheduler from `QuantApp` lifecycle**

In `QuantApp.__init__`, immediately after the existing periodic `root.after` registrations and before `root.protocol`, add:

```python
        self._start_strategy_status_email_scheduler()
```

In `_on_close`, after the user has confirmed closing and before saving or stopping sessions, add:

```python
        self._stop_strategy_status_email_scheduler()
```

This placement is important: canceling a close dialog must leave the scheduler running, while an accepted close must prevent the timer callback from scheduling itself again.

- [ ] **Step 5: Run focused scheduler and mail suites**

Run:

```powershell
python -m pytest tests/test_strategy_status_email.py tests/test_notifications.py -v
python -m pytest tests/test_ui.py -k "strategy_status or notification_config" -v
```

Expected: all focused tests pass.

- [ ] **Step 6: Run syntax and full regression verification**

Run:

```powershell
python -m py_compile okx_quant/strategy_status_email.py okx_quant/ui_strategy_sessions.py okx_quant/ui_shell.py
python -m pytest -q
git diff --check
```

Expected: compilation succeeds, the complete pytest suite passes, and `git diff --check` prints no errors. If unrelated pre-existing tests fail, record the exact failing test and verify that every new/focused test still passes before deciding whether the failure is in scope.

- [ ] **Step 7: Commit Task 5 implementation**

```powershell
git add -- okx_quant/strategy_status_email.py okx_quant/ui_strategy_sessions.py okx_quant/ui_shell.py tests/test_strategy_status_email.py tests/test_ui.py
git commit -m "feat: schedule all-api strategy status emails"
```

- [ ] **Step 8: Perform a manual OKXQQ smoke test**

Run OKXQQ with the normal project launcher, then verify:

```powershell
python main.py
```

Expected manual observations:

1. “设置 → API 与通知设置” still shows the original “发送测试邮件”.
2. The new “发送策略状态测试邮件” button is beside it.
3. Clicking the new button reports that the request was submitted.
4. The received subject contains “策略运行状态测试”.
5. The received HTML table includes all currently active APIs and excludes stopped sessions.
6. The body states that the test does not consume a scheduled slot.
7. Strategy monitoring and the main window remain responsive during SMTP delivery.

Do not use the manual test to modify the four scheduled slot records.

---

## Final Verification Checklist

- [ ] `git status --short` contains only intended task files plus preserved unrelated pre-existing changes.
- [ ] `python -m pytest tests/test_strategy_status_email.py tests/test_notifications.py -v` passes.
- [ ] `python -m pytest tests/test_ui.py -k "strategy_status or notification_config" -v` passes.
- [ ] `python -m py_compile okx_quant/strategy_status_email.py okx_quant/ui_strategy_sessions.py okx_quant/ui_shell.py` passes.
- [ ] `python -m pytest -q` passes or any unrelated baseline failure is recorded with exact evidence.
- [ ] `git diff --check` reports no whitespace errors.
- [ ] Manual strategy status test email renders correctly in the user's mail client.
- [ ] No SMTP secret or API credential appears in logs, tests, commits, or email content.
