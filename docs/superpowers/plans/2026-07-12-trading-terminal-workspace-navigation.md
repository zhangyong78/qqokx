# Trading Terminal Workspace Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a single-window Qt trading workspace that opens on K line by default, moves API selection to the global header, keeps running tasks bound to their creation API, and gives every existing feature one clear entry.

**Architecture:** Add a small shell layer around the existing persistent page stack. `WorkspaceHeader` owns navigation presentation, while `LauncherWindow` remains the coordinator for page creation and global profile changes. Existing account, K-line, and arbitrage pages expose narrow workspace-profile interfaces; execution objects and monitored tasks capture their API profile at creation and never read the later global selection.

**Tech Stack:** Python 3.11+, PySide6/Qt Widgets, existing OKX REST/WebSocket clients, pytest, `tests.qt_test_case.QtWidgetTestCase`.

## Global Constraints

- The default workspace is `K线`; if no saved symbol exists use `BTC-USDT-SWAP`, and if no saved period exists use `4H`.
- The chart is visible by default; EMA15 and SMA50 are enabled; 1H/4H/1D pattern signals and MA-touch filtering are disabled.
- K line, positions, and professional arbitrage stay in one main window and retain page state when switched.
- The global API controls current viewing and new operations only; running RR, line-condition, and arbitrage tasks remain bound to their creation API.
- Public K line remains usable when an API is locked or unavailable; trading controls are disabled with an explicit reason.
- Do not change Tk/server strategy, backtest, shared-data-directory, RR calculation, condition calculation, or arbitrage calculation semantics.
- Preserve unrelated dirty-worktree changes. Stage and commit only files listed in the current task.
- Existing unrelated repository failures must not be “fixed” by changing stable trading logic: the three named Qt window failures and `test_scan_can_filter_only_futures` remain outside scope unless their behavior is directly changed by this plan.

---

## File Structure

- Create `roll_terminal_qt/workspace_shell.py`: global header, page keys, task-count value type, and pure summary helpers.
- Modify `roll_terminal_qt/launcher.py`: assemble header + page stack, lazy page creation, global profile coordination, menu routing, shutdown.
- Modify `roll_terminal_qt/kline_analysis_window.py`: contextual K-line toolbar, default pattern state, external profile interface, multi-profile RR/line-task monitoring.
- Modify `roll_terminal_qt/account_positions_home.py`: remove duplicated profile selector and accept global profile changes through the existing asynchronous feed restart path.
- Modify `roll_terminal_qt/ui.py`: remove duplicated profile selector, split current viewing runtime from auto-task runtime, accept global profile changes.
- Modify `roll_terminal_qt/module_overview.py`: expose the final unique route labels used by the shell.
- Create `tests/test_workspace_shell_qt.py`: isolated shell/header/task-summary tests.
- Modify `tests/test_roll_terminal_qt_windows.py`: launcher, K-line defaults, task binding, and professional-arbitrage tests.
- Modify `tests/test_account_positions_home_qt.py`: external profile switching and manual-action capture tests.
- Modify `README.md`, `软件开发指南.md`, and `docs/kline_analysis_m1_acceptance.md`: document the final user workflow and safety boundary.

---

### Task 1: Global Workspace Header and Task Summary Types

**Files:**
- Create: `roll_terminal_qt/workspace_shell.py`
- Create: `tests/test_workspace_shell_qt.py`

**Interfaces:**
- Produces: `WorkspacePageKey = Literal["kline", "account", "roll", "smart-order"]`
- Produces: `LocalTaskCount(profile_name: str, rr: int = 0, line_conditions: int = 0, arbitrage: int = 0)`
- Produces: `merge_local_task_counts(items: Iterable[LocalTaskCount]) -> tuple[LocalTaskCount, ...]`
- Produces: `format_local_task_counts(items: Iterable[LocalTaskCount]) -> str`
- Produces: `WorkspaceHeader(QWidget)` with signals `page_requested(str)`, `profile_requested(str)`, and `tool_requested(str)`.
- Produces: `WorkspaceHeader.set_active_page(page_key: str)`, `set_profiles(names: Sequence[str], selected: str, environment: str)`, `restore_profile(profile_name: str)`, `set_connection_text(text: str, healthy: bool)`, and `set_task_text(text: str)`.
- Produces: `WorkspaceHeader.action(route_key: str) -> QAction` and `route_keys() -> tuple[str, ...]` for deterministic routing tests and keyboard shortcuts.

- [ ] **Step 1: Write the failing pure-helper tests**

```python
def test_merge_local_task_counts_groups_by_profile() -> None:
    merged = merge_local_task_counts([
        LocalTaskCount("moni", rr=2),
        LocalTaskCount("api2", line_conditions=1),
        LocalTaskCount("moni", arbitrage=1),
    ])
    assert merged == (
        LocalTaskCount("api2", line_conditions=1),
        LocalTaskCount("moni", rr=2, arbitrage=1),
    )
    assert format_local_task_counts(merged) == "api2：条件单 1｜moni：RR 2 / 套利 1"
```

- [ ] **Step 2: Write the failing widget tests**

```python
def test_workspace_header_emits_page_and_profile_requests(self) -> None:
    header = WorkspaceHeader()
    pages: list[str] = []
    profiles: list[str] = []
    header.page_requested.connect(pages.append)
    header.profile_requested.connect(profiles.append)
    header.set_profiles(["moni", "api2"], "moni", "demo")
    header.page_button("account").click()
    header.profile_combo.setCurrentText("api2")
    self.assertEqual(pages, ["account"])
    self.assertEqual(profiles, ["api2"])
```

- [ ] **Step 3: Run the tests and verify the red state**

Run: `python -m pytest tests/test_workspace_shell_qt.py -q`

Expected: FAIL because `roll_terminal_qt.workspace_shell` does not exist.

- [ ] **Step 4: Implement the value type, helpers, and header**

```python
WorkspacePageKey = Literal["kline", "account", "roll", "smart-order"]

@dataclass(frozen=True)
class LocalTaskCount:
    profile_name: str
    rr: int = 0
    line_conditions: int = 0
    arbitrage: int = 0

class WorkspaceHeader(QFrame):
    page_requested = Signal(str)
    profile_requested = Signal(str)
    tool_requested = Signal(str)

    def set_profiles(self, names: Sequence[str], selected: str, environment: str) -> None:
        with QSignalBlocker(self.profile_combo):
            self.profile_combo.clear()
            self.profile_combo.addItems(list(names) or ["未配置"])
            index = self.profile_combo.findText(selected)
            self.profile_combo.setCurrentIndex(index if index >= 0 else 0)
        self.environment_label.setText("模拟" if environment == "demo" else "实盘")
```

Build `QToolButton` page buttons for `K线`, `持仓`, and `专业套利`; `InstantPopup` menus for `交易工具` and `期权工具`; a right-side profile combo, connection label, task button, and settings button. Keep style rules local to the header object name so existing page CSS is not changed.

- [ ] **Step 5: Run the focused tests**

Run: `python -m pytest tests/test_workspace_shell_qt.py -q`

Expected: all tests in the file PASS.

- [ ] **Step 6: Commit the isolated shell**

```powershell
git add roll_terminal_qt/workspace_shell.py tests/test_workspace_shell_qt.py
git commit -m "feat(qt): add global trading workspace header"
```

---

### Task 2: Launcher Opens K Line and Routes Every Unique Entry

**Files:**
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/module_overview.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: `WorkspaceHeader` and `WorkspacePageKey` from Task 1.
- Produces: `LauncherWindow.show_page(page_key: str) -> None` for all persistent workspace keys.
- Produces: `LauncherWindow.current_page_key() -> str`.
- Produces: `LauncherWindow._create_page(page_key: str) -> QWidget` as the only page factory used by `show_page`.

- [ ] **Step 1: Write failing launcher tests**

```python
def test_launcher_opens_kline_as_default_without_constructing_account(self) -> None:
    class KlineStub(QWidget):
        def __init__(self, *, embedded: bool = False) -> None:
            super().__init__()
            self.embedded = embedded

    class AccountStub(QWidget):
        def begin_shutdown(self, callback) -> None:  # noqa: ANN001
            callback()

    with (
        patch("roll_terminal_qt.launcher.KlineAnalysisWindow", side_effect=KlineStub),
        patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", side_effect=AccountStub) as account_cls,
    ):
        launcher = LauncherWindow()
        self.assertEqual(launcher.current_page_key(), "kline")
        self.assertIsInstance(launcher._page_stack.currentWidget(), KlineStub)
        account_cls.assert_not_called()

def test_launcher_routes_primary_pages_without_child_windows(self) -> None:
    launcher.show_page("account")
    launcher.show_page("roll")
    launcher.show_page("kline")
    self.assertEqual(launcher._child_windows, [])
    self.assertEqual(set(launcher._pages), {"kline", "account", "roll"})

def test_workspace_header_exposes_every_unique_route(self) -> None:
    expected = {
        "page:kline", "page:account", "page:roll",
        "tool:smart-order",
        "option:option-strategy", "option:deribit-volatility",
        "settings:paths", "settings:logs", "settings:version",
    }
    self.assertEqual(set(launcher._workspace_header.route_keys()), expected)
```

- [ ] **Step 2: Run focused tests and verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "launcher_opens_kline or launcher_routes_primary or workspace_tool" -q`

Expected: FAIL because the launcher still constructs the account page first and has no `WorkspaceHeader` routing.

- [ ] **Step 3: Assemble the shell and page stack**

```python
self._workspace_root = QWidget(self)
layout = QVBoxLayout(self._workspace_root)
layout.setContentsMargins(0, 0, 0, 0)
layout.setSpacing(0)
self._workspace_header = WorkspaceHeader(self._workspace_root)
self._page_stack = QStackedWidget(self._workspace_root)
layout.addWidget(self._workspace_header)
layout.addWidget(self._page_stack, 1)
self.setCentralWidget(self._workspace_root)
self._pages = {}
self._active_page_key = ""
self.show_page("kline")
```

Route `kline`, `account`, `roll`, and `smart-order` through `_create_page`. Embed persistent trading tools by setting `Qt.WindowType.Widget` before adding them to the stack. Keep the option calculator and Deribit volatility as simple auxiliary windows through the existing child-window lifecycle. Do not expose line trading or auto channel as independent workspace pages; auto channel is reserved for later K-line integration.

- [ ] **Step 4: Remove duplicate core menu routes**

Replace the visible `模块导航`, `K线分析`, `期权`, and `系统` menu-bar structure with header actions. Keep `create_module_window` for auxiliary tools, but make `launcher_module_specs()` return unique route metadata and remove the global line-trading route because line/RR tools live inside K line.

- [ ] **Step 5: Make refresh and shutdown page-agnostic**

```python
def refresh_current_page(self) -> None:
    page = self._page_stack.currentWidget()
    refresh = getattr(page, "refresh_view", None) or getattr(page, "_load_data", None)
    if callable(refresh):
        refresh()
```

Iterate all constructed pages during shutdown. Do not assume the account page exists. Preserve the existing local-task close warning.

- [ ] **Step 6: Run launcher regression tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "launcher or close_warns_when_embedded" -q`

Expected: selected launcher tests PASS.

- [ ] **Step 7: Commit launcher routing**

```powershell
git add roll_terminal_qt/launcher.py roll_terminal_qt/module_overview.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat(qt): make kline the default workspace"
```

---

### Task 3: Global API Selection Coordinator

**Files:**
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/workspace_shell.py`
- Test: `tests/test_workspace_shell_qt.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `LauncherWindow.active_profile_name() -> str`.
- Produces: `LauncherWindow._request_workspace_profile(profile_name: str) -> None`.
- Page contract: optional `apply_workspace_profile(profile_name: str) -> None`.
- Page contract: optional `workspace_profile_name() -> str` for assertions and status only.

- [ ] **Step 1: Write failing selection tests**

```python
def test_global_profile_change_unlocks_once_and_updates_loaded_pages(self) -> None:
    launcher.show_page("account")
    launcher.show_page("roll")
    with patch("roll_terminal_qt.launcher.ensure_profile_unlocked", return_value=True) as unlock:
        launcher._request_workspace_profile("api2")
    unlock.assert_called_once()
    for page in launcher._pages.values():
        page.apply_workspace_profile.assert_called_once_with("api2")
    self.assertEqual(launcher.active_profile_name(), "api2")

def test_rejected_global_profile_change_restores_header(self) -> None:
    with patch("roll_terminal_qt.launcher.ensure_profile_unlocked", return_value=False):
        launcher._request_workspace_profile("api2")
    self.assertEqual(launcher.active_profile_name(), "moni")
    self.assertEqual(launcher._workspace_header.profile_combo.currentText(), "moni")
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py -k "global_profile" -q`

Expected: FAIL because launcher has no global profile coordinator.

- [ ] **Step 3: Implement validation before broadcasting**

```python
def _request_workspace_profile(self, profile_name: str) -> None:
    target = profile_name.strip()
    previous = self._active_profile_name
    snapshots, _selected = load_profile_snapshots()
    if not target or load_runtime(target) is None:
        self._workspace_header.restore_profile(previous)
        return
    if not ensure_profile_unlocked(self, target, snapshots, self._unlocked_profiles):
        self._workspace_header.restore_profile(previous)
        return
    self._active_profile_name = target
    runtime = load_runtime(target)
    self._workspace_header.set_profiles(list(snapshots), target, runtime.environment)
    for page in self._pages.values():
        apply_profile = getattr(page, "apply_workspace_profile", None)
        if callable(apply_profile):
            apply_profile(target)
```

When a page is constructed later, call `apply_workspace_profile(self._active_profile_name)` exactly once after adding it to the stack.

- [ ] **Step 4: Keep public-data operation available without credentials**

If no profiles exist, render `未配置`, keep K-line navigation enabled, and do not call page profile methods. Pages decide which trading buttons to disable.

- [ ] **Step 5: Run focused profile tests**

Run: `python -m pytest tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py -k "profile" -q`

Expected: all new shell/launcher profile tests PASS; existing page-specific profile tests remain unchanged at this task boundary.

- [ ] **Step 6: Commit global profile coordination**

```powershell
git add roll_terminal_qt/launcher.py roll_terminal_qt/workspace_shell.py tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat(qt): coordinate api selection from workspace header"
```

---

### Task 4: K-Line Context Toolbar and Default Display State

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: page contract `apply_workspace_profile(profile_name: str)` from Task 3.
- Produces: `KlineAnalysisWindow.apply_workspace_profile(profile_name: str) -> None`.
- Produces: `KlineAnalysisWindow.workspace_profile_name() -> str`.
- Produces: `KlineAnalysisWindow.pattern_signals_enabled() -> bool`.

- [ ] **Step 1: Write failing default-state tests**

```python
def test_kline_defaults_to_visible_chart_with_patterns_disabled(self) -> None:
    window = KlineAnalysisWindow(embedded=True)
    self.assertFalse(window._hide_chart_btn.isChecked())
    self.assertTrue(window._chart_host.isVisibleTo(window))
    self.assertTrue(window._ema9.isChecked())
    self.assertTrue(window._ema21.isChecked())
    self.assertFalse(window._show_1h_shape_signal_check.isChecked())
    self.assertFalse(window._show_4h_shape_signal_check.isChecked())
    self.assertFalse(window._show_1d_shape_signal_check.isChecked())
    self.assertFalse(window._shape_signal_ma_touch_check.isChecked())
```

Add a test that `_api_profile_combo` is absent in embedded workspace mode and that `apply_workspace_profile("api2")` updates `_runtime`, `_last_profile_name`, account drawer context, and new-order context without changing chart symbol or period.

- [ ] **Step 2: Run the tests and verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "kline_defaults_to_visible or kline_workspace_profile" -q`

Expected: FAIL because the three pattern checkboxes are currently checked and the page owns an API combo.

- [ ] **Step 3: Apply the exact default values**

```python
self._show_1h_shape_signal_check.setChecked(False)
self._show_4h_shape_signal_check.setChecked(False)
self._show_1d_shape_signal_check.setChecked(False)
self._shape_signal_ma_touch_check.setChecked(False)
```

Keep `_hide_chart_btn` unchecked, EMA15/SMA50 checked, and the fallback period `4H`.

- [ ] **Step 4: Restructure the K-line context toolbar**

Keep symbol, period buttons, EMA15, SMA50, a single `形态：关/开` button, `双图`, quantity, and `图表设置` in the visible row. Put 1H/4H/1D pattern details, MA-touch, average K line, reverse K line, local-first, range mode, and auto-refresh actions inside the graph-settings popup. Preserve the underlying widgets and signal handlers so chart calculations do not change.

- [ ] **Step 5: Add the external profile interface**

```python
def apply_workspace_profile(self, profile_name: str) -> None:
    target = profile_name.strip()
    if not target or target == self._last_profile_name:
        return
    runtime = load_runtime(target)
    if runtime is None:
        return
    self._runtime = runtime
    self._last_profile_name = target
    self._sync_account_context()
    self._sync_account_drawer_context()
    self._load_data()
```

Do not prompt for a password inside this method; Task 3 already performed the single global access check.

- [ ] **Step 6: Run K-line UI tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "kline and (default or profile or shape or chart)" -q`

Expected: selected tests PASS.

- [ ] **Step 7: Commit K-line presentation changes**

```powershell
git add roll_terminal_qt/kline_analysis_window.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat(qt): simplify kline context controls"
```

---

### Task 5: Bind RR and Line-Condition Tasks to Their Creation API

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: persisted line payload fields `trade_profile_name: str` and `trade_environment: str`.
- Produces: `KlineAnalysisWindow.local_task_counts() -> tuple[LocalTaskCount, ...]`.
- Produces: `_monitorable_rr_trade_ledger_entries() -> list[RRTradeLedgerEntry]`, which is not filtered by the selected UI profile.

- [ ] **Step 1: Write failing task-binding tests**

```python
def test_line_condition_keeps_profile_bound_when_armed(self) -> None:
    window.apply_workspace_profile("api1")
    window._line_trade_armed_check.setChecked(True)
    window._enable_selected_line_trade()
    saved = window._selected_line_payload()
    self.assertEqual(saved["trade_profile_name"], "api1")
    window.apply_workspace_profile("api2")
    plan = window._build_line_trade_plan(saved)
    self.assertEqual(plan.profile_name, "api1")

def test_rr_monitor_reconciles_entries_for_hidden_profiles(self) -> None:
    window.apply_workspace_profile("api2")
    window._rr_trade_ledger_snapshot = {"entries": [api1_entry.to_dict(), api2_entry.to_dict()]}
    self.assertEqual(
        {entry.plan.profile_name for entry in window._monitorable_rr_trade_ledger_entries()},
        {"api1", "api2"},
    )
```

Add a test that execution loads runtime using `entry.plan.profile_name`, not `_active_profile_name()`, and a legacy line payload without binding fields uses the current profile once and persists the resulting binding.

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "condition_keeps_profile or monitor_reconciles_entries_for_hidden_profiles or legacy_line_profile" -q`

Expected: FAIL because line tasks currently consult current UI profile and RR matching filters by selected profile.

- [ ] **Step 3: Persist line-task identity when enabling trading**

```python
payload["trade_profile_name"] = str(payload.get("trade_profile_name") or self._active_profile_name()).strip()
payload["trade_environment"] = str(payload.get("trade_environment") or self._active_environment()).strip()
```

Use those fields in `_build_line_trade_plan`. Never overwrite non-empty binding fields during later UI profile changes.

- [ ] **Step 4: Decouple RR monitoring from visible-account filtering**

Keep `_matching_rr_trade_ledger_entries()` for the visible table. Add `_monitorable_rr_trade_ledger_entries()` to return every entry for which `RRTradeExecutionService.should_monitor_status(entry.status)` is true. For each reconcile/cancel/management action, call `load_runtime(entry.plan.profile_name)` and pass that runtime’s client, credentials, and config.

- [ ] **Step 5: Group task status by bound profile**

Count RR tasks from each entry’s `plan.profile_name`; count enabled line-trading tasks from `trade_profile_name`. Return `LocalTaskCount` objects and keep `local_task_summary()` as a temporary aggregate compatibility wrapper until Task 8 removes it.

- [ ] **Step 6: Run K-line execution and persistence regression tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "rr or line_trade or condition" -q`

Expected: selected tests PASS; RR sizing, fee, stop, and target assertions remain unchanged.

- [ ] **Step 7: Commit immutable K-line task binding**

```powershell
git add roll_terminal_qt/kline_analysis_window.py tests/test_roll_terminal_qt_windows.py
git commit -m "fix(qt): keep kline tasks bound to creation api"
```

---

### Task 6: Positions Page Uses the Global Profile Safely

**Files:**
- Modify: `roll_terminal_qt/account_positions_home.py`
- Test: `tests/test_account_positions_home_qt.py`

**Interfaces:**
- Produces: `AccountPositionsHomeWidget.apply_workspace_profile(profile_name: str) -> None`.
- Produces: signal `workspace_profile_applied = Signal(str, bool)`.
- Produces: `AccountPositionsHomeWidget.workspace_profile_name() -> str`.

- [ ] **Step 1: Write failing external-profile tests**

```python
def test_account_page_accepts_workspace_profile_without_internal_combo(self) -> None:
    page = AccountPositionsHomeWidget()
    self.assertFalse(hasattr(page, "_profile_combo"))
    page.apply_workspace_profile("api2")
    self.assertEqual(page._profile_switch_requested_target, "api2")

def test_manual_flatten_captures_profile_before_async_execution(self) -> None:
    request = page._build_selected_position_flatten_request(position)
    self.assertEqual(request.profile_name, "api1")
    page.apply_workspace_profile("api2")
    self.assertEqual(request.profile_name, "api1")
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_account_positions_home_qt.py -k "workspace_profile or flatten_captures_profile" -q`

Expected: FAIL because profile switching is driven by `_profile_combo`.

- [ ] **Step 3: Refactor existing asynchronous switching behind the public method**

```python
def apply_workspace_profile(self, profile_name: str) -> None:
    target = profile_name.strip()
    if not target or target == self._last_profile_name:
        return
    self._profile_change_serial += 1
    serial = self._profile_change_serial
    self._set_profile_switch_in_progress(True)
    QTimer.singleShot(0, lambda: self._apply_profile_change(target, serial))
```

Remove combo-dependent equality checks from `_apply_profile_change`. Keep the existing orderly shutdown, thread-generation guard, feed restart, and timeout behavior. Emit `workspace_profile_applied(target, True)` only after the new runtime and feeds are installed; emit `(target, False)` after runtime failure.

- [ ] **Step 4: Remove only the duplicated API UI**

Delete `API配置` and `_profile_combo` from `_build_header`. Do not change the positions toolbar, filters, tree, history tabs, or column settings.

- [ ] **Step 5: Capture manual-operation identity**

Ensure flatten/cancel request objects receive explicit `profile_name`, `environment`, and runtime before starting their worker thread. Confirmation and result text must use the captured values, never the later `_last_profile_name`.

- [ ] **Step 6: Run account tests**

Run: `python -m pytest tests/test_account_positions_home_qt.py -q`

Expected: all account-page tests PASS.

- [ ] **Step 7: Commit account integration**

```powershell
git add roll_terminal_qt/account_positions_home.py tests/test_account_positions_home_qt.py
git commit -m "feat(qt): drive positions from global api context"
```

---

### Task 7: Professional Arbitrage Separates Viewing Runtime from Task Runtime

**Files:**
- Modify: `roll_terminal_qt/ui.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `RollTerminalWindow.apply_workspace_profile(profile_name: str) -> None`.
- Produces: `RollTerminalWindow.workspace_profile_name() -> str`.
- Produces: `_auto_task_runtime: ArbitrageTradeRuntime | None` captured when auto monitoring starts.
- Produces: `RollTerminalWindow.local_task_counts() -> tuple[LocalTaskCount, ...]`.

- [ ] **Step 1: Write failing runtime-separation tests**

```python
def test_arbitrage_profile_switch_does_not_replace_running_auto_runtime(self) -> None:
    window._runtime = runtime_api1
    window._start_auto_monitor()
    self.assertIs(window._auto_task_runtime, runtime_api1)
    window.apply_workspace_profile("api2")
    self.assertIs(window._runtime, runtime_api2)
    self.assertIs(window._auto_task_runtime, runtime_api1)

def test_new_arbitrage_task_uses_new_global_profile(self) -> None:
    window.apply_workspace_profile("api2")
    thread = window._build_professional_open_thread(plan)
    self.assertIs(thread.runtime, runtime_api2)
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "arbitrage_profile_switch or new_arbitrage_task_uses" -q`

Expected: FAIL because `_runtime` currently serves both the view and auto monitor, and profile switching is blocked while auto trading is enabled.

- [ ] **Step 3: Capture runtime at task activation**

```python
def _start_auto_monitor(self) -> None:
    if self._runtime is None:
        raise RuntimeError("当前 API 不可用")
    self._auto_task_runtime = self._runtime
    self._auto_enabled = True

def _stop_auto_monitor(self) -> None:
    self._auto_enabled = False
    self._auto_task_runtime = None
```

Every timer-triggered auto execution must use `_auto_task_runtime`; every manually created or newly enabled task uses current `_runtime`. Existing execution threads already receive a runtime in their constructor and therefore remain stable after launch.

- [ ] **Step 4: Add the external profile interface and remove the local selector**

Refactor `_apply_api_profile` into `apply_workspace_profile`. It may stop and restart account/market feed threads for the visible account, but it must not stop `_execution_thread`, change `_auto_task_runtime`, or disable `_auto_enabled`. Remove the API label/combo from the page header.

- [ ] **Step 5: Report tasks by bound profile**

Return an arbitrage count for `_auto_task_runtime.credential_profile_name` while auto monitoring is enabled and for any in-flight execution thread’s captured runtime. Deduplicate a single logical task if both references point to the same execution.

- [ ] **Step 6: Run professional-arbitrage tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "roll or arbitrage or professional" -q`

Expected: new tests PASS. If the known three unrelated window tests still fail with their existing assertions, record them without altering stable flatten logic.

- [ ] **Step 7: Commit arbitrage runtime separation**

```powershell
git add roll_terminal_qt/ui.py tests/test_roll_terminal_qt_windows.py
git commit -m "fix(qt): bind arbitrage tasks to start api"
```

---

### Task 8: Global Connection and Per-API Task Status

**Files:**
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/workspace_shell.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Modify: `roll_terminal_qt/account_positions_home.py`
- Modify: `roll_terminal_qt/ui.py`
- Test: `tests/test_workspace_shell_qt.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Page contract: optional `local_task_counts() -> tuple[LocalTaskCount, ...]`.
- Page contract: optional `connection_snapshot() -> dict[str, str | bool]` with keys `public_online`, `private_online`, and `private_status`.
- Produces: `LauncherWindow._refresh_workspace_status() -> None`.

- [ ] **Step 1: Write failing aggregation tests**

```python
def test_launcher_groups_hidden_page_tasks_by_api(self) -> None:
    kline.local_task_counts.return_value = (LocalTaskCount("api1", rr=2),)
    roll.local_task_counts.return_value = (LocalTaskCount("api2", arbitrage=1),)
    launcher._refresh_workspace_status()
    self.assertEqual(
        launcher._workspace_header.task_button.text(),
        "api1：RR 2｜api2：套利 1",
    )

def test_connection_text_distinguishes_public_and_private_state(self) -> None:
    launcher._workspace_public_online = True
    account.connection_snapshot.return_value = {
        "public_online": True,
        "private_online": False,
        "private_status": "API 未解锁",
    }
    launcher._refresh_workspace_status()
    self.assertIn("行情在线", launcher._workspace_header.connection_label.text())
    self.assertIn("账户未连接", launcher._workspace_header.connection_label.text())
```

- [ ] **Step 2: Run tests and verify failure**

Run: `python -m pytest tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py -k "hidden_page_tasks or connection_text" -q`

Expected: FAIL because the launcher currently accepts only aggregate dictionaries and one status label.

- [ ] **Step 3: Replace aggregate task polling**

On the existing one-second launcher timer, collect `local_task_counts()` from every constructed page, merge with `merge_local_task_counts`, and format with `format_local_task_counts`. Keep the status visible regardless of the active page.

- [ ] **Step 4: Separate public and private connection text**

Use one compact header label but render both facts, for example `● 行情在线 · 账户未连接` or `● 行情在线 · 私有WS在线`. A locked API is not reported as a market-data outage.

- [ ] **Step 5: Run status and shutdown tests**

Run: `python -m pytest tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py -k "task or connection or close_warns" -q`

Expected: selected tests PASS, including close warning for hidden tasks.

- [ ] **Step 6: Commit global status behavior**

```powershell
git add roll_terminal_qt/launcher.py roll_terminal_qt/workspace_shell.py roll_terminal_qt/kline_analysis_window.py roll_terminal_qt/account_positions_home.py roll_terminal_qt/ui.py tests/test_workspace_shell_qt.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat(qt): show per-api tasks in global header"
```

---

### Task 9: Integration Acceptance and Documentation

**Files:**
- Modify: `README.md`
- Modify: `软件开发指南.md`
- Modify: `docs/kline_analysis_m1_acceptance.md`
- Test: `tests/test_workspace_shell_qt.py`
- Test: `tests/test_account_positions_home_qt.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes all prior task interfaces.
- Produces no new runtime interface.

- [ ] **Step 1: Add one end-to-end Qt test**

```python
def test_workspace_end_to_end_default_navigation_and_profile_task_isolation(self) -> None:
    runtimes = {
        "api1": SimpleNamespace(credential_profile_name="api1", environment="demo"),
        "api2": SimpleNamespace(credential_profile_name="api2", environment="live"),
    }

    class KlineStub(QWidget):
        def __init__(self, *, embedded: bool = False) -> None:
            super().__init__()
            self.profile_name = "api1"
            self.bound_counts = (LocalTaskCount("api1", line_conditions=1),)

        def apply_workspace_profile(self, profile_name: str) -> None:
            self.profile_name = profile_name

        def local_task_counts(self) -> tuple[LocalTaskCount, ...]:
            return self.bound_counts

    class AccountStub(QWidget):
        def __init__(self, parent=None) -> None:  # noqa: ANN001
            super().__init__(parent)
            self.profile_name = ""

        def apply_workspace_profile(self, profile_name: str) -> None:
            self.profile_name = profile_name

        def workspace_profile_name(self) -> str:
            return self.profile_name

    with (
        patch("roll_terminal_qt.launcher.KlineAnalysisWindow", side_effect=KlineStub),
        patch("roll_terminal_qt.launcher.AccountPositionsHomeWidget", side_effect=AccountStub),
        patch("roll_terminal_qt.launcher.load_profile_snapshots", return_value=({"api1": {}, "api2": {}}, "api1")),
        patch("roll_terminal_qt.launcher.load_runtime", side_effect=lambda name=None: runtimes[name or "api1"]),
        patch("roll_terminal_qt.launcher.ensure_profile_unlocked", return_value=True),
    ):
        launcher = LauncherWindow()
        self.assertEqual(launcher.current_page_key(), "kline")
        launcher._request_workspace_profile("api2")
        launcher.show_page("account")
        self.assertEqual(launcher._pages["account"].workspace_profile_name(), "api2")
        self.assertEqual(
            launcher._pages["kline"].local_task_counts(),
            (LocalTaskCount("api1", line_conditions=1),),
        )
```

- [ ] **Step 2: Run the integration test in red/green sequence**

Run: `python -m pytest tests/test_workspace_shell_qt.py tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py -k "workspace_end_to_end" -q`

Expected before final wiring: FAIL because one or more shell/page contracts are incomplete. Expected after final wiring: PASS.

- [ ] **Step 3: Update user and maintenance documentation**

Document the exact top-level entries, default K-line/pattern state, global API semantics, task-binding rule, single-window page lifecycle, auxiliary-window exception, and the fact that Tk/server backtest/live strategy behavior is unchanged.

- [ ] **Step 4: Run targeted verification**

```powershell
python -m py_compile roll_terminal_qt/workspace_shell.py roll_terminal_qt/launcher.py roll_terminal_qt/kline_analysis_window.py roll_terminal_qt/account_positions_home.py roll_terminal_qt/ui.py
python -m pytest tests/test_workspace_shell_qt.py tests/test_account_positions_home_qt.py tests/test_qt_realtime_account_store.py tests/test_qt_incremental_views.py -q
python -m pytest tests/test_roll_terminal_qt_windows.py -k "launcher or workspace or profile or rr or line_trade or arbitrage" -q
```

Expected: compilation succeeds; all new and directly affected tests PASS.

- [ ] **Step 5: Run the broader regression suite and classify only known failures**

Run: `python -m pytest -q`

Expected: no new failures. Compare any remaining failures with the pre-existing list in Global Constraints. Do not claim the full suite passes if those failures remain.

- [ ] **Step 6: Perform manual acceptance**

Launch the Qt client and verify all ten acceptance criteria in `docs/superpowers/specs/2026-07-12-trading-terminal-navigation-design.md`: startup page, defaults, navigation, retained state, cross-API viewing, immutable task API, confirmation identity, hidden-page monitoring, unique routes, and distinguishable connection states.

- [ ] **Step 7: Commit documentation and final acceptance test**

```powershell
git add README.md 软件开发指南.md docs/kline_analysis_m1_acceptance.md tests/test_workspace_shell_qt.py tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py
git commit -m "docs(qt): document single-window trading workspace"
```

---

## Rollback Boundaries

- Tasks 1-4 can be reverted without changing any trading-task persistence schema.
- Task 5 adds backward-compatible line payload fields; old payloads remain readable and receive a binding only when next armed.
- Task 7 changes runtime ownership only; persisted arbitrage plans and ledgers remain unchanged.
- If global profile propagation fails during rollout, keep the selected page on its prior runtime and restore the header profile; never partially redirect an already submitted operation.
- Never roll back by deleting shared state, candle cache, RR ledger, order history, or the common `qqokx_data` directory.
