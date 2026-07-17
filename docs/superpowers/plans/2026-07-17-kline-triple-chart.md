# 三图 K 线联动 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 增加固定横向三列的三图 K 线联动，第二、第三图可独立选择交易对和周期。

**Architecture:** 第三图新增独立 QtChart、控件、加载器和缓存；主图仍负责画线、预警和实时订阅。将现有主图与副图一对一的时间范围、十字光标同步扩展为向所有可见图表广播。

**Tech Stack:** Python 3、PySide6、QtCharts、unittest。

## Global Constraints

- 三图模式固定横向三列；第三图只支持 K 线。
- 不修改主图工作区、画线、预警、账户抽屉或实时订阅。
- 第二、第三图的交易对和周期独立；切换时只加载对应副图。
- 只运行直接相关的定向测试；单个测试命令不得超过 5 分钟。

---

### Task 1: 三图模式控件与布局状态

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `_tertiary_chart_check: QCheckBox`
- Produces: `_triple_chart_enabled() -> bool`

- [ ] **Step 1: Write failing tests**

```python
def test_triple_chart_mode_forces_horizontal_layout(self) -> None:
    window._tertiary_chart_check.setChecked(True)
    self.assertTrue(window._triple_chart_enabled())
    self.assertEqual(window._chart_stack_splitter.orientation(), Qt.Orientation.Horizontal)

def test_triple_chart_controls_show_only_when_enabled(self) -> None:
    window._tertiary_chart_check.setChecked(True)
    self.assertFalse(window._tertiary_symbol_combo.isHidden())
    self.assertFalse(window._tertiary_period_combo.isHidden())
```

- [ ] **Step 2: Run tests to verify failure**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_triple_chart_mode_forces_horizontal_layout tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_triple_chart_controls_show_only_when_enabled`

Expected: FAIL because third-chart controls and mode do not exist.

- [ ] **Step 3: Implement minimal UI state**

Add the three-chart switch and third symbol/period controls. Enabling it turns on the existing secondary chart, hides incompatible secondary volatility mode, displays the third frame, and lays the chart splitter out horizontally in equal thirds. Disabling it returns to existing two-chart behavior.

- [ ] **Step 4: Run tests to verify pass**

Run the Task 1 command. Expected: PASS.

### Task 2: 第三图独立请求与渲染

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `_selected_tertiary_symbol() -> str`
- Produces: `_current_tertiary_request_key() -> tuple[Any, ...] | None`
- Produces: `_load_tertiary_data() -> None`

- [ ] **Step 1: Write failing tests**

```python
def test_tertiary_request_key_uses_its_own_symbol_and_period(self) -> None:
    window._tertiary_chart_check.setChecked(True)
    window._tertiary_symbol_combo.setCurrentText("SOL-USDT-SWAP")
    window._tertiary_period_combo.setCurrentText("1H")
    key = window._current_tertiary_request_key()
    self.assertEqual(key[2:4], ("SOL-USDT-SWAP", "1H"))

def test_tertiary_symbol_change_loads_only_tertiary_chart(self) -> None:
    with patch.object(window, "_load_tertiary_data") as load_tertiary, patch.object(window, "_load_data") as load_primary:
        window._on_tertiary_symbol_changed("SOL-USDT-SWAP")
    load_tertiary.assert_called_once()
    load_primary.assert_not_called()
```

- [ ] **Step 2: Run tests to verify failure**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_tertiary_request_key_uses_its_own_symbol_and_period tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_tertiary_symbol_change_loads_only_tertiary_chart`

Expected: FAIL because third-chart loading state does not exist.

- [ ] **Step 3: Implement minimal data path**

Add a separate loader, request id, cache and payload state. Use `KlineDataLoader` with alerts and shape signals disabled. Render the returned payload into the third native chart, reject stale request results, and ensure primary refresh starts the third load only while triple mode is enabled.

- [ ] **Step 4: Run tests to verify pass**

Run the Task 2 command. Expected: PASS.

### Task 3: 三图时间轴与十字光标联动

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: primary, secondary and tertiary chart views.
- Produces: synchronized visible range and external hover time for every active chart.

- [ ] **Step 1: Write failing test**

```python
def test_primary_range_sync_broadcasts_to_both_secondary_charts(self) -> None:
    window._tertiary_chart_check.setChecked(True)
    with patch.object(window._secondary_native_chart_view, "set_external_x_range") as secondary_range, patch.object(window._tertiary_native_chart_view, "set_external_x_range") as tertiary_range:
        window._on_primary_x_range_changed(100.0, 200.0)
    secondary_range.assert_called_once_with(100.0, 200.0)
    tertiary_range.assert_called_once_with(100.0, 200.0)
```

- [ ] **Step 2: Run test to verify failure**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_primary_range_sync_broadcasts_to_both_secondary_charts`

Expected: FAIL because range synchronization only targets the second chart.

- [ ] **Step 3: Implement minimal broadcast synchronization**

Replace the single target range and hover update paths with helpers that apply to every other visible chart. Keep the existing recursion guard and do not synchronize inactive third-chart state.

- [ ] **Step 4: Run test to verify pass**

Run the Task 3 command. Expected: PASS.

### Task 4: 定向回归验证

**Files:**
- Modify: none
- Test: `tests/test_roll_terminal_qt_windows.py`

- [ ] **Step 1: Run new three-chart tests and existing dual-chart tests separately**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_triple_chart_mode_forces_horizontal_layout tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_triple_chart_controls_show_only_when_enabled tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_tertiary_request_key_uses_its_own_symbol_and_period tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_tertiary_symbol_change_loads_only_tertiary_chart tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_primary_range_sync_broadcasts_to_both_secondary_charts`

Then run the existing dual-chart tests one command at a time, because their combined Qt process has a known exit-stage thread-cleanup issue.

- [ ] **Step 2: Run syntax and whitespace checks**

Run: `python -m py_compile roll_terminal_qt/kline_analysis_window.py; git diff --check`

Expected: PASS.
