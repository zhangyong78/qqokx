# 双图 K 线独立交易对 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让双图 K 线的副图可独立选择交易对，适用于左右与上下分屏。

**Architecture:** 主图继续从 `_symbol_combo` 读取交易对；新增副图下拉框与 `_selected_secondary_symbol()`，只供副图 K 线请求使用。副图波动率仍使用主图币种。副图请求键包含该独立交易对，以隔离内存缓存。

**Tech Stack:** Python 3、PySide6、unittest、QtCharts。

## Global Constraints

- 不新增持久化配置或副图实时订阅。
- 不修改主图画线、预警、工作区和账户抽屉上下文。
- 只运行本次改动直接相关的定向测试；单个命令不超过 5 分钟。

---

### Task 1: 副图交易对控件与状态

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `_selected_secondary_symbol() -> str`
- Produces: `_secondary_symbol_combo: QComboBox`

- [ ] **Step 1: Write the failing test**

```python
def test_secondary_symbol_control_only_available_for_dual_kline(self) -> None:
    window._secondary_chart_check.setChecked(True)
    window._secondary_chart_kind_mode = "kline"
    window._update_secondary_controls_state()
    self.assertTrue(window._secondary_symbol_combo.isVisible())
    self.assertTrue(window._secondary_symbol_combo.isEnabled())
    window._secondary_chart_kind_mode = "volatility"
    window._update_secondary_controls_state()
    self.assertFalse(window._secondary_symbol_combo.isVisible())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_symbol_control_only_available_for_dual_kline`

Expected: FAIL because the control does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
self._secondary_symbol_combo = QComboBox()
self._secondary_symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
self._secondary_symbol_combo.currentTextChanged.connect(self._on_secondary_symbol_changed)

def _selected_secondary_symbol(self) -> str:
    return self._secondary_symbol_combo.currentText().strip().upper()
```

Set its visible and enabled state only for an enabled K-line secondary chart.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_symbol_control_only_available_for_dual_kline`

Expected: PASS.

### Task 2: 独立副图请求、缓存与切换

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: `_selected_secondary_symbol() -> str`
- Produces: `_on_secondary_symbol_changed(value: str) -> None`

- [ ] **Step 1: Write the failing tests**

```python
def test_secondary_kline_request_key_uses_secondary_symbol(self) -> None:
    window._secondary_chart_check.setChecked(True)
    window._secondary_chart_kind_mode = "kline"
    window._symbol_combo.setCurrentText("BTC-USDT-SWAP")
    window._secondary_symbol_combo.setCurrentText("ETH-USDT-SWAP")
    self.assertEqual(window._current_secondary_request_key()[2], "ETH-USDT-SWAP")

def test_secondary_symbol_change_loads_only_secondary_chart(self) -> None:
    with patch.object(window, "_load_secondary_data") as load_secondary, patch.object(window, "_load_data") as load_primary:
        window._on_secondary_symbol_changed("ETH-USDT-SWAP")
    load_secondary.assert_called_once_with(symbol="ETH-USDT-SWAP")
    load_primary.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_kline_request_key_uses_secondary_symbol tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_symbol_change_loads_only_secondary_chart`

Expected: FAIL because the current request key and loader use the primary symbol.

- [ ] **Step 3: Write minimal implementation**

Update `_current_secondary_request_key()` and normal K-line `_load_secondary_data()` calls to use `_selected_secondary_symbol()`. In `_load_data()`, continue loading the primary as before and pass the selected secondary symbol to the secondary loader. In `_on_secondary_symbol_changed()`, load only the secondary chart when dual K-line mode is active; retain the existing queued-reload behavior when another loader is running.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_kline_request_key_uses_secondary_symbol tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_secondary_symbol_change_loads_only_secondary_chart`

Expected: PASS.

### Task 3: 回归验证

**Files:**
- Modify: none
- Test: `tests/test_roll_terminal_qt_windows.py`

- [ ] **Step 1: Run focused dual-chart regression tests**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_selecting_symbol_without_volatility_reverts_secondary_chart_to_kline tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_load_secondary_data_previews_cached_secondary_payload_before_loader_returns tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_primary_average_secondary_normal_request_keys_use_primary_average_only`

Expected: PASS.

- [ ] **Step 2: Run syntax and whitespace checks**

Run: `python -m py_compile roll_terminal_qt/kline_analysis_window.py; git diff --check`

Expected: PASS.
