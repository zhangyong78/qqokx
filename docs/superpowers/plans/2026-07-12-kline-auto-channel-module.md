# K 线自动通道模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 K 线页交付默认关闭的自动箱体与通道研究模块，不再提供独立自动通道页面入口。

**Architecture:** 保持 `KlineChartPayload` 作为唯一图表渲染输入；新增通道叠加列表，并从旧自动通道的分析配置和检测函数构建结果。页面控制区只保存开关和参数，渲染时按开关过滤对应图层；告警仍沿用既有箱体告警逻辑。

**Tech Stack:** Python 3、PySide6、现有 K 线 Web/原生图表、`okx_quant.analysis`、pytest。

## Global Constraints

- 自动通道仅用于分析和展示，不新增交易或下单。
- 默认关闭，旧工作区记录必须兼容。
- 不删除旧自动通道研究模块和快照存储。
- 只修改与 K 线模块、载荷、图表渲染和对应测试相关的文件。

---

### Task 1: 定义通道叠加数据与检测转换

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `KlineChartPayload.channel_overlays: list[dict[str, Any]]`。
- Produces: `_build_channel_current_overlays(candles: list[Any]) -> list[dict[str, Any]]`。

- [ ] **Step 1: Write the failing test**

```python
def test_chart_payload_contains_current_channel_overlay_when_detector_finds_channel() -> None:
    overlay = _build_channel_current_overlays(_channel_candles())

    assert overlay[0]["mode"] == "current"
    assert overlay[0]["upper_start"] > overlay[0]["lower_start"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "channel_overlay" -q`

Expected: FAIL because `_build_channel_current_overlays` does not exist.

- [ ] **Step 3: Write minimal implementation**

```python
def _build_channel_current_overlays(candles: list[Any]) -> list[dict[str, Any]]:
    channels = detect_channels(candles, ChannelDetectionConfig())
    if not channels:
        return []
    channel = channels[0]
    return [{
        "mode": "current",
        "start_index": int(channel.start_index),
        "end_index": int(channel.end_index),
        "upper_start": float(channel.upper_start),
        "upper_end": float(channel.upper_end),
        "lower_start": float(channel.lower_start),
        "lower_end": float(channel.lower_end),
        "label": "自动通道",
    }]
```

Extend `KlineChartPayload`, all payload constructors, cache/reverse/slice helpers, and primary chart worker to carry this list.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "channel_overlay" -q`

Expected: PASS.

### Task 2: 为网页与原生图表增加通道绘制

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: `KlineChartPayload.channel_overlays`。
- Produces: `_visible_channel_overlays(payload) -> list[dict[str, Any]]`。

- [ ] **Step 1: Write the failing test**

```python
def test_disabled_auto_channel_does_not_send_channel_layer_to_chart() -> None:
    app = SimpleNamespace(_auto_channel_check=FakeCheckBox(False))

    assert KlineAnalysisWindow._visible_channel_overlays(app, payload) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "visible_channel" -q`

Expected: FAIL because the visibility helper does not exist.

- [ ] **Step 3: Write minimal implementation**

Add `channels` to the `window.applyChartData` payload. Extend the existing chart JavaScript draw pass and the native `QPainter` path to draw each enabled channel as two boundary lines plus a translucent band. Empty or malformed entries are skipped.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "visible_channel" -q`

Expected: PASS.

### Task 3: 加入 K 线自动通道控制与工作区持久化

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `self._auto_box_check`, `self._history_box_check`, `self._auto_channel_check`。
- Consumes: existing workspace `alerts` and settings persistence helpers.

- [ ] **Step 1: Write the failing test**

```python
def test_kline_auto_channel_controls_default_to_disabled() -> None:
    window = KlineAnalysisWindow(embedded=True)
    try:
        assert not window._auto_box_check.isChecked()
        assert not window._history_box_check.isChecked()
        assert not window._auto_channel_check.isChecked()
    finally:
        dispose_widget(window)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "auto_channel_controls_default" -q`

Expected: FAIL because the controls do not exist.

- [ ] **Step 3: Write minimal implementation**

Create an “自动通道” control group. Move the display meaning out of `_box_breakout_alert_check`: `self._auto_box_check` controls current boxes, `self._history_box_check` controls history boxes, `self._auto_channel_check` controls channels, while `_box_breakout_alert_check` remains alert-only. Save/load `auto_box_visible`, `history_box_visible`, and `auto_channel_visible` on the workspace entry, falling back to `False`.

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "auto_channel_controls_default" -q`

Expected: PASS.

### Task 4: 回归验证与文档同步

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-12-trading-terminal-navigation-design.md`
- Test: `tests/test_roll_terminal_qt_windows.py`

- [ ] **Step 1: Write the failing regression test**

```python
def test_auto_box_visibility_is_independent_from_breakout_alert() -> None:
    app = SimpleNamespace(_auto_box_check=FakeCheckBox(True), _history_box_check=FakeCheckBox(False))

    assert len(KlineAnalysisWindow._visible_box_overlays(app, payload)) == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "auto_box_visibility_is_independent" -q`

Expected: FAIL because the old helper reads the alert checkbox.

- [ ] **Step 3: Implement the minimal regression fix and documentation**

Update `_visible_box_overlays` to read the two display checkboxes only. Document that automatic channel is now a K-line research module and that its old independent entry remains hidden.

- [ ] **Step 4: Run focused and related tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "auto_channel or box or kline" -q`

Expected: PASS, except separately documented pre-existing failures.

Run: `python -m py_compile roll_terminal_qt/kline_analysis_window.py`

Expected: exit code 0.
