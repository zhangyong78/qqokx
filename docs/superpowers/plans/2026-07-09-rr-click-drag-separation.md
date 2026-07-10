# RR Click Drag Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent RR overlays in the Qt Kline analysis window from changing prices on simple click while preserving explicit drag editing.

**Architecture:** Keep the existing RR price update logic intact and only change interaction activation. Add a small scene-pixel drag threshold between pointer press and the first RR mutation, then cover the behavior with focused regression tests.

**Tech Stack:** Python, PySide6, unittest

## Global Constraints

- Only modify RR overlay interaction in the Qt Kline analysis window.
- Do not change non-RR drawing tools in this task.
- Use TDD for the interaction change.
- Keep edits surgical and local to the affected files.

---

### Task 1: Lock the expected interaction with tests

**Files:**
- Modify: `D:\qqokx\tests\test_roll_terminal_qt_windows.py`

**Interfaces:**
- Consumes: `KlineAnalysisWindow._on_chart_pointer_pressed`, `_on_chart_pointer_moved`, `_on_chart_pointer_released`
- Produces: regression tests for RR click-only vs RR drag activation

- [ ] **Step 1: Write the failing test**
- [ ] **Step 2: Run the RR interaction tests and verify click-only currently mutates RR**
- [ ] **Step 3: Update existing RR drag tests to use `press -> move -> release`**
- [ ] **Step 4: Add a click-only RR test**
- [ ] **Step 5: Run the focused tests again and confirm the new click-only test fails before implementation**

### Task 2: Add RR drag activation threshold

**Files:**
- Modify: `D:\qqokx\roll_terminal_qt\kline_analysis_window.py`

**Interfaces:**
- Consumes: native chart pointer events and existing RR drag update helpers
- Produces: threshold-activated RR drag state that only mutates on real drag

- [ ] **Step 1: Record scene pointer position from the native chart view**
- [ ] **Step 2: Store RR drag activation metadata on press**
- [ ] **Step 3: Activate RR drag only after pointer movement exceeds threshold**
- [ ] **Step 4: On release, save only active RR drags and ignore click-only RR selection**
- [ ] **Step 5: Keep double-click RR card opening unchanged**

### Task 3: Verify and clean up

**Files:**
- Modify: `D:\qqokx\tests\test_roll_terminal_qt_windows.py`
- Modify: `D:\qqokx\roll_terminal_qt\kline_analysis_window.py`

**Interfaces:**
- Consumes: updated RR interaction flow
- Produces: verified regression coverage and clean syntax

- [ ] **Step 1: Run focused RR interaction tests**
- [ ] **Step 2: Run related RR rendering/save tests**
- [ ] **Step 3: Run `py_compile` on touched files**
- [ ] **Step 4: Stop if any unrelated regression appears and isolate it instead of broadening scope**
