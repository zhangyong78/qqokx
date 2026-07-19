# K 线日线时区对比 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 增加一键配置 UTC+8 与 UTC 日线双图对比的按钮。

**Architecture:** 复用现有主/副 KlineDataLoader，右图传入 OKX 原生周期 `1Dutc`。按钮只写入已有双图控件状态并调用一次加载，不引入新的数据层。

**Tech Stack:** Python、PySide6、OKX REST、unittest。

## Global Constraints

- 不重采样日线，不增加新接口。
- 只影响 K 线分析窗口。
- 不运行全量测试。

### Task 1: 写入失败测试

**Files:**
- Modify: `D:/qqokx/tests/test_roll_terminal_qt_windows.py`

- [x] 新增测试，调用日线时区对比预设后断言：双图开启、第三图关闭、布局为左右、主图 `1D`、副图 `1Dutc`、副图交易对等于主图；并断言 `_bar_to_ms("1Dutc") == 86_400_000`。
- [x] 运行该测试，确认因按钮处理方法缺失而失败。

### Task 2: 实现预设按钮

**Files:**
- Modify: `D:/qqokx/roll_terminal_qt/kline_analysis_window.py`
- Test: `D:/qqokx/tests/test_roll_terminal_qt_windows.py`

- [x] 在副图周期下拉框加入 `1Dutc`，并添加“UTC+8/UTC日线”按钮和槽方法。
- [x] 在槽方法中设置同一交易对、`1D`/`1Dutc`、左右布局和 K 线副图，然后一次性加载。
- [x] 让日线时间步长计算接受 `1Dutc`。
- [x] 运行新增测试与现有双图周期测试。
- [x] 运行 `py_compile` 和 `git diff --check`。
