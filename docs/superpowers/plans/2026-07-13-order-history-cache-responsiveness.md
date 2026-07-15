# Qt 历史数据本地优先与无阻塞刷新 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 历史委托、历史成交、历史仓位和持仓备注沿用旧版 zt 的本地数据，并消除同步与切换标签时的界面阻塞。

**Architecture:** 复用现有 `orders / fills / positions` JSON 与 `position_notes.json`，工作线程先发本地缓存，再请求远端并合并落盘。页面和共享委托总表按数据变化分区刷新；运行中的同步任务不被 UI 线程等待或重启。

**Tech Stack:** Python, PySide6, unittest, existing JSON persistence

## Global Constraints

- 不改变现有缓存格式或覆盖旧记录。
- 不修改 K 线行情和交易逻辑。
- 单个测试命令最长 5 分钟。

---

### Task 1: 三类历史缓存先显示、后台合并

**Files:**
- Modify: `roll_terminal_qt/history_service.py`
- Test: `tests/test_history_service.py`

- [ ] 写失败测试，覆盖委托、成交、仓位的缓存优先、远端合并和失败回退。
- [ ] 实现三类缓存读取与合并，保留旧版去重字段。
- [ ] 运行定向测试。

### Task 2: 无阻塞同步与增量渲染

**Files:**
- Modify: `roll_terminal_qt/account_positions_home.py`
- Modify: `roll_terminal_qt/shared_order_store.py`
- Test: `tests/test_account_positions_home_qt.py`
- Test: `tests/test_shared_order_store.py`

- [ ] 写失败测试，覆盖运行中任务不等待、相同快照不重绘、历史表不使用内容扫描列宽。
- [ ] 实现任务复用、快照去重和条件渲染。
- [ ] 运行定向测试。

### Task 3: 备注继承和持久化

**Files:**
- Modify: `roll_terminal_qt/account_positions_home.py`
- Test: `tests/test_account_positions_home_qt.py`

- [ ] 写失败测试，覆盖当前持仓备注状态保存及平仓后继承到历史仓位。
- [ ] 接入旧版备注 reconcile / inherit / prune 链路。
- [ ] 运行定向测试。

### Task 4: 最小验证

- [ ] 编译修改文件。
- [ ] 运行上述三个定向测试模块。
- [ ] 检查 git diff，确认无关模块未改。
