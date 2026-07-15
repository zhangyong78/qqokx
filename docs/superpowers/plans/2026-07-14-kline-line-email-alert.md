# K线画线邮件提醒 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为动作=提醒的 K 线画线规则增加异步邮件提醒，支持一次或每次重新触发。

**Architecture:** 在线条工作区保存邮件配置和单次发送状态；K线窗口接收新线条事件后，复用现有邮件配置创建 `EmailNotifier` 并异步投递。告警模块继续只负责事件去重。

**Tech Stack:** Python, PySide6, existing `EmailNotifier`, unittest

## Global Constraints

- 仅支持动作=`notify` 的画线规则。
- 默认关闭邮件、默认单次模式。
- 不在测试中发送真实邮件。

---

### Task 1: 线条邮件配置

**Files:**
- Modify: `roll_terminal_qt/kline_alerts.py`
- Test: `tests/test_kline_alerts.py`

- [ ] 写失败测试：旧线条默认邮件关闭，单次发送状态可持久化。
- [ ] 运行测试确认失败。
- [ ] 在规范化线条记录中加入 `email_enabled`、`email_delivery_mode`、`email_sent_once`。
- [ ] 运行测试确认通过。

### Task 2: 邮件投递与界面配置

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

- [ ] 写失败测试：单次模式只投递一次，重复模式每个事件都投递，邮件禁用不消耗资格。
- [ ] 运行测试确认失败。
- [ ] 增加当前线条邮件控件、工作区保存和异步 `EmailNotifier` 投递。
- [ ] 运行测试确认通过。

### Task 3: 定向验证

- [ ] 编译两个修改模块。
- [ ] 运行两组相关测试。
- [ ] 检查 diff，确认不改交易线和其他告警。

