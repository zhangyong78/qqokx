# 双线反转结构过滤 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 仅在具备反转结构的两根 K 线组合上显示双线向上/向下反转标记。

**Architecture:** 在 `roll_terminal_qt/kline_analysis_window.py` 的双线标记构建前加入局部极值、盘整假破位、均线收回与实体收回判定。原有大小排名和图表显示流程保持不变。

**Tech Stack:** Python、PySide6 K 线分析窗口。

## Global Constraints

- 只改双线反转算法，不影响其它形态、界面和双图 1D 过滤。
- 4H/1D 使用 EMA15 与 SMA50，1H 只用 SMA50。
- 保留最近 10 根前 4 的实体/振幅门槛。

---

### Task 1: 收紧双线反转结构条件

**Files:**
- Modify: `roll_terminal_qt/kline_analysis_window.py:1619`

**Interfaces:**
- Consumes: `candles`、`ema15_values`、`sma50_values` 与周期均线开关。
- Produces: 仅当有效收回且命中局部极值、盘整假破位或均线收回时，输出双线反转标记。

- [ ] 新增实体收回、局部极值、盘整假破位和均线收回的纯函数。
- [ ] 在 `double_reversal_up/down` 进入现有大小排名逻辑前调用结构判定。
- [ ] 保持标签、颜色、排名显示和其它重放信号逻辑不变。
