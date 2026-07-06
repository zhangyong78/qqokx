# BTC日线+4小时多空策略 波段高低点止损 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `BTC日线+4小时多空策略` 的开仓止损从 ATR 固定距离改成当前波段高低点，找不到时退回最近 10 根 K 线高低点。

**Architecture:** 只修改 `okx_quant/backtest.py` 里新双向策略的止损价格计算，不改候选信号扫描、不改 UI 参数、不改动态止盈。波段范围直接复用已有 candidate 的 `cross_index -> signal_index`，失败时退回最近 10 根 K 线窗口。

**Tech Stack:** Python, unittest, 现有 backtest engine

---

### Task 1: 先用测试锁定新的止损口径

**Files:**
- Modify: `tests/test_btc_daily_4h_long_short.py`
- Test: `tests/test_btc_daily_4h_long_short.py`

- [ ] **Step 1: 写做多止损取波段低点的失败测试**

- [ ] **Step 2: 跑单测确认当前仍按 ATR 止损而失败**

- [ ] **Step 3: 再写做空止损取波段高点、以及找不到时退回 10 根窗口的失败测试**

- [ ] **Step 4: 跑单测确认失败原因正确**

### Task 2: 在双向回测里实现波段止损

**Files:**
- Modify: `okx_quant/backtest.py`
- Test: `tests/test_btc_daily_4h_long_short.py`

- [ ] **Step 1: 新增一个只给双向 BTC 策略用的止损辅助函数**

- [ ] **Step 2: 做多分支改成优先取波段最低点，失败再取最近 10 根最低点**

- [ ] **Step 3: 做空分支改成优先取波段最高点，失败再取最近 10 根最高点**

- [ ] **Step 4: 在 trade metadata 里补一个 stop source，便于后续排查**

### Task 3: 回归验证

**Files:**
- Modify: `tests/test_btc_daily_4h_long_short.py`
- Test: `tests/test_btc_daily_4h_long_short.py`, `tests/test_backtest.py`, `tests/test_btc_ema15_ma50_pullback_long.py`, `tests/test_btc_ema15_ma50_pullback_short.py`

- [ ] **Step 1: 跑新策略定向单测**

- [ ] **Step 2: 跑旧 BTC 回踩策略与 backtest 公共回归**

- [ ] **Step 3: 确认没有误伤原来的长/短研究策略**
