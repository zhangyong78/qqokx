# RR Exchange Trade Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a selected RR drawing be explicitly enabled as a managed OKX perpetual-swap order without allowing drawing or saving to place an order.

**Architecture:** Add a small execution-domain module that owns order state transitions and exchange reconciliation. The K-line window remains a thin UI adapter that presents the selected RR item, asks for confirmation, and refreshes a persisted ledger; all exchange calls run outside the UI thread.

**Tech Stack:** Python 3, PySide6, existing `OkxRestClient`, `StrategyConfig`, JSON persistence, `unittest`.

## Global Constraints

- Support only `SWAP` for this delivery.
- Saving/confirming an RR drawing is analysis-only; only `Enable trading` may send an order.
- Use ordinary limit orders for best bid/ask chasing; use OKX conditional market algos for stop loss and take profit.
- Preserve current net/long-short account-mode handling through existing client helpers.
- Automated external validation may use `moni` only, never `live`.

---

### Task 1: Extend the RR execution data model

**Files:**
- Modify: `okx_quant/kline_rr_trade.py`
- Test: `tests/test_kline_rr_trade.py`

**Interfaces:**
- Produces: `RRTradeExecutionMode`, `RRTradeLedgerEntry` fields for entry fill and protection status.
- Consumes: existing `RRTradePlan`, `RRTradeOrderLink`, and JSON persistence.

- [ ] Write failing tests proving that `chase_best_quote` round-trips through a plan and an entry ledger stores entry/protection lifecycle state.
- [ ] Run `python -m unittest tests.test_kline_rr_trade` and verify the new tests fail because those fields do not exist.
- [ ] Add the smallest backward-compatible dataclass fields and serialization defaults.
- [ ] Run `python -m unittest tests.test_kline_rr_trade` and verify it passes.

### Task 2: Add exchange-safe RR execution service

**Files:**
- Create: `okx_quant/kline_rr_execution.py`
- Test: `tests/test_kline_rr_execution.py`

**Interfaces:**
- Produces: `RRTradeExecutionService.activate`, `reconcile`, and `cancel`.
- Consumes: `OkxRestClient`, credentials, `StrategyConfig`, `RRTradePlan`, and `RRTradeLedgerEntry`.

- [ ] Write failing tests for best bid/ask entry pricing, no replacement after a fill, one cancellation before replacement, and preservation of protection after partial-fill cancellation.
- [ ] Run `python -m unittest tests.test_kline_rr_execution` and verify expected failures.
- [ ] Implement client-ID creation, ordinary market/fixed-limit/best-quote entry submission, exchange-state reconciliation, and conditional-market protection creation.
- [ ] Implement cancel recovery by checking exchange order state before and after cancellation; do not create a replacement if any fill exists.
- [ ] Run the focused tests until green.

### Task 3: Persist and refresh RR execution state

**Files:**
- Modify: `okx_quant/persistence.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: selected RR execution status text and saved ledger updates.
- Consumes: `RRTradeExecutionService` and existing RR ledger persistence.

- [ ] Write failing UI tests showing a saved RR has no order action, while `Enable trading` creates an execution request and refreshes status from the ledger.
- [ ] Add `Enable trading`, `Cancel trade`, and execution-mode controls near the existing RR tracking panel.
- [ ] Run exchange work in a dedicated worker/QThread with one in-flight reconciliation guard.
- [ ] Save ledger transitions atomically and reload them by profile, environment, and symbol.
- [ ] Run the focused UI tests until green.

### Task 4: Chase and local protection monitoring

**Files:**
- Modify: `okx_quant/kline_rr_execution.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_kline_rr_execution.py`

**Interfaces:**
- Produces: a timer-driven, coalesced reconciliation loop for active RR entries.
- Consumes: current order book, exchange order states, and persisted ledger entries.

- [ ] Write failing tests proving that unchanged best quote does not submit another order and changed best quote performs cancel-then-replace exactly once.
- [ ] Add a coalesced monitor that never overlaps API calls and only tracks entries created by this RR ledger.
- [ ] Add protection reconciliation and stop-loss algo amendment hooks without allowing the monitor to create a new opening order.
- [ ] Run focused execution and UI tests until green.

### Task 5: Verify and run a demo smoke test

**Files:**
- Test: `tests/test_kline_rr_trade.py`
- Test: `tests/test_kline_rr_execution.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

- [ ] Run `python -m py_compile okx_quant/kline_rr_trade.py okx_quant/kline_rr_execution.py roll_terminal_qt/kline_analysis_window.py`.
- [ ] Run the focused unittest suite.
- [ ] With the selected runtime confirmed as `moni`, submit the smallest valid passive demo entry through the new service, verify the returned order identifier, and cancel it.
- [ ] Record the exact demo result in the RR event ledger and report any exchange permission or account-mode error verbatim.
