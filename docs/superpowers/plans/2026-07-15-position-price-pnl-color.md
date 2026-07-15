# Position Price and PnL Color Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply fixed buy-one and sell-one colors, and make market value follow each position row's unrealized PnL color.

**Architecture:** Keep all formatting and calculations unchanged. Update only the existing Qt tree-item foreground-color selection for concrete position rows; aggregate rows, empty values, and the two approximate-USDT quote columns remain untouched.

**Tech Stack:** Python, PySide6, unittest.

## Global Constraints

- `bid_price` is always green and `ask_price` is always red when a concrete price is present.
- `bid_usdt` and `ask_usdt` retain their current appearance.
- `market_value` uses the sign of the same row's `upl`; zero, missing values, and aggregate rows retain their current appearance.

---

### Task 1: Position-tree foreground colors

**Files:**
- Modify: `roll_terminal_qt/account_positions_home.py`
- Test: `tests/test_account_positions_home_qt.py`

- [ ] Add a focused failing test that creates a positive-PnL position row and asserts green `bid_price`, red `ask_price`, and green `market_value`; then create a negative-PnL row and assert red `market_value`.
- [ ] Run only that test and confirm the failure is caused by missing foreground-color assignment.
- [ ] Add the smallest foreground-color assignments in the existing position-tree row rendering path, leaving `bid_usdt`, `ask_usdt`, aggregates, and missing values unchanged.
- [ ] Re-run the focused test and the directly related position-home test module.

### Task 2: Targeted verification

- [ ] Compile the modified production and test files.
- [ ] Run `git diff --check` and review the diff to confirm only presentation code and its regression test changed.
