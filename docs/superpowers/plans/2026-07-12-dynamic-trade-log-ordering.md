# EMA Dynamic Trade Log Ordering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make dynamic-entry logs distinguish confirmed position closure from delayed historical settlement without changing trading behavior.

**Architecture:** Keep the engine's synchronous position-management and re-entry flow unchanged, but replace the ambiguous round-completion message with a structured closure-confirmation message carrying the prior entry identifiers. Update the UI runtime parser to recognize both the new message and legacy logs, then rename reconciliation output to historical settlement terminology with round and order correlation fields.

**Tech Stack:** Python 3.11+, `unittest`, existing `StrategyEngine` and `UiStrategySessionsMixin` logging/reconciliation infrastructure.

## Global Constraints

- Do not change order submission, position monitoring, re-entry timing, risk parameters, or backtest behavior.
- Keep long and short strategy sessions independent.
- Missing identifiers render as `-` and never block trading.
- Legacy `本轮持仓已结束，继续监控下一次信号。` logs remain readable for recovery.

---

### Task 1: Emit an unambiguous position-closure log

**Files:**
- Modify: `okx_quant/engine.py:930`
- Test: `tests/test_strategy_engine.py:746`

**Interfaces:**
- Consumes: `ManagedEntryOrder.ord_id` and `ManagedEntryOrder.cl_ord_id`.
- Produces: `StrategyEngine._log_dynamic_position_closed(active_order: ManagedEntryOrder) -> None`.

- [ ] **Step 1: Write the failing test**

Add a focused assertion to the dynamic re-entry probe tests:

```python
def test_dynamic_exchange_strategy_logs_confirmed_close_before_reentry(self) -> None:
    _attempted, _accepted, _evaluate_calls, _waits, messages = self._run_dynamic_exchange_reentry_probe(
        candle_counts=[80, 80, 82, 82, 83],
        stop_after_waits=3,
    )

    close_index = next(index for index, message in enumerate(messages) if "仓位关闭已确认" in message)
    reentry_index = next(index for index, message in enumerate(messages) if "本波第2次委托" in message)
    self.assertLess(close_index, reentry_index)
    self.assertIn("原开仓ordId=ord-1", messages[close_index])
    self.assertIn("clOrdId=cl-1", messages[close_index])
    self.assertIn("开仓门禁已释放", messages[close_index])
    self.assertIn("结算归因中", messages[close_index])
```

- [ ] **Step 2: Run the test to verify RED**

Run:

```powershell
python -m pytest tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_logs_confirmed_close_before_reentry -q
```

Expected: FAIL because no message contains `仓位关闭已确认`.

- [ ] **Step 3: Implement the minimal engine log helper**

Add to `StrategyEngine`:

```python
def _log_dynamic_position_closed(self, active_order: ManagedEntryOrder) -> None:
    self._logger(
        "仓位关闭已确认"
        f" | 原开仓ordId={active_order.ord_id or '-'}"
        f" | clOrdId={active_order.cl_ord_id or '-'}"
        " | 开仓门禁已释放 | 结算归因中 | 开始评估下一次委托"
    )
```

In each dynamic exchange filled/partially-filled completion path, call this helper after position management returns and before clearing `active_order`. Remove only the corresponding `本轮持仓已结束，继续监控下一次信号。` log call. Do not move waits, candle-baseline refreshes, counters, or `continue` statements.

- [ ] **Step 4: Run the targeted engine tests**

Run:

```powershell
python -m pytest tests/test_strategy_engine.py -q -k "dynamic_exchange_strategy and (reentry or position_closed or waits_next_candle)"
```

Expected: PASS, with existing re-entry counts and wait timing unchanged.

- [ ] **Step 5: Commit Task 1**

```powershell
git add okx_quant/engine.py tests/test_strategy_engine.py
git commit -m "fix: clarify dynamic position closure logs"
```

### Task 2: Rename delayed reconciliation output and preserve legacy parsing

**Files:**
- Modify: `okx_quant/ui_strategy_sessions.py:6895`
- Test: `tests/test_ui.py:1658`
- Test: `tests/test_ui.py:3600`

**Interfaces:**
- Consumes: `StrategyTradeRuntimeState.round_id`, `entry_order_id`, and `entry_client_order_id`.
- Produces: reconciliation summaries beginning with `历史交易结算完成` and runtime parsing that accepts new and legacy closure messages.

- [ ] **Step 1: Write failing reconciliation and parser tests**

Add assertions equivalent to:

```python
self.assertIn("历史交易结算完成", result.attribution_summary)
self.assertIn(f"roundId={trade.round_id}", result.attribution_summary)
self.assertIn(f"原开仓ordId={trade.entry_order_id}", result.attribution_summary)
self.assertNotIn("本轮结束 |", result.attribution_summary)
```

Update the runtime-status test to require:

```python
self.assertEqual(
    _infer_session_runtime_status(
        "仓位关闭已确认 | 原开仓ordId=1001 | clOrdId=cl-1001 | 开仓门禁已释放 | 结算归因中 | 开始评估下一次委托",
        "持仓监控中",
    ),
    "等待信号",
)
```

Add a tracking test proving the new closure marker starts reconciliation while the legacy marker still does so for historical logs.

- [ ] **Step 2: Run tests to verify RED**

Run:

```powershell
python -m pytest tests/test_ui.py -q -k "reconciliation or round_completion or confirmed_close"
```

Expected: FAIL because the new marker is not recognized and the summary still begins with `本轮结束`.

- [ ] **Step 3: Implement settlement terminology and compatibility**

Build the reconciliation summary as:

```python
attribution_summary = (
    "历史交易结算完成"
    f" | roundId={trade.round_id or '-'}"
    f" | 原开仓ordId={trade.entry_order_id or '-'}"
    f" | clOrdId={trade.entry_client_order_id or '-'}"
    f" | 原因={close_reason} | 开仓均价={_format_optional_decimal(entry_price)} | "
    f"平仓均价={_format_optional_decimal(exit_price)} | 数量={_format_optional_decimal(size)} | "
    f"开仓手续费={_format_optional_usdt_precise(entry_fee, places=2)} | "
    f"平仓手续费={_format_optional_usdt_precise(exit_fee, places=2)} | "
    f"资金费={_format_optional_usdt_precise(funding_fee, places=2)} | "
    f"毛盈亏={_format_optional_usdt_precise(gross_pnl, places=2)} | "
    f"净盈亏={_format_optional_usdt_precise(net_pnl, places=2)}"
)
```

Change reconciliation failure text to `历史交易结算失败`. Treat `仓位关闭已确认` as the new terminal/reconciliation marker in runtime-status inference, trade tracking, log restoration, and recoverable-session terminal checks. Retain the legacy marker in all compatibility branches.

- [ ] **Step 4: Run targeted UI tests**

Run:

```powershell
python -m pytest tests/test_ui.py -q -k "reconciliation or runtime_status or restore_session_trade or transition_to_recoverable"
```

Expected: PASS.

- [ ] **Step 5: Run focused regression tests**

Run:

```powershell
python -m pytest tests/test_strategy_engine.py tests/test_ui.py -q
```

Expected: PASS with no changes to order counts, wait timing, position monitoring, or recovery behavior.

- [ ] **Step 6: Commit Task 2**

```powershell
git add okx_quant/ui_strategy_sessions.py tests/test_ui.py
git commit -m "fix: distinguish closure from settlement logs"
```
