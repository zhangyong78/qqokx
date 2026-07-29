# 交易邮件标题交易方向与事件 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让交易相关邮件标题在不打开正文时明确显示策略、实际做多/做空方向、开仓/平仓以及止盈或止损事件。

**Architecture:** 在 `EmailNotifier` 内增加纯标题上下文辅助方法。成交方向由实际订单 side 推导；平仓方向由反向平仓 side 推导；事件由成交标题或平仓触发原因归类。保留现有 API、会话、交易员标题上下文和正文内容。

**Tech Stack:** Python 3、unittest、现有 `EmailNotifier`。

## Global Constraints

- 仅修改邮件标题构造和对应测试，不改变邮件正文、SMTP 发送、订单与策略执行逻辑。
- 标题必须保留策略名、标的、API、会话与交易员（若已有）。
- 开仓邮件标明 `做多｜开仓` 或 `做空｜开仓`；平仓邮件标明 `做多｜平仓` 或 `做空｜平仓`，止盈/止损须显示为 `平仓-止盈` / `平仓-止损`。

---

### Task 1: 统一交易邮件标题

**Files:**

- Modify: `okx_quant/notifications.py`
- Modify: `tests/test_notifications.py`

**Interfaces:**

- Produces private `EmailNotifier` helpers that map order side to position direction and classify trade event text.
- Updates `send_trade_fill` and `send_trade_close` subjects while preserving their existing public call signatures.

- [ ] **Step 1: Write failing tests**

```python
def test_send_trade_fill_subject_marks_actual_long_open(self) -> None:
    notifier = self._make_notifier()
    notifier.send_trade_fill(..., title="开仓成交", side="buy", ...)
    subject, _ = notifier.notify_async.call_args.args
    self.assertIn("EMA 动态委托 | 做多 | 开仓 | ETH-USDT-SWAP", subject)

def test_send_trade_close_subject_marks_short_stop_loss(self) -> None:
    notifier = self._make_notifier()
    notifier.send_trade_close(..., side="buy", trigger_reason="止损", ...)
    subject, _ = notifier.notify_async.call_args.args
    self.assertIn("EMA 动态委托 | 做空 | 平仓-止损 | ETH-USDT-SWAP", subject)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m unittest tests.test_notifications.EmailNotifierTest.test_send_trade_fill_subject_marks_actual_long_open tests.test_notifications.EmailNotifierTest.test_send_trade_close_subject_marks_short_stop_loss -q`

Expected: FAIL because current subjects have no position direction and use the old generic event layout.

- [ ] **Step 3: Write minimal implementation**

```python
def _position_direction_from_trade_side(side: str, *, closing: bool) -> str:
    normalized = side.strip().lower()
    if closing:
        return {"sell": "做多", "buy": "做空"}.get(normalized, "")
    return {"buy": "做多", "sell": "做空"}.get(normalized, "")

def _trade_event_label(title_or_reason: str, *, closing: bool) -> str:
    text = title_or_reason.strip()
    if closing and "止损" in text:
        return "平仓-止损"
    if closing and "止盈" in text:
        return "平仓-止盈"
    return "平仓" if closing else "开仓"
```

Use these helpers when building `send_trade_fill` and `send_trade_close` subjects, then pass the result through existing `_subject_with_context`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m unittest tests.test_notifications -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add okx_quant/notifications.py tests/test_notifications.py docs/superpowers/plans/2026-07-29-trade-email-subject-context.md
git commit -m "feat: clarify trade email subjects"
```
