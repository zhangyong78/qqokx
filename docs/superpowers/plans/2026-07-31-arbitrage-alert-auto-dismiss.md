# 套利机会提醒自动关闭 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 防止套利机会超阈值提醒窗口重叠，并让未处理的提醒在 10 秒后自动关闭。

**Architecture:** `ArbitrageWindow` 保存当前非阻塞 Tk 提醒窗口的引用。扫描命中阈值时，若该窗口仍存在则跳过创建；新窗口通过 `after(10000, ...)` 关闭，并在手动或定时关闭时清除引用。

**Tech Stack:** Python 3.11、Tkinter、unittest、unittest.mock。

## Global Constraints

- 仅修改套利机会扫描提醒；不得修改扫描频率、阈值计算、自动下单或自动交易。
- 同时最多一个提醒窗口，存活时间固定为 10 秒。
- 所有 Tk 操作均在已有的界面回调线程执行。

---

### Task 1: 单一且自动关闭的机会提醒

**Files:**

- Modify: `okx_quant/arbitrage_ui.py:46, 709-710, 3007-3026`
- Modify: `tests/test_arbitrage.py:1-45, ArbitrageWindow 测试区`

**Interfaces:**

- Consumes: `ArbitrageWindow._maybe_alert(rows)`、`ArbitrageWindow._widget_exists(widget)`。
- Produces: `ArbitrageWindow._alert_window`，当前 `Toplevel | None`；`ALERT_DISMISS_MS = 10_000`。

- [x] **Step 1: Write the failing test**

```python
def test_maybe_alert_skips_new_popup_while_existing_popup_is_open(self) -> None:
    window = _alert_test_window(active_popup=True)
    rows = [_alert_row()]

    with patch("okx_quant.arbitrage_ui.Toplevel") as popup_type:
        window._maybe_alert(rows)

    popup_type.assert_not_called()


def test_maybe_alert_auto_closes_popup_and_allows_later_popup(self) -> None:
    window = _alert_test_window()
    scheduled: dict[int, object] = {}
    popup = MagicMock()
    popup.winfo_exists.return_value = True
    popup.after.side_effect = lambda delay, callback: scheduled.__setitem__(delay, callback)

    with patch("okx_quant.arbitrage_ui.Toplevel", return_value=popup):
        window._maybe_alert([_alert_row()])
        scheduled[10_000]()

    popup.destroy.assert_called_once()
    self.assertIsNone(window._alert_window)
```

- [x] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_arbitrage.py -k maybe_alert -v`

Expected: FAIL because the current code creates a blocking `messagebox.showinfo` and has neither `_alert_window` nor a 10-second close callback.

- [x] **Step 3: Write minimal implementation**

```python
ALERT_DISMISS_MS = 10_000

def _close_alert() -> None:
    if self._alert_window is alert:
        self._alert_window = None
    if self._widget_exists(alert):
        alert.destroy()

if self._widget_exists(self._alert_window):
    return
alert = Toplevel(self.window)
self._alert_window = alert
alert.protocol("WM_DELETE_WINDOW", _close_alert)
alert.after(ALERT_DISMISS_MS, _close_alert)
```

Create the small `Toplevel` content using existing `ttk.Label` and `ttk.Button`; the button uses `_close_alert`. Do not change scan scheduling or threshold logic.

- [x] **Step 4: Run focused test to verify it passes**

Run: `python -m pytest tests/test_arbitrage.py -k maybe_alert -v`

Expected: PASS for both tests.

- [x] **Step 5: Run regression tests and commit**

Run: `python -m pytest tests/test_arbitrage.py -v`

Expected: PASS with no failures.

```bash
git add okx_quant/arbitrage_ui.py tests/test_arbitrage.py docs/superpowers/plans/2026-07-31-arbitrage-alert-auto-dismiss.md
git commit -m "fix: auto-dismiss arbitrage opportunity alerts"
```
