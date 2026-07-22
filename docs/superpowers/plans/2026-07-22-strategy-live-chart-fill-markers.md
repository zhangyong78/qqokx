# 实时策略 K 线成交标记 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在实时策略 K 线中标出每笔开仓/平仓的成交时间和成交价，并在成交日志中记录明确的成交时间。

**Architecture:** 图表只读取已持久化的 `StrategyTradeLedgerRecord`，扩展开/平仓时间标记的文本，不增加 OKX 请求。两个会话日志入口在写入前为成交消息补充时间，不改变既有运行时账本追踪接口。

**Tech Stack:** Python 3、Tkinter Canvas、`unittest`、`pytest`。

## Global Constraints

- 只修改实时策略图表标记和会话成交日志。
- 价格使用既有 `format_decimal`；缺失价格的历史记录只显示时间。
- 不补拉或改写历史成交数据。

---

## File Structure

- `okx_quant/ui_strategy_sessions.py`：图表标记和会话日志入口。
- `tests/test_ui.py`：标记文本与成交日志字段的回归测试。

### Task 1: 图表开仓和平仓标签显示成交价

**Files:**

- Modify: `okx_quant/ui_strategy_sessions.py:1-20, 2151-2185`
- Test: `tests/test_ui.py:1915-2028`

**Interfaces:**

- Consumes: `StrategyTradeLedgerRecord.opened_at`, `closed_at`, `entry_price`, `exit_price`。
- Produces: `_strategy_live_chart_event_time_markers(...)` 返回的开/平仓标记标签含可用成交价。

- [ ] **Step 1: 写入失败测试**

```python
def test_strategy_live_chart_event_time_markers_include_fill_prices(self) -> None:
    opened_at = datetime(2026, 7, 16, 15, 36)
    closed_at = datetime(2026, 7, 17, 7, 20)
    app = QuantApp.__new__(QuantApp)
    app._strategy_trade_ledger_records = [
        StrategyTradeLedgerRecord(
            record_id="L01", history_record_id="H01", session_id="S01",
            api_name="moni", strategy_id="ema55_slope_short", strategy_name="EMA55",
            symbol="BTC-USDT-SWAP", direction_label="SHORT_ONLY", run_mode_label="TRADE",
            environment="demo", opened_at=opened_at, closed_at=closed_at,
            entry_price=Decimal("64937.00"), exit_price=Decimal("63771.00"),
        )
    ]
    app._credentials_for_profile_or_none = lambda _profile_name: None
    markers = QuantApp._strategy_live_chart_event_time_markers(
        app, SimpleNamespace(session_id="S01", history_record_id="H01", api_name="moni", active_trade=None),
        "BTC-USDT-SWAP",
    )
    self.assertEqual([(m.key, m.label) for m in markers], [
        ("open:L01", "开仓 07-16 15:36 | 价格=64937"),
        ("close:L01", "平仓 07-17 07:20 | 价格=63771"),
    ])
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_strategy_live_chart_event_time_markers_include_fill_prices`

Expected: FAIL；现有标签只含时间。

- [ ] **Step 3: 最小实现**

在 `okx_quant/ui_strategy_sessions.py` imports 中添加：

```python
from okx_quant.pricing import format_decimal
```

在 `_strategy_live_chart_event_time_markers` 中添加，并替换开、平仓的 `label=`：

```python
def label_with_price(action: str, at: datetime, price: Decimal | None) -> str:
    label = f"{action} {at.strftime('%m-%d %H:%M')}"
    return f"{label} | 价格={format_decimal(price)}" if price is not None else label

label=label_with_price("开仓", ledger.opened_at, ledger.entry_price)
label=label_with_price("平仓", ledger.closed_at, ledger.exit_price)
```

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_strategy_live_chart_event_time_markers_include_fill_prices tests.test_ui.UiHelpersTest.test_strategy_live_chart_event_time_markers_anchor_by_trade_direction`

Expected: PASS；新标签显示价格，原有多空锚点不变。

- [ ] **Step 5: 提交本任务**

```powershell
git add tests/test_ui.py okx_quant/ui_strategy_sessions.py
git commit -m "feat: show fill prices on strategy chart markers"
```

### Task 2: 会话成交日志记录成交时间

**Files:**

- Modify: `okx_quant/ui_strategy_sessions.py:6119-6144, 6265-6295`
- Test: `tests/test_ui.py:3650-3810`

**Interfaces:**

- Consumes: `_log_session_message(session, message)`、`_make_session_logger(...)(message)` 的成交消息与 `datetime`。
- Produces: `_with_session_fill_time(message, observed_at) -> str`，为含 `已成交` 的消息添加一次 `成交时间=`。

- [ ] **Step 1: 写入失败测试**

```python
def test_with_session_fill_time_adds_time_only_to_fill_messages(self) -> None:
    observed_at = datetime(2026, 7, 16, 15, 36, 9)
    entry = QuantApp._with_session_fill_time(
        "本地下单成交 | ordId=1001 | 成交均价=64937", observed_at=observed_at
    )
    close = QuantApp._with_session_fill_time(
        "本地止损平仓已成交 | ordId=2001 | 成交均价=63771", observed_at=observed_at
    )
    duplicate = QuantApp._with_session_fill_time(
        "挂单已成交 | 成交时间=2026-07-16 15:35:00", observed_at=observed_at
    )
    ordinary = QuantApp._with_session_fill_time("正在读取K线", observed_at=observed_at)
    self.assertTrue(entry.endswith("成交时间=2026-07-16 15:36:09"))
    self.assertTrue(close.endswith("成交时间=2026-07-16 15:36:09"))
    self.assertEqual(duplicate, "挂单已成交 | 成交时间=2026-07-16 15:35:00")
    self.assertEqual(ordinary, "正在读取K线")
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.StrategyTradeTrackingTest.test_with_session_fill_time_adds_time_only_to_fill_messages`

Expected: FAIL；`_with_session_fill_time` 尚不存在。

- [ ] **Step 3: 最小实现**

在 `_log_session_message` 前添加：

```python
@staticmethod
def _with_session_fill_time(message: str, *, observed_at: datetime) -> str:
    text = str(message or "").strip()
    if not text or "已成交" not in text or "成交时间=" in text:
        return text
    return f"{text} | 成交时间={observed_at.strftime('%Y-%m-%d %H:%M:%S')}"
```

将 `_log_session_message` 开头改为：

```python
message = self._with_session_fill_time(message, observed_at=datetime.now())
self._record_session_runtime_message(session.session_id, message)
```

并在 `_make_session_logger` 的内部 `_logger` 开头使用同一行消息转换，再调用既有 `_record_session_runtime_message(session_id, message)`。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.StrategyTradeTrackingTest.test_with_session_fill_time_adds_time_only_to_fill_messages tests.test_ui.StrategyTradeTrackingTest.test_track_session_trade_runtime_starts_reconciliation_after_local_terminal_message`

Expected: PASS；成交日志有唯一时间字段，普通日志未改变，运行时开/平仓记录仍正确。

- [ ] **Step 5: 全量相关回归并提交本任务**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui tests.test_strategy_trade_ledger_backfill -q`

Expected: PASS，无失败。

```powershell
git add tests/test_ui.py okx_quant/ui_strategy_sessions.py
git commit -m "feat: record fill time in strategy logs"
```

## Plan Self-Review

- Spec coverage: Task 1 覆盖开/平仓成交价与时间标记及无价格兼容；Task 2 覆盖开仓、平仓成交日志的明确时间字段。
- Placeholder scan: 无未完成项。
- Type consistency: 只使用已有 `datetime`、`Decimal`、`StrategyTradeLedgerRecord` 和 `StrategyLiveChartTimeMarker`。

## Execution Note

当前 `pyproject.toml` 以 UTF-8 BOM 开头，Python 3.11 的 `tomllib` 无法解析，`pytest` 因而不能加载。实现验证改用同一虚拟环境中的 `unittest`，不修改本次范围外的项目配置。
