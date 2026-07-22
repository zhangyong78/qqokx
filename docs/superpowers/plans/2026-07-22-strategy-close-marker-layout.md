# 实时策略平仓标记布局 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让平仓时间标记以两行展示成交价和本次净盈亏，并让所有时间标记框完整包住单行或多行文本。

**Architecture:** `ui_strategy_sessions.py` 根据 `StrategyTradeLedgerRecord.net_pnl` 生成多行平仓标签。`strategy_live_chart.py` 以逐行宽度、行数和固定内边距计算布局框，再用同一高度将文字垂直居中绘制；现有错行逻辑直接使用新的框尺寸。

**Tech Stack:** Python 3、Tkinter Canvas、`unittest`。

## Global Constraints

- 开仓标签保持单行；平仓标签仅在存在 `net_pnl` 时增加净盈亏第二行。
- 净盈亏正数显式带 `+`，且使用 `format_decimal` 的已有小数格式。
- 宽字符按双列宽度估算；无净盈亏的历史记录仍为单行。
- 不修改成交账本、OKX 请求、时间竖线或非时间标记图层。

---

## File Structure

- `okx_quant/ui_strategy_sessions.py`：生成带净盈亏的平仓标签。
- `okx_quant/strategy_live_chart.py`：计算和绘制自适应的时间标记框。
- `tests/test_ui.py`：覆盖平仓标签的两行内容。
- `tests/test_strategy_live_chart.py`：覆盖中文多行标签的框宽、框高和错行。

### Task 1: 平仓两行标签与自适应时间标记框

**Files:**

- Modify: `okx_quant/ui_strategy_sessions.py:2151-2185`
- Modify: `okx_quant/strategy_live_chart.py:1-45, 754-763, 1104-1175`
- Test: `tests/test_ui.py:1980-2090`
- Test: `tests/test_strategy_live_chart.py:399-460`

**Interfaces:**

- Consumes: `StrategyTradeLedgerRecord.entry_price`, `exit_price`, `net_pnl`。
- Produces: `_strategy_live_chart_event_time_markers(...)` 的平仓 `label`，以及 `_layout_time_marker_label_positions(...)` 的 `(x1, x2, y1, y2)` 自适应框尺寸。

- [ ] **Step 1: 写入失败测试**

在 `tests/test_ui.py` 的成交价标记测试中为账本记录加入 `net_pnl=Decimal("12.34")`，并将平仓预期标签改为：

```python
("close:L01", "平仓 07-17 07:20 | 价格=63771\n本次盈亏=+12.34 USDT")
```

在 `tests/test_strategy_live_chart.py` 加入：

```python
def test_time_marker_layout_sizes_box_for_two_line_chinese_label(self) -> None:
    bounds = _ChartBounds(left=76.0, top=40.0, right=476.0, bottom=544.0)
    candle = Candle(ts=1714330800000, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100"), volume=Decimal("1"), confirmed=True)
    marker = StrategyLiveChartTimeMarker(
        key="close", label="平仓 04-29 10:00 | 价格=64336\n本次盈亏=+12.34 USDT",
        at=datetime.fromtimestamp(candle.ts / 1000), color="#cf222e",
    )
    placement = _layout_time_marker_label_positions((marker,), StrategyLiveChartSnapshot(session_id="S01", candles=(candle,)), bounds, candle_step=80.0)[0]
    self.assertGreaterEqual(placement[3] - placement[2], 190.0)
    self.assertGreaterEqual(placement[5] - placement[4], 40.0)
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_strategy_live_chart_event_time_markers_include_fill_prices tests.test_strategy_live_chart.StrategyLiveChartHelpersTest.test_time_marker_layout_sizes_box_for_two_line_chinese_label`

Expected: FAIL；平仓标签没有第二行，布局框仍固定为 18 像素高且按单字节字符宽度计算。

- [ ] **Step 3: 最小实现标签和尺寸计算**

在 `ui_strategy_sessions.py` 的局部标签函数中，为平仓记录追加净盈亏行：

```python
def format_net_pnl(value: Decimal) -> str:
    return f"{'+' if value > 0 else ''}{format_decimal(value)}"

close_label = label_with_price("平仓", ledger.closed_at, ledger.exit_price)
if ledger.net_pnl is not None:
    close_label = f"{close_label}\n本次盈亏={format_net_pnl(ledger.net_pnl)} USDT"
```

在 `strategy_live_chart.py` 导入 `east_asian_width`，并增加：

```python
def _time_marker_label_size(label: str) -> tuple[float, float]:
    lines = str(label or "").splitlines() or [""]
    text_width = max(sum(14.0 if east_asian_width(char) in {"F", "W"} else 7.0 for char in line) for line in lines)
    return max(text_width + 16.0, 88.0), max(len(lines) * 16.0 + 8.0, 22.0)
```

用该函数替换 `_layout_time_marker_label_positions` 中固定 `text_width` 和 `y2 = y1 + 18.0` 的计算；将错行步长改为当前标签高度加 6 像素。绘制时将 `create_text` 的 y 坐标改为 `(y1 + y2) / 2`，使多行文本在框内垂直居中。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_strategy_live_chart_event_time_markers_include_fill_prices tests.test_strategy_live_chart.StrategyLiveChartHelpersTest.test_time_marker_layout_sizes_box_for_two_line_chinese_label tests.test_strategy_live_chart.StrategyLiveChartHelpersTest.test_time_marker_labels_wrap_to_multiple_rows_when_x_positions_overlap`

Expected: PASS；平仓标签含两行净盈亏，框宽高完整覆盖文本，重叠标记仍分行。

- [ ] **Step 5: 运行相关回归并提交**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui tests.test_strategy_live_chart tests.test_strategy_trade_ledger_backfill -q`

Expected: PASS，无失败。

```powershell
git add okx_quant/ui_strategy_sessions.py okx_quant/strategy_live_chart.py tests/test_ui.py tests/test_strategy_live_chart.py
git commit -m "feat: improve strategy close marker layout"
```

## Plan Self-Review

- Spec coverage: 平仓两行净盈亏、中文宽度、动态高度、错行避让、旧记录单行和既有锚点均由 Task 1 覆盖。
- Placeholder scan: 无未完成项。
- Type consistency: 只使用已有 `Decimal`、`StrategyTradeLedgerRecord`、`StrategyLiveChartTimeMarker` 和 `_ChartBounds`。
