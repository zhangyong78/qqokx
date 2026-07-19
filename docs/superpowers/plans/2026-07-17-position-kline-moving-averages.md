# 持仓 K 线均线叠加 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在持仓弹出的合约 K 线窗口中显示 EMA 15 与 SMA 50，且不改变其他复用图表。

**Architecture:** 为共享的 `CandlestickChartView.set_candles` 增加默认关闭的 `show_moving_averages` 参数。参数开启时，图表从现有的 `Candle.close` 构造两个 `QLineSeries`，并与蜡烛图绑定到同一坐标轴；持仓弹窗是唯一开启该参数的调用方。

**Tech Stack:** Python 3、PySide6 QtCharts、unittest。

## Global Constraints

- 只修改持仓弹窗均线显示和其直接测试。
- 不新增网络请求、不改变 K 线数据源、不改主 K 线分析页。
- SMA 50 前 49 根无值，不能以零值或插值连接。
- 每个新增生产行为必须先由定向失败测试证明。

---

### Task 1: 覆盖可选均线叠加

**Files:**
- Modify: `D:/qqokx/tests/test_roll_terminal_qt_windows.py:6229-6279`

**Interfaces:**
- Consumes: `CandlestickChartView.set_candles(*, title: str, candles: list[Candle], hide_wicks: bool = False, show_moving_averages: bool = False)`。
- Produces: 一个验证默认行为、EMA 15、SMA 50 预热规则和均线系列名称的回归测试。

- [x] **Step 1: 写入失败测试**

```python
def test_option_candlestick_chart_only_adds_moving_averages_when_enabled(self) -> None:
    view = CandlestickChartView()
    try:
        candles = [
            Candle(
                ts=1_700_000_000_000 + (index * 3_600_000),
                open=Decimal(index + 1), high=Decimal(index + 2),
                low=Decimal(index), close=Decimal(index + 1),
                volume=Decimal("1"), confirmed=True,
            )
            for index in range(60)
        ]
        view.set_candles(title="test", candles=candles)
        self.assertEqual(len(view.chart().series()), 2)
        view.set_candles(title="test", candles=candles, show_moving_averages=True)
        moving_averages = [series for series in view.chart().series() if isinstance(series, QLineSeries)]
        self.assertEqual([series.name() for series in moving_averages], ["EMA 15", "SMA 50"])
        self.assertEqual(moving_averages[0].count(), 60)
        self.assertEqual(moving_averages[1].count(), 11)
    finally:
        self.__class__.dispose_widget(view)
```

- [x] **Step 2: 运行测试并确认失败**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_option_candlestick_chart_only_adds_moving_averages_when_enabled`

Expected: FAIL，`set_candles()` 尚不接受 `show_moving_averages`。

### Task 2: 实现均线叠加并在持仓窗口启用

**Files:**
- Modify: `D:/qqokx/roll_terminal_qt/option_strategy_window.py:927-1061`
- Modify: `D:/qqokx/roll_terminal_qt/account_positions_home.py:972`
- Test: `D:/qqokx/tests/test_roll_terminal_qt_windows.py:6229-6279`

**Interfaces:**
- Consumes: 有序的 `list[Candle]`，每项的 `ts` 为毫秒时间戳、`close` 为 `Decimal`。
- Produces: `set_candles(..., show_moving_averages: bool = False)`；开启时添加名为 `EMA 15`、`SMA 50` 的两条 `QLineSeries`。

- [x] **Step 1: 写最小计算与绘制实现**

```python
def _build_moving_average_series(candles: list[Candle]) -> tuple[list[Decimal], list[Decimal | None]]:
    closes = [candle.close for candle in candles]
    multiplier = Decimal("2") / Decimal("16")
    ema15: list[Decimal] = []
    sma50: list[Decimal | None] = []
    rolling_total = Decimal("0")
    for index, close in enumerate(closes):
        ema15.append(close if index == 0 else ((close - ema15[-1]) * multiplier) + ema15[-1])
        rolling_total += close
        if index >= 50:
            rolling_total -= closes[index - 50]
        sma50.append(rolling_total / Decimal("50") if index >= 49 else None)
    return ema15, sma50
```

在 `set_candles` 中，仅当 `show_moving_averages` 为真时创建 `QLineSeries`；EMA 追加全部时间点，SMA 仅追加非 `None` 的时间点，设置名称和画笔颜色后加入图表，并与当前 `axis_x`、`axis_y` 绑定。将可见均线值并入 `_fit_y_axis_to_visible_range` 的最小/最大值计算。持仓弹窗调用改为：

```python
self._chart.set_candles(
    title=f"{inst_id} {source_label}K线 | {bar}",
    candles=candles,
    show_moving_averages=True,
)
```

- [x] **Step 2: 运行新增测试并确认通过**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_option_candlestick_chart_only_adds_moving_averages_when_enabled`

Expected: PASS。

- [x] **Step 3: 运行相关回归测试**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_option_candlestick_chart_clears_state_before_chart_rebuild tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_deribit_overlay_moving_average_series_keeps_length_and_ma50_warmup`

Expected: PASS；共享图表重建和既有均线计算不回归。

- [x] **Step 4: 编译与差异检查**

Run: `python -m py_compile roll_terminal_qt/option_strategy_window.py roll_terminal_qt/account_positions_home.py tests/test_roll_terminal_qt_windows.py; git diff --check; git diff --stat`

Expected: 三项成功，无空白错误；差异仅涉及两处生产代码、一处测试和本计划/规格文档。
