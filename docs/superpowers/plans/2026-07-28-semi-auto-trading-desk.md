# 半自动操盘台 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现可以把多个一次性策略任务归入同一操盘组合、自动结束任务并提供连续账本和 K 线复盘的半自动操盘台。

**Architecture:** 新建 `semi_auto_desk.py` 负责组合、任务、持久化快照和统计等纯领域逻辑；新建 `semi_auto_desk_ui.py` 负责窗口。`ui_shell.py` 保存会话和交易账本的组合/任务归属，`ui_strategy_sessions.py` 负责启动任务、监听首次信号/成交/结算并执行冲突保护。K 线复盘复用 `strategy_live_chart.py` 的渲染器，以组合账本而非活动会话生成时间标记。

**Tech Stack:** Python 3、Tkinter、`dataclasses`、JSON 持久化、`unittest`、现有 OKX 策略会话与交易账本。

## Global Constraints

- 半自动任务绝不调用交易员管理台的自动补位、额度格或虚拟止损逻辑。
- 任务在账本结算完成前不得标记为完成或停止会话。
- 同币种同方向仅允许第一笔成交任务继续；其余未成交任务必须走现有安全停止/撤单流程。
- 组合虚拟资金只由初始资金和 `net_pnl` 累计计算；不得混入 API 真实权益。
- 现有普通策略账本和交易员管理台行为保持向后兼容。

---

## File Structure

- `okx_quant/semi_auto_desk.py`：组合/任务数据模型、序列化、统计、冲突和复盘筛选等纯逻辑。
- `okx_quant/semi_auto_desk_ui.py`：半自动操盘台、总账本和组合复盘窗口的 Tkinter 界面。
- `okx_quant/persistence.py`：半自动操盘台 JSON 文件路径和快照读写。
- `okx_quant/ui_shell.py`：应用状态、会话/账本归属字段和窗口入口。
- `okx_quant/ui_strategy_sessions.py`：任务启动、运行状态同步、成交方向锁、结算后自动停止、任务撤销。
- `okx_quant/strategy_live_chart.py`：从组合交易记录生成可复用的复盘时间标记。
- `tests/test_semi_auto_desk.py`：领域、持久化、统计和冲突测试。
- `tests/test_semi_auto_desk_ui.py`：窗口行、总账本和复盘入口测试。
- `tests/test_ui.py`：会话归属、启动、结算与自动停止回归测试。
- `tests/test_persistence.py`：新快照向后兼容读写测试。

### Task 1: 组合、任务、统计和持久化领域模型

**Files:**

- Create: `okx_quant/semi_auto_desk.py`
- Modify: `okx_quant/persistence.py`
- Test: `tests/test_semi_auto_desk.py`
- Test: `tests/test_persistence.py`

**Interfaces:**

- Produces `SemiAutoPoolRecord`, `SemiAutoTaskRecord`, `SemiAutoDeskSnapshot`, `SemiAutoPoolSummary`。
- Produces `build_semi_auto_pool_summary(pool, tasks, ledger_records)` 和 `semi_auto_pool_ledger_records(pool_id, ledger_records)`。
- Produces `load_semi_auto_desk_snapshot(path=None)` 和 `save_semi_auto_desk_snapshot(snapshot, path=None)`。

- [ ] **Step 1: 写入失败测试**

```python
def test_pool_summary_uses_only_matching_pool_realized_pnl(self) -> None:
    pool = SemiAutoPoolRecord(pool_id="P001", name="主操盘", api_name="real", initial_capital=Decimal("1000"))
    records = [
        _ledger("L1", "P001", Decimal("20")),
        _ledger("L2", "P001", Decimal("-10")),
        _ledger("L3", "P002", Decimal("999")),
    ]

    summary = build_semi_auto_pool_summary(pool, [], records)

    self.assertEqual(summary.net_pnl, Decimal("10"))
    self.assertEqual(summary.virtual_equity, Decimal("1010"))
    self.assertEqual(summary.win_rate, Decimal("50"))
    self.assertEqual(summary.profit_loss_ratio, Decimal("2"))
```

```python
def test_snapshot_round_trip_keeps_task_mode_and_terminal_reason(self) -> None:
    snapshot = SemiAutoDeskSnapshot(
        pools=[SemiAutoPoolRecord(pool_id="P001", name="主操盘", api_name="real", initial_capital=Decimal("1000"))],
        tasks=[SemiAutoTaskRecord(task_id="P001-1", pool_id="P001", template_payload={"strategy_id": "ema"}, mode="wait_one", status="completed_closed", ended_reason="止盈")],
    )
    save_semi_auto_desk_snapshot(snapshot, self.path)

    restored = load_semi_auto_desk_snapshot(self.path)

    self.assertEqual(restored.tasks[0].mode, "wait_one")
    self.assertEqual(restored.tasks[0].ended_reason, "止盈")
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_semi_auto_desk tests.test_persistence -q`

Expected: FAIL，因为模块、模型和持久化函数尚不存在。

- [ ] **Step 3: 实现最小领域模型和 JSON 快照**

在 `semi_auto_desk.py` 定义状态常量、三个数据类、十进制/时间序列化和统计函数；在 `persistence.py` 增加 `SEMI_AUTO_DESK_FILE_NAME`、`semi_auto_desk_file_path` 及 JSON 读写。读旧文件或缺字段时返回空组合/任务或字段默认值，不修改普通策略账本文件。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_semi_auto_desk tests.test_persistence -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```powershell
git add okx_quant/semi_auto_desk.py okx_quant/persistence.py tests/test_semi_auto_desk.py tests/test_persistence.py
git commit -m "feat: add semi-auto desk domain model"
```

### Task 2: 会话和交易账本的组合归属

**Files:**

- Modify: `okx_quant/ui_shell.py:1038-1150,3398-3420,4019-4026,8589-8620`
- Modify: `okx_quant/ui_strategy_sessions.py:4006-4091,7049-7085,10166-10240`
- Test: `tests/test_ui.py`

**Interfaces:**

- Extends `StrategySession` with `semi_auto_pool_id`, `semi_auto_task_id`, `semi_auto_mode`.
- Extends `StrategyTradeLedgerRecord` with `semi_auto_pool_id`, `semi_auto_task_id`.
- Extends `_start_strategy_session(..., semi_auto_pool_id="", semi_auto_task_id="", semi_auto_mode="")`.

- [ ] **Step 1: 写入失败测试**

```python
def test_trade_ledger_payload_round_trip_keeps_semi_auto_task_identity(self) -> None:
    record = StrategyTradeLedgerRecord(..., semi_auto_pool_id="P001", semi_auto_task_id="P001-1")
    restored = QuantApp._trade_ledger_record_from_payload(QuantApp._trade_ledger_payload(record))

    self.assertEqual(restored.semi_auto_pool_id, "P001")
    self.assertEqual(restored.semi_auto_task_id, "P001-1")
```

```python
def test_start_strategy_session_keeps_semi_auto_context_on_session(self) -> None:
    session_id = app._start_strategy_session(..., semi_auto_pool_id="P001", semi_auto_task_id="P001-1", semi_auto_mode="wait_one")

    self.assertEqual(app.sessions[session_id].semi_auto_pool_id, "P001")
    self.assertEqual(app.sessions[session_id].semi_auto_task_id, "P001-1")
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_trade_ledger_payload_round_trip_keeps_semi_auto_task_identity tests.test_ui.UiHelpersTest.test_start_strategy_session_keeps_semi_auto_context_on_session`

Expected: FAIL，因为账本和会话尚无半自动归属字段。

- [ ] **Step 3: 最小实现归属字段与序列化**

给两个数据类增加默认空字符串字段；在账本载入/保存和结算记录创建处透传会话归属；在启动会话方法中接收并写入三个半自动上下文字段。应用启动时加载半自动快照到独立成员，不触碰交易员管理台快照。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```powershell
git add okx_quant/ui_shell.py okx_quant/ui_strategy_sessions.py tests/test_ui.py
git commit -m "feat: link strategy ledger to semi-auto tasks"
```

### Task 3: 一次性任务协调、成交锁和结算后停止

**Files:**

- Modify: `okx_quant/ui_strategy_sessions.py:6158-6320,6438-6468,7160-7256`
- Modify: `okx_quant/semi_auto_desk.py`
- Test: `tests/test_ui.py`
- Test: `tests/test_semi_auto_desk.py`

**Interfaces:**

- Produces `_start_semi_auto_task(task_id)`, `_cancel_semi_auto_task(task_id)` 和 `_apply_semi_auto_task_settlement(session, ledger_record)`。
- Produces `_semi_auto_same_direction_conflicts(task)`，仅返回同组合/同 API/同币种/同方向且未结束的任务。

- [ ] **Step 1: 写入失败测试**

```python
def test_single_check_task_stops_when_first_evaluation_reports_no_entry(self) -> None:
    task = _task(mode="evaluate_once", status="running", session_id="S01")
    app._semi_auto_desk_tasks = [task]
    app.sessions = {"S01": _session(semi_auto_task_id=task.task_id)}

    QuantApp._apply_semi_auto_runtime_message(app, app.sessions["S01"], "当前无法生成挂单 | 条件未满足")

    self.assertEqual(task.status, "completed_no_signal")
    app._request_stop_strategy_session.assert_called_once()
```

```python
def test_first_opened_task_blocks_unfilled_same_symbol_direction_task(self) -> None:
    entered = _task(task_id="P001-1", session_id="S01", symbol="ETH-USDT-SWAP", direction="只做多")
    waiting = _task(task_id="P001-2", session_id="S02", symbol="ETH-USDT-SWAP", direction="只做多")

    QuantApp._apply_semi_auto_task_opened(app, entered)

    self.assertEqual(waiting.status, "blocked_conflict")
    app._request_stop_strategy_session.assert_called_once_with("S02", ended_reason="仓位冲突未执行", source_label="半自动操盘台仓位冲突", show_dialog=False)
```

```python
def test_settlement_marks_wait_one_task_completed_then_stops_session(self) -> None:
    task = _task(mode="wait_one", status="settling", session_id="S01")
    ledger = _ledger("L01", "P001", Decimal("5"), task_id=task.task_id)

    QuantApp._apply_semi_auto_task_settlement(app, _session(semi_auto_task_id=task.task_id), ledger)

    self.assertEqual(task.status, "completed_closed")
    app._request_stop_strategy_session.assert_called_once()
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui.UiHelpersTest.test_single_check_task_stops_when_first_evaluation_reports_no_entry tests.test_ui.UiHelpersTest.test_first_opened_task_blocks_unfilled_same_symbol_direction_task tests.test_ui.UiHelpersTest.test_settlement_marks_wait_one_task_completed_then_stops_session`

Expected: FAIL，因为协调方法和任务状态转换尚不存在。

- [ ] **Step 3: 实现最小协调逻辑**

在会话运行消息接收处调用半自动协调器。`evaluate_once` 看到首个“当前无法生成挂单”运行事件时标记无信号并走现有安全停止流程；`wait_one` 保持运行。开仓状态首次同步时建立方向锁并停止相同 API、币种、方向的未成交任务。结算写入账本后才完成任务并请求停止对应会话。每次状态改变都保存半自动快照并写入组合事件日志。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui tests.test_semi_auto_desk -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```powershell
git add okx_quant/ui_strategy_sessions.py okx_quant/semi_auto_desk.py tests/test_ui.py tests/test_semi_auto_desk.py
git commit -m "feat: coordinate one-shot semi-auto tasks"
```

### Task 4: 操盘台、任务列表、总账本和统计界面

**Files:**

- Create: `okx_quant/semi_auto_desk_ui.py`
- Modify: `okx_quant/ui_shell.py:8589-8620` 
- Test: `tests/test_semi_auto_desk_ui.py`

**Interfaces:**

- Produces `SemiAutoDeskWindow`。
- Consumes pool/task snapshot provider、当前策略模板工厂、任务启动/取消回调、账本记录提供者和会话日志打开器。
- Produces `build_semi_auto_task_rows(...)`、`build_semi_auto_pool_ledger_rows(...)`。

- [ ] **Step 1: 写入失败测试**

```python
def test_pool_ledger_rows_mix_strategies_but_keep_one_pool_only(self) -> None:
    rows = build_semi_auto_pool_ledger_rows("P001", [_ledger("L1", "P001", Decimal("2"), strategy="EMA"), _ledger("L2", "P001", Decimal("-1"), strategy="斜率"), _ledger("L3", "P002", Decimal("9"))])

    self.assertEqual([row[1][2] for row in rows], ["EMA", "斜率"])
```

```python
def test_task_rows_show_one_shot_mode_and_terminal_reason(self) -> None:
    rows = build_semi_auto_task_rows([_task(mode="wait_one", status="completed_closed", ended_reason="止盈")])

    self.assertEqual(rows[0][1][5], "等待一单")
    self.assertEqual(rows[0][1][-1], "止盈")
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_semi_auto_desk_ui -q`

Expected: FAIL，因为窗口和行构建函数尚不存在。

- [ ] **Step 3: 实现最小窗口与菜单入口**

实现组合列表和任务列表；“加入当前策略”从现有启动器捕获策略配置快照，用户可更改启动器配置后重复加入；任务模式可选“单次判断”或“等待一单”。提供启动选中、取消未开仓、打开总账本和打开日志按钮。总账本显示组合统计、资金曲线数据摘要、策略/币种分组和交易明细。`ui_shell.py` 菜单新增“半自动操盘台”，并在应用关闭时销毁窗口引用。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_semi_auto_desk_ui tests.test_ui -q`

Expected: PASS。

- [ ] **Step 5: 提交**

```powershell
git add okx_quant/semi_auto_desk_ui.py okx_quant/ui_shell.py tests/test_semi_auto_desk_ui.py
git commit -m "feat: add semi-auto trading desk window"
```

### Task 5: 按币种叠加交易记录的组合 K 线复盘

**Files:**

- Modify: `okx_quant/strategy_live_chart.py`
- Modify: `okx_quant/semi_auto_desk_ui.py`
- Test: `tests/test_strategy_live_chart.py`
- Test: `tests/test_semi_auto_desk_ui.py`

**Interfaces:**

- Produces `build_semi_auto_replay_time_markers(records)`，返回按开仓/平仓时间排序的 `StrategyLiveChartTimeMarker`。
- Consumes同一组合、同一币种的交易账本记录。

- [ ] **Step 1: 写入失败测试**

```python
def test_semi_auto_replay_markers_include_all_strategies_and_directions(self) -> None:
    markers = build_semi_auto_replay_time_markers([
        _ledger("L1", "P001", Decimal("2"), strategy="EMA", direction="只做多"),
        _ledger("L2", "P001", Decimal("-1"), strategy="斜率", direction="只做空"),
    ])

    self.assertEqual([item.key for item in markers], ["open:L1", "close:L1", "open:L2", "close:L2"])
    self.assertIn("EMA", markers[0].label)
    self.assertIn("斜率", markers[2].label)
    self.assertIn("本次盈亏=-1.00 USDT", markers[3].label)
```

- [ ] **Step 2: 运行测试并确认失败**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_strategy_live_chart.StrategyLiveChartHelpersTest.test_semi_auto_replay_markers_include_all_strategies_and_directions`

Expected: FAIL，因为组合复盘标记构建函数尚不存在。

- [ ] **Step 3: 实现标记构建和复盘窗口入口**

从选中组合和币种筛选已结算账本，按成交时间生成开仓/平仓标记；标签包含策略、方向、时间、价格和两位小数净盈亏。复盘窗口读取所选任务周期的 K 线并复用 `render_strategy_live_chart` 渲染；没有活动会话时仍可打开。

- [ ] **Step 4: 运行测试并确认通过**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_strategy_live_chart tests.test_semi_auto_desk_ui -q`

Expected: PASS。

- [ ] **Step 5: 运行相关完整回归并提交**

Run: `.\\.venv\\Scripts\\python.exe -m unittest tests.test_ui tests.test_strategy_live_chart tests.test_trader_desk tests.test_trader_desk_ui tests.test_semi_auto_desk tests.test_semi_auto_desk_ui tests.test_persistence -q`

Expected: PASS，且现有交易员管理台与普通策略账本测试不回归。

```powershell
git add okx_quant/strategy_live_chart.py okx_quant/semi_auto_desk_ui.py tests/test_strategy_live_chart.py tests/test_semi_auto_desk_ui.py
git commit -m "feat: add semi-auto pool chart replay"
```

## Plan Self-Review

- Spec coverage: 一次性两种模式、同向首单锁、任务结束、连续组合账本、虚拟资金统计、跨策略/币种、复盘和与交易员管理台隔离分别由 Task 1 至 Task 5 覆盖。
- Placeholder scan: 无 TBD、TODO 或待定接口。
- Type consistency: 组合和任务主键为字符串；会话和账本使用同名 `semi_auto_pool_id`/`semi_auto_task_id`；纯领域层不依赖 Tkinter 或 OKX 客户端。
