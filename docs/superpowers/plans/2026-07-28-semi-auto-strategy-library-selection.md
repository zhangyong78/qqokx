# 半自动操盘台内置策略库选择 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让半自动操盘台直接选择内置实盘策略、配置本次参数并创建一次性任务，不再使用主策略工作台的当前策略。

**Architecture:** 新增独立策略库/参数草稿弹窗，策略来源是 `STRATEGY_DEFINITIONS`。应用层为所选策略构建隔离的 `StrategyConfig`，复用既有默认值、固定参数、校验、模板序列化和一次性任务启动；不读取或切换主策略工作台的 Tk 变量。

**Tech Stack:** Python 3、Tkinter、dataclasses、现有 `StrategyConfig` / `StrategyDefinition`、`unittest`。

## Global Constraints

- 只展示支持正常实盘启动的内置策略；不加载或执行外部 Python 脚本。
- 每条任务保存完整参数快照；后续修改策略库或主窗口不影响已加入任务。
- 保留 `wait_one`、`evaluate_once`、成交锁、结算、账本、复盘和重启安全规则。
- 移除半自动窗口的“加入当前策略”入口；普通实盘启动和交易员管理台不改变。

---

## File Structure

- `okx_quant/semi_auto_strategy_library_ui.py`：策略列表、独立参数草稿弹窗和纯展示辅助函数。
- `okx_quant/semi_auto_desk_ui.py`：替换操作栏入口并调用策略库弹窗。
- `okx_quant/ui_strategy_sessions.py`：内置策略过滤、独立配置构建与模板快照。
- `okx_quant/ui_shell.py`：注入策略库回调。
- `tests/test_semi_auto_strategy_library_ui.py`：策略过滤和参数载荷测试。
- `tests/test_semi_auto_desk_ui.py`、`tests/test_ui.py`：窗口接入和应用层回归测试。

### Task 1: 内置策略和隔离配置接口

**Files:**

- Modify: `okx_quant/ui_strategy_sessions.py`
- Modify: `okx_quant/ui_shell.py`
- Test: `tests/test_ui.py`

**Interfaces:**

- Produces `QuantApp.semi_auto_strategy_definitions() -> tuple[StrategyDefinition, ...]`。
- Produces `QuantApp.semi_auto_strategy_parameter_defaults(strategy_id: str) -> dict[str, object]`。
- Produces `QuantApp.build_semi_auto_strategy_template(strategy_id: str, parameter_values: dict[str, object], api_name: str) -> StrategyTemplateRecord`。

- [ ] **Step 1: Write the failing tests**

```python
def test_semi_auto_strategy_definitions_only_return_launchable_builtin_strategies(self) -> None:
    entries = QuantApp.semi_auto_strategy_definitions(SimpleNamespace())
    self.assertTrue(entries)
    self.assertTrue(all(item.supports_trader_desk for item in entries))
    self.assertIn("EMA 动态委托做多", [item.name for item in entries])


def test_library_template_ignores_current_launcher_selection(self) -> None:
    app = self._semi_auto_template_app(current_strategy="EMA55 斜率做空")
    record = QuantApp.build_semi_auto_strategy_template(
        app,
        STRATEGY_DYNAMIC_LONG_ID,
        {"symbol": "ETH-USDT-SWAP", "bar": "1H", "signal_mode": "long_only"},
        "real",
    )
    self.assertEqual(record.strategy_id, STRATEGY_DYNAMIC_LONG_ID)
    self.assertEqual(record.symbol, "ETH-USDT-SWAP")
    self.assertEqual(record.config.run_mode, "trade")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m unittest tests.test_ui.UiHelpersTest.test_semi_auto_strategy_definitions_only_return_launchable_builtin_strategies tests.test_ui.UiHelpersTest.test_library_template_ignores_current_launcher_selection -q`

Expected: FAIL because the three library interfaces do not exist.

- [ ] **Step 3: Write the minimal implementation**

```python
def semi_auto_strategy_definitions(self) -> tuple[StrategyDefinition, ...]:
    return tuple(item for item in STRATEGY_DEFINITIONS if item.supports_trader_desk)


def build_semi_auto_strategy_template(self, strategy_id, parameter_values, api_name):
    definition = get_strategy_definition(strategy_id)
    if definition not in self.semi_auto_strategy_definitions():
        raise ValueError(f"{definition.name} 暂不支持半自动实盘任务。")
    config = self._build_strategy_config_from_isolated_values(
        definition, parameter_values, api_name=api_name, run_mode="trade"
    )
    return self._strategy_template_record_from_config(definition, config, api_name=api_name)
```

`_build_strategy_config_from_isolated_values` 以 `iter_strategy_parameter_keys(strategy_id)` 的默认值和弹窗值为来源，并由 `strategy_fixed_value` 覆盖固定参数；它不得读取 `self.strategy_name` 或其他主窗口 Tk 变量。

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m unittest tests.test_ui -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add okx_quant/ui_strategy_sessions.py okx_quant/ui_shell.py tests/test_ui.py
git commit -m "feat: expose semi-auto strategy library templates"
```

### Task 2: 策略库和参数草稿弹窗

**Files:**

- Create: `okx_quant/semi_auto_strategy_library_ui.py`
- Test: `tests/test_semi_auto_strategy_library_ui.py`

**Interfaces:**

- Produces `build_semi_auto_strategy_library_rows(definitions)`。
- Produces `build_semi_auto_strategy_parameter_payload(strategy_id, *, api_name, values)`。
- Produces `SemiAutoStrategyLibraryDialog(parent, *, definitions, initial_api_name, parameter_defaults_provider, template_builder, on_confirm)`。

- [ ] **Step 1: Write the failing tests**

```python
def test_strategy_library_rows_only_include_launchable_builtin_definitions(self) -> None:
    rows = build_semi_auto_strategy_library_rows(
        (_definition("ema", "EMA 做多", True), _definition("research", "研究策略", False))
    )
    self.assertEqual(rows, [("ema", ("EMA 做多", "只做多", ""))])


def test_parameter_payload_keeps_selected_strategy_and_dialog_values(self) -> None:
    payload = build_semi_auto_strategy_parameter_payload(
        "ema", api_name="real", values={"symbol": "BTC-USDT-SWAP", "bar": "1H"}
    )
    self.assertEqual(payload["strategy_id"], "ema")
    self.assertEqual(payload["api_name"], "real")
    self.assertEqual(payload["symbol"], "BTC-USDT-SWAP")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m unittest tests.test_semi_auto_strategy_library_ui -q`

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Write the minimal implementation**

```python
def build_semi_auto_strategy_library_rows(definitions):
    return [
        (item.strategy_id, (item.name, item.default_signal_label, item.summary))
        for item in definitions if item.supports_trader_desk
    ]


def build_semi_auto_strategy_parameter_payload(strategy_id, *, api_name, values):
    return {"strategy_id": strategy_id, "api_name": api_name, **dict(values)}
```

弹窗上半部分用 `Treeview` 展示策略名称、默认方向和说明。下半部分持有独立 `StringVar` / `BooleanVar`，显示 API、币种、周期、方向、持仓模式、数量/风险、止损止盈和策略专属字段；切换策略时仅从 `parameter_defaults_provider` 重建草稿。点击“加入一次性任务”调用 `template_builder`；校验异常显示 `messagebox.showerror` 且不调用 `on_confirm`。

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m unittest tests.test_semi_auto_strategy_library_ui -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add okx_quant/semi_auto_strategy_library_ui.py tests/test_semi_auto_strategy_library_ui.py
git commit -m "feat: add semi-auto strategy library dialog"
```

### Task 3: 半自动操盘台接入

**Files:**

- Modify: `okx_quant/semi_auto_desk_ui.py`
- Modify: `okx_quant/ui_shell.py`
- Test: `tests/test_semi_auto_desk_ui.py`
- Test: `tests/test_ui.py`

**Interfaces:**

- `SemiAutoDeskWindow` 删除 `current_template_factory` 和 `template_serializer` 回调。
- 新增 `strategy_library_opener(pool_id: str, mode: str) -> None` 回调。
- 新增 `QuantApp.open_semi_auto_strategy_library(pool_id: str, mode: str) -> None`。

- [ ] **Step 1: Write the failing tests**

```python
def test_strategy_library_action_passes_selected_pool_and_one_shot_mode(self) -> None:
    opener = MagicMock()
    window = _desk_window_without_tk(strategy_library_opener=opener)
    window._selected_pool_id = "P001"
    window.mode_var = _Var("等待一单")
    window._open_strategy_library()
    opener.assert_called_once_with("P001", "wait_one")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv\Scripts\python.exe -m unittest tests.test_semi_auto_desk_ui tests.test_ui -q`

Expected: FAIL because the old current-strategy callback is still the only add path.

- [ ] **Step 3: Write the minimal implementation**

```python
def _open_strategy_library(self) -> None:
    if not self._selected_pool_id:
        messagebox.showinfo("提示", "请先选择操盘组合。", parent=self.window)
        return
    mode = {"等待一单": "wait_one", "单次判断": "evaluate_once"}[self.mode_var.get()]
    self._strategy_library_opener(self._selected_pool_id, mode)
```

替换按钮文本为“从策略库添加”，删除 `_add_current_strategy`。应用层弹窗确认回调调用 `_build_strategy_template_payload_from_record(record)`，然后调用既有 `add_semi_auto_task(pool_id, payload, mode)` 并刷新操盘台。

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv\Scripts\python.exe -m unittest tests.test_semi_auto_desk tests.test_semi_auto_desk_ui tests.test_ui tests.test_strategy_live_chart -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add okx_quant/semi_auto_desk_ui.py okx_quant/ui_shell.py tests/test_semi_auto_desk_ui.py tests/test_ui.py
git commit -m "feat: add library strategies to semi-auto desk"
```

### Task 4: 最终验证与人工验收

**Files:**

- Modify: `docs/superpowers/specs/2026-07-28-semi-auto-strategy-library-selection-design.md`（仅在实现与设计不一致时）

- [ ] **Step 1: Run the full test suite**

Run: `.venv\Scripts\python.exe -m unittest discover -s tests -q`

Expected: exit code 0.

- [ ] **Step 2: Perform the manual acceptance path**

1. 创建或选择半自动操盘组合。
2. 点击“从策略库添加”，选择“EMA 动态委托做多”，加入 ETH-USDT-SWAP 1H 的等待一单任务。
3. 再选择“EMA55 斜率做空”，加入 BTC-USDT-SWAP 的单次判断任务。
4. 确认主策略工作台的当前选择未参与任务创建，且两条任务显示在列表。
5. 启动任务后确认仍沿用一次性结算、账本和复盘行为。

- [ ] **Step 3: Check workspace scope**

Run: `git diff --check` and `git status --short`

Expected: 不暂存现有 `roll_terminal_qt` 修改。

- [ ] **Step 4: Commit final documentation only if needed**

```powershell
git add docs/superpowers/specs/2026-07-28-semi-auto-strategy-library-selection-design.md
git commit -m "docs: finalize semi-auto library strategy selection"
```

