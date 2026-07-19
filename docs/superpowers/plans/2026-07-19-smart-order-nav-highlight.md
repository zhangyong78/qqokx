# 无限下单导航高亮修复 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 进入“交易工具 → 无限下单”后，让“交易工具”显示完整选中样式并取消“专业套利”的选中状态。

**Architecture:** 保留现有页面路由和 `WorkspaceHeader.set_active_page()` 数据流。把“交易工具”菜单按钮登记为 `smart-order` 页面对应的互斥可选中按钮，使其复用现有 `WorkspacePageButton:checked` 样式。

**Tech Stack:** Python 3、PySide6、unittest/pytest

## Global Constraints

- 只修改顶部工作区导航状态和对应测试。
- 不重构路由系统。
- 不改动任何交易或下单逻辑。
- 不修改工作区中已有的 `roll_terminal_qt/kline_analysis_window.py` 未提交变更。

---

### Task 1: 为无限下单绑定交易工具选中状态

**Files:**
- Modify: `tests/test_workspace_shell_qt.py`
- Modify: `roll_terminal_qt/workspace_shell.py:92-115`

**Interfaces:**
- Consumes: `WorkspaceHeader.set_active_page(page_key: str) -> None` 和现有 `_page_buttons: dict[str, QToolButton]`。
- Produces: `_page_buttons["smart-order"]` 指向“交易工具”按钮；该按钮与直达页面按钮保持互斥选中。

- [ ] **Step 1: 写入失败的回归测试**

在 `WorkspaceShellQtTests` 中加入：

```python
def test_workspace_header_marks_trading_tools_active_for_smart_order(self) -> None:
    header = WorkspaceHeader()

    header.set_active_page("roll")
    self.assertTrue(header._page_buttons["roll"].isChecked())

    header.set_active_page("smart-order")

    self.assertTrue(header._page_buttons["smart-order"].isChecked())
    self.assertFalse(header._page_buttons["roll"].isChecked())
```

- [ ] **Step 2: 运行测试并确认修复前失败**

Run:

```powershell
python -m pytest tests/test_workspace_shell_qt.py::WorkspaceShellQtTests::test_workspace_header_marks_trading_tools_active_for_smart_order -v
```

Expected: FAIL，错误为 `KeyError: 'smart-order'`，证明当前导航没有登记“交易工具”按钮。

- [ ] **Step 3: 实现最小修复**

在 `WorkspaceHeader.__init__()` 中替换直接添加“交易工具”菜单按钮的语句：

```python
trading_tools_button = self._menu_button("交易工具", self._ROUTES[3:4])
trading_tools_button.setCheckable(True)
trading_tools_button.setAutoExclusive(True)
trading_tools_button.setObjectName("WorkspacePageButton")
self._page_buttons["smart-order"] = trading_tools_button
layout.addWidget(trading_tools_button)
```

“期权工具”和设置菜单保持原样，不加入页面互斥组选中状态。

- [ ] **Step 4: 运行单项测试并确认通过**

Run:

```powershell
python -m pytest tests/test_workspace_shell_qt.py::WorkspaceShellQtTests::test_workspace_header_marks_trading_tools_active_for_smart_order -v
```

Expected: PASS。

- [ ] **Step 5: 运行工作区导航组件回归测试**

Run:

```powershell
python -m pytest tests/test_workspace_shell_qt.py -v
```

Expected: 全部 PASS，现有页面、资料和工具请求信号保持正常。

- [ ] **Step 6: 运行启动器导航回归测试**

Run:

```powershell
python -m pytest tests/test_roll_terminal_qt_windows.py -k "launcher and navigation" -v
```

Expected: 所有选中的启动器导航测试 PASS。

- [ ] **Step 7: 检查差异并提交修复**

```powershell
git diff --check
git diff -- tests/test_workspace_shell_qt.py roll_terminal_qt/workspace_shell.py
git add -- tests/test_workspace_shell_qt.py roll_terminal_qt/workspace_shell.py
git commit -m "fix: highlight trading tools for smart order"
```

提交中只能包含上述两个实现文件；不得包含 `roll_terminal_qt/kline_analysis_window.py`。
