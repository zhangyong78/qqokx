# K 线分析账户抽屉实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (\`- [ ]\`) syntax for tracking.

**Goal:** 在 K 线分析图表下方增加可动态展开的“当前委托 / 当前持仓”账户抽屉，支持当前交易对与全部数据筛选，并允许确认后撤销普通或算法委托。

**Architecture:** 新建 \`KlineAccountDrawer\` 组件，封装账户快照后台加载、表格展示、本地筛选和后台撤单；K 线窗口只提供当前运行时、账户、环境和交易对，并使用垂直 \`QSplitter\` 管理图表与抽屉高度。组件直接复用 \`OkxRestClient\` 的持仓、当前委托、普通撤单和算法撤单接口，不嵌入或重构完整账户主页。

**Tech Stack:** Python 3、PySide6、QThread、QSplitter、QTableWidget、unittest、unittest.mock

## Global Constraints

- 账户抽屉默认收起，展开后允许拖动分隔线调整高度。
- 默认筛选“当前交易对”，可切换为“全部”。
- 当前委托同时包含普通委托与算法委托；撤单必须二次确认并在后台执行。
- 当前持仓只读，不提供平仓入口。
- 抽屉收起时不自动轮询；只在首次展开、手动刷新、账户变化、交易对变化和撤单成功时刷新。
- 私有接口不得在 GUI 主线程执行；旧请求不得覆盖新上下文。
- 数据读取或撤单失败不得影响 K 线功能，并保留最近一次成功表格数据。
- 保留当前工作区所有未提交改动，不修改账户主页，不重构无关代码。

---

## 文件结构

- Create: \`roll_terminal_qt/kline_account_drawer.py\` — 快照类型、纯筛选函数、加载与撤单线程、抽屉 UI 和生命周期。
- Modify: \`roll_terminal_qt/kline_analysis_window.py\` — 添加入口按钮、右侧垂直分割器、上下文联动与关闭清理。
- Create: \`tests/test_kline_account_drawer.py\` — 纯函数、组件交互、请求代次、撤单路由和 K 线接入测试。

### Task 1: 账户快照与纯筛选逻辑

**Files:**
- Create: \`roll_terminal_qt/kline_account_drawer.py\`
- Create: \`tests/test_kline_account_drawer.py\`

**Interfaces:**
- Consumes: \`OkxPosition\`、\`OkxTradeOrderItem\`。
- Produces: \`AccountDrawerSnapshot\`、\`filter_account_items(items, scope, symbol)\`、\`order_source_kind(order)\`、\`order_cancel_reference(order)\`。

- [ ] **Step 1: 编写失败测试**

\`\`\`python
def test_filter_account_items_uses_normalized_symbol_and_all_scope():
    btc = SimpleNamespace(inst_id="BTC-USDT-SWAP")
    eth = SimpleNamespace(inst_id="ETH-USDT-SWAP")
    assert filter_account_items([btc, eth], scope="symbol", symbol="btc-usdt-swap") == [btc]
    assert filter_account_items([btc, eth], scope="all", symbol="btc-usdt-swap") == [btc, eth]


def test_order_source_and_cancel_reference_support_normal_and_algo_orders():
    normal = SimpleNamespace(source_kind="normal", order_id="ord-1", client_order_id="", algo_id="", algo_client_order_id="")
    algo = SimpleNamespace(source_kind="algo", order_id="", client_order_id="", algo_id="algo-1", algo_client_order_id="")
    assert (order_source_kind(normal), order_cancel_reference(normal)) == ("normal", "ord-1")
    assert (order_source_kind(algo), order_cancel_reference(algo)) == ("algo", "algo-1")
\`\`\`

- [ ] **Step 2: 运行测试并确认失败**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k "filter_account_items or order_source" -v\`

Expected: FAIL，错误包含模块或函数尚不存在。

- [ ] **Step 3: 实现最小逻辑**

\`\`\`python
@dataclass(frozen=True)
class AccountDrawerSnapshot:
    positions: tuple[OkxPosition, ...] = ()
    orders: tuple[OkxTradeOrderItem, ...] = ()


def filter_account_items(items: Iterable[object], *, scope: str, symbol: str) -> list[object]:
    records = list(items)
    if scope == "all":
        return records
    normalized = symbol.strip().upper()
    return [item for item in records if str(getattr(item, "inst_id", "") or "").strip().upper() == normalized]


def order_source_kind(order: object) -> str:
    source = str(getattr(order, "source_kind", "") or "").strip().lower()
    return "algo" if source == "algo" or str(getattr(order, "algo_id", "") or "").strip() else "normal"


def order_cancel_reference(order: object) -> str:
    names = ("algo_id", "algo_client_order_id") if order_source_kind(order) == "algo" else ("order_id", "client_order_id")
    return next((str(getattr(order, name, "") or "").strip() for name in names if str(getattr(order, name, "") or "").strip()), "")
\`\`\`

- [ ] **Step 4: 运行 Task 1 测试**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k "filter_account_items or order_source" -v\`

Expected: PASS。

- [ ] **Step 5: 提交**

\`\`\`powershell
git add -- roll_terminal_qt/kline_account_drawer.py tests/test_kline_account_drawer.py
git commit -m "feat: add kline account snapshot helpers"
\`\`\`

### Task 2: 后台账户快照加载与抽屉展示

**Files:**
- Modify: \`roll_terminal_qt/kline_account_drawer.py\`
- Modify: \`tests/test_kline_account_drawer.py\`

**Interfaces:**
- Consumes: \`ArbitrageTradeRuntime | None\`、账户名称、环境、交易对。
- Produces: \`AccountDrawerLoadThread.completed(int, object)\`、\`AccountDrawerLoadThread.failed(int, str)\`、\`KlineAccountDrawer.set_context(...)\`、\`refresh_data()\`、\`show_tab(tab_name)\`、\`shutdown()\`。

- [ ] **Step 1: 编写 UI 初始状态失败测试**

\`\`\`python
def test_drawer_defaults_to_symbol_scope_and_read_only_positions():
    drawer = KlineAccountDrawer()
    try:
        assert drawer._scope_combo.currentData() == "symbol"
        assert drawer._tabs.tabText(0) == "当前委托"
        assert drawer._tabs.tabText(1) == "当前持仓"
        assert drawer._positions_table.editTriggers() == QAbstractItemView.EditTrigger.NoEditTriggers
        assert not hasattr(drawer, "_flatten_button")
    finally:
        drawer.deleteLater()
\`\`\`

- [ ] **Step 2: 运行测试并确认失败**

Run: \`python -m pytest tests/test_kline_account_drawer.py::KlineAccountDrawerWidgetTests::test_drawer_defaults_to_symbol_scope_and_read_only_positions -v\`

Expected: FAIL，错误包含 \`KlineAccountDrawer\` 尚不存在。

- [ ] **Step 3: 实现 UI 骨架和上下文接口**

\`\`\`python
class KlineAccountDrawer(QWidget):
    collapseRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = None
        self._profile_name = ""
        self._environment = ""
        self._symbol = ""
        self._request_generation = 0
        self._snapshot = AccountDrawerSnapshot()
        self._load_thread = None
        self._cancel_thread = None
        self._cancel_in_flight = False
        self._build_ui()

    def show_tab(self, tab_name: str) -> None:
        self._tabs.setCurrentIndex(0 if tab_name == "orders" else 1)

    def set_context(self, *, runtime, profile_name: str, environment: str, symbol: str, refresh_if_visible: bool = True) -> None:
        normalized = symbol.strip().upper()
        changed = runtime is not self._runtime or profile_name != self._profile_name or environment != self._environment or normalized != self._symbol
        self._runtime, self._profile_name, self._environment, self._symbol = runtime, profile_name, environment, normalized
        if changed and refresh_if_visible and self.isVisible():
            self.refresh_data()
\`\`\`

\`_build_ui()\` 必须创建 \`_tabs\`、\`_scope_combo\`、\`_refresh_button\`、\`_collapse_button\`、\`_status_label\`、\`_orders_table\`、\`_positions_table\`、\`_cancel_button\`。两个表格均设置 \`NoEditTriggers\` 和整行选择；不得创建任何平仓按钮。

- [ ] **Step 4: 编写加载线程接口失败测试**

\`\`\`python
def test_load_thread_reads_positions_and_all_pending_order_kinds():
    runtime = SimpleNamespace(credentials=object(), environment="demo")
    client = Mock()
    client.get_positions.return_value = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
    client.get_pending_orders.return_value = [SimpleNamespace(inst_id="BTC-USDT-SWAP")]
    thread = AccountDrawerLoadThread(request_generation=3, runtime=runtime, client=client)
    completed = []
    thread.completed.connect(lambda generation, snapshot: completed.append((generation, snapshot)))
    thread.run()
    assert completed[0][0] == 3
    client.get_pending_orders.assert_called_once_with(runtime.credentials, environment="demo", limit=100, include_algo=True)
\`\`\`

- [ ] **Step 5: 实现单次加载线程**

\`\`\`python
class AccountDrawerLoadThread(QThread):
    completed = Signal(int, object)
    failed = Signal(int, str)

    def __init__(self, *, request_generation: int, runtime, client: OkxRestClient | None = None) -> None:
        super().__init__()
        self._request_generation = request_generation
        self._runtime = runtime
        self._client = client or OkxRestClient()

    def run(self) -> None:
        try:
            positions = self._client.get_positions(self._runtime.credentials, environment=self._runtime.environment)
            orders = self._client.get_pending_orders(self._runtime.credentials, environment=self._runtime.environment, limit=100, include_algo=True)
            self.completed.emit(self._request_generation, AccountDrawerSnapshot(tuple(positions), tuple(orders)))
        except Exception as exc:
            self.failed.emit(self._request_generation, str(exc))
\`\`\`

- [ ] **Step 6: 实现代次保护、筛选和表格填充**

\`KlineAccountDrawer._apply_snapshot(generation, snapshot)\` 仅在 \`generation == self._request_generation\` 时替换快照并调用 \`_refresh_tables()\`。表格填充必须使用 \`filter_account_items\` 同时过滤委托和持仓；委托列为合约、来源、方向、类型、价格、数量、已成交、状态、更新时间、标识；持仓列为合约、方向、持仓量、可平量、均价、标记价、浮盈亏、保证金模式、持仓模式。空结果使用空表和状态文字，不插入伪行。

- [ ] **Step 7: 编写旧代次和错误保留测试**

\`\`\`python
def test_stale_snapshot_and_load_error_keep_current_data():
    drawer = KlineAccountDrawer()
    current = AccountDrawerSnapshot(positions=(SimpleNamespace(inst_id="ETH-USDT-SWAP"),))
    drawer._request_generation = 2
    drawer._snapshot = current
    drawer._apply_snapshot(1, AccountDrawerSnapshot(positions=(SimpleNamespace(inst_id="BTC-USDT-SWAP"),)))
    drawer._apply_load_error(2, "network error")
    assert drawer._snapshot is current
    assert "network error" in drawer._status_label.text()
\`\`\`

- [ ] **Step 8: 运行并提交 Task 2**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k "drawer or load_thread or stale_snapshot" -v\`

Expected: PASS。

\`\`\`powershell
git add -- roll_terminal_qt/kline_account_drawer.py tests/test_kline_account_drawer.py
git commit -m "feat: add kline account drawer tables"
\`\`\`

### Task 3: 普通与算法委托后台撤单

**Files:**
- Modify: \`roll_terminal_qt/kline_account_drawer.py\`
- Modify: \`tests/test_kline_account_drawer.py\`

**Interfaces:**
- Consumes: 当前选中的 \`OkxTradeOrderItem\` 与当前运行时。
- Produces: \`AccountDrawerCancelThread.completed(object)\`、\`failed(str)\`、\`KlineAccountDrawer._cancel_selected_order()\`。

- [ ] **Step 1: 编写普通与算法撤单路由失败测试**

\`\`\`python
def test_cancel_thread_routes_normal_and_algo_orders():
    runtime = SimpleNamespace(credentials=object(), environment="demo")
    normal_client = Mock()
    normal = SimpleNamespace(inst_id="BTC-USDT-SWAP", source_kind="normal", order_id="ord-1", client_order_id="", algo_id="", algo_client_order_id="")
    AccountDrawerCancelThread(runtime=runtime, order=normal, client=normal_client).run()
    normal_client.cancel_order_by_id.assert_called_once_with(runtime.credentials, environment="demo", inst_id="BTC-USDT-SWAP", ord_id="ord-1", cl_ord_id=None)

    algo_client = Mock()
    algo = SimpleNamespace(inst_id="BTC-USDT-SWAP", source_kind="algo", order_id="", client_order_id="", algo_id="algo-1", algo_client_order_id="")
    AccountDrawerCancelThread(runtime=runtime, order=algo, client=algo_client).run()
    algo_client.cancel_algo_order.assert_called_once_with(runtime.credentials, environment="demo", inst_id="BTC-USDT-SWAP", algo_id="algo-1", algo_cl_ord_id=None)
\`\`\`

- [ ] **Step 2: 运行并确认失败**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k cancel_thread -v\`

Expected: FAIL，错误包含 \`AccountDrawerCancelThread\` 尚不存在。

- [ ] **Step 3: 实现撤单线程**

\`\`\`python
class AccountDrawerCancelThread(QThread):
    completed = Signal(object)
    failed = Signal(str)

    def run(self) -> None:
        try:
            if order_source_kind(self._order) == "algo":
                result = self._client.cancel_algo_order(self._runtime.credentials, environment=self._runtime.environment, inst_id=self._order.inst_id, algo_id=self._order.algo_id or None, algo_cl_ord_id=self._order.algo_client_order_id or None)
            else:
                result = self._client.cancel_order_by_id(self._runtime.credentials, environment=self._runtime.environment, inst_id=self._order.inst_id, ord_id=self._order.order_id or None, cl_ord_id=self._order.client_order_id or None)
            self.completed.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))
\`\`\`

- [ ] **Step 4: 编写确认与单次提交测试**

\`\`\`python
def test_cancel_requires_selection_confirmation_and_single_inflight_action():
    drawer = KlineAccountDrawer()
    with patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
        drawer._cancel_selected_order()
    assert drawer._cancel_thread is None
    drawer._cancel_in_flight = True
    drawer._cancel_selected_order()
    assert drawer._cancel_thread is None
\`\`\`

- [ ] **Step 5: 实现确认、按钮状态和成功刷新**

\`_cancel_selected_order()\` 依次检查运行时、进行中状态、选中行和撤单标识；确认框显示合约、方向、类型与标识。确认后禁用撤单按钮并启动线程。成功回调恢复按钮、显示成功状态并调用 \`refresh_data()\`；失败回调恢复按钮、保留表格并显示错误。

- [ ] **Step 6: 运行并提交 Task 3**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k cancel -v\`

Expected: PASS。

\`\`\`powershell
git add -- roll_terminal_qt/kline_account_drawer.py tests/test_kline_account_drawer.py
git commit -m "feat: support cancelling kline drawer orders"
\`\`\`

### Task 4: 接入 K 线窗口和动态分割布局

**Files:**
- Modify: \`roll_terminal_qt/kline_analysis_window.py:4620-5140\`
- Modify: \`roll_terminal_qt/kline_analysis_window.py:5486-5510\`
- Modify: \`roll_terminal_qt/kline_analysis_window.py:5615-5670\`
- Modify: \`roll_terminal_qt/kline_analysis_window.py:5893-5897\`
- Modify: \`tests/test_kline_account_drawer.py\`

**Interfaces:**
- Consumes: \`KlineAccountDrawer.set_context(...)\`、\`show_tab(tab_name)\`、\`refresh_data()\`、\`shutdown()\`。
- Produces: \`KlineAnalysisWindow._show_account_drawer(tab_name)\`、\`_collapse_account_drawer()\`、\`_sync_account_drawer_context(...)\`。

- [ ] **Step 1: 编写默认收起与入口失败测试**

\`\`\`python
def test_kline_window_has_collapsed_account_drawer_and_two_entry_buttons():
    with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None), patch("roll_terminal_qt.kline_analysis_window.load_kline_analysis_workspace_entries", return_value={}), patch("roll_terminal_qt.kline_analysis_window.load_kline_rr_trade_ledger_snapshot", return_value={"entries": []}):
        window = KlineAnalysisWindow()
    try:
        assert window._chart_account_splitter.orientation() == Qt.Orientation.Vertical
        assert window._account_drawer.isHidden()
        assert window._orders_drawer_button.text() == "委托"
        assert window._positions_drawer_button.text() == "持仓"
    finally:
        window.deleteLater()
\`\`\`

- [ ] **Step 2: 运行并确认失败**

Run: \`python -m pytest tests/test_kline_account_drawer.py -k collapsed_account_drawer -v\`

Expected: FAIL，错误包含缺少 \`_chart_account_splitter\` 或 \`_account_drawer\`。

- [ ] **Step 3: 添加入口按钮与垂直分割器**

在头部 action row 加入“委托”和“持仓”按钮。右侧 \`chart_host\` 内创建垂直 \`QSplitter\`，上方承载现有图表，下方承载 \`KlineAccountDrawer\` 并默认隐藏。

\`\`\`python
self._chart_account_splitter = QSplitter(Qt.Orientation.Vertical)
self._chart_account_splitter.setChildrenCollapsible(False)
self._chart_account_splitter.addWidget(chart_frame)
self._account_drawer = KlineAccountDrawer()
self._account_drawer.collapseRequested.connect(self._collapse_account_drawer)
self._chart_account_splitter.addWidget(self._account_drawer)
self._account_drawer.hide()
chart_layout.addWidget(self._chart_account_splitter, 1)
\`\`\`

\`_show_account_drawer(tab_name)\` 显示抽屉、切换标签、同步上下文并刷新；首次展开按约 72%/28% 设置高度。同一入口在当前标签已经显示时再次点击则收起。\`_collapse_account_drawer()\` 隐藏抽屉并将全部高度还给图表。

- [ ] **Step 4: 编写上下文联动测试**

\`\`\`python
def test_visible_drawer_receives_normalized_symbol_change():
    window._account_drawer.show()
    with patch.object(window._account_drawer, "set_context") as set_context, patch.object(window, "_load_data"):
        window._symbol_input.setText("eth-usdt-swap")
        window._on_symbol_confirmed()
    assert set_context.call_args.kwargs["symbol"] == "ETH-USDT-SWAP"
\`\`\`

- [ ] **Step 5: 接入账户、交易对和关闭生命周期**

\`\`\`python
def _sync_account_drawer_context(self, *, refresh_if_visible: bool = True) -> None:
    self._account_drawer.set_context(
        runtime=self._runtime,
        profile_name=self._active_profile_name(),
        environment=self._active_environment(),
        symbol=self._symbol_input.text().strip().upper(),
        refresh_if_visible=refresh_if_visible,
    )
\`\`\`

在 API 账户成功切换后及 \`_on_symbol_confirmed()\` 中调用该方法。\`closeEvent()\` 调用 \`self._account_drawer.shutdown()\`；若账户线程未及时结束，则忽略关闭并短延时重试，避免销毁运行中的 QThread。

- [ ] **Step 6: 运行接入与回归测试**

Run: \`python -m pytest tests/test_kline_account_drawer.py -v\`

Expected: PASS。

Run: \`python -m pytest tests/test_roll_terminal_qt_windows.py -k "kline and (header or splitter or api_profile or symbol)" -v\`

Expected: PASS 或无匹配测试，不得出现新的失败。

- [ ] **Step 7: 提交 Task 4**

\`\`\`powershell
git add -- roll_terminal_qt/kline_analysis_window.py roll_terminal_qt/kline_account_drawer.py tests/test_kline_account_drawer.py
git commit -m "feat: add account drawer to kline analysis"
\`\`\`

### Task 5: 回归验证与工作区保护检查

**Files:**
- Verify: \`roll_terminal_qt/kline_account_drawer.py\`
- Verify: \`roll_terminal_qt/kline_analysis_window.py\`
- Verify: \`tests/test_kline_account_drawer.py\`

**Interfaces:**
- Consumes: Tasks 1-4 的完整实现。
- Produces: 可验收的账户抽屉和验证证据。

- [ ] **Step 1: 编译受影响模块**

Run: \`python -m py_compile roll_terminal_qt/kline_account_drawer.py roll_terminal_qt/kline_analysis_window.py tests/test_kline_account_drawer.py\`

Expected: exit code 0，无输出。

- [ ] **Step 2: 运行账户抽屉完整测试**

Run: \`python -m pytest tests/test_kline_account_drawer.py -v\`

Expected: 全部 PASS。

- [ ] **Step 3: 运行 K 线窗口回归测试**

Run: \`python -m pytest tests/test_roll_terminal_qt_windows.py -k kline -v\`

Expected: 全部 PASS。

- [ ] **Step 4: 检查差异和无关文件保护**

Run: \`git diff --check\`

Expected: exit code 0。

Run: \`git status --short\`

Expected: 原有未提交文件仍存在；本功能只新增或修改计划中列出的文件，不包含 \`reports/arbitrage_moni_test_report.html\`。

- [ ] **Step 5: 若验证产生未提交修正，则提交修正**

\`\`\`powershell
git add -- roll_terminal_qt/kline_account_drawer.py roll_terminal_qt/kline_analysis_window.py tests/test_kline_account_drawer.py
git commit -m "test: verify kline account drawer"
\`\`\`

