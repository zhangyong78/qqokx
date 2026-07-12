# Qt Local Trading Terminal Event-Driven Single-Window Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the Qt desktop client into a responsive, single-window local trading terminal whose orders, positions, and selected K-line are driven by WebSocket events while preserving REST snapshots/reconciliation and leaving the stable Tk/server strategy system unchanged.

**Architecture:** `LauncherWindow` becomes the only user-facing Qt window and lazily keeps account, K-line, and professional-arbitrage pages alive in a `QStackedWidget`. A Qt-owned realtime store consumes private `orders/account/positions`, business `orders-algo`, and business candle WebSocket events; REST is used only for startup snapshots, reconnect reconciliation, low-frequency safety reconciliation, and historical pages. UI consumers receive coalesced immutable snapshots and apply identity-based row/candle deltas instead of rebuilding entire tables and charts.

**Tech Stack:** Python 3.11+, PySide6, QtCharts/QWebEngine fallback, `websockets`, OKX V5 REST/WebSocket APIs, `unittest`/pytest-compatible tests.

## Global Constraints

- Do not modify the stable Tk live-strategy, Tk backtest, or server execution flows.
- Do not change `okx_quant/engine.py`, strategy signal logic, backtest calculations, or server launch scripts in this project.
- The Qt client remains a local, temporary trading terminal for sessions lasting hours to tens of hours.
- The product exposes one Qt top-level window; no second trade-capable K-line window is added.
- A selected symbol/period may change inside the single K-line page; saved workspace state remains keyed by `symbol + period`.
- Ordinary orders use private `orders`; conditional/OCO/trigger orders use business `orders-algo`; do not depend on the VIP-gated `fills` channel.
- REST remains mandatory for initial snapshots because OKX order/algo channels do not provide a complete initial snapshot.
- REST reconciliation runs after reconnect and at a 60-second safety interval, never every 350 ms.
- UI account/order updates are coalesced to at most 10 frames per second; candle pushes are applied at the server push rate without rebuilding history.
- Demo/live and API-profile state must remain isolated; never merge snapshots across profile or environment.
- Active RR, line-condition, and arbitrage tasks continue when their page is hidden; closing the Qt app warns before stopping local-only monitoring.
- Preserve existing workspace/ledger file formats unless a task explicitly adds backward-compatible optional fields.
- Use TDD for every behavior change and run the targeted tests before moving to the next task.

---

## File Map

### New files

- `roll_terminal_qt/perf_metrics.py`: lightweight timing records and GUI-apply duration logging.
- `okx_quant/okx_algo_ws.py`: authenticated `/ws/v5/business` `orders-algo` cache and update notifications.
- `okx_quant/okx_candle_ws.py`: `/ws/v5/business` selected-symbol candle stream and update notifications.
- `roll_terminal_qt/realtime_account_store.py`: one Qt-process owner for startup snapshots, WS event merging, reconnect/periodic REST reconciliation, and coalesced Qt signals.
- `roll_terminal_qt/incremental_views.py`: pure identity/diff helpers for positions and current orders.
- `tests/test_okx_algo_ws.py`: algo-channel parsing, deduplication, and reconnect behavior.
- `tests/test_okx_candle_ws.py`: candle parsing, replacement/append behavior, and subscription keys.
- `tests/test_qt_realtime_account_store.py`: startup snapshot, WS merge, coalescing, reconnect reconciliation, and profile isolation.
- `tests/test_qt_incremental_views.py`: deterministic position/order diff tests.

### Existing files to modify

- `okx_quant/okx_private_ws.py`: additive update-listener hook for `orders/account/positions`; no behavior change for current server consumers.
- `okx_quant/okx_client.py`: own/cache algo and candle WS connections and expose typed access/watch methods.
- `roll_terminal_qt/order_service.py`: retire the 350 ms REST loop after the realtime store is wired.
- `roll_terminal_qt/account_service.py`: retire the 2.5 s account polling loop after the realtime store is wired.
- `roll_terminal_qt/account_positions_home.py`: consume realtime snapshots and apply incremental tree/table changes.
- `roll_terminal_qt/kline_analysis_window.py`: embedded page mode, selected candle subscription, incremental candle application, chart visibility, and page-active behavior.
- `roll_terminal_qt/launcher.py`: single-window `QStackedWidget`, lazy page creation, global task status, and shutdown orchestration.
- `tests/test_account_positions_home_qt.py`: incremental view application and selection/expansion preservation.
- `tests/test_roll_terminal_qt_windows.py`: single-window navigation, K-line lifecycle, hidden-page task continuity, and close warnings.
- `tests/test_okx_client_orders.py`: client WS connection ownership and REST fallback compatibility.
- `README.md`: document Qt realtime architecture and fallback semantics.
- `软件开发指南.md`: document ownership boundaries, test commands, and server/Tk non-impact.

---

### Task 1: Establish Measurable Performance Baselines

**Files:**
- Create: `roll_terminal_qt/perf_metrics.py`
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/account_positions_home.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces: `measure_ui_step(name: str, **fields: object) -> ContextManager[None]`
- Produces: log lines shaped as `[qt_perf] <name> | elapsed_ms=<float> | ...`
- Consumes: existing `append_log_line` logging path.

- [ ] **Step 1: Write failing tests for the timing context manager**

```python
def test_measure_ui_step_logs_elapsed_ms() -> None:
    messages: list[str] = []
    with patch("roll_terminal_qt.perf_metrics.append_log_line", side_effect=messages.append):
        with measure_ui_step("orders_apply", rows=3):
            pass
    assert len(messages) == 1
    assert "[qt_perf] orders_apply" in messages[0]
    assert "elapsed_ms=" in messages[0]
    assert "rows=3" in messages[0]
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k measure_ui_step -v`

Expected: FAIL because `roll_terminal_qt.perf_metrics` does not exist.

- [ ] **Step 3: Implement the minimal timing helper**

```python
from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

from okx_quant.log_utils import append_log_line


@contextmanager
def measure_ui_step(name: str, **fields: object) -> Iterator[None]:
    started = perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (perf_counter() - started) * 1000.0
        suffix = " | ".join(f"{key}={value}" for key, value in fields.items())
        append_log_line(
            f"[qt_perf] {name} | elapsed_ms={elapsed_ms:.3f}"
            + (f" | {suffix}" if suffix else "")
        )
```

- [ ] **Step 4: Instrument only four boundaries**

Wrap, without altering behavior:

```python
with measure_ui_step("launcher_first_show"):
    ...
with measure_ui_step("positions_apply", rows=len(self._raw_positions)):
    self._render_positions_tree()
with measure_ui_step("orders_apply", rows=len(self._orders)):
    self._refresh_current_orders_table()
with measure_ui_step("kline_full_render", candles=len(payload.candles)):
    self._render_native_chart_target(...)
```

Do not optimize in this task.

- [ ] **Step 5: Run targeted Qt tests**

Run: `python -m pytest tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the isolated instrumentation**

```powershell
git add roll_terminal_qt/perf_metrics.py roll_terminal_qt/launcher.py roll_terminal_qt/account_positions_home.py roll_terminal_qt/kline_analysis_window.py tests/test_roll_terminal_qt_windows.py
git commit -m "perf: measure qt update boundaries"
```

---

### Task 2: Add Event Notifications to the Existing Private WS Cache

**Files:**
- Modify: `okx_quant/okx_private_ws.py`
- Modify: `tests/test_okx_client_orders.py`

**Interfaces:**
- Produces: `OkxPrivateWsConnection.add_update_listener(listener: Callable[[str, int], None]) -> Callable[[], None]`
- Listener channel values: exactly `orders`, `positions`, or `account`.
- Returned callable unsubscribes the listener and is idempotent.

- [ ] **Step 1: Write failing listener tests**

```python
def test_private_ws_notifies_listener_after_order_store() -> None:
    connection = _private_connection()
    received: list[tuple[str, int]] = []
    unsubscribe = connection.add_update_listener(lambda channel, version: received.append((channel, version)))
    connection._store_orders([{"ordId": "1", "state": "live"}])
    unsubscribe()
    connection._store_orders([{"ordId": "1", "state": "filled"}])
    assert received == [("orders", 1)]
```

Add equivalent position/account assertions and an idempotent double-unsubscribe assertion.

- [ ] **Step 2: Run the tests and verify failure**

Run: `python -m pytest tests/test_okx_client_orders.py -k update_listener -v`

Expected: FAIL because `add_update_listener` is missing.

- [ ] **Step 3: Implement thread-safe additive listeners**

```python
def add_update_listener(self, listener: Callable[[str, int], None]) -> Callable[[], None]:
    with self._condition:
        self._update_listeners.add(listener)

    def unsubscribe() -> None:
        with self._condition:
            self._update_listeners.discard(listener)

    return unsubscribe
```

Copy listeners while holding the lock, then invoke them after releasing it. Listener exceptions must be caught so the WS receive loop cannot die.

- [ ] **Step 4: Verify existing cache and retry-policy behavior**

Run: `python -m pytest tests/test_okx_client_orders.py tests/test_engine_retry_policy.py tests/test_strategy_engine.py -q`

Expected: PASS; server-facing cache behavior is unchanged.

- [ ] **Step 5: Commit**

```powershell
git add okx_quant/okx_private_ws.py tests/test_okx_client_orders.py
git commit -m "feat: publish private websocket cache updates"
```

---

### Task 3: Add the OKX Algo-Order Business WebSocket

**Files:**
- Create: `okx_quant/okx_algo_ws.py`
- Modify: `okx_quant/okx_client.py`
- Create: `tests/test_okx_algo_ws.py`
- Modify: `tests/test_okx_client_orders.py`

**Interfaces:**
- Produces: `OkxAlgoWsConnection(credentials: Credentials, environment: str)`.
- Produces: `get_latest_orders(limit: int = 80) -> tuple[int, tuple[dict[str, Any], ...]] | None`.
- Produces: `add_update_listener(listener: Callable[[str, int], None]) -> Callable[[], None]` with channel `orders-algo`.
- Produces client methods `get_cached_algo_order_statuses(...)` and `add_algo_order_update_listener(...)`.

- [ ] **Step 1: Write parser/state tests before networking code**

```python
def test_algo_ws_keeps_latest_state_by_algo_id() -> None:
    connection = _algo_connection_without_start()
    connection._store_orders([{"algoId": "a1", "state": "live", "uTime": "10"}])
    connection._store_orders([{"algoId": "a1", "state": "effective", "uTime": "20"}])
    version, rows = connection.get_latest_orders()
    assert version == 2
    assert rows == ({"algoId": "a1", "state": "effective", "uTime": "20"},)
```

Also test `algoClOrdId` fallback identity, demo/live URL selection, listener notification, ping/pong, and reconnect subscription payload.

- [ ] **Step 2: Verify the tests fail**

Run: `python -m pytest tests/test_okx_algo_ws.py -v`

Expected: FAIL because the module is missing.

- [ ] **Step 3: Implement the business connection by following `OkxPrivateWsConnection` completely**

Required subscription payload:

```python
{
    "op": "subscribe",
    "args": [{"channel": "orders-algo", "instType": "ANY"}],
}
```

Use live URL `wss://ws.okx.com:8443/ws/v5/business` and demo URL `wss://wspap.okx.com:8443/ws/v5/business`. Preserve reconnect/backoff/login behavior from the private connection; do not generalize both classes in this task.

- [ ] **Step 4: Add client ownership/cache methods**

```python
def get_cached_algo_order_statuses(
    self,
    credentials: Credentials,
    *,
    environment: str,
    limit: int = 80,
) -> tuple[int, list[OkxTradeOrderItem]] | None:
    ...
```

Normalize algo rows using the existing pending-order conversion helpers so the UI receives the same `OkxTradeOrderItem` semantics as REST.

- [ ] **Step 5: Run targeted tests**

Run: `python -m pytest tests/test_okx_algo_ws.py tests/test_okx_client_orders.py tests/test_arbitrage.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```powershell
git add okx_quant/okx_algo_ws.py okx_quant/okx_client.py tests/test_okx_algo_ws.py tests/test_okx_client_orders.py
git commit -m "feat: stream okx algo order updates"
```

---

### Task 4: Replace Qt Account/Order Polling with One Realtime Store

**Files:**
- Create: `roll_terminal_qt/realtime_account_store.py`
- Modify: `roll_terminal_qt/order_service.py`
- Modify: `roll_terminal_qt/account_service.py`
- Modify: `roll_terminal_qt/account_positions_home.py`
- Create: `tests/test_qt_realtime_account_store.py`
- Modify: `tests/test_account_positions_home_qt.py`

**Interfaces:**
- Produces immutable `AccountRealtimeSnapshot(profile_name, environment, positions, orders, account, generation, source)`.
- Produces `RealtimeAccountStore(QObject)` signals `snapshot_ready(object)` and `status_changed(str)`.
- Produces `start(runtime)`, `stop()`, and `request_reconcile(reason: str)`.
- Consumes private/algo listener APIs from Tasks 2–3.

- [ ] **Step 1: Write failing startup/realtime tests**

```python
def test_store_loads_rest_once_then_uses_ws_events() -> None:
    client = _FakeRealtimeClient()
    store = RealtimeAccountStore(client=client, coalesce_ms=0, reconcile_seconds=60)
    snapshots: list[AccountRealtimeSnapshot] = []
    store.snapshot_ready.connect(snapshots.append)
    store.start(_runtime("moni", "demo"))
    client.publish_order({"ordId": "1", "state": "filled", "uTime": "20"})
    _drain_qt_events()
    assert client.pending_order_rest_calls == 1
    assert snapshots[-1].orders[0].state == "filled"
```

Also test algo merge, reconnect-triggered REST reconciliation, 60-second timer configuration, 100 ms coalescing, stale-generation rejection after profile switch, and stop/unsubscribe behavior.

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_qt_realtime_account_store.py -v`

Expected: FAIL because the store is missing.

- [ ] **Step 3: Implement startup snapshot + event queue**

The store must:

```python
REST startup snapshot
    -> merge ordinary pending + algo pending + positions + account
    -> emit one snapshot
WS listener event
    -> enqueue channel/version only
    -> read the matching cache
    -> merge by ordId/clOrdId/algoId/position identity
    -> start or restart a single-shot 100 ms emit timer
reconnect or 60-second timer
    -> run one REST reconciliation worker
    -> reject stale generation results
```

No network call may run on the Qt GUI thread.

- [ ] **Step 4: Wire the account home to the store**

Replace creation of `AccountFeedThread` and `OrderFeedThread` with one store connection. Keep the old classes temporarily available for non-home callers, but the launcher home must not start their loops.

```python
self._realtime_store.snapshot_ready.connect(self._apply_realtime_snapshot)
self._realtime_store.status_changed.connect(self._set_realtime_status)
self._realtime_store.start(self._runtime)
```

- [ ] **Step 5: Add a regression test that forbids the 350 ms REST loop**

```python
def test_account_home_does_not_start_legacy_order_feed() -> None:
    with patch("roll_terminal_qt.account_positions_home.OrderFeedThread") as order_feed:
        widget = _build_home()
        assert order_feed.call_count == 0
        widget.begin_shutdown(lambda: None)
```

- [ ] **Step 6: Run targeted tests**

Run: `python -m pytest tests/test_qt_realtime_account_store.py tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py -q`

Expected: PASS.

- [ ] **Step 7: Commit**

```powershell
git add roll_terminal_qt/realtime_account_store.py roll_terminal_qt/order_service.py roll_terminal_qt/account_service.py roll_terminal_qt/account_positions_home.py tests/test_qt_realtime_account_store.py tests/test_account_positions_home_qt.py
git commit -m "refactor: drive qt account state from websocket events"
```

---

### Task 5: Apply Identity-Based Position and Order Deltas

**Files:**
- Create: `roll_terminal_qt/incremental_views.py`
- Modify: `roll_terminal_qt/account_positions_home.py`
- Create: `tests/test_qt_incremental_views.py`
- Modify: `tests/test_account_positions_home_qt.py`

**Interfaces:**
- Produces `ViewDelta[T](added, updated, removed, unchanged)`.
- Produces `diff_by_identity(old, new, identity, fingerprint) -> ViewDelta[T]`.
- Position identity: existing `_position_tree_row_id` semantics.
- Order identity priority: `ord_id`, `client_order_id`, then `inst_id + update_time`.

- [ ] **Step 1: Write pure diff tests**

```python
def test_diff_by_identity_separates_changed_and_unchanged_rows() -> None:
    old = [_Row("a", "live"), _Row("b", "live")]
    new = [_Row("a", "filled"), _Row("b", "live"), _Row("c", "live")]
    delta = diff_by_identity(old, new, lambda row: row.key, lambda row: row.state)
    assert [row.key for row in delta.updated] == ["a"]
    assert [row.key for row in delta.unchanged] == ["b"]
    assert [row.key for row in delta.added] == ["c"]
```

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_qt_incremental_views.py -v`

Expected: FAIL because the module is missing.

- [ ] **Step 3: Implement deterministic diff helpers**

```python
@dataclass(frozen=True)
class ViewDelta(Generic[T]):
    added: tuple[T, ...]
    updated: tuple[T, ...]
    removed: tuple[T, ...]
    unchanged: tuple[T, ...]
```

Reject duplicate identities in tests rather than silently choosing one.

- [ ] **Step 4: Incrementally update the current-order table**

Maintain `self._order_row_by_id: dict[str, int]`. Block sorting and selection signals while applying a delta; update only changed cells; remove rows from bottom to top; restore selection by order identity.

- [ ] **Step 5: Incrementally update the position tree**

Maintain `self._position_item_by_id: dict[str, QTreeWidgetItem]`. Rebuild only when grouping/filter structure changes; otherwise update changed position rows and affected aggregate group rows. Preserve expanded keys, scroll position, and selected identity.

- [ ] **Step 6: Add UI regression assertions**

```python
def test_unchanged_order_snapshot_does_not_replace_items() -> None:
    widget = _build_home_with_orders()
    before = widget._orders_table.item(0, 0)
    widget._apply_realtime_snapshot(widget._last_realtime_snapshot)
    assert widget._orders_table.item(0, 0) is before
```

Add equivalent selection/expansion tests for positions.

- [ ] **Step 7: Run tests and record timing comparison**

Run: `python -m pytest tests/test_qt_incremental_views.py tests/test_account_positions_home_qt.py -q`

Expected: PASS. Manual benchmark must show unchanged snapshot apply does not call `_position_tree.clear()` and is materially below the baseline timing.

- [ ] **Step 8: Commit**

```powershell
git add roll_terminal_qt/incremental_views.py roll_terminal_qt/account_positions_home.py tests/test_qt_incremental_views.py tests/test_account_positions_home_qt.py
git commit -m "perf: update qt account views incrementally"
```

---

### Task 6: Add Selected-Symbol Candle WebSocket and Incremental Chart Updates

**Files:**
- Create: `okx_quant/okx_candle_ws.py`
- Modify: `okx_quant/okx_client.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Create: `tests/test_okx_candle_ws.py`
- Modify: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces `CandleStreamKey(inst_id: str, bar: str, environment: str)`.
- Produces `OkxCandleWsConnection.watch(key, listener) -> unsubscribe`.
- Listener receives one normalized `Candle` plus `confirmed: bool`.
- K-line page produces `_apply_realtime_candle(candle: Candle) -> None` and never fetches 1200 REST candles for an ordinary realtime update.

- [ ] **Step 1: Write candle merge tests**

```python
def test_candle_stream_replaces_open_candle_and_appends_next_candle() -> None:
    state = CandleStreamState([_candle(ts=1000, close="10", confirmed=False)])
    state.apply(_candle(ts=1000, close="11", confirmed=False))
    state.apply(_candle(ts=2000, close="12", confirmed=False))
    assert [item.close for item in state.candles] == [Decimal("11"), Decimal("12")]
```

Also test bar-to-channel mapping (`1H -> candle1H`, `4H -> candle4H`, `1D -> candle1D`), symbol-switch unsubscribe, reconnect resubscribe, and malformed push rejection.

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_okx_candle_ws.py -v`

Expected: FAIL because the module is missing.

- [ ] **Step 3: Implement the business candle connection**

Use `/ws/v5/business`, subscribe to exactly one current page key, keep ping/pong/reconnect behavior, and normalize OKX array payloads into the project `Candle` type. Do not write every unconfirmed push to disk; persist on confirmation or scheduled cache flush.

- [ ] **Step 4: Keep REST for initial history only**

On symbol/period selection:

```python
load local cache immediately
fetch missing/stale history in KlineDataLoader
render one full initial chart
subscribe to current candle stream
```

On a candle event:

```python
replace the last candle when timestamps match
append one candle when timestamp advances
recompute only tail indicators/alerts needed by the changed candle
update or append chart objects
do not call _load_data()
```

- [ ] **Step 5: Add native and Web incremental paths**

For native QtCharts, keep timestamp-to-`QCandlestickSet` and timestamp-to-line-point mappings; use setters for the open candle and append for a new candle. For WebEngine, call a small JavaScript `updateCandle(payload)` bridge that uses series `.update(...)`. Full `_render_to_chart` remains only for initial history, symbol/period changes, workspace geometry changes, or recovery after a missed range.

- [ ] **Step 6: Add regression tests that ordinary candle events do not full-render**

```python
def test_realtime_candle_update_does_not_reload_history() -> None:
    window = _build_kline_window()
    window._load_data = MagicMock()
    window._render_to_chart = MagicMock()
    window._apply_realtime_candle(_next_candle())
    window._load_data.assert_not_called()
    window._render_to_chart.assert_not_called()
```

- [ ] **Step 7: Run targeted tests**

Run: `python -m pytest tests/test_okx_candle_ws.py tests/test_roll_terminal_qt_windows.py tests/test_kline_alerts.py tests/test_kline_rr_trade.py -q`

Expected: PASS.

- [ ] **Step 8: Commit**

```powershell
git add okx_quant/okx_candle_ws.py okx_quant/okx_client.py roll_terminal_qt/kline_analysis_window.py tests/test_okx_candle_ws.py tests/test_roll_terminal_qt_windows.py
git commit -m "perf: update selected kline from websocket deltas"
```

---

### Task 7: Convert the Qt Launcher to One Persistent Window

**Files:**
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Modify: `tests/test_roll_terminal_qt_windows.py`
- Modify: `tests/test_kline_account_drawer.py`

**Interfaces:**
- `LauncherWindow.show_page(page_key: str) -> None`.
- Page keys in this task: `account`, `kline`, `roll`.
- K-line page methods: `set_page_active(active: bool)`, `begin_shutdown(callback: Callable[[], None])`, and `has_local_active_tasks() -> bool`.

- [ ] **Step 1: Write failing single-window navigation tests**

```python
def test_first_kline_navigation_embeds_one_persistent_page() -> None:
    launcher = _build_launcher()
    launcher.show_page("kline")
    first = launcher._pages["kline"]
    launcher.show_page("account")
    launcher.show_page("kline")
    assert launcher._pages["kline"] is first
    assert first.parent() is launcher._page_stack
    assert launcher._child_windows == []
```

Also assert K-line construction is lazy, account first-show is not delayed, `open_module_window("kline-analysis")` routes to `show_page("kline")`, and no trade-capable second K-line window is created.

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "single_window or kline_navigation" -v`

Expected: FAIL because the launcher still creates a top-level window.

- [ ] **Step 3: Add `QStackedWidget` and lazy page factories**

```python
self._page_stack = QStackedWidget(self)
self._pages = {"account": self._home_widget}
self._page_stack.addWidget(self._home_widget)
self.setCentralWidget(self._page_stack)
```

`show_page` creates `kline`/`roll` only on first use, calls `set_page_active(False)` on the previous page when available, switches the stack, then calls `set_page_active(True)` on the target.

- [ ] **Step 4: Make the K-line class embeddable without a second implementation**

Add `embedded: bool = False` to `KlineAnalysisWindow.__init__`. In embedded mode set `Qt.Widget` window flags, skip top-level-only resize/title behavior, and expose `begin_shutdown`. Do not copy the existing K-line UI into another class in this milestone.

- [ ] **Step 5: Define active/hidden behavior**

`set_page_active(False)` pauses chart auto-refresh/render timers only. It must not stop `_rr_monitor_timer`, pending RR execution, line-trade queue processing, or account/order realtime stores. `set_page_active(True)` renders the newest cached payload once and resumes the selected chart subscription.

- [ ] **Step 6: Add hide/show chart control**

Add a checkable `隐藏图表` button. Hiding collapses the chart pane and expands RR/account/task controls; showing restores the last splitter sizes. It does not unsubscribe trading/account streams and does not stop RR monitoring.

- [ ] **Step 7: Run targeted window tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py tests/test_kline_account_drawer.py tests/test_account_positions_home_qt.py -q`

Expected: PASS.

- [ ] **Step 8: Commit**

```powershell
git add roll_terminal_qt/launcher.py roll_terminal_qt/kline_analysis_window.py tests/test_roll_terminal_qt_windows.py tests/test_kline_account_drawer.py
git commit -m "feat: embed qt trading pages in one window"
```

---

### Task 8: Add Global Temporary-Task Status and Safe Shutdown

**Files:**
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `roll_terminal_qt/kline_analysis_window.py`
- Modify: `roll_terminal_qt/ui.py`
- Modify: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Produces `LocalTaskSummary(rr: int, line_conditions: int, arbitrage: int)`.
- Produces page method `local_task_summary() -> LocalTaskSummary`.
- Launcher status text format: `RR <n> | 条件单 <n> | 套利 <n>` with zero groups omitted.

- [ ] **Step 1: Write failing task-continuity and shutdown tests**

```python
def test_switching_to_roll_keeps_rr_monitor_running() -> None:
    launcher = _build_launcher_with_kline()
    monitor = launcher._pages["kline"]._rr_monitor_timer
    launcher.show_page("roll")
    assert monitor.isActive()


def test_close_warns_when_local_tasks_are_active() -> None:
    launcher = _build_launcher_with_task_summary(rr=1)
    with patch("roll_terminal_qt.launcher.QMessageBox.question", return_value=QMessageBox.StandardButton.No):
        launcher.close()
    assert launcher.isVisible()
```

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py -k "task_summary or close_warns or keeps_rr" -v`

Expected: FAIL because task aggregation is missing.

- [ ] **Step 3: Implement summary aggregation**

Launcher polls only cheap in-memory counts once per second or reacts to a `task_summary_changed` signal. It must not query OKX or read files for the status strip.

- [ ] **Step 4: Implement close confirmation and orderly shutdown**

If local-only tasks exist, show one confirmation. On approval, stop accepting new actions, call each created page's `begin_shutdown`, wait asynchronously using existing launcher shutdown orchestration, then quit. Exchange-hosted orders are listed separately from local monitors so the warning text is accurate.

- [ ] **Step 5: Run targeted tests**

Run: `python -m pytest tests/test_roll_terminal_qt_windows.py tests/test_kline_rr_execution.py tests/test_kline_rr_trade.py tests/test_arbitrage.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```powershell
git add roll_terminal_qt/launcher.py roll_terminal_qt/kline_analysis_window.py roll_terminal_qt/ui.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat: surface and protect local qt trading tasks"
```

---

### Task 9: Remove Tk Event Pumping from the Qt Main Process

**Files:**
- Modify: `roll_terminal_qt/account_positions_home.py`
- Modify: `roll_terminal_qt/launcher.py`
- Modify: `tests/test_account_positions_home_qt.py`
- Modify: `tests/test_roll_terminal_qt_windows.py`

**Interfaces:**
- Tk live/backtest tools remain external processes with the same shared `data_root` argument.
- Qt main process must not create a hidden `Tk()` root or run `root.update()` on a 40 ms timer.

- [ ] **Step 1: Write a failing regression test**

```python
def test_qt_launcher_does_not_start_tk_event_pump() -> None:
    launcher = _build_launcher()
    assert launcher.findChildren(QTimer, "legacy_tk_pump") == []
```

Add a test that any retained legacy action launches an external process and passes the configured data directory.

- [ ] **Step 2: Verify failure**

Run: `python -m pytest tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py -k tk -v`

Expected: FAIL while the embedded Tk bridge remains.

- [ ] **Step 3: Replace embedded Tk bridge uses**

Route stable legacy live-strategy/backtest tools through the existing subprocess launch pattern used by `ui_backtest_entry.py`. Do not migrate or restyle their UI. Remove the hidden `Tk()` root and 40 ms pump from the Qt process only after all callers have an external launch route.

- [ ] **Step 4: Run targeted and startup tests**

Run: `python -m pytest tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py tests/test_roll_terminal_launcher.py -q`

Expected: PASS; Qt can start without importing/creating Tk UI objects.

- [ ] **Step 5: Commit**

```powershell
git add roll_terminal_qt/account_positions_home.py roll_terminal_qt/launcher.py tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py
git commit -m "perf: isolate legacy tk tools from qt event loop"
```

---

### Task 10: Documentation, Full Verification, and Performance Acceptance

**Files:**
- Modify: `README.md`
- Modify: `软件开发指南.md`
- Modify: `docs/kline_analysis_m1_acceptance.md`

**Interfaces:**
- Documents exact data ownership and recovery rules for future maintainers.

- [ ] **Step 1: Update documentation with implemented behavior**

Document:

```text
Qt: single local terminal, temporary task owner
Tk/server: unchanged long-running strategies and backtests
REST: startup/reconnect/60s reconciliation/history
WS: orders, orders-algo, positions, account, selected K-line
UI: coalesced snapshots and incremental updates
Shutdown: warns for local-only active tasks
```

- [ ] **Step 2: Run syntax checks**

Run:

```powershell
python -m py_compile okx_quant/okx_private_ws.py okx_quant/okx_algo_ws.py okx_quant/okx_candle_ws.py okx_quant/okx_client.py roll_terminal_qt/realtime_account_store.py roll_terminal_qt/incremental_views.py roll_terminal_qt/account_positions_home.py roll_terminal_qt/kline_analysis_window.py roll_terminal_qt/launcher.py
```

Expected: exit code 0.

- [ ] **Step 3: Run the focused regression suite**

Run:

```powershell
python -m pytest tests/test_okx_algo_ws.py tests/test_okx_candle_ws.py tests/test_qt_realtime_account_store.py tests/test_qt_incremental_views.py tests/test_okx_client_orders.py tests/test_account_positions_home_qt.py tests/test_roll_terminal_qt_windows.py tests/test_kline_account_drawer.py tests/test_kline_alerts.py tests/test_kline_rr_trade.py tests/test_kline_rr_execution.py tests/test_arbitrage.py -q
```

Expected: PASS.

- [ ] **Step 4: Run the full suite**

Run: `python -m pytest tests -q`

Expected: PASS. If unrelated pre-existing failures exist, record exact test names and prove they also fail on the pre-change revision before proceeding.

- [ ] **Step 5: Perform manual acceptance**

Verify all of the following in demo mode:

1. Main account page appears before K-line code is constructed.
2. Opening K-line stays in the same OS window.
3. Switching account -> K-line -> professional arbitrage is immediate after first construction.
4. An ordinary order update changes one row without clearing the table.
5. An algo order appears from `orders-algo`; reconnect restores it from REST snapshot.
6. A candle push updates the last candle without `_load_data()` or a full chart render.
7. Changing symbol unsubscribes the old candle key and loads/subscribes the new key.
8. Hiding the chart preserves RR monitoring.
9. Switching to professional arbitrage preserves RR monitoring.
10. Closing with local tasks shows a warning; canceling the close leaves tasks running.
11. WS disconnect shows status, reconnects, and performs one REST reconciliation.
12. Old Tk live/backtest programs still launch separately and use the same data directory.

- [ ] **Step 6: Compare performance logs against Task 1 baseline**

Acceptance targets on the same workstation and data set:

- Warm page switch performs no network call and completes below 100 ms.
- An unchanged order snapshot performs no row replacement.
- An ordinary order event produces at most one UI apply in a 100 ms coalescing window.
- A candle event does not invoke full-history REST loading or full chart reconstruction.
- Qt main process contains no 40 ms Tk event pump.

- [ ] **Step 7: Commit documentation and acceptance evidence**

```powershell
git add README.md 软件开发指南.md docs/kline_analysis_m1_acceptance.md
git commit -m "docs: describe qt realtime terminal architecture"
```

---

## Execution Order and Stop Gates

1. Execute Tasks 1–3 first. Stop if existing strategy/OKX client tests regress.
2. Execute Tasks 4–5 next. Stop unless account/order UI remains correct under reconnect and profile switching.
3. Execute Task 6 separately. Stop unless RR/alert tests pass and realtime updates avoid full history reload.
4. Execute Tasks 7–9 only after realtime stores are stable; single-window work must not conceal data-feed defects.
5. Execute Task 10 last and do not claim completion without the full verification output.

Do not bundle multiple tasks into one commit. Do not release or modify version numbers unless the user explicitly asks for a release after verification.
