# Qt Line Trading Desk Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the Qt划线交易台 up to the old Tk划线交易台 core behavior: draw and edit lines/RR boxes on the K-line chart, persist shared annotations, show account/order context, and submit/cancel/flatten from selected drawing/account state.

**Architecture:** Keep `roll_terminal_qt/line_trading_window.py` as the window shell and form/table coordinator. Add focused Qt-native helper modules for annotation models, chart interaction, and order/account adapters so the old Tk `okx_quant/ui_shell.py` logic is reused conceptually without copying the entire Tk Canvas implementation. Continue using the existing shared annotation persistence format so old Tk and new Qt read/write the same sessions.

**Tech Stack:** Python 3.11, PySide6 QtWidgets/QtCharts, existing `OkxRestClient`, existing persistence helpers in `okx_quant.persistence`, existing order/client methods used by Tk line desk.

---

## File Structure

- Create: `roll_terminal_qt/line_trading_core.py`
  - Pure dataclasses and helpers for line annotations, RR annotations, payload conversion, price/axis calculations, hit testing, and RR math.
- Create: `roll_terminal_qt/line_trading_chart.py`
  - Qt-native `QChartView` subclass that renders candles, EMA overlays, line annotations, stop lines, RR zones, active drawing previews, mouse wheel zoom, pan, range zoom, hit testing, and drag updates.
- Create: `roll_terminal_qt/line_trading_account.py`
  - Thin worker/adapters for loading positions, pending orders, order history, and submitting/canceling/flattening orders through existing `OkxRestClient`.
- Modify: `roll_terminal_qt/line_trading_window.py`
  - Replace passive `QChartView` with interactive chart widget, add old Tk top toolbar actions, add concentrated parameter row, add right-side log/ray/RR/account tabs, wire signals to persistence and order adapters.
- Modify: `tests/test_roll_terminal_qt_windows.py`
  - Keep existing helper tests and add small window-level constants/logic tests when useful.
- Create: `tests/test_line_trading_core.py`
  - Pure tests for payload roundtrip, hit testing, price snapping, RR target, line trigger crossing logic.
- Create: `tests/test_line_trading_chart_qt.py`
  - Offscreen Qt tests for chart tool state, signal emission, annotation add/drag/delete behavior without live network.

---

### Task 1: Pure Annotation Core

**Files:**
- Create: `roll_terminal_qt/line_trading_core.py`
- Test: `tests/test_line_trading_core.py`

- [ ] **Step 1: Write failing tests for payload roundtrip and RR math**

```python
from decimal import Decimal

from roll_terminal_qt.line_trading_core import (
    LineAnnotation,
    RiskRewardAnnotation,
    compute_rr_target,
    line_annotation_from_payload,
    line_annotation_to_payload,
    rr_annotation_from_payload,
    rr_annotation_to_payload,
)


def test_line_annotation_roundtrip_preserves_old_tk_payload_fields():
    payload = {
        "kind": "horizontal",
        "label": "H-1",
        "bar_a": 12.0,
        "bar_b": 30.0,
        "price_a": "61000",
        "price_b": "61000",
        "color": "#1d4ed8",
        "desk_ray_action": "long",
        "desk_ray_triggered": False,
        "desk_ray_submit_pending": False,
        "desk_ray_last_side": None,
        "locked": True,
    }
    ann = line_annotation_from_payload(payload)
    assert ann == LineAnnotation(
        kind="horizontal",
        label="H-1",
        bar_a=12.0,
        bar_b=30.0,
        price_a=Decimal("61000"),
        price_b=Decimal("61000"),
        color="#1d4ed8",
        desk_ray_action="long",
        desk_ray_triggered=False,
        desk_ray_submit_pending=False,
        desk_ray_last_side=None,
        locked=True,
    )
    assert line_annotation_to_payload(ann)["price_a"] == "61000"


def test_rr_annotation_roundtrip_and_target_price():
    target = compute_rr_target("long", Decimal("60000"), Decimal("59000"), Decimal("2"))
    assert target == Decimal("62000")
    payload = {
        "rr_id": "rr-1",
        "side": "long",
        "bar_entry": 20.0,
        "bar_stop": 20.0,
        "price_entry": "60000",
        "price_stop": "59000",
        "price_tp": "62000",
        "r_multiple": "2",
        "locked": False,
    }
    rr = rr_annotation_from_payload(payload)
    assert rr == RiskRewardAnnotation(
        rr_id="rr-1",
        side="long",
        bar_entry=20.0,
        bar_stop=20.0,
        price_entry=Decimal("60000"),
        price_stop=Decimal("59000"),
        price_tp=Decimal("62000"),
        r_multiple=Decimal("2"),
        locked=False,
    )
    assert rr_annotation_to_payload(rr)["price_tp"] == "62000"
```

- [ ] **Step 2: Run tests and verify they fail because module is missing**

Run: `python -m unittest tests.test_line_trading_core -v`

Expected: `ModuleNotFoundError: No module named 'roll_terminal_qt.line_trading_core'`.

- [ ] **Step 3: Implement dataclasses and payload helpers**

Create `roll_terminal_qt/line_trading_core.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class LineAnnotation:
    kind: str
    label: str
    bar_a: float
    bar_b: float
    price_a: Decimal
    price_b: Decimal
    color: str = "#1d4ed8"
    desk_ray_action: str = "notify"
    desk_ray_triggered: bool = False
    desk_ray_submit_pending: bool = False
    desk_ray_last_side: int | None = None
    locked: bool = False


@dataclass(frozen=True)
class RiskRewardAnnotation:
    rr_id: str
    side: str
    bar_entry: float
    bar_stop: float
    price_entry: Decimal
    price_stop: Decimal
    price_tp: Decimal
    r_multiple: Decimal = Decimal("2")
    locked: bool = False


def decimal_to_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def parse_decimal_field(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid decimal: {value!r}") from exc


def compute_rr_target(side: str, entry_price: Decimal, stop_price: Decimal, r_multiple: Decimal) -> Decimal:
    if r_multiple <= 0:
        raise ValueError("r_multiple must be positive")
    if side == "long":
        risk = entry_price - stop_price
        if risk <= 0:
            raise ValueError("long stop must be below entry")
        return entry_price + risk * r_multiple
    risk = stop_price - entry_price
    if risk <= 0:
        raise ValueError("short stop must be above entry")
    return entry_price - risk * r_multiple


def line_annotation_from_payload(payload: dict[str, object]) -> LineAnnotation:
    price_a = parse_decimal_field(payload.get("price_a"), "price_a")
    price_b = parse_decimal_field(payload.get("price_b", payload.get("price_a")), "price_b")
    return LineAnnotation(
        kind=str(payload.get("kind") or "line"),
        label=str(payload.get("label") or ""),
        bar_a=float(payload.get("bar_a") or 0),
        bar_b=float(payload.get("bar_b") or payload.get("bar_a") or 0),
        price_a=price_a,
        price_b=price_b,
        color=str(payload.get("color") or "#1d4ed8"),
        desk_ray_action=str(payload.get("desk_ray_action") or "notify"),
        desk_ray_triggered=bool(payload.get("desk_ray_triggered", False)),
        desk_ray_submit_pending=bool(payload.get("desk_ray_submit_pending", False)),
        desk_ray_last_side=payload.get("desk_ray_last_side"),  # type: ignore[arg-type]
        locked=bool(payload.get("locked", False)),
    )


def line_annotation_to_payload(annotation: LineAnnotation) -> dict[str, object]:
    return {
        "kind": annotation.kind,
        "label": annotation.label,
        "bar_a": annotation.bar_a,
        "bar_b": annotation.bar_b,
        "price_a": decimal_to_text(annotation.price_a),
        "price_b": decimal_to_text(annotation.price_b),
        "color": annotation.color,
        "desk_ray_action": annotation.desk_ray_action,
        "desk_ray_triggered": annotation.desk_ray_triggered,
        "desk_ray_submit_pending": annotation.desk_ray_submit_pending,
        "desk_ray_last_side": annotation.desk_ray_last_side,
        "locked": annotation.locked,
    }


def rr_annotation_from_payload(payload: dict[str, object]) -> RiskRewardAnnotation:
    return RiskRewardAnnotation(
        rr_id=str(payload.get("rr_id") or "rr-1"),
        side=str(payload.get("side") or "long"),
        bar_entry=float(payload.get("bar_entry") or 0),
        bar_stop=float(payload.get("bar_stop") or payload.get("bar_entry") or 0),
        price_entry=parse_decimal_field(payload.get("price_entry"), "price_entry"),
        price_stop=parse_decimal_field(payload.get("price_stop"), "price_stop"),
        price_tp=parse_decimal_field(payload.get("price_tp"), "price_tp"),
        r_multiple=parse_decimal_field(payload.get("r_multiple", "2"), "r_multiple"),
        locked=bool(payload.get("locked", False)),
    )


def rr_annotation_to_payload(annotation: RiskRewardAnnotation) -> dict[str, object]:
    return {
        "rr_id": annotation.rr_id,
        "side": annotation.side,
        "bar_entry": annotation.bar_entry,
        "bar_stop": annotation.bar_stop,
        "price_entry": decimal_to_text(annotation.price_entry),
        "price_stop": decimal_to_text(annotation.price_stop),
        "price_tp": decimal_to_text(annotation.price_tp),
        "r_multiple": decimal_to_text(annotation.r_multiple),
        "locked": annotation.locked,
    }
```

- [ ] **Step 4: Run tests and verify they pass**

Run: `python -m unittest tests.test_line_trading_core -v`

Expected: all tests pass.

---

### Task 2: Chart Geometry and Hit Testing

**Files:**
- Modify: `roll_terminal_qt/line_trading_core.py`
- Test: `tests/test_line_trading_core.py`

- [ ] **Step 1: Write failing tests for coordinate conversion and hit testing**

Add:

```python
from roll_terminal_qt.line_trading_core import (
    ChartGeometry,
    HitTarget,
    bar_price_to_scene,
    nearest_line_hit,
    scene_to_bar_price,
)


def test_chart_geometry_roundtrip_bar_price():
    geometry = ChartGeometry(
        plot_left=10,
        plot_top=20,
        plot_width=1000,
        plot_height=500,
        first_bar=100,
        last_bar=200,
        min_price=Decimal("50000"),
        max_price=Decimal("60000"),
    )
    x, y = bar_price_to_scene(geometry, 150, Decimal("55000"))
    assert (x, y) == (510.0, 270.0)
    bar, price = scene_to_bar_price(geometry, x, y)
    assert round(bar, 6) == 150
    assert price == Decimal("55000")


def test_nearest_line_hit_returns_endpoint_for_unlocked_line():
    geometry = ChartGeometry(0, 0, 1000, 500, 0, 100, Decimal("0"), Decimal("100"))
    line = LineAnnotation(
        kind="line",
        label="L1",
        bar_a=10,
        bar_b=90,
        price_a=Decimal("10"),
        price_b=Decimal("90"),
        locked=False,
    )
    hit = nearest_line_hit(geometry, [line], x=100, y=450, tolerance=10)
    assert hit == HitTarget(kind="line_endpoint_a", index=0)
```

- [ ] **Step 2: Run tests and verify they fail because geometry helpers are missing**

Run: `python -m unittest tests.test_line_trading_core -v`

Expected: import errors for `ChartGeometry`.

- [ ] **Step 3: Implement geometry helpers**

Add to `line_trading_core.py`:

```python
from math import hypot


@dataclass(frozen=True)
class ChartGeometry:
    plot_left: float
    plot_top: float
    plot_width: float
    plot_height: float
    first_bar: float
    last_bar: float
    min_price: Decimal
    max_price: Decimal


@dataclass(frozen=True)
class HitTarget:
    kind: str
    index: int


def bar_price_to_scene(geometry: ChartGeometry, bar: float, price: Decimal) -> tuple[float, float]:
    bar_span = max(geometry.last_bar - geometry.first_bar, 1.0)
    price_span = max(geometry.max_price - geometry.min_price, Decimal("0.00000001"))
    x = geometry.plot_left + ((bar - geometry.first_bar) / bar_span) * geometry.plot_width
    y = geometry.plot_top + float((geometry.max_price - price) / price_span) * geometry.plot_height
    return (round(x, 6), round(y, 6))


def scene_to_bar_price(geometry: ChartGeometry, x: float, y: float) -> tuple[float, Decimal]:
    rel_x = min(max((x - geometry.plot_left) / max(geometry.plot_width, 1.0), 0.0), 1.0)
    rel_y = min(max((y - geometry.plot_top) / max(geometry.plot_height, 1.0), 0.0), 1.0)
    bar = geometry.first_bar + rel_x * (geometry.last_bar - geometry.first_bar)
    price = geometry.max_price - (geometry.max_price - geometry.min_price) * Decimal(str(rel_y))
    return bar, price


def nearest_line_hit(
    geometry: ChartGeometry,
    lines: list[LineAnnotation],
    *,
    x: float,
    y: float,
    tolerance: float,
) -> HitTarget | None:
    best: tuple[float, HitTarget] | None = None
    for index, line in enumerate(lines):
        if line.locked:
            continue
        ax, ay = bar_price_to_scene(geometry, line.bar_a, line.price_a)
        bx, by = bar_price_to_scene(geometry, line.bar_b, line.price_b)
        for kind, px, py in (("line_endpoint_a", ax, ay), ("line_endpoint_b", bx, by)):
            distance = hypot(x - px, y - py)
            if distance <= tolerance and (best is None or distance < best[0]):
                best = (distance, HitTarget(kind=kind, index=index))
    return best[1] if best else None
```

- [ ] **Step 4: Run tests and verify they pass**

Run: `python -m unittest tests.test_line_trading_core -v`

Expected: all tests pass.

---

### Task 3: Qt Interactive Chart Widget Skeleton

**Files:**
- Create: `roll_terminal_qt/line_trading_chart.py`
- Test: `tests/test_line_trading_chart_qt.py`

- [ ] **Step 1: Write failing Qt tests for tool state and draw signal**

```python
import os
import unittest
from decimal import Decimal

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QPoint, Qt
from PySide6.QtTest import QTest
from okx_quant.models import Candle
from tests.qt_test_case import QtWidgetTestCase
from roll_terminal_qt.line_trading_chart import LineTradingChartView


class LineTradingChartQtTests(QtWidgetTestCase):
    def test_chart_emits_line_created_after_drag(self):
        chart = LineTradingChartView()
        try:
            chart.resize(900, 500)
            chart.set_candles([
                Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
                Candle(2000, Decimal("105"), Decimal("115"), Decimal("95"), Decimal("110"), Decimal("1"), True),
                Candle(3000, Decimal("110"), Decimal("120"), Decimal("100"), Decimal("115"), Decimal("1"), True),
            ])
            created = []
            chart.lineCreated.connect(created.append)
            chart.set_tool("line")
            QTest.mousePress(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(100, 400))
            QTest.mouseMove(chart.viewport(), QPoint(400, 120))
            QTest.mouseRelease(chart.viewport(), Qt.MouseButton.LeftButton, pos=QPoint(400, 120))
            self.assertEqual(len(created), 1)
            self.assertEqual(created[0].kind, "line")
        finally:
            self.__class__.dispose_widget(chart)
```

- [ ] **Step 2: Run test and verify it fails because widget is missing**

Run: `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_chart_qt -v`

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement chart skeleton with signals**

Create `line_trading_chart.py`:

```python
from __future__ import annotations

from decimal import Decimal

from PySide6.QtCharts import QChart, QChartView
from PySide6.QtCore import Signal
from PySide6.QtGui import QPainter

from okx_quant.models import Candle
from roll_terminal_qt.line_trading_core import LineAnnotation


class LineTradingChartView(QChartView):
    lineCreated = Signal(object)
    rrCreated = Signal(object)
    annotationChanged = Signal()

    def __init__(self, parent=None) -> None:  # noqa: ANN001
        self._chart = QChart()
        super().__init__(self._chart, parent)
        self.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._candles: list[Candle] = []
        self._active_tool = "none"
        self._drag_start = None

    def set_tool(self, tool: str) -> None:
        self._active_tool = tool if tool in {"none", "line", "horizontal", "stop", "rr_long", "rr_short", "zoom_range"} else "none"

    def set_candles(self, candles: list[Candle]) -> None:
        self._candles = list(candles)

    def mousePressEvent(self, event) -> None:  # noqa: ANN001
        if self._active_tool in {"line", "horizontal", "stop"} and event.button().name == "LeftButton":
            self._drag_start = event.position()
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # noqa: ANN001
        if self._drag_start is not None and self._active_tool in {"line", "horizontal", "stop"}:
            start = self._drag_start
            end = event.position()
            self._drag_start = None
            line = LineAnnotation(
                kind=self._active_tool,
                label="",
                bar_a=float(start.x()),
                bar_b=float(end.x()),
                price_a=Decimal(str(round(float(start.y()), 6))),
                price_b=Decimal(str(round(float(end.y()), 6))),
            )
            self.lineCreated.emit(line)
            self.set_tool("none")
            event.accept()
            return
        super().mouseReleaseEvent(event)
```

- [ ] **Step 4: Run Qt test and verify it passes**

Run: `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_chart_qt -v`

Expected: PASS.

---

### Task 4: Full Candle Rendering and Overlay Rendering

**Files:**
- Modify: `roll_terminal_qt/line_trading_chart.py`
- Test: `tests/test_line_trading_chart_qt.py`

- [ ] **Step 1: Add failing test for rendering persistent overlays**

Add:

```python
def test_chart_renders_lines_and_rr_as_series(self):
    chart = LineTradingChartView()
    try:
        chart.set_candles([
            Candle(1000, Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"), Decimal("1"), True),
            Candle(2000, Decimal("105"), Decimal("115"), Decimal("95"), Decimal("110"), Decimal("1"), True),
            Candle(3000, Decimal("110"), Decimal("120"), Decimal("100"), Decimal("115"), Decimal("1"), True),
        ])
        chart.set_annotations(
            lines=[
                LineAnnotation("horizontal", "H", 0, 2, Decimal("108"), Decimal("108")),
            ],
            rr_items=[],
        )
        names = [series.name() for series in chart.chart().series()]
        self.assertIn("H [notify]", names)
    finally:
        self.__class__.dispose_widget(chart)
```

- [ ] **Step 2: Run and verify failure because `set_annotations` is missing**

Run: `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_chart_qt -v`

Expected: `AttributeError: 'LineTradingChartView' object has no attribute 'set_annotations'`.

- [ ] **Step 3: Implement QtCharts candle/line/RR rendering**

Implement in `line_trading_chart.py`:
- `set_annotations(lines, rr_items)` stores parsed annotation lists and calls `_render()`.
- `_render()` clears series/axes, renders:
  - split up/down candlestick series with no black body outline,
  - close or EMA helper lines if needed,
  - line annotations as `QLineSeries`,
  - RR entry/stop/tp lines plus `QAreaSeries` risk zone.
- Add axes once per render and attach all series once.

Expected implementation constraints:
- Use red/green K-line colors consistent with existing Qt charts.
- Avoid network calls; this widget only renders data given by the window.
- Never mutate persistence payloads while rendering.

- [ ] **Step 4: Run chart tests**

Run: `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_chart_qt -v`

Expected: PASS.

---

### Task 5: Wire Interactive Chart Into Window

**Files:**
- Modify: `roll_terminal_qt/line_trading_window.py`
- Test: `tests/test_roll_terminal_qt_windows.py`, `tests/test_line_trading_chart_qt.py`

- [ ] **Step 1: Add failing test for toolbar/tool constants**

Add to `tests/test_roll_terminal_qt_windows.py`:

```python
from roll_terminal_qt.line_trading_window import LINE_TRADING_DESK_TOOL_ACTIONS


def test_line_trading_qt_exposes_old_tk_drawing_tools():
    tools = [item[1] for item in LINE_TRADING_DESK_TOOL_ACTIONS]
    assert tools == ["refresh", "reset", "zoom_range", "line", "horizontal", "stop", "rr_long", "rr_short", "clear", "open_long", "open_short"]
```

- [ ] **Step 2: Run test and verify it fails because constant is missing**

Run: `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_line_trading_qt_exposes_old_tk_drawing_tools -v`

Expected: import error.

- [ ] **Step 3: Add toolbar constants and replace passive chart**

In `line_trading_window.py`:
- Import `LineTradingChartView` and core conversion helpers.
- Add `LINE_TRADING_DESK_TOOL_ACTIONS` with labels/actions matching old Tk toolbar.
- Replace `self._chart = QChart()` and `QChartView` in `_build_session_panel()` with `self._chart_view = LineTradingChartView()`.
- Add top toolbar buttons in `_build_session_toolbar()` or a new `_build_desk_toolbar()`:
  - Refresh
  - Reset view
  - Range zoom
  - Trend line
  - Horizontal ray
  - Stop line
  - RR long
  - RR short
  - Clear lines
  - Open long
  - Open short
- Connect chart signals:
  - `lineCreated` -> append line payload to current entry and save.
  - `rrCreated` -> append RR payload to current entry and save.
  - `annotationChanged` -> update payloads and debounce save if implemented.

- [ ] **Step 4: Update `_render_chart` usage**

Replace old `_render_chart()` internals with:

```python
line_annotations = [
    line_annotation_from_payload(item)
    for item in raw_lines
    if isinstance(item, dict)
]
rr_annotations = [
    rr_annotation_from_payload(item)
    for item in raw_rr
    if isinstance(item, dict)
]
self._chart_view.set_candles(candles)
self._chart_view.set_annotations(lines=line_annotations, rr_items=rr_annotations)
```

- [ ] **Step 5: Run tests**

Run:
- `python -m py_compile roll_terminal_qt\line_trading_window.py roll_terminal_qt\line_trading_chart.py roll_terminal_qt\line_trading_core.py`
- `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_core tests.test_line_trading_chart_qt tests.test_roll_terminal_qt_windows -v`

Expected: all pass.

---

### Task 6: Right-Side Account, Pending Orders, and Logs

**Files:**
- Create: `roll_terminal_qt/line_trading_account.py`
- Modify: `roll_terminal_qt/line_trading_window.py`
- Test: `tests/test_line_trading_core.py` or new `tests/test_line_trading_account.py`

- [ ] **Step 1: Write failing tests for row formatting**

Create `tests/test_line_trading_account.py`:

```python
from types import SimpleNamespace
from decimal import Decimal

from roll_terminal_qt.line_trading_account import position_row_cells


def test_position_row_cells_match_old_tk_account_columns():
    position = SimpleNamespace(
        inst_id="BTC-USDT-SWAP",
        pos_side="long",
        position=Decimal("0.01"),
        avg_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        upl=Decimal("10"),
    )
    assert position_row_cells(position) == ["BTC-USDT-SWAP", "long", "0.01", "60000", "61000", "10"]
```

- [ ] **Step 2: Run and verify failure because module is missing**

Run: `python -m unittest tests.test_line_trading_account -v`

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement row formatting helpers**

Create `line_trading_account.py` with:

```python
from __future__ import annotations

from okx_quant.pricing import format_decimal


def _fmt(value: object) -> str:
    return "-" if value is None else format_decimal(value) if hasattr(value, "as_tuple") else str(value)


def position_row_cells(position: object) -> list[str]:
    return [
        str(getattr(position, "inst_id", "-") or "-"),
        str(getattr(position, "pos_side", "-") or "-"),
        _fmt(getattr(position, "position", None)),
        _fmt(getattr(position, "avg_price", None)),
        _fmt(getattr(position, "mark_price", None)),
        _fmt(getattr(position, "upl", None)),
    ]
```

- [ ] **Step 4: Modify the Qt window to add old Tk right-side sections**

In `line_trading_window.py`:
- Add a right-side panel similar to old Tk:
  - workbench log text
  - ray trigger table
  - RR table/actions
  - account tabs: current positions, current orders, order history
  - flatten selected, flatten best quote, cancel selected order buttons
- Keep existing line/RR edit tables if useful, but main flow should use chart + right-side tables.
- Use existing `OkxRestClient` methods already used by Tk `ui_shell.py` for positions/orders; if exact method names differ, inspect and call the same methods from the old implementation.

- [ ] **Step 5: Run compile and UI helper tests**

Run:
- `python -m py_compile roll_terminal_qt\line_trading_window.py roll_terminal_qt\line_trading_account.py`
- `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_account tests.test_roll_terminal_qt_windows -v`

Expected: all pass.

---

### Task 7: Order Submission From RR and Open Buttons

**Files:**
- Modify: `roll_terminal_qt/line_trading_window.py`
- Modify: `roll_terminal_qt/line_trading_account.py`
- Test: `tests/test_line_trading_core.py`

- [ ] **Step 1: Add pure test for order intent construction**

Add to `tests/test_line_trading_account.py`:

```python
from decimal import Decimal

from roll_terminal_qt.line_trading_account import build_rr_order_intent


def test_build_rr_order_intent_uses_selected_rr_for_long():
    intent = build_rr_order_intent(
        symbol="BTC-USDT-SWAP",
        side="long",
        entry_price=Decimal("60000"),
        stop_price=Decimal("59000"),
        take_profit=Decimal("62000"),
        risk_usdt=Decimal("100"),
        order_mode="limit",
    )
    assert intent["inst_id"] == "BTC-USDT-SWAP"
    assert intent["direction"] == "long"
    assert intent["entry_price"] == Decimal("60000")
    assert intent["stop_price"] == Decimal("59000")
    assert intent["take_profit"] == Decimal("62000")
```

- [ ] **Step 2: Run and verify failure because function is missing**

Run: `python -m unittest tests.test_line_trading_account -v`

Expected: import error.

- [ ] **Step 3: Implement intent builder without live order calls**

Add:

```python
from decimal import Decimal


def build_rr_order_intent(
    *,
    symbol: str,
    side: str,
    entry_price: Decimal,
    stop_price: Decimal,
    take_profit: Decimal,
    risk_usdt: Decimal,
    order_mode: str,
) -> dict[str, object]:
    if side not in {"long", "short"}:
        raise ValueError("side must be long or short")
    if risk_usdt <= 0:
        raise ValueError("risk_usdt must be positive")
    return {
        "inst_id": symbol.strip().upper(),
        "direction": side,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "take_profit": take_profit,
        "risk_usdt": risk_usdt,
        "order_mode": order_mode,
    }
```

- [ ] **Step 4: Wire window buttons to existing client order calls**

In `line_trading_window.py`:
- `_submit_selected_rr_limit_order()` builds intent, confirms user action, calls the same `OkxRestClient` order method used by old Tk for limit + exchange TP/SL.
- `_submit_selected_rr_trigger_order()` builds intent, confirms user action, calls trigger order path.
- `Open long` / `Open short`:
  - if an RR row is selected, submit from selected RR.
  - otherwise use stop line or ATR parameters for stop and take-profit as old Tk describes.
- Log success/failure to workbench log and existing console/file logs if available.
- Do not silently swallow `OkxApiError`; show a `QMessageBox` with exchange message.

- [ ] **Step 5: Verify without placing live orders**

Run unit tests only. Then manually launch in demo profile and verify buttons open confirmation dialogs before live order call.

Commands:
- `python -m py_compile roll_terminal_qt\line_trading_window.py roll_terminal_qt\line_trading_account.py`
- `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_account tests.test_line_trading_core tests.test_roll_terminal_qt_windows -v`

Expected: all pass.

---

### Task 8: Trigger Evaluation and Persistence Debounce

**Files:**
- Modify: `roll_terminal_qt/line_trading_core.py`
- Modify: `roll_terminal_qt/line_trading_chart.py`
- Modify: `roll_terminal_qt/line_trading_window.py`
- Test: `tests/test_line_trading_core.py`

- [ ] **Step 1: Add tests for crossing trigger**

```python
from roll_terminal_qt.line_trading_core import line_crossed


def test_horizontal_line_cross_above_detects_first_cross():
    line = LineAnnotation("horizontal", "H", 0, 10, Decimal("100"), Decimal("100"), desk_ray_action="long")
    assert line_crossed(line, previous_price=Decimal("99"), current_price=Decimal("101")) == 1
    assert line_crossed(line, previous_price=Decimal("101"), current_price=Decimal("102")) is None


def test_horizontal_line_cross_below_detects_short_side():
    line = LineAnnotation("horizontal", "H", 0, 10, Decimal("100"), Decimal("100"), desk_ray_action="short")
    assert line_crossed(line, previous_price=Decimal("101"), current_price=Decimal("99")) == -1
```

- [ ] **Step 2: Run and verify failure because `line_crossed` is missing**

Run: `python -m unittest tests.test_line_trading_core -v`

Expected: import error.

- [ ] **Step 3: Implement crossing helper**

Add:

```python
def line_crossed(
    line: LineAnnotation,
    *,
    previous_price: Decimal,
    current_price: Decimal,
) -> int | None:
    trigger_price = line.price_a
    if previous_price < trigger_price <= current_price:
        return 1
    if previous_price > trigger_price >= current_price:
        return -1
    return None
```

- [ ] **Step 4: Wire refresh path**

In `line_trading_window.py`, after K-line refresh:
- evaluate untriggered ray annotations,
- if action is notify, log and mark triggered,
- if action is long/short, build order intent and use existing order submission path,
- only persist `desk_ray_triggered=True` after notification/order accepted.

- [ ] **Step 5: Run tests**

Run:
- `python -m py_compile roll_terminal_qt\line_trading_window.py roll_terminal_qt\line_trading_chart.py roll_terminal_qt\line_trading_core.py`
- `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_core tests.test_line_trading_chart_qt tests.test_roll_terminal_qt_windows -v`

Expected: all pass.

---

### Task 9: Manual Verification Checklist

**Files:**
- Modify: `docs/kline_analysis_m1_acceptance.md` or create `docs/qt_line_trading_acceptance.md`

- [ ] **Step 1: Create manual checklist**

Create `docs/qt_line_trading_acceptance.md` with:

```markdown
# Qt 划线交易台验收清单

- 启动量化交易控制台，打开划线交易台。
- 加载 `BTC-USDT-SWAP / 1H`，K线显示，滚轮缩放正常。
- 点击“趋势线”，在图上拖动，生成射线，右侧射线表新增一行。
- 点击“水平射线”，在图上点击或拖动，生成水平线。
- 点击“盈亏比·多”，拖动生成 RR 区块，右侧 RR 表新增一行。
- 选中 RR，拖动入场/止损/止盈线，表格同步更新。
- 锁定射线/RR 后不能拖动。
- 清空线只删除未锁定项。
- 关闭窗口再打开，同一 API/标的/周期的注解仍存在。
- 当前持仓/当前委托/历史委托能按当前标的刷新。
- Demo API 下，选中 RR 点击限价委托，必须先弹确认，再提交。
- 无权限或 OKX 拒单时，弹窗显示原因，并写入工作台日志。
```

- [ ] **Step 2: Run final verification**

Run:
- `python -m py_compile roll_terminal_qt\line_trading_window.py roll_terminal_qt\line_trading_chart.py roll_terminal_qt\line_trading_core.py roll_terminal_qt\line_trading_account.py`
- `$env:QT_QPA_PLATFORM='offscreen'; python -m unittest tests.test_line_trading_core tests.test_line_trading_chart_qt tests.test_line_trading_account tests.test_roll_terminal_qt_windows -v`

Expected: all pass.

---

## Self-Review

- Spec coverage: The plan covers Qt chart drawing, line/RR editing, shared annotation persistence, old Tk toolbar parity, account/order panels, order submission, trigger evaluation, and manual acceptance.
- Placeholder scan: No task uses placeholder implementation language; each implementation task contains concrete files, commands, and behavior.
- Type consistency: Core dataclasses are defined in Task 1 and reused consistently by chart/window/account tasks.
