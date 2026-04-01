from __future__ import annotations

import csv
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tkinter import BOTH, END, Canvas, StringVar, Toplevel
from tkinter import messagebox, ttk

from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.pricing import format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


DERIBIT_CURRENCY_OPTIONS = ("BTC", "ETH")
DERIBIT_RESOLUTION_OPTIONS = {
    "1秒": "1",
    "1分钟": "60",
    "1小时": "3600",
    "4小时": "14400",
    "12小时": "43200",
    "1天": "1D",
}
DERIBIT_RESOLUTION_SECONDS = {
    "1": 1,
    "60": 60,
    "3600": 3600,
    "14400": 14_400,
    "43200": 43_200,
    "1D": 86_400,
}
DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS = {"14400"}


@dataclass(frozen=True)
class DeribitVolatilitySnapshot:
    currency: str
    resolution: str
    candles: list[DeribitVolatilityCandle]
    fetched_at: datetime


@dataclass
class _ChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


class DeribitVolatilityWindow:
    def __init__(self, master, client: DeribitRestClient, *, logger=None) -> None:
        self.client = client
        self.logger = logger
        self.window = Toplevel(master)
        self.window.title("Deribit 波动率指数")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.84,
            height_ratio=0.8,
            min_width=1120,
            min_height=740,
            max_width=1680,
            max_height=1080,
        )

        self.currency = StringVar(value="BTC")
        self.resolution_label = StringVar(value="1小时")
        self.candle_limit = StringVar(value="1000")
        self.status_text = StringVar(value="选择参数后点击“获取历史K线”。")
        self.summary_text = StringVar(value="暂无数据。")

        self._latest_snapshot: DeribitVolatilitySnapshot | None = None
        self._loading = False
        self._chart_viewport = _ChartViewport()
        self._chart_render_token = 0

        self._build_layout()
        self.window.protocol("WM_DELETE_WINDOW", self.window.withdraw)

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        controls = ttk.LabelFrame(self.window, text="查询参数", padding=16)
        controls.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        for column in range(7):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="币种").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.currency,
            values=DERIBIT_CURRENCY_OPTIONS,
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=(0, 12))

        ttk.Label(controls, text="周期").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.resolution_label,
            values=list(DERIBIT_RESOLUTION_OPTIONS.keys()),
            state="readonly",
        ).grid(row=0, column=3, sticky="ew", padx=(0, 12))

        ttk.Label(controls, text="K线数量").grid(row=0, column=4, sticky="w")
        ttk.Entry(controls, textvariable=self.candle_limit).grid(row=0, column=5, sticky="ew", padx=(0, 12))

        ttk.Button(controls, text="重置视图", command=self.reset_chart_view).grid(row=0, column=6, sticky="e")

        ttk.Label(
            controls,
            textvariable=self.status_text,
            wraplength=980,
            justify="left",
        ).grid(row=1, column=0, columnspan=5, sticky="w", pady=(12, 0))
        ttk.Button(controls, text="获取历史K线", command=self.fetch_history).grid(row=1, column=5, sticky="e", pady=(12, 0))
        ttk.Button(controls, text="导出CSV", command=self.export_csv).grid(row=1, column=6, sticky="e", pady=(12, 0))

        body = ttk.Panedwindow(self.window, orient="vertical")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))

        chart_frame = ttk.LabelFrame(body, text="Deribit 波动率指数K线", padding=12)
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(1, weight=1)
        ttk.Label(chart_frame, textvariable=self.summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.chart_canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0)
        self.chart_canvas.grid(row=1, column=0, sticky="nsew")
        self.chart_canvas.bind("<Configure>", self._on_chart_configure)
        self.chart_canvas.bind("<MouseWheel>", self._on_chart_mousewheel)
        self.chart_canvas.bind("<Button-1>", self._on_chart_press)
        self.chart_canvas.bind("<B1-Motion>", self._on_chart_drag)
        self.chart_canvas.bind("<ButtonRelease-1>", self._on_chart_release)
        self.chart_canvas.bind("<Double-Button-1>", lambda _event: self.reset_chart_view())
        body.add(chart_frame, weight=3)

        table_frame = ttk.LabelFrame(body, text="历史K线明细", padding=12)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
        self.tree = ttk.Treeview(
            table_frame,
            columns=("time", "open", "high", "low", "close"),
            show="headings",
            selectmode="browse",
        )
        for column, label, width in (
            ("time", "时间", 180),
            ("open", "开", 110),
            ("high", "高", 110),
            ("low", "低", 110),
            ("close", "收", 110),
        ):
            self.tree.heading(column, text=label)
            self.tree.column(column, width=width, anchor="center" if column == "time" else "e")
        tree_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_y.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_y.grid(row=0, column=1, sticky="ns")
        body.add(table_frame, weight=2)

    def fetch_history(self) -> None:
        if self._loading:
            return
        try:
            limit = int(self.candle_limit.get().strip())
        except ValueError:
            messagebox.showerror("参数错误", "K线数量必须是整数。", parent=self.window)
            return
        if limit <= 0 or limit > 10000:
            messagebox.showerror("参数错误", "K线数量必须在 1 到 10000 之间。", parent=self.window)
            return

        currency = self.currency.get().strip().upper()
        resolution = DERIBIT_RESOLUTION_OPTIONS[self.resolution_label.get()]
        now_ts = int(datetime.now().timestamp() * 1000)
        span_seconds = DERIBIT_RESOLUTION_SECONDS[resolution] * max(limit + 8, 16)
        start_ts = now_ts - (span_seconds * 1000)

        self._loading = True
        if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
            self.status_text.set("正在从 Deribit 拉取 1小时波动率指数并本地聚合为 4小时，请稍候...")
        else:
            self.status_text.set("正在从 Deribit 拉取波动率指数历史K线，请稍候...")
        threading.Thread(
            target=self._fetch_worker,
            args=(currency, resolution, start_ts, now_ts, limit),
            daemon=True,
        ).start()

    def _fetch_worker(self, currency: str, resolution: str, start_ts: int, end_ts: int, limit: int) -> None:
        try:
            if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
                base_start_ts = max(0, start_ts - (DERIBIT_RESOLUTION_SECONDS[resolution] * 1000))
                base_candles = self.client.get_volatility_index_candles(
                    currency,
                    "3600",
                    start_ts=base_start_ts,
                    end_ts=end_ts,
                    max_records=max(limit * 4 + 16, 64),
                )
                candles = _aggregate_candles_to_resolution(base_candles, 14_400_000)
                candles = [candle for candle in candles if start_ts <= candle.ts <= end_ts]
                if limit > 0:
                    candles = candles[-limit:]
            else:
                candles = self.client.get_volatility_index_candles(
                    currency,
                    resolution,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    max_records=limit,
                )
            snapshot = DeribitVolatilitySnapshot(
                currency=currency,
                resolution=resolution,
                candles=candles,
                fetched_at=datetime.now(),
            )
            self.window.after(0, lambda: self._apply_snapshot(snapshot))
        except Exception as exc:
            self.window.after(0, lambda: self._show_fetch_error(exc))

    def _apply_snapshot(self, snapshot: DeribitVolatilitySnapshot) -> None:
        self._loading = False
        self._latest_snapshot = snapshot
        self._chart_viewport = _ChartViewport()
        self.tree.delete(*self.tree.get_children())
        places = _decimal_places(snapshot.candles)
        for candle in reversed(snapshot.candles):
            self.tree.insert(
                "",
                END,
                values=(
                    _format_ts(candle.ts),
                    format_decimal_fixed(candle.open, places),
                    format_decimal_fixed(candle.high, places),
                    format_decimal_fixed(candle.low, places),
                    format_decimal_fixed(candle.close, places),
                ),
            )

        resolution_text = self.resolution_label.get()
        local_note = "（本地聚合）" if snapshot.resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS else ""
        self.status_text.set("Deribit 波动率指数历史K线获取完成。支持滚轮缩放、左键拖动、双击重置视图。")
        if snapshot.candles:
            self.summary_text.set(
                f"{snapshot.currency} | 周期 {resolution_text}{local_note} | 共 {len(snapshot.candles)} 根 | "
                f"{_format_ts(snapshot.candles[0].ts)} -> {_format_ts(snapshot.candles[-1].ts)} | "
                f"获取时间 {snapshot.fetched_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            self.summary_text.set("当前区间没有返回数据。")
        self._draw_chart(snapshot)
        if self.logger is not None:
            self.logger(f"[Deribit波动率指数] 已加载 {snapshot.currency} {resolution_text}{local_note} {len(snapshot.candles)} 根K线")

    def _show_fetch_error(self, exc: Exception) -> None:
        self._loading = False
        self.status_text.set("Deribit 波动率指数历史K线获取失败。")
        messagebox.showerror("获取失败", str(exc), parent=self.window)

    def export_csv(self) -> None:
        if self._latest_snapshot is None or not self._latest_snapshot.candles:
            messagebox.showinfo("没有数据", "请先获取历史K线。", parent=self.window)
            return
        export_dir = Path("D:/qqokx/reports/deribit")
        export_dir.mkdir(parents=True, exist_ok=True)
        resolution_text = _resolution_file_label(self.resolution_label.get())
        file_path = export_dir / (
            f"deribit_vol_{self._latest_snapshot.currency}_{resolution_text}_"
            f"{self._latest_snapshot.fetched_at.strftime('%Y%m%d_%H%M%S')}.csv"
        )
        with file_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "time", "open", "high", "low", "close"])
            for candle in self._latest_snapshot.candles:
                writer.writerow(
                    [
                        candle.ts,
                        _format_ts(candle.ts),
                        str(candle.open),
                        str(candle.high),
                        str(candle.low),
                        str(candle.close),
                    ]
                )
        messagebox.showinfo("导出成功", f"已导出到：{file_path}", parent=self.window)

    def reset_chart_view(self) -> None:
        if self._latest_snapshot is None:
            return
        self._chart_viewport = _ChartViewport()
        self._draw_chart(self._latest_snapshot)

    def _on_chart_configure(self, _event=None) -> None:
        if self._latest_snapshot is None:
            return
        self._schedule_chart_redraw()

    def _on_chart_mousewheel(self, event) -> None:
        if self._latest_snapshot is None or not self._latest_snapshot.candles:
            return
        width = max(self.chart_canvas.winfo_width(), 980)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        anchor_ratio = min(max((float(getattr(event, "x", left)) - left) / inner_width, 0.0), 1.0)
        next_start, next_visible = _zoom_chart_viewport(
            start_index=self._chart_viewport.start_index,
            visible_count=self._chart_viewport.visible_count,
            total_count=len(self._latest_snapshot.candles),
            anchor_ratio=anchor_ratio,
            zoom_in=getattr(event, "delta", 0) > 0,
            min_visible=24,
        )
        if next_start == self._chart_viewport.start_index and next_visible == self._chart_viewport.visible_count:
            return
        self._chart_viewport.start_index = next_start
        self._chart_viewport.visible_count = next_visible
        self._schedule_chart_redraw()

    def _on_chart_press(self, event) -> None:
        if self._latest_snapshot is None:
            return
        self._chart_viewport.pan_anchor_x = int(getattr(event, "x", 0))
        self._chart_viewport.pan_anchor_start = self._chart_viewport.start_index

    def _on_chart_drag(self, event) -> None:
        if self._latest_snapshot is None or self._chart_viewport.pan_anchor_x is None:
            return
        width = max(self.chart_canvas.winfo_width(), 980)
        left = 56
        right = 20
        inner_width = max(width - left - right, 1)
        _, visible_count = _normalize_chart_viewport(
            self._chart_viewport.start_index,
            self._chart_viewport.visible_count,
            len(self._latest_snapshot.candles),
            min_visible=24,
        )
        candle_step = inner_width / max(visible_count, 1)
        current_x = int(getattr(event, "x", self._chart_viewport.pan_anchor_x))
        shift = int(round((self._chart_viewport.pan_anchor_x - current_x) / max(candle_step, 1)))
        next_start = _pan_chart_viewport(
            self._chart_viewport.pan_anchor_start,
            visible_count,
            len(self._latest_snapshot.candles),
            shift,
            min_visible=24,
        )
        if next_start == self._chart_viewport.start_index:
            return
        self._chart_viewport.start_index = next_start
        self._schedule_chart_redraw()

    def _on_chart_release(self, _event=None) -> None:
        self._chart_viewport.pan_anchor_x = None

    def _schedule_chart_redraw(self) -> None:
        self._chart_render_token += 1
        token = self._chart_render_token

        def _redraw() -> None:
            if token != self._chart_render_token:
                return
            if self._latest_snapshot is None:
                return
            self._draw_chart(self._latest_snapshot)

        self.window.after(10, _redraw)

    def _draw_chart(self, snapshot: DeribitVolatilitySnapshot) -> None:
        canvas = self.chart_canvas
        canvas.delete("all")
        width = max(canvas.winfo_width(), 980)
        height = max(canvas.winfo_height(), 420)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        candles = snapshot.candles
        if not candles:
            canvas.create_text(width / 2, height / 2, text="当前没有可显示的K线数据。", fill="#6e7781")
            return

        left = 56
        right = 20
        top = 20
        bottom = 32
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        start_index, visible_count = _normalize_chart_viewport(
            self._chart_viewport.start_index,
            self._chart_viewport.visible_count,
            len(candles),
            min_visible=24,
        )
        self._chart_viewport.start_index = start_index
        self._chart_viewport.visible_count = visible_count
        end_index = start_index + visible_count
        visible_candles = candles[start_index:end_index]
        candle_step = inner_width / max(len(visible_candles), 1)
        body_width = max(2.0, candle_step * 0.6)

        price_max = max(float(candle.high) for candle in visible_candles)
        price_min = min(float(candle.low) for candle in visible_candles)
        if price_max == price_min:
            price_max += 1
            price_min -= 1

        def y_for(price: Decimal) -> float:
            ratio = (price_max - float(price)) / (price_max - price_min)
            return top + (ratio * inner_height)

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        for level in _build_axis_values(Decimal(str(price_min)), Decimal(str(price_max)), steps=4):
            y = y_for(level)
            canvas.create_line(left, y, width - right, y, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(
                left - 8,
                y,
                text=format_decimal_fixed(level, _decimal_places(visible_candles)),
                anchor="e",
                fill="#57606a",
            )

        for index, candle in enumerate(visible_candles):
            x = x_for(index)
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            color = "#1a7f37" if candle.close >= candle.open else "#d1242f"
            canvas.create_line(x, high_y, x, low_y, fill=color, width=1)
            body_top = min(open_y, close_y)
            body_bottom = max(open_y, close_y)
            if abs(body_bottom - body_top) < 1:
                body_bottom = body_top + 1
            canvas.create_rectangle(
                x - body_width / 2,
                body_top,
                x + body_width / 2,
                body_bottom,
                outline=color,
                fill=color,
            )

        for relative_index in _sample_time_indices(len(visible_candles)):
            x = x_for(relative_index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_short_ts(visible_candles[relative_index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        local_note = "（本地聚合）" if snapshot.resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS else ""
        canvas.create_text(
            width - right,
            top - 6,
            text=f"{snapshot.currency} Deribit 波动率指数K线 | {self.resolution_label.get()}{local_note}",
            anchor="ne",
            fill="#24292f",
            font=("Microsoft YaHei UI", 10, "bold"),
        )


def _aggregate_candles_to_resolution(
    candles: list[DeribitVolatilityCandle],
    bucket_ms: int,
) -> list[DeribitVolatilityCandle]:
    if bucket_ms <= 0:
        raise ValueError("bucket_ms must be positive")
    if not candles:
        return []

    ordered = sorted(candles, key=lambda item: item.ts)
    aggregated: list[DeribitVolatilityCandle] = []
    current_bucket_ts: int | None = None
    open_price = high_price = low_price = close_price = None

    for candle in ordered:
        bucket_ts = candle.ts - (candle.ts % bucket_ms)
        if current_bucket_ts != bucket_ts:
            if current_bucket_ts is not None and open_price is not None and high_price is not None and low_price is not None and close_price is not None:
                aggregated.append(
                    DeribitVolatilityCandle(
                        ts=current_bucket_ts,
                        open=open_price,
                        high=high_price,
                        low=low_price,
                        close=close_price,
                    )
                )
            current_bucket_ts = bucket_ts
            open_price = candle.open
            high_price = candle.high
            low_price = candle.low
            close_price = candle.close
            continue

        high_price = max(high_price, candle.high) if high_price is not None else candle.high
        low_price = min(low_price, candle.low) if low_price is not None else candle.low
        close_price = candle.close

    if current_bucket_ts is not None and open_price is not None and high_price is not None and low_price is not None and close_price is not None:
        aggregated.append(
            DeribitVolatilityCandle(
                ts=current_bucket_ts,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
            )
        )
    return aggregated


def _resolution_file_label(resolution_label: str) -> str:
    return {
        "1秒": "1s",
        "1分钟": "1m",
        "1小时": "1h",
        "4小时": "4h",
        "12小时": "12h",
        "1天": "1d",
    }.get(resolution_label, resolution_label)


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _format_short_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _sample_time_indices(count: int) -> list[int]:
    if count <= 6:
        return list(range(count))
    step = max(count // 6, 1)
    indices = list(range(0, count, step))
    if indices[-1] != count - 1:
        indices[-1] = count - 1
    return indices[:6]


def _decimal_places(candles: list[DeribitVolatilityCandle]) -> int:
    places = 2
    for candle in candles:
        for value in (candle.open, candle.high, candle.low, candle.close):
            places = max(places, max(-value.normalize().as_tuple().exponent, 0))
    return min(places, 8)


def _build_axis_values(min_value: Decimal, max_value: Decimal, *, steps: int) -> list[Decimal]:
    if steps <= 0:
        return [min_value, max_value]
    if max_value == min_value:
        return [min_value for _ in range(steps + 1)]
    step = (max_value - min_value) / Decimal(steps)
    return [min_value + (step * Decimal(index)) for index in range(steps + 1)]


def _normalize_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    *,
    min_visible: int = 24,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, 0
    normalized_min_visible = max(1, min(min_visible, total_count))
    normalized_visible = total_count if visible_count is None else max(normalized_min_visible, min(visible_count, total_count))
    max_start = max(total_count - normalized_visible, 0)
    normalized_start = max(0, min(start_index, max_start))
    return normalized_start, normalized_visible


def _zoom_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int = 24,
) -> tuple[int, int]:
    normalized_start, normalized_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    if total_count <= 0:
        return 0, 0
    factor = 0.8 if zoom_in else 1.25
    target_visible = int(round(normalized_visible * factor))
    min_count = max(1, min(min_visible, total_count))
    target_visible = max(min_count, min(target_visible, total_count))
    if target_visible == normalized_visible:
        return normalized_start, normalized_visible
    clamped_ratio = min(max(anchor_ratio, 0.0), 1.0)
    anchor_index = normalized_start + (normalized_visible * clamped_ratio)
    target_start = int(round(anchor_index - (target_visible * clamped_ratio)))
    return _normalize_chart_viewport(target_start, target_visible, total_count, min_visible=min_visible)


def _pan_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    shift: int,
    *,
    min_visible: int = 24,
) -> int:
    normalized_start, normalized_visible = _normalize_chart_viewport(
        start_index,
        visible_count,
        total_count,
        min_visible=min_visible,
    )
    target_start, _ = _normalize_chart_viewport(
        normalized_start + shift,
        normalized_visible,
        total_count,
        min_visible=min_visible,
    )
    return target_start
