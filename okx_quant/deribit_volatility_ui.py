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
    "12小时": "43200",
    "1天": "1D",
}
DERIBIT_RESOLUTION_SECONDS = {
    "1": 1,
    "60": 60,
    "3600": 3600,
    "43200": 43200,
    "1D": 86_400,
}


@dataclass(frozen=True)
class DeribitVolatilitySnapshot:
    currency: str
    resolution: str
    candles: list[DeribitVolatilityCandle]
    fetched_at: datetime


class DeribitVolatilityWindow:
    def __init__(self, master, client: DeribitRestClient, *, logger=None) -> None:
        self.client = client
        self.logger = logger
        self.window = Toplevel(master)
        self.window.title("Deribit 波动率指数")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.8,
            height_ratio=0.76,
            min_width=1100,
            min_height=700,
            max_width=1600,
            max_height=1020,
        )

        self.currency = StringVar(value="BTC")
        self.resolution_label = StringVar(value="1小时")
        self.candle_limit = StringVar(value="500")
        self.status_text = StringVar(value="选择参数后点击“获取历史K线”。")
        self.summary_text = StringVar(value="暂无数据。")

        self._latest_snapshot: DeribitVolatilitySnapshot | None = None
        self._loading = False

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
        for column in range(6):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="币种").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.currency,
            values=DERIBIT_CURRENCY_OPTIONS,
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=(0, 12))

        ttk.Label(controls, text="分辨率").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.resolution_label,
            values=list(DERIBIT_RESOLUTION_OPTIONS.keys()),
            state="readonly",
        ).grid(row=0, column=3, sticky="ew", padx=(0, 12))

        ttk.Label(controls, text="K线数量").grid(row=0, column=4, sticky="w")
        ttk.Entry(controls, textvariable=self.candle_limit).grid(row=0, column=5, sticky="ew")

        ttk.Label(
            controls,
            textvariable=self.status_text,
            wraplength=860,
            justify="left",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(12, 0))
        ttk.Button(controls, text="获取历史K线", command=self.fetch_history).grid(row=1, column=4, sticky="e", pady=(12, 0))
        ttk.Button(controls, text="导出CSV", command=self.export_csv).grid(row=1, column=5, sticky="e", pady=(12, 0))

        body = ttk.Panedwindow(self.window, orient="vertical")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))

        chart_frame = ttk.LabelFrame(body, text="Deribit 波动率指数K线", padding=12)
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(1, weight=1)
        ttk.Label(chart_frame, textvariable=self.summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))
        self.chart_canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0)
        self.chart_canvas.grid(row=1, column=0, sticky="nsew")
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
        span_seconds = DERIBIT_RESOLUTION_SECONDS[resolution] * max(limit + 5, 10)
        start_ts = now_ts - (span_seconds * 1000)

        self._loading = True
        self.status_text.set("正在从 Deribit 拉取波动率指数历史K线，请稍候...")
        threading.Thread(
            target=self._fetch_worker,
            args=(currency, resolution, start_ts, now_ts, limit),
            daemon=True,
        ).start()

    def _fetch_worker(self, currency: str, resolution: str, start_ts: int, end_ts: int, limit: int) -> None:
        try:
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
        self.status_text.set("Deribit 波动率指数历史K线获取完成。")
        if snapshot.candles:
            self.summary_text.set(
                f"{snapshot.currency} | 分辨率 {self.resolution_label.get()} | 共 {len(snapshot.candles)} 根 | "
                f"{_format_ts(snapshot.candles[0].ts)} -> {_format_ts(snapshot.candles[-1].ts)} | "
                f"获取时间 {snapshot.fetched_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            self.summary_text.set("当前区间没有返回数据。")
        self._draw_chart(snapshot)
        if self.logger is not None:
            self.logger(f"[Deribit波动率指数] 已加载 {snapshot.currency} {self.resolution_label.get()} {len(snapshot.candles)} 根K线")

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
        file_path = export_dir / (
            f"deribit_vol_{self._latest_snapshot.currency}_{self._latest_snapshot.resolution}_"
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

    def _draw_chart(self, snapshot: DeribitVolatilitySnapshot) -> None:
        canvas = self.chart_canvas
        canvas.delete("all")
        width = max(canvas.winfo_width(), 980)
        height = max(canvas.winfo_height(), 360)
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
        candle_step = inner_width / max(len(candles), 1)
        body_width = max(2.0, candle_step * 0.6)

        price_max = max(float(candle.high) for candle in candles)
        price_min = min(float(candle.low) for candle in candles)
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
            canvas.create_text(left - 8, y, text=format_decimal_fixed(level, _decimal_places(candles)), anchor="e", fill="#57606a")

        for index, candle in enumerate(candles):
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

        time_indices = _sample_time_indices(len(candles))
        for index in time_indices:
            x = x_for(index)
            canvas.create_line(x, top, x, height - bottom, fill="#f3f4f6", dash=(2, 4))
            canvas.create_text(
                x,
                height - bottom + 8,
                text=_format_short_ts(candles[index].ts),
                anchor="n",
                fill="#57606a",
                font=("Microsoft YaHei UI", 9),
            )

        canvas.create_text(
            width - right,
            top - 6,
            text=f"{snapshot.currency} Deribit 波动率指数K线 | {self.resolution_label.get()}",
            anchor="ne",
            fill="#24292f",
            font=("Microsoft YaHei UI", 10, "bold"),
        )


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
