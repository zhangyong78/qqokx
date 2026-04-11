from __future__ import annotations

import csv
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tkinter import BooleanVar, END, Canvas, StringVar, Toplevel
from tkinter import messagebox, ttk

from okx_quant.deribit_client import DeribitRestClient, DeribitVolatilityCandle
from okx_quant.models import Candle
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


DERIBIT_CURRENCY_OPTIONS = ("BTC", "ETH")
DERIBIT_RESOLUTION_OPTIONS = {
    "1分钟": "60",
    "1小时": "3600",
    "4小时": "14400",
    "1天": "1D",
}
DERIBIT_RESOLUTION_SECONDS = {
    "60": 60,
    "3600": 3_600,
    "14400": 14_400,
    "1D": 86_400,
}
DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS = {"14400", "1D"}
OKX_SPOT_SYMBOLS = {"BTC": "BTC-USDT", "ETH": "ETH-USDT"}
OKX_BAR_BY_RESOLUTION = {
    "60": "1m",
    "3600": "1H",
    "14400": "4H",
    "1D": "1D",
}
DAY_ALIGN_OPTIONS = {
    "北京时间凌晨12点": 0,
    "北京时间早上8点": 8,
}


@dataclass(frozen=True)
class DeribitMarketSnapshot:
    currency: str
    resolution: str
    volatility_candles: list[DeribitVolatilityCandle]
    spot_inst_id: str
    spot_candles: list[Candle]
    aligned_volatility_candles: list[DeribitVolatilityCandle]
    aligned_spot_candles: list[Candle]
    fetched_at: datetime


@dataclass
class _ChartViewport:
    start_index: int = 0
    visible_count: int | None = None
    pan_anchor_x: int | None = None
    pan_anchor_start: int = 0


@dataclass(frozen=True)
class _ChartBounds:
    left: float
    top: float
    right: float
    bottom: float

    def contains(self, x: float, y: float) -> bool:
        return self.left <= x <= self.right and self.top <= y <= self.bottom


@dataclass(frozen=True)
class _KlineHoverState:
    bounds: _ChartBounds
    candles: tuple[DeribitVolatilityCandle | Candle, ...]
    x_positions: tuple[float, ...]
    y_positions: tuple[float, ...]
    title: str
    value_suffix: str
    places: int


class DeribitVolatilityWindow:
    def __init__(
        self,
        master,
        client: DeribitRestClient,
        *,
        market_client: OkxRestClient | None = None,
        logger=None,
    ) -> None:
        self.client = client
        self.market_client = market_client or OkxRestClient()
        self.logger = logger

        self.window = Toplevel(master)
        self.window.title("Deribit 波动率指数")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.86,
            height_ratio=0.86,
            min_width=1180,
            min_height=860,
            max_width=1760,
            max_height=1180,
        )

        self.currency = StringVar(value="BTC")
        self.resolution_label = StringVar(value="1小时")
        self.day_align_label = StringVar(value="北京时间凌晨12点")
        self.candle_limit = StringVar(value="300")
        self.average_kline = BooleanVar(value=False)
        self.status_text = StringVar(value="选择参数后点击“获取历史K线”。")
        self.summary_text = StringVar(value="暂无数据。")
        self.spot_chart_title_text = StringVar(value="同币种现货K线")

        self._latest_snapshot: DeribitMarketSnapshot | None = None
        self._loading = False
        self._chart_viewport = _ChartViewport()
        self._chart_render_token = 0
        self._volatility_hover_state: _KlineHoverState | None = None
        self._spot_hover_state: _KlineHoverState | None = None

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
        for column in range(10):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="币种").grid(row=0, column=0, sticky="w")
        ttk.Combobox(controls, textvariable=self.currency, values=DERIBIT_CURRENCY_OPTIONS, state="readonly").grid(
            row=0,
            column=1,
            sticky="ew",
            padx=(0, 12),
        )
        ttk.Label(controls, text="周期").grid(row=0, column=2, sticky="w")
        self.resolution_combo = ttk.Combobox(
            controls,
            textvariable=self.resolution_label,
            values=list(DERIBIT_RESOLUTION_OPTIONS.keys()),
            state="readonly",
        )
        self.resolution_combo.grid(row=0, column=3, sticky="ew", padx=(0, 12))
        self.resolution_combo.bind("<<ComboboxSelected>>", self._on_resolution_changed, add="+")
        ttk.Label(controls, text="日线对齐").grid(row=0, column=4, sticky="w")
        self.day_align_combo = ttk.Combobox(
            controls,
            textvariable=self.day_align_label,
            values=list(DAY_ALIGN_OPTIONS.keys()),
            state="readonly",
        )
        self.day_align_combo.grid(row=0, column=5, sticky="ew", padx=(0, 12))
        ttk.Label(controls, text="K线数量").grid(row=0, column=6, sticky="w")
        ttk.Entry(controls, textvariable=self.candle_limit).grid(row=0, column=7, sticky="ew", padx=(0, 12))
        ttk.Checkbutton(
            controls,
            text="平均K线",
            variable=self.average_kline,
            command=self._on_chart_style_changed,
        ).grid(row=0, column=8, sticky="w")
        ttk.Button(controls, text="重置视图", command=self.reset_chart_view).grid(row=0, column=8, sticky="e")

        ttk.Label(controls, textvariable=self.status_text, wraplength=980, justify="left").grid(
            row=1,
            column=0,
            columnspan=7,
            sticky="w",
            pady=(12, 0),
        )
        ttk.Button(controls, text="获取历史K线", command=self.fetch_history).grid(row=1, column=7, sticky="e", pady=(12, 0))
        ttk.Button(controls, text="导出CSV", command=self.export_csv).grid(row=1, column=8, sticky="e", pady=(12, 0))

        body = ttk.Panedwindow(self.window, orient="vertical")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 16))

        charts_frame = ttk.LabelFrame(body, text="Deribit 波动率指数与同币种现货K线", padding=12)
        charts_frame.columnconfigure(0, weight=1)
        charts_frame.rowconfigure(1, weight=1)
        ttk.Label(charts_frame, textvariable=self.summary_text, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))

        charts_pane = ttk.Panedwindow(charts_frame, orient="vertical")
        charts_pane.grid(row=1, column=0, sticky="nsew")

        volatility_frame = ttk.Frame(charts_pane)
        volatility_frame.columnconfigure(0, weight=1)
        volatility_frame.rowconfigure(1, weight=1)
        ttk.Label(volatility_frame, text="Deribit 波动率指数K线").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.volatility_canvas = Canvas(volatility_frame, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.volatility_canvas.grid(row=1, column=0, sticky="nsew")
        charts_pane.add(volatility_frame, weight=3)

        spot_frame = ttk.Frame(charts_pane)
        spot_frame.columnconfigure(0, weight=1)
        spot_frame.rowconfigure(1, weight=1)
        ttk.Label(spot_frame, textvariable=self.spot_chart_title_text).grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.spot_canvas = Canvas(spot_frame, background="#ffffff", highlightthickness=0, cursor="crosshair")
        self.spot_canvas.grid(row=1, column=0, sticky="nsew")
        charts_pane.add(spot_frame, weight=3)

        for canvas, name in ((self.volatility_canvas, "volatility"), (self.spot_canvas, "spot")):
            canvas.bind("<Configure>", self._on_chart_configure)
            canvas.bind("<MouseWheel>", self._on_chart_mousewheel)
            canvas.bind("<Button-1>", self._on_chart_press, add="+")
            canvas.bind("<B1-Motion>", self._on_chart_drag, add="+")
            canvas.bind("<ButtonRelease-1>", self._on_chart_release, add="+")
            canvas.bind("<Double-Button-1>", lambda _event: self.reset_chart_view(), add="+")
            canvas.bind("<Motion>", lambda event, source=name: self._on_linked_chart_motion(source, event), add="+")
            canvas.bind("<Leave>", lambda _event: self._clear_linked_hover(), add="+")

        body.add(charts_frame, weight=4)

        detail_frame = ttk.LabelFrame(body, text="历史K线明细", padding=12)
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self.detail_notebook = ttk.Notebook(detail_frame)
        self.detail_notebook.grid(row=0, column=0, sticky="nsew")
        self.volatility_tree = self._build_detail_tree(self.detail_notebook, tab_text="波动率明细")
        self.spot_tree = self._build_detail_tree(self.detail_notebook, tab_text="现货明细")
        body.add(detail_frame, weight=1)
        self._on_resolution_changed()

    def _build_detail_tree(self, notebook: ttk.Notebook, *, tab_text: str) -> ttk.Treeview:
        frame = ttk.Frame(notebook)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        tree = ttk.Treeview(frame, columns=("time", "open", "high", "low", "close"), show="headings", selectmode="browse")
        for column, label, width in (
            ("time", "时间", 180),
            ("open", "开", 110),
            ("high", "高", 110),
            ("low", "低", 110),
            ("close", "收", 110),
        ):
            tree.heading(column, text=label)
            tree.column(column, width=width, anchor="center" if column == "time" else "e")
        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        notebook.add(frame, text=tab_text)
        return tree

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
        self.status_text.set("正在获取 Deribit 波动率K线和同币种现货K线，请稍候...")
        threading.Thread(
            target=self._fetch_worker,
            args=(currency, resolution, start_ts, now_ts, limit),
            daemon=True,
        ).start()

    def _on_resolution_changed(self, _event=None) -> None:
        is_daily = DERIBIT_RESOLUTION_OPTIONS.get(self.resolution_label.get()) == "1D"
        self.day_align_combo.configure(state="readonly" if is_daily else "disabled")

    def _fetch_worker(self, currency: str, resolution: str, start_ts: int, end_ts: int, limit: int) -> None:
        try:
            volatility_candles = self._fetch_volatility_candles(currency, resolution, start_ts, end_ts, limit)
            spot_inst_id = OKX_SPOT_SYMBOLS[currency]
            spot_candles = self._fetch_spot_candles(spot_inst_id, resolution, limit)
            aligned_volatility, aligned_spot = _align_candles_by_timestamp(volatility_candles, spot_candles)
            snapshot = DeribitMarketSnapshot(
                currency=currency,
                resolution=resolution,
                volatility_candles=volatility_candles,
                spot_inst_id=spot_inst_id,
                spot_candles=spot_candles,
                aligned_volatility_candles=aligned_volatility,
                aligned_spot_candles=aligned_spot,
                fetched_at=datetime.now(),
            )
            self.window.after(0, lambda data=snapshot: self._apply_snapshot(data))
        except Exception as exc:
            self.window.after(0, lambda error=exc: self._show_fetch_error(error))

    def _fetch_volatility_candles(
        self,
        currency: str,
        resolution: str,
        start_ts: int,
        end_ts: int,
        limit: int,
    ) -> list[DeribitVolatilityCandle]:
        if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
            base_resolution = "3600"
            multiplier = 4 if resolution == "14400" else 24
            base_start_ts = max(0, start_ts - (DERIBIT_RESOLUTION_SECONDS[resolution] * 1000))
            base_candles = self.client.get_volatility_index_candles(
                currency,
                base_resolution,
                start_ts=base_start_ts,
                end_ts=end_ts,
                max_records=max(limit * multiplier + 48, 96),
            )
            if resolution == "1D":
                four_hour_candles = _aggregate_candles_to_resolution(
                    base_candles,
                    14_400_000,
                    anchor_offset_ms=self._daily_anchor_offset_ms(),
                )
                candles = _aggregate_candles_to_resolution(
                    four_hour_candles,
                    86_400_000,
                    anchor_offset_ms=self._daily_anchor_offset_ms(),
                )
            else:
                candles = _aggregate_candles_to_resolution(
                    base_candles,
                    14_400_000,
                    anchor_offset_ms=0,
                )
            candles = [candle for candle in candles if start_ts <= candle.ts <= end_ts]
            return candles[-limit:] if limit > 0 else candles
        return self.client.get_volatility_index_candles(
            currency,
            resolution,
            start_ts=start_ts,
            end_ts=end_ts,
            max_records=limit,
        )

    def _fetch_spot_candles(self, inst_id: str, resolution: str, limit: int) -> list[Candle]:
        okx_bar = OKX_BAR_BY_RESOLUTION[resolution]
        if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
            multiplier = 4 if resolution == "14400" else 24
            base_candles = self.market_client.get_candles_history(inst_id, "1H", limit=max(limit * multiplier + 48, 96))
            confirmed = [candle for candle in base_candles if candle.confirmed]
            if resolution == "1D":
                four_hour_candles = _aggregate_price_candles_to_resolution(
                    confirmed,
                    14_400_000,
                    anchor_offset_ms=self._daily_anchor_offset_ms(),
                )
                candles = _aggregate_price_candles_to_resolution(
                    four_hour_candles,
                    86_400_000,
                    anchor_offset_ms=self._daily_anchor_offset_ms(),
                )
            else:
                candles = _aggregate_price_candles_to_resolution(
                    confirmed,
                    14_400_000,
                    anchor_offset_ms=0,
                )
        else:
            candles = self.market_client.get_candles_history(inst_id, okx_bar, limit=limit)
            candles = [candle for candle in candles if candle.confirmed]
        return candles[-limit:]

    def _apply_snapshot(self, snapshot: DeribitMarketSnapshot) -> None:
        self._loading = False
        self._latest_snapshot = snapshot
        self._chart_viewport = _ChartViewport()
        self._clear_linked_hover()
        _fill_deribit_tree(self.volatility_tree, snapshot.aligned_volatility_candles)
        _fill_price_tree(self.spot_tree, snapshot.aligned_spot_candles)

        resolution_text = self.resolution_label.get()
        local_note = self._local_note_for_resolution(snapshot.resolution)
        self.spot_chart_title_text.set(f"{snapshot.spot_inst_id} 现货K线 | {resolution_text}{local_note}")
        self.status_text.set("Deribit 波动率K线与同币种现货K线获取完成，支持滚轮缩放、左键拖动、双击重置视图和联动十字光标。")
        if snapshot.aligned_volatility_candles and snapshot.aligned_spot_candles:
            self.summary_text.set(
                f"{snapshot.currency} | 周期 {resolution_text}{local_note} | "
                f"波动率 {len(snapshot.volatility_candles)} 根 | 现货 {len(snapshot.spot_candles)} 根 | "
                f"共同时间 {len(snapshot.aligned_volatility_candles)} 根 | "
                f"{_format_ts(snapshot.aligned_volatility_candles[0].ts)} -> {_format_ts(snapshot.aligned_volatility_candles[-1].ts)} | "
                f"获取时间 {snapshot.fetched_at.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            self.summary_text.set("当前区间缺少可联动的共同K线数据。")
        self._draw_linked_charts(snapshot)
        if self.logger is not None:
            self.logger(
                f"[Deribit波动率指数] 已加载 {snapshot.currency} {resolution_text}{local_note} | "
                f"波动率={len(snapshot.volatility_candles)} | 现货={len(snapshot.spot_candles)} | 共同={len(snapshot.aligned_volatility_candles)}"
            )

    def _daily_anchor_offset_ms(self) -> int:
        anchor_hour = DAY_ALIGN_OPTIONS.get(self.day_align_label.get(), 0)
        utc_hour = (anchor_hour - 8) % 24
        return utc_hour * 3_600_000

    def _local_note_for_resolution(self, resolution: str) -> str:
        if resolution == "1D":
            return f"（本地4小时对齐聚合，{self.day_align_label.get()}收线）"
        if resolution in DERIBIT_LOCAL_AGGREGATED_RESOLUTIONS:
            return "（本地聚合）"
        return ""

    def _show_fetch_error(self, exc: Exception) -> None:
        self._loading = False
        self.status_text.set("Deribit 波动率指数历史K线获取失败。")
        messagebox.showerror("获取失败", str(exc), parent=self.window)

    def export_csv(self) -> None:
        if self._latest_snapshot is None or not self._latest_snapshot.aligned_volatility_candles:
            messagebox.showinfo("没有数据", "请先获取历史K线。", parent=self.window)
            return
        export_dir = Path("D:/qqokx/reports/deribit")
        export_dir.mkdir(parents=True, exist_ok=True)
        resolution_text = _resolution_file_label(self.resolution_label.get())
        base_name = f"{self._latest_snapshot.currency}_{resolution_text}_{self._latest_snapshot.fetched_at.strftime('%Y%m%d_%H%M%S')}"
        vol_path = export_dir / f"deribit_vol_{base_name}.csv"
        spot_path = export_dir / f"deribit_spot_{base_name}.csv"

        with vol_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "time", "open", "high", "low", "close"])
            for candle in self._latest_snapshot.aligned_volatility_candles:
                writer.writerow([candle.ts, _format_ts(candle.ts), str(candle.open), str(candle.high), str(candle.low), str(candle.close)])

        with spot_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "time", "open", "high", "low", "close"])
            for candle in self._latest_snapshot.aligned_spot_candles:
                writer.writerow([candle.ts, _format_ts(candle.ts), str(candle.open), str(candle.high), str(candle.low), str(candle.close)])

        messagebox.showinfo("导出成功", f"已导出到：\n{vol_path}\n{spot_path}", parent=self.window)

    def reset_chart_view(self) -> None:
        if self._latest_snapshot is None:
            return
        self._chart_viewport = _ChartViewport()
        self._draw_linked_charts(self._latest_snapshot)

    def _on_chart_style_changed(self) -> None:
        if self._latest_snapshot is None:
            return
        self._clear_linked_hover()
        self._draw_linked_charts(self._latest_snapshot)

    def _on_chart_configure(self, _event=None) -> None:
        if self._latest_snapshot is None:
            return
        self._schedule_chart_redraw()

    def _on_chart_mousewheel(self, event) -> None:
        snapshot = self._latest_snapshot
        if snapshot is None or not snapshot.aligned_volatility_candles:
            return
        canvas = event.widget
        width = max(canvas.winfo_width(), 980)
        left = 62
        right = 20
        inner_width = max(width - left - right, 1)
        anchor_ratio = min(max((float(getattr(event, "x", left)) - left) / inner_width, 0.0), 1.0)
        next_start, next_visible = _zoom_chart_viewport(
            start_index=self._chart_viewport.start_index,
            visible_count=self._chart_viewport.visible_count,
            total_count=len(snapshot.aligned_volatility_candles),
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
        snapshot = self._latest_snapshot
        if snapshot is None or self._chart_viewport.pan_anchor_x is None or not snapshot.aligned_volatility_candles:
            return
        canvas = event.widget
        width = max(canvas.winfo_width(), 980)
        left = 62
        right = 20
        inner_width = max(width - left - right, 1)
        visible_count = self._chart_viewport.visible_count or len(snapshot.aligned_volatility_candles)
        step = inner_width / max(visible_count, 1)
        delta_px = int(getattr(event, "x", 0)) - self._chart_viewport.pan_anchor_x
        index_delta = int(round(-delta_px / max(step, 1)))
        self._chart_viewport.start_index = _pan_chart_viewport(
            self._chart_viewport.pan_anchor_start,
            visible_count,
            len(snapshot.aligned_volatility_candles),
            index_delta,
            min_visible=24,
        )
        self._schedule_chart_redraw()

    def _on_chart_release(self, _event) -> None:
        self._chart_viewport.pan_anchor_x = None

    def _schedule_chart_redraw(self) -> None:
        token = self._chart_render_token + 1
        self._chart_render_token = token
        self.window.after(
            10,
            lambda current=token: self._draw_linked_charts(self._latest_snapshot)
            if current == self._chart_render_token and self._latest_snapshot is not None
            else None,
        )

    def _draw_linked_charts(self, snapshot: DeribitMarketSnapshot | None) -> None:
        if snapshot is None:
            return
        self._draw_volatility_chart(self.volatility_canvas, snapshot)
        self._draw_spot_chart(self.spot_canvas, snapshot)

    def _visible_aligned_candles(self, snapshot: DeribitMarketSnapshot) -> tuple[list[DeribitVolatilityCandle], list[Candle]]:
        total = len(snapshot.aligned_volatility_candles)
        if total == 0:
            return [], []
        start_index, visible_count = _normalize_chart_viewport(
            self._chart_viewport.start_index,
            self._chart_viewport.visible_count,
            total,
            min_visible=24,
        )
        self._chart_viewport.start_index = start_index
        self._chart_viewport.visible_count = visible_count
        end_index = min(total, start_index + visible_count)
        return (
            snapshot.aligned_volatility_candles[start_index:end_index],
            snapshot.aligned_spot_candles[start_index:end_index],
        )

    def _draw_volatility_chart(self, canvas: Canvas, snapshot: DeribitMarketSnapshot) -> None:
        visible_volatility, _ = self._visible_aligned_candles(snapshot)
        if self.average_kline.get():
            visible_volatility = _to_average_volatility_candles(visible_volatility)
        title = f"{snapshot.currency} Deribit 波动率指数K线 | {self.resolution_label.get()}"
        self._volatility_hover_state = self._draw_kline_chart(canvas, visible_volatility, title=title, axis_suffix="%")

    def _draw_spot_chart(self, canvas: Canvas, snapshot: DeribitMarketSnapshot) -> None:
        _, visible_spot = self._visible_aligned_candles(snapshot)
        if self.average_kline.get():
            visible_spot = _to_average_price_candles(visible_spot)
        title = f"{snapshot.spot_inst_id} 现货K线 | {self.resolution_label.get()}"
        self._spot_hover_state = self._draw_kline_chart(canvas, visible_spot, title=title, axis_suffix="")

    def _draw_kline_chart(
        self,
        canvas: Canvas,
        candles: list[DeribitVolatilityCandle | Candle],
        *,
        title: str,
        axis_suffix: str,
    ) -> _KlineHoverState | None:
        canvas.delete("all")
        width = max(canvas.winfo_width(), 980)
        height = max(canvas.winfo_height(), 320)
        canvas.create_rectangle(0, 0, width, height, outline="", fill="#ffffff")
        if not candles:
            canvas.create_text(width / 2, height / 2, text="当前没有可显示的K线数据。", fill="#6e7781")
            return None

        left = 62
        right = 20
        top = 20
        bottom = 32
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return None

        candle_step = inner_width / max(len(candles), 1)
        body_width = max(2.0, min(10.0, candle_step * 0.62))
        price_max = max(float(candle.high) for candle in candles)
        price_min = min(float(candle.low) for candle in candles)
        if price_max == price_min:
            price_max += 1.0
            price_min -= 1.0
        else:
            price_span = price_max - price_min
            padding = max(price_span * 0.06, abs(price_max) * 0.002, 0.01)
            price_max += padding
            price_min -= padding

        def y_for(price: Decimal) -> float:
            ratio = (price_max - float(price)) / (price_max - price_min)
            return top + (ratio * inner_height)

        def x_for(index: int) -> float:
            return left + (index * candle_step) + (candle_step / 2)

        places = _decimal_places_generic(candles)
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        for level in _build_axis_values(Decimal(str(price_min)), Decimal(str(price_max)), steps=4):
            y = y_for(level)
            canvas.create_line(left, y, width - right, y, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(left - 8, y, text=f"{format_decimal_fixed(level, places)}{axis_suffix}", anchor="e", fill="#57606a")

        x_positions: list[float] = []
        y_positions: list[float] = []
        for index, candle in enumerate(candles):
            x = x_for(index)
            x_positions.append(x)
            y_positions.append(y_for(candle.close))
            open_y = y_for(candle.open)
            close_y = y_for(candle.close)
            high_y = y_for(candle.high)
            low_y = y_for(candle.low)
            up = candle.close >= candle.open
            color = "#16a34a" if up else "#dc2626"
            canvas.create_line(x, high_y, x, low_y, fill=color)
            top_y = min(open_y, close_y)
            bottom_y = max(open_y, close_y)
            if abs(bottom_y - top_y) < 1:
                bottom_y = top_y + 1
            canvas.create_rectangle(
                x - (body_width / 2),
                top_y,
                x + (body_width / 2),
                bottom_y,
                outline=color,
                fill=color,
            )

        for index in _sample_time_indices(len(candles)):
            x = x_for(index)
            label = _format_short_ts(candles[index].ts)
            canvas.create_line(x, top, x, height - bottom, fill="#eef2f7", dash=(2, 4))
            canvas.create_text(x, height - bottom + 16, text=label, anchor="n", fill="#57606a")

        return _KlineHoverState(
            bounds=_ChartBounds(left=left, top=top, right=width - right, bottom=height - bottom),
            candles=tuple(candles),
            x_positions=tuple(x_positions),
            y_positions=tuple(y_positions),
            title=title,
            value_suffix=axis_suffix,
            places=places,
        )

    def _on_linked_chart_motion(self, source: str, event) -> None:
        if source == "volatility":
            source_canvas = self.volatility_canvas
            source_state = self._volatility_hover_state
            other_canvas = self.spot_canvas
            other_state = self._spot_hover_state
        else:
            source_canvas = self.spot_canvas
            source_state = self._spot_hover_state
            other_canvas = self.volatility_canvas
            other_state = self._volatility_hover_state
        if source_state is None or not source_state.candles:
            return
        if not source_state.bounds.contains(float(event.x), float(event.y)):
            self._clear_linked_hover()
            return
        index = _nearest_linear_index(float(event.x), source_state.bounds.left, source_state.bounds.right, len(source_state.candles))
        self._draw_chart_hover_overlay(source_canvas, source_state, index)
        if other_state is not None and index < len(other_state.candles):
            self._draw_chart_hover_overlay(other_canvas, other_state, index)

    def _clear_linked_hover(self) -> None:
        self.volatility_canvas.delete("hover-overlay")
        self.spot_canvas.delete("hover-overlay")

    def _draw_chart_hover_overlay(self, canvas: Canvas, state: _KlineHoverState, index: int) -> None:
        candle = state.candles[index]
        x = state.x_positions[index]
        y = state.y_positions[index]
        canvas.delete("hover-overlay")
        canvas.create_line(x, state.bounds.top, x, state.bounds.bottom, fill="#94a3b8", dash=(2, 4), tags="hover-overlay")
        canvas.create_line(state.bounds.left, y, state.bounds.right, y, fill="#94a3b8", dash=(2, 4), tags="hover-overlay")
        canvas.create_oval(x - 3, y - 3, x + 3, y + 3, outline="#16a34a", fill="#ffffff", width=2, tags="hover-overlay")

        lines = (
            _format_short_ts(candle.ts),
            f"O {format_decimal_fixed(candle.open, state.places)}",
            f"H {format_decimal_fixed(candle.high, state.places)}",
            f"L {format_decimal_fixed(candle.low, state.places)}",
            f"C {format_decimal_fixed(candle.close, state.places)}{state.value_suffix}",
        )
        line_height = 13
        tooltip_width = max(92, max(len(line) for line in lines) * 5 + 12)
        tooltip_height = 6 + (line_height * len(lines))
        mid_x = (state.bounds.left + state.bounds.right) / 2
        if x <= mid_x:
            tooltip_left = state.bounds.right - tooltip_width - 10
        else:
            tooltip_left = state.bounds.left + 10
        tooltip_top = state.bounds.top + 10
        canvas.create_rectangle(
            tooltip_left,
            tooltip_top,
            tooltip_left + tooltip_width,
            tooltip_top + tooltip_height,
            fill="#0b1220",
            outline="#1f2937",
            tags="hover-overlay",
        )
        for line_index, line in enumerate(lines):
            canvas.create_text(
                tooltip_left + 6,
                tooltip_top + 4 + (line_index * line_height),
                text=line,
                anchor="nw",
                fill="#f9fafb",
                font=("Consolas", 7),
                tags="hover-overlay",
            )


def _fill_deribit_tree(tree: ttk.Treeview, candles: list[DeribitVolatilityCandle]) -> None:
    tree.delete(*tree.get_children())
    for candle in reversed(candles):
        tree.insert(
            "",
            END,
            values=(
                _format_ts(candle.ts),
                format_decimal_fixed(candle.open, 2),
                format_decimal_fixed(candle.high, 2),
                format_decimal_fixed(candle.low, 2),
                format_decimal_fixed(candle.close, 2),
            ),
        )


def _fill_price_tree(tree: ttk.Treeview, candles: list[Candle]) -> None:
    tree.delete(*tree.get_children())
    for candle in reversed(candles):
        places = max(2, _decimal_places_generic([candle]))
        tree.insert(
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


def _align_candles_by_timestamp(
    volatility_candles: list[DeribitVolatilityCandle],
    spot_candles: list[Candle],
) -> tuple[list[DeribitVolatilityCandle], list[Candle]]:
    spot_map = {candle.ts: candle for candle in spot_candles}
    aligned_volatility: list[DeribitVolatilityCandle] = []
    aligned_spot: list[Candle] = []
    for candle in volatility_candles:
        spot_candle = spot_map.get(candle.ts)
        if spot_candle is None:
            continue
        aligned_volatility.append(candle)
        aligned_spot.append(spot_candle)
    return aligned_volatility, aligned_spot


def _aggregate_candles_to_resolution(
    candles: list[DeribitVolatilityCandle],
    resolution_ms: int,
    *,
    anchor_offset_ms: int = 0,
) -> list[DeribitVolatilityCandle]:
    if not candles:
        return []
    buckets: dict[int, list[DeribitVolatilityCandle]] = {}
    for candle in candles:
        bucket_ts = ((candle.ts - anchor_offset_ms) // resolution_ms) * resolution_ms + anchor_offset_ms
        buckets.setdefault(bucket_ts, []).append(candle)
    aggregated: list[DeribitVolatilityCandle] = []
    for bucket_ts in sorted(buckets):
        group = sorted(buckets[bucket_ts], key=lambda item: item.ts)
        aggregated.append(
            DeribitVolatilityCandle(
                ts=bucket_ts,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
            )
        )
    return aggregated


def _aggregate_price_candles_to_resolution(
    candles: list[Candle],
    resolution_ms: int,
    *,
    anchor_offset_ms: int = 0,
) -> list[Candle]:
    if not candles:
        return []
    buckets: dict[int, list[Candle]] = {}
    for candle in candles:
        bucket_ts = ((candle.ts - anchor_offset_ms) // resolution_ms) * resolution_ms + anchor_offset_ms
        buckets.setdefault(bucket_ts, []).append(candle)
    aggregated: list[Candle] = []
    for bucket_ts in sorted(buckets):
        group = sorted(buckets[bucket_ts], key=lambda item: item.ts)
        aggregated.append(
            Candle(
                ts=bucket_ts,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum((item.volume for item in group), Decimal("0")),
                confirmed=all(item.confirmed for item in group),
            )
        )
    return aggregated


def _to_average_volatility_candles(candles: list[DeribitVolatilityCandle]) -> list[DeribitVolatilityCandle]:
    if not candles:
        return []
    places = _decimal_places_generic(candles)
    quantum = Decimal("1").scaleb(-places)
    averaged: list[DeribitVolatilityCandle] = []
    previous_open: Decimal | None = None
    previous_close: Decimal | None = None
    divisor = Decimal("4")
    two = Decimal("2")
    for candle in candles:
        average_close = ((candle.open + candle.high + candle.low + candle.close) / divisor).quantize(quantum)
        if previous_open is None or previous_close is None:
            average_open = ((candle.open + candle.close) / two).quantize(quantum)
        else:
            average_open = ((previous_open + previous_close) / two).quantize(quantum)
        averaged.append(
            DeribitVolatilityCandle(
                ts=candle.ts,
                open=average_open,
                high=max(candle.high, average_open, average_close).quantize(quantum),
                low=min(candle.low, average_open, average_close).quantize(quantum),
                close=average_close,
            )
        )
        previous_open = average_open
        previous_close = average_close
    return averaged


def _to_average_price_candles(candles: list[Candle]) -> list[Candle]:
    if not candles:
        return []
    places = _decimal_places_generic(candles)
    quantum = Decimal("1").scaleb(-places)
    averaged: list[Candle] = []
    previous_open: Decimal | None = None
    previous_close: Decimal | None = None
    divisor = Decimal("4")
    two = Decimal("2")
    for candle in candles:
        average_close = ((candle.open + candle.high + candle.low + candle.close) / divisor).quantize(quantum)
        if previous_open is None or previous_close is None:
            average_open = ((candle.open + candle.close) / two).quantize(quantum)
        else:
            average_open = ((previous_open + previous_close) / two).quantize(quantum)
        averaged.append(
            Candle(
                ts=candle.ts,
                open=average_open,
                high=max(candle.high, average_open, average_close).quantize(quantum),
                low=min(candle.low, average_open, average_close).quantize(quantum),
                close=average_close,
                volume=candle.volume,
                confirmed=candle.confirmed,
            )
        )
        previous_open = average_open
        previous_close = average_close
    return averaged


def _resolution_file_label(label: str) -> str:
    return label.replace("小时", "h").replace("分钟", "m").replace("天", "d").replace("秒", "s")


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")


def _format_short_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")


def _sample_time_indices(count: int) -> list[int]:
    if count <= 1:
        return [0] if count == 1 else []
    sample_count = min(6, count)
    indices = {0, count - 1}
    for slot in range(1, sample_count - 1):
        indices.add(int(round((count - 1) * slot / (sample_count - 1))))
    return sorted(indices)


def _decimal_places_generic(candles: list[DeribitVolatilityCandle | Candle]) -> int:
    places = 2
    for candle in candles:
        for value in (candle.open, candle.high, candle.low, candle.close):
            value_places = max(0, -value.normalize().as_tuple().exponent)
            places = max(places, min(value_places, 6))
    return places


def _build_axis_values(minimum: Decimal, maximum: Decimal, *, steps: int) -> list[Decimal]:
    if steps <= 0:
        return []
    if maximum <= minimum:
        return [minimum]
    span = maximum - minimum
    return [minimum + (span * Decimal(index) / Decimal(steps)) for index in range(steps + 1)]


def _normalize_chart_viewport(
    start_index: int,
    visible_count: int | None,
    total_count: int,
    *,
    min_visible: int,
) -> tuple[int, int]:
    if total_count <= 0:
        return 0, max(min_visible, 1)
    normalized_visible = visible_count if visible_count is not None else total_count
    normalized_visible = max(min_visible, min(total_count, normalized_visible))
    normalized_start = max(0, min(start_index, total_count - normalized_visible))
    return normalized_start, normalized_visible


def _zoom_chart_viewport(
    *,
    start_index: int,
    visible_count: int | None,
    total_count: int,
    anchor_ratio: float,
    zoom_in: bool,
    min_visible: int,
) -> tuple[int, int]:
    current_start, current_visible = _normalize_chart_viewport(start_index, visible_count, total_count, min_visible=min_visible)
    if total_count <= 0:
        return current_start, current_visible
    step = max(1, int(round(current_visible * 0.2)))
    next_visible = current_visible - step if zoom_in else current_visible + step
    next_visible = max(min_visible, min(total_count, next_visible))
    anchor_index = current_start + int(round(anchor_ratio * max(current_visible - 1, 0)))
    next_start = anchor_index - int(round(anchor_ratio * max(next_visible - 1, 0)))
    return _normalize_chart_viewport(next_start, next_visible, total_count, min_visible=min_visible)


def _pan_chart_viewport(
    start_index: int,
    visible_count: int,
    total_count: int,
    index_delta: int,
    *,
    min_visible: int,
) -> int:
    next_start, _ = _normalize_chart_viewport(start_index + index_delta, visible_count, total_count, min_visible=min_visible)
    return next_start


def _nearest_linear_index(x: float, left: float, right: float, count: int) -> int:
    if count <= 1 or right <= left:
        return 0
    ratio = min(max((x - left) / (right - left), 0.0), 1.0)
    return min(count - 1, max(0, int(round(ratio * (count - 1)))))
