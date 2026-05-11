from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from tkinter import BOTH, END, Canvas, StringVar, Text, Toplevel
from tkinter import messagebox, ttk

from okx_quant.okx_client import OkxPosition, OkxRestClient
from okx_quant.position_protection import (
    OptionProtectionConfig,
    ProtectionReplayEvent,
    ProtectionReplayPoint,
    ProtectionReplayResult,
    replay_option_protection,
    uses_underlying_price_trigger,
)
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.window_layout import apply_adaptive_window_geometry


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]


@dataclass(frozen=True)
class ProtectionReplayLaunchState:
    bar: str = "15m"
    candle_limit: str = "120"


class ProtectionReplayWindow:
    def __init__(
        self,
        parent,
        client: OkxRestClient,
        position: OkxPosition,
        protection: OptionProtectionConfig,
        *,
        initial_state: ProtectionReplayLaunchState | None = None,
    ) -> None:
        self.client = client
        self.position = position
        self.protection = protection
        self.initial_state = initial_state or ProtectionReplayLaunchState()
        self.window = Toplevel(parent)
        self.window.title("期权持仓保护回放模拟")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.78,
            height_ratio=0.8,
            min_width=1080,
            min_height=760,
            max_width=1540,
            max_height=1060,
        )

        self.bar = StringVar(value=self.initial_state.bar)
        self.candle_limit = StringVar(value=self.initial_state.candle_limit)
        self.summary_text = StringVar(value="点击“开始回放”后，会按当前保护参数回放最近一段历史 K 线。")
        self._latest_points: list[ProtectionReplayPoint] = []
        self._latest_result: ProtectionReplayResult | None = None
        self._latest_trigger_label = protection.trigger_label or protection.trigger_inst_id

        self._build_layout()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=1)
        self.window.rowconfigure(3, weight=1)

        header = ttk.Frame(self.window, padding=(16, 16, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="期权持仓保护回放模拟", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.summary_text, justify="right").grid(row=0, column=1, sticky="e")

        controls = ttk.LabelFrame(self.window, text="当前保护参数", padding=14)
        controls.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))
        for column in range(6):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="期权合约").grid(row=0, column=0, sticky="w")
        ttk.Label(controls, text=self.position.inst_id).grid(row=0, column=1, sticky="w")
        ttk.Label(controls, text="触发源").grid(row=0, column=2, sticky="w")
        ttk.Label(controls, text=self._latest_trigger_label).grid(row=0, column=3, sticky="w")
        ttk.Label(controls, text="方向").grid(row=0, column=4, sticky="w")
        ttk.Label(controls, text=self.protection.direction.upper()).grid(row=0, column=5, sticky="w")

        ttk.Label(controls, text="止盈触发").grid(row=1, column=0, sticky="w", pady=(12, 0))
        ttk.Label(controls, text=_fmt_optional(self.protection.take_profit_trigger)).grid(row=1, column=1, sticky="w", pady=(12, 0))
        ttk.Label(controls, text="止损触发").grid(row=1, column=2, sticky="w", pady=(12, 0))
        ttk.Label(controls, text=_fmt_optional(self.protection.stop_loss_trigger)).grid(row=1, column=3, sticky="w", pady=(12, 0))
        ttk.Label(controls, text="持仓量").grid(row=1, column=4, sticky="w", pady=(12, 0))
        ttk.Label(controls, text=format_decimal(abs(self.position.position))).grid(row=1, column=5, sticky="w", pady=(12, 0))

        ttk.Label(controls, text="K线周期").grid(row=2, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(controls, textvariable=self.bar, values=BAR_OPTIONS, state="readonly").grid(
            row=2, column=1, sticky="ew", pady=(12, 0), padx=(0, 12)
        )
        ttk.Label(controls, text="回放K线数").grid(row=2, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(controls, textvariable=self.candle_limit).grid(row=2, column=3, sticky="ew", pady=(12, 0), padx=(0, 12))
        ttk.Button(controls, text="开始回放", command=self.start_replay).grid(row=2, column=4, pady=(12, 0), padx=(0, 8))
        ttk.Button(controls, text="关闭", command=self.window.destroy).grid(row=2, column=5, pady=(12, 0), sticky="e")

        ttk.Label(
            controls,
            text=(
                "说明：回放基于已收盘 K 线 close。期权侧取标记价格 K 线；现货触发时取现货 K 线。"
                "回放默认假设 IOC 按计算出的平仓价立即成交。"
            ),
            wraplength=1020,
            justify="left",
        ).grid(row=3, column=0, columnspan=6, sticky="w", pady=(12, 0))

        report_frame = ttk.Panedwindow(self.window, orient="horizontal")
        report_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 8))

        summary_frame = ttk.LabelFrame(report_frame, text="回放结果", padding=12)
        event_frame = ttk.LabelFrame(report_frame, text="事件列表", padding=12)
        report_frame.add(summary_frame, weight=1)
        report_frame.add(event_frame, weight=1)

        summary_frame.columnconfigure(0, weight=1)
        summary_frame.rowconfigure(0, weight=1)
        event_frame.columnconfigure(0, weight=1)
        event_frame.rowconfigure(0, weight=1)

        self.report_text = Text(summary_frame, wrap="word", font=("Consolas", 10), height=16)
        self.report_text.grid(row=0, column=0, sticky="nsew")

        self.event_tree = ttk.Treeview(
            event_frame,
            columns=("time", "type", "trigger", "mark", "order", "remaining", "message"),
            show="headings",
            selectmode="browse",
        )
        self.event_tree.heading("time", text="时间")
        self.event_tree.heading("type", text="事件")
        self.event_tree.heading("trigger", text="触发价")
        self.event_tree.heading("mark", text="期权标记价")
        self.event_tree.heading("order", text="报单价")
        self.event_tree.heading("remaining", text="剩余仓位")
        self.event_tree.heading("message", text="说明")
        self.event_tree.column("time", width=140, anchor="center")
        self.event_tree.column("type", width=80, anchor="center")
        self.event_tree.column("trigger", width=110, anchor="e")
        self.event_tree.column("mark", width=110, anchor="e")
        self.event_tree.column("order", width=110, anchor="e")
        self.event_tree.column("remaining", width=100, anchor="e")
        self.event_tree.column("message", width=300, anchor="w")
        self.event_tree.grid(row=0, column=0, sticky="nsew")
        event_scroll = ttk.Scrollbar(event_frame, orient="vertical", command=self.event_tree.yview)
        event_scroll.grid(row=0, column=1, sticky="ns")
        self.event_tree.configure(yscrollcommand=event_scroll.set)

        chart_frame = ttk.LabelFrame(self.window, text="触发价回放图", padding=12)
        chart_frame.grid(row=3, column=0, sticky="nsew", padx=16, pady=(0, 16))
        chart_frame.columnconfigure(0, weight=1)
        chart_frame.rowconfigure(0, weight=1)
        self.chart_canvas = Canvas(chart_frame, background="#ffffff", highlightthickness=0)
        self.chart_canvas.grid(row=0, column=0, sticky="nsew")

    def start_replay(self) -> None:
        try:
            candle_limit = self._parse_positive_int(self.candle_limit.get(), "回放K线数")
        except Exception as exc:
            messagebox.showerror("回放参数错误", str(exc), parent=self.window)
            return

        self.summary_text.set("正在拉取历史数据并回放，请稍候...")
        self.report_text.delete("1.0", END)
        self.event_tree.delete(*self.event_tree.get_children())
        self.chart_canvas.delete("all")

        threading.Thread(
            target=self._run_replay_worker,
            args=(self.bar.get().strip(), candle_limit),
            daemon=True,
        ).start()

    def _run_replay_worker(self, bar: str, candle_limit: int) -> None:
        try:
            points = self._load_replay_points(bar=bar, candle_limit=candle_limit)
            instrument = self.client.get_instrument(self.protection.option_inst_id)
            result = replay_option_protection(
                protection=self.protection,
                initial_position=abs(self.position.position),
                tick_size=instrument.tick_size,
                lot_size=instrument.lot_size,
                min_size=instrument.min_size,
                points=points,
            )
            self.window.after(0, lambda: self._apply_replay_result(points, result))
        except Exception as exc:
            self.window.after(0, lambda err=exc: self._show_replay_error(err))

    def _load_replay_points(self, *, bar: str, candle_limit: int) -> list[ProtectionReplayPoint]:
        option_mark_candles = self.client.get_mark_price_candles(self.protection.option_inst_id, bar, limit=candle_limit)
        if uses_underlying_price_trigger(self.protection):
            if self.protection.trigger_price_type == "mark":
                trigger_candles = self.client.get_mark_price_candles(self.protection.trigger_inst_id, bar, limit=candle_limit)
            else:
                trigger_candles = self.client.get_candles(self.protection.trigger_inst_id, bar, limit=candle_limit)
        else:
            trigger_candles = option_mark_candles

        option_map = {item.ts: item for item in option_mark_candles if item.confirmed}
        trigger_map = {item.ts: item for item in trigger_candles if item.confirmed}
        matched_ts = sorted(set(option_map).intersection(trigger_map))
        points = [
            ProtectionReplayPoint(
                ts=ts,
                trigger_price=trigger_map[ts].close,
                option_mark_price=option_map[ts].close,
            )
            for ts in matched_ts
        ]
        if not points:
            raise RuntimeError("没有拿到可用于回放的已收盘 K 线数据。")
        return points

    def _apply_replay_result(self, points: list[ProtectionReplayPoint], result: ProtectionReplayResult) -> None:
        self._latest_points = points
        self._latest_result = result
        self.summary_text.set(
            f"状态：{_status_label(result.status)} | 周期：{self.bar.get().strip()} | K线数：{len(points)}"
        )
        self.report_text.delete("1.0", END)
        self.report_text.insert("1.0", self._format_report(points, result))

        self.event_tree.delete(*self.event_tree.get_children())
        for index, event in enumerate(result.events, start=1):
            self.event_tree.insert(
                "",
                END,
                iid=f"E{index:03d}",
                values=(
                    _format_ts(event.ts),
                    _event_type_label(event.event_type),
                    format_decimal(event.trigger_price),
                    format_decimal(event.option_mark_price),
                    _fmt_optional(event.order_price),
                    _fmt_optional(event.remaining_position),
                    event.message,
                ),
            )

        self._draw_chart(points, result)

    def _show_replay_error(self, exc: Exception) -> None:
        self.summary_text.set("回放失败")
        messagebox.showerror("回放失败", str(exc), parent=self.window)

    def _format_report(self, points: list[ProtectionReplayPoint], result: ProtectionReplayResult) -> str:
        lines = [
            f"期权合约：{self.position.inst_id}",
            f"触发源：{self._latest_trigger_label}",
            f"回放周期：{self.bar.get().strip()} | 已收盘K线：{len(points)}",
            f"初始持仓：{format_decimal(result.initial_position)} | 最终剩余：{format_decimal(result.final_position)}",
            f"平仓方向：{result.close_side.upper()}",
            f"状态：{_status_label(result.status)}",
            f"结果摘要：{result.summary}",
        ]
        if result.trigger_ts is not None:
            lines.extend(
                [
                    f"触发时间：{_format_ts(result.trigger_ts)}",
                    f"触发价格：{format_decimal(result.trigger_price or Decimal('0'))}",
                    f"平仓报单价：{_fmt_optional(result.close_order_price)}",
                    f"成交价：{_fmt_optional(result.fill_price)}",
                ]
            )
        lines.extend(
            [
                "",
                "假设说明：",
                "1. 只使用已收盘 K 线 close 回放，不模拟盘中穿越。",
                "2. 期权报单价按当根期权标记价和滑点规则计算。",
                "3. 默认假设 IOC 以计算价立即成交，不模拟订单簿深度。",
            ]
        )
        return "\n".join(lines)

    def _draw_chart(self, points: list[ProtectionReplayPoint], result: ProtectionReplayResult) -> None:
        canvas = self.chart_canvas
        canvas.delete("all")
        if not points:
            return

        canvas.update_idletasks()
        width = max(canvas.winfo_width(), 960)
        height = max(canvas.winfo_height(), 300)
        left = 56
        right = 20
        top = 20
        bottom = 30
        inner_width = width - left - right
        inner_height = height - top - bottom
        if inner_width <= 0 or inner_height <= 0:
            return

        trigger_values = [point.trigger_price for point in points]
        price_candidates = list(trigger_values)
        if self.protection.take_profit_trigger is not None:
            price_candidates.append(self.protection.take_profit_trigger)
        if self.protection.stop_loss_trigger is not None:
            price_candidates.append(self.protection.stop_loss_trigger)
        price_max = max(float(value) for value in price_candidates)
        price_min = min(float(value) for value in price_candidates)
        if price_max == price_min:
            price_max += 1
            price_min -= 1

        def y_for(price: Decimal) -> float:
            ratio = (price_max - float(price)) / (price_max - price_min)
            return top + (ratio * inner_height)

        step = inner_width / max(len(points) - 1, 1)
        canvas.create_rectangle(left, top, width - right, height - bottom, outline="#d0d7de")
        canvas.create_text(left - 8, top, text=format_decimal_fixed(Decimal(str(price_max)), 2), anchor="e", fill="#57606a")
        canvas.create_text(left - 8, height - bottom, text=format_decimal_fixed(Decimal(str(price_min)), 2), anchor="e", fill="#57606a")

        line_points: list[float] = []
        for index, point in enumerate(points):
            x = left + (index * step)
            line_points.extend([x, y_for(point.trigger_price)])
        if len(line_points) >= 4:
            canvas.create_line(*line_points, fill="#0969da", width=2, smooth=False)

        if self.protection.take_profit_trigger is not None:
            y = y_for(self.protection.take_profit_trigger)
            canvas.create_line(left, y, width - right, y, fill="#1a7f37", dash=(6, 4))
            canvas.create_text(width - right, y - 8, text=f"止盈 {format_decimal(self.protection.take_profit_trigger)}", anchor="ne", fill="#1a7f37")
        if self.protection.stop_loss_trigger is not None:
            y = y_for(self.protection.stop_loss_trigger)
            canvas.create_line(left, y, width - right, y, fill="#cf222e", dash=(6, 4))
            canvas.create_text(width - right, y - 8, text=f"止损 {format_decimal(self.protection.stop_loss_trigger)}", anchor="ne", fill="#cf222e")

        if result.trigger_index is not None and 0 <= result.trigger_index < len(points):
            x = left + (result.trigger_index * step)
            canvas.create_line(x, top, x, height - bottom, fill="#bf8700", dash=(4, 4))
            canvas.create_text(x + 4, top + 4, text="触发", anchor="nw", fill="#bf8700")

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        cleaned = raw.strip()
        try:
            value = int(cleaned)
        except Exception as exc:
            raise ValueError(f"{field_name} 不是有效整数") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value


def _fmt_optional(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value)


def _status_label(status: str) -> str:
    if status == "filled":
        return "已触发并完成平仓"
    if status == "not_triggered":
        return "未触发"
    return "触发后出错"


def _event_type_label(event_type: str) -> str:
    if event_type == "trigger":
        return "触发"
    if event_type == "fill":
        return "成交"
    return "异常"


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000).strftime("%m-%d %H:%M")
