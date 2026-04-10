from __future__ import annotations

import threading
from datetime import datetime
from decimal import Decimal
from tkinter import END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.log_utils import append_log_line
from okx_quant.models import Instrument
from okx_quant.okx_client import OkxPosition, OkxRestClient
from okx_quant.option_roll import (
    OptionRollSuggestion,
    OptionRollTransferPayload,
    RollPreference,
    RollStrikeScope,
    build_option_roll_suggestions,
    build_option_roll_transfer_payload,
)
from okx_quant.option_strategy import (
    OptionQuote,
    StrategyLegDefinition,
    estimate_leg_greeks,
    infer_implied_volatility_for_leg,
    option_contract_value,
    option_intrinsic_value_at_expiry,
    parse_option_contract,
    resolve_strategy_leg,
)
from okx_quant.pricing import format_decimal
from okx_quant.window_layout import apply_adaptive_window_geometry


Logger = Callable[[str], None]
SendToStrategyCallback = Callable[[OptionRollTransferPayload], None]

STRIKE_LEVEL_PRIORITY_OPTIONS: dict[str, int | None] = {
    "不限": None,
    "1档内优先": 1,
    "2档内优先": 2,
    "3档内优先": 3,
    "5档内优先": 5,
}

PREFERENCE_OPTIONS: dict[str, RollPreference] = {
    "优先净收权利金": "credit",
    "优先降低风险": "risk",
    "优先保持 Delta 接近": "delta",
    "优先时间价值": "time_value",
    "优先更近到期": "near_expiry",
}

STRIKE_SCOPE_OPTIONS: dict[str, RollStrikeScope] = {
    "更虚值优先": "safer_preferred",
    "同执行价优先": "same_preferred",
    "同行权价及更安全方向": "same_and_safer",
    "全部": "all",
}


def _fmt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value)


def _fmt_quote(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value.quantize(Decimal("0.0001")))


def _fmt_spread_ratio(value: Decimal | None) -> str:
    return _fmt_quote(value)


class OptionRollSuggestionWindow:
    def __init__(
        self,
        parent,
        client: OkxRestClient,
        *,
        position: OkxPosition,
        instrument: Instrument,
        quote: OptionQuote,
        api_name: str,
        send_to_strategy_callback: SendToStrategyCallback,
        logger: Logger | None = None,
    ) -> None:
        self.client = client
        self.logger = logger
        self._send_to_strategy_callback = send_to_strategy_callback

        self.window = Toplevel(parent)
        self.window.title("期权展期建议")
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.76,
            height_ratio=0.78,
            min_width=1180,
            min_height=760,
            max_width=1680,
            max_height=1080,
        )
        self.window.protocol("WM_DELETE_WINDOW", self.window.withdraw)

        self.preference_label = StringVar(value="优先净收权利金")
        self.strike_scope_label = StringVar(value="更虚值优先")
        self.strike_level_priority_label = StringVar(value="不限")
        self.max_results_label = StringVar(value="10")
        self.status_text = StringVar(value="请选择参数后扫描展期建议。")

        self._current_position: OkxPosition = position
        self._current_instrument: Instrument = instrument
        self._current_quote: OptionQuote = quote
        self._api_name = api_name
        self._request_id = 0
        self._suggestions: list[OptionRollSuggestion] = []
        self._candidate_instruments: dict[str, Instrument] = {}
        self._candidate_quotes: dict[str, OptionQuote] = {}

        self._summary_label: ttk.Label | None = None
        self._result_tree: ttk.Treeview | None = None
        self._detail_text: Text | None = None
        self._scan_button: ttk.Button | None = None
        self._send_button: ttk.Button | None = None

        self._build_ui()
        self.load_position(position=position, instrument=instrument, quote=quote, api_name=api_name, auto_scan=True)

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def load_position(
        self,
        *,
        position: OkxPosition,
        instrument: Instrument,
        quote: OptionQuote,
        api_name: str,
        auto_scan: bool = False,
    ) -> None:
        self._current_position = position
        self._current_instrument = instrument
        self._current_quote = quote
        self._api_name = api_name
        self._suggestions = []
        self._candidate_instruments = {}
        self._candidate_quotes = {}
        self._render_current_position_v2()
        self._render_results()
        self._set_detail("点击“扫描建议”查看候选展期方案。")
        self.status_text.set("已载入当前持仓，可开始扫描。")
        if auto_scan:
            self.start_scan()

    def _build_ui(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=1)
        self.window.rowconfigure(3, weight=1)

        current_frame = ttk.LabelFrame(self.window, text="当前持仓", padding=10)
        current_frame.grid(row=0, column=0, sticky="ew", padx=12, pady=(12, 8))
        current_frame.columnconfigure(0, weight=1)
        self._summary_label = ttk.Label(current_frame, justify="left")
        self._summary_label.grid(row=0, column=0, sticky="ew")

        filter_frame = ttk.LabelFrame(self.window, text="扫描条件", padding=10)
        filter_frame.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 8))
        filter_frame.columnconfigure(8, weight=1)

        ttk.Label(filter_frame, text="目标偏好").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            filter_frame,
            textvariable=self.preference_label,
            values=list(PREFERENCE_OPTIONS.keys()),
            state="readonly",
            width=20,
        ).grid(row=0, column=1, sticky="w", padx=(6, 16))

        ttk.Label(filter_frame, text="行权价方向").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            filter_frame,
            textvariable=self.strike_scope_label,
            values=list(STRIKE_SCOPE_OPTIONS.keys()),
            state="readonly",
            width=14,
        ).grid(row=0, column=3, sticky="w", padx=(6, 16))

        ttk.Label(filter_frame, text="档位优先").grid(row=0, column=4, sticky="w")
        ttk.Combobox(
            filter_frame,
            textvariable=self.strike_level_priority_label,
            values=list(STRIKE_LEVEL_PRIORITY_OPTIONS.keys()),
            state="readonly",
            width=12,
        ).grid(row=0, column=5, sticky="w", padx=(6, 16))

        ttk.Label(filter_frame, text="候选数量").grid(row=0, column=6, sticky="w")
        ttk.Combobox(
            filter_frame,
            textvariable=self.max_results_label,
            values=["10", "20", "30"],
            state="readonly",
            width=6,
        ).grid(row=0, column=7, sticky="w", padx=(6, 16))

        self._scan_button = ttk.Button(filter_frame, text="扫描建议", command=self.start_scan)
        self._scan_button.grid(row=0, column=8, sticky="e")

        ttk.Label(filter_frame, textvariable=self.status_text).grid(
            row=1,
            column=0,
            columnspan=9,
            sticky="w",
            pady=(8, 0),
        )

        result_frame = ttk.LabelFrame(self.window, text="建议结果", padding=10)
        result_frame.grid(row=2, column=0, sticky="nsew", padx=12, pady=(0, 8))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        columns = (
            "new_inst_id",
            "roll_type",
            "days_to_expiry",
            "net_credit",
            "candidate_bid",
            "candidate_ask",
            "candidate_mark",
            "new_delta",
            "risk_change",
            "price_gap",
            "reason",
        )
        tree = ttk.Treeview(result_frame, columns=columns, show="headings", height=10)
        headings = {
            "new_inst_id": "建议新仓",
            "roll_type": "展期方式",
            "days_to_expiry": "距到期天数",
            "net_credit": "预计净收/付",
            "candidate_bid": "买一价",
            "candidate_ask": "卖一价",
            "candidate_mark": "标记价",
            "new_delta": "新Delta",
            "risk_change": "风险变化",
            "spread_ratio": "价差比",
            "reason": "建议理由",
        }
        headings["price_gap"] = "价差"
        widths = {
            "new_inst_id": 220,
            "roll_type": 120,
            "days_to_expiry": 90,
            "net_credit": 110,
            "candidate_bid": 90,
            "candidate_ask": 90,
            "candidate_mark": 90,
            "new_delta": 90,
            "risk_change": 100,
            "price_gap": 90,
            "reason": 420,
        }
        for key in columns:
            tree.heading(key, text=headings[key])
            tree.column(key, width=widths[key], stretch=key == "reason", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        result_scroll = ttk.Scrollbar(result_frame, orient="vertical", command=tree.yview)
        result_scroll.grid(row=0, column=1, sticky="ns")
        tree.configure(yscrollcommand=result_scroll.set)
        tree.bind("<<TreeviewSelect>>", self._on_suggestion_selected)
        tree.bind("<Double-1>", lambda _event: self.send_selected_to_strategy())
        self._result_tree = tree

        detail_frame = ttk.LabelFrame(self.window, text="建议详情", padding=10)
        detail_frame.grid(row=3, column=0, sticky="nsew", padx=12, pady=(0, 12))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        detail_text = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        detail_text.configure(yscrollcommand=detail_scroll.set)
        button_row = ttk.Frame(detail_frame)
        button_row.grid(row=1, column=0, columnspan=2, sticky="e", pady=(8, 0))
        self._send_button = ttk.Button(button_row, text="送入期权策略计算器", command=self.send_selected_to_strategy)
        self._send_button.grid(row=0, column=0)
        self._detail_text = detail_text

    def _render_current_position_v2(self) -> None:
        if self._summary_label is None:
            return
        parsed = parse_option_contract(self._current_position.inst_id)
        current_mark_price = self._current_position.mark_price or self._current_quote.mark_price
        parts = [
            f"API配置：{self._api_name}",
            f"合约：{self._current_position.inst_id}",
            f"方向：{'卖方' if self._current_position.position < 0 else '买方'}",
            f"数量：{_fmt(abs(self._current_position.position))}",
            f"开仓价：{_fmt(self._current_position.avg_price)}",
            f"标记价：{_fmt_quote(current_mark_price)}",
            f"买一/卖一：{_fmt(self._current_quote.bid_price)} / {_fmt(self._current_quote.ask_price)}",
            f"类型：{'认购' if parsed.option_type == 'C' else '认沽'}",
            f"到期日：{parsed.expiry_label}",
            f"行权价：{_fmt(parsed.strike)}",
            f"Delta：{_fmt(self._estimate_current_delta())}",
            f"时间价值：{_fmt(self._estimate_current_time_value())}",
        ]
        self._summary_label.configure(text=" | ".join(parts))

    def _render_current_position(self) -> None:
        if self._summary_label is None:
            return
        parsed = parse_option_contract(self._current_position.inst_id)
        current_mark_price = self._current_position.mark_price or self._current_quote.mark_price
        parts = [
            f"API配置：{self._api_name}",
            f"合约：{self._current_position.inst_id}",
            f"方向：{'卖方' if self._current_position.position < 0 else '买方'}",
            f"数量：{_fmt(abs(self._current_position.position))}",
            f"开仓价：{_fmt(self._current_position.avg_price)}",
            f"标记价：{_fmt(current_mark_price)}",
            f"买一/卖一：{_fmt(self._current_quote.bid_price)} / {_fmt(self._current_quote.ask_price)}",
            f"类型：{'认购' if parsed.option_type == 'C' else '认沽'}",
            f"到期日：{parsed.expiry_label}",
            f"行权价：{_fmt(parsed.strike)}",
            f"Delta：{_fmt(self._estimate_current_delta())}",
            f"时间价值：{_fmt(self._estimate_current_time_value())}",
        ]
        self._summary_label.configure(text=" | ".join(parts))

    def _estimate_current_delta(self) -> Decimal | None:
        price = self._current_position.mark_price or self._current_quote.mark_price or self._current_quote.reference_price
        spot = self._current_quote.index_price
        quantity = abs(self._current_position.position)
        if price is None or spot is None or spot <= 0 or quantity <= 0:
            return None
        try:
            leg = resolve_strategy_leg(
                StrategyLegDefinition(
                    alias="TMP",
                    inst_id=self._current_position.inst_id,
                    side="sell",
                    quantity=quantity,
                    premium=price,
                ),
                instrument=self._current_instrument,
                quote=self._current_quote,
            )
            implied_vol = infer_implied_volatility_for_leg(
                leg,
                settlement_price=spot,
                valuation_time=datetime.now(),
            )
            if implied_vol is None:
                return None
            greeks = estimate_leg_greeks(
                leg,
                settlement_price=spot,
                valuation_time=datetime.now(),
                implied_volatility=implied_vol,
            )
            return greeks.delta
        except Exception:
            return None

    def _estimate_current_time_value(self) -> Decimal | None:
        price = self._current_position.mark_price or self._current_quote.mark_price or self._current_quote.reference_price
        spot = self._current_quote.index_price
        if price is None or spot is None or spot <= 0:
            return None
        parsed = parse_option_contract(self._current_position.inst_id)
        intrinsic = option_intrinsic_value_at_expiry(
            settlement_price=spot,
            strike=parsed.strike,
            option_type=parsed.option_type,
            contract_value=option_contract_value(self._current_instrument),
        )
        value = price - intrinsic
        return value if value > 0 else Decimal("0")

    def start_scan(self) -> None:
        if self._scan_button is not None:
            self._scan_button.configure(state="disabled")
        self._request_id += 1
        request_id = self._request_id
        self.status_text.set("正在扫描建议，请稍候...")
        self._log(f"[展期建议] 开始扫描 | {self._current_position.inst_id} | API={self._api_name}")
        worker = threading.Thread(target=self._scan_worker, args=(request_id,), daemon=True)
        worker.start()

    def _scan_worker(self, request_id: int) -> None:
        try:
            parsed = parse_option_contract(self._current_position.inst_id)
            settlement_price = self._current_quote.index_price or self._current_quote.mark_price or self._current_quote.last_price
            if settlement_price is None or settlement_price <= 0:
                raise ValueError("当前持仓缺少有效标的价格，无法生成展期建议。")

            family = parsed.inst_family
            instruments = self.client.get_option_instruments(inst_family=family)
            candidate_instruments = [item for item in instruments if item.state.strip().lower() == "live"]
            tickers = self.client.get_tickers("OPTION", inst_family=family)
            ticker_map = {item.inst_id: item for item in tickers}
            quote_map: dict[str, OptionQuote] = {}
            instrument_map: dict[str, Instrument] = {item.inst_id: item for item in candidate_instruments}
            for inst_id, ticker in ticker_map.items():
                instrument = instrument_map.get(inst_id)
                if instrument is None:
                    continue
                quote_map[inst_id] = OptionQuote(
                    instrument=instrument,
                    mark_price=ticker.mark,
                    bid_price=ticker.bid,
                    ask_price=ticker.ask,
                    last_price=ticker.last,
                    index_price=ticker.index,
                )

            suggestions = build_option_roll_suggestions(
                current_position=self._current_position,
                current_instrument=self._current_instrument,
                current_quote=self._current_quote,
                candidate_instruments=candidate_instruments,
                candidate_quotes_by_inst_id=quote_map,
                settlement_price=settlement_price,
                valuation_time=datetime.now(),
                preference=PREFERENCE_OPTIONS[self.preference_label.get()],
                strike_scope=STRIKE_SCOPE_OPTIONS[self.strike_scope_label.get()],
                preferred_strike_levels=STRIKE_LEVEL_PRIORITY_OPTIONS[self.strike_level_priority_label.get()],
                max_results=int(self.max_results_label.get()),
            )
            self.window.after(
                0,
                lambda result=suggestions, req=request_id, insts=instrument_map, quotes=quote_map: self._apply_scan_results(
                    req, result, insts, quotes
                ),
            )
        except Exception as exc:
            self.window.after(0, lambda error=exc, req=request_id: self._apply_scan_error(req, error))

    def _apply_scan_results(
        self,
        request_id: int,
        suggestions: list[OptionRollSuggestion],
        instruments: dict[str, Instrument],
        quotes: dict[str, OptionQuote],
    ) -> None:
        if request_id != self._request_id:
            return
        if self._scan_button is not None:
            self._scan_button.configure(state="normal")
        self._suggestions = suggestions
        self._candidate_instruments = instruments
        self._candidate_quotes = quotes
        self._render_results()
        if suggestions:
            self.status_text.set(f"已生成 {len(suggestions)} 条展期建议。")
            self._log(f"[展期建议] 扫描完成 | {self._current_position.inst_id} | 建议数={len(suggestions)}")
        else:
            self.status_text.set("未找到符合条件的展期建议。")
            self._set_detail("未找到符合条件的候选合约。建议放宽行权价方向、档位优先或稍后重试。")

    def _apply_scan_error(self, request_id: int, error: Exception) -> None:
        if request_id != self._request_id:
            return
        if self._scan_button is not None:
            self._scan_button.configure(state="normal")
        self.status_text.set(f"扫描失败：{error}")
        self._set_detail(f"扫描失败：{error}")
        self._log(f"[展期建议] 扫描失败 | {self._current_position.inst_id} | {error}")

    def _render_results(self) -> None:
        if self._result_tree is None:
            return
        self._result_tree.delete(*self._result_tree.get_children())
        for index, suggestion in enumerate(self._suggestions):
            self._result_tree.insert(
                "",
                END,
                iid=str(index),
                values=(
                    suggestion.new_inst_id,
                    suggestion.roll_type,
                    suggestion.days_to_expiry,
                    _fmt(suggestion.net_credit),
                    _fmt(suggestion.candidate_bid),
                    _fmt(suggestion.candidate_ask),
                    _fmt(suggestion.candidate_mark),
                    _fmt(suggestion.new_delta),
                    suggestion.risk_change,
                    _fmt_quote(suggestion.price_gap),
                    suggestion.reason,
                ),
            )
        if self._send_button is not None:
            self._send_button.configure(state="normal" if self._suggestions else "disabled")
        if self._suggestions and self._result_tree.get_children():
            first = self._result_tree.get_children()[0]
            self._result_tree.selection_set(first)
            self._result_tree.focus(first)
            self._on_suggestion_selected()
        elif not self._suggestions:
            self._set_detail("暂无建议结果。")

    def _on_suggestion_selected(self, _event=None) -> None:
        suggestion = self._selected_suggestion()
        if suggestion is None:
            self._set_detail("请选择一条展期建议。")
            return
        detail_lines = [
            f"当前合约：{suggestion.current_inst_id}",
            f"建议新仓：{suggestion.new_inst_id}",
            f"展期方式：{suggestion.roll_type}",
            f"平旧参考：{_fmt(suggestion.close_price)}（{suggestion.close_price_source}）",
            f"开新参考：{_fmt(suggestion.open_price)}（{suggestion.open_price_source}）",
            f"预计净收/净付：{_fmt(suggestion.net_credit)}",
            f"风险变化：{suggestion.risk_change}",
            f"新到期距今：{suggestion.days_to_expiry} 天",
            f"新Delta：{_fmt(suggestion.new_delta)}",
            f"时间价值：{_fmt(suggestion.current_time_value)} -> {_fmt(suggestion.new_time_value)}",
            f"盘口买一/卖一/标记：{_fmt(suggestion.candidate_bid)} / {_fmt(suggestion.candidate_ask)} / {_fmt(suggestion.candidate_mark)}",
            f"价差比：{_fmt_spread_ratio(suggestion.spread_ratio)}",
            f"建议理由：{suggestion.reason}",
        ]
        detail_lines[-2] = f"价差：{_fmt_quote(suggestion.price_gap)}"
        self._set_detail("\n".join(detail_lines))

    def send_selected_to_strategy(self) -> None:
        suggestion = self._selected_suggestion()
        if suggestion is None:
            messagebox.showinfo("送入策略计算器", "请先选择一条展期建议。", parent=self.window)
            return
        candidate_instrument = self._candidate_instruments.get(suggestion.new_inst_id)
        candidate_quote = self._candidate_quotes.get(suggestion.new_inst_id)
        if candidate_instrument is None or candidate_quote is None:
            messagebox.showerror("送入策略计算器失败", "候选合约数据缺失，请重新扫描。", parent=self.window)
            return
        payload = build_option_roll_transfer_payload(
            current_position=self._current_position,
            current_instrument=self._current_instrument,
            current_quote=self._current_quote,
            suggestion=suggestion,
            candidate_instrument=candidate_instrument,
            candidate_quote=candidate_quote,
        )
        self._send_to_strategy_callback(payload)
        self._log(f"[展期建议] 已送入策略计算器 | {suggestion.current_inst_id} -> {suggestion.new_inst_id}")
        self.status_text.set("已送入期权策略计算器，可继续查看盈亏图。")

    def _selected_suggestion(self) -> OptionRollSuggestion | None:
        if self._result_tree is None:
            return None
        selection = self._result_tree.selection()
        if not selection:
            return None
        try:
            return self._suggestions[int(selection[0])]
        except Exception:
            return None

    def _set_detail(self, text: str) -> None:
        if self._detail_text is None:
            return
        self._detail_text.configure(state="normal")
        self._detail_text.delete("1.0", END)
        self._detail_text.insert("1.0", text)
        self._detail_text.configure(state="disabled")

    def _log(self, message: str) -> None:
        if self.logger is not None:
            self.logger(message)
        else:
            append_log_line(message)
