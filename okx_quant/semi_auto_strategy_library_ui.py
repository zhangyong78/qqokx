from __future__ import annotations

import json
from tkinter import BooleanVar, Canvas, StringVar, Toplevel, messagebox, ttk
from typing import Callable

from okx_quant.strategy_catalog import StrategyDefinition


_FIELD_LABELS = {
    "symbol": "交易标的",
    "bar": "K线周期",
    "signal_mode": "信号方向",
    "trade_mode": "保证金模式",
    "position_mode": "持仓模式",
    "risk_amount": "风险金额 (USDT)",
    "order_size": "下单数量",
    "atr_stop_multiplier": "止损 ATR 倍数",
    "atr_take_multiplier": "止盈 ATR 倍数",
    "take_profit_mode": "止盈模式",
    "tp_sl_mode": "止盈止损执行",
    "entry_side_mode": "开仓方向模式",
    "ema_period": "EMA 周期",
    "trend_ema_period": "趋势 EMA 周期",
    "atr_period": "ATR 周期",
    "entry_reference_ema_period": "挂单参考 EMA",
    "mtf_filter_bar": "多周期过滤周期",
    "mtf_filter_fast_ema_period": "多周期快 EMA",
    "mtf_filter_slow_ema_period": "多周期慢 EMA",
    "trend_ema_slope_filter_min_ratio": "趋势斜率阈值",
    "atr_percentile_filter_max": "ATR 分位上限",
    "body_retest_breakdown_atr_multiplier": "破位 ATR 倍数",
    "body_retest_retest_atr_multiplier": "回踩 ATR 倍数",
    "body_retest_stop_buffer_atr_multiplier": "止损缓冲 ATR 倍数",
    "body_retest_body_atr_limit": "实体 ATR 上限",
    "body_retest_watch_bars": "回踩等待 K 线数",
    "dynamic_protection_rules": "动态保护规则 (JSON)",
}

_COMMON_FIELD_KEYS = (
    "symbol",
    "bar",
    "signal_mode",
    "trade_mode",
    "position_mode",
    "risk_amount",
    "order_size",
    "atr_stop_multiplier",
    "atr_take_multiplier",
    "take_profit_mode",
    "tp_sl_mode",
)


def build_semi_auto_strategy_library_rows(
    definitions: tuple[StrategyDefinition, ...] | list[StrategyDefinition],
) -> list[tuple[str, tuple[str, str, str]]]:
    """Return the supplied built-in definitions in their library order."""
    return [
        (item.strategy_id, (item.name, item.default_signal_label, item.summary))
        for item in definitions
    ]


def build_semi_auto_strategy_parameter_payload(
    strategy_id: str,
    *,
    api_name: str,
    values: dict[str, object],
) -> dict[str, object]:
    return {"strategy_id": strategy_id, "api_name": api_name, **dict(values)}


class SemiAutoStrategyLibraryDialog:
    """Independent one-shot strategy draft dialog for a selected semi-auto pool."""

    def __init__(
        self,
        parent,
        *,
        definitions: tuple[StrategyDefinition, ...] | list[StrategyDefinition],
        initial_api_name: str,
        parameter_defaults_provider: Callable[[str], dict[str, object]],
        template_builder: Callable[[str, dict[str, object], str], object],
        on_confirm: Callable[[object], None],
    ) -> None:
        self._definitions = tuple(definitions)
        self._definition_by_id = {item.strategy_id: item for item in self._definitions}
        self._api_name = str(initial_api_name or "").strip()
        self._parameter_defaults_provider = parameter_defaults_provider
        self._template_builder = template_builder
        self._on_confirm = on_confirm
        self._selected_strategy_id = ""
        self._draft_vars: dict[str, StringVar | BooleanVar] = {}

        self.window = Toplevel(parent)
        self.window.title("从策略库添加一次性任务")
        self.window.geometry("980x760")
        self.window.minsize(820, 620)
        self.window.transient(parent)

        root = ttk.Frame(self.window, padding=12)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        root.rowconfigure(1, weight=1)

        library_frame = ttk.LabelFrame(root, text="内置策略库", padding=8)
        library_frame.grid(row=0, column=0, sticky="nsew")
        library_frame.columnconfigure(0, weight=1)
        library_frame.rowconfigure(0, weight=1)
        self.strategy_tree = ttk.Treeview(
            library_frame,
            columns=("name", "direction", "summary"),
            show="headings",
            selectmode="browse",
            height=9,
        )
        for key, label, width in (
            ("name", "策略", 220),
            ("direction", "默认方向", 100),
            ("summary", "说明", 600),
        ):
            self.strategy_tree.heading(key, text=label)
            self.strategy_tree.column(key, width=width, anchor="w")
        self.strategy_tree.grid(row=0, column=0, sticky="nsew")
        self.strategy_tree.bind("<<TreeviewSelect>>", self._on_strategy_select)
        for strategy_id, values in build_semi_auto_strategy_library_rows(self._definitions):
            self.strategy_tree.insert("", "end", iid=strategy_id, values=values)

        draft_frame = ttk.LabelFrame(root, text="本次一次性任务参数", padding=8)
        draft_frame.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        draft_frame.columnconfigure(0, weight=1)
        draft_frame.rowconfigure(0, weight=1)
        self._draft_canvas = Canvas(draft_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(draft_frame, orient="vertical", command=self._draft_canvas.yview)
        self._draft_canvas.configure(yscrollcommand=scrollbar.set)
        self._draft_canvas.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        self._field_container = ttk.Frame(self._draft_canvas)
        self._draft_canvas.create_window((0, 0), window=self._field_container, anchor="nw")
        self._field_container.bind("<Configure>", self._sync_draft_scroll_region)

        actions = ttk.Frame(root)
        actions.grid(row=2, column=0, sticky="e", pady=(10, 0))
        ttk.Button(actions, text="取消", command=self.window.destroy).pack(side="right")
        ttk.Button(actions, text="加入一次性任务", command=self._confirm).pack(side="right", padx=(0, 8))

        if self._definitions:
            first_id = self._definitions[0].strategy_id
            self.strategy_tree.selection_set(first_id)
            self._replace_draft(first_id)

    def _sync_draft_scroll_region(self, _event=None) -> None:
        self._draft_canvas.configure(scrollregion=self._draft_canvas.bbox("all"))

    def _on_strategy_select(self, _event=None) -> None:
        selected = self.strategy_tree.selection()
        if selected:
            self._replace_draft(str(selected[0]))

    def _replace_draft(self, strategy_id: str) -> None:
        if strategy_id not in self._definition_by_id:
            return
        self._selected_strategy_id = strategy_id
        defaults = dict(self._parameter_defaults_provider(strategy_id) or {})
        self._draft_vars = {}
        for child in self._field_container.winfo_children():
            child.destroy()

        ttk.Label(self._field_container, text="API（使用所选操盘组合，不能在此修改）").grid(
            row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 5)
        )
        ttk.Label(self._field_container, text=self._api_name or "-").grid(row=0, column=1, sticky="ew", pady=(0, 5))
        self._field_container.columnconfigure(1, weight=1)
        for row_index, key in enumerate(self._ordered_parameter_keys(defaults), start=1):
            value = defaults[key]
            ttk.Label(self._field_container, text=_FIELD_LABELS.get(key, key)).grid(
                row=row_index, column=0, sticky="nw", padx=(0, 8), pady=3
            )
            variable = self._new_draft_variable(value)
            self._draft_vars[key] = variable
            if isinstance(variable, BooleanVar):
                ttk.Checkbutton(self._field_container, variable=variable).grid(row=row_index, column=1, sticky="w", pady=3)
            else:
                width = 90 if key == "dynamic_protection_rules" else 34
                ttk.Entry(self._field_container, textvariable=variable, width=width).grid(
                    row=row_index, column=1, sticky="ew", pady=3
                )

    @staticmethod
    def _ordered_parameter_keys(defaults: dict[str, object]) -> tuple[str, ...]:
        common = [key for key in _COMMON_FIELD_KEYS if key in defaults]
        extras = [key for key in defaults if key not in _COMMON_FIELD_KEYS]
        return tuple(common + extras)

    def _new_draft_variable(self, value: object) -> StringVar | BooleanVar:
        if isinstance(value, bool):
            return BooleanVar(master=self.window, value=value)
        if isinstance(value, (tuple, list, dict)):
            text = json.dumps(value, ensure_ascii=False, default=str)
        elif value is None:
            text = ""
        else:
            text = str(value)
        return StringVar(master=self.window, value=text)

    def _draft_values(self) -> dict[str, object]:
        values: dict[str, object] = {key: variable.get() for key, variable in self._draft_vars.items()}
        raw_rules = values.get("dynamic_protection_rules")
        if isinstance(raw_rules, str):
            text = raw_rules.strip()
            if not text:
                values["dynamic_protection_rules"] = ()
            else:
                try:
                    values["dynamic_protection_rules"] = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ValueError("动态保护规则必须是有效 JSON。") from exc
        return values

    def _confirm(self) -> None:
        try:
            values = self._draft_values()
            record = self._template_builder(self._selected_strategy_id, values, self._api_name)
        except Exception as exc:
            messagebox.showerror("加入一次性任务失败", str(exc), parent=self.window)
            return
        self._on_confirm(record)
        self.window.destroy()
