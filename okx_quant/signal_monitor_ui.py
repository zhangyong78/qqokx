from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.persistence import signal_observer_presets_file_path, signal_observer_templates_file_path
from okx_quant.strategy_catalog import get_strategy_definition, resolve_dynamic_signal_mode
from okx_quant.strategy_parameters import (
    iter_strategy_parameter_keys,
    strategy_fixed_value,
    strategy_is_parameter_editable,
    strategy_parameter_default_value,
)
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]
CurrentTemplateFactory = Callable[[], object]
TemplateSerializer = Callable[[object], dict[str, object]]
TemplateDeserializer = Callable[[dict[str, object]], object | None]
TemplateCloner = Callable[[object, str], object]
TemplateLauncher = Callable[[object, str], str]
SessionProvider = Callable[[], list[dict[str, str]]]
SessionStopper = Callable[[list[str]], None]
SessionDeleter = Callable[[list[str]], tuple[int, list[str]]]
SessionLogOpener = Callable[[str], None]
SessionChartOpener = Callable[[str], None]

DEFAULT_SIGNAL_OBSERVER_SYMBOLS: tuple[str, ...] = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
_SIGNAL_MODE_VALUE_TO_LABEL = {
    "both": "双向",
    "long_only": "只做多",
    "short_only": "只做空",
}
_SIGNAL_MODE_LABEL_TO_VALUE = {label: value for value, label in _SIGNAL_MODE_VALUE_TO_LABEL.items()}
_TAKE_PROFIT_MODE_VALUE_TO_LABEL = {
    "fixed": "固定止盈",
    "dynamic": "动态止盈",
}
_TAKE_PROFIT_MODE_LABEL_TO_VALUE = {label: value for value, label in _TAKE_PROFIT_MODE_VALUE_TO_LABEL.items()}
_OBSERVER_PARAMETER_LABELS = {
    "bar": "观察周期",
    "signal_mode": "开仓方向",
    "ema_period": "快EMA周期",
    "trend_ema_period": "中EMA周期",
    "big_ema_period": "大EMA周期",
    "atr_period": "ATR周期",
    "atr_stop_multiplier": "止损 ATR 倍数",
    "atr_take_multiplier": "止盈 ATR 倍数",
    "entry_reference_ema_period": "进场参考EMA",
    "take_profit_mode": "出场方式",
    "max_entries_per_trend": "单波最多开仓次数",
    "dynamic_two_r_break_even": "2R后保本",
    "dynamic_fee_offset_enabled": "保本加手续费缓冲",
    "time_stop_break_even_enabled": "超时启用保本",
    "time_stop_break_even_bars": "超时K线根数",
    "startup_chase_window_seconds": "启动追单窗口",
}
_OBSERVER_PARAMETER_ORDER: tuple[str, ...] = tuple(_OBSERVER_PARAMETER_LABELS.keys())
_OBSERVER_PARAMETER_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("基础设定", ("bar", "signal_mode", "startup_chase_window_seconds")),
    ("趋势过滤", ("ema_period", "trend_ema_period", "big_ema_period", "atr_period")),
    (
        "进场与保护",
        (
            "entry_reference_ema_period",
            "max_entries_per_trend",
            "atr_stop_multiplier",
            "atr_take_multiplier",
            "take_profit_mode",
            "dynamic_two_r_break_even",
            "dynamic_fee_offset_enabled",
            "time_stop_break_even_enabled",
            "time_stop_break_even_bars",
        ),
    ),
)
_OBSERVER_PARAMETER_GROUP_BY_KEY = {
    key: group_name
    for group_name, keys in _OBSERVER_PARAMETER_GROUPS
    for key in keys
}


@dataclass
class _ObserverDraft:
    draft_id: str
    template_payload: dict[str, object]
    created_at: datetime
    updated_at: datetime


@dataclass
class _ObserverPreset:
    preset_name: str
    template_payload: dict[str, object]
    created_at: datetime
    updated_at: datetime


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _normalize_template_payload(payload: dict[str, object]) -> dict[str, object]:
    normalized = dict(payload)
    config_snapshot = normalized.get("config_snapshot")
    if not isinstance(config_snapshot, dict):
        return normalized
    strategy_id = str(normalized.get("strategy_id") or config_snapshot.get("strategy_id") or "").strip()
    if not strategy_id:
        return normalized
    normalized_snapshot = dict(config_snapshot)
    for key in iter_strategy_parameter_keys(strategy_id):
        fixed_value = strategy_fixed_value(strategy_id, key)
        if fixed_value is not None:
            normalized_snapshot[key] = fixed_value
    fixed_signal_mode = strategy_fixed_value(strategy_id, "signal_mode")
    if fixed_signal_mode is not None:
        normalized_snapshot["signal_mode"] = str(fixed_signal_mode)
    else:
        normalized_snapshot["signal_mode"] = resolve_dynamic_signal_mode(
            strategy_id,
            str(normalized_snapshot.get("signal_mode") or "both"),
        )
    normalized["config_snapshot"] = normalized_snapshot
    normalized["strategy_id"] = strategy_id
    if not str(normalized.get("strategy_name") or "").strip():
        try:
            normalized["strategy_name"] = get_strategy_definition(strategy_id).name
        except KeyError:
            pass
    if fixed_signal_mode is not None:
        normalized["direction_label"] = _SIGNAL_MODE_VALUE_TO_LABEL.get(str(fixed_signal_mode), str(fixed_signal_mode))
    else:
        sm = str(normalized_snapshot.get("signal_mode") or "both")
        try:
            default_label = get_strategy_definition(strategy_id).default_signal_label
        except KeyError:
            default_label = sm
        normalized["direction_label"] = _SIGNAL_MODE_VALUE_TO_LABEL.get(sm, default_label)
    if not str(normalized.get("run_mode_label") or "").strip():
        normalized["run_mode_label"] = "只发邮件"
    return normalized


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    return normalized in {"1", "true", "yes", "on"}


class SignalMonitorWindow:
    def __init__(
        self,
        parent,
        *,
        logger: Logger,
        current_template_factory: CurrentTemplateFactory,
        template_serializer: TemplateSerializer,
        template_deserializer: TemplateDeserializer,
        template_symbol_cloner: TemplateCloner,
        template_launcher: TemplateLauncher,
        session_provider: SessionProvider,
        session_stopper: SessionStopper,
        session_deleter: SessionDeleter,
        session_log_opener: SessionLogOpener,
        session_chart_opener: SessionChartOpener,
    ) -> None:
        self._logger = logger
        self._current_template_factory = current_template_factory
        self._template_serializer = template_serializer
        self._template_deserializer = template_deserializer
        self._template_symbol_cloner = template_symbol_cloner
        self._template_launcher = template_launcher
        self._session_provider = session_provider
        self._session_stopper = session_stopper
        self._session_deleter = session_deleter
        self._session_log_opener = session_log_opener
        self._session_chart_opener = session_chart_opener
        self._drafts: list[_ObserverDraft] = []
        self._presets: list[_ObserverPreset] = []
        self._draft_counter = 0
        self._refresh_job: str | None = None

        self._symbol_vars: dict[str, BooleanVar] = {
            symbol: BooleanVar(value=symbol in {"BTC-USDT-SWAP", "ETH-USDT-SWAP"})
            for symbol in DEFAULT_SIGNAL_OBSERVER_SYMBOLS
        }
        self._custom_symbols = StringVar(value="")
        self._status_text = StringVar(value="信号 0 条 | 运行中 0 条")
        self._editor_status_text = StringVar(value="请选择左侧一条观察模板，右侧可微调周期、趋势和保护参数。")
        self._editor_strategy_text = StringVar(value="-")
        self._editor_api_text = StringVar(value="-")
        self._editor_symbol = StringVar(value="")
        self._editor_run_mode_text = StringVar(value="只发邮件")
        self._preset_name = StringVar(value="")
        self._preset_choice = StringVar(value="")
        self._editor_parameter_vars: dict[str, StringVar | BooleanVar] = {}
        for key in _OBSERVER_PARAMETER_ORDER:
            default_value = strategy_parameter_default_value(key)
            if isinstance(default_value, bool):
                self._editor_parameter_vars[key] = BooleanVar(value=default_value)
            else:
                self._editor_parameter_vars[key] = StringVar(value="" if default_value is None else str(default_value))
        self._editor_parameter_labels: dict[str, ttk.Label] = {}
        self._editor_parameter_inputs: dict[str, object] = {}
        self._editor_group_frames: dict[str, ttk.Frame] = {}
        self._editor_group_placeholders: dict[str, ttk.Label] = {}

        self.window = Toplevel(parent)
        self.window.title("信号观察台")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.78,
            height_ratio=0.78,
            min_width=1160,
            min_height=760,
            max_width=1700,
            max_height=1100,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.draft_tree: ttk.Treeview | None = None
        self.session_tree: ttk.Treeview | None = None
        self.log_text: Text | None = None
        self._editor_save_button: ttk.Button | None = None
        self._preset_combo: ttk.Combobox | None = None
        self._session_tree_hover_tip_window: Toplevel | None = None
        self._session_tree_hover_tip_label: ttk.Label | None = None

        try:
            self._build_layout()
            self._load_drafts()
            self._load_presets()
            self._refresh_views()
            self._schedule_refresh()
        except Exception:
            try:
                if self.window.winfo_exists():
                    self.window.destroy()
            except Exception:
                pass
            raise

    def show(self) -> None:
        if not self.window.winfo_exists():
            return
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()
        self._refresh_views()

    def destroy(self) -> None:
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _on_close(self) -> None:
        self.window.withdraw()

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)
        self.window.rowconfigure(2, weight=0)

        header = ttk.Frame(self.window, padding=(16, 14, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="信号观察台", font=("Microsoft YaHei UI", 18, "bold")).grid(
            row=0,
            column=0,
            sticky="w",
        )
        ttk.Label(header, textvariable=self._status_text).grid(row=0, column=1, sticky="e")
        ttk.Label(
            header,
            text="统一管理 signal_only 信号，支持多币种批量启动，只观察不下单。",
            foreground="#556070",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))

        body = ttk.Frame(self.window, padding=(16, 0, 16, 12))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        left = ttk.LabelFrame(body, text="观察信号", padding=12)
        left.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 12))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(2, weight=1)

        toolbar = ttk.Frame(left)
        toolbar.grid(row=0, column=0, sticky="ew")
        ttk.Button(toolbar, text="加入当前参数", command=self.add_current_template).grid(row=0, column=0)
        ttk.Button(toolbar, text="复制到勾选币种", command=self.clone_selected_to_symbols).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(toolbar, text="启动选中信号", command=self.start_selected_drafts).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(toolbar, text="启动全部信号", command=self.start_all_drafts).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(toolbar, text="删除选中信号", command=self.delete_selected_drafts).grid(row=0, column=4, padx=(8, 0))

        symbol_frame = ttk.LabelFrame(left, text="默认批量币种", padding=10)
        symbol_frame.grid(row=1, column=0, sticky="ew", pady=(10, 10))
        for idx, symbol in enumerate(DEFAULT_SIGNAL_OBSERVER_SYMBOLS):
            ttk.Checkbutton(symbol_frame, text=symbol, variable=self._symbol_vars[symbol]).grid(
                row=idx // 3,
                column=idx % 3,
                sticky="w",
                padx=(0, 14),
                pady=(0, 6),
            )
        ttk.Label(symbol_frame, text="额外币种（逗号分隔）").grid(row=2, column=0, sticky="w", pady=(4, 0))
        ttk.Entry(symbol_frame, textvariable=self._custom_symbols).grid(row=2, column=1, columnspan=2, sticky="ew", pady=(4, 0))
        symbol_frame.columnconfigure(1, weight=1)

        self.draft_tree = ttk.Treeview(
            left,
            columns=("draft", "strategy", "symbol", "bar", "api", "mode", "updated"),
            show="headings",
            selectmode="extended",
            height=18,
        )
        self.draft_tree.grid(row=2, column=0, sticky="nsew")
        for column, text, width, anchor in (
            ("draft", "信号", 90, "center"),
            ("strategy", "策略", 180, "w"),
            ("symbol", "标的", 160, "w"),
            ("bar", "周期", 70, "center"),
            ("api", "API", 90, "center"),
            ("mode", "模式", 90, "center"),
            ("updated", "更新时间", 150, "center"),
        ):
            self.draft_tree.heading(column, text=text)
            self.draft_tree.column(column, width=width, anchor=anchor)
        draft_scroll = ttk.Scrollbar(left, orient="vertical", command=self.draft_tree.yview)
        draft_scroll.grid(row=2, column=1, sticky="ns")
        self.draft_tree.configure(yscrollcommand=draft_scroll.set)
        self.draft_tree.bind("<<TreeviewSelect>>", lambda *_: self._refresh_editor_from_selection())

        upper_right = ttk.LabelFrame(body, text="运行中的 signal_only 会话", padding=12)
        upper_right.grid(row=0, column=1, sticky="nsew")
        upper_right.columnconfigure(0, weight=1)
        upper_right.rowconfigure(1, weight=1)
        run_toolbar = ttk.Frame(upper_right)
        run_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(run_toolbar, text="刷新", command=self._refresh_views).grid(row=0, column=0)
        ttk.Button(run_toolbar, text="停止选中会话", command=self.stop_selected_sessions).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(run_toolbar, text="删除选中会话", command=self.delete_selected_sessions).grid(row=0, column=2, padx=(8, 0))
        self.session_tree = ttk.Treeview(
            upper_right,
            columns=("session", "strategy", "symbol", "api", "status", "last"),
            show="headings",
            selectmode="extended",
            height=14,
        )
        self.session_tree.grid(row=1, column=0, sticky="nsew")
        for column, text, width, anchor in (
            ("session", "会话(双击日志)", 80, "center"),
            ("strategy", "策略", 150, "w"),
            ("symbol", "标的(双击K线)", 150, "w"),
            ("api", "API", 90, "center"),
            ("status", "状态", 100, "center"),
            ("last", "最近消息", 280, "w"),
        ):
            self.session_tree.heading(column, text=text)
            self.session_tree.column(column, width=width, anchor=anchor)
        session_scroll = ttk.Scrollbar(upper_right, orient="vertical", command=self.session_tree.yview)
        session_scroll.grid(row=1, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=session_scroll.set)
        self.session_tree.bind("<Double-1>", self._on_session_tree_double_click)
        self.session_tree.bind("<Motion>", self._on_session_tree_hover)
        self.session_tree.bind("<Leave>", self._on_session_tree_hover_leave)

        lower_right = ttk.LabelFrame(body, text="模板详情", padding=12)
        lower_right.grid(row=1, column=1, sticky="nsew", pady=(12, 0))
        lower_right.columnconfigure(1, weight=1)
        lower_right.rowconfigure(6, weight=1)
        ttk.Label(lower_right, textvariable=self._editor_status_text, foreground="#556070", wraplength=420).grid(
            row=0,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(0, 8),
        )
        ttk.Label(lower_right, text="观察策略").grid(row=1, column=0, sticky="w")
        ttk.Label(lower_right, textvariable=self._editor_strategy_text).grid(row=1, column=1, sticky="w")
        ttk.Label(lower_right, text="API").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Label(lower_right, textvariable=self._editor_api_text).grid(row=2, column=1, sticky="w", pady=(8, 0))
        ttk.Label(lower_right, text="观察标的").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self._editor_symbol_entry = ttk.Entry(lower_right, textvariable=self._editor_symbol)
        self._editor_symbol_entry.grid(row=3, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(lower_right, text="运行模式").grid(row=4, column=0, sticky="w", pady=(8, 0))
        ttk.Label(lower_right, textvariable=self._editor_run_mode_text).grid(row=4, column=1, sticky="w", pady=(8, 0))

        preset_frame = ttk.LabelFrame(lower_right, text="观察预设", padding=10)
        preset_frame.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        preset_frame.columnconfigure(1, weight=1)
        ttk.Label(preset_frame, text="预设名称").grid(row=0, column=0, sticky="w")
        ttk.Entry(preset_frame, textvariable=self._preset_name).grid(row=0, column=1, sticky="ew")
        ttk.Button(preset_frame, text="保存为预设", command=self.save_current_as_preset).grid(row=0, column=2, padx=(8, 0))
        ttk.Label(preset_frame, text="已存预设").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._preset_combo = ttk.Combobox(preset_frame, textvariable=self._preset_choice, state="readonly")
        self._preset_combo.grid(row=1, column=1, sticky="ew", pady=(8, 0))
        self._preset_combo.bind("<<ComboboxSelected>>", lambda *_: self._on_preset_selected())
        ttk.Button(preset_frame, text="套用到当前模板", command=self.apply_selected_preset).grid(
            row=1,
            column=2,
            padx=(8, 0),
            pady=(8, 0),
        )
        ttk.Button(preset_frame, text="从预设新建", command=self.create_draft_from_preset).grid(
            row=2,
            column=2,
            padx=(8, 0),
            pady=(8, 0),
            sticky="e",
        )

        notebook = ttk.Notebook(lower_right)
        notebook.grid(row=6, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
        for group_name, keys in _OBSERVER_PARAMETER_GROUPS:
            group_frame = ttk.Frame(notebook, padding=(8, 8, 8, 8))
            group_frame.columnconfigure(1, weight=1)
            notebook.add(group_frame, text=group_name)
            placeholder = ttk.Label(
                group_frame,
                text="当前策略在这个分组没有需要调整的观察参数。",
                foreground="#556070",
                wraplength=360,
                justify="left",
            )
            placeholder.grid(row=0, column=0, columnspan=2, sticky="w")
            self._editor_group_frames[group_name] = group_frame
            self._editor_group_placeholders[group_name] = placeholder
            for offset, key in enumerate(keys, start=1):
                label = ttk.Label(group_frame, text=_OBSERVER_PARAMETER_LABELS[key])
                label.grid(row=offset, column=0, sticky="w", pady=(8, 0))
                variable = self._editor_parameter_vars[key]
                if isinstance(variable, BooleanVar):
                    widget = ttk.Checkbutton(group_frame, variable=variable, onvalue=True, offvalue=False)
                elif key == "signal_mode":
                    widget = ttk.Combobox(
                        group_frame,
                        textvariable=variable,
                        values=list(_SIGNAL_MODE_LABEL_TO_VALUE.keys()),
                        state="readonly",
                    )
                elif key == "take_profit_mode":
                    widget = ttk.Combobox(
                        group_frame,
                        textvariable=variable,
                        values=list(_TAKE_PROFIT_MODE_LABEL_TO_VALUE.keys()),
                        state="readonly",
                    )
                elif key == "bar":
                    widget = ttk.Combobox(
                        group_frame,
                        textvariable=variable,
                        values=["1m", "3m", "5m", "15m", "1H", "4H"],
                        state="readonly",
                    )
                else:
                    widget = ttk.Entry(group_frame, textvariable=variable)
                widget.grid(row=offset, column=1, sticky="ew", pady=(8, 0))
                self._editor_parameter_labels[key] = label
                self._editor_parameter_inputs[key] = widget

        action_row = 7
        self._editor_save_button = ttk.Button(lower_right, text="保存当前模板", command=self.save_selected_draft)
        self._editor_save_button.grid(row=action_row, column=0, sticky="w", pady=(12, 0))
        ttk.Label(
            lower_right,
            justify="left",
            foreground="#556070",
            wraplength=420,
            text="这里只维护 signal_only 观察参数，不改下单数量、不改额度配置；真正的额度托管与审批，会放到独立的交易员管理台。",
        ).grid(row=action_row, column=1, sticky="w", pady=(12, 0))

        log_frame = ttk.LabelFrame(self.window, text="操作日志", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = Text(log_frame, height=8, wrap="word", font=("Consolas", 10), relief="flat")
        self.log_text.grid(row=0, column=0, sticky="nsew")

    def _store_path(self) -> Path:
        return signal_observer_templates_file_path()

    def _load_drafts(self) -> None:
        path = self._store_path()
        self._drafts = []
        self._draft_counter = 0
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._append_log(f"读取信号失败：{exc}")
            return
        if not isinstance(payload, list):
            return
        for item in payload:
            if not isinstance(item, dict):
                continue
            draft_id = str(item.get("draft_id") or "").strip()
            template_payload = item.get("template_payload")
            if not draft_id or not isinstance(template_payload, dict):
                continue
            template_payload = _normalize_template_payload(template_payload)
            created_at = _parse_time(item.get("created_at")) or datetime.now()
            updated_at = _parse_time(item.get("updated_at")) or created_at
            self._drafts.append(
                _ObserverDraft(
                    draft_id=draft_id,
                    template_payload=template_payload,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
            digits = "".join(ch for ch in draft_id if ch.isdigit())
            if digits:
                self._draft_counter = max(self._draft_counter, int(digits))

    def _save_drafts(self) -> None:
        payload = [
            {
                "draft_id": item.draft_id,
                "template_payload": item.template_payload,
                "created_at": item.created_at.isoformat(timespec="seconds"),
                "updated_at": item.updated_at.isoformat(timespec="seconds"),
            }
            for item in self._drafts
        ]
        path = self._store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _preset_store_path(self) -> Path:
        return signal_observer_presets_file_path()

    def _load_presets(self) -> None:
        path = self._preset_store_path()
        self._presets = []
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            self._append_log(f"读取观察预设失败：{exc}")
            return
        if not isinstance(payload, list):
            return
        for item in payload:
            if not isinstance(item, dict):
                continue
            preset_name = str(item.get("preset_name") or "").strip()
            template_payload = item.get("template_payload")
            if not preset_name or not isinstance(template_payload, dict):
                continue
            template_payload = _normalize_template_payload(template_payload)
            created_at = _parse_time(item.get("created_at")) or datetime.now()
            updated_at = _parse_time(item.get("updated_at")) or created_at
            self._presets.append(
                _ObserverPreset(
                    preset_name=preset_name,
                    template_payload=template_payload,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )

    def _save_presets(self) -> None:
        payload = [
            {
                "preset_name": item.preset_name,
                "template_payload": item.template_payload,
                "created_at": item.created_at.isoformat(timespec="seconds"),
                "updated_at": item.updated_at.isoformat(timespec="seconds"),
            }
            for item in self._presets
        ]
        path = self._preset_store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _find_preset(self, preset_name: str) -> _ObserverPreset | None:
        target = preset_name.strip()
        for preset in self._presets:
            if preset.preset_name == target:
                return preset
        return None

    def _refresh_preset_choices(self) -> None:
        if self._preset_combo is None:
            return
        choices = [item.preset_name for item in sorted(self._presets, key=lambda item: item.updated_at, reverse=True)]
        self._preset_combo.configure(values=choices)
        current = self._preset_choice.get().strip()
        if current and current in choices:
            return
        self._preset_choice.set(choices[0] if choices else "")

    def _next_draft_id(self) -> str:
        self._draft_counter += 1
        return f"D{self._draft_counter:03d}"

    def _selected_symbols(self) -> list[str]:
        selected = [symbol for symbol, var in self._symbol_vars.items() if var.get()]
        custom = [item.strip().upper() for item in self._custom_symbols.get().split(",") if item.strip()]
        return list(dict.fromkeys(selected + custom))

    def _selected_draft_ids(self) -> list[str]:
        if self.draft_tree is None:
            return []
        return [str(item) for item in self.draft_tree.selection()]

    def _selected_drafts(self) -> list[_ObserverDraft]:
        selected_ids = set(self._selected_draft_ids())
        return [item for item in self._drafts if item.draft_id in selected_ids]

    def _selected_session_ids(self) -> list[str]:
        if self.session_tree is None:
            return []
        return [str(item) for item in self.session_tree.selection()]

    @staticmethod
    def _session_tree_double_click_hint(column_id: str) -> str:
        return {
            "#1": "双击打开这条会话的独立日志",
            "#3": "双击打开这条会话的实时K线图",
        }.get(str(column_id or "").strip(), "")

    def _show_session_tree_hover_tip(self, text: str, *, x_root: int, y_root: int) -> None:
        if not text:
            self._hide_session_tree_hover_tip()
            return
        if self._session_tree_hover_tip_window is None or not self._session_tree_hover_tip_window.winfo_exists():
            window = Toplevel(self.window)
            window.withdraw()
            window.overrideredirect(True)
            window.attributes("-topmost", True)
            label = ttk.Label(window, text=text, padding=(8, 4), relief="solid", borderwidth=1)
            label.pack()
            self._session_tree_hover_tip_window = window
            self._session_tree_hover_tip_label = label
        else:
            window = self._session_tree_hover_tip_window
            label = self._session_tree_hover_tip_label
            if label is not None:
                label.configure(text=text)
        if window is None:
            return
        window.geometry(f"+{x_root + 12}+{y_root + 16}")
        window.deiconify()

    def _hide_session_tree_hover_tip(self) -> None:
        window = self._session_tree_hover_tip_window
        if window is not None and window.winfo_exists():
            window.withdraw()

    def _on_session_tree_hover(self, event: object) -> None:
        if self.session_tree is None:
            self._hide_session_tree_hover_tip()
            return
        try:
            x = int(getattr(event, "x", 0) or 0)
            y = int(getattr(event, "y", 0) or 0)
            x_root = int(getattr(event, "x_root", 0) or 0)
            y_root = int(getattr(event, "y_root", 0) or 0)
            region = str(self.session_tree.identify_region(x, y))
            column_id = str(self.session_tree.identify_column(x))
        except Exception:
            self._hide_session_tree_hover_tip()
            return
        if region != "heading":
            self._hide_session_tree_hover_tip()
            return
        tip_text = SignalMonitorWindow._session_tree_double_click_hint(column_id)
        if not tip_text:
            self._hide_session_tree_hover_tip()
            return
        self._show_session_tree_hover_tip(tip_text, x_root=x_root, y_root=y_root)

    def _on_session_tree_hover_leave(self, *_: object) -> None:
        self._hide_session_tree_hover_tip()

    def _on_session_tree_double_click(self, event: object) -> str | None:
        if self.session_tree is None:
            return None
        try:
            column_id = str(self.session_tree.identify_column(getattr(event, "x", 0) or 0))
            row_id = str(self.session_tree.identify_row(getattr(event, "y", 0) or 0)).strip()
        except Exception:
            return None
        if not row_id:
            return None
        if column_id == "#1":
            self._session_log_opener(row_id)
            return "break"
        if column_id == "#3":
            self._session_chart_opener(row_id)
            return "break"
        return None

    def _selected_single_draft(self) -> _ObserverDraft | None:
        selected = self._selected_drafts()
        if len(selected) != 1:
            return None
        return selected[0]

    def _selected_preset(self) -> _ObserverPreset | None:
        return self._find_preset(self._preset_choice.get())

    def _suggest_preset_name(self, draft: _ObserverDraft) -> str:
        payload = draft.template_payload
        strategy_name = str(payload.get("strategy_name") or payload.get("strategy_id") or "观察模板").strip()
        config_snapshot = payload.get("config_snapshot")
        bar = ""
        if isinstance(config_snapshot, dict):
            bar = str(config_snapshot.get("bar") or "").strip()
        direction = str(payload.get("direction_label") or "").strip()
        parts = [strategy_name]
        if bar:
            parts.append(bar)
        if direction:
            parts.append(direction)
        return " | ".join(parts)

    def _on_preset_selected(self) -> None:
        preset = self._selected_preset()
        if preset is None:
            return
        self._preset_name.set(preset.preset_name)

    def _set_editor_widget_state(self, widget: object, *, editable: bool) -> None:
        if not editable:
            try:
                widget.configure(state="disabled")
            except Exception:
                pass
            return
        widget_class = ""
        try:
            widget_class = str(widget.winfo_class())
        except Exception:
            pass
        preferred_states = ("readonly", "normal") if "Combobox" in widget_class else ("normal", "readonly")
        for state in preferred_states:
            try:
                widget.configure(state=state)
            except Exception:
                continue
            return

    def _clear_editor(self, status: str) -> None:
        self._editor_status_text.set(status)
        self._editor_strategy_text.set("-")
        self._editor_api_text.set("-")
        self._editor_symbol.set("")
        self._editor_run_mode_text.set("只发邮件")
        for placeholder in self._editor_group_placeholders.values():
            placeholder.configure(text="请选择左侧一条观察模板，右侧可微调周期、趋势和保护参数。")
            placeholder.grid()
        for key, variable in self._editor_parameter_vars.items():
            default_value = strategy_parameter_default_value(key)
            if isinstance(variable, BooleanVar):
                variable.set(_coerce_bool(default_value))
            else:
                variable.set("" if default_value is None else str(default_value))
            label = self._editor_parameter_labels.get(key)
            if label is not None:
                label.configure(text=_OBSERVER_PARAMETER_LABELS[key])
            widget = self._editor_parameter_inputs.get(key)
            if label is not None:
                label.grid_remove()
            if widget is not None:
                widget.grid_remove()
        self._set_editor_widget_state(self._editor_symbol_entry, editable=False)
        if self._editor_save_button is not None:
            self._set_editor_widget_state(self._editor_save_button, editable=False)

    def _apply_editor_payload(self, draft: _ObserverDraft) -> None:
        payload = draft.template_payload
        strategy_id = str(payload.get("strategy_id") or "").strip()
        config_snapshot = payload.get("config_snapshot")
        if not strategy_id or not isinstance(config_snapshot, dict):
            self._clear_editor("当前模板缺少策略参数，建议删除后重新加入。")
            return
        missing_keys = [
            key
            for key in _OBSERVER_PARAMETER_ORDER
            if key not in self._editor_parameter_labels or key not in self._editor_parameter_inputs
        ]
        if missing_keys:
            self._clear_editor("观察台参数面板尚未准备完成，请重新打开观察台。")
            return
        definition = get_strategy_definition(strategy_id)
        self._editor_status_text.set(f"正在编辑 {draft.draft_id}，保存后只影响这条观察模板。")
        self._editor_strategy_text.set(str(payload.get("strategy_name") or definition.name))
        self._editor_api_text.set(str(payload.get("api_name") or "-"))
        self._editor_symbol.set(str(payload.get("symbol") or config_snapshot.get("inst_id") or ""))
        self._editor_run_mode_text.set(str(payload.get("run_mode_label") or "只发邮件"))
        if not str(self._preset_name.get() or "").strip():
            self._preset_name.set(self._suggest_preset_name(draft))
        self._set_editor_widget_state(self._editor_symbol_entry, editable=True)
        if self._editor_save_button is not None:
            self._set_editor_widget_state(self._editor_save_button, editable=True)
        visible_keys = set(iter_strategy_parameter_keys(strategy_id))
        group_visible_counts = {group_name: 0 for group_name, _ in _OBSERVER_PARAMETER_GROUPS}
        for key in _OBSERVER_PARAMETER_ORDER:
            label = self._editor_parameter_labels[key]
            widget = self._editor_parameter_inputs[key]
            if key not in visible_keys:
                label.grid_remove()
                widget.grid_remove()
                continue
            group_name = _OBSERVER_PARAMETER_GROUP_BY_KEY.get(key, "")
            if group_name:
                group_visible_counts[group_name] = group_visible_counts.get(group_name, 0) + 1
            label_text = _OBSERVER_PARAMETER_LABELS[key]
            fixed_value = strategy_fixed_value(strategy_id, key)
            if fixed_value is not None:
                label_text = f"{label_text}（本策略固定）"
            label.configure(text=label_text)
            label.grid()
            widget.grid()
            raw_value = config_snapshot.get(key, strategy_parameter_default_value(key))
            variable = self._editor_parameter_vars[key]
            if key == "signal_mode":
                variable.set(_SIGNAL_MODE_VALUE_TO_LABEL.get(str(raw_value), definition.default_signal_label))
            elif key == "take_profit_mode":
                variable.set(_TAKE_PROFIT_MODE_VALUE_TO_LABEL.get(str(raw_value), "动态止盈"))
            elif isinstance(variable, BooleanVar):
                variable.set(_coerce_bool(raw_value))
            else:
                variable.set("" if raw_value is None else str(raw_value))
            self._set_editor_widget_state(widget, editable=strategy_is_parameter_editable(strategy_id, key, "observer"))
        for group_name, placeholder in self._editor_group_placeholders.items():
            if group_visible_counts.get(group_name, 0) > 0:
                placeholder.grid_remove()
            else:
                placeholder.configure(text="当前策略在这个分组没有需要调整的观察参数。")
                placeholder.grid()

    def _refresh_editor_from_selection(self) -> None:
        draft = self._selected_single_draft()
        if draft is None:
            if self._selected_draft_ids():
                self._clear_editor("多选时仅支持批量启动；如需编辑，请只选中一条观察模板。")
            else:
                self._clear_editor("请选择左侧一条信号，右侧可微调观察参数。")
            return
        self._apply_editor_payload(draft)

    def save_selected_draft(self) -> None:
        draft = self._selected_single_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先只选中一条观察模板。", parent=self.window)
            return
        payload = dict(draft.template_payload)
        config_snapshot = payload.get("config_snapshot")
        strategy_id = str(payload.get("strategy_id") or "").strip()
        if not strategy_id or not isinstance(config_snapshot, dict):
            messagebox.showerror("保存失败", "当前模板缺少有效的策略参数。", parent=self.window)
            return
        updated_snapshot = dict(config_snapshot)
        symbol = str(self._editor_symbol.get() or "").strip().upper()
        if not symbol:
            messagebox.showinfo("提示", "请先填写有效标的。", parent=self.window)
            return
        old_symbol = str(payload.get("symbol") or updated_snapshot.get("inst_id") or "").strip().upper()
        updated_snapshot["inst_id"] = symbol
        if updated_snapshot.get("trade_inst_id") not in (None, ""):
            updated_snapshot["trade_inst_id"] = symbol
        if updated_snapshot.get("local_tp_sl_inst_id") not in (None, "") and updated_snapshot.get("local_tp_sl_inst_id") == old_symbol:
            updated_snapshot["local_tp_sl_inst_id"] = symbol
        direction_label = str(payload.get("direction_label") or "").strip()
        definition = get_strategy_definition(strategy_id)
        for key in iter_strategy_parameter_keys(strategy_id):
            fixed_value = strategy_fixed_value(strategy_id, key)
            if fixed_value is not None:
                updated_snapshot[key] = fixed_value
                if key == "signal_mode":
                    direction_label = _SIGNAL_MODE_VALUE_TO_LABEL.get(str(fixed_value), definition.default_signal_label)
                continue
            variable = self._editor_parameter_vars.get(key)
            if variable is None:
                continue
            if key == "signal_mode":
                raw_value = _SIGNAL_MODE_LABEL_TO_VALUE.get(str(variable.get()).strip(), updated_snapshot.get(key, "both"))
                direction_label = _SIGNAL_MODE_VALUE_TO_LABEL.get(str(raw_value), definition.default_signal_label)
            elif key == "take_profit_mode":
                raw_value = _TAKE_PROFIT_MODE_LABEL_TO_VALUE.get(
                    str(variable.get()).strip(),
                    updated_snapshot.get(key, "dynamic"),
                )
            elif isinstance(variable, BooleanVar):
                raw_value = bool(variable.get())
            else:
                raw_value = str(variable.get()).strip()
            updated_snapshot[key] = raw_value
        resolved_signal_mode = resolve_dynamic_signal_mode(
            strategy_id,
            str(updated_snapshot.get("signal_mode") or "both"),
        )
        updated_snapshot["signal_mode"] = resolved_signal_mode
        direction_label = _SIGNAL_MODE_VALUE_TO_LABEL.get(
            str(resolved_signal_mode),
            definition.default_signal_label,
        )
        payload["symbol"] = symbol
        payload["direction_label"] = direction_label or definition.default_signal_label
        payload["run_mode_label"] = "只发邮件"
        payload["config_snapshot"] = updated_snapshot
        draft.template_payload = _normalize_template_payload(payload)
        draft.updated_at = datetime.now()
        self._save_drafts()
        self._refresh_views()
        if self.draft_tree is not None and self.draft_tree.exists(draft.draft_id):
            self.draft_tree.selection_set(draft.draft_id)
        self._refresh_editor_from_selection()
        self._append_log(f"[{draft.draft_id}] 已更新观察模板参数。")

    def save_current_as_preset(self) -> None:
        draft = self._selected_single_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先只选中一条观察模板，再保存为预设。", parent=self.window)
            return
        preset_name = str(self._preset_name.get() or "").strip()
        if not preset_name:
            preset_name = self._suggest_preset_name(draft)
            self._preset_name.set(preset_name)
        existing = self._find_preset(preset_name)
        if existing is not None:
            confirmed = messagebox.askyesno(
                "覆盖预设",
                f"预设“{preset_name}”已经存在，是否用当前观察模板覆盖它？",
                parent=self.window,
            )
            if not confirmed:
                return
            existing.template_payload = _normalize_template_payload(dict(draft.template_payload))
            existing.updated_at = datetime.now()
            if existing.created_at is None:
                existing.created_at = existing.updated_at
        else:
            now = datetime.now()
            self._presets.append(
                _ObserverPreset(
                    preset_name=preset_name,
                    template_payload=_normalize_template_payload(dict(draft.template_payload)),
                    created_at=now,
                    updated_at=now,
                )
            )
        self._save_presets()
        self._refresh_preset_choices()
        self._preset_choice.set(preset_name)
        self._append_log(f"[预设] 已保存观察预设：{preset_name}")

    def apply_selected_preset(self) -> None:
        draft = self._selected_single_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先只选中一条观察模板，再套用预设。", parent=self.window)
            return
        preset = self._selected_preset()
        if preset is None:
            messagebox.showinfo("提示", "请先选择一个观察预设。", parent=self.window)
            return
        preset_strategy_id = str(preset.template_payload.get("strategy_id") or "").strip()
        draft_strategy_id = str(draft.template_payload.get("strategy_id") or "").strip()
        if preset_strategy_id != draft_strategy_id:
            messagebox.showinfo(
                "提示",
                "当前版本为了避免参数错位，只支持把同策略预设套用到当前观察模板。",
                parent=self.window,
            )
            return
        payload = dict(draft.template_payload)
        config_snapshot = payload.get("config_snapshot")
        preset_snapshot = preset.template_payload.get("config_snapshot")
        if not isinstance(config_snapshot, dict) or not isinstance(preset_snapshot, dict):
            messagebox.showerror("套用失败", "预设或模板缺少有效参数。", parent=self.window)
            return
        current_symbol = str(payload.get("symbol") or config_snapshot.get("inst_id") or "").strip().upper()
        updated_snapshot = dict(preset_snapshot)
        if current_symbol:
            updated_snapshot["inst_id"] = current_symbol
            if updated_snapshot.get("trade_inst_id") not in (None, ""):
                updated_snapshot["trade_inst_id"] = current_symbol
            if updated_snapshot.get("local_tp_sl_inst_id") not in (None, ""):
                updated_snapshot["local_tp_sl_inst_id"] = current_symbol
            payload["symbol"] = current_symbol
        payload["direction_label"] = str(preset.template_payload.get("direction_label") or payload.get("direction_label") or "")
        payload["run_mode_label"] = "只发邮件"
        payload["config_snapshot"] = updated_snapshot
        draft.template_payload = _normalize_template_payload(payload)
        draft.updated_at = datetime.now()
        self._save_drafts()
        self._refresh_views()
        if self.draft_tree is not None and self.draft_tree.exists(draft.draft_id):
            self.draft_tree.selection_set(draft.draft_id)
        self._refresh_editor_from_selection()
        self._append_log(f"[{draft.draft_id}] 已套用观察预设：{preset.preset_name}")

    def create_draft_from_preset(self) -> None:
        preset = self._selected_preset()
        if preset is None:
            messagebox.showinfo("提示", "请先选择一个观察预设。", parent=self.window)
            return
        now = datetime.now()
        draft = _ObserverDraft(
            draft_id=self._next_draft_id(),
            template_payload=_normalize_template_payload(dict(preset.template_payload)),
            created_at=now,
            updated_at=now,
        )
        self._drafts.append(draft)
        self._save_drafts()
        self._refresh_views()
        if self.draft_tree is not None and self.draft_tree.exists(draft.draft_id):
            self.draft_tree.selection_set(draft.draft_id)
        self._refresh_editor_from_selection()
        self._append_log(f"[{draft.draft_id}] 已从预设新建：{preset.preset_name}")

    def add_current_template(self) -> None:
        try:
            template = self._current_template_factory()
            payload = _normalize_template_payload(self._template_serializer(template))
        except Exception as exc:
            messagebox.showerror("加入失败", str(exc), parent=self.window)
            return
        now = datetime.now()
        draft = _ObserverDraft(
            draft_id=self._next_draft_id(),
            template_payload=payload,
            created_at=now,
            updated_at=now,
        )
        self._drafts.append(draft)
        self._save_drafts()
        self._refresh_views()
        if self.draft_tree is not None and self.draft_tree.exists(draft.draft_id):
            self.draft_tree.selection_set(draft.draft_id)
        self._refresh_editor_from_selection()
        self._append_log(f"[{draft.draft_id}] 已加入当前 signal_only 信号。")

    def clone_selected_to_symbols(self) -> None:
        drafts = self._selected_drafts()
        if not drafts:
            messagebox.showinfo("提示", "请先选中一条信号。", parent=self.window)
            return
        symbols = self._selected_symbols()
        if not symbols:
            messagebox.showinfo("提示", "请先勾选至少一个币种。", parent=self.window)
            return
        created = 0
        now = datetime.now()
        for draft in drafts:
            template = self._template_deserializer(draft.template_payload)
            if template is None:
                continue
            for symbol in symbols:
                cloned = self._template_symbol_cloner(template, symbol)
                payload = _normalize_template_payload(self._template_serializer(cloned))
                self._drafts.append(
                    _ObserverDraft(
                        draft_id=self._next_draft_id(),
                        template_payload=payload,
                        created_at=now,
                        updated_at=now,
                    )
                )
                created += 1
        if created <= 0:
            messagebox.showwarning("提示", "没有生成新的信号。", parent=self.window)
            return
        self._save_drafts()
        self._refresh_views()
        self._refresh_editor_from_selection()
        self._append_log(f"已复制 {created} 条信号到批量币种。")

    def delete_selected_drafts(self) -> None:
        selected_ids = set(self._selected_draft_ids())
        if not selected_ids:
            messagebox.showinfo("提示", "请先选中要删除的信号。", parent=self.window)
            return
        self._drafts = [item for item in self._drafts if item.draft_id not in selected_ids]
        self._save_drafts()
        self._refresh_views()
        self._refresh_editor_from_selection()
        self._append_log(f"已删除 {len(selected_ids)} 条观察信号。")

    def _start_drafts(self, drafts: list[_ObserverDraft], source_label: str) -> None:
        if not drafts:
            messagebox.showinfo("提示", "没有可启动的信号。", parent=self.window)
            return
        started = 0
        failures: list[str] = []
        for draft in drafts:
            template = self._template_deserializer(draft.template_payload)
            if template is None:
                failures.append(f"{draft.draft_id}: 信号已损坏")
                continue
            try:
                session_id = self._template_launcher(template, source_label)
            except Exception as exc:
                failures.append(f"{draft.draft_id}: {exc}")
            else:
                started += 1
                self._append_log(f"[{draft.draft_id}] 已启动 signal_only 会话：{session_id}")
        self._refresh_views()
        if failures:
            messagebox.showwarning("部分启动失败", "\n".join(failures[:12]), parent=self.window)
        if started:
            self._append_log(f"本次共启动 {started} 条信号。")

    def start_selected_drafts(self) -> None:
        self._start_drafts(self._selected_drafts(), "selected")

    def start_all_drafts(self) -> None:
        self._start_drafts(list(self._drafts), "all")

    def stop_selected_sessions(self) -> None:
        session_ids = self._selected_session_ids()
        if not session_ids:
            messagebox.showinfo("提示", "请先选中要停止的会话。", parent=self.window)
            return
        try:
            self._session_stopper(session_ids)
        except Exception as exc:
            messagebox.showerror("停止失败", str(exc), parent=self.window)
            return
        self._append_log(f"已请求停止 {len(session_ids)} 个 signal_only 会话。")
        self._refresh_views()

    def delete_selected_sessions(self) -> None:
        session_ids = self._selected_session_ids()
        if not session_ids:
            messagebox.showinfo("提示", "请先选中要删除的会话。", parent=self.window)
            return
        confirmed = messagebox.askyesno(
            "确认删除",
            (
                f"确认从监控列表删除 {len(session_ids)} 个选中会话吗？\n\n"
                "只会删除已停止的 signal_only 会话记录；"
                "运行中或停止中的会话会保留。"
            ),
            parent=self.window,
        )
        if not confirmed:
            return
        try:
            deleted_count, blocked_ids = self._session_deleter(session_ids)
        except Exception as exc:
            messagebox.showerror("删除失败", str(exc), parent=self.window)
            return
        if deleted_count > 0:
            self._append_log(f"已删除 {deleted_count} 个 signal_only 会话记录。")
        if blocked_ids:
            messagebox.showinfo(
                "部分未删除",
                f"{len(blocked_ids)} 个会话当前还不能删除，请先停止并等待状态变为“已停止”。",
                parent=self.window,
            )
        elif deleted_count <= 0:
            messagebox.showinfo("提示", "选中的会话已经不在监控列表中。", parent=self.window)
        self._refresh_views()

    def _refresh_draft_tree(self) -> None:
        if self.draft_tree is None:
            return
        selected = set(self.draft_tree.selection())
        for item_id in self.draft_tree.get_children():
            self.draft_tree.delete(item_id)
        for draft in self._drafts:
            payload = draft.template_payload
            config_snapshot = payload.get("config_snapshot")
            bar = "-"
            if isinstance(config_snapshot, dict):
                bar = str(config_snapshot.get("bar") or "-")
            self.draft_tree.insert(
                "",
                END,
                iid=draft.draft_id,
                values=(
                    draft.draft_id,
                    str(payload.get("strategy_name") or payload.get("strategy_id") or "-"),
                    str(payload.get("symbol") or "-"),
                    bar,
                    str(payload.get("api_name") or "-"),
                    str(payload.get("run_mode_label") or "只发邮件"),
                    draft.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        for item_id in selected:
            if self.draft_tree.exists(item_id):
                self.draft_tree.selection_add(item_id)

    def _refresh_session_tree(self) -> int:
        if self.session_tree is None:
            return 0
        selected = set(self.session_tree.selection())
        rows = self._session_provider()
        for item_id in self.session_tree.get_children():
            self.session_tree.delete(item_id)
        for row in rows:
            session_id = str(row.get("session_id") or "")
            if not session_id:
                continue
            self.session_tree.insert(
                "",
                END,
                iid=session_id,
                values=(
                    session_id,
                    row.get("strategy_name", "-"),
                    row.get("symbol", "-"),
                    row.get("api_name", "-"),
                    row.get("status", "-"),
                    row.get("last_message", "-"),
                ),
            )
        for item_id in selected:
            if self.session_tree.exists(item_id):
                self.session_tree.selection_add(item_id)
        return len(rows)

    def _refresh_views(self) -> None:
        self._refresh_draft_tree()
        session_count = self._refresh_session_tree()
        self._refresh_preset_choices()
        self._status_text.set(f"信号 {len(self._drafts)} 条 | 运行中 {session_count} 条")
        self._refresh_editor_from_selection()

    def _append_log(self, message: str) -> None:
        timestamped = f"[信号观察台] {message}"
        self._logger(timestamped)
        if self.log_text is not None:
            self.log_text.insert(END, f"{datetime.now():%H:%M:%S} {message}\n")
            self.log_text.see(END)

    def _schedule_refresh(self) -> None:
        if not self.window.winfo_exists():
            return
        self._refresh_job = self.window.after(2500, self._refresh_tick)

    def _refresh_tick(self) -> None:
        self._refresh_job = None
        if not self.window.winfo_exists():
            return
        self._refresh_views()
        self._schedule_refresh()
