from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from tkinter import BooleanVar, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable

from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import get_strategy_definition, is_dynamic_strategy_id, supports_trader_desk
from okx_quant.trader_desk import (
    TRADER_DRAFT_STATUS_VALUES,
    TRADER_GATE_CONDITION_VALUES,
    TraderDeskSnapshot,
    TraderDraftRecord,
    normalize_trader_draft_inputs,
    trader_open_position_summary,
    trader_realized_close_counts,
    trader_realized_net_pnl,
    trader_remaining_quota_steps,
    trader_slots_for,
    trader_used_quota_steps,
)
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]
CurrentTemplateFactory = Callable[[], object]
TemplateSerializer = Callable[[object], dict[str, object]]
TemplateDeserializer = Callable[[dict[str, object]], object | None]
TemplateTargetCloner = Callable[[object, str, str], object]
SnapshotProvider = Callable[[], TraderDeskSnapshot]
DraftSaver = Callable[[TraderDraftRecord], None]
DraftDeleter = Callable[[str], None]
TraderAction = Callable[[str], None]
SymbolProvider = Callable[[], list[str]]
RuntimeSnapshotProvider = Callable[[str], dict[str, object] | None]


PRICE_TYPE_VALUES: tuple[str, ...] = ("mark", "last", "index")
GATE_CONDITION_LABELS: dict[str, str] = {
    "always": "始终开启",
    "above": "高于 >=",
    "below": "低于 <=",
    "between": "区间内 [下限, 上限]",
}
GATE_CONDITION_LABEL_TO_VALUE: dict[str, str] = {
    label: value for value, label in GATE_CONDITION_LABELS.items()
}
DRAFT_STATUS_LABELS: dict[str, str] = {
    "draft": "草稿中",
    "ready": "可启动",
    "paused": "已暂停",
}
DRAFT_STATUS_LABEL_TO_VALUE: dict[str, str] = {
    label: value for value, label in DRAFT_STATUS_LABELS.items()
}


def _should_reload_draft_form(
    *,
    explicit_select_id: str | None,
    selected_id: str | None,
    loaded_trader_id: str,
    form_dirty: bool,
) -> bool:
    if explicit_select_id is not None:
        return True
    if (selected_id or "") != loaded_trader_id:
        return True
    return not form_dirty


def _strategy_label_from_payload(payload: dict[str, object]) -> str:
    strategy_name = str(payload.get("strategy_name") or "").strip()
    strategy_id = str(payload.get("strategy_id") or "").strip()
    return strategy_name or strategy_id or "未命名策略"


def _validate_trader_desk_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("当前参数缺少可保存的策略模板。")
    strategy_id = str(payload.get("strategy_id") or "").strip()
    if not strategy_id:
        raise ValueError("当前参数缺少策略标识，无法加入交易员管理台。")
    try:
        if not supports_trader_desk(strategy_id):
            raise ValueError(f"{_strategy_label_from_payload(payload)} 暂不支持加入交易员管理台。")
    except KeyError as exc:
        raise ValueError(f"当前版本不认识这个策略：{strategy_id}") from exc
    return payload


def _normalize_decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _normalize_draft_form_values(
    total_quota: str,
    unit_quota: str,
    quota_steps: str,
    status: str,
) -> tuple[str, str, str, str]:
    try:
        normalized = normalize_trader_draft_inputs(
            total_quota=total_quota,
            unit_quota=unit_quota,
            quota_steps=quota_steps,
            status=status,
            gate_enabled=False,
            gate_condition="always",
            gate_trigger_inst_id="",
            gate_trigger_price_type="mark",
            gate_lower_price="",
            gate_upper_price="",
        )
    except ValueError as exc:
        message = str(exc).replace("固定数量", "单次额度")
        raise ValueError(message) from None
    return (
        _normalize_decimal_text(normalized["total_quota"]),
        _normalize_decimal_text(normalized["unit_quota"]),
        str(normalized["quota_steps"]),
        str(normalized["status"]),
    )


def _draft_template_identity(payload: dict[str, object]) -> str:
    identity_payload = {
        "api_name": str(payload.get("api_name") or "").strip(),
        "strategy_id": str(payload.get("strategy_id") or "").strip(),
        "direction_label": str(payload.get("direction_label") or "").strip(),
        "run_mode_label": str(payload.get("run_mode_label") or "").strip(),
        "symbol": str(payload.get("symbol") or "").strip().upper(),
        "config_snapshot": payload.get("config_snapshot"),
    }
    return json.dumps(identity_payload, ensure_ascii=False, sort_keys=True)


def _payload_config_snapshot(payload: dict[str, object]) -> dict[str, object]:
    raw = payload.get("config_snapshot")
    return raw if isinstance(raw, dict) else {}


def _payload_trade_symbol(payload: dict[str, object]) -> str:
    config = _payload_config_snapshot(payload)
    trade_symbol = str(config.get("trade_inst_id") or "").strip().upper()
    signal_symbol = str(config.get("inst_id") or payload.get("symbol") or "").strip().upper()
    return trade_symbol or signal_symbol


def _payload_signal_symbol(payload: dict[str, object]) -> str:
    config = _payload_config_snapshot(payload)
    local_symbol = str(config.get("local_tp_sl_inst_id") or "").strip().upper()
    signal_symbol = str(config.get("inst_id") or payload.get("symbol") or "").strip().upper()
    return local_symbol or signal_symbol


def _payload_bar(payload: dict[str, object]) -> str:
    config = _payload_config_snapshot(payload)
    bar = str(config.get("bar") or "").strip()
    return bar or "-"


def _format_optional_decimal(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return format_decimal(value)


def _format_optional_pnl(value: Decimal | None) -> str:
    if value is None:
        return "-"
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format_decimal_fixed(decimal_value, 2)


def _gate_condition_label(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized in GATE_CONDITION_LABELS:
        return GATE_CONDITION_LABELS[normalized]
    if normalized in GATE_CONDITION_LABEL_TO_VALUE:
        return normalized
    return GATE_CONDITION_LABELS["always"]


def _gate_condition_value(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized in GATE_CONDITION_LABEL_TO_VALUE:
        return GATE_CONDITION_LABEL_TO_VALUE[normalized]
    if normalized in GATE_CONDITION_LABELS:
        return normalized
    return "always"


def _draft_status_label(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized in DRAFT_STATUS_LABELS:
        return DRAFT_STATUS_LABELS[normalized]
    if normalized in DRAFT_STATUS_LABEL_TO_VALUE:
        return normalized
    return DRAFT_STATUS_LABELS["draft"]


def _draft_status_value(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized in DRAFT_STATUS_LABEL_TO_VALUE:
        return DRAFT_STATUS_LABEL_TO_VALUE[normalized]
    if normalized in DRAFT_STATUS_LABELS:
        return normalized
    return "draft"


def _snapshot_text(snapshot: dict[str, object], key: str, default: str = "-") -> str:
    value = str(snapshot.get(key) or "").strip()
    if value:
        return value
    return default


def _bool_label(value: object) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        return "开启" if normalized in {"1", "true", "yes", "on", "开启"} else "关闭"
    return "开启" if bool(value) else "关闭"


def _entry_reference_ema_label(snapshot: dict[str, object]) -> str:
    raw_period = str(snapshot.get("entry_reference_ema_period") or "").strip()
    if raw_period.isdigit() and int(raw_period) > 0:
        return f"EMA{int(raw_period)}"
    ema_period = _snapshot_text(snapshot, "ema_period")
    return f"跟随EMA小周期(EMA{ema_period})"


def _startup_chase_window_label(snapshot: dict[str, object]) -> str:
    raw_value = str(snapshot.get("startup_chase_window_seconds") or "").strip()
    if raw_value.isdigit() and int(raw_value) > 0:
        return f"{int(raw_value)}秒"
    return "关闭（启动不追老信号）"


def _gate_summary_text(draft: TraderDraftRecord) -> str:
    gate = draft.gate
    if not gate.enabled:
        return "关闭"
    parts = [
        _gate_condition_label(gate.condition),
        f"标的={gate.trigger_inst_id or '-'}",
        f"价格类型={gate.trigger_price_type or 'mark'}",
    ]
    if gate.lower_price is not None:
        parts.append(f"下限={_normalize_decimal_text(gate.lower_price)}")
    if gate.upper_price is not None:
        parts.append(f"上限={_normalize_decimal_text(gate.upper_price)}")
    return " | ".join(parts)


def _build_trader_strategy_lines(
    draft: TraderDraftRecord,
    *,
    runtime_snapshot: dict[str, object] | None = None,
) -> list[str]:
    payload = draft.template_payload
    snapshot = _payload_config_snapshot(payload)
    strategy_id = str(payload.get("strategy_id") or snapshot.get("strategy_id") or "").strip()
    direction_label = str(payload.get("direction_label") or "-").strip() or "-"
    run_mode_label = str(payload.get("run_mode_label") or "-").strip() or "-"
    bar = _payload_bar(payload)
    try:
        definition = get_strategy_definition(strategy_id)
    except KeyError:
        definition = None

    lines = [
        f"策略说明：{definition.summary if definition is not None else '-'}",
        f"开单逻辑：{definition.rule_description if definition is not None else '-'}",
        f"参数提示：{definition.parameter_hint if definition is not None else '-'}",
        f"K线周期：{bar}",
        f"方向：{direction_label} | 运行模式：{run_mode_label}",
        (
            f"检查时机：每根 {bar} 已收盘K线确认后检查一次。"
            if bar != "-"
            else "检查时机：按策略轮询节奏持续检查。"
        ),
        f"交易员固定数量：{_normalize_decimal_text(draft.unit_quota)}",
    ]

    template_risk = str(snapshot.get("risk_amount") or "").strip()
    if template_risk:
        lines.append(f"模板风险金：{template_risk}（交易员模式不使用）")

    lines.append(
        "EMA参数："
        f"小={_snapshot_text(snapshot, 'ema_period')} | "
        f"趋势={_snapshot_text(snapshot, 'trend_ema_period')} | "
        f"大={_snapshot_text(snapshot, 'big_ema_period')}"
    )
    lines.append(
        "ATR参数："
        f"周期={_snapshot_text(snapshot, 'atr_period')} | "
        f"止损={_snapshot_text(snapshot, 'atr_stop_multiplier')} | "
        f"止盈={_snapshot_text(snapshot, 'atr_take_multiplier')}"
    )
    if is_dynamic_strategy_id(strategy_id):
        lines.append(f"挂单参考EMA：{_entry_reference_ema_label(snapshot)}")
        take_profit_mode = "动态" if _snapshot_text(snapshot, "take_profit_mode", "dynamic") == "dynamic" else "固定"
        lines.append(
            f"止盈模式：{take_profit_mode} | "
            f"2R保本={_bool_label(snapshot.get('dynamic_two_r_break_even', True))} | "
            f"手续费偏移={_bool_label(snapshot.get('dynamic_fee_offset_enabled', True))}"
        )
        lines.append(
            "时间保本："
            f"{_bool_label(snapshot.get('time_stop_break_even_enabled', False))} / "
            f"{_snapshot_text(snapshot, 'time_stop_break_even_bars', '0')}根"
        )
        lines.append(f"启动追单窗口：{_startup_chase_window_label(snapshot)}")
    lines.append(
        f"止盈止损模式：{_snapshot_text(snapshot, 'tp_sl_mode')} | "
        f"轮询秒数：{_snapshot_text(snapshot, 'poll_seconds')}"
    )
    lines.append(f"价格开关：{_gate_summary_text(draft)}")

    if runtime_snapshot:
        session_id = str(runtime_snapshot.get("session_id") or "-").strip() or "-"
        runtime_status = str(runtime_snapshot.get("runtime_status") or "-").strip() or "-"
        thread_status = "运行中" if bool(runtime_snapshot.get("is_running")) else "已停止"
        lines.append("")
        lines.append("当前 watcher：")
        lines.append(f"会话：{session_id} | 状态：{runtime_status} | 线程：{thread_status}")
        started_at = runtime_snapshot.get("started_at")
        if isinstance(started_at, datetime):
            lines.append(f"会话启动：{started_at:%Y-%m-%d %H:%M:%S}")
        last_message = str(runtime_snapshot.get("last_message") or "").strip()
        ended_reason = str(runtime_snapshot.get("ended_reason") or "").strip()
        if last_message:
            lines.append(f"最近日志：{last_message}")
        elif ended_reason:
            lines.append(f"结束原因：{ended_reason}")
        else:
            lines.append("最近日志：-")
    else:
        lines.append("")
        lines.append("当前 watcher：暂无活动会话")

    return lines


class TraderDeskWindow:
    def __init__(
        self,
        parent,
        *,
        logger: Logger,
        current_template_factory: CurrentTemplateFactory,
        template_serializer: TemplateSerializer,
        template_deserializer: TemplateDeserializer,
        template_target_cloner: TemplateTargetCloner,
        snapshot_provider: SnapshotProvider,
        draft_saver: DraftSaver,
        draft_deleter: DraftDeleter,
        trader_starter: TraderAction,
        trader_pauser: TraderAction,
        trader_resumer: TraderAction,
        trader_flattener: TraderAction,
        symbol_provider: SymbolProvider,
        runtime_snapshot_provider: RuntimeSnapshotProvider,
    ) -> None:
        self._logger = logger
        self._current_template_factory = current_template_factory
        self._template_serializer = template_serializer
        self._template_deserializer = template_deserializer
        self._template_target_cloner = template_target_cloner
        self._snapshot_provider = snapshot_provider
        self._draft_saver = draft_saver
        self._draft_deleter = draft_deleter
        self._trader_starter = trader_starter
        self._trader_pauser = trader_pauser
        self._trader_resumer = trader_resumer
        self._trader_flattener = trader_flattener
        self._symbol_provider = symbol_provider
        self._runtime_snapshot_provider = runtime_snapshot_provider
        self._refresh_job: str | None = None
        self._trader_counter = 0
        self._snapshot = TraderDeskSnapshot()
        self._form_dirty = False
        self._suspend_form_tracking = False
        self._loaded_trader_id = ""
        self._pending_delete_trader_id = ""

        self._status_text = StringVar(value="草稿 0 条 | 运行中 0 条")
        self._summary_text = StringVar(value="交易员策略会固定数量下单，并按额度格持续补 watcher。")
        self.total_quota_var = StringVar(value="1")
        self.unit_quota_var = StringVar(value="0.1")
        self.quota_steps_var = StringVar(value="10")
        self.status_var = StringVar(value=_draft_status_label("draft"))
        self.notes_var = StringVar(value="")
        self.auto_restart_on_profit_var = BooleanVar(value=True)
        self.pause_on_stop_loss_var = BooleanVar(value=True)
        self.gate_enabled_var = BooleanVar(value=False)
        self.gate_condition_var = StringVar(value=_gate_condition_label("always"))
        self.gate_trigger_inst_id_var = StringVar(value="")
        self.gate_price_type_var = StringVar(value="mark")
        self.gate_lower_price_var = StringVar(value="")
        self.gate_upper_price_var = StringVar(value="")
        self.copy_trade_symbol_var = StringVar(value="")
        self.copy_trigger_symbol_var = StringVar(value="")

        self.window = Toplevel(parent)
        self.window.title("交易员管理台")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.84,
            height_ratio=0.84,
            min_width=1420,
            min_height=900,
            max_width=1820,
            max_height=1220,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        self.tree: ttk.Treeview | None = None
        self.slot_tree: ttk.Treeview | None = None
        self.event_text: Text | None = None
        self.detail_text: Text | None = None
        self._gate_trigger_combo: ttk.Combobox | None = None
        self._copy_trade_combo: ttk.Combobox | None = None
        self._copy_trigger_combo: ttk.Combobox | None = None

        self._bind_form_dirty_tracking()
        self._build_layout()
        self._refresh_views()
        self._schedule_refresh()

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

    def _bind_form_dirty_tracking(self) -> None:
        for variable in (
            self.total_quota_var,
            self.unit_quota_var,
            self.quota_steps_var,
            self.status_var,
            self.notes_var,
            self.auto_restart_on_profit_var,
            self.pause_on_stop_loss_var,
            self.gate_enabled_var,
            self.gate_condition_var,
            self.gate_trigger_inst_id_var,
            self.gate_price_type_var,
            self.gate_lower_price_var,
            self.gate_upper_price_var,
            self.copy_trade_symbol_var,
            self.copy_trigger_symbol_var,
        ):
            variable.trace_add("write", self._mark_form_dirty)

    def _mark_form_dirty(self, *_: object) -> None:
        if self._suspend_form_tracking:
            return
        self._form_dirty = True

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        header = ttk.Frame(self.window, padding=(16, 14, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        ttk.Label(header, text="交易员管理台", font=("Microsoft YaHei UI", 18, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(header, textvariable=self._status_text).grid(row=0, column=1, sticky="e")
        ttk.Label(
            header,
            text="固定数量下单、额度格占用、亏损记录、均价追踪、手动平仓都统一在这里完成，避免日志对不上。",
            foreground="#556070",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(6, 0))

        body = ttk.Frame(self.window, padding=(16, 0, 16, 16))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        left = ttk.LabelFrame(body, text="交易员列表", padding=12)
        left.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 12))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(left)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(toolbar, text="招募交易员", command=self.add_current_template).grid(row=0, column=0)
        ttk.Button(toolbar, text="保存交易规则", command=self.save_selected_draft).grid(row=0, column=1, padx=(8, 0))
        ttk.Button(toolbar, text="启动", command=self.start_selected_trader).grid(row=0, column=2, padx=(8, 0))
        ttk.Button(toolbar, text="暂停", command=self.pause_selected_trader).grid(row=0, column=3, padx=(8, 0))
        ttk.Button(toolbar, text="恢复", command=self.resume_selected_trader).grid(row=0, column=4, padx=(8, 0))
        ttk.Button(toolbar, text="手动平仓", command=self.flatten_selected_trader).grid(row=0, column=5, padx=(8, 0))
        ttk.Button(toolbar, text="辞退交易员", command=self.delete_selected_draft).grid(row=0, column=6, padx=(8, 0))

        self.tree = ttk.Treeview(
            left,
            columns=("trader", "strategy", "bar", "symbol", "api", "run", "quota", "avg", "pnl", "updated"),
            show="headings",
            selectmode="browse",
            height=22,
        )
        self.tree.grid(row=1, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        for column, text, width, anchor in (
            ("trader", "交易员", 80, "center"),
            ("strategy", "策略", 170, "w"),
            ("bar", "周期", 70, "center"),
            ("symbol", "交易/触发", 170, "w"),
            ("api", "API", 80, "center"),
            ("run", "运行状态", 110, "center"),
            ("quota", "额度格", 110, "center"),
            ("avg", "当前均价", 110, "center"),
            ("pnl", "累计净盈亏", 110, "e"),
            ("updated", "更新时间", 150, "center"),
        ):
            self.tree.heading(column, text=text)
            self.tree.column(column, width=width, anchor=anchor)
        tree_scroll = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=1, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll.set)

        right_top = ttk.LabelFrame(body, text="选中交易员", padding=12)
        right_top.grid(row=0, column=1, sticky="nsew")
        right_top.columnconfigure(1, weight=1)
        right_top.rowconfigure(8, weight=1)

        ttk.Label(right_top, text="总额度").grid(row=0, column=0, sticky="w")
        ttk.Entry(right_top, textvariable=self.total_quota_var).grid(row=0, column=1, sticky="ew")
        ttk.Label(right_top, text="固定数量").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(right_top, textvariable=self.unit_quota_var).grid(row=1, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(right_top, text="额度次数").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(right_top, textvariable=self.quota_steps_var).grid(row=2, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(right_top, text="规则状态").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            right_top,
            textvariable=self.status_var,
            values=tuple(_draft_status_label(value) for value in TRADER_DRAFT_STATUS_VALUES),
            state="readonly",
        ).grid(row=3, column=1, sticky="ew", pady=(8, 0))
        ttk.Checkbutton(right_top, text="盈利后自动补位", variable=self.auto_restart_on_profit_var).grid(
            row=4,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(10, 0),
        )
        ttk.Checkbutton(right_top, text="止损后暂停补位", variable=self.pause_on_stop_loss_var).grid(
            row=5,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(6, 0),
        )
        ttk.Label(right_top, text="备注").grid(row=6, column=0, sticky="nw", pady=(8, 0))
        ttk.Entry(right_top, textvariable=self.notes_var).grid(row=6, column=1, sticky="ew", pady=(8, 0))

        gate_frame = ttk.LabelFrame(right_top, text="价格开关", padding=10)
        gate_frame.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        gate_frame.columnconfigure(1, weight=1)
        ttk.Checkbutton(gate_frame, text="启用", variable=self.gate_enabled_var).grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            gate_frame,
            textvariable=self.gate_condition_var,
            values=tuple(GATE_CONDITION_LABELS[value] for value in TRADER_GATE_CONDITION_VALUES),
            state="readonly",
            width=16,
        ).grid(row=0, column=1, sticky="w")
        ttk.Label(gate_frame, text="触发标的").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._gate_trigger_combo = ttk.Combobox(gate_frame, textvariable=self.gate_trigger_inst_id_var)
        self._gate_trigger_combo.grid(row=1, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(gate_frame, text="价格类型").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            gate_frame,
            textvariable=self.gate_price_type_var,
            values=PRICE_TYPE_VALUES,
            state="readonly",
        ).grid(row=2, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(gate_frame, text="下限").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(gate_frame, textvariable=self.gate_lower_price_var).grid(row=3, column=1, sticky="ew", pady=(8, 0))
        ttk.Label(gate_frame, text="上限").grid(row=4, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(gate_frame, textvariable=self.gate_upper_price_var).grid(row=4, column=1, sticky="ew", pady=(8, 0))

        copy_frame = ttk.LabelFrame(right_top, text="复制到目标标的", padding=10)
        copy_frame.grid(row=8, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
        copy_frame.columnconfigure(1, weight=1)
        ttk.Label(copy_frame, text="交易标的").grid(row=0, column=0, sticky="w")
        self._copy_trade_combo = ttk.Combobox(copy_frame, textvariable=self.copy_trade_symbol_var)
        self._copy_trade_combo.grid(row=0, column=1, sticky="ew")
        ttk.Label(copy_frame, text="触发标的").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self._copy_trigger_combo = ttk.Combobox(copy_frame, textvariable=self.copy_trigger_symbol_var)
        self._copy_trigger_combo.grid(row=1, column=1, sticky="ew", pady=(8, 0))
        ttk.Button(copy_frame, text="复制当前草稿", command=self.clone_selected_to_target).grid(
            row=2,
            column=1,
            sticky="e",
            pady=(10, 0),
        )

        right_bottom = ttk.LabelFrame(body, text="槽位记录与事件", padding=12)
        right_bottom.grid(row=1, column=1, sticky="nsew", pady=(12, 0))
        right_bottom.columnconfigure(0, weight=1)
        right_bottom.rowconfigure(2, weight=1)
        right_bottom.rowconfigure(4, weight=1)

        ttk.Label(right_bottom, textvariable=self._summary_text, justify="left").grid(row=0, column=0, sticky="w")
        self.detail_text = Text(right_bottom, height=8, wrap="word", font=("Consolas", 10), relief="flat")
        self.detail_text.grid(row=1, column=0, sticky="ew", pady=(8, 10))
        detail_scroll = ttk.Scrollbar(right_bottom, orient="vertical", command=self.detail_text.yview)
        detail_scroll.grid(row=1, column=1, sticky="ns", pady=(8, 10))
        self.detail_text.configure(yscrollcommand=detail_scroll.set)

        self.slot_tree = ttk.Treeview(
            right_bottom,
            columns=("slot", "status", "session", "opened", "entry", "size", "closed", "pnl", "reason"),
            show="headings",
            selectmode="browse",
            height=8,
        )
        self.slot_tree.grid(row=2, column=0, sticky="nsew")
        for column, text, width, anchor in (
            ("slot", "额度格", 90, "center"),
            ("status", "状态", 90, "center"),
            ("session", "会话", 80, "center"),
            ("opened", "开仓时间", 120, "center"),
            ("entry", "开仓价", 90, "center"),
            ("size", "数量", 90, "center"),
            ("closed", "结束时间", 120, "center"),
            ("pnl", "净盈亏", 90, "e"),
            ("reason", "原因", 180, "w"),
        ):
            self.slot_tree.heading(column, text=text)
            self.slot_tree.column(column, width=width, anchor=anchor)
        slot_scroll = ttk.Scrollbar(right_bottom, orient="vertical", command=self.slot_tree.yview)
        slot_scroll.grid(row=2, column=1, sticky="ns")
        self.slot_tree.configure(yscrollcommand=slot_scroll.set)

        ttk.Label(right_bottom, text="事件日志").grid(row=3, column=0, sticky="w", pady=(10, 4))
        self.event_text = Text(right_bottom, height=10, wrap="word", font=("Consolas", 10), relief="flat")
        self.event_text.grid(row=4, column=0, sticky="nsew")

    @property
    def _drafts(self) -> list[TraderDraftRecord]:
        return list(self._snapshot.drafts)

    def _load_snapshot(self) -> None:
        self._snapshot = self._snapshot_provider()
        for draft in self._snapshot.drafts:
            digits = "".join(ch for ch in draft.trader_id if ch.isdigit())
            if digits:
                self._trader_counter = max(self._trader_counter, int(digits))

    def _selected_trader_id(self) -> str | None:
        if self.tree is None:
            return None
        selection = self.tree.selection()
        return str(selection[0]) if selection else None

    def _selected_draft(self) -> TraderDraftRecord | None:
        trader_id = self._selected_trader_id()
        if not trader_id:
            return None
        for draft in self._snapshot.drafts:
            if draft.trader_id == trader_id:
                return draft
        return None

    def _selected_run_status(self, trader_id: str) -> str:
        for run in self._snapshot.runs:
            if run.trader_id == trader_id:
                return run.status
        return "idle"

    def _selected_run_reason(self, trader_id: str) -> str:
        for run in self._snapshot.runs:
            if run.trader_id == trader_id:
                return run.paused_reason
        return ""

    def _next_trader_id(self) -> str:
        self._trader_counter += 1
        return f"T{self._trader_counter:03d}"

    def _append_log(self, message: str) -> None:
        self._logger(f"[交易员管理台] {message}")

    def _clear_pending_delete(self, trader_id: str, *, reason: str = "") -> None:
        if self._pending_delete_trader_id != trader_id:
            return
        self._pending_delete_trader_id = ""
        if reason:
            self._append_log(f"[{trader_id}] 已取消自动删除：{reason}")

    def add_current_template(self) -> None:
        try:
            template = self._current_template_factory()
            payload = _validate_trader_desk_payload(self._template_serializer(template))
        except Exception as exc:
            messagebox.showerror("加入失败", str(exc), parent=self.window)
            return
        existing = next(
            (item for item in self._snapshot.drafts if _draft_template_identity(item.template_payload) == _draft_template_identity(payload)),
            None,
        )
        if existing is not None:
            self._refresh_views(select_id=existing.trader_id)
            messagebox.showinfo("提示", f"相同参数草稿已存在：{existing.trader_id}", parent=self.window)
            return
        now = datetime.now()
        draft = TraderDraftRecord(
            trader_id=self._next_trader_id(),
            template_payload=payload,
            total_quota=Decimal("1"),
            unit_quota=Decimal("0.1"),
            quota_steps=10,
            auto_restart_on_profit=True,
            pause_on_stop_loss=True,
            status="draft",
            notes="",
            created_at=now,
            updated_at=now,
        )
        try:
            self._draft_saver(draft)
        except Exception as exc:
            messagebox.showerror("加入失败", str(exc), parent=self.window)
            return
        self._append_log(f"[{draft.trader_id}] 已招募交易员。")
        self._refresh_views(select_id=draft.trader_id)

    def clone_selected_to_target(self) -> None:
        draft = self._selected_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先选中一条交易员草稿。", parent=self.window)
            return
        trade_symbol = self.copy_trade_symbol_var.get().strip().upper()
        trigger_symbol = self.copy_trigger_symbol_var.get().strip().upper()
        if not trade_symbol:
            messagebox.showinfo("提示", "请先填写目标交易标的。", parent=self.window)
            return
        try:
            template = self._template_deserializer(draft.template_payload)
            if template is None:
                raise ValueError("当前草稿无法反序列化。")
            cloned = self._template_target_cloner(template, trade_symbol, trigger_symbol)
            payload = _validate_trader_desk_payload(self._template_serializer(cloned))
        except Exception as exc:
            messagebox.showerror("复制失败", str(exc), parent=self.window)
            return
        existing = next(
            (item for item in self._snapshot.drafts if _draft_template_identity(item.template_payload) == _draft_template_identity(payload)),
            None,
        )
        if existing is not None:
            self._refresh_views(select_id=existing.trader_id)
            messagebox.showinfo("提示", f"目标草稿已存在：{existing.trader_id}", parent=self.window)
            return
        now = datetime.now()
        gate = replace(draft.gate)
        if gate.enabled and trigger_symbol:
            gate.trigger_inst_id = trigger_symbol
        cloned_draft = TraderDraftRecord(
            trader_id=self._next_trader_id(),
            template_payload=payload,
            total_quota=draft.total_quota,
            unit_quota=draft.unit_quota,
            quota_steps=draft.quota_steps,
            auto_restart_on_profit=draft.auto_restart_on_profit,
            pause_on_stop_loss=draft.pause_on_stop_loss,
            status=draft.status,
            notes=draft.notes,
            gate=gate,
            created_at=now,
            updated_at=now,
        )
        try:
            self._draft_saver(cloned_draft)
        except Exception as exc:
            messagebox.showerror("复制失败", str(exc), parent=self.window)
            return
        self._append_log(
            f"[{cloned_draft.trader_id}] 已复制到交易标的 {trade_symbol}{f' / 触发标的 {trigger_symbol}' if trigger_symbol else ''}。"
        )
        self._refresh_views(select_id=cloned_draft.trader_id)

    def delete_selected_draft(self) -> None:
        draft = self._selected_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先选中要辞退的交易员。", parent=self.window)
            return
        slots = trader_slots_for(self._snapshot.slots, draft.trader_id)
        open_slots = [slot for slot in slots if slot.status == "open"]
        watching_slots = [slot for slot in slots if slot.status == "watching"]
        run_status = self._selected_run_status(draft.trader_id)
        if open_slots:
            messagebox.showerror("辞退失败", "该交易员仍有持仓中的额度格，请先手动平仓。", parent=self.window)
            return
        try:
            self._draft_deleter(draft.trader_id)
        except Exception as exc:
            message = str(exc)
            if (
                "仍有关联会话在运行" in message or "仍有活动中的额度格" in message
            ) and (watching_slots or run_status in {"running", "quota_exhausted", "paused_manual", "stopped"}):
                try:
                    self._trader_pauser(draft.trader_id)
                except Exception as pause_exc:
                    messagebox.showerror("辞退失败", f"{message}\n\n同时自动停止 watcher 失败：{pause_exc}", parent=self.window)
                    return
                self._pending_delete_trader_id = draft.trader_id
                self._append_log(f"[{draft.trader_id}] 已请求停止 watcher，清理完成后将自动辞退交易员。")
                self._refresh_views(select_id=draft.trader_id)
                messagebox.showinfo(
                    "辞退处理中",
                    "已先停止该交易员的 watcher。\n\n等待委托/槽位清理完成后，会自动辞退这名交易员。",
                    parent=self.window,
                )
                return
            messagebox.showerror("辞退失败", str(exc), parent=self.window)
            return
        self._clear_pending_delete(draft.trader_id)
        self._refresh_views()

    def save_selected_draft(self) -> None:
        draft = self._selected_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先选中一条交易员草稿。", parent=self.window)
            return
        try:
            normalized = normalize_trader_draft_inputs(
                total_quota=self.total_quota_var.get(),
                unit_quota=self.unit_quota_var.get(),
                quota_steps=self.quota_steps_var.get(),
                status=_draft_status_value(self.status_var.get()),
                gate_enabled=bool(self.gate_enabled_var.get()),
                gate_condition=_gate_condition_value(self.gate_condition_var.get()),
                gate_trigger_inst_id=self.gate_trigger_inst_id_var.get(),
                gate_trigger_price_type=self.gate_price_type_var.get(),
                gate_lower_price=self.gate_lower_price_var.get(),
                gate_upper_price=self.gate_upper_price_var.get(),
            )
        except ValueError as exc:
            messagebox.showerror("保存失败", str(exc), parent=self.window)
            return
        updated = replace(
            draft,
            total_quota=normalized["total_quota"],
            unit_quota=normalized["unit_quota"],
            quota_steps=normalized["quota_steps"],
            status=str(normalized["status"]),
            notes=self.notes_var.get().strip(),
            auto_restart_on_profit=bool(self.auto_restart_on_profit_var.get()),
            pause_on_stop_loss=bool(self.pause_on_stop_loss_var.get()),
            gate=normalized["gate"],
            updated_at=datetime.now(),
        )
        try:
            self._draft_saver(updated)
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc), parent=self.window)
            return
        self._clear_pending_delete(updated.trader_id, reason="你又手动保存了这条草稿")
        self._append_log(f"[{updated.trader_id}] 已保存交易规则。")
        self._refresh_views(select_id=updated.trader_id)

    def start_selected_trader(self) -> None:
        self._run_action(self._trader_starter, "启动", "已启动交易员")

    def pause_selected_trader(self) -> None:
        self._run_action(self._trader_pauser, "暂停", "已暂停交易员")

    def resume_selected_trader(self) -> None:
        self._run_action(self._trader_resumer, "恢复", "已恢复交易员")

    def flatten_selected_trader(self) -> None:
        self._run_action(self._trader_flattener, "平仓", "已请求手动平仓")

    def _run_action(self, action: TraderAction, title: str, success_text: str) -> None:
        draft = self._selected_draft()
        if draft is None:
            messagebox.showinfo("提示", "请先选中一条交易员草稿。", parent=self.window)
            return
        try:
            action(draft.trader_id)
        except Exception as exc:
            messagebox.showerror(f"{title}失败", str(exc), parent=self.window)
            return
        if title in {"启动", "暂停", "恢复", "平仓"}:
            self._clear_pending_delete(draft.trader_id, reason=f"你手动执行了{title}")
        self._append_log(f"[{draft.trader_id}] {success_text}。")
        self._refresh_views(select_id=draft.trader_id)

    def _on_select(self, *_: object) -> None:
        selected_id = self._selected_trader_id()
        if _should_reload_draft_form(
            explicit_select_id=None,
            selected_id=selected_id,
            loaded_trader_id=self._loaded_trader_id,
            form_dirty=self._form_dirty,
        ):
            self._load_selected_draft_into_form()
        self._refresh_slot_tree()
        self._refresh_event_text()
        self._refresh_detail_text()

    def _load_selected_draft_into_form(self) -> None:
        draft = self._selected_draft()
        symbol_choices = self._symbol_provider()
        for variable in (self.gate_trigger_inst_id_var, self.copy_trade_symbol_var, self.copy_trigger_symbol_var):
            value = variable.get().strip().upper()
            if value and value not in symbol_choices:
                symbol_choices.append(value)
        self._suspend_form_tracking = True
        try:
            if draft is None:
                self.total_quota_var.set("1")
                self.unit_quota_var.set("0.1")
                self.quota_steps_var.set("10")
                self.status_var.set(_draft_status_label("draft"))
                self.notes_var.set("")
                self.auto_restart_on_profit_var.set(True)
                self.pause_on_stop_loss_var.set(True)
                self.gate_enabled_var.set(False)
                self.gate_condition_var.set(_gate_condition_label("always"))
                self.gate_trigger_inst_id_var.set("")
                self.gate_price_type_var.set("mark")
                self.gate_lower_price_var.set("")
                self.gate_upper_price_var.set("")
                self.copy_trade_symbol_var.set("")
                self.copy_trigger_symbol_var.set("")
                self._loaded_trader_id = ""
            else:
                self.total_quota_var.set(_normalize_decimal_text(draft.total_quota))
                self.unit_quota_var.set(_normalize_decimal_text(draft.unit_quota))
                self.quota_steps_var.set(str(draft.quota_steps))
                self.status_var.set(_draft_status_label(draft.status))
                self.notes_var.set(draft.notes)
                self.auto_restart_on_profit_var.set(draft.auto_restart_on_profit)
                self.pause_on_stop_loss_var.set(draft.pause_on_stop_loss)
                self.gate_enabled_var.set(draft.gate.enabled)
                self.gate_condition_var.set(_gate_condition_label(draft.gate.condition))
                self.gate_trigger_inst_id_var.set(draft.gate.trigger_inst_id)
                self.gate_price_type_var.set(draft.gate.trigger_price_type)
                self.gate_lower_price_var.set("" if draft.gate.lower_price is None else _normalize_decimal_text(draft.gate.lower_price))
                self.gate_upper_price_var.set("" if draft.gate.upper_price is None else _normalize_decimal_text(draft.gate.upper_price))
                self.copy_trade_symbol_var.set(_payload_trade_symbol(draft.template_payload))
                self.copy_trigger_symbol_var.set(_payload_signal_symbol(draft.template_payload))
                self._loaded_trader_id = draft.trader_id
        finally:
            self._suspend_form_tracking = False
        self._form_dirty = False
        self._set_combo_values(symbol_choices)

    def _set_combo_values(self, symbol_choices: list[str]) -> None:
        values = list(dict.fromkeys(["", *symbol_choices]))
        for combo in (self._gate_trigger_combo, self._copy_trade_combo, self._copy_trigger_combo):
            if combo is not None:
                combo["values"] = values

    def _refresh_trader_tree(self, *, select_id: str | None = None) -> int:
        if self.tree is None:
            return 0
        selected = select_id or self._selected_trader_id()
        for item_id in self.tree.get_children():
            self.tree.delete(item_id)
        running_count = 0
        for draft in self._snapshot.drafts:
            run_status = self._selected_run_status(draft.trader_id)
            if run_status in {"running", "quota_exhausted"}:
                running_count += 1
            used = trader_used_quota_steps(self._snapshot.slots, draft.trader_id)
            remaining = trader_remaining_quota_steps(draft, self._snapshot.slots)
            open_count, average_entry, _ = trader_open_position_summary(self._snapshot.slots, draft.trader_id)
            realized_pnl = trader_realized_net_pnl(self._snapshot.slots, draft.trader_id)
            used_quota = draft.unit_quota * Decimal(used)
            self.tree.insert(
                "",
                END,
                iid=draft.trader_id,
                values=(
                    draft.trader_id,
                    _strategy_label_from_payload(draft.template_payload),
                    _payload_bar(draft.template_payload),
                    str(draft.template_payload.get("symbol") or "-"),
                    str(draft.template_payload.get("api_name") or "-"),
                    run_status,
                    f"{used}/{draft.quota_steps} | {_normalize_decimal_text(used_quota)}/{_normalize_decimal_text(draft.total_quota)}",
                    f"{open_count} 单 / {_format_optional_decimal(average_entry)}",
                    _format_optional_pnl(realized_pnl),
                    draft.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        if selected and self.tree.exists(selected):
            self.tree.selection_set(selected)
            self.tree.focus(selected)
            self.tree.see(selected)
        return running_count

    def _refresh_slot_tree(self) -> None:
        if self.slot_tree is None:
            return
        trader_id = self._selected_trader_id()
        for item_id in self.slot_tree.get_children():
            self.slot_tree.delete(item_id)
        if not trader_id:
            return
        slots = sorted(
            trader_slots_for(self._snapshot.slots, trader_id),
            key=lambda item: (item.created_at, item.slot_id),
            reverse=True,
        )
        for slot in slots:
            self.slot_tree.insert(
                "",
                END,
                iid=slot.slot_id,
                values=(
                    slot.slot_id,
                    slot.status,
                    slot.session_id,
                    slot.opened_at.strftime("%m-%d %H:%M:%S") if slot.opened_at else "-",
                    _format_optional_decimal(slot.entry_price),
                    _format_optional_decimal(slot.size),
                    slot.closed_at.strftime("%m-%d %H:%M:%S") if slot.closed_at else "-",
                    _format_optional_pnl(slot.net_pnl),
                    slot.close_reason or "-",
                ),
            )

    def _refresh_event_text(self) -> None:
        if self.event_text is None:
            return
        self.event_text.delete("1.0", END)
        trader_id = self._selected_trader_id()
        if not trader_id:
            return
        events = [event for event in self._snapshot.events if event.trader_id == trader_id][:80]
        for event in events:
            self.event_text.insert(
                END,
                f"{event.created_at:%Y-%m-%d %H:%M:%S} [{event.level}] {event.message}\n",
            )

    def _refresh_detail_text(self) -> None:
        if self.detail_text is None:
            return
        self.detail_text.delete("1.0", END)
        draft = self._selected_draft()
        if draft is None:
            self._summary_text.set("交易员策略会固定数量下单，并按额度格持续补 watcher。")
            return
        trader_id = draft.trader_id
        run_status = self._selected_run_status(trader_id)
        run_reason = self._selected_run_reason(trader_id)
        slots = trader_slots_for(self._snapshot.slots, trader_id)
        open_count, average_entry, total_size = trader_open_position_summary(self._snapshot.slots, trader_id)
        close_count, win_count, loss_count = trader_realized_close_counts(self._snapshot.slots, trader_id)
        used = trader_used_quota_steps(self._snapshot.slots, trader_id)
        remaining = trader_remaining_quota_steps(draft, self._snapshot.slots)
        realized_pnl = trader_realized_net_pnl(self._snapshot.slots, trader_id)
        loss_slots = [slot for slot in slots if slot.status == "closed_loss"][:8]
        runtime_snapshot = self._runtime_snapshot_provider(trader_id)
        runtime_status = ""
        runtime_session_id = ""
        if runtime_snapshot is not None:
            runtime_status = str(runtime_snapshot.get("runtime_status") or "").strip()
            runtime_session_id = str(runtime_snapshot.get("session_id") or "").strip()
        self._summary_text.set(
            f"运行状态：{run_status}"
            + (f" | 原因：{run_reason}" if run_reason else "")
            + f" | 周期：{_payload_bar(draft.template_payload)}"
            + (
                f" | watcher：{runtime_session_id or '-'} {runtime_status}"
                if runtime_session_id or runtime_status
                else ""
            )
            + f" | 额度：已用 {used}/{draft.quota_steps}，剩余 {remaining}"
        )
        lines = [
            f"交易员：{trader_id}",
            f"策略：{_strategy_label_from_payload(draft.template_payload)}",
            f"交易标的：{_payload_trade_symbol(draft.template_payload) or '-'}",
            f"触发标的：{_payload_signal_symbol(draft.template_payload) or '-'}",
            f"固定数量：{_normalize_decimal_text(draft.unit_quota)}",
            f"当前持仓：{open_count} 单 | 均价={_format_optional_decimal(average_entry)} | 总数量={_format_optional_decimal(total_size)}",
            f"累计结果：平仓 {close_count} 单 | 盈利 {win_count} | 亏损 {loss_count} | 净盈亏={_format_optional_pnl(realized_pnl)}",
            "",
        ]
        if any(slot.status == "watching" for slot in slots) and open_count == 0:
            lines.append("当前阶段：观察中，尚未开仓；只有满足策略条件后才会真正挂单。")
            lines.append("")
        lines.extend(
            [
                *_build_trader_strategy_lines(draft, runtime_snapshot=runtime_snapshot),
                "",
            ]
        )
        lines.extend(
            [
            "最近亏损单：",
            ]
        )
        if not loss_slots:
            lines.append("- 暂无")
        else:
            for slot in loss_slots:
                cost = None
                if slot.entry_price is not None and slot.size is not None:
                    cost = slot.entry_price * slot.size
                lines.append(
                    " | ".join(
                        [
                            slot.slot_id,
                            f"成本={_format_optional_decimal(cost)}",
                            f"开仓={slot.opened_at:%m-%d %H:%M:%S}" if slot.opened_at else "开仓=-",
                            f"净盈亏={_format_optional_pnl(slot.net_pnl)}",
                            f"原因={slot.close_reason or '-'}",
                        ]
                    )
                )
        self.detail_text.insert(END, "\n".join(lines))

    def _refresh_views(self, *, select_id: str | None = None) -> None:
        selected_id = select_id or self._selected_trader_id()
        reload_form = _should_reload_draft_form(
            explicit_select_id=select_id,
            selected_id=selected_id,
            loaded_trader_id=self._loaded_trader_id,
            form_dirty=self._form_dirty,
        )
        self._load_snapshot()
        if self._pending_delete_trader_id:
            pending_before = self._pending_delete_trader_id
            self._try_complete_pending_delete()
            if self._pending_delete_trader_id != pending_before:
                self._load_snapshot()
        running_count = self._refresh_trader_tree(select_id=select_id)
        self._status_text.set(f"草稿 {len(self._snapshot.drafts)} 条 | 运行中 {running_count} 条")
        if reload_form:
            self._load_selected_draft_into_form()
        self._refresh_slot_tree()
        self._refresh_event_text()
        self._refresh_detail_text()

    def _try_complete_pending_delete(self) -> None:
        trader_id = self._pending_delete_trader_id.strip()
        if not trader_id:
            return
        try:
            self._draft_deleter(trader_id)
        except Exception as exc:
            message = str(exc)
            if "仍有关联会话在运行" in message or "仍有活动中的额度格" in message:
                return
            self._append_log(f"[{trader_id}] 自动辞退失败：{exc}")
            self._pending_delete_trader_id = ""
            return
        self._append_log(f"[{trader_id}] 已自动辞退交易员。")
        self._pending_delete_trader_id = ""

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
