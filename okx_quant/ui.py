from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from tkinter import END, Menu, StringVar, Text, Tk, Toplevel
from tkinter import messagebox, ttk

from okx_quant.engine import StrategyEngine, fetch_hourly_ema_debug, format_hourly_debug
from okx_quant.models import Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import (
    credentials_file_path,
    load_credentials_snapshot,
    save_credentials_snapshot,
)
from okx_quant.strategy_catalog import STRATEGY_DEFINITIONS, StrategyDefinition, get_strategy_definition


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
SIGNAL_LABEL_TO_VALUE = {
    "双向": "both",
    "只做多": "long_only",
    "只做空": "short_only",
}
POSITION_MODE_OPTIONS = {
    "净持仓 net": "net",
    "双向持仓 long/short": "long_short",
}
TRADE_MODE_OPTIONS = {
    "全仓 cross": "cross",
    "逐仓 isolated": "isolated",
}
ENV_OPTIONS = {
    "模拟盘 demo": "demo",
    "实盘 live": "live",
}
TRIGGER_TYPE_OPTIONS = {
    "标记价格 mark": "mark",
    "最新成交价 last": "last",
    "指数价格 index": "index",
}


@dataclass
class StrategySession:
    session_id: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    engine: StrategyEngine
    config: StrategyConfig
    started_at: datetime
    status: str = "运行中"

    @property
    def log_prefix(self) -> str:
        return f"[{self.session_id} {self.strategy_name} {self.symbol}]"


class QuantApp:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("OKX 策略工作台")
        self.root.geometry("1360x900")
        self.root.minsize(1180, 760)

        self.client = OkxRestClient()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.instruments: list[Instrument] = []
        self.sessions: dict[str, StrategySession] = {}
        self._session_counter = 0
        self._settings_window: Toplevel | None = None

        self._strategy_name_to_id = {item.name: item.strategy_id for item in STRATEGY_DEFINITIONS}
        self.strategy_name = StringVar(value=STRATEGY_DEFINITIONS[0].name)

        self.api_key = StringVar()
        self.secret_key = StringVar()
        self.passphrase = StringVar()
        self.environment_label = StringVar(value="模拟盘 demo")

        self.symbol = StringVar(value="BTC-USDT-SWAP")
        self.bar = StringVar(value="15m")
        self.ema_period = StringVar(value="21")
        self.atr_period = StringVar(value="10")
        self.stop_atr = StringVar(value="2")
        self.take_atr = StringVar(value="4")
        self.risk_amount = StringVar(value="100")
        self.poll_seconds = StringVar(value="10")
        self.signal_mode_label = StringVar(value=STRATEGY_DEFINITIONS[0].default_signal_label)
        self.trade_mode_label = StringVar(value="全仓 cross")
        self.position_mode_label = StringVar(value="净持仓 net")
        self.trigger_type_label = StringVar(value="标记价格 mark")
        self.status_text = StringVar(value="运行中策略：0")
        self.settings_summary_text = StringVar()
        self.strategy_summary_text = StringVar()
        self.strategy_rule_text = StringVar()
        self.strategy_hint_text = StringVar()
        self.selected_session_text = StringVar(value="右侧选择一个运行中的策略后，这里会显示详细信息。")

        self._credential_watch_enabled = False
        self._credential_save_job: str | None = None
        self._last_saved_credentials: tuple[str, str, str] | None = None
        self._auto_save_notice_shown = False

        self._load_saved_credentials()
        self._build_menu()
        self._build_layout()
        self._bind_credential_auto_save()
        self._apply_selected_strategy_definition()
        self._update_settings_summary()
        self.root.after(250, self._drain_log_queue)
        self.root.after(500, self._refresh_status)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_menu(self) -> None:
        menu_bar = Menu(self.root)

        settings_menu = Menu(menu_bar, tearoff=False)
        settings_menu.add_command(label="API 与交易设置", command=self.open_settings_window)
        settings_menu.add_separator()
        settings_menu.add_command(label="退出", command=self._on_close)
        menu_bar.add_cascade(label="设置", menu=settings_menu)

        self.root.config(menu=menu_bar)

    def _build_layout(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=3)
        self.root.rowconfigure(2, weight=2)

        header = ttk.Frame(self.root, padding=(16, 16, 16, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        header.columnconfigure(1, weight=1)

        ttk.Label(
            header,
            text="OKX 多策略工作台",
            font=("Microsoft YaHei UI", 20, "bold"),
        ).grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            textvariable=self.status_text,
            font=("Microsoft YaHei UI", 11, "bold"),
        ).grid(row=0, column=1, sticky="e")
        ttk.Label(
            header,
            textvariable=self.settings_summary_text,
            justify="right",
            wraplength=560,
        ).grid(row=1, column=1, sticky="e", pady=(6, 0))

        body = ttk.Panedwindow(self.root, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 10))

        launcher_frame = ttk.Frame(body, padding=12)
        sessions_frame = ttk.Frame(body, padding=12)
        body.add(launcher_frame, weight=3)
        body.add(sessions_frame, weight=2)

        launcher_frame.columnconfigure(0, weight=1)
        launcher_frame.rowconfigure(1, weight=1)
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(0, weight=3)
        sessions_frame.rowconfigure(1, weight=2)

        start_frame = ttk.LabelFrame(launcher_frame, text="策略启动", padding=16)
        start_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            start_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(start_frame, text="选择策略").grid(row=row, column=0, sticky="w")
        self.strategy_combo = ttk.Combobox(
            start_frame,
            textvariable=self.strategy_name,
            values=[item.name for item in STRATEGY_DEFINITIONS],
            state="readonly",
        )
        self.strategy_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        self.strategy_combo.bind("<<ComboboxSelected>>", self._on_strategy_selected)
        ttk.Label(start_frame, text="交易对").grid(row=row, column=2, sticky="w")
        self.symbol_combo = ttk.Combobox(start_frame, textvariable=self.symbol, state="normal")
        self.symbol_combo.grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(start_frame, text="K线周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(start_frame, textvariable=self.bar, values=BAR_OPTIONS, state="readonly").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="信号方向").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self.signal_combo = ttk.Combobox(start_frame, textvariable=self.signal_mode_label, state="readonly")
        self.signal_combo.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="EMA 周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.ema_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="ATR 周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.atr_period).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="止损 ATR 倍数").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.stop_atr).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="止盈 ATR 倍数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.take_atr).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="风险金").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.risk_amount).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        button_row = ttk.Frame(start_frame)
        button_row.grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))
        ttk.Button(button_row, text="启动", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="加载 OKX SWAP", command=self.load_symbols).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="输出 1小时调试", command=self.debug_hourly_values).grid(row=0, column=2)

        row += 1
        ttk.Label(
            start_frame,
            text="API Key、交易模式、持仓模式等设置已移动到菜单：设置 > API 与交易设置",
            wraplength=760,
            justify="left",
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(14, 0))

        strategy_info = ttk.LabelFrame(launcher_frame, text="策略说明", padding=16)
        strategy_info.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        strategy_info.columnconfigure(0, weight=1)

        ttk.Label(strategy_info, text="策略简介", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_summary_text,
            wraplength=780,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="规则说明", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=2, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_rule_text,
            wraplength=780,
            justify="left",
        ).grid(row=3, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="参数提示", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_hint_text,
            wraplength=780,
            justify="left",
        ).grid(row=5, column=0, sticky="w", pady=(6, 0))

        running_frame = ttk.LabelFrame(sessions_frame, text="运行中策略", padding=12)
        running_frame.grid(row=0, column=0, sticky="nsew")
        running_frame.columnconfigure(0, weight=1)
        running_frame.rowconfigure(0, weight=1)

        self.session_tree = ttk.Treeview(
            running_frame,
            columns=("strategy", "symbol", "direction", "status", "started"),
            show="headings",
            selectmode="browse",
        )
        self.session_tree.heading("strategy", text="策略")
        self.session_tree.heading("symbol", text="交易对")
        self.session_tree.heading("direction", text="方向")
        self.session_tree.heading("status", text="状态")
        self.session_tree.heading("started", text="启动时间")
        self.session_tree.column("strategy", width=150, anchor="w")
        self.session_tree.column("symbol", width=130, anchor="w")
        self.session_tree.column("direction", width=90, anchor="center")
        self.session_tree.column("status", width=90, anchor="center")
        self.session_tree.column("started", width=150, anchor="center")
        self.session_tree.grid(row=0, column=0, sticky="nsew")
        self.session_tree.bind("<<TreeviewSelect>>", self._on_session_selected)

        tree_scroll = ttk.Scrollbar(running_frame, orient="vertical", command=self.session_tree.yview)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=tree_scroll.set)

        control_row = ttk.Frame(running_frame)
        control_row.grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(control_row, text="停止选中策略", command=self.stop_selected_session).grid(row=0, column=0)

        detail_frame = ttk.LabelFrame(sessions_frame, text="选中策略详情", padding=16)
        detail_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        ttk.Label(
            detail_frame,
            textvariable=self.selected_session_text,
            wraplength=420,
            justify="left",
        ).grid(row=0, column=0, sticky="nw")

        log_frame = ttk.LabelFrame(self.root, text="运行日志", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = Text(log_frame, height=18, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def open_settings_window(self) -> None:
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("API 与交易设置")
        window.geometry("760x420")
        window.minsize(680, 360)
        window.transient(self.root)
        self._settings_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_settings_window)

        frame = ttk.Frame(window, padding=16)
        frame.pack(fill="both", expand=True)
        for column in range(4):
            frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(frame, text="API Key").grid(row=row, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.api_key).grid(row=row, column=1, sticky="ew", padx=(0, 16))
        ttk.Label(frame, text="Passphrase").grid(row=row, column=2, sticky="w")
        ttk.Entry(frame, textvariable=self.passphrase, show="*").grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(frame, text="Secret Key").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(frame, textvariable=self.secret_key, show="*").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(frame, text="环境").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            frame,
            textvariable=self.environment_label,
            values=list(ENV_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(frame, text="交易模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            frame,
            textvariable=self.trade_mode_label,
            values=list(TRADE_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(frame, text="持仓模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            frame,
            textvariable=self.position_mode_label,
            values=list(POSITION_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(frame, text="TP/SL 触发价类型").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            frame,
            textvariable=self.trigger_type_label,
            values=list(TRIGGER_TYPE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))

        row += 1
        ttk.Label(
            frame,
            text=f"凭证会自动保存到本地文件：{credentials_file_path().name}",
            wraplength=680,
            justify="left",
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))

        row += 1
        footer = ttk.Frame(frame)
        footer.grid(row=row, column=0, columnspan=4, sticky="e", pady=(20, 0))
        ttk.Button(footer, text="关闭", command=self._close_settings_window).grid(row=0, column=0)

    def _close_settings_window(self) -> None:
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.destroy()
        self._settings_window = None

    def _load_saved_credentials(self) -> None:
        try:
            snapshot = load_credentials_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取本地凭证文件失败：{exc}")
            return

        self.api_key.set(snapshot["api_key"])
        self.secret_key.set(snapshot["secret_key"])
        self.passphrase.set(snapshot["passphrase"])

        if any(snapshot.values()):
            self._last_saved_credentials = (
                snapshot["api_key"],
                snapshot["secret_key"],
                snapshot["passphrase"],
            )
            self._enqueue_log(f"已自动读取本地凭证文件：{credentials_file_path().name}")

    def _bind_credential_auto_save(self) -> None:
        self.api_key.trace_add("write", self._on_credentials_changed)
        self.secret_key.trace_add("write", self._on_credentials_changed)
        self.passphrase.trace_add("write", self._on_credentials_changed)
        self.environment_label.trace_add("write", self._on_settings_changed)
        self.trade_mode_label.trace_add("write", self._on_settings_changed)
        self.position_mode_label.trace_add("write", self._on_settings_changed)
        self.trigger_type_label.trace_add("write", self._on_settings_changed)
        self._credential_watch_enabled = True

    def _on_credentials_changed(self, *_: str) -> None:
        if not self._credential_watch_enabled:
            return
        if self._credential_save_job is not None:
            self.root.after_cancel(self._credential_save_job)
        self._credential_save_job = self.root.after(600, self._save_credentials_now)

    def _on_settings_changed(self, *_: str) -> None:
        self._update_settings_summary()

    def _save_credentials_now(self, silent: bool = False) -> None:
        if self._credential_save_job is not None:
            try:
                self.root.after_cancel(self._credential_save_job)
            except Exception:
                pass
            self._credential_save_job = None

        current = (
            self.api_key.get().strip(),
            self.secret_key.get().strip(),
            self.passphrase.get().strip(),
        )
        if current == self._last_saved_credentials:
            return

        try:
            save_credentials_snapshot(*current)
        except Exception as exc:
            if not silent:
                self._enqueue_log(f"自动保存凭证失败：{exc}")
            return

        self._last_saved_credentials = current
        if not silent and any(current) and not self._auto_save_notice_shown:
            self._enqueue_log(f"已自动保存 API 凭证到：{credentials_file_path().name}")
            self._auto_save_notice_shown = True

    def load_symbols(self) -> None:
        self._enqueue_log("正在从 OKX 加载永续合约列表...")
        threading.Thread(target=self._load_symbols_worker, daemon=True).start()

    def _load_symbols_worker(self) -> None:
        try:
            instruments = [item for item in self.client.get_swap_instruments() if item.state.lower() == "live"]
            symbols = [item.inst_id for item in instruments]
            self.root.after(0, lambda: self._apply_symbols(instruments, symbols))
        except Exception as exc:
            self._enqueue_log(f"加载交易对失败：{exc}")

    def _apply_symbols(self, instruments: list[Instrument], symbols: list[str]) -> None:
        self.instruments = instruments
        self.symbol_combo["values"] = symbols
        if self.symbol.get() not in symbols and symbols:
            self.symbol.set(symbols[0])
        self._enqueue_log(f"已加载 {len(symbols)} 个可交易永续合约。")

    def start(self) -> None:
        try:
            definition = self._selected_strategy_definition()
            credentials, config = self._collect_inputs(definition)
            if not self._confirm_start(definition, config):
                return

            self._save_credentials_now(silent=True)

            session_id = self._next_session_id()
            engine = StrategyEngine(self.client, self._make_session_logger(session_id, definition.name, config.inst_id))
            session = StrategySession(
                session_id=session_id,
                strategy_id=definition.strategy_id,
                strategy_name=definition.name,
                symbol=config.inst_id,
                direction_label=self.signal_mode_label.get(),
                engine=engine,
                config=config,
                started_at=datetime.now(),
            )

            self.sessions[session_id] = session
            self._upsert_session_row(session)
            engine.start(credentials, config)
            self.session_tree.selection_set(session_id)
            self.session_tree.focus(session_id)
            self._refresh_selected_session_details()
            self._enqueue_log(f"{session.log_prefix} 已提交启动请求。")
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc))

    def stop_selected_session(self) -> None:
        session = self._selected_session()
        if session is None:
            messagebox.showinfo("提示", "请先在右侧选择一个策略会话")
            return
        if not session.engine.is_running:
            messagebox.showinfo("提示", "这个策略已经停止了")
            return

        session.status = "停止中"
        session.engine.stop()
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._enqueue_log(f"{session.log_prefix} 已请求停止。")

    def debug_hourly_values(self) -> None:
        symbol = self.symbol.get().strip().upper()
        if not symbol:
            messagebox.showerror("提示", "请先选择交易对")
            return
        ema_period = self._parse_positive_int(self.ema_period.get(), "EMA 周期")
        self._enqueue_log(f"正在获取 {symbol} 的 1 小时调试值，EMA 周期={ema_period} ...")
        threading.Thread(
            target=self._debug_hourly_values_worker,
            args=(symbol, ema_period),
            daemon=True,
        ).start()

    def _debug_hourly_values_worker(self, symbol: str, ema_period: int) -> None:
        try:
            snapshot = fetch_hourly_ema_debug(self.client, symbol, ema_period=ema_period)
            self._enqueue_log(format_hourly_debug(symbol, snapshot))
        except Exception as exc:
            self._enqueue_log(f"获取 1 小时调试值失败：{exc}")

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        self.signal_combo["values"] = definition.allowed_signal_labels
        if self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        self.strategy_summary_text.set(definition.summary)
        self.strategy_rule_text.set(definition.rule_description)
        self.strategy_hint_text.set(definition.parameter_hint)

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _confirm_start(self, definition: StrategyDefinition, config: StrategyConfig) -> bool:
        message = (
            f"策略：{definition.name}\n"
            f"交易对：{config.inst_id}\n"
            f"K线周期：{config.bar}\n"
            f"方向：{self.signal_mode_label.get()}\n"
            f"EMA 周期：{config.ema_period}\n"
            f"ATR 周期：{config.atr_period}\n"
            f"风险金：{self.risk_amount.get().strip()}\n\n"
            f"{definition.rule_description}\n\n"
            "确认启动这个策略吗？"
        )
        return messagebox.askokcancel(f"确认启动 {definition.name}", message)

    def _collect_inputs(self, definition: StrategyDefinition) -> tuple[Credentials, StrategyConfig]:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        symbol = self.symbol.get().strip().upper()

        if not api_key or not secret_key or not passphrase:
            raise ValueError("请先在 菜单 > 设置 > API 与交易设置 中填写 API 凭证")
        if not symbol:
            raise ValueError("请选择交易对")

        credentials = Credentials(api_key=api_key, secret_key=secret_key, passphrase=passphrase)
        config = StrategyConfig(
            inst_id=symbol,
            bar=self.bar.get(),
            ema_period=self._parse_positive_int(self.ema_period.get(), "EMA 周期"),
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=Decimal("0"),
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=definition.strategy_id,
            poll_seconds=float(self._parse_positive_decimal(self.poll_seconds.get(), "轮询秒数")),
            risk_amount=self._parse_positive_decimal(self.risk_amount.get(), "风险金"),
        )
        return credentials, config

    def _update_settings_summary(self) -> None:
        api_status = "API 已配置" if all(
            [self.api_key.get().strip(), self.secret_key.get().strip(), self.passphrase.get().strip()]
        ) else "API 未配置"
        self.settings_summary_text.set(
            f"{api_status} | {self.environment_label.get()} | {self.trade_mode_label.get()} | {self.position_mode_label.get()}"
        )

    def _make_session_logger(self, session_id: str, strategy_name: str, symbol: str):
        prefix = f"[{session_id} {strategy_name} {symbol}]"

        def _logger(message: str) -> None:
            self._enqueue_log(f"{prefix} {message}")

        return _logger

    def _next_session_id(self) -> str:
        self._session_counter += 1
        return f"S{self._session_counter:02d}"

    def _upsert_session_row(self, session: StrategySession) -> None:
        values = (
            session.strategy_name,
            session.symbol,
            session.direction_label,
            session.status,
            session.started_at.strftime("%H:%M:%S"),
        )
        if self.session_tree.exists(session.session_id):
            self.session_tree.item(session.session_id, values=values)
        else:
            self.session_tree.insert("", END, iid=session.session_id, values=values)

    def _selected_session(self) -> StrategySession | None:
        selected = self.session_tree.selection()
        if not selected:
            return None
        return self.sessions.get(selected[0])

    def _on_session_selected(self, *_: object) -> None:
        self._refresh_selected_session_details()

    def _refresh_selected_session_details(self) -> None:
        session = self._selected_session()
        if session is None:
            self.selected_session_text.set("右侧选择一个运行中的策略后，这里会显示详细信息。")
            return

        definition = get_strategy_definition(session.strategy_id)
        self.selected_session_text.set(
            f"会话：{session.session_id}\n"
            f"状态：{session.status}\n"
            f"策略：{session.strategy_name}\n"
            f"交易对：{session.symbol}\n"
            f"方向：{session.direction_label}\n"
            f"K线周期：{session.config.bar}\n"
            f"EMA 周期：{session.config.ema_period}\n"
            f"ATR 周期：{session.config.atr_period}\n"
            f"止损 ATR 倍数：{session.config.atr_stop_multiplier}\n"
            f"止盈 ATR 倍数：{session.config.atr_take_multiplier}\n"
            f"风险金：{session.config.risk_amount}\n"
            f"轮询秒数：{session.config.poll_seconds}\n"
            f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"策略简介：{definition.summary}\n\n"
            f"规则说明：{definition.rule_description}\n\n"
            f"参数提示：{definition.parameter_hint}"
        )

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        value = int(raw)
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _parse_positive_decimal(self, raw: str, field_name: str) -> Decimal:
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0")
        return value

    def _enqueue_log(self, message: str) -> None:
        self.log_queue.put(message)

    def _drain_log_queue(self) -> None:
        while not self.log_queue.empty():
            line = self.log_queue.get_nowait()
            self.log_text.insert(END, line + "\n")
            self.log_text.see(END)
        self.root.after(250, self._drain_log_queue)

    def _refresh_status(self) -> None:
        running_count = 0
        for session in self.sessions.values():
            if session.engine.is_running:
                if session.status != "停止中":
                    session.status = "运行中"
                running_count += 1
            elif session.status in {"运行中", "停止中"}:
                session.status = "已停止"
            self._upsert_session_row(session)

        self.status_text.set(f"运行中策略：{running_count}")
        self._update_settings_summary()
        self._refresh_selected_session_details()
        self.root.after(500, self._refresh_status)

    def _on_close(self) -> None:
        self._save_credentials_now(silent=True)
        for session in self.sessions.values():
            session.engine.stop()
        self._close_settings_window()
        self.root.destroy()


def run_app() -> None:
    app = QuantApp()
    app.root.mainloop()
