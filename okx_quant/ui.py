from __future__ import annotations

import queue
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import tkinter.font as tkfont
from tkinter import BooleanVar, END, Menu, StringVar, Text, Tk, Toplevel, simpledialog
from tkinter import messagebox, ttk

from okx_quant.app_meta import APP_VERSION, build_app_title, build_version_info_text
from okx_quant.backtest_ui import BacktestCompareOverviewWindow, BacktestLaunchState, BacktestWindow
from okx_quant.deribit_client import DeribitRestClient
from okx_quant.deribit_volatility_monitor_ui import DeribitVolatilityMonitorWindow
from okx_quant.deribit_volatility_ui import DeribitVolatilityWindow
from okx_quant.engine import StrategyEngine, fetch_hourly_ema_debug, format_hourly_debug
from okx_quant.log_utils import append_log_line
from okx_quant.models import Credentials, EmailNotificationConfig, Instrument, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.option_strategy_ui import OptionStrategyCalculatorWindow
from okx_quant.okx_client import (
    OkxAccountAssetItem,
    OkxAccountConfig,
    OkxAccountOverview,
    OkxApiError,
    OkxFillHistoryItem,
    OkxPosition,
    OkxPositionHistoryItem,
    OkxRestClient,
    OkxTicker,
    infer_inst_type,
    infer_option_family,
)
from okx_quant.persistence import (
    credentials_file_path,
    DEFAULT_CREDENTIAL_PROFILE_NAME,
    load_credentials_profiles_snapshot,
    load_notification_snapshot,
    save_credentials_profiles_snapshot,
    save_notification_snapshot,
    settings_file_path,
)
from okx_quant.position_protection import (
    OptionProtectionConfig,
    PositionProtectionManager,
    build_close_order_price_from_mark,
    describe_protection_price_logic,
    derive_position_direction,
    infer_protection_profit_on_rise,
    infer_default_spot_inst_id,
    normalize_spot_inst_id,
    validate_live_protection_order_price_guard,
)
from okx_quant.protection_replay_ui import ProtectionReplayLaunchState, ProtectionReplayWindow
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.signal_monitor import DEFAULT_MONITOR_SYMBOLS
from okx_quant.signal_monitor_ui import SignalMonitorWindow
from okx_quant.smart_order import SmartOrderRuntimeConfig
from okx_quant.smart_order_ui import SmartOrderWindow
from okx_quant.strategy_catalog import (
    STRATEGY_DEFINITIONS,
    STRATEGY_EMA5_EMA8_ID,
    StrategyDefinition,
    get_strategy_definition,
)
from okx_quant.window_layout import (
    apply_adaptive_window_geometry,
    apply_fill_window_geometry,
    apply_window_icon,
)


BAR_OPTIONS = ["1m", "3m", "5m", "15m", "1H", "4H"]
POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS = {
    "positions": (
        "inst_type",
        "time_value",
        "time_value_usdt",
        "intrinsic_value",
        "intrinsic_usdt",
        "bid_price",
        "bid_usdt",
        "ask_price",
        "ask_usdt",
        "mark",
        "mark_usdt",
        "avg",
        "avg_usdt",
        "pos",
        "option_side",
        "upl",
        "upl_usdt",
        "realized",
        "market_value",
        "mgn_ratio",
        "delta",
        "gamma",
        "vega",
        "theta",
        "theta_usdt",
    ),
}
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
TP_SL_MODE_OPTIONS = {
    "OKX 托管（仅同标的永续）": "exchange",
    "按下单标的价格（本地）": "local_trade",
    "按信号标的价格（本地）": "local_signal",
    "按自定义标的价格（本地）": "local_custom",
}
ENTRY_SIDE_MODE_OPTIONS = {
    "跟随信号": "follow_signal",
    "固定买入": "fixed_buy",
    "固定卖出": "fixed_sell",
}
RUN_MODE_OPTIONS = {
    "交易并下单": "trade",
    "只发信号邮件": "signal_only",
}
POSITION_TYPE_OPTIONS = {
    "全部类型": "",
    "永续 SWAP": "SWAP",
    "交割 FUTURES": "FUTURES",
    "期权 OPTION": "OPTION",
    "现货 SPOT": "SPOT",
}
HISTORY_MARGIN_MODE_FILTER_OPTIONS = {
    "全部模式": "",
    "全仓 cross": "cross",
    "逐仓 isolated": "isolated",
}
HISTORY_FILL_SIDE_FILTER_OPTIONS = {
    "全部方向": "",
    "买入 buy": "buy",
    "卖出 sell": "sell",
}
POSITION_REFRESH_INTERVAL_OPTIONS = {
    "10秒": 10_000,
    "15秒": 15_000,
    "30秒": 30_000,
    "60秒": 60_000,
}
PROTECTION_TRIGGER_SOURCE_OPTIONS = {
    "期权标记价格": "option_mark",
    "现货最新价": "spot_last",
}
PROTECTION_ORDER_MODE_OPTIONS = {
    "设定价格": "fixed_price",
    "标记价格加减滑点": "mark_with_slippage",
}


@dataclass
class StrategySession:
    session_id: str
    api_name: str
    strategy_id: str
    strategy_name: str
    symbol: str
    direction_label: str
    run_mode_label: str
    engine: StrategyEngine
    config: StrategyConfig
    started_at: datetime
    status: str = "运行中"

    @property
    def log_prefix(self) -> str:
        if self.api_name:
            return f"[{self.api_name}] [{self.session_id} {self.strategy_name} {self.symbol}]"
        return f"[{self.session_id} {self.strategy_name} {self.symbol}]"


class QuantApp:
    def __init__(self) -> None:
        self.root = Tk()
        apply_window_icon(self.root)
        self.root.title(build_app_title())
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        default_width = min(max(int(screen_width * 0.9), 1560), max(screen_width - 80, 1560))
        default_height = min(max(int(screen_height * 0.88), 980), max(screen_height - 80, 980))
        offset_x = max((screen_width - default_width) // 2, 20)
        offset_y = max((screen_height - default_height) // 2 - 12, 20)
        self.root.geometry(f"{default_width}x{default_height}+{offset_x}+{offset_y}")
        self.root.minsize(1420, 900)

        self.client = OkxRestClient()
        self.deribit_client = DeribitRestClient()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.instruments: list[Instrument] = []
        self.sessions: dict[str, StrategySession] = {}
        self._session_counter = 0
        self._settings_window: Toplevel | None = None
        self._backtest_window: BacktestWindow | None = None
        self._backtest_compare_window: BacktestCompareOverviewWindow | None = None
        self._signal_monitor_window: SignalMonitorWindow | None = None
        self._smart_order_window: SmartOrderWindow | None = None
        self._deribit_volatility_monitor_window: DeribitVolatilityMonitorWindow | None = None
        self._deribit_volatility_window: DeribitVolatilityWindow | None = None
        self._option_strategy_window: OptionStrategyCalculatorWindow | None = None
        self._positions_zoom_window: Toplevel | None = None
        self._protection_window: Toplevel | None = None
        self._protection_replay_window: ProtectionReplayWindow | None = None
        self._positions_refreshing = False
        self._positions_history_refreshing = False
        self._default_symbol_values = [label for label, _ in DEFAULT_MONITOR_SYMBOLS]
        self._latest_positions: list[OkxPosition] = []
        self._latest_fill_history: list[OkxFillHistoryItem] = []
        self._latest_position_history: list[OkxPositionHistoryItem] = []
        self._positions_context_note: str | None = None
        self._positions_last_refresh_at: datetime | None = None
        self._positions_history_last_refresh_at: datetime | None = None
        self._positions_effective_environment: str | None = None
        self._upl_usdt_prices: dict[str, Decimal] = {}
        self._position_history_usdt_prices: dict[str, Decimal] = {}
        self._position_instruments: dict[str, Instrument] = {}
        self._position_tickers: dict[str, OkxTicker] = {}
        self._position_row_payloads: dict[str, dict[str, object]] = {}
        self._positions_view_rendering = False
        self._selected_session_detail: Text | None = None
        self._position_detail_panel: Text | None = None
        self._positions_zoom_tree: ttk.Treeview | None = None
        self._positions_zoom_detail: Text | None = None
        self._positions_zoom_notebook: ttk.Notebook | None = None
        self._positions_zoom_fills_tree: ttk.Treeview | None = None
        self._positions_zoom_fills_detail: Text | None = None
        self._positions_zoom_position_history_tree: ttk.Treeview | None = None
        self._positions_zoom_position_history_detail: Text | None = None
        self._account_info_window: Toplevel | None = None
        self._account_info_tree: ttk.Treeview | None = None
        self._account_info_detail_panel: Text | None = None
        self._account_info_config_panel: Text | None = None
        self._account_info_refreshing = False
        self._latest_account_overview: OkxAccountOverview | None = None
        self._latest_account_config: OkxAccountConfig | None = None
        self._positions_zoom_column_window: Toplevel | None = None
        self._positions_zoom_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_fills_detail_frame: ttk.LabelFrame | None = None
        self._positions_zoom_position_history_detail_frame: ttk.LabelFrame | None = None
        self._position_selection_syncing = False
        self._positions_zoom_sync_job: str | None = None
        self._positions_zoom_selected_item_id: str | None = None
        self._fills_history_refreshing = False
        self._position_history_refreshing = False
        self._fills_history_last_refresh_at: datetime | None = None
        self._position_history_last_refresh_at: datetime | None = None
        self._positions_zoom_column_groups: dict[str, dict[str, object]] = {}
        self._positions_zoom_column_vars: dict[str, dict[str, BooleanVar]] = {}
        self._main_positions_pane: ttk.Panedwindow | None = None
        self._main_position_detail_frame: ttk.LabelFrame | None = None
        self._main_position_detail_collapsed = True
        self._main_position_detail_toggle_text = StringVar(value="展开持仓详情")
        self._positions_zoom_detail_collapsed = False
        self._positions_zoom_history_collapsed = False
        self._positions_zoom_fills_detail_collapsed = False
        self._positions_zoom_position_history_detail_collapsed = False
        self._positions_zoom_detail_toggle_text = StringVar(value="折叠持仓详情")
        self._positions_zoom_history_toggle_text = StringVar(value="折叠历史区域")
        self._positions_zoom_fills_detail_toggle_text = StringVar(value="折叠成交详情")
        self._positions_zoom_position_history_detail_toggle_text = StringVar(value="折叠仓位详情")
        self._positions_zoom_fills_summary_text = StringVar(value="历史成交尚未读取。")
        self._positions_zoom_fills_load_more_text = StringVar(value="增加100条")
        self._positions_zoom_position_history_summary_text = StringVar(value="历史仓位尚未读取。")
        self._positions_zoom_position_history_base_summary = "历史仓位尚未读取。"
        self._fill_history_fetch_limit = 500
        self._fill_history_load_more_clicks = 0
        self._positions_zoom_summary_text = StringVar(value="当前尚未获取持仓。")
        self._positions_zoom_option_search_hint_text = StringVar(
            value="\u9009\u4e2d\u671f\u6743\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_fill_history_search_hint_text = StringVar(
            value="\u9009\u4e2d\u5386\u53f2\u671f\u6743\u6210\u4ea4\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_position_history_search_hint_text = StringVar(
            value="\u9009\u4e2d\u5386\u53f2\u671f\u6743\u540e\uff0c\u53ef\u4e00\u952e\u5e26\u5165\u5408\u7ea6\u6216\u5230\u671f\u524d\u7f00\u3002"
        )
        self._positions_zoom_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_apply_expiry_prefix_button: ttk.Button | None = None
        self._positions_zoom_fills_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_fills_apply_expiry_prefix_button: ttk.Button | None = None
        self._positions_zoom_position_history_apply_contract_button: ttk.Button | None = None
        self._positions_zoom_position_history_apply_expiry_prefix_button: ttk.Button | None = None
        self._main_body_pane: ttk.Panedwindow | None = None
        self._sessions_pane: ttk.Panedwindow | None = None
        self._protection_sessions_tree: ttk.Treeview | None = None
        self._protection_detail_text: Text | None = None
        self._protection_form_title_text = StringVar(value="请选择一个期权持仓后，再设置保护。")
        self._protection_logic_hint_text = StringVar(value="请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
        self._protection_status_text = StringVar(value="当前没有运行中的期权持仓保护任务。")
        self._protection_selected_session_id: str | None = None
        self._protection_form_position_id: str | None = None
        self._protection_form_position_key: str | None = None
        self._protection_take_profit_order_price_entry: ttk.Entry | None = None
        self._protection_stop_loss_order_price_entry: ttk.Entry | None = None
        self._protection_take_profit_slippage_entry: ttk.Entry | None = None
        self._protection_stop_loss_slippage_entry: ttk.Entry | None = None
        self._protection_take_profit_fixed_price_memory = ""
        self._protection_stop_loss_fixed_price_memory = ""
        self._protection_order_mode_job: str | None = None

        self._protection_manager = PositionProtectionManager(
            self.client,
            self._make_system_logger("持仓保护"),
            notifier=None,
            on_change=self._schedule_protection_window_refresh,
        )

        self._strategy_name_to_id = {item.name: item.strategy_id for item in STRATEGY_DEFINITIONS}
        self.strategy_name = StringVar(value=STRATEGY_DEFINITIONS[0].name)

        self.api_key = StringVar()
        self.secret_key = StringVar()
        self.passphrase = StringVar()
        self.api_profile_name = StringVar(value=DEFAULT_CREDENTIAL_PROFILE_NAME)
        self.environment_label = StringVar(value="模拟盘 demo")

        self.symbol = StringVar(value=self._default_symbol_values[0])
        self.trade_symbol = StringVar(value="")
        self.local_tp_sl_symbol = StringVar(value="")
        self.bar = StringVar(value="15m")
        self.ema_period = StringVar(value="21")
        self.trend_ema_period = StringVar(value="55")
        self.big_ema_period = StringVar(value="233")
        self.atr_period = StringVar(value="10")
        self.stop_atr = StringVar(value="2")
        self.take_atr = StringVar(value="4")
        self.risk_amount = StringVar(value="100")
        self.order_size = StringVar(value="1")
        self.poll_seconds = StringVar(value="10")
        self.signal_mode_label = StringVar(value=STRATEGY_DEFINITIONS[0].default_signal_label)
        self.run_mode_label = StringVar(value="交易并下单")
        self.trade_mode_label = StringVar(value="全仓 cross")
        self.position_mode_label = StringVar(value="净持仓 net")
        self.trigger_type_label = StringVar(value="标记价格 mark")
        self.tp_sl_mode_label = StringVar(value="OKX 托管（仅同标的永续）")
        self.entry_side_mode_label = StringVar(value="跟随信号")

        self.notify_enabled = BooleanVar(value=False)
        self.smtp_host = StringVar()
        self.smtp_port = StringVar(value="465")
        self.smtp_username = StringVar()
        self.smtp_password = StringVar()
        self.sender_email = StringVar()
        self.recipient_emails = StringVar()
        self.use_ssl = BooleanVar(value=True)
        self.notify_trade_fills = BooleanVar(value=True)
        self.notify_signals = BooleanVar(value=True)
        self.notify_errors = BooleanVar(value=True)
        self.position_type_filter = StringVar(value="全部类型")
        self.position_keyword = StringVar()
        self.fill_history_type_filter = StringVar(value="全部类型")
        self.fill_history_side_filter = StringVar(value="全部方向")
        self.fill_history_keyword = StringVar()
        self.position_history_type_filter = StringVar(value="全部类型")
        self.position_history_margin_filter = StringVar(value="全部模式")
        self.position_history_keyword = StringVar()
        self.position_refresh_interval_label = StringVar(value="15秒")
        self.position_auto_refresh_button_text = StringVar(value="暂停自动刷新")
        self.position_auto_refresh_enabled = True
        self.protection_trigger_source_label = StringVar(value="期权标记价格")
        self.protection_spot_symbol = StringVar()
        self.protection_take_profit_trigger = StringVar()
        self.protection_stop_loss_trigger = StringVar()
        self.protection_take_profit_order_mode_label = StringVar(value="设定价格")
        self.protection_take_profit_order_price = StringVar()
        self.protection_take_profit_slippage = StringVar(value="0")
        self.protection_stop_loss_order_mode_label = StringVar(value="设定价格")
        self.protection_stop_loss_order_price = StringVar()
        self.protection_stop_loss_slippage = StringVar(value="0")
        self.protection_poll_seconds = StringVar(value="2")

        self.status_text = StringVar(value="运行中策略：0")
        self.settings_summary_text = StringVar()
        self.strategy_summary_text = StringVar()
        self.strategy_rule_text = StringVar()
        self.strategy_hint_text = StringVar()
        self.selected_session_text = StringVar(value=self._default_selected_session_text())
        self.positions_summary_text = StringVar(value="当前尚未获取持仓。")
        self.position_total_text = StringVar(value="-")
        self.position_upl_text = StringVar(value="-")
        self.position_realized_text = StringVar(value="-")
        self.position_margin_text = StringVar(value="-")
        self.position_delta_text = StringVar(value="-")
        self.position_short_call_text = StringVar(value="-")
        self.position_short_put_text = StringVar(value="-")
        self.position_long_call_text = StringVar(value="-")
        self.position_long_put_text = StringVar(value="-")
        self.position_detail_text = StringVar(value=self._default_position_detail_text())
        self.account_info_summary_text = StringVar(value="尚未读取账户信息。")
        self.account_total_equity_text = StringVar(value="-")
        self.account_adjusted_equity_text = StringVar(value="-")
        self.account_available_equity_text = StringVar(value="-")
        self.account_upl_text = StringVar(value="-")
        self.account_imr_text = StringVar(value="-")
        self.account_mmr_text = StringVar(value="-")
        self._main_position_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_fills_detail_toggle_text.set("\u5c55\u5f00\u6210\u4ea4\u8be6\u60c5")
        self._positions_zoom_position_history_detail_toggle_text.set("\u5c55\u5f00\u4ed3\u4f4d\u8be6\u60c5")

        self._credential_watch_enabled = False
        self._credential_save_job: str | None = None
        self._last_saved_credentials: tuple[str, str, str, str] | None = None
        self._auto_save_notice_shown = False
        self._credential_profiles: dict[str, dict[str, str]] = {}
        self._header_credential_profile_combo: ttk.Combobox | None = None
        self._credential_profile_combo: ttk.Combobox | None = None
        self._loaded_credential_profile_name = DEFAULT_CREDENTIAL_PROFILE_NAME

        self._settings_watch_enabled = False
        self._settings_save_job: str | None = None
        self._last_saved_notification_state: tuple[object, ...] | None = None

        self._load_saved_credentials()
        self._load_saved_notification_settings()
        self._build_menu()
        self._build_layout()
        self._apply_initial_detail_visibility()
        self._bind_auto_save()
        self._apply_selected_strategy_definition()
        self._update_settings_summary()
        self.root.after_idle(self._apply_initial_pane_layout)
        self.root.after(250, self._drain_log_queue)
        self.root.after(500, self._refresh_status)
        self.root.after(1200, self._refresh_positions_periodic)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_menu(self) -> None:
        menu_bar = Menu(self.root)

        settings_menu = Menu(menu_bar, tearoff=False)
        settings_menu.add_command(label="API 与通知设置", command=self.open_settings_window)
        menu_bar.add_cascade(label="设置", menu=settings_menu)

        tools_menu = Menu(menu_bar, tearoff=False)
        tools_menu.add_command(label="打开无限下单", command=self.open_smart_order_window)
        tools_menu.add_command(label="打开回测窗口", command=self.open_backtest_window)
        tools_menu.add_command(label="打开回测对比总览", command=self.open_backtest_compare_window)
        tools_menu.add_command(label="打开信号监控", command=self.open_signal_monitor_window)
        tools_menu.add_command(label="打开波动率监控", command=self.open_deribit_volatility_monitor_window)
        tools_menu.add_command(label="打开Deribit波动率指数", command=self.open_deribit_volatility_window)
        tools_menu.add_command(label="打开期权策略计算器", command=self.open_option_strategy_window)
        tools_menu.add_command(label="刷新账户持仓", command=self.refresh_positions)
        menu_bar.add_cascade(label="工具", menu=tools_menu)

        system_menu = Menu(menu_bar, tearoff=False)
        system_menu.add_command(label=f"版本信息 (v{APP_VERSION})", command=self.show_version_info)
        system_menu.add_separator()
        system_menu.add_command(label="退出", command=self._on_close)
        menu_bar.add_cascade(label="系统", menu=system_menu)

        self.root.config(menu=menu_bar)

    def show_version_info(self) -> None:
        messagebox.showinfo("版本信息", build_version_info_text(), parent=self.root)

    def _build_layout(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=4)
        self.root.rowconfigure(2, weight=1)

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
        summary_row = ttk.Frame(header)
        summary_row.grid(row=1, column=1, sticky="e", pady=(6, 0))
        ttk.Label(summary_row, text="API").grid(row=0, column=0, sticky="e")
        self._header_credential_profile_combo = ttk.Combobox(
            summary_row,
            textvariable=self.api_profile_name,
            values=self._credential_profile_names(),
            state="readonly",
            width=10,
        )
        self._header_credential_profile_combo.grid(row=0, column=1, sticky="e", padx=(4, 8))
        self._header_credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        ttk.Label(
            summary_row,
            textvariable=self.settings_summary_text,
            justify="right",
            wraplength=540,
        ).grid(row=0, column=2, sticky="e")

        body = ttk.Panedwindow(self.root, orient="horizontal")
        body.grid(row=1, column=0, sticky="nsew", padx=16, pady=(0, 10))
        self._main_body_pane = body

        launcher_frame = ttk.Frame(body, padding=12)
        sessions_frame = ttk.Frame(body, padding=12)
        body.add(launcher_frame, weight=2)
        body.add(sessions_frame, weight=3)

        launcher_frame.columnconfigure(0, weight=1)
        launcher_frame.rowconfigure(1, weight=1)
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(0, weight=1)

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
        ttk.Label(start_frame, text="信号标的").grid(row=row, column=2, sticky="w")
        self.symbol_combo = ttk.Combobox(
            start_frame,
            textvariable=self.symbol,
            values=self._default_symbol_values,
            state="normal",
        )
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
        ttk.Label(start_frame, text="运行模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            start_frame,
            textvariable=self.run_mode_label,
            values=list(RUN_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(start_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="EMA小周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.ema_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="EMA中周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.trend_ema_period).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(start_frame, text="EMA大周期").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.big_ema_period).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="ATR 周期").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.atr_period).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

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
        ttk.Label(start_frame, text="固定数量").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.order_size).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="下单标的").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(start_frame, textvariable=self.trade_symbol, values=self._default_symbol_values, state="normal").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(start_frame, text="下单方向模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            start_frame,
            textvariable=self.entry_side_mode_label,
            values=list(ENTRY_SIDE_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(start_frame, text="止盈止损模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            start_frame,
            textvariable=self.tp_sl_mode_label,
            values=list(TP_SL_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(start_frame, text="自定义触发标的").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(start_frame, textvariable=self.local_tp_sl_symbol).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        button_row = ttk.Frame(start_frame)
        button_row.grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))
        ttk.Button(button_row, text="启动", command=self.start).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_row, text="加载 OKX SWAP", command=self.load_symbols).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_row, text="导出 1小时调试", command=self.debug_hourly_values).grid(row=0, column=2)

        row += 1
        ttk.Label(
            start_frame,
            text="API、交易模式、持仓模式和邮件通知都已移动到菜单：设置 > API 与通知设置",
            wraplength=820,
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
            wraplength=820,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="规则说明", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=2, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_rule_text,
            wraplength=820,
            justify="left",
        ).grid(row=3, column=0, sticky="w", pady=(6, 12))

        ttk.Label(strategy_info, text="参数提示", font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=4, column=0, sticky="w"
        )
        ttk.Label(
            strategy_info,
            textvariable=self.strategy_hint_text,
            wraplength=820,
            justify="left",
        ).grid(row=5, column=0, sticky="w", pady=(6, 0))

        sessions_pane = ttk.Panedwindow(sessions_frame, orient="vertical")
        sessions_pane.grid(row=0, column=0, sticky="nsew")
        self._sessions_pane = sessions_pane

        session_top_frame = ttk.Frame(sessions_pane)
        session_top_frame.columnconfigure(0, weight=1)
        session_top_frame.rowconfigure(0, weight=4)
        session_top_frame.rowconfigure(1, weight=1)
        sessions_pane.add(session_top_frame, weight=2)

        running_frame = ttk.LabelFrame(session_top_frame, text="运行中策略", padding=12)
        running_frame.grid(row=0, column=0, sticky="nsew")
        running_frame.columnconfigure(0, weight=1)
        running_frame.rowconfigure(0, weight=1)

        self.session_tree = ttk.Treeview(
            running_frame,
            columns=("api", "strategy", "symbol", "direction", "mode", "status", "started"),
            show="headings",
            selectmode="browse",
        )
        self.session_tree.heading("api", text="API")
        self.session_tree.heading("strategy", text="策略")
        self.session_tree.heading("symbol", text="信号 -> 下单")
        self.session_tree.heading("direction", text="方向")
        self.session_tree.heading("mode", text="模式")
        self.session_tree.heading("status", text="状态")
        self.session_tree.heading("started", text="启动时间")
        self.session_tree.column("api", width=96, anchor="center")
        self.session_tree.column("strategy", width=130, anchor="w")
        self.session_tree.column("symbol", width=180, anchor="w")
        self.session_tree.column("direction", width=90, anchor="center")
        self.session_tree.column("mode", width=110, anchor="center")
        self.session_tree.column("status", width=80, anchor="center")
        self.session_tree.column("started", width=120, anchor="center")
        self.session_tree.grid(row=0, column=0, sticky="nsew")
        self.session_tree.bind("<<TreeviewSelect>>", self._on_session_selected)

        tree_scroll = ttk.Scrollbar(running_frame, orient="vertical", command=self.session_tree.yview)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.session_tree.configure(yscrollcommand=tree_scroll.set)

        control_row = ttk.Frame(running_frame)
        control_row.grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(control_row, text="停止选中策略", command=self.stop_selected_session).grid(row=0, column=0)

        detail_frame = ttk.LabelFrame(session_top_frame, text="选中策略详情", padding=16)
        detail_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._selected_session_detail = Text(
            detail_frame,
            height=9,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._selected_session_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._selected_session_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._selected_session_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())

        positions_frame = ttk.LabelFrame(sessions_pane, text="账户持仓（仿 OKX 客户端风格）", padding=12)
        positions_frame.columnconfigure(0, weight=1)
        positions_frame.rowconfigure(3, weight=1)
        sessions_pane.add(positions_frame, weight=7)

        header_row = ttk.Frame(positions_frame)
        header_row.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header_row.columnconfigure(0, weight=1)
        ttk.Label(header_row, textvariable=self.positions_summary_text).grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(header_row)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(action_row, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 6))
        ttk.Label(action_row, text="自动刷新").grid(row=0, column=1, padx=(0, 6))
        position_refresh_interval_combo = ttk.Combobox(
            action_row,
            textvariable=self.position_refresh_interval_label,
            values=list(POSITION_REFRESH_INTERVAL_OPTIONS.keys()),
            state="readonly",
            width=8,
        )
        position_refresh_interval_combo.grid(row=0, column=2, padx=(0, 6))
        position_refresh_interval_combo.bind("<<ComboboxSelected>>", self._on_position_refresh_interval_changed)
        ttk.Button(
            action_row,
            textvariable=self.position_auto_refresh_button_text,
            command=self.toggle_position_auto_refresh,
        ).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(action_row, text="账户信息", command=self.open_account_info_window).grid(row=0, column=4, padx=(0, 6))
        ttk.Button(action_row, text="持仓大窗", command=self.open_positions_zoom_window).grid(row=0, column=5, padx=(0, 6))
        ttk.Button(action_row, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(action_row, text="全部展开", command=self.expand_all_position_groups).grid(
            row=0, column=7, padx=(0, 6)
        )
        ttk.Button(action_row, text="折叠分组", command=self.collapse_position_groups).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(action_row, text="复制合约", command=self.copy_selected_position_symbol).grid(row=0, column=9)

        filter_row = ttk.Frame(positions_frame)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        position_keyword_entry = ttk.Entry(filter_row, textvariable=self.position_keyword)
        position_keyword_entry.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_view).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_filters).grid(row=0, column=5)

        overview_row = ttk.Frame(positions_frame)
        overview_row.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        for column in range(9):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "持仓笔数", self.position_total_text)
        self._build_metric_card(overview_row, 1, "浮动盈亏(USDT)", self.position_upl_text)
        self._build_metric_card(overview_row, 2, "已实现盈亏", self.position_realized_text)
        self._build_metric_card(overview_row, 3, "初始保证金(IMR)", self.position_margin_text)
        self._build_metric_card(overview_row, 4, "Delta 合计", self.position_delta_text)
        self._build_metric_card(overview_row, 5, "买购数量", self.position_long_call_text)
        self._build_metric_card(overview_row, 6, "卖购数量", self.position_short_call_text)
        self._build_metric_card(overview_row, 7, "买沽数量", self.position_long_put_text)
        self._build_metric_card(overview_row, 8, "卖沽数量", self.position_short_put_text)

        position_table = ttk.Frame(positions_frame)
        position_table.grid(row=3, column=0, sticky="nsew")
        position_table.columnconfigure(0, weight=1)
        position_table.rowconfigure(0, weight=1)

        self.position_tree = ttk.Treeview(
            position_table,
            columns=(
                "inst_type",
                "mgn_mode",
                "time_value",
                "time_value_usdt",
                "intrinsic_value",
                "intrinsic_usdt",
                "bid_price",
                "bid_usdt",
                "ask_price",
                "ask_usdt",
                "mark",
                "mark_usdt",
                "avg",
                "avg_usdt",
                "pos",
                "option_side",
                "upl",
                "upl_usdt",
                "realized",
                "market_value",
                "liq",
                "mgn_ratio",
                "imr",
                "mmr",
                "delta",
                "gamma",
                "vega",
                "theta",
                "theta_usdt",
            ),
            show="tree headings",
            selectmode="browse",
        )
        self.position_tree.heading("#0", text="合约 / 分组")
        self.position_tree.heading("inst_type", text="类型")
        self.position_tree.heading("mgn_mode", text="保证金模式")
        self.position_tree.heading("time_value", text="时间价值")
        self.position_tree.heading("time_value_usdt", text="时间≈USDT")
        self.position_tree.heading("intrinsic_value", text="内在价值")
        self.position_tree.heading("intrinsic_usdt", text="内在≈USDT")
        self.position_tree.heading("bid_price", text="买一价")
        self.position_tree.heading("bid_usdt", text="买一≈USDT")
        self.position_tree.heading("ask_price", text="卖一价")
        self.position_tree.heading("ask_usdt", text="卖一≈USDT")
        self.position_tree.heading("mark", text="标记价")
        self.position_tree.heading("mark_usdt", text="标记≈USDT")
        self.position_tree.heading("avg", text="开仓价")
        self.position_tree.heading("avg_usdt", text="开仓≈USDT")
        self.position_tree.heading("pos", text="持仓量")
        self.position_tree.heading("option_side", text="买购:卖购 | 买沽:卖沽")
        self.position_tree.heading("upl", text="浮盈亏")
        self.position_tree.heading("upl_usdt", text="浮盈≈USDT")
        self.position_tree.heading("realized", text="已实现盈亏")
        self.position_tree.heading("market_value", text="市值")
        self.position_tree.heading("liq", text="强平价")
        self.position_tree.heading("mgn_ratio", text="保证金率")
        self.position_tree.heading("imr", text="初始保证金")
        self.position_tree.heading("mmr", text="维持保证金")
        self.position_tree.heading("delta", text="Delta(PA)")
        self.position_tree.heading("gamma", text="Gamma(PA)")
        self.position_tree.heading("vega", text="Vega(PA)")
        self.position_tree.heading("theta", text="Theta(PA)")
        self.position_tree.heading("theta_usdt", text="Theta≈USDT")
        self.position_tree.column("#0", width=240, anchor="w", stretch=True)
        self.position_tree.column("inst_type", width=72, anchor="center")
        self.position_tree.column("mgn_mode", width=92, anchor="center")
        self.position_tree.column("time_value", width=88, anchor="e")
        self.position_tree.column("time_value_usdt", width=44, anchor="e")
        self.position_tree.column("intrinsic_value", width=88, anchor="e")
        self.position_tree.column("intrinsic_usdt", width=44, anchor="e")
        self.position_tree.column("bid_price", width=78, anchor="e")
        self.position_tree.column("bid_usdt", width=50, anchor="e")
        self.position_tree.column("ask_price", width=78, anchor="e")
        self.position_tree.column("ask_usdt", width=50, anchor="e")
        self.position_tree.column("mark", width=108, anchor="e")
        self.position_tree.column("mark_usdt", width=54, anchor="e")
        self.position_tree.column("avg", width=108, anchor="e")
        self.position_tree.column("avg_usdt", width=54, anchor="e")
        self.position_tree.column("pos", width=110, anchor="e")
        self.position_tree.column("option_side", width=170, anchor="center")
        self.position_tree.column("upl", width=210, anchor="e")
        self.position_tree.column("upl_usdt", width=105, anchor="e")
        self.position_tree.column("realized", width=118, anchor="e")
        self.position_tree.column("market_value", width=160, anchor="e")
        self.position_tree.column("liq", width=92, anchor="e")
        self.position_tree.column("mgn_ratio", width=88, anchor="e")
        self.position_tree.column("imr", width=100, anchor="e")
        self.position_tree.column("mmr", width=100, anchor="e")
        self.position_tree.column("delta", width=82, anchor="e")
        self.position_tree.column("gamma", width=82, anchor="e")
        self.position_tree.column("vega", width=82, anchor="e")
        self.position_tree.column("theta", width=108, anchor="e")
        self.position_tree.column("theta_usdt", width=54, anchor="e")
        self.position_tree.configure(
            displaycolumns=(
                "inst_type",
                "mgn_mode",
                "mark",
                "mark_usdt",
                "avg",
                "avg_usdt",
                "pos",
                "option_side",
                "upl",
                "upl_usdt",
                "realized",
                "market_value",
                "liq",
                "mgn_ratio",
                "imr",
                "mmr",
                "delta",
                "gamma",
                "vega",
                "theta",
                "theta_usdt",
            )
        )
        self.position_tree.grid(row=0, column=0, sticky="nsew")
        self.position_tree.bind("<<TreeviewSelect>>", self._on_position_selected)
        self.position_tree.tag_configure("profit", foreground="#13803d")
        self.position_tree.tag_configure("loss", foreground="#c23b3b")
        self.position_tree.tag_configure("group", foreground="#2f3a4a")
        self.position_tree.tag_configure("isolated_mode", background="#fff4e5")
        self.position_tree.tag_configure("cross_mode", background="#f4f8ff")

        position_scroll_y = ttk.Scrollbar(position_table, orient="vertical", command=self.position_tree.yview)
        position_scroll_y.grid(row=0, column=1, sticky="ns")
        position_scroll_x = ttk.Scrollbar(position_table, orient="horizontal", command=self.position_tree.xview)
        position_scroll_x.grid(row=1, column=0, sticky="ew")
        self.position_tree.configure(yscrollcommand=position_scroll_y.set, xscrollcommand=position_scroll_x.set)

        self._main_positions_pane = None
        self._main_position_detail_frame = None

        log_frame = ttk.LabelFrame(self.root, text="运行日志", padding=12)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = Text(log_frame, height=18, wrap="word", font=("Consolas", 10))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def _apply_initial_pane_layout(self) -> None:
        try:
            if self._main_body_pane is not None and self._main_body_pane.winfo_exists():
                total_width = self._main_body_pane.winfo_width()
                if total_width > 1200:
                    self._main_body_pane.sashpos(0, int(total_width * Decimal("0.39")))
            if self._sessions_pane is not None and self._sessions_pane.winfo_exists():
                total_height = self._sessions_pane.winfo_height()
                if total_height > 600:
                    self._sessions_pane.sashpos(0, int(total_height * Decimal("0.15")))
        except Exception:
            return

    def _apply_initial_detail_visibility(self) -> None:
        return

    def toggle_main_position_detail(self) -> None:
        if self._main_positions_pane is None or self._main_position_detail_frame is None:
            return
        try:
            panes = tuple(str(pane) for pane in self._main_positions_pane.panes())
            frame_id = str(self._main_position_detail_frame)
            if self._main_position_detail_collapsed:
                if frame_id not in panes:
                    self._main_positions_pane.add(self._main_position_detail_frame, weight=2)
                self._main_position_detail_toggle_text.set("\u6298\u53e0\u6301\u4ed3\u8be6\u60c5")
            else:
                if frame_id in panes:
                    self._main_positions_pane.forget(self._main_position_detail_frame)
                self._main_position_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
            self._main_position_detail_collapsed = not self._main_position_detail_collapsed
        except Exception:
            return

    def _default_selected_session_text(self) -> str:
        return (
            "启动后，这里会显示选中策略的完整详情。\n"
            "左侧选择策略并点击“启动”后，右侧列表会出现会话；选中某个会话，就能在这里查看规则、参数和运行状态。"
        )

    def _default_position_detail_text(self) -> str:
        return (
            "这里会显示选中持仓或风险分组的详细信息。\n"
            "你可以先刷新持仓，再用上面的类型筛选、搜索、展开/折叠，按 OKX 客户端那种方式查看账户结构。"
        )

    def _default_account_info_detail_text(self) -> str:
        return (
            "这里会显示账户概览、账户配置和选中资产详情。\n"
            "点击“账户信息”后，程序会读取 OKX 账户余额与账户配置接口，方便你在持仓页里直接查看账户状态。"
        )

    def _set_readonly_text(self, widget: Text | None, content: str) -> None:
        if widget is None:
            return
        widget.configure(state="normal")
        widget.delete("1.0", END)
        widget.insert("1.0", content)
        widget.configure(state="disabled")

    def _build_metric_card(self, parent: ttk.Frame, column: int, title: str, value_var: StringVar) -> None:
        card = ttk.LabelFrame(parent, text=title, padding=(10, 8))
        card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 0))
        card.columnconfigure(0, weight=1)
        ttk.Label(card, textvariable=value_var, font=("Microsoft YaHei UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )

    def open_account_info_window(self) -> None:
        if self._account_info_window is not None and self._account_info_window.winfo_exists():
            self._account_info_window.focus_force()
            self.refresh_account_info()
            return

        window = Toplevel(self.root)
        window.title("账户信息")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.78,
            height_ratio=0.76,
            min_width=1120,
            min_height=760,
            max_width=1600,
            max_height=1080,
        )
        self._account_info_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_account_info_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self.account_info_summary_text, justify="left").grid(row=0, column=0, sticky="w")
        action_row = ttk.Frame(header)
        action_row.grid(row=0, column=1, sticky="e")
        ttk.Button(action_row, text="刷新", command=self.refresh_account_info).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(action_row, text="关闭", command=self._close_account_info_window).grid(row=0, column=1)

        overview_row = ttk.Frame(container)
        overview_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        for column in range(6):
            overview_row.columnconfigure(column, weight=1)
        self._build_metric_card(overview_row, 0, "总权益", self.account_total_equity_text)
        self._build_metric_card(overview_row, 1, "调整后权益", self.account_adjusted_equity_text)
        self._build_metric_card(overview_row, 2, "可用权益", self.account_available_equity_text)
        self._build_metric_card(overview_row, 3, "未实现盈亏", self.account_upl_text)
        self._build_metric_card(overview_row, 4, "初始保证金(IMR)", self.account_imr_text)
        self._build_metric_card(overview_row, 5, "维持保证金(MMR)", self.account_mmr_text)

        config_frame = ttk.LabelFrame(container, text="账户配置", padding=10)
        config_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 10))
        config_frame.columnconfigure(0, weight=1)
        config_frame.rowconfigure(0, weight=1)
        self._account_info_config_panel = Text(
            config_frame,
            height=5,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_config_panel.grid(row=0, column=0, sticky="nsew")
        config_scroll = ttk.Scrollbar(config_frame, orient="vertical", command=self._account_info_config_panel.yview)
        config_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_config_panel.configure(yscrollcommand=config_scroll.set)
        self._set_readonly_text(self._account_info_config_panel, self._default_account_info_detail_text())

        asset_frame = ttk.LabelFrame(container, text="资产明细", padding=10)
        asset_frame.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
        asset_frame.columnconfigure(0, weight=1)
        asset_frame.rowconfigure(0, weight=1)

        self._account_info_tree = ttk.Treeview(
            asset_frame,
            columns=("ccy", "eq", "eq_usd", "cash", "avail_bal", "avail_eq", "upl", "frozen", "liab"),
            show="headings",
            selectmode="browse",
        )
        tree = self._account_info_tree
        tree.heading("ccy", text="币种")
        tree.heading("eq", text="权益")
        tree.heading("eq_usd", text="折合USD")
        tree.heading("cash", text="现金余额")
        tree.heading("avail_bal", text="可用余额")
        tree.heading("avail_eq", text="可用权益")
        tree.heading("upl", text="未实现盈亏")
        tree.heading("frozen", text="冻结")
        tree.heading("liab", text="负债")
        tree.column("ccy", width=90, anchor="center")
        tree.column("eq", width=110, anchor="e")
        tree.column("eq_usd", width=110, anchor="e")
        tree.column("cash", width=110, anchor="e")
        tree.column("avail_bal", width=110, anchor="e")
        tree.column("avail_eq", width=110, anchor="e")
        tree.column("upl", width=110, anchor="e")
        tree.column("frozen", width=100, anchor="e")
        tree.column("liab", width=100, anchor="e")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._refresh_account_info_detail)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        asset_scroll_y = ttk.Scrollbar(asset_frame, orient="vertical", command=tree.yview)
        asset_scroll_y.grid(row=0, column=1, sticky="ns")
        asset_scroll_x = ttk.Scrollbar(asset_frame, orient="horizontal", command=tree.xview)
        asset_scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=asset_scroll_y.set, xscrollcommand=asset_scroll_x.set)

        detail_frame = ttk.LabelFrame(container, text="选中资产详情", padding=10)
        detail_frame.grid(row=4, column=0, sticky="nsew")
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._account_info_detail_panel = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._account_info_detail_panel.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._account_info_detail_panel.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._account_info_detail_panel.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())

        self._expand_to_screen(window, margin=30)
        self.refresh_account_info()

    def _close_account_info_window(self) -> None:
        if self._account_info_window is not None and self._account_info_window.winfo_exists():
            self._account_info_window.destroy()
        self._account_info_window = None
        self._account_info_tree = None
        self._account_info_detail_panel = None
        self._account_info_config_panel = None

    def refresh_account_info(self) -> None:
        if self._account_info_refreshing:
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self.account_info_summary_text.set("未配置 API 凭证，无法读取账户信息。")
            self._set_readonly_text(self._account_info_config_panel, "未配置 API 凭证，无法读取账户配置。")
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            if self._account_info_tree is not None:
                self._account_info_tree.delete(*self._account_info_tree.get_children())
            return
        self._account_info_refreshing = True
        self.account_info_summary_text.set("正在刷新账户信息...")
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        threading.Thread(
            target=self._refresh_account_info_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_account_info_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            overview = self.client.get_account_overview(credentials, environment=environment)
            config = self.client.get_account_config(credentials, environment=environment)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    overview = self.client.get_account_overview(credentials, environment=alternate)
                    config = self.client.get_account_config(credentials, environment=alternate)
                except Exception:
                    self.root.after(0, lambda: self._apply_account_info_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取账户信息。"
                )
                self.root.after(0, lambda: self._apply_account_info(overview, config, summary, alternate))
                return
            self.root.after(0, lambda: self._apply_account_info_error(message))
            return
        self.root.after(0, lambda: self._apply_account_info(overview, config, None, environment))

    def _apply_account_info(
        self,
        overview: OkxAccountOverview,
        config: OkxAccountConfig,
        summary_note: str | None,
        effective_environment: str,
    ) -> None:
        self._account_info_refreshing = False
        self._latest_account_overview = overview
        self._latest_account_config = config
        environment_label = "实盘 live" if effective_environment == "live" else "模拟盘 demo"
        summary_parts = []
        if summary_note:
            summary_parts.append(summary_note)
        summary_parts.append(f"API配置：{self._current_credential_profile()}")
        summary_parts.append(f"环境：{environment_label}")
        summary_parts.append(f"账户模式：{_format_account_level(config.account_level)}")
        summary_parts.append(f"持仓模式：{_format_account_position_mode(config.position_mode)}")
        summary_parts.append(f"Greeks：{_format_greeks_type(config.greeks_type)}")
        self.account_info_summary_text.set(" | ".join(summary_parts))
        self.account_total_equity_text.set(_format_optional_usdt_precise(overview.total_equity, places=2, with_sign=False))
        self.account_adjusted_equity_text.set(_format_optional_usdt_precise(overview.adjusted_equity, places=2, with_sign=False))
        self.account_available_equity_text.set(_format_optional_usdt_precise(overview.available_equity, places=2, with_sign=False))
        self.account_upl_text.set(_format_optional_usdt_precise(overview.unrealized_pnl, places=2))
        self.account_imr_text.set(_format_optional_usdt_precise(overview.initial_margin, places=2, with_sign=False))
        self.account_mmr_text.set(_format_optional_usdt_precise(overview.maintenance_margin, places=2, with_sign=False))
        self._set_readonly_text(
            self._account_info_config_panel,
            _build_account_config_detail_text(
                config,
                overview,
                profile_name=self._current_credential_profile(),
                environment=effective_environment,
            ),
        )
        if self._account_info_tree is not None:
            selected_before = self._account_info_tree.selection()[0] if self._account_info_tree.selection() else None
            self._account_info_tree.delete(*self._account_info_tree.get_children())
            for index, asset in enumerate(overview.details):
                tags: tuple[str, ...] = ()
                if asset.unrealized_pnl is not None:
                    tags = (_pnl_tag(asset.unrealized_pnl),)
                self._account_info_tree.insert(
                    "",
                    END,
                    iid=f"acct-{index}",
                    values=(
                        asset.ccy or "-",
                        _format_optional_decimal(asset.equity),
                        _format_optional_usdt_precise(asset.equity_usd, places=2, with_sign=False),
                        _format_optional_decimal(asset.cash_balance),
                        _format_optional_decimal(asset.available_balance),
                        _format_optional_decimal(asset.available_equity),
                        _format_optional_decimal(asset.unrealized_pnl, with_sign=True),
                        _format_optional_decimal(asset.frozen_balance),
                        _format_optional_decimal(asset.liability),
                    ),
                    tags=tags,
                )
            if selected_before and self._account_info_tree.exists(selected_before):
                self._account_info_tree.selection_set(selected_before)
            elif overview.details:
                self._account_info_tree.selection_set("acct-0")
            self._refresh_account_info_detail()

    def _apply_account_info_error(self, message: str) -> None:
        self._account_info_refreshing = False
        self.account_info_summary_text.set(f"账户信息读取失败：{message}")
        self._set_readonly_text(self._account_info_config_panel, f"账户配置读取失败：{message}")
        self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())

    def _refresh_account_info_detail(self, *_: object) -> None:
        tree = self._account_info_tree
        overview = self._latest_account_overview
        if tree is None or overview is None:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        selection = tree.selection()
        if not selection:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        try:
            index = int(selection[0].split("-", 1)[1])
        except Exception:
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        if index < 0 or index >= len(overview.details):
            self._set_readonly_text(self._account_info_detail_panel, self._default_account_info_detail_text())
            return
        self._set_readonly_text(self._account_info_detail_panel, _build_account_asset_detail_text(overview.details[index]))

    def _schedule_protection_window_refresh(self) -> None:
        try:
            if self.root.winfo_exists():
                self.root.after(0, self._refresh_protection_window_view)
        except Exception:
            return

    def _position_refresh_interval_ms(self) -> int:
        return POSITION_REFRESH_INTERVAL_OPTIONS.get(self.position_refresh_interval_label.get(), 15_000)

    def toggle_position_auto_refresh(self) -> None:
        self.position_auto_refresh_enabled = not self.position_auto_refresh_enabled
        if self.position_auto_refresh_enabled:
            self.position_auto_refresh_button_text.set("暂停自动刷新")
            self._enqueue_log(f"账户持仓已恢复自动刷新，当前间隔：{self.position_refresh_interval_label.get()}")
            self.refresh_positions()
        else:
            self.position_auto_refresh_button_text.set("恢复自动刷新")
            self._enqueue_log("账户持仓已暂停自动刷新。需要更新时可以手动点“刷新”。")
            self._update_position_summary(_filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
            ))

    def _on_position_refresh_interval_changed(self, *_: object) -> None:
        visible_positions = _filter_positions(
            self._latest_positions,
            inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
            keyword=self.position_keyword.get(),
        )
        self._update_position_summary(visible_positions)
        self._enqueue_log(f"账户持仓自动刷新间隔已切换为：{self.position_refresh_interval_label.get()}")

    def _on_position_filter_changed(self, *_: object) -> None:
        self._render_positions_view()

    def reset_position_filters(self) -> None:
        self.position_type_filter.set("全部类型")
        self.position_keyword.set("")
        self._render_positions_view()

    def expand_all_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            self.position_tree.item(item_id, open=True)
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=True)

    def collapse_position_groups(self) -> None:
        for item_id in self.position_tree.get_children():
            for child_id in self.position_tree.get_children(item_id):
                self.position_tree.item(child_id, open=False)
            self.position_tree.item(item_id, open=False)

    def copy_selected_position_symbol(self) -> None:
        payload = self._selected_position_payload()
        if payload is None or payload["kind"] != "position":
            messagebox.showinfo("提示", "请先在持仓列表中选中一条具体持仓。")
            return
        position = payload["item"]
        if not isinstance(position, OkxPosition):
            return
        self.root.clipboard_clear()
        self.root.clipboard_append(position.inst_id)
        self._enqueue_log(f"已复制合约代码：{position.inst_id}")

    def _expand_to_screen(self, window: Toplevel, *, margin: int = 20) -> None:
        try:
            apply_fill_window_geometry(window, min_width=1200, min_height=800, margin=margin)
        except Exception:
            return

    def _schedule_positions_zoom_sync(self, delay_ms: int = 10) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
            self._positions_zoom_sync_job = None
        self._positions_zoom_sync_job = self.root.after(delay_ms, self._sync_positions_zoom_window)

    def open_positions_zoom_window(self) -> None:
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.focus_force()
            self._schedule_positions_zoom_sync()
            self.refresh_position_histories()
            return

        window = Toplevel(self.root)
        window.title("账户持仓大窗")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.84,
            height_ratio=0.82,
            min_width=1280,
            min_height=860,
            max_width=1800,
            max_height=1120,
        )
        self._positions_zoom_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=3)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=2)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_summary_text).grid(row=0, column=0, sticky="w")
        zoom_actions = ttk.Frame(header)
        zoom_actions.grid(row=0, column=1, sticky="e")
        ttk.Button(zoom_actions, text="刷新", command=self.refresh_positions).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(zoom_actions, text="刷新历史", command=self.refresh_position_histories).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(zoom_actions, text="刷新历史成交", command=self.refresh_fill_history).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(zoom_actions, text="账户信息", command=self.open_account_info_window).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(zoom_actions, text="设置期权保护", command=self.open_position_protection_window).grid(
            row=0, column=4, padx=(0, 6)
        )
        ttk.Button(zoom_actions, text="关闭", command=self._close_positions_zoom_window).grid(row=0, column=5)
        ttk.Button(zoom_actions, text="列设置", command=self.open_positions_zoom_column_window).grid(
            row=0, column=6, padx=(0, 6)
        )

        ttk.Button(zoom_actions, textvariable=self._positions_zoom_detail_toggle_text, command=self.toggle_positions_zoom_detail).grid(
            row=0, column=7, padx=(0, 6)
        )
        ttk.Button(zoom_actions, textvariable=self._positions_zoom_history_toggle_text, command=self.toggle_positions_zoom_history).grid(
            row=0, column=8
        )
        for column_index, child in enumerate(zoom_actions.winfo_children()):
            child.grid_configure(column=column_index)

        filter_row = ttk.Frame(container)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        filter_row.columnconfigure(3, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        zoom_position_type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        zoom_position_type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        zoom_position_type_combo.bind("<<ComboboxSelected>>", self._on_position_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=2, sticky="w")
        zoom_position_keyword_entry = ttk.Entry(filter_row, textvariable=self.position_keyword)
        zoom_position_keyword_entry.grid(row=0, column=3, sticky="ew", padx=(6, 12))
        zoom_position_keyword_entry.bind("<KeyRelease>", self._on_position_filter_changed)
        self._positions_zoom_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_search,
        )
        self._positions_zoom_apply_contract_button.grid(row=0, column=4, padx=(0, 6))
        self._positions_zoom_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_search,
        )
        self._positions_zoom_apply_expiry_prefix_button.grid(row=0, column=5, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_view).grid(
            row=0, column=6, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_filters).grid(row=0, column=7)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_option_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=2, columnspan=6, sticky="w", pady=(6, 0))

        tree_frame = ttk.Frame(container)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = tuple(self.position_tree["columns"])
        zoom_tree = ttk.Treeview(tree_frame, columns=columns, show="tree headings", selectmode="browse")
        self._positions_zoom_tree = zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.grid(row=0, column=0, sticky="nsew")
        zoom_tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_selected)
        zoom_tree.tag_configure("profit", foreground="#13803d")
        zoom_tree.tag_configure("loss", foreground="#c23b3b")
        zoom_tree.tag_configure("group", foreground="#2f3a4a")
        zoom_tree.tag_configure("isolated_mode", background="#fff4e5")
        zoom_tree.tag_configure("cross_mode", background="#f4f8ff")
        zoom_scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=zoom_tree.yview)
        zoom_scroll_y.grid(row=0, column=1, sticky="ns")
        zoom_scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=zoom_tree.xview)
        zoom_scroll_x.grid(row=1, column=0, sticky="ew")
        zoom_tree.configure(yscrollcommand=zoom_scroll_y.set, xscrollcommand=zoom_scroll_x.set)
        self._register_positions_zoom_columns("positions", "当前持仓", zoom_tree, columns)

        detail_frame = ttk.LabelFrame(container, text="大窗持仓详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew")
        self._positions_zoom_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_detail = Text(
            detail_frame,
            height=10,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
        self._positions_zoom_summary_text.set("正在打开持仓大窗...")
        self._set_readonly_text(
            self._positions_zoom_detail,
            "大窗已经创建，正在同步当前持仓视图。若你的持仓较多，会在一瞬间完成填充。",
        )
        history_notebook = ttk.Notebook(container)
        history_notebook.grid(row=4, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_notebook = history_notebook

        fills_tab = ttk.Frame(history_notebook, padding=10)
        fills_tab.columnconfigure(0, weight=1)
        fills_tab.rowconfigure(1, weight=1)
        fills_tab.rowconfigure(2, weight=1)
        history_notebook.add(fills_tab, text="历史成交")
        self._build_positions_zoom_fills_tab(fills_tab)

        position_history_tab = ttk.Frame(history_notebook, padding=10)
        position_history_tab.columnconfigure(0, weight=1)
        position_history_tab.rowconfigure(2, weight=1)
        position_history_tab.rowconfigure(3, weight=1)
        history_notebook.add(position_history_tab, text="历史仓位")
        self._build_positions_zoom_position_history_tab(position_history_tab)
        if not self._positions_zoom_detail_collapsed:
            self.toggle_positions_zoom_detail()
        if not self._positions_zoom_fills_detail_collapsed:
            self.toggle_positions_zoom_fills_detail()
        if not self._positions_zoom_position_history_detail_collapsed:
            self.toggle_positions_zoom_position_history_detail()
        self.refresh_position_histories()
        self._expand_to_screen(window)
        self._update_positions_zoom_search_shortcuts()
        self._update_position_history_search_shortcuts()
        self._schedule_positions_zoom_sync(30)

    def _build_positions_zoom_fills_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_fills_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_fills_load_more_text,
            command=self.expand_fill_history_limit,
        ).grid(row=0, column=1, sticky="e", padx=(0, 6))
        ttk.Button(header, textvariable=self._positions_zoom_fills_detail_toggle_text, command=self.toggle_positions_zoom_fills_detail).grid(
            row=0, column=2, sticky="e"
        )
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(5, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="方向").grid(row=0, column=2, sticky="w")
        side_combo = ttk.Combobox(
            filter_row,
            textvariable=self.fill_history_side_filter,
            values=list(HISTORY_FILL_SIDE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        side_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        side_combo.bind("<<ComboboxSelected>>", self._on_fill_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=4, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.fill_history_keyword)
        keyword_entry.grid(row=0, column=5, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_fill_history_filter_changed)
        self._positions_zoom_fills_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_contract_button.grid(row=0, column=6, padx=(0, 6))
        self._positions_zoom_fills_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_fill_history_search,
        )
        self._positions_zoom_fills_apply_expiry_prefix_button.grid(row=0, column=7, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_fills_view).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_fill_history_filters).grid(row=0, column=9)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_fill_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=4, columnspan=6, sticky="w", pady=(6, 0))
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("time", "inst_type", "inst_id", "side", "price", "size", "fee", "pnl", "exec_type")
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_fills_tree = tree
        headings = {
            "time": "时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "side": "方向",
            "price": "成交价",
            "size": "成交量",
            "fee": "手续费",
            "pnl": "已实现盈亏",
            "exec_type": "成交类型",
            "realized_usdt": "鎶樺悎USDT",
        }
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("side", 96),
            ("price", 100),
            ("size", 100),
            ("fee", 100),
            ("pnl", 110),
            ("exec_type", 96),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(column_id, width=width, anchor="e" if column_id in {"price", "size", "fee", "pnl"} else "center")
        tree.column("inst_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_fills_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("fills", "历史成交", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="成交详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_fills_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_fills_detail = Text(detail_frame, height=8, wrap="word", font=("Microsoft YaHei UI", 10), relief="flat")
        self._positions_zoom_fills_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_fills_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_fills_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")

    def _build_positions_zoom_position_history_tab(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(header, textvariable=self._positions_zoom_position_history_summary_text).grid(row=0, column=0, sticky="w")
        ttk.Button(
            header,
            textvariable=self._positions_zoom_position_history_detail_toggle_text,
            command=self.toggle_positions_zoom_position_history_detail,
        ).grid(row=0, column=1, sticky="e")
        filter_row = ttk.Frame(parent)
        filter_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(5, weight=1)
        ttk.Label(filter_row, text="类型").grid(row=0, column=0, sticky="w")
        type_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_type_filter,
            values=list(POSITION_TYPE_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        type_combo.grid(row=0, column=1, sticky="w", padx=(6, 12))
        type_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="保证金模式").grid(row=0, column=2, sticky="w")
        margin_combo = ttk.Combobox(
            filter_row,
            textvariable=self.position_history_margin_filter,
            values=list(HISTORY_MARGIN_MODE_FILTER_OPTIONS.keys()),
            state="readonly",
            width=16,
        )
        margin_combo.grid(row=0, column=3, sticky="w", padx=(6, 12))
        margin_combo.bind("<<ComboboxSelected>>", self._on_position_history_filter_changed)
        ttk.Label(filter_row, text="搜索").grid(row=0, column=4, sticky="w")
        keyword_entry = ttk.Entry(filter_row, textvariable=self.position_history_keyword)
        keyword_entry.grid(row=0, column=5, sticky="ew", padx=(6, 12))
        keyword_entry.bind("<KeyRelease>", self._on_position_history_filter_changed)
        self._positions_zoom_position_history_apply_contract_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5408\u7ea6",
            command=self.apply_selected_option_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_contract_button.grid(row=0, column=6, padx=(0, 6))
        self._positions_zoom_position_history_apply_expiry_prefix_button = ttk.Button(
            filter_row,
            text="\u5e26\u5165\u5230\u671f\u524d\u7f00",
            command=self.apply_selected_option_expiry_prefix_to_position_history_search,
        )
        self._positions_zoom_position_history_apply_expiry_prefix_button.grid(row=0, column=7, padx=(0, 6))
        ttk.Button(filter_row, text="应用筛选", command=self._render_positions_zoom_position_history_view).grid(
            row=0, column=8, padx=(0, 6)
        )
        ttk.Button(filter_row, text="清空筛选", command=self.reset_position_history_filters).grid(row=0, column=9)
        ttk.Label(
            filter_row,
            textvariable=self._positions_zoom_position_history_search_hint_text,
            foreground="#6b7280",
        ).grid(row=1, column=4, columnspan=6, sticky="w", pady=(6, 0))
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=2, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = (
            "time",
            "inst_type",
            "inst_id",
            "mgn_mode",
            "side",
            "open_avg",
            "close_avg",
            "close_size",
            "pnl",
            "realized",
            "realized_usdt",
        )
        tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self._positions_zoom_position_history_tree = tree
        headings = {
            "time": "更新时间",
            "inst_type": "类型",
            "inst_id": "合约",
            "mgn_mode": "保证金模式",
            "side": "方向",
            "open_avg": "开仓均价",
            "close_avg": "平仓均价",
            "close_size": "平仓数量",
            "pnl": "盈亏",
            "realized": "已实现盈亏",
        }
        headings["realized_usdt"] = "折合USDT"
        for column_id, width in (
            ("time", 150),
            ("inst_type", 72),
            ("inst_id", 240),
            ("mgn_mode", 96),
            ("side", 96),
            ("open_avg", 100),
            ("close_avg", 100),
            ("close_size", 100),
            ("pnl", 100),
            ("realized", 110),
            ("realized_usdt", 110),
        ):
            tree.heading(column_id, text=headings[column_id])
            tree.column(
                column_id,
                width=width,
                anchor="e" if column_id in {"open_avg", "close_avg", "close_size", "pnl", "realized", "realized_usdt"} else "center",
            )
        tree.column("inst_id", anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")
        tree.bind("<<TreeviewSelect>>", self._on_positions_zoom_position_history_selected)
        tree.tag_configure("profit", foreground="#13803d")
        tree.tag_configure("loss", foreground="#c23b3b")
        scroll_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        scroll_x.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self._register_positions_zoom_columns("position_history", "历史仓位", tree, columns)

        detail_frame = ttk.LabelFrame(parent, text="历史仓位详情", padding=12)
        detail_frame.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        self._positions_zoom_position_history_detail_frame = detail_frame
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._positions_zoom_position_history_detail = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._positions_zoom_position_history_detail.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._positions_zoom_position_history_detail.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._positions_zoom_position_history_detail.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")

    def _close_positions_zoom_window(self) -> None:
        if self._positions_zoom_sync_job is not None:
            try:
                self.root.after_cancel(self._positions_zoom_sync_job)
            except Exception:
                pass
        self._positions_zoom_sync_job = None
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            self._positions_zoom_window.destroy()
        self._positions_zoom_window = None
        self._positions_zoom_tree = None
        self._positions_zoom_detail = None
        self._positions_zoom_notebook = None
        self._positions_zoom_fills_tree = None
        self._positions_zoom_fills_detail = None
        self._positions_zoom_position_history_tree = None
        self._positions_zoom_position_history_detail = None
        self._positions_zoom_column_groups = {}
        self._positions_zoom_column_vars = {}
        self._position_history_usdt_prices = {}
        self._positions_zoom_detail_frame = None
        self._positions_zoom_fills_detail_frame = None
        self._positions_zoom_position_history_detail_frame = None
        self._positions_zoom_selected_item_id = None
        self._positions_zoom_apply_contract_button = None
        self._positions_zoom_apply_expiry_prefix_button = None
        self._positions_zoom_fills_apply_contract_button = None
        self._positions_zoom_fills_apply_expiry_prefix_button = None
        self._positions_zoom_position_history_apply_contract_button = None
        self._positions_zoom_position_history_apply_expiry_prefix_button = None
        self._positions_zoom_detail_collapsed = False
        self._positions_zoom_history_collapsed = False
        self._positions_zoom_fills_detail_collapsed = False
        self._positions_zoom_position_history_detail_collapsed = False
        self._positions_zoom_detail_toggle_text.set("\u5c55\u5f00\u6301\u4ed3\u8be6\u60c5")
        self._positions_zoom_fills_detail_toggle_text.set("\u5c55\u5f00\u6210\u4ea4\u8be6\u60c5")
        self._positions_zoom_position_history_detail_toggle_text.set("\u5c55\u5f00\u4ed3\u4f4d\u8be6\u60c5")
        self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        self._positions_zoom_history_toggle_text.set("折叠历史区域")
        self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
        self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")

    def _register_positions_zoom_columns(
        self,
        group_key: str,
        title: str,
        tree: ttk.Treeview,
        columns: tuple[str, ...],
    ) -> None:
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            tree.configure(
                displaycolumns=tuple(column_id for column_id in columns if column_id in default_visible_columns)
            )
        self._positions_zoom_column_groups[group_key] = {
            "title": title,
            "tree": tree,
            "columns": tuple(columns),
            "headings": {column_id: tree.heading(column_id).get("text", column_id) for column_id in columns},
        }

    def open_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_window is None or not self._positions_zoom_window.winfo_exists():
            return
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.focus_force()
            return

        window = Toplevel(self._positions_zoom_window)
        window.title("持仓大窗列设置")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.34,
            height_ratio=0.48,
            min_width=480,
            min_height=420,
            max_width=700,
            max_height=760,
        )
        window.transient(self._positions_zoom_window)
        self._positions_zoom_column_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_positions_zoom_column_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(
            container,
            text="可按区域勾选显示/隐藏列。'合约/分组' 为结构列，当前固定显示。",
        ).grid(row=0, column=0, sticky="w", pady=(0, 10))

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew")

        self._positions_zoom_column_vars = {}
        group_order = ("positions", "fills", "position_history")
        for group_key in group_order:
            group = self._positions_zoom_column_groups.get(group_key)
            if not group:
                continue
            title = str(group["title"])
            columns = tuple(group["columns"])
            headings = dict(group["headings"])
            tree = group["tree"]
            if not isinstance(tree, ttk.Treeview):
                continue
            visible_columns = set(_tree_display_columns(tree, columns))
            tab = ttk.Frame(notebook, padding=12)
            tab.columnconfigure(0, weight=1)
            notebook.add(tab, text=title)

            actions = ttk.Frame(tab)
            actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
            actions.columnconfigure(0, weight=1)
            ttk.Label(actions, text=f"{title} 列").grid(row=0, column=0, sticky="w")
            ttk.Button(
                actions,
                text="全部显示",
                command=lambda key=group_key: self._set_positions_zoom_columns_visible(key, True),
            ).grid(row=0, column=1, padx=(0, 6))
            ttk.Button(
                actions,
                text="恢复默认",
                command=lambda key=group_key: self._reset_positions_zoom_columns(key),
            ).grid(row=0, column=2)

            checks = ttk.Frame(tab)
            checks.grid(row=1, column=0, sticky="nsew")
            for column_index in range(2):
                checks.columnconfigure(column_index, weight=1)

            group_vars: dict[str, BooleanVar] = {}
            for index, column_id in enumerate(columns):
                var = BooleanVar(value=column_id in visible_columns)
                group_vars[column_id] = var
                ttk.Checkbutton(
                    checks,
                    text=headings.get(column_id, column_id),
                    variable=var,
                    command=lambda key=group_key: self._apply_positions_zoom_column_visibility(key),
                ).grid(row=index // 2, column=index % 2, sticky="w", padx=(0, 12), pady=4)
            self._positions_zoom_column_vars[group_key] = group_vars

    def _close_positions_zoom_column_window(self) -> None:
        if self._positions_zoom_column_window is not None and self._positions_zoom_column_window.winfo_exists():
            self._positions_zoom_column_window.destroy()
        self._positions_zoom_column_window = None

    def _apply_positions_zoom_column_visibility(self, group_key: str) -> None:
        group = self._positions_zoom_column_groups.get(group_key)
        variables = self._positions_zoom_column_vars.get(group_key)
        if not group or not variables:
            return
        tree = group["tree"]
        columns = tuple(group["columns"])
        if not isinstance(tree, ttk.Treeview):
            return
        visible_columns = [column_id for column_id in columns if variables[column_id].get()]
        if not visible_columns:
            fallback = columns[0]
            variables[fallback].set(True)
            visible_columns = [fallback]
        tree.configure(displaycolumns=tuple(visible_columns))

    def _set_positions_zoom_columns_visible(self, group_key: str, visible: bool) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        if not columns:
            return
        for column_id in columns:
            variables[column_id].set(visible)
        if not visible:
            variables[columns[0]].set(True)
        self._apply_positions_zoom_column_visibility(group_key)

    def _reset_positions_zoom_columns(self, group_key: str) -> None:
        variables = self._positions_zoom_column_vars.get(group_key)
        group = self._positions_zoom_column_groups.get(group_key)
        if not variables or not group:
            return
        columns = tuple(group["columns"])
        default_visible_columns = POSITIONS_ZOOM_DEFAULT_VISIBLE_COLUMNS.get(group_key)
        if default_visible_columns:
            default_set = set(default_visible_columns)
            for column_id in columns:
                variables[column_id].set(column_id in default_set)
            self._apply_positions_zoom_column_visibility(group_key)
            return
        self._set_positions_zoom_columns_visible(group_key, True)

    def toggle_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail_frame is None:
            return
        if self._positions_zoom_detail_collapsed:
            self._positions_zoom_detail_frame.grid()
            self._positions_zoom_detail_toggle_text.set("折叠持仓详情")
        else:
            self._positions_zoom_detail_frame.grid_remove()
            self._positions_zoom_detail_toggle_text.set("展开持仓详情")
        self._positions_zoom_detail_collapsed = not self._positions_zoom_detail_collapsed

    def toggle_positions_zoom_history(self) -> None:
        if self._positions_zoom_notebook is None:
            return
        if self._positions_zoom_history_collapsed:
            self._positions_zoom_notebook.grid()
            self._positions_zoom_history_toggle_text.set("折叠历史区域")
        else:
            self._positions_zoom_notebook.grid_remove()
            self._positions_zoom_history_toggle_text.set("展开历史区域")
        self._positions_zoom_history_collapsed = not self._positions_zoom_history_collapsed

    def toggle_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_detail_frame is None:
            return
        if self._positions_zoom_fills_detail_collapsed:
            self._positions_zoom_fills_detail_frame.grid()
            self._positions_zoom_fills_detail_toggle_text.set("折叠成交详情")
        else:
            self._positions_zoom_fills_detail_frame.grid_remove()
            self._positions_zoom_fills_detail_toggle_text.set("展开成交详情")
        self._positions_zoom_fills_detail_collapsed = not self._positions_zoom_fills_detail_collapsed

    def toggle_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_detail_frame is None:
            return
        if self._positions_zoom_position_history_detail_collapsed:
            self._positions_zoom_position_history_detail_frame.grid()
            self._positions_zoom_position_history_detail_toggle_text.set("折叠仓位详情")
        else:
            self._positions_zoom_position_history_detail_frame.grid_remove()
            self._positions_zoom_position_history_detail_toggle_text.set("展开仓位详情")
        self._positions_zoom_position_history_detail_collapsed = not self._positions_zoom_position_history_detail_collapsed

    def _sync_positions_zoom_window(self) -> None:
        self._positions_zoom_sync_job = None
        if (
            self._positions_zoom_window is None
            or not self._positions_zoom_window.winfo_exists()
            or self._positions_zoom_tree is None
        ):
            return

        self._positions_zoom_summary_text.set(self.positions_summary_text.get())
        zoom_tree = self._positions_zoom_tree
        self._sync_positions_zoom_columns_from_main()
        zoom_tree.delete(*zoom_tree.get_children())

        def _copy_branch(source_parent: str, target_parent: str) -> None:
            for item_id in self.position_tree.get_children(source_parent):
                item = self.position_tree.item(item_id)
                zoom_tree.insert(
                    target_parent,
                    END,
                    iid=item_id,
                    text=item.get("text", ""),
                    values=item.get("values", ()),
                    open=bool(item.get("open")),
                    tags=item.get("tags", ()),
                )
                _copy_branch(item_id, item_id)

        _copy_branch("", "")
        selected = self.position_tree.selection()
        if selected and zoom_tree.exists(selected[0]):
            self._position_selection_syncing = True
            try:
                zoom_tree.selection_set(selected[0])
                zoom_tree.focus(selected[0])
                self._positions_zoom_selected_item_id = selected[0]
            finally:
                self._position_selection_syncing = False
        self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()

    def _sync_positions_zoom_columns_from_main(self) -> None:
        if self.position_tree is None or self._positions_zoom_tree is None:
            return
        zoom_tree = self._positions_zoom_tree
        columns = tuple(self.position_tree["columns"])
        approx_heading_columns = {
            "bid_usdt",
            "ask_usdt",
            "mark_usdt",
            "avg_usdt",
            "upl_usdt",
            "theta_usdt",
        }
        compact_zoom_columns = {
            "time_value": 88,
            "time_value_usdt": 72,
            "intrinsic_value": 88,
            "intrinsic_usdt": 72,
            "bid_price": 72,
            "bid_usdt": 78,
            "ask_price": 72,
            "ask_usdt": 78,
        }
        heading_font_name = ttk.Style().lookup("Treeview.Heading", "font") or "TkDefaultFont"
        try:
            heading_font = tkfont.nametofont(heading_font_name)
        except Exception:
            heading_font = tkfont.nametofont("TkDefaultFont")
        for column_id in ("#0", *columns):
            heading = self.position_tree.heading(column_id)
            column = self.position_tree.column(column_id)
            width = column.get("width")
            stretch = column.get("stretch")
            if column_id in compact_zoom_columns:
                width = compact_zoom_columns[column_id]
                stretch = False
            elif column_id in approx_heading_columns:
                heading_text = str(heading.get("text", ""))
                width = max(heading_font.measure(heading_text) + 20, 84)
                stretch = False
            zoom_tree.heading(column_id, text=heading.get("text", ""))
            zoom_tree.column(
                column_id,
                width=width,
                anchor=column.get("anchor"),
                stretch=stretch,
            )

    def _on_positions_zoom_selected(self, *_: object) -> None:
        if self._positions_zoom_tree is None or self._positions_view_rendering:
            return
        selection = self._positions_zoom_tree.selection()
        if not selection:
            self._positions_zoom_selected_item_id = None
            self._refresh_positions_zoom_detail()
            self._update_positions_zoom_search_shortcuts()
            return
        self._positions_zoom_selected_item_id = selection[0]
        self._refresh_positions_zoom_detail()
        self._update_positions_zoom_search_shortcuts()

    def _selected_positions_zoom_option_for_search(self) -> OkxPosition | None:
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is None or payload.get("kind") != "position":
            return None
        position = payload.get("item")
        if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
            return position
        return None

    def _selected_position_history_option_for_search(self) -> OkxPositionHistoryItem | None:
        if self._positions_zoom_position_history_tree is None:
            return None
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            return None
        item = self._latest_position_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _set_optional_button_enabled(self, button: ttk.Button | None, enabled: bool) -> None:
        if button is None:
            return
        if enabled:
            button.state(["!disabled"])
        else:
            button.state(["disabled"])

    def _update_positions_zoom_search_shortcuts(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_option_search_hint_text.set("选中期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_option_search_hint_text.set(
            f"已选期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _update_position_history_search_shortcuts(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_position_history_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_position_history_search_hint_text.set("选中历史期权后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_position_history_search_hint_text.set(
            f"已选历史期权：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def _selected_fill_history_option_for_search(self) -> OkxFillHistoryItem | None:
        if self._positions_zoom_fills_tree is None:
            return None
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            return None
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            return None
        item = self._latest_fill_history[index]
        if item.inst_type == "OPTION":
            return item
        return None

    def _update_fill_history_search_shortcuts(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        enabled = bool(contract)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_contract_button, enabled)
        self._set_optional_button_enabled(self._positions_zoom_fills_apply_expiry_prefix_button, enabled)
        if not enabled:
            self._positions_zoom_fill_history_search_hint_text.set("选中历史期权成交后，可一键带入合约或到期前缀。")
            return
        self._positions_zoom_fill_history_search_hint_text.set(
            f"已选历史期权成交：{contract} | 快捷筛选：合约={contract} | 到期前缀={expiry_prefix}"
        )

    def apply_selected_option_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        contract, _ = _option_search_shortcuts(position.inst_id if position else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.position_keyword.set(contract)
        self._render_positions_view()

    def apply_selected_option_expiry_prefix_to_position_search(self) -> None:
        position = self._selected_positions_zoom_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(position.inst_id if position else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在当前持仓里选中一条期权合约。")
            return
        self.position_keyword.set(expiry_prefix)
        self._render_positions_view()

    def apply_selected_option_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_keyword.set(contract)
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_expiry_prefix_to_position_history_search(self) -> None:
        item = self._selected_position_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史仓位里选中一条期权合约。")
            return
        self.position_history_keyword.set(expiry_prefix)
        self._render_positions_zoom_position_history_view()

    def apply_selected_option_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        contract, _ = _option_search_shortcuts(item.inst_id if item else "")
        if not contract:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_keyword.set(contract)
        self._render_positions_zoom_fills_view()

    def apply_selected_option_expiry_prefix_to_fill_history_search(self) -> None:
        item = self._selected_fill_history_option_for_search()
        _, expiry_prefix = _option_search_shortcuts(item.inst_id if item else "")
        if not expiry_prefix:
            messagebox.showinfo("快捷筛选", "请先在历史成交里选中一条期权合约。")
            return
        self.fill_history_keyword.set(expiry_prefix)
        self._render_positions_zoom_fills_view()

    def _refresh_positions_zoom_detail(self) -> None:
        if self._positions_zoom_detail is None:
            return
        payload = None
        if self._positions_zoom_selected_item_id:
            payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())
            return
        if payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self._set_readonly_text(
                    self._positions_zoom_detail,
                    _build_position_detail_text(position, self._upl_usdt_prices, self._position_instruments),
                )
                return
        label = payload["label"]
        positions = payload["item"]
        metrics = payload["metrics"]
        if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
            self._set_readonly_text(
                self._positions_zoom_detail,
                _build_group_detail_text(
                    label,
                    positions,
                    metrics,
                    self._upl_usdt_prices,
                    self._position_instruments,
                ),
            )
            return
        self._set_readonly_text(self._positions_zoom_detail, self.position_detail_text.get())

    def refresh_position_histories(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            self._positions_zoom_position_history_base_summary = "未配置 API 凭证，无法读取历史仓位。"
            self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_position_history_refresh(credentials, environment)
        self._start_fill_history_refresh(credentials, environment)

    def refresh_fill_history(self) -> None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._positions_zoom_fills_summary_text.set("未配置 API 凭证，无法读取历史成交。")
            return
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        self._start_fill_history_refresh(credentials, environment)

    def expand_fill_history_limit(self) -> None:
        self._fill_history_fetch_limit, self._fill_history_load_more_clicks, next_label = _advance_fill_history_limit(
            self._fill_history_fetch_limit,
            self._fill_history_load_more_clicks,
        )
        self._positions_zoom_fills_load_more_text.set(next_label)
        self.refresh_fill_history()

    def _start_fill_history_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._fills_history_refreshing:
            return
        self._fills_history_refreshing = True
        self._positions_zoom_fills_summary_text.set("正在刷新历史成交...")
        threading.Thread(
            target=self._refresh_fill_history_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _start_position_history_refresh(self, credentials: Credentials, environment: str) -> None:
        if self._position_history_refreshing:
            return
        self._position_history_refreshing = True
        self._positions_zoom_position_history_summary_text.set("正在刷新历史仓位...")
        threading.Thread(
            target=self._refresh_position_history_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_fill_history_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            fills = self.client.get_fills_history(credentials, environment=environment, limit=self._fill_history_fetch_limit)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    fills = self.client.get_fills_history(credentials, environment=alternate, limit=self._fill_history_fetch_limit)
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_fill_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_fill_history_error(message))
                return
        self.root.after(0, lambda: self._apply_fill_history(fills, note, effective_environment))

    def _refresh_position_history_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            position_history = self.client.get_positions_history(credentials, environment=environment, limit=100)
            usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
            note = None
            effective_environment = environment
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    position_history = self.client.get_positions_history(credentials, environment=alternate, limit=100)
                    usdt_prices = _build_position_history_usdt_price_map(self.client, position_history)
                    note = f"历史数据自动切换到 {'实盘' if alternate == 'live' else '模拟'} 环境读取。"
                    effective_environment = alternate
                except Exception:
                    self.root.after(0, lambda: self._apply_position_history_error(message))
                    return
            else:
                self.root.after(0, lambda: self._apply_position_history_error(message))
                return
        self.root.after(0, lambda: self._apply_position_history(position_history, usdt_prices, note, effective_environment))

    def _apply_fill_history(
        self,
        fills: list[OkxFillHistoryItem],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._fills_history_refreshing = False
        self._latest_fill_history = list(fills)
        self._fills_history_last_refresh_at = datetime.now()
        self._positions_history_last_refresh_at = self._fills_history_last_refresh_at
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
        fill_summary = f"历史成交：{len(fills)} 条 | 最近刷新：{timestamp}"
        if note:
            fill_summary = f"{fill_summary} | {note}"
        self._positions_zoom_fills_summary_text.set(fill_summary)
        self._render_positions_zoom_fills_view()

    def _apply_position_history(
        self,
        position_history: list[OkxPositionHistoryItem],
        usdt_prices: dict[str, Decimal],
        note: str | None = None,
        effective_environment: str | None = None,
    ) -> None:
        self._position_history_refreshing = False
        self._latest_position_history = list(position_history)
        self._position_history_usdt_prices = dict(usdt_prices)
        self._position_history_last_refresh_at = datetime.now()
        self._positions_history_last_refresh_at = self._position_history_last_refresh_at
        if effective_environment:
            self._positions_effective_environment = effective_environment
        timestamp = self._position_history_last_refresh_at.strftime("%H:%M:%S")
        history_summary = f"历史仓位：{len(position_history)} 条 | 最近刷新：{timestamp}"
        if note:
            history_summary = f"{history_summary} | {note}"
        self._positions_zoom_position_history_base_summary = history_summary
        self._positions_zoom_position_history_summary_text.set(history_summary)
        self._render_positions_zoom_position_history_view()

    def _apply_fill_history_error(self, message: str) -> None:
        self._fills_history_refreshing = False
        self._positions_zoom_fills_summary_text.set(f"历史成交读取失败：{message}")

    def _apply_position_history_error(self, message: str) -> None:
        self._position_history_refreshing = False
        self._positions_zoom_position_history_base_summary = f"历史仓位读取失败：{message}"
        self._positions_zoom_position_history_summary_text.set(self._positions_zoom_position_history_base_summary)

    def _render_positions_zoom_fills_view(self) -> None:
        if self._positions_zoom_fills_tree is None:
            return
        tree = self._positions_zoom_fills_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        filtered_items = _filter_fill_history_items(
            self._latest_fill_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), ""),
            side=HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), ""),
            keyword=self.fill_history_keyword.get(),
        )
        for index, item in filtered_items:
            iid = f"fill-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.fill_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_history_side(item.side, item.pos_side),
                    _format_fill_history_price(item),
                    _format_optional_decimal(item.fill_size),
                    _format_optional_decimal(item.fill_fee),
                    _format_fill_history_pnl(item),
                    item.exec_type or "-",
                  ),
                  tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
              )
        if self._fills_history_last_refresh_at is not None:
            timestamp = self._fills_history_last_refresh_at.strftime("%H:%M:%S")
            summary = f"历史成交：{len(self._latest_fill_history)} 条 | 最近刷新：{timestamp}"
        else:
            summary = f"历史成交：{len(self._latest_fill_history)} 条"
        if (
            POSITION_TYPE_OPTIONS.get(self.fill_history_type_filter.get(), "")
            or HISTORY_FILL_SIDE_FILTER_OPTIONS.get(self.fill_history_side_filter.get(), "")
            or self.fill_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_fill_history)}"
        self._positions_zoom_fills_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_fill_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_fills_view()

    def reset_fill_history_filters(self) -> None:
        self.fill_history_type_filter.set("全部类型")
        self.fill_history_side_filter.set("全部方向")
        self.fill_history_keyword.set("")
        self._render_positions_zoom_fills_view()

    def _render_positions_zoom_position_history_view(self) -> None:
        if self._positions_zoom_position_history_tree is None:
            return
        tree = self._positions_zoom_position_history_tree
        selected_before = tree.selection()[0] if tree.selection() else None
        tree.delete(*tree.get_children())
        filtered_items = _filter_position_history_items(
            self._latest_position_history,
            inst_type=POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), ""),
            margin_mode=HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), ""),
            keyword=self.position_history_keyword.get(),
        )
        for index, item in filtered_items:
            iid = f"ph-{index}"
            tree.insert(
                "",
                END,
                iid=iid,
                values=(
                    _format_okx_ms_timestamp(item.update_time),
                    item.inst_type or "-",
                    item.inst_id or "-",
                    _format_margin_mode(item.mgn_mode or ""),
                    _format_history_side(None, item.pos_side or item.direction),
                    _format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type),
                    _format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type),
                    _format_optional_decimal(item.close_size),
                    _format_position_history_pnl(item.pnl, item),
                    _format_position_history_pnl(item.realized_pnl, item, with_sign=True),
                    _format_optional_usdt(
                        _position_history_realized_pnl_usdt(item, self._position_history_usdt_prices),
                        with_sign=True,
                    ),
                ),
                tags=tuple(tag for tag in (_pnl_tag(item.pnl),) if tag),
            )
        summary = self._positions_zoom_position_history_base_summary
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_keyword.get().strip()
        ):
            summary = f"{summary} | 当前显示：{len(filtered_items)}/{len(self._latest_position_history)}"
        if (
            POSITION_TYPE_OPTIONS.get(self.position_history_type_filter.get(), "")
            or HISTORY_MARGIN_MODE_FILTER_OPTIONS.get(self.position_history_margin_filter.get(), "")
            or self.position_history_keyword.get().strip()
        ):
            summary = (
                f"{self._positions_zoom_position_history_base_summary} | \u5f53\u524d\u663e\u793a\uff1a{len(filtered_items)}/{len(self._latest_position_history)}"
                f"\n\u7b5b\u9009\u7edf\u8ba1\uff1a"
                f"{_format_position_history_filter_stats(filtered_items, self._position_history_usdt_prices)}"
            )
        self._positions_zoom_position_history_summary_text.set(summary)
        if selected_before and tree.exists(selected_before):
            tree.selection_set(selected_before)
            tree.focus(selected_before)
        elif tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _on_positions_zoom_fills_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_fills_detail()
        self._update_fill_history_search_shortcuts()

    def _on_position_history_filter_changed(self, *_: object) -> None:
        self._render_positions_zoom_position_history_view()

    def reset_position_history_filters(self) -> None:
        self.position_history_type_filter.set("全部类型")
        self.position_history_margin_filter.set("全部模式")
        self.position_history_keyword.set("")
        self._render_positions_zoom_position_history_view()

    def _on_positions_zoom_position_history_selected(self, *_: object) -> None:
        self._refresh_positions_zoom_position_history_detail()
        self._update_position_history_search_shortcuts()

    def _refresh_positions_zoom_fills_detail(self) -> None:
        if self._positions_zoom_fills_tree is None or self._positions_zoom_fills_detail is None:
            return
        selection = self._positions_zoom_fills_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        index = _history_tree_index(selection[0], "fill")
        if index is None or index >= len(self._latest_fill_history):
            self._set_readonly_text(self._positions_zoom_fills_detail, "这里会显示选中历史成交单的详情。")
            return
        self._set_readonly_text(self._positions_zoom_fills_detail, _build_fill_history_detail_text(self._latest_fill_history[index]))

    def _refresh_positions_zoom_position_history_detail(self) -> None:
        if self._positions_zoom_position_history_tree is None or self._positions_zoom_position_history_detail is None:
            return
        selection = self._positions_zoom_position_history_tree.selection()
        if not selection:
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        index = _history_tree_index(selection[0], "ph")
        if index is None or index >= len(self._latest_position_history):
            self._set_readonly_text(self._positions_zoom_position_history_detail, "这里会显示选中历史仓位的详情。")
            return
        self._set_readonly_text(
            self._positions_zoom_position_history_detail,
            _build_position_history_detail_text(
                self._latest_position_history[index],
                self._position_history_usdt_prices,
            ),
        )

    def _selected_option_position(self, *, prefer_protection_form: bool = False) -> OkxPosition | None:
        if prefer_protection_form and self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        payload = None
        if self._positions_zoom_window is not None and self._positions_zoom_window.winfo_exists():
            if self._positions_zoom_selected_item_id:
                payload = self._position_row_payloads.get(self._positions_zoom_selected_item_id)
        if payload is None:
            payload = self._selected_position_payload()
        if payload is not None and payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition) and position.inst_type == "OPTION":
                return position
        if self._protection_form_position_key:
            fallback = _find_position_by_key(self._latest_positions, self._protection_form_position_key)
            if fallback is not None and fallback.inst_type == "OPTION":
                return fallback
        return None

    def open_position_protection_window(self) -> None:
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._populate_protection_form_from_selection(force=True)
            self._refresh_protection_window_view()
            self._protection_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("期权持仓保护")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.76,
            height_ratio=0.78,
            min_width=980,
            min_height=760,
            max_width=1520,
            max_height=1060,
        )
        self._protection_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_position_protection_window)

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)
        container.rowconfigure(2, weight=1)

        form_frame = ttk.LabelFrame(container, text="选中期权持仓保护", padding=16)
        form_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            form_frame.columnconfigure(column, weight=1)

        ttk.Label(
            form_frame,
            textvariable=self._protection_form_title_text,
            justify="left",
            wraplength=960,
            font=("Microsoft YaHei UI", 10, "bold"),
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 12))
        ttk.Label(
            form_frame,
            textvariable=self._protection_logic_hint_text,
            justify="left",
            wraplength=960,
            foreground="#8a4600",
        ).grid(row=1, column=0, columnspan=4, sticky="w", pady=(0, 8))

        row = 2
        ttk.Label(form_frame, text="触发条件").grid(row=row, column=0, sticky="w")
        protection_trigger_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_trigger_source_label,
            values=list(PROTECTION_TRIGGER_SOURCE_OPTIONS.keys()),
            state="readonly",
        )
        protection_trigger_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        protection_trigger_combo.bind("<<ComboboxSelected>>", self._on_protection_trigger_source_changed)
        ttk.Label(form_frame, text="现货标的").grid(row=row, column=2, sticky="w")
        self._protection_spot_symbol_entry = ttk.Entry(form_frame, textvariable=self.protection_spot_symbol)
        self._protection_spot_symbol_entry.grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(form_frame, text="止盈触发价").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_take_profit_trigger).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(form_frame, text="止损触发价").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_stop_loss_trigger).grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(form_frame, text="止盈报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        take_profit_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_take_profit_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        take_profit_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        take_profit_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止盈报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_take_profit_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_order_price,
        )
        self._protection_take_profit_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止盈滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_take_profit_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_take_profit_slippage,
        )
        self._protection_take_profit_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(form_frame, text="轮询秒数").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(form_frame, textvariable=self.protection_poll_seconds).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损报单方式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        stop_loss_mode_combo = ttk.Combobox(
            form_frame,
            textvariable=self.protection_stop_loss_order_mode_label,
            values=list(PROTECTION_ORDER_MODE_OPTIONS.keys()),
            state="readonly",
        )
        stop_loss_mode_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        stop_loss_mode_combo.bind("<<ComboboxSelected>>", self._on_protection_order_mode_changed)
        ttk.Label(form_frame, text="止损报单价格").grid(row=row, column=2, sticky="w", pady=(12, 0))
        self._protection_stop_loss_order_price_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_order_price,
        )
        self._protection_stop_loss_order_price_entry.grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(form_frame, text="止损滑点").grid(row=row, column=0, sticky="w", pady=(12, 0))
        self._protection_stop_loss_slippage_entry = ttk.Entry(
            form_frame,
            textvariable=self.protection_stop_loss_slippage,
        )
        self._protection_stop_loss_slippage_entry.grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(
            form_frame,
            text="说明：触发条件可用“期权标记价格”或“现货最新价”；报单价格可用“设定价格”或“标记价格加减滑点”。",
            justify="left",
            wraplength=520,
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        action_frame = ttk.Frame(form_frame)
        action_frame.grid(row=row + 1, column=0, columnspan=4, sticky="e", pady=(16, 0))
        ttk.Button(action_frame, text="启动保护", command=self.start_selected_position_protection).grid(
            row=0, column=0, padx=(0, 8)
        )
        ttk.Button(action_frame, text="回放模拟", command=self.open_position_protection_replay_window).grid(
            row=0, column=1, padx=(0, 8)
        )
        ttk.Button(action_frame, text="停止选中任务", command=self.stop_selected_position_protection).grid(
            row=0, column=2, padx=(0, 8)
        )
        ttk.Button(action_frame, text="关闭", command=self._close_position_protection_window).grid(row=0, column=3)

        sessions_frame = ttk.LabelFrame(container, text="运行中的期权保护任务", padding=12)
        sessions_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        sessions_frame.columnconfigure(0, weight=1)
        sessions_frame.rowconfigure(1, weight=1)
        sessions_header = ttk.Frame(sessions_frame)
        sessions_header.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        sessions_header.columnconfigure(0, weight=1)
        ttk.Label(sessions_header, textvariable=self._protection_status_text).grid(row=0, column=0, sticky="w")
        ttk.Button(sessions_header, text="清除已结束", command=self.clear_finished_position_protections).grid(
            row=0, column=1, sticky="e"
        )
        task_tree = ttk.Treeview(
            sessions_frame,
            columns=("api", "option", "trigger", "direction", "status", "started"),
            show="headings",
            selectmode="browse",
        )
        task_tree.heading("api", text="API")
        task_tree.heading("option", text="期权合约")
        task_tree.heading("trigger", text="触发条件")
        task_tree.heading("direction", text="方向")
        task_tree.heading("status", text="状态")
        task_tree.heading("started", text="启动时间")
        task_tree.column("api", width=96, anchor="center")
        task_tree.column("option", width=250, anchor="w")
        task_tree.column("trigger", width=180, anchor="w")
        task_tree.column("direction", width=80, anchor="center")
        task_tree.column("status", width=100, anchor="center")
        task_tree.column("started", width=120, anchor="center")
        task_tree.grid(row=1, column=0, sticky="nsew")
        task_tree.bind("<<TreeviewSelect>>", self._on_protection_session_selected)
        task_scroll = ttk.Scrollbar(sessions_frame, orient="vertical", command=task_tree.yview)
        task_scroll.grid(row=1, column=1, sticky="ns")
        task_tree.configure(yscrollcommand=task_scroll.set)
        self._protection_sessions_tree = task_tree

        detail_frame = ttk.LabelFrame(container, text="保护任务详情", padding=12)
        detail_frame.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(0, weight=1)
        self._protection_detail_text = Text(
            detail_frame,
            height=8,
            wrap="word",
            font=("Microsoft YaHei UI", 10),
            relief="flat",
        )
        self._protection_detail_text.grid(row=0, column=0, sticky="nsew")
        detail_scroll = ttk.Scrollbar(detail_frame, orient="vertical", command=self._protection_detail_text.yview)
        detail_scroll.grid(row=0, column=1, sticky="ns")
        self._protection_detail_text.configure(yscrollcommand=detail_scroll.set)
        self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")

        self._populate_protection_form_from_selection(force=True)
        self._refresh_protection_window_view()

    def _close_position_protection_window(self) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
            self._protection_order_mode_job = None
        if self._protection_window is not None and self._protection_window.winfo_exists():
            self._protection_window.destroy()
        self._protection_window = None
        self._protection_sessions_tree = None
        self._protection_detail_text = None
        self._protection_selected_session_id = None
        self._protection_form_position_id = None
        self._protection_form_position_key = None

    def _populate_protection_form_from_selection(self, *, force: bool = False) -> None:
        position = self._selected_option_position(prefer_protection_form=not force)
        if position is None:
            if self._positions_view_rendering and self._protection_form_position_key:
                return
            self._protection_form_position_id = None
            self._protection_form_position_key = None
            self._protection_form_title_text.set("当前没有选中期权持仓。请先在账户持仓里选中一条期权仓位。")
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return

        direction = derive_position_direction(position)
        self._protection_form_title_text.set(
            f"当前选中：{position.inst_id} | 方向={direction.upper()} | 持仓量={format_decimal(position.position)} | "
            f"开仓均价={_format_optional_decimal(position.avg_price)}"
        )
        position_key = _position_tree_row_id(position)
        if force or self._protection_form_position_key != position_key:
            self._protection_form_position_id = position.inst_id
            self._protection_form_position_key = position_key
            self.protection_trigger_source_label.set("期权标记价格")
            self.protection_spot_symbol.set(infer_default_spot_inst_id(position.inst_id))
            self.protection_take_profit_trigger.set("")
            self.protection_stop_loss_trigger.set("")
            self.protection_take_profit_order_mode_label.set("标记价格加减滑点")
            self.protection_take_profit_order_price.set("")
            self.protection_take_profit_slippage.set("0")
            self.protection_stop_loss_order_mode_label.set("标记价格加减滑点")
            self.protection_stop_loss_order_price.set("")
            self.protection_stop_loss_slippage.set("0")
            self.protection_poll_seconds.set("2")
        self._on_protection_trigger_source_changed()
        self._update_protection_order_mode_widgets()
        self._update_protection_logic_hint()

    def _on_protection_trigger_source_changed(self, *_: object) -> None:
        if hasattr(self, "_protection_spot_symbol_entry"):
            state = "normal" if self.protection_trigger_source_label.get() == "现货最新价" else "disabled"
            self._protection_spot_symbol_entry.configure(state=state)
        self._update_protection_logic_hint()

    def _update_protection_logic_hint(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            self._protection_logic_hint_text.set("请先选择一条期权持仓，系统会显示当前组合下的止盈止损方向。")
            return
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS.get(self.protection_trigger_source_label.get(), "option_mark")
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get()) or infer_default_spot_inst_id(position.inst_id)
            trigger_price_type = "last"
        self._protection_logic_hint_text.set(
            describe_protection_price_logic(
                option_inst_id=position.inst_id,
                direction=derive_position_direction(position),
                trigger_inst_id=trigger_inst_id,
                trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
            )
        )

    def _on_protection_order_mode_changed(self, *_: object) -> None:
        if self._protection_order_mode_job is not None:
            try:
                self.root.after_cancel(self._protection_order_mode_job)
            except Exception:
                pass
        self._protection_order_mode_job = self.root.after(1, self._apply_protection_order_mode_widgets)

    def _apply_protection_order_mode_widgets(self) -> None:
        self._protection_order_mode_job = None
        self._update_protection_order_mode_widgets()

    def _update_protection_order_mode_widgets(self) -> None:
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_take_profit_order_mode_label.get(),
            price_var=self.protection_take_profit_order_price,
            price_entry=self._protection_take_profit_order_price_entry,
            slippage_entry=self._protection_take_profit_slippage_entry,
            memory_attr="_protection_take_profit_fixed_price_memory",
        )
        self._sync_protection_order_mode_widget(
            mode_label=self.protection_stop_loss_order_mode_label.get(),
            price_var=self.protection_stop_loss_order_price,
            price_entry=self._protection_stop_loss_order_price_entry,
            slippage_entry=self._protection_stop_loss_slippage_entry,
            memory_attr="_protection_stop_loss_fixed_price_memory",
        )

    def _sync_protection_order_mode_widget(
        self,
        *,
        mode_label: str,
        price_var: StringVar,
        price_entry: ttk.Entry | None,
        slippage_entry: ttk.Entry | None,
        memory_attr: str,
    ) -> None:
        placeholder = "自动按标记价与滑点计算"
        fixed_mode = _resolve_protection_order_mode_value(mode_label) == "fixed_price"
        if fixed_mode:
            if price_var.get() == placeholder:
                price_var.set(getattr(self, memory_attr, ""))
            if price_entry is not None:
                price_entry.configure(state="normal")
            if slippage_entry is not None:
                slippage_entry.configure(state="disabled")
        else:
            current = price_var.get().strip()
            if current and current != placeholder:
                setattr(self, memory_attr, current)
            price_var.set(placeholder)
            if price_entry is not None:
                price_entry.configure(state="readonly")
            if slippage_entry is not None:
                slippage_entry.configure(state="normal")

    def _refresh_protection_window_view(self) -> None:
        if self._protection_window is None or not self._protection_window.winfo_exists():
            return
        self._populate_protection_form_from_selection(force=False)
        sessions = self._protection_manager.list_sessions()
        self._protection_status_text.set(
            f"当前保护任务：{len(sessions)}"
            if sessions
            else "当前没有运行中的期权持仓保护任务。"
        )
        if self._protection_sessions_tree is not None:
            selected_before = self._protection_sessions_tree.selection()
            self._protection_sessions_tree.delete(*self._protection_sessions_tree.get_children())
            for session in sessions:
                self._protection_sessions_tree.insert(
                    "",
                    END,
                    iid=session.session_id,
                    values=(
                        session.api_name or "-",
                        session.option_inst_id,
                        session.trigger_label,
                        session.direction,
                        session.status,
                        session.started_at.strftime("%H:%M:%S"),
                    ),
                )
            if selected_before and self._protection_sessions_tree.exists(selected_before[0]):
                target = selected_before[0]
            else:
                target = sessions[0].session_id if sessions else None
            if target is not None:
                self._protection_sessions_tree.selection_set(target)
                self._protection_sessions_tree.focus(target)
                self._protection_selected_session_id = target
            else:
                self._protection_selected_session_id = None
        self._refresh_protection_detail_panel()

    def _on_protection_session_selected(self, *_: object) -> None:
        if self._protection_sessions_tree is None:
            return
        selection = self._protection_sessions_tree.selection()
        self._protection_selected_session_id = selection[0] if selection else None
        self._refresh_protection_detail_panel()

    def _refresh_protection_detail_panel(self) -> None:
        if self._protection_detail_text is None:
            return
        sessions = {item.session_id: item for item in self._protection_manager.list_sessions()}
        session = sessions.get(self._protection_selected_session_id or "")
        if session is None:
            self._set_readonly_text(self._protection_detail_text, "请选择一个保护任务查看详情。")
            return
        self._set_readonly_text(
            self._protection_detail_text,
            "\n".join(
                [
                    f"任务：{session.session_id}",
                    f"API配置：{session.api_name or '-'}",
                    f"期权合约：{session.option_inst_id}",
                    f"触发条件：{session.trigger_label}",
                    f"触发标的：{session.trigger_inst_id}",
                    f"触发价格类型：{_format_protection_trigger_price_type(session.trigger_price_type)}",
                    f"方向：{session.direction}",
                    f"持仓方向：{session.pos_side or '-'}",
                    f"止盈触发：{_format_optional_decimal(session.take_profit_trigger)}",
                    f"止盈报单方式：{_format_protection_order_mode_label(session.take_profit_order_mode)}",
                    f"止盈报单价格：{_format_protection_order_price_detail(session.take_profit_order_mode, session.take_profit_order_price)}",
                    f"止盈滑点：{_format_optional_decimal(session.take_profit_slippage)}",
                    f"止损触发：{_format_optional_decimal(session.stop_loss_trigger)}",
                    f"止损报单方式：{_format_protection_order_mode_label(session.stop_loss_order_mode)}",
                    f"止损报单价格：{_format_protection_order_price_detail(session.stop_loss_order_mode, session.stop_loss_order_price)}",
                    f"止损滑点：{_format_optional_decimal(session.stop_loss_slippage)}",
                    f"轮询秒数：{session.poll_seconds:g}",
                    f"状态：{session.status}",
                    f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}",
                    "",
                    f"最新状态：{session.last_message}",
                ]
            ),
        )

    def start_selected_position_protection(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        credentials = self._current_credentials_or_none()
        if credentials is None:
            messagebox.showerror("启动失败", "请先在设置里配置 API 凭证。", parent=self._protection_window or self.root)
            return

        try:
            notifier = self._build_optional_protection_notifier()
            self._protection_manager.set_notifier(notifier)
            protection = self._build_selected_position_protection(position)
            _validate_protection_live_price_availability(self.client, protection, position)
            config = self._build_manual_protection_strategy_config(position, protection)
            session_id = self._protection_manager.start(credentials, config, protection)
            if credentials.profile_name:
                self._enqueue_log(f"[持仓保护 {credentials.profile_name} {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            else:
                self._enqueue_log(f"[持仓保护 {session_id}] 已启动 {position.inst_id} 的期权保护任务。")
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("启动保护失败", str(exc), parent=self._protection_window or self.root)

    def stop_selected_position_protection(self) -> None:
        if not self._protection_selected_session_id:
            messagebox.showinfo("提示", "请先在下方列表中选中一个保护任务。", parent=self._protection_window or self.root)
            return
        try:
            self._protection_manager.stop(self._protection_selected_session_id)
            self._refresh_protection_window_view()
        except Exception as exc:
            messagebox.showerror("停止保护失败", str(exc), parent=self._protection_window or self.root)

    def open_position_protection_replay_window(self) -> None:
        position = self._selected_option_position(prefer_protection_form=True)
        if position is None:
            messagebox.showinfo("提示", "请先在账户持仓中选中一条期权仓位。", parent=self._protection_window or self.root)
            return
        try:
            protection = self._build_selected_position_protection(position)
        except Exception as exc:
            messagebox.showerror("回放参数错误", str(exc), parent=self._protection_window or self.root)
            return

        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()

        self._protection_replay_window = ProtectionReplayWindow(
            self.root,
            self.client,
            position,
            protection,
            initial_state=ProtectionReplayLaunchState(bar=self.bar.get(), candle_limit="120"),
        )

    def clear_finished_position_protections(self) -> None:
        cleared = self._protection_manager.clear_finished()
        self._refresh_protection_window_view()
        if cleared <= 0:
            messagebox.showinfo("提示", "当前没有可清除的已结束任务。", parent=self._protection_window or self.root)

    def _build_selected_position_protection(self, position: OkxPosition) -> OptionProtectionConfig:
        trigger_source = PROTECTION_TRIGGER_SOURCE_OPTIONS[self.protection_trigger_source_label.get()]
        if trigger_source == "option_mark":
            trigger_inst_id = position.inst_id
            trigger_price_type = "mark"
            trigger_label = f"{position.inst_id} 标记价"
        else:
            trigger_inst_id = normalize_spot_inst_id(self.protection_spot_symbol.get())
            if not trigger_inst_id:
                raise ValueError("现货触发模式下，请填写现货标的。")
            trigger_instrument = self.client.get_instrument(trigger_inst_id)
            if trigger_instrument.inst_type != "SPOT":
                raise ValueError("现货触发模式下，请填写现货交易对，例如 BTC-USDT。")
            trigger_price_type = "last"
            trigger_label = f"{trigger_inst_id} 最新价"

        take_profit_trigger = self._parse_optional_positive_decimal(self.protection_take_profit_trigger.get(), "止盈触发价")
        stop_loss_trigger = self._parse_optional_positive_decimal(self.protection_stop_loss_trigger.get(), "止损触发价")
        if take_profit_trigger is None and stop_loss_trigger is None:
            raise ValueError("止盈触发价和止损触发价至少要填写一个。")

        direction = derive_position_direction(position)
        _validate_protection_price_relationship(
            option_inst_id=position.inst_id,
            direction=direction,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            take_profit=take_profit_trigger,
            stop_loss=stop_loss_trigger,
        )

        take_profit_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_take_profit_order_mode_label.get()]
        stop_loss_order_mode = PROTECTION_ORDER_MODE_OPTIONS[self.protection_stop_loss_order_mode_label.get()]
        take_profit_order_price = self._parse_protection_order_price(
            self.protection_take_profit_order_price.get(),
            "止盈报单价格",
            take_profit_order_mode,
        )
        stop_loss_order_price = self._parse_protection_order_price(
            self.protection_stop_loss_order_price.get(),
            "止损报单价格",
            stop_loss_order_mode,
        )
        return OptionProtectionConfig(
            option_inst_id=position.inst_id,
            trigger_inst_id=trigger_inst_id,
            trigger_price_type=trigger_price_type,
            direction=direction,
            pos_side=position.pos_side if position.pos_side and position.pos_side.lower() != "net" else None,
            take_profit_trigger=take_profit_trigger,
            stop_loss_trigger=stop_loss_trigger,
            take_profit_order_mode=take_profit_order_mode,
            take_profit_order_price=take_profit_order_price,
            take_profit_slippage=self._parse_nonnegative_decimal(self.protection_take_profit_slippage.get(), "止盈滑点"),
            stop_loss_order_mode=stop_loss_order_mode,
            stop_loss_order_price=stop_loss_order_price,
            stop_loss_slippage=self._parse_nonnegative_decimal(self.protection_stop_loss_slippage.get(), "止损滑点"),
            poll_seconds=float(self._parse_positive_decimal(self.protection_poll_seconds.get(), "轮询秒数")),
            trigger_label=trigger_label,
        )

    def _parse_protection_order_price(self, raw: str, field_name: str, order_mode: str) -> Decimal | None:
        if order_mode != "fixed_price":
            return None
        return self._parse_positive_decimal(raw, field_name)

    def _parse_nonnegative_decimal(self, raw: str, field_name: str) -> Decimal:
        cleaned = raw.strip()
        if not cleaned:
            return Decimal("0")
        try:
            value = Decimal(cleaned)
        except InvalidOperation as exc:
            raise ValueError(f"{field_name} 不是有效数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _build_manual_protection_strategy_config(
        self,
        position: OkxPosition,
        protection: OptionProtectionConfig,
    ) -> StrategyConfig:
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        trade_mode = position.mgn_mode if position.mgn_mode in {"cross", "isolated"} else TRADE_MODE_OPTIONS[self.trade_mode_label.get()]
        position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
        return StrategyConfig(
            inst_id=protection.trigger_inst_id,
            bar=self.bar.get(),
            ema_period=1,
            atr_period=1,
            atr_stop_multiplier=Decimal("1"),
            atr_take_multiplier=Decimal("1"),
            order_size=abs(position.position),
            trade_mode=trade_mode,
            signal_mode="long_only" if protection.direction == "long" else "short_only",
            position_mode=position_mode,
            environment=environment,
            tp_sl_trigger_type=protection.trigger_price_type,
            strategy_id="manual_option_protection",
            poll_seconds=protection.poll_seconds,
            risk_amount=None,
            trade_inst_id=position.inst_id,
            tp_sl_mode="local_trade",
            local_tp_sl_inst_id=protection.trigger_inst_id,
            entry_side_mode="follow_signal",
            run_mode="trade",
        )

    def _build_optional_protection_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=False)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 持仓保护"))

    def open_settings_window(self) -> None:
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.focus_force()
            return

        window = Toplevel(self.root)
        window.title("API 与通知设置")
        apply_adaptive_window_geometry(
            window,
            width_ratio=0.58,
            height_ratio=0.66,
            min_width=760,
            min_height=620,
            max_width=1080,
            max_height=900,
        )
        window.transient(self.root)
        self._settings_window = window
        window.protocol("WM_DELETE_WINDOW", self._close_settings_window)

        container = ttk.Frame(window, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        account_frame = ttk.LabelFrame(container, text="API 与交易设置", padding=16)
        account_frame.grid(row=0, column=0, sticky="ew")
        for column in range(4):
            account_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Label(account_frame, text="API 配置").grid(row=row, column=0, sticky="w")
        self._credential_profile_combo = ttk.Combobox(
            account_frame,
            textvariable=self.api_profile_name,
            state="readonly",
        )
        self._credential_profile_combo.grid(row=row, column=1, sticky="ew", padx=(0, 16))
        self._credential_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        profile_buttons = ttk.Frame(account_frame)
        profile_buttons.grid(row=row, column=2, columnspan=2, sticky="e")
        ttk.Button(profile_buttons, text="新建配置", command=self._create_api_profile).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(profile_buttons, text="重命名", command=self._rename_current_api_profile).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(profile_buttons, text="删除当前", command=self._delete_current_api_profile).grid(row=0, column=2)
        self._sync_credential_profile_combo()

        row += 1
        ttk.Label(account_frame, text="API Key").grid(row=row, column=0, sticky="w")
        ttk.Entry(account_frame, textvariable=self.api_key).grid(row=row, column=1, sticky="ew", padx=(0, 16))
        ttk.Label(account_frame, text="Passphrase").grid(row=row, column=2, sticky="w")
        ttk.Entry(account_frame, textvariable=self.passphrase, show="*").grid(row=row, column=3, sticky="ew")

        row += 1
        ttk.Label(account_frame, text="Secret Key").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(account_frame, textvariable=self.secret_key, show="*").grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(account_frame, text="环境").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.environment_label,
            values=list(ENV_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(account_frame, text="交易模式").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.trade_mode_label,
            values=list(TRADE_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(account_frame, text="持仓模式").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.position_mode_label,
            values=list(POSITION_MODE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(account_frame, text="TP/SL 触发价格类型").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Combobox(
            account_frame,
            textvariable=self.trigger_type_label,
            values=list(TRIGGER_TYPE_OPTIONS.keys()),
            state="readonly",
        ).grid(row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0))
        ttk.Label(
            account_frame,
            text=f"凭证自动保存到：{credentials_file_path().name}（支持多个 API 配置）",
            justify="left",
        ).grid(row=row, column=2, columnspan=2, sticky="w", pady=(12, 0))

        mail_frame = ttk.LabelFrame(container, text="邮件通知设置", padding=16)
        mail_frame.grid(row=1, column=0, sticky="nsew", pady=(14, 0))
        for column in range(4):
            mail_frame.columnconfigure(column, weight=1)

        row = 0
        ttk.Checkbutton(mail_frame, text="启用邮件通知", variable=self.notify_enabled).grid(
            row=row, column=0, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="使用 SSL", variable=self.use_ssl).grid(
            row=row, column=1, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="成交邮件", variable=self.notify_trade_fills).grid(
            row=row, column=2, sticky="w"
        )
        ttk.Checkbutton(mail_frame, text="信号邮件", variable=self.notify_signals).grid(
            row=row, column=3, sticky="w"
        )

        row += 1
        ttk.Label(mail_frame, text="SMTP 主机").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_host).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="SMTP 端口").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_port).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Label(mail_frame, text="SMTP 用户名").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_username).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="SMTP 密码").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.smtp_password, show="*").grid(
            row=row, column=3, sticky="ew", pady=(12, 0)
        )

        row += 1
        ttk.Label(mail_frame, text="发件邮箱").grid(row=row, column=0, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.sender_email).grid(
            row=row, column=1, sticky="ew", padx=(0, 16), pady=(12, 0)
        )
        ttk.Label(mail_frame, text="收件邮箱").grid(row=row, column=2, sticky="w", pady=(12, 0))
        ttk.Entry(mail_frame, textvariable=self.recipient_emails).grid(row=row, column=3, sticky="ew", pady=(12, 0))

        row += 1
        ttk.Checkbutton(mail_frame, text="异常邮件", variable=self.notify_errors).grid(
            row=row, column=0, sticky="w", pady=(12, 0)
        )
        ttk.Label(
            mail_frame,
            text="多个收件人可用逗号、分号或换行分隔。",
            justify="left",
        ).grid(row=row, column=1, columnspan=3, sticky="w", pady=(12, 0))

        row += 1
        ttk.Label(
            mail_frame,
            text=f"通知设置会保存到：{settings_file_path().name}",
            justify="left",
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(16, 0))

        footer = ttk.Frame(container)
        footer.grid(row=2, column=0, sticky="e", pady=(16, 0))
        ttk.Button(footer, text="发送测试邮件", command=self.send_test_email).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(footer, text="关闭", command=self._close_settings_window).grid(row=0, column=1)

    def open_backtest_window(self) -> None:
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.focus_force()
            return

        self._backtest_window = BacktestWindow(
            self.root,
            self.client,
            BacktestLaunchState(
                strategy_name=self.strategy_name.get(),
                symbol=_normalize_symbol_input(self.symbol.get()),
                bar=self.bar.get(),
                ema_period=self.ema_period.get(),
                trend_ema_period=self.trend_ema_period.get(),
                big_ema_period=self.big_ema_period.get(),
                atr_period=self.atr_period.get(),
                stop_atr=self.stop_atr.get(),
                take_atr=self.take_atr.get(),
                risk_amount=self.risk_amount.get(),
                signal_mode_label=self.signal_mode_label.get(),
                trade_mode_label=self.trade_mode_label.get(),
                position_mode_label=self.position_mode_label.get(),
                trigger_type_label=self.trigger_type_label.get(),
                environment_label=self.environment_label.get(),
                maker_fee_percent="0.01",
                taker_fee_percent="0.028",
                initial_capital="10000",
                sizing_mode_label="固定风险金",
                risk_percent="1",
                compounding_enabled=False,
                slippage_percent="0",
                funding_rate_percent="0",
                candle_limit="10000",
            ),
        )

    def open_smart_order_window(self) -> None:
        if self._smart_order_window is not None and self._smart_order_window.window.winfo_exists():
            self._smart_order_window.show()
            return
        self._smart_order_window = SmartOrderWindow(
            self.root,
            self.client,
            runtime_config_provider=self._build_smart_order_runtime_config_or_none,
            logger=self._enqueue_log,
        )

    def open_backtest_compare_window(self) -> None:
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.show()
            return
        self._backtest_compare_window = BacktestCompareOverviewWindow(self.root)

    def open_signal_monitor_window(self) -> None:
        if self._signal_monitor_window is not None and self._signal_monitor_window.window.winfo_exists():
            self._signal_monitor_window.show()
            return

        self._signal_monitor_window = SignalMonitorWindow(
            self.root,
            self.client,
            notifier_factory=self._build_signal_monitor_notifier,
            logger=self._enqueue_log,
        )

    def open_deribit_volatility_monitor_window(self) -> None:
        if (
            self._deribit_volatility_monitor_window is not None
            and self._deribit_volatility_monitor_window.window.winfo_exists()
        ):
            self._deribit_volatility_monitor_window.show()
            return

        self._deribit_volatility_monitor_window = DeribitVolatilityMonitorWindow(
            self.root,
            self.deribit_client,
            notifier_factory=self._build_signal_monitor_notifier,
            logger=self._enqueue_log,
        )

    def open_deribit_volatility_window(self) -> None:
        if self._deribit_volatility_window is not None and self._deribit_volatility_window.window.winfo_exists():
            self._deribit_volatility_window.show()
            return

        self._deribit_volatility_window = DeribitVolatilityWindow(
            self.root,
            self.deribit_client,
            logger=self._enqueue_log,
        )

    def open_option_strategy_window(self) -> None:
        if self._option_strategy_window is not None and self._option_strategy_window.window.winfo_exists():
            self._option_strategy_window.show()
            return

        self._option_strategy_window = OptionStrategyCalculatorWindow(
            self.root,
            self.client,
            runtime_provider=self._build_option_strategy_runtime_or_none,
            logger=self._enqueue_log,
        )

    def _close_settings_window(self) -> None:
        self._save_credentials_now(silent=True)
        self._save_notification_settings_now(silent=True)
        if self._settings_window is not None and self._settings_window.winfo_exists():
            self._settings_window.destroy()
        self._settings_window = None
        self._credential_profile_combo = None

    def _credential_profile_names(self) -> list[str]:
        if not self._credential_profiles:
            return [DEFAULT_CREDENTIAL_PROFILE_NAME]
        return sorted(self._credential_profiles.keys())

    def _current_credential_profile(self) -> str:
        profile_name = self.api_profile_name.get().strip()
        if profile_name:
            return profile_name
        return self._credential_profile_names()[0]

    def _editing_credential_profile(self) -> str:
        profile_name = self._loaded_credential_profile_name.strip()
        if profile_name:
            return profile_name
        return self._current_credential_profile()

    def _current_credentials_state(self) -> tuple[str, str, str, str]:
        return (
            self._editing_credential_profile(),
            self.api_key.get().strip(),
            self.secret_key.get().strip(),
            self.passphrase.get().strip(),
        )

    def _set_credentials_fields(self, snapshot: dict[str, str]) -> None:
        was_enabled = self._credential_watch_enabled
        self._credential_watch_enabled = False
        self.api_key.set(snapshot["api_key"])
        self.secret_key.set(snapshot["secret_key"])
        self.passphrase.set(snapshot["passphrase"])
        self._credential_watch_enabled = was_enabled

    def _sync_credential_profile_combo(self) -> None:
        values = self._credential_profile_names()
        header_width = max(8, min(14, max((len(item) for item in values), default=8) + 1))
        if self._header_credential_profile_combo is not None:
            self._header_credential_profile_combo.configure(values=values, width=header_width)
        if self._credential_profile_combo is not None:
            self._credential_profile_combo.configure(values=values)
        current = self._current_credential_profile()
        if current not in values:
            current = values[0]
        if self.api_profile_name.get() != current:
            self.api_profile_name.set(current)

    def _apply_credentials_profile(self, profile_name: str, *, log_change: bool = False) -> None:
        target = profile_name.strip() or DEFAULT_CREDENTIAL_PROFILE_NAME
        snapshot = self._credential_profiles.get(target, {"api_key": "", "secret_key": "", "passphrase": ""})
        self._loaded_credential_profile_name = target
        self.api_profile_name.set(target)
        self._set_credentials_fields(snapshot)
        self._last_saved_credentials = (
            target,
            snapshot["api_key"],
            snapshot["secret_key"],
            snapshot["passphrase"],
        )
        self._sync_credential_profile_combo()
        self._update_settings_summary()
        if log_change:
            self._enqueue_log(f"已切换 API 配置：{target}")

    def _load_saved_credentials(self) -> None:
        try:
            snapshot = load_credentials_profiles_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取本地凭证文件失败：{exc}")
            return

        profiles = snapshot.get("profiles", {})
        self._credential_profiles = profiles if isinstance(profiles, dict) else {}
        self._sync_credential_profile_combo()
        self._apply_credentials_profile(str(snapshot.get("selected_profile", DEFAULT_CREDENTIAL_PROFILE_NAME)))
        if any(self._current_credentials_state()[1:]):
            self._enqueue_log(f"已自动读取本地凭证文件：{credentials_file_path().name}")

    def _load_saved_notification_settings(self) -> None:
        try:
            snapshot = load_notification_snapshot()
        except Exception as exc:
            self._enqueue_log(f"读取通知设置失败：{exc}")
            return

        self.environment_label.set(str(snapshot["environment_label"]))
        self.trade_mode_label.set(str(snapshot["trade_mode_label"]))
        self.position_mode_label.set(str(snapshot["position_mode_label"]))
        self.trigger_type_label.set(str(snapshot["trigger_type_label"]))
        self.notify_enabled.set(bool(snapshot["enabled"]))
        self.smtp_host.set(str(snapshot["smtp_host"]))
        self.smtp_port.set(str(snapshot["smtp_port"]))
        self.smtp_username.set(str(snapshot["smtp_username"]))
        self.smtp_password.set(str(snapshot["smtp_password"]))
        self.sender_email.set(str(snapshot["sender_email"]))
        self.recipient_emails.set(str(snapshot["recipient_emails"]))
        self.use_ssl.set(bool(snapshot["use_ssl"]))
        self.notify_trade_fills.set(bool(snapshot["notify_trade_fills"]))
        self.notify_signals.set(bool(snapshot["notify_signals"]))
        self.notify_errors.set(bool(snapshot["notify_errors"]))
        self._last_saved_notification_state = self._current_notification_state()

    def _bind_auto_save(self) -> None:
        self.api_key.trace_add("write", self._on_credentials_changed)
        self.secret_key.trace_add("write", self._on_credentials_changed)
        self.passphrase.trace_add("write", self._on_credentials_changed)
        self.environment_label.trace_add("write", self._on_settings_changed)
        self.trade_mode_label.trace_add("write", self._on_settings_changed)
        self.position_mode_label.trace_add("write", self._on_settings_changed)
        self.trigger_type_label.trace_add("write", self._on_settings_changed)
        self._credential_watch_enabled = True

        for variable in (
            self.notify_enabled,
            self.smtp_host,
            self.smtp_port,
            self.smtp_username,
            self.smtp_password,
            self.sender_email,
            self.recipient_emails,
            self.use_ssl,
            self.notify_trade_fills,
            self.notify_signals,
            self.notify_errors,
        ):
            variable.trace_add("write", self._on_notification_settings_changed)
        self._settings_watch_enabled = True

    def _on_credentials_changed(self, *_: str) -> None:
        if not self._credential_watch_enabled:
            return
        if self._credential_save_job is not None:
            self.root.after_cancel(self._credential_save_job)
        self._credential_save_job = self.root.after(600, self._save_credentials_now)

    def _on_notification_settings_changed(self, *_: str) -> None:
        if not self._settings_watch_enabled:
            return
        self._on_settings_changed()
        if self._settings_save_job is not None:
            self.root.after_cancel(self._settings_save_job)
        self._settings_save_job = self.root.after(600, self._save_notification_settings_now)

    def _on_settings_changed(self, *_: str) -> None:
        self._update_settings_summary()

    def _save_credentials_now(self, silent: bool = False) -> None:
        if self._credential_save_job is not None:
            try:
                self.root.after_cancel(self._credential_save_job)
            except Exception:
                pass
            self._credential_save_job = None

        current = self._current_credentials_state()
        if current == self._last_saved_credentials:
            return

        try:
            profile_name, api_key, secret_key, passphrase = current
            self._credential_profiles[profile_name] = {
                "api_key": api_key,
                "secret_key": secret_key,
                "passphrase": passphrase,
            }
            save_credentials_profiles_snapshot(
                selected_profile=profile_name,
                profiles=self._credential_profiles,
            )
        except Exception as exc:
            if not silent:
                self._enqueue_log(f"自动保存凭证失败：{exc}")
            return

        self._last_saved_credentials = current
        if not silent and any(current[1:]) and not self._auto_save_notice_shown:
            self._enqueue_log(f"已自动保存 API 凭证到：{credentials_file_path().name}")
            self._auto_save_notice_shown = True
        self._sync_credential_profile_combo()
        self._update_settings_summary()

    def _next_api_profile_name(self) -> str:
        used = set(self._credential_profile_names())
        index = 1
        while True:
            candidate = f"api{index}"
            if candidate not in used:
                return candidate
            index += 1

    def _on_api_profile_selected(self, *_: object) -> None:
        selected = self.api_profile_name.get().strip()
        if not selected:
            return
        if selected == self._loaded_credential_profile_name:
            return
        self._save_credentials_now(silent=True)
        if selected not in self._credential_profiles:
            self._credential_profiles[selected] = {"api_key": "", "secret_key": "", "passphrase": ""}
            save_credentials_profiles_snapshot(
                selected_profile=selected,
                profiles=self._credential_profiles,
            )
        self._apply_credentials_profile(selected, log_change=True)

    def _create_api_profile(self) -> None:
        self._save_credentials_now(silent=True)
        profile_name = self._next_api_profile_name()
        self._credential_profiles[profile_name] = {"api_key": "", "secret_key": "", "passphrase": ""}
        save_credentials_profiles_snapshot(
            selected_profile=profile_name,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(profile_name, log_change=True)
        self._enqueue_log(f"已新增 API 配置：{profile_name}")

    def _rename_current_api_profile(self) -> None:
        current_name = self._editing_credential_profile()
        new_name = simpledialog.askstring(
            "重命名 API 配置",
            "请输入新的 API 配置名称：",
            initialvalue=current_name,
            parent=self._settings_window or self.root,
        )
        if new_name is None:
            return

        target_name = new_name.strip()
        if not target_name:
            messagebox.showerror("重命名失败", "API 配置名称不能为空。", parent=self._settings_window or self.root)
            return
        if target_name == current_name:
            return
        if target_name in self._credential_profiles:
            messagebox.showerror(
                "重命名失败",
                f"API 配置 {target_name} 已存在，请换一个名字。",
                parent=self._settings_window or self.root,
            )
            return

        self._save_credentials_now(silent=True)
        profiles = dict(self._credential_profiles)
        profile_payload = profiles.pop(current_name, {"api_key": "", "secret_key": "", "passphrase": ""})
        profiles[target_name] = profile_payload
        self._credential_profiles = profiles
        save_credentials_profiles_snapshot(
            selected_profile=target_name,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(target_name, log_change=True)
        self._enqueue_log(f"已将 API 配置 {current_name} 重命名为：{target_name}")

    def _delete_current_api_profile(self) -> None:
        profile_name = self._editing_credential_profile()
        if not messagebox.askyesno(
            "删除确认",
            f"确认删除 API 配置 {profile_name} 吗？",
            parent=self._settings_window or self.root,
        ):
            return

        profiles = dict(self._credential_profiles)
        profiles.pop(profile_name, None)
        if not profiles:
            next_profile = DEFAULT_CREDENTIAL_PROFILE_NAME
            profiles[next_profile] = {"api_key": "", "secret_key": "", "passphrase": ""}
        else:
            next_profile = sorted(profiles.keys())[0]

        self._credential_profiles = profiles
        save_credentials_profiles_snapshot(
            selected_profile=next_profile,
            profiles=self._credential_profiles,
        )
        self._apply_credentials_profile(next_profile, log_change=True)
        self._enqueue_log(f"已删除 API 配置：{profile_name}")

    def _save_notification_settings_now(self, silent: bool = False) -> None:
        if self._settings_save_job is not None:
            try:
                self.root.after_cancel(self._settings_save_job)
            except Exception:
                pass
            self._settings_save_job = None

        current = self._current_notification_state()
        if current == self._last_saved_notification_state:
            return

        try:
            port = self._parse_optional_port(self.smtp_port.get())
            save_notification_snapshot(
                environment_label=self.environment_label.get(),
                trade_mode_label=self.trade_mode_label.get(),
                position_mode_label=self.position_mode_label.get(),
                trigger_type_label=self.trigger_type_label.get(),
                enabled=self.notify_enabled.get(),
                smtp_host=self.smtp_host.get(),
                smtp_port=port,
                smtp_username=self.smtp_username.get(),
                smtp_password=self.smtp_password.get(),
                sender_email=self.sender_email.get(),
                recipient_emails=self.recipient_emails.get(),
                use_ssl=self.use_ssl.get(),
                notify_trade_fills=self.notify_trade_fills.get(),
                notify_signals=self.notify_signals.get(),
                notify_errors=self.notify_errors.get(),
            )
        except Exception as exc:
            if not silent:
                self._enqueue_log(f"保存通知设置失败：{exc}")
            return

        self._last_saved_notification_state = current

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
        merged = list(dict.fromkeys(self._default_symbol_values + symbols))
        self.symbol_combo["values"] = merged
        if self.symbol.get() not in symbols and symbols:
            self.symbol.set(merged[0])
        self._enqueue_log(f"已加载 {len(symbols)} 个可交易永续合约。")

    def start(self) -> None:
        try:
            definition = self._selected_strategy_definition()
            credentials, config = self._collect_inputs(definition)
            notifier = self._build_notifier(config)
            if not self._confirm_start(definition, config):
                return

            self._save_credentials_now(silent=True)
            self._save_notification_settings_now(silent=True)

            session_id = self._next_session_id()
            session_symbol = f"{config.inst_id} -> {config.trade_inst_id or config.inst_id}"
            api_name = credentials.profile_name or self._current_credential_profile()
            engine = StrategyEngine(
                self.client,
                self._make_session_logger(session_id, definition.name, session_symbol, api_name),
                notifier=notifier,
                strategy_name=definition.name,
            )
            session = StrategySession(
                session_id=session_id,
                api_name=api_name,
                strategy_id=definition.strategy_id,
                strategy_name=definition.name,
                symbol=session_symbol,
                direction_label=self.signal_mode_label.get(),
                run_mode_label=self.run_mode_label.get(),
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
            messagebox.showinfo("提示", "请先在右侧选择一个策略会话。")
            return
        if not session.engine.is_running:
            messagebox.showinfo("提示", "这个策略已经停止了。")
            return

        session.status = "停止中"
        session.engine.stop()
        self._upsert_session_row(session)
        self._refresh_selected_session_details()
        self._enqueue_log(f"{session.log_prefix} 已请求停止。")

    def debug_hourly_values(self) -> None:
        symbol = _normalize_symbol_input(self.symbol.get())
        if not symbol:
            messagebox.showerror("提示", "请先选择信号标的")
            return
        ema_period = self._parse_positive_int(self.ema_period.get(), "EMA小周期")
        self._enqueue_log(f"正在获取 {symbol} 的 1 小时调试值，EMA小周期={ema_period} ...")
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

    def refresh_positions(self) -> None:
        if self._positions_refreshing:
            return

        credentials = self._current_credentials_or_none()
        if credentials is None:
            self._apply_positions([], "未配置 API 凭证，无法读取持仓。")
            return

        self._positions_refreshing = True
        self.positions_summary_text.set("正在刷新账户持仓...")
        environment = ENV_OPTIONS[self.environment_label.get()]
        threading.Thread(
            target=self._refresh_positions_worker,
            args=(credentials, environment),
            daemon=True,
        ).start()

    def _refresh_positions_worker(self, credentials: Credentials, environment: str) -> None:
        try:
            positions = self.client.get_positions(credentials, environment=environment)
            upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
            position_instruments = _build_position_instrument_map(self.client, positions)
            position_tickers = _build_position_ticker_map(self.client, positions)
        except Exception as exc:
            message = str(exc)
            if "50101" in message and "current environment" in message:
                alternate = "live" if environment == "demo" else "demo"
                try:
                    positions = self.client.get_positions(credentials, environment=alternate)
                    upl_usdt_prices = _build_upl_usdt_price_map(self.client, positions)
                    position_instruments = _build_position_instrument_map(self.client, positions)
                    position_tickers = _build_position_ticker_map(self.client, positions)
                except Exception:
                    self.root.after(0, lambda: self._apply_positions_error(message))
                    return
                summary = (
                    f"当前 API Key 与 {alternate} 环境匹配，已自动按 "
                    f"{'实盘' if alternate == 'live' else '模拟盘'} 读取持仓。"
                )
                self.root.after(
                    0,
                    lambda: self._apply_positions(
                        positions,
                        summary,
                        alternate,
                        upl_usdt_prices,
                        position_instruments,
                        position_tickers,
                    ),
                )
                return
            self.root.after(0, lambda: self._apply_positions_error(message))
            return
        self.root.after(
            0,
            lambda: self._apply_positions(
                positions,
                None,
                environment,
                upl_usdt_prices,
                position_instruments,
                position_tickers,
            ),
        )

    def _apply_positions(
        self,
        positions: list[OkxPosition],
        summary: str | None = None,
        effective_environment: str | None = None,
        upl_usdt_prices: dict[str, Decimal] | None = None,
        position_instruments: dict[str, Instrument] | None = None,
        position_tickers: dict[str, OkxTicker] | None = None,
    ) -> None:
        self._positions_refreshing = False
        self._latest_positions = list(positions)
        self._positions_context_note = summary
        self._positions_last_refresh_at = datetime.now()
        self._positions_effective_environment = effective_environment
        self._upl_usdt_prices = dict(upl_usdt_prices or {})
        self._position_instruments = dict(position_instruments or {})
        self._position_tickers = dict(position_tickers or {})
        self._render_positions_view()

    def _render_positions_view(self) -> None:
        selected_before = self.position_tree.selection()[0] if self.position_tree.selection() else None
        selected_payload = self._selected_position_payload()
        selected_position_key = None
        if selected_payload is not None and selected_payload["kind"] == "position":
            item = selected_payload["item"]
            if isinstance(item, OkxPosition):
                selected_position_key = _position_tree_row_id(item)

        self._positions_view_rendering = True
        try:
            self.position_tree.delete(*self.position_tree.get_children())
            self._position_row_payloads.clear()
            visible_positions = _filter_positions(
                self._latest_positions,
                inst_type=POSITION_TYPE_OPTIONS[self.position_type_filter.get()],
                keyword=self.position_keyword.get(),
            )
            groups = _group_positions_for_tree(visible_positions)
            for asset_label, buckets in groups.items():
                asset_id = _asset_group_row_id(asset_label)
                asset_positions = [item for bucket in buckets.values() for item in bucket]
                asset_metrics = _aggregate_position_metrics(asset_positions, self._upl_usdt_prices, self._position_instruments)
                asset_label_text = f"{asset_label} 风险单元"
                self.position_tree.insert(
                    "",
                    END,
                    iid=asset_id,
                    text=asset_label_text,
                    values=_build_group_row_values("组合", asset_metrics),
                    open=True,
                    tags=("group", _pnl_tag(asset_metrics["upl"])),
                )
                self._position_row_payloads[asset_id] = {
                    "kind": "group",
                    "label": asset_label_text,
                    "item": asset_positions,
                    "metrics": asset_metrics,
                }

                for bucket_label, bucket_positions in buckets.items():
                    if bucket_label == "__DIRECT__":
                        for position in bucket_positions:
                            self._insert_position_row(asset_id, position, _position_tree_row_id(position))
                        continue

                    bucket_id = _bucket_group_row_id(asset_label, bucket_label)
                    bucket_metrics = _aggregate_position_metrics(
                        bucket_positions,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                    self.position_tree.insert(
                        asset_id,
                        END,
                        iid=bucket_id,
                        text=bucket_label,
                        values=_build_group_row_values("分组", bucket_metrics),
                        open=True,
                        tags=("group", _pnl_tag(bucket_metrics["upl"])),
                    )
                    self._position_row_payloads[bucket_id] = {
                        "kind": "group",
                        "label": bucket_label,
                        "item": bucket_positions,
                        "metrics": bucket_metrics,
                    }
                    for position in bucket_positions:
                        self._insert_position_row(bucket_id, position, _position_tree_row_id(position))

            self._update_position_summary(visible_positions)
            self._update_position_metrics(visible_positions)

            target = _resolve_position_selection_target(
                existing_ids=set(self._position_row_payloads.keys()),
                selected_position_key=selected_position_key,
                protection_position_key=self._protection_form_position_key,
                selected_before=selected_before,
                top_items=self.position_tree.get_children(),
            )

            if target is not None:
                self.position_tree.selection_set(target)
                self.position_tree.focus(target)
        finally:
            self._positions_view_rendering = False
        self._refresh_position_detail_panel()
        self._sync_positions_zoom_window()
        self._refresh_protection_window_view()

    def _insert_position_row(self, parent_id: str, position: OkxPosition, row_id: str) -> None:
        label = position.inst_id
        if position.pos_side and position.pos_side.lower() != "net":
            label = f"{label} [{position.pos_side}]"
        tags = [tag for tag in (_pnl_tag(position.unrealized_pnl), _margin_mode_tag(position.mgn_mode)) if tag]
        self.position_tree.insert(
            parent_id,
            END,
            iid=row_id,
            text=label,
            values=(
                position.inst_type,
                _format_margin_mode(position.mgn_mode),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="time_value"),
                _format_position_option_price_component(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_option_component_usdt(position, self._upl_usdt_prices, component="intrinsic_value"),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="bid",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="bid",
                ),
                _format_position_quote_price(
                    position,
                    self._position_instruments,
                    self._position_tickers,
                    side="ask",
                ),
                _format_position_quote_price_usdt(
                    position,
                    self._position_tickers,
                    self._upl_usdt_prices,
                    side="ask",
                ),
                _format_mark_price(position),
                _format_position_mark_price_usdt(position, self._upl_usdt_prices),
                _format_position_avg_price(position, self._position_instruments),
                _format_position_avg_price_usdt(position, self._upl_usdt_prices),
                _format_position_size(position, self._position_instruments),
                _format_option_trade_side_display(position),
                _format_position_unrealized_pnl(position),
                _format_optional_usdt(_position_unrealized_pnl_usdt(position, self._upl_usdt_prices)),
                _format_optional_decimal_fixed(position.realized_pnl, places=5, with_sign=True),
                _format_position_market_value(position, self._position_instruments, self._upl_usdt_prices),
                _format_optional_decimal(position.liquidation_price),
                _format_ratio(position.margin_ratio, places=2),
                _format_optional_integer(position.initial_margin),
                _format_optional_integer(position.maintenance_margin),
                _format_optional_decimal_fixed(_position_delta_value(position, self._position_instruments), places=5),
                _format_optional_decimal_fixed(position.gamma, places=5),
                _format_optional_decimal_fixed(position.vega, places=5),
                _format_optional_decimal_fixed(position.theta, places=5),
                _format_optional_usdt_precise(_position_theta_usdt(position, self._upl_usdt_prices), places=2),
            ),
            tags=tuple(tags),
        )
        self._position_row_payloads[row_id] = {
            "kind": "position",
            "label": label,
            "item": position,
            "metrics": None,
        }

    def _update_position_summary(self, visible_positions: list[OkxPosition]) -> None:
        timestamp = self._positions_last_refresh_at.strftime("%H:%M:%S") if self._positions_last_refresh_at else "--:--:--"
        parts: list[str] = []
        if self._positions_context_note:
            parts.append(self._positions_context_note)
        parts.append(f"API配置：{self._current_credential_profile()}")

        total_count = len(self._latest_positions)
        visible_count = len(visible_positions)
        if total_count:
            summary = f"当前仓位（{total_count}）"
            if visible_count != total_count:
                summary += f"，当前显示 {visible_count}"
            parts.append(summary)
        else:
            parts.append("当前没有持仓")

        filter_text = _format_position_filter_summary(
            self.position_type_filter.get(),
            self.position_keyword.get(),
        )
        if filter_text:
            parts.append(f"筛选：{filter_text}")
        if self.position_auto_refresh_enabled:
            parts.append(f"自动刷新：{self.position_refresh_interval_label.get()}")
        else:
            parts.append("自动刷新：已暂停")
        parts.append(f"最近刷新：{timestamp}")
        self.positions_summary_text.set(" | ".join(parts))

    def _update_position_metrics(self, visible_positions: list[OkxPosition]) -> None:
        metrics = _aggregate_position_metrics(visible_positions, self._upl_usdt_prices, self._position_instruments)
        visible_count = len(visible_positions)
        total_count = len(self._latest_positions)
        self.position_total_text.set(
            f"{visible_count} 笔" if visible_count == total_count else f"{visible_count} / {total_count} 笔"
        )
        self.position_upl_text.set(
            _format_optional_usdt(metrics["upl_usdt"] if isinstance(metrics["upl_usdt"], Decimal) else None)
        )
        self.position_realized_text.set(
            _format_optional_decimal_fixed(
                metrics["realized"] if isinstance(metrics["realized"], Decimal) else None,
                places=2,
                with_sign=True,
            )
        )
        self.position_margin_text.set(
            _format_optional_integer(metrics["imr"] if isinstance(metrics["imr"], Decimal) else None)
        )
        self.position_delta_text.set(
            _format_summary_delta(metrics["delta"] if isinstance(metrics["delta"], Decimal) else None)
        )
        self.position_short_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="short",
            )
        )
        self.position_short_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="short",
            )
        )
        self.position_long_call_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="C",
                direction="long",
            )
        )
        self.position_long_put_text.set(
            _format_filtered_option_position_size(
                visible_positions,
                self._position_instruments,
                option_kind="P",
                direction="long",
            )
        )

    def _selected_position_payload(self) -> dict[str, object] | None:
        selection = self.position_tree.selection()
        if not selection:
            return None
        return self._position_row_payloads.get(selection[0])

    def _on_position_selected(self, *_: object) -> None:
        if self._position_selection_syncing or self._positions_view_rendering:
            return
        self._refresh_position_detail_panel()

    def _refresh_position_detail_panel(self) -> None:
        payload = self._selected_position_payload()
        if payload is None:
            self.position_detail_text.set(self._default_position_detail_text())
        elif payload["kind"] == "position":
            position = payload["item"]
            if isinstance(position, OkxPosition):
                self.position_detail_text.set(
                    _build_position_detail_text(position, self._upl_usdt_prices, self._position_instruments)
                )
        else:
            label = payload["label"]
            positions = payload["item"]
            metrics = payload["metrics"]
            if isinstance(label, str) and isinstance(positions, list) and isinstance(metrics, dict):
                self.position_detail_text.set(
                    _build_group_detail_text(
                        label,
                        positions,
                        metrics,
                        self._upl_usdt_prices,
                        self._position_instruments,
                    )
                )
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        if self._positions_zoom_tree is not None:
            selection = self.position_tree.selection()
            if selection and self._positions_zoom_tree.exists(selection[0]):
                self._position_selection_syncing = True
                try:
                    self._positions_zoom_tree.selection_set(selection[0])
                    self._positions_zoom_tree.focus(selection[0])
                finally:
                    self._position_selection_syncing = False
        self._refresh_positions_zoom_detail()
        self._refresh_protection_window_view()

    def _apply_positions_error(self, message: str) -> None:
        self._positions_refreshing = False
        self._latest_positions = []
        self._positions_context_note = None
        self._positions_last_refresh_at = None
        self._positions_effective_environment = None
        self._upl_usdt_prices = {}
        self._position_tickers = {}
        self.position_tree.delete(*self.position_tree.get_children())
        self._position_row_payloads.clear()
        self.positions_summary_text.set(f"持仓读取失败：{message}")
        self.position_total_text.set("-")
        self.position_upl_text.set("-")
        self.position_realized_text.set("-")
        self.position_margin_text.set("-")
        self.position_delta_text.set("-")
        self.position_short_call_text.set("-")
        self.position_short_put_text.set("-")
        self.position_long_call_text.set("-")
        self.position_long_put_text.set("-")
        self.position_detail_text.set(self._default_position_detail_text())
        self._set_readonly_text(self._position_detail_panel, self.position_detail_text.get())
        self._sync_positions_zoom_window()
        self._refresh_positions_zoom_detail()
        self._enqueue_log(f"持仓读取失败：{message}")

    def _refresh_positions_periodic(self) -> None:
        if self.position_auto_refresh_enabled:
            self.refresh_positions()
        self.root.after(self._position_refresh_interval_ms(), self._refresh_positions_periodic)

    def _on_strategy_selected(self, *_: object) -> None:
        self._apply_selected_strategy_definition()

    def _apply_selected_strategy_definition(self) -> None:
        definition = self._selected_strategy_definition()
        self.signal_combo["values"] = definition.allowed_signal_labels
        if self.signal_mode_label.get() not in definition.allowed_signal_labels:
            self.signal_mode_label.set(definition.default_signal_label)
        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            self.bar.set("4H")
            self.ema_period.set("5")
            self.trend_ema_period.set("8")
            self.big_ema_period.set("233")
            self.risk_amount.set("100")
            self.entry_side_mode_label.set("跟随信号")
            self.tp_sl_mode_label.set("按信号标的的价格（本地）")
        self.strategy_summary_text.set(definition.summary)
        self.strategy_rule_text.set(definition.rule_description)
        self.strategy_hint_text.set(definition.parameter_hint)

    def _selected_strategy_definition(self) -> StrategyDefinition:
        strategy_id = self._strategy_name_to_id[self.strategy_name.get()]
        return get_strategy_definition(strategy_id)

    def _confirm_start(self, definition: StrategyDefinition, config: StrategyConfig) -> bool:
        trade_symbol = config.trade_inst_id or config.inst_id
        risk_value = self.risk_amount.get().strip() or "-"
        fixed_size = self.order_size.get().strip() or "-"
        message = (
            f"策略：{definition.name}\n"
            f"运行模式：{self.run_mode_label.get()}\n"
            f"信号标的：{config.inst_id}\n"
            f"下单标的：{trade_symbol}\n"
            f"K线周期：{config.bar}\n"
            f"信号方向：{self.signal_mode_label.get()}\n"
            f"下单方向模式：{self.entry_side_mode_label.get()}\n"
            f"止盈止损模式：{self.tp_sl_mode_label.get()}\n"
            f"自定义触发标的：{self.local_tp_sl_symbol.get().strip().upper() or '-'}\n"
            f"EMA小周期：{config.ema_period}\n"
            f"EMA中周期：{config.trend_ema_period}\n"
            f"EMA大周期：{config.big_ema_period}\n"
            f"ATR 周期：{config.atr_period}\n"
            f"风险金：{risk_value}\n"
            f"固定数量：{fixed_size}\n\n"
            f"{definition.rule_description}\n\n"
            "确认启动这个策略吗？"
        )
        return messagebox.askokcancel(f"确认启动 {definition.name}", message)

    def _collect_inputs(self, definition: StrategyDefinition) -> tuple[Credentials, StrategyConfig]:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        symbol = _normalize_symbol_input(self.symbol.get())
        trade_symbol = _normalize_symbol_input(self.trade_symbol.get()) or symbol
        local_tp_sl_symbol = _normalize_symbol_input(self.local_tp_sl_symbol.get()) or None
        tp_sl_mode = TP_SL_MODE_OPTIONS[self.tp_sl_mode_label.get()]
        run_mode = RUN_MODE_OPTIONS[self.run_mode_label.get()]
        risk_amount = self._parse_optional_positive_decimal(self.risk_amount.get(), "风险金")
        order_size = self._parse_optional_positive_decimal(self.order_size.get(), "固定数量") or Decimal("0")

        if not api_key or not secret_key or not passphrase:
            raise ValueError("请先在 菜单 > 设置 > API 与通知设置 中填写 API 凭证")
        if not symbol:
            raise ValueError("请选择信号标的")
        if run_mode == "trade":
            if tp_sl_mode == "exchange":
                if trade_symbol != symbol:
                    raise ValueError("OKX 托管止盈止损只支持信号标的和下单标的相同")
                if infer_inst_type(trade_symbol) != "SWAP":
                    raise ValueError("OKX 托管止盈止损当前只支持永续合约")
            if tp_sl_mode == "local_custom" and not local_tp_sl_symbol:
                raise ValueError("已选择自定义本地止盈止损，请填写触发标的")
            if risk_amount is None and order_size <= 0:
                raise ValueError("交易并下单模式下，风险金和固定数量至少填写一个")

        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if run_mode == "signal_only":
            if not notification_config.enabled:
                raise ValueError("只发信号邮件模式需要先在设置里启用邮件通知")
            if not notification_config.notify_signals:
                raise ValueError("只发信号邮件模式需要勾选“信号邮件”")

        if definition.strategy_id == STRATEGY_EMA5_EMA8_ID:
            trade_symbol = symbol
            local_tp_sl_symbol = symbol
            tp_sl_mode = "local_signal"
            risk_amount = Decimal("100")
            order_size = Decimal("0")

        credentials = Credentials(api_key=api_key, secret_key=secret_key, passphrase=passphrase)
        config = StrategyConfig(
            inst_id=symbol,
            bar="4H" if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self.bar.get(),
            ema_period=5 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.ema_period.get(), "EMA小周期"),
            trend_ema_period=8 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.trend_ema_period.get(), "EMA中周期"),
            big_ema_period=233 if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else self._parse_positive_int(self.big_ema_period.get(), "EMA大周期"),
            atr_period=self._parse_positive_int(self.atr_period.get(), "ATR 周期"),
            atr_stop_multiplier=self._parse_positive_decimal(self.stop_atr.get(), "止损 ATR 倍数"),
            atr_take_multiplier=self._parse_positive_decimal(self.take_atr.get(), "止盈 ATR 倍数"),
            order_size=order_size,
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            signal_mode=SIGNAL_LABEL_TO_VALUE[self.signal_mode_label.get()],
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            environment=ENV_OPTIONS[self.environment_label.get()],
            tp_sl_trigger_type=TRIGGER_TYPE_OPTIONS[self.trigger_type_label.get()],
            strategy_id=definition.strategy_id,
            poll_seconds=float(self._parse_positive_decimal(self.poll_seconds.get(), "轮询秒数")),
            risk_amount=risk_amount,
            trade_inst_id=trade_symbol,
            tp_sl_mode=tp_sl_mode,
            local_tp_sl_inst_id=local_tp_sl_symbol,
            entry_side_mode="follow_signal" if definition.strategy_id == STRATEGY_EMA5_EMA8_ID else ENTRY_SIDE_MODE_OPTIONS[self.entry_side_mode_label.get()],
            run_mode=run_mode,
        )
        return credentials, config

    def _collect_notification_config(self, *, validate_if_enabled: bool) -> EmailNotificationConfig:
        smtp_port = self._parse_optional_port(self.smtp_port.get())
        recipients = tuple(self._split_recipients(self.recipient_emails.get()))
        config = EmailNotificationConfig(
            enabled=self.notify_enabled.get(),
            smtp_host=self.smtp_host.get().strip(),
            smtp_port=smtp_port,
            smtp_username=self.smtp_username.get().strip(),
            smtp_password=self.smtp_password.get(),
            sender_email=self.sender_email.get().strip(),
            recipient_emails=recipients,
            use_ssl=self.use_ssl.get(),
            notify_trade_fills=self.notify_trade_fills.get(),
            notify_signals=self.notify_signals.get(),
            notify_errors=self.notify_errors.get(),
        )
        if validate_if_enabled and config.enabled:
            if not config.smtp_host:
                raise ValueError("已启用邮件通知，请填写 SMTP 主机")
            if not recipients:
                raise ValueError("已启用邮件通知，请填写至少一个收件邮箱")
            if not (config.sender_email or config.smtp_username):
                raise ValueError("已启用邮件通知，请填写发件邮箱或 SMTP 用户名")
        return config

    def _build_notifier(self, config: StrategyConfig) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(
            notification_config,
            logger=self._make_system_logger(f"邮件 {config.strategy_id}"),
        )

    def _build_signal_monitor_notifier(self) -> EmailNotifier | None:
        notification_config = self._collect_notification_config(validate_if_enabled=True)
        if not notification_config.enabled:
            return None
        return EmailNotifier(notification_config, logger=self._make_system_logger("邮件 信号监控"))

    def send_test_email(self) -> None:
        try:
            notifier = self._build_signal_monitor_notifier()
        except Exception as exc:
            messagebox.showerror("测试邮件失败", str(exc), parent=self._settings_window or self.root)
            return

        if notifier is None:
            messagebox.showinfo("提示", "当前未启用邮件通知。", parent=self._settings_window or self.root)
            return

        subject = f"[QQOKX] 测试邮件 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        body = "\n".join(
            [
                "这是一封来自 QQOKX 的测试邮件。",
                f"当前环境：{self.environment_label.get()}",
                f"发送时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            ]
        )
        notifier.notify_async(subject, body)
        self._enqueue_log("已提交测试邮件发送请求。")
        messagebox.showinfo("提示", "测试邮件已提交，请检查收件箱。", parent=self._settings_window or self.root)

    def _update_settings_summary(self) -> None:
        api_status = "已配置" if all(
            [self.api_key.get().strip(), self.secret_key.get().strip(), self.passphrase.get().strip()]
        ) else "未配置"
        mail_status = "邮件已启用" if self.notify_enabled.get() else "邮件未启用"
        self.settings_summary_text.set(
            f"{api_status} | {mail_status} | {self.environment_label.get()} | {self.trade_mode_label.get()} | "
            f"{self.position_mode_label.get()}"
        )

    def _make_session_logger(self, session_id: str, strategy_name: str, symbol: str, api_name: str = ""):
        prefix = f"[{api_name}] [{session_id} {strategy_name} {symbol}]" if api_name else f"[{session_id} {strategy_name} {symbol}]"

        def _logger(message: str) -> None:
            self._enqueue_log(f"{prefix} {message}")

        return _logger

    def _make_system_logger(self, name: str):
        prefix = f"[{name}]"

        def _logger(message: str) -> None:
            self._enqueue_log(f"{prefix} {message}")

        return _logger

    def _next_session_id(self) -> str:
        self._session_counter += 1
        return f"S{self._session_counter:02d}"

    def _upsert_session_row(self, session: StrategySession) -> None:
        values = (
            session.api_name or "-",
            session.strategy_name,
            session.symbol,
            session.direction_label,
            session.run_mode_label,
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
            self.selected_session_text.set(self._default_selected_session_text())
            self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())
            return

        definition = get_strategy_definition(session.strategy_id)
        self.selected_session_text.set(
            f"会话：{session.session_id}\n"
            f"API配置：{session.api_name or '-'}\n"
            f"状态：{session.status}\n"
            f"策略：{session.strategy_name}\n"
            f"运行模式：{session.run_mode_label}\n"
            f"信号标的：{session.config.inst_id}\n"
            f"下单标的：{session.config.trade_inst_id or session.config.inst_id}\n"
            f"方向：{session.direction_label}\n"
            f"K线周期：{session.config.bar}\n"
            f"EMA小周期：{session.config.ema_period}\n"
            f"EMA中周期：{session.config.trend_ema_period}\n"
            f"EMA大周期：{session.config.big_ema_period}\n"
            f"ATR 周期：{session.config.atr_period}\n"
            f"止损 ATR 倍数：{session.config.atr_stop_multiplier}\n"
            f"止盈 ATR 倍数：{session.config.atr_take_multiplier}\n"
            f"风险金：{session.config.risk_amount}\n"
            f"固定数量：{session.config.order_size}\n"
            f"下单方向模式：{session.config.entry_side_mode}\n"
            f"止盈止损模式：{session.config.tp_sl_mode}\n"
            f"自定义触发标的：{session.config.local_tp_sl_inst_id or '-'}\n"
            f"轮询秒数：{session.config.poll_seconds}\n"
            f"启动时间：{session.started_at.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"策略简介：{definition.summary}\n\n"
            f"规则说明：{definition.rule_description}\n\n"
            f"参数提示：{definition.parameter_hint}"
        )
        self._set_readonly_text(self._selected_session_detail, self.selected_session_text.get())

    def _parse_positive_int(self, raw: str, field_name: str) -> int:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{field_name} 不是有效整数") from exc
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

    def _parse_optional_positive_decimal(self, raw: str, field_name: str) -> Decimal | None:
        cleaned = raw.strip()
        if not cleaned:
            return None
        return self._parse_positive_decimal(cleaned, field_name)

    def _parse_optional_port(self, raw: str) -> int:
        cleaned = raw.strip()
        if not cleaned:
            return 465
        value = int(cleaned)
        if value <= 0:
            raise ValueError("SMTP 端口必须大于 0")
        return value

    def _split_recipients(self, raw: str) -> list[str]:
        return [item.strip() for item in re.split(r"[,\n;]+", raw) if item.strip()]

    def _current_credentials_or_none(self) -> Credentials | None:
        api_key = self.api_key.get().strip()
        secret_key = self.secret_key.get().strip()
        passphrase = self.passphrase.get().strip()
        if not api_key or not secret_key or not passphrase:
            return None
        return Credentials(
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            profile_name=self._current_credential_profile(),
        )

    def _build_smart_order_runtime_config_or_none(self) -> SmartOrderRuntimeConfig | None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            return None
        return SmartOrderRuntimeConfig(
            credentials=credentials,
            environment=ENV_OPTIONS[self.environment_label.get()],
            trade_mode=TRADE_MODE_OPTIONS[self.trade_mode_label.get()],
            position_mode=POSITION_MODE_OPTIONS[self.position_mode_label.get()],
            credential_profile_name=self._loaded_credential_profile_name,
        )

    def _build_option_strategy_runtime_or_none(self) -> tuple[Credentials, str] | None:
        credentials = self._current_credentials_or_none()
        if credentials is None:
            return None
        environment = self._positions_effective_environment or ENV_OPTIONS[self.environment_label.get()]
        return credentials, environment

    def _current_notification_state(self) -> tuple[object, ...]:
        return (
            self.environment_label.get(),
            self.trade_mode_label.get(),
            self.position_mode_label.get(),
            self.trigger_type_label.get(),
            self.notify_enabled.get(),
            self.smtp_host.get().strip(),
            self.smtp_port.get().strip(),
            self.smtp_username.get().strip(),
            self.smtp_password.get(),
            self.sender_email.get().strip(),
            self.recipient_emails.get().strip(),
            self.use_ssl.get(),
            self.notify_trade_fills.get(),
            self.notify_signals.get(),
            self.notify_errors.get(),
        )

    def _enqueue_log(self, message: str) -> None:
        self.log_queue.put(append_log_line(message))

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
        self._save_notification_settings_now(silent=True)
        for session in self.sessions.values():
            session.engine.stop()
        self._protection_manager.stop_all()
        self._close_settings_window()
        if self._backtest_window is not None and self._backtest_window.window.winfo_exists():
            self._backtest_window.window.destroy()
        if self._backtest_compare_window is not None and self._backtest_compare_window.window.winfo_exists():
            self._backtest_compare_window.window.destroy()
        if self._signal_monitor_window is not None and self._signal_monitor_window.window.winfo_exists():
            self._signal_monitor_window.destroy()
        if self._smart_order_window is not None and self._smart_order_window.window.winfo_exists():
            self._smart_order_window.destroy()
        if (
            self._deribit_volatility_monitor_window is not None
            and self._deribit_volatility_monitor_window.window.winfo_exists()
        ):
            self._deribit_volatility_monitor_window.destroy()
        if self._deribit_volatility_window is not None and self._deribit_volatility_window.window.winfo_exists():
            self._deribit_volatility_window.window.destroy()
        if self._option_strategy_window is not None and self._option_strategy_window.window.winfo_exists():
            self._option_strategy_window.window.destroy()
        if self._protection_replay_window is not None and self._protection_replay_window.window.winfo_exists():
            self._protection_replay_window.window.destroy()
        self._close_positions_zoom_window()
        self._close_position_protection_window()
        self.root.destroy()

def _format_optional_decimal(value: Decimal | None, *, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal(value)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_decimal_fixed(value: Decimal | None, *, places: int, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, places)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_summary_delta(value: Decimal | None) -> str:
    if value is None:
        return "-"
    magnitude = abs(value)
    if magnitude >= Decimal("1000"):
        places = 2
    elif magnitude >= Decimal("1"):
        places = 4
    else:
        places = 5
    return _format_optional_decimal_fixed(value, places=places)


def _format_optional_integer(value: Decimal | None, *, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, 0)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_approx_usdt(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"≈{_format_optional_usdt(value, with_sign=False)} USDT"


def _format_optional_usdt(value: Decimal | None, *, with_sign: bool = True) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, 0)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_optional_usdt_precise(value: Decimal | None, *, places: int = 2, with_sign: bool = True) -> str:
    if value is None:
        return "-"
    text = format_decimal_fixed(value, places)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_ratio(value: Decimal | None, *, places: int = 2) -> str:
    if value is None:
        return "-"
    return f"{format_decimal_fixed(value * Decimal('100'), places)}%"


def _format_margin_mode(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "-"
    if text == "isolated":
        return "逐仓 isolated"
    if text == "cross":
        return "全仓 cross"
    return text


def _format_account_level(value: str | None) -> str:
    mapping = {
        "1": "简单交易",
        "2": "单币种保证金",
        "3": "跨币种保证金",
        "4": "组合保证金",
    }
    text = (value or "").strip()
    if not text:
        return "-"
    return mapping.get(text, text)


def _format_account_position_mode(value: str | None) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "-"
    if text == "net":
        return "净持仓 net"
    if text in {"long_short", "long/short"}:
        return "双向持仓 long/short"
    return text


def _format_greeks_type(value: str | None) -> str:
    text = (value or "").strip().upper()
    if not text:
        return "-"
    if text in {"PA", "BS"}:
        return text
    return text


def _margin_mode_tag(value: str | None) -> str | None:
    text = (value or "").strip().lower()
    if text == "isolated":
        return "isolated_mode"
    if text == "cross":
        return "cross_mode"
    return None


def _format_pos_side(pos_side: str, position: Decimal) -> str:
    if pos_side and pos_side.lower() != "net":
        return pos_side
    if position > 0:
        return "long"
    if position < 0:
        return "short"
    return pos_side or "-"


def _normalize_symbol_input(raw: str) -> str:
    cleaned = raw.strip().upper()
    if not cleaned:
        return ""
    if "-" in cleaned:
        return cleaned
    if cleaned.endswith("USDT") and len(cleaned) > 4:
        return f"{cleaned[:-4]}-USDT-SWAP"
    return cleaned


def _extract_asset_key(inst_id: str) -> str:
    return inst_id.split("-")[0] if inst_id else "UNKNOWN"


def _extract_quote_key(inst_id: str) -> str | None:
    parts = inst_id.split("-")
    if len(parts) >= 2 and parts[1]:
        return parts[1].upper()
    return None


def _extract_bucket_key(position: OkxPosition) -> str:
    parts = position.inst_id.split("-")
    if position.inst_type == "OPTION" and len(parts) >= 3:
        return parts[2]
    if position.inst_type == "FUTURES" and len(parts) >= 3:
        return parts[2]
    return "__DIRECT__"


def _group_positions_for_tree(positions: list[OkxPosition]) -> dict[str, dict[str, list[OkxPosition]]]:
    grouped: dict[str, dict[str, list[OkxPosition]]] = {}
    for position in positions:
        asset_key = _extract_asset_key(position.inst_id)
        bucket_key = _extract_bucket_key(position)
        grouped.setdefault(asset_key, {}).setdefault(bucket_key, []).append(position)
    ordered: dict[str, dict[str, list[OkxPosition]]] = {}
    for asset_key, buckets in sorted(grouped.items(), key=lambda item: item[0]):
        ordered[asset_key] = dict(
            sorted(
                (
                    (
                        bucket_key,
                        sorted(bucket_positions, key=_position_bucket_sort_key),
                    )
                    for bucket_key, bucket_positions in buckets.items()
                ),
                key=lambda item: _bucket_sort_key(item[0]),
            )
        )
    return ordered


def _bucket_sort_key(bucket_key: str) -> tuple[int, int | str]:
    if bucket_key.isdigit():
        return (0, int(bucket_key))
    if bucket_key == "__DIRECT__":
        return (2, bucket_key)
    return (1, bucket_key)


def _position_bucket_sort_key(position: OkxPosition) -> tuple[int, int, int, str]:
    if position.inst_type == "OPTION":
        strike, option_side = _extract_option_sort_components(position.inst_id)
        option_side_rank = 0 if option_side == "C" else 1 if option_side == "P" else 2
        return (0, strike, option_side_rank, position.inst_id)
    if position.inst_type == "FUTURES":
        return (2, 0, 0, position.inst_id)
    return (1, 0, 0, position.inst_id)


def _extract_option_sort_components(inst_id: str) -> tuple[int, str]:
    parts = inst_id.split("-")
    if len(parts) >= 5:
        try:
            strike = int(parts[3])
        except ValueError:
            strike = 10**9
        return strike, parts[4].upper()
    return 10**9, ""


def _option_search_shortcuts(inst_id: str) -> tuple[str, str]:
    normalized = inst_id.strip().upper()
    if not normalized or infer_option_family(normalized) is None:
        return "", ""
    parts = normalized.split("-")
    if len(parts) < 3:
        return normalized, normalized
    return normalized, f"{parts[0]}-{parts[1]}-{parts[2]}-"


def _advance_fill_history_limit(current_limit: int, load_more_clicks: int) -> tuple[int, int, str]:
    increment = 100 if load_more_clicks == 0 else 200
    return current_limit + increment, load_more_clicks + 1, "增加200条"


def _aggregate_position_metrics(
    positions: list[OkxPosition],
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
) -> dict[str, Decimal | int | None]:
    def _sum_decimal(values: list[Decimal | None]) -> Decimal | None:
        decimals = [value for value in values if value is not None]
        if not decimals:
            return None
        return sum(decimals, Decimal("0"))

    pnl_currencies = sorted(
        {
            _infer_upl_currency(item)
            for item in positions
            if item.unrealized_pnl is not None or item.realized_pnl is not None
        }
    )
    pnl_currency: str | None = pnl_currencies[0] if len(pnl_currencies) == 1 else None
    return {
        "count": len(positions),
        "size_display": _format_group_position_size(positions, position_instruments),
        "option_side_display": _format_group_option_trade_side(positions, position_instruments),
        "upl": _sum_decimal([item.unrealized_pnl for item in positions]),
        "upl_usdt": _sum_decimal([_position_unrealized_pnl_usdt(item, upl_usdt_prices) for item in positions]),
        "market_value_usdt": _sum_decimal(
            [_position_market_value_usdt(item, position_instruments, upl_usdt_prices) for item in positions]
        ),
        "realized": _sum_decimal([item.realized_pnl for item in positions]),
        "pnl_currency": pnl_currency,
        "imr": _sum_decimal([item.initial_margin for item in positions]),
        "mmr": _sum_decimal([item.maintenance_margin for item in positions]),
        "delta": _sum_decimal([_position_delta_value(item, position_instruments) for item in positions]),
        "gamma": _sum_decimal([item.gamma for item in positions]),
        "vega": _sum_decimal([item.vega for item in positions]),
        "theta": _sum_decimal([item.theta for item in positions]),
        "theta_usdt": _sum_decimal([_position_theta_usdt(item, upl_usdt_prices) for item in positions]),
    }


def _build_group_row_values(group_type: str, metrics: dict[str, Decimal | int | None]) -> tuple[str, ...]:
    count = metrics["count"]
    pnl_places = _group_pnl_places(metrics.get("pnl_currency"))
    size_display = metrics.get("size_display")
    option_side_display = metrics.get("option_side_display")
    return (
        group_type,
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        "--",
        (
            f"{count} 个持仓 | {size_display}"
            if isinstance(count, int) and isinstance(size_display, str) and size_display
            else (f"{count} 个持仓" if isinstance(count, int) else "--")
        ),
        option_side_display if isinstance(option_side_display, str) and option_side_display else "--",
        _format_optional_decimal_fixed(
            metrics["upl"] if isinstance(metrics["upl"], Decimal) else None,
            places=pnl_places,
            with_sign=True,
        ),
        _format_optional_usdt(metrics["upl_usdt"] if isinstance(metrics["upl_usdt"], Decimal) else None),
        _format_optional_decimal_fixed(
            metrics["realized"] if isinstance(metrics["realized"], Decimal) else None,
            places=pnl_places,
            with_sign=True,
        ),
        _format_optional_approx_usdt(
            metrics["market_value_usdt"] if isinstance(metrics["market_value_usdt"], Decimal) else None
        ),
        "--",
        "--",
        _format_optional_integer(metrics["imr"] if isinstance(metrics["imr"], Decimal) else None),
        _format_optional_integer(metrics["mmr"] if isinstance(metrics["mmr"], Decimal) else None),
        _format_optional_decimal_fixed(metrics["delta"] if isinstance(metrics["delta"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["gamma"] if isinstance(metrics["gamma"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["vega"] if isinstance(metrics["vega"], Decimal) else None, places=5),
        _format_optional_decimal_fixed(metrics["theta"] if isinstance(metrics["theta"], Decimal) else None, places=5),
        _format_optional_usdt_precise(metrics["theta_usdt"] if isinstance(metrics["theta_usdt"], Decimal) else None, places=2),
    )


def _position_signed_display_amount(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    if position.position == 0:
        return None, None

    direction = _format_pos_side(position.pos_side, position.position)
    sign = Decimal("-1") if direction == "short" else Decimal("1")
    instrument = position_instruments.get(position.inst_id)

    if instrument is not None and instrument.ct_val is not None and instrument.ct_val > 0 and instrument.ct_val_ccy:
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        quote_currency = instrument.ct_val_ccy.upper()
        if quote_currency in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            base_currency = _extract_asset_key(position.inst_id).upper()
            if reference_price is not None and reference_price > 0 and base_currency:
                amount = (abs(position.position) * instrument.ct_val * multiplier / reference_price) * sign
                return amount, base_currency
        amount = abs(position.position) * instrument.ct_val * multiplier * sign
        return amount, quote_currency

    asset_currency = _extract_asset_key(position.inst_id).upper()
    return abs(position.position) * sign, asset_currency if asset_currency else None


def _format_group_position_size(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
) -> str:
    totals: dict[str, Decimal] = {}
    for position in positions:
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[currency] = totals.get(currency, Decimal("0")) + amount

    if not totals:
        return ""

    parts: list[str] = []
    for currency in sorted(totals.keys()):
        amount = totals[currency]
        parts.append(f"{format_decimal_fixed(amount, 2)} {currency}")
    return " / ".join(parts)


def _extract_option_kind(inst_id: str) -> str | None:
    parts = inst_id.split("-")
    if not parts:
        return None
    suffix = parts[-1].strip().upper()
    return suffix if suffix in {"C", "P"} else None


def _format_option_trade_side(position: OkxPosition) -> str:
    if position.inst_type != "OPTION":
        return "-"
    option_kind = _extract_option_kind(position.inst_id)
    direction = _format_pos_side(position.pos_side, position.position)
    if option_kind == "C":
        if direction == "long":
            return "买购"
        if direction == "short":
            return "卖购"
    if option_kind == "P":
        if direction == "long":
            return "买沽"
        if direction == "short":
            return "卖沽"
    return "-"


def _format_option_trade_side_display(position: OkxPosition) -> str:
    return _format_option_trade_side(position)


def _format_group_option_trade_side(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
) -> str:
    totals: dict[str, Decimal] = {}
    ordered_labels = ("买购", "卖购", "买沽", "卖沽")
    for position in positions:
        label = _format_option_trade_side(position)
        if label == "-":
            continue
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[label] = totals.get(label, Decimal("0")) + abs(amount)

    def _slot_text(label: str) -> str:
        if label not in totals:
            return "-"
        amount = totals[label]
        return format_decimal_fixed(amount, 2)

    return (
        f"{_slot_text('买购')} : {_slot_text('卖购')} | "
        f"{_slot_text('买沽')} : {_slot_text('卖沽')}"
    )


def _format_filtered_option_position_size(
    positions: list[OkxPosition],
    position_instruments: dict[str, Instrument],
    *,
    option_kind: str,
    direction: str,
) -> str:
    totals: dict[str, Decimal] = {}
    for position in positions:
        if position.inst_type != "OPTION":
            continue
        if _extract_option_kind(position.inst_id) != option_kind:
            continue
        current_direction = _format_pos_side(position.pos_side, position.position)
        if current_direction != direction:
            continue
        amount, currency = _position_signed_display_amount(position, position_instruments)
        if amount is None or not currency:
            continue
        totals[currency] = totals.get(currency, Decimal("0")) + abs(amount)

    if not totals:
        return "-"

    parts: list[str] = []
    for currency in sorted(totals.keys()):
        parts.append(f"{format_decimal_fixed(totals[currency], 2)} {currency}")
    return " / ".join(parts)


def _format_position_size(position: OkxPosition, position_instruments: dict[str, Instrument]) -> str:
    if position.position == 0:
        return "-"

    direction = _format_pos_side(position.pos_side, position.position)
    amount, currency = _position_signed_display_amount(position, position_instruments)
    if amount is None:
        return "-"
    if currency and currency not in {"USD", "USDT", "USDC"} and position.inst_type in {"FUTURES", "SWAP"}:
        return f"{format_decimal_fixed(amount, 4)} {currency} ({direction})"
    if currency:
        return f"{format_decimal(amount)} {currency} ({direction})"
    return f"{format_decimal(amount)} ({direction})"


def _position_delta_value(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> Decimal | None:
    if position.inst_type == "OPTION":
        return position.delta

    amount, amount_currency = _position_contract_amount(position, position_instruments)
    direction = _format_pos_side(position.pos_side, position.position)
    sign = Decimal("-1") if direction == "short" else Decimal("1")
    if amount is not None and amount_currency:
        if amount_currency in {"USD", "USDT", "USDC"}:
            reference_price = position.mark_price or position.last_price or position.avg_price
            base_currency = _extract_asset_key(position.inst_id).upper()
            if reference_price is not None and reference_price > 0 and base_currency:
                return (amount / reference_price) * sign
        return amount * sign

    if position.inst_type == "SPOT":
        return abs(position.position) * sign
    return position.delta


def _position_theta_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.theta is None:
        return None
    currency = _infer_upl_currency(position)
    if currency in {"USDT", "USD", "USDC"}:
        return position.theta
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.theta * price


def _position_option_intrinsic_value(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION":
        return None
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None or underlying_price <= 0:
        return None
    strike, option_kind = _extract_option_sort_components(position.inst_id)
    strike_price = Decimal(str(strike))
    if option_kind == "C":
        intrinsic_usdt = max(underlying_price - strike_price, Decimal("0"))
    elif option_kind == "P":
        intrinsic_usdt = max(strike_price - underlying_price, Decimal("0"))
    else:
        return None

    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return intrinsic_usdt
    return intrinsic_usdt / underlying_price


def _position_option_intrinsic_value_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    intrinsic_value = _position_option_intrinsic_value(position, upl_usdt_prices)
    if intrinsic_value is None:
        return None
    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return intrinsic_value
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None:
        return None
    return intrinsic_value * underlying_price


def _position_option_time_value(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION":
        return None
    mark_price = position.mark_price or position.last_price
    intrinsic_value = _position_option_intrinsic_value(position, upl_usdt_prices)
    if mark_price is None or intrinsic_value is None:
        return None
    return mark_price - intrinsic_value


def _position_option_time_value_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    time_value = _position_option_time_value(position, upl_usdt_prices)
    if time_value is None:
        return None
    payout_currency = _infer_upl_currency(position)
    if payout_currency in {"USDT", "USD", "USDC"}:
        return time_value
    asset_currency = _extract_asset_key(position.inst_id).upper()
    underlying_price = upl_usdt_prices.get(asset_currency)
    if underlying_price is None:
        return None
    return time_value * underlying_price


def _format_option_price_component(value: Decimal | None, position: OkxPosition) -> str:
    if value is None:
        return "-"
    prefix = _mark_price_prefix(position)
    text = format_decimal_fixed(value, 4)
    return f"{prefix} {text}" if prefix else text


def _format_position_option_price_component(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    *,
    component: str,
) -> str:
    if component == "time_value":
        return _format_option_price_component(_position_option_time_value(position, upl_usdt_prices), position)
    if component == "intrinsic_value":
        return _format_option_price_component(_position_option_intrinsic_value(position, upl_usdt_prices), position)
    return "-"


def _format_position_option_component_usdt(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    *,
    component: str,
) -> str:
    if component == "time_value":
        value = _position_option_time_value_usdt(position, upl_usdt_prices)
    elif component == "intrinsic_value":
        value = _position_option_intrinsic_value_usdt(position, upl_usdt_prices)
    else:
        value = None
    return _format_optional_usdt_precise(value, places=2, with_sign=False)


def _position_mark_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    mark_price = position.mark_price or position.last_price
    if mark_price is None:
        return None
    if position.inst_type == "OPTION":
        currency = _infer_upl_currency(position)
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USDT", "USD", "USDC"}:
        return mark_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return mark_price * price


def _format_mark_price(position: OkxPosition) -> str:
    mark_price = position.mark_price or position.last_price
    if mark_price is None:
        return "-"
    if position.inst_type == "OPTION":
        amount_text = format_decimal_fixed(mark_price, 4)
    else:
        amount_text = format_decimal(mark_price)
    prefix = _mark_price_prefix(position)
    return f"{prefix} {amount_text}" if prefix else amount_text


def _format_position_quote_price(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    position_tickers: dict[str, OkxTicker],
    *,
    side: str,
) -> str:
    ticker = position_tickers.get(position.inst_id)
    if ticker is None:
        return "-"
    if side == "bid":
        quote_price = ticker.bid
    elif side == "ask":
        quote_price = ticker.ask
    else:
        quote_price = None
    if quote_price is None:
        return "-"
    prefix = _mark_price_prefix(position)
    instrument = position_instruments.get(position.inst_id)
    places = _tick_size_places(instrument.tick_size) if instrument is not None else None
    if position.inst_type == "OPTION":
        if places is None:
            places = 4
        amount_text = format_decimal_fixed(quote_price, places)
    elif position.inst_type not in {"FUTURES", "SWAP"}:
        amount_text = _format_optional_decimal(quote_price)
    elif places is None:
        amount_text = _format_optional_decimal(quote_price)
    else:
        amount_text = format_decimal_fixed(quote_price, places)
    return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text


def _position_quote_price_usdt(
    position: OkxPosition,
    position_tickers: dict[str, OkxTicker],
    upl_usdt_prices: dict[str, Decimal],
    *,
    side: str,
) -> Decimal | None:
    ticker = position_tickers.get(position.inst_id)
    if ticker is None:
        return None
    if side == "bid":
        quote_price = ticker.bid
    elif side == "ask":
        quote_price = ticker.ask
    else:
        quote_price = None
    if quote_price is None:
        return None
    if position.inst_type == "OPTION":
        currency = _infer_upl_currency(position)
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USDT", "USD", "USDC"}:
        return quote_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return quote_price * price


def _format_position_quote_price_usdt(
    position: OkxPosition,
    position_tickers: dict[str, OkxTicker],
    upl_usdt_prices: dict[str, Decimal],
    *,
    side: str,
) -> str:
    return _format_optional_usdt(_position_quote_price_usdt(position, position_tickers, upl_usdt_prices, side=side), with_sign=False)


def _mark_price_prefix(position: OkxPosition) -> str:
    if position.inst_type == "OPTION":
        currency = _extract_asset_key(position.inst_id).upper()
    else:
        currency = (_extract_quote_key(position.inst_id) or "").upper()
    if currency in {"USD", "USDT", "USDC"}:
        return "$"
    if currency == "BTC":
        return "B"
    if currency == "ETH":
        return "E"
    return currency[:1] if currency else ""


def _tick_size_places(tick_size: Decimal | None) -> int | None:
    if tick_size is None or tick_size <= 0:
        return None
    normalized = tick_size.normalize()
    exponent = normalized.as_tuple().exponent
    return max(0, -exponent)


def _format_position_avg_price(position: OkxPosition, position_instruments: dict[str, Instrument]) -> str:
    if position.avg_price is None:
        return "-"
    prefix = _mark_price_prefix(position)
    instrument = position_instruments.get(position.inst_id)
    places = _tick_size_places(instrument.tick_size) if instrument is not None else None
    if position.inst_type == "OPTION":
        if places is None:
            places = 4
        amount_text = format_decimal_fixed(position.avg_price, places)
        return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text
    if position.inst_type not in {"FUTURES", "SWAP"}:
        amount_text = _format_optional_decimal(position.avg_price)
        return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text
    if places is None:
        amount_text = _format_optional_decimal(position.avg_price)
    else:
        amount_text = format_decimal_fixed(position.avg_price, places)
    return f"{prefix} {amount_text}" if prefix and amount_text != "-" else amount_text


def _position_avg_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.inst_type != "OPTION" or position.avg_price is None:
        return None
    currency = _infer_upl_currency(position)
    if currency in {"USDT", "USD", "USDC"}:
        return position.avg_price
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.avg_price * price


def _format_position_avg_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> str:
    return _format_optional_usdt(_position_avg_price_usdt(position, upl_usdt_prices), with_sign=False)


def _format_position_mark_price_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> str:
    return _format_optional_usdt(_position_mark_price_usdt(position, upl_usdt_prices), with_sign=False)


def _infer_upl_currency(position: OkxPosition) -> str:
    if position.margin_ccy:
        return position.margin_ccy.strip().upper()
    return _extract_asset_key(position.inst_id).upper()


def _group_pnl_places(currency: object) -> int:
    text = str(currency).strip().upper() if currency is not None else ""
    if text in {"USDT", "USD", "USDC"}:
        return 2
    return 5


def _position_contract_amount(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    instrument = position_instruments.get(position.inst_id)
    if instrument is not None and instrument.ct_val is not None and instrument.ct_val > 0 and instrument.ct_val_ccy:
        multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
        amount = abs(position.position) * instrument.ct_val * multiplier
        return amount, instrument.ct_val_ccy.upper()
    if position.inst_type == "SPOT":
        return abs(position.position), _extract_asset_key(position.inst_id).upper()
    return None, None


def _position_market_value_native(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
) -> tuple[Decimal | None, str | None]:
    mark_price = position.mark_price or position.last_price
    amount, amount_currency = _position_contract_amount(position, position_instruments)
    if amount is None or amount_currency is None:
        return None, None

    if position.inst_type == "OPTION":
        if mark_price is None:
            return None, None
        return amount * mark_price, amount_currency

    if amount_currency in {"USD", "USDT", "USDC"}:
        return amount, amount_currency

    if mark_price is None:
        return None, None

    quote_currency = _extract_quote_key(position.inst_id)
    if quote_currency is None:
        return None, None
    return amount * mark_price, quote_currency


def _position_market_value_usdt(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
    native_value, native_currency = _position_market_value_native(position, position_instruments)
    if native_value is None or native_currency is None:
        return None
    if native_currency in {"USDT", "USD", "USDC"}:
        return native_value
    conversion = upl_usdt_prices.get(native_currency)
    if conversion is None:
        return None
    return native_value * conversion


def _format_position_market_value(
    position: OkxPosition,
    position_instruments: dict[str, Instrument],
    upl_usdt_prices: dict[str, Decimal],
) -> str:
    native_value, native_currency = _position_market_value_native(position, position_instruments)
    if native_value is None or native_currency is None:
        return "-"
    native_text = f"{format_decimal_fixed(native_value, 5)} {native_currency}"
    if native_currency in {"USDT", "USD", "USDC"}:
        return native_text
    usdt_value = _position_market_value_usdt(position, position_instruments, upl_usdt_prices)
    if usdt_value is None:
        return native_text
    return f"{native_text} ({_format_optional_approx_usdt(usdt_value)})"


def _format_position_unrealized_pnl(position: OkxPosition) -> str:
    if position.unrealized_pnl is None:
        return "-"
    places = 2 if position.inst_type in {"FUTURES", "SWAP"} else 8
    amount_text = _format_optional_decimal_fixed(position.unrealized_pnl, places=places, with_sign=True)
    currency = _infer_upl_currency(position)
    if currency:
        amount_text = f"{amount_text} {currency}"
    if position.unrealized_pnl_ratio is not None:
        amount_text = f"{amount_text}（{_format_ratio(position.unrealized_pnl_ratio, places=2)}）"
    return amount_text


def _position_unrealized_pnl_usdt(position: OkxPosition, upl_usdt_prices: dict[str, Decimal]) -> Decimal | None:
    if position.unrealized_pnl is None:
        return None
    currency = _infer_upl_currency(position)
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return position.unrealized_pnl * price


def _build_upl_usdt_price_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, Decimal]:
    currencies = {_infer_upl_currency(position) for position in positions if position.unrealized_pnl is not None}
    return _build_usdt_price_snapshot(client, currencies)


def _build_position_history_usdt_price_map(
    client: OkxRestClient,
    items: list[OkxPositionHistoryItem],
) -> dict[str, Decimal]:
    currencies = {_infer_position_history_pnl_currency(item) for item in items if item.realized_pnl is not None}
    return _build_usdt_price_snapshot(client, currencies)


def _build_usdt_price_snapshot(client: OkxRestClient, currencies: set[str]) -> dict[str, Decimal]:
    prices: dict[str, Decimal] = {}
    for currency in sorted(currencies):
        if currency in {"USDT", "USD"}:
            prices[currency] = Decimal("1")
            continue
        if currency == "USDC":
            prices[currency] = Decimal("1")
            continue
        inst_id = f"{currency}-USDT"
        try:
            ticker = client.get_ticker(inst_id)
        except Exception:
            continue
        spot_price = ticker.last or ticker.bid or ticker.ask or ticker.mark or ticker.index
        if spot_price is not None and spot_price > 0:
            prices[currency] = spot_price
    return prices


def _build_position_instrument_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, Instrument]:
    needed_ids = {position.inst_id for position in positions}
    result: dict[str, Instrument] = {}

    option_families = sorted(
        {
            infer_option_family(position.inst_id)
            for position in positions
            if position.inst_type == "OPTION" and infer_option_family(position.inst_id)
        }
    )
    for family in option_families:
        for instrument in client.get_option_instruments(inst_family=family):
            if instrument.inst_id in needed_ids:
                result[instrument.inst_id] = instrument

    swap_ids = {position.inst_id for position in positions if position.inst_type == "SWAP"}
    if swap_ids:
        for instrument in client.get_swap_instruments():
            if instrument.inst_id in swap_ids:
                result[instrument.inst_id] = instrument

    futures_ids = {position.inst_id for position in positions if position.inst_type == "FUTURES"}
    if futures_ids:
        for instrument in client.get_instruments("FUTURES"):
            if instrument.inst_id in futures_ids:
                result[instrument.inst_id] = instrument

    return result


def _build_position_ticker_map(client: OkxRestClient, positions: list[OkxPosition]) -> dict[str, OkxTicker]:
    result: dict[str, OkxTicker] = {}
    for inst_id in sorted({position.inst_id for position in positions}):
        try:
            result[inst_id] = client.get_ticker(inst_id)
        except Exception:
            continue
    return result


def _filter_positions(
    positions: list[OkxPosition],
    *,
    inst_type: str,
    keyword: str,
) -> list[OkxPosition]:
    normalized_keyword = keyword.strip().lower()
    results: list[OkxPosition] = []
    for position in positions:
        if inst_type and position.inst_type != inst_type:
            continue
        if normalized_keyword:
            haystack = " ".join(
                part.lower()
                for part in (
                    position.inst_id,
                    position.inst_type,
                    position.pos_side,
                    position.mgn_mode,
                    _extract_asset_key(position.inst_id),
                )
                if part
            )
            if normalized_keyword not in haystack:
                continue
        results.append(position)
    return results


def _format_position_filter_summary(type_label: str, keyword: str) -> str:
    parts: list[str] = []
    if type_label and type_label != "全部类型":
        parts.append(type_label)
    if keyword.strip():
        parts.append(keyword.strip().upper())
    return " + ".join(parts)


def _build_position_detail_text(
    position: OkxPosition,
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
) -> str:
    delta_value = _position_delta_value(position, position_instruments)
    return (
        f"合约：{position.inst_id}\n"
        f"类型：{position.inst_type}\n"
        f"方向：{_format_pos_side(position.pos_side, position.position)}\n"
        f"持仓量：{_format_position_size(position, position_instruments)}\n"
        f"可平数量：{_format_optional_decimal(position.avail_position)}\n"
        f"保证金模式：{position.mgn_mode or '-'}\n"
        f"杠杆：{_format_optional_decimal(position.leverage)}\n"
        f"开仓价 / 开仓≈USDT：{_format_position_avg_price(position, position_instruments)} / "
        f"{_format_position_avg_price_usdt(position, upl_usdt_prices)}\n"
        f"标记价 / 标记≈USDT：{_format_mark_price(position)} / "
        f"{_format_position_mark_price_usdt(position, upl_usdt_prices)}\n"
        f"时间价值 / 时间≈USDT："
        f"{_format_position_option_price_component(position, upl_usdt_prices, component='time_value')} / "
        f"{_format_position_option_component_usdt(position, upl_usdt_prices, component='time_value')}\n"
        f"内在价值 / 内在≈USDT："
        f"{_format_position_option_price_component(position, upl_usdt_prices, component='intrinsic_value')} / "
        f"{_format_position_option_component_usdt(position, upl_usdt_prices, component='intrinsic_value')}\n"
        f"市值：{_format_position_market_value(position, position_instruments, upl_usdt_prices)}\n"
        f"最新价：{_format_optional_decimal(position.last_price)}\n"
        f"浮盈亏 / 浮盈≈USDT：{_format_position_unrealized_pnl(position)} / "
        f"{_format_optional_usdt(_position_unrealized_pnl_usdt(position, upl_usdt_prices))}\n"
        f"已实现盈亏：{_format_optional_decimal_fixed(position.realized_pnl, places=5, with_sign=True)}\n"
        f"强平价：{_format_optional_decimal(position.liquidation_price)}\n"
        f"保证金币种：{position.margin_ccy or '-'}\n"
        f"保证金率：{_format_ratio(position.margin_ratio, places=2)}\n"
        f"初始保证金(IMR)：{_format_optional_integer(position.initial_margin)}\n"
        f"维持保证金：{_format_optional_integer(position.maintenance_margin)}\n"
        f"Delta / Gamma(PA) / Vega(PA) / Theta(PA) / Theta≈USDT："
        f"{_format_optional_decimal_fixed(delta_value, places=5)} / "
        f"{_format_optional_decimal_fixed(position.gamma, places=5)} / "
        f"{_format_optional_decimal_fixed(position.vega, places=5)} / "
        f"{_format_optional_decimal_fixed(position.theta, places=5)} / "
        f"{_format_optional_usdt_precise(_position_theta_usdt(position, upl_usdt_prices), places=2)}"
    )


def _build_group_detail_text(
    label: str,
    positions: list[OkxPosition],
    metrics: dict[str, Decimal | int | None],
    upl_usdt_prices: dict[str, Decimal],
    position_instruments: dict[str, Instrument],
) -> str:
    pnl_places = _group_pnl_places(metrics.get("pnl_currency"))
    lines = [
        f"分组：{label}",
        f"持仓笔数：{metrics['count']}",
        f"浮动盈亏：{_format_optional_decimal_fixed(metrics['upl'] if isinstance(metrics['upl'], Decimal) else None, places=pnl_places, with_sign=True)}",
        f"折合USDT：{_format_optional_usdt(metrics['upl_usdt'] if isinstance(metrics['upl_usdt'], Decimal) else None)}",
        f"市值：{_format_optional_approx_usdt(metrics['market_value_usdt'] if isinstance(metrics['market_value_usdt'], Decimal) else None)}",
        f"已实现盈亏：{_format_optional_decimal_fixed(metrics['realized'] if isinstance(metrics['realized'], Decimal) else None, places=pnl_places, with_sign=True)}",
        f"初始保证金(IMR)：{_format_optional_integer(metrics['imr'] if isinstance(metrics['imr'], Decimal) else None)}",
        f"维持保证金：{_format_optional_integer(metrics['mmr'] if isinstance(metrics['mmr'], Decimal) else None)}",
        f"Greeks 汇总(PA)：Δ {_format_optional_decimal(metrics['delta'] if isinstance(metrics['delta'], Decimal) else None)}"
        f" / Γ {_format_optional_decimal(metrics['gamma'] if isinstance(metrics['gamma'], Decimal) else None)}"
        f" / V {_format_optional_decimal(metrics['vega'] if isinstance(metrics['vega'], Decimal) else None)}"
        f" / Θ {_format_optional_decimal(metrics['theta'] if isinstance(metrics['theta'], Decimal) else None)}"
        f" / Θ≈USDT {_format_optional_usdt_precise(metrics['theta_usdt'] if isinstance(metrics['theta_usdt'], Decimal) else None, places=2)}",
        "",
        "包含持仓：",
    ]
    preview = positions[:8]
    lines.extend(
        f"- {item.inst_id} | {_format_position_size(item, position_instruments)} | 浮盈 {_format_position_unrealized_pnl(item)}"
        f" | 折合USDT {_format_optional_usdt(_position_unrealized_pnl_usdt(item, upl_usdt_prices))}"
        for item in preview
    )
    if len(positions) > len(preview):
        lines.append(f"- ... 还有 {len(positions) - len(preview)} 笔")
    return "\n".join(lines)


def _validate_protection_price_relationship(
    *,
    option_inst_id: str,
    direction: str,
    trigger_inst_id: str,
    trigger_price_type: str,
    take_profit: Decimal | None,
    stop_loss: Decimal | None,
) -> None:
    if take_profit is None or stop_loss is None:
        return
    profit_on_rise = infer_protection_profit_on_rise(
        option_inst_id=option_inst_id,
        direction="long" if direction == "long" else "short",
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
    )
    logic_hint = describe_protection_price_logic(
        option_inst_id=option_inst_id,
        direction="long" if direction == "long" else "short",
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=trigger_price_type,  # type: ignore[arg-type]
    )
    if profit_on_rise and take_profit <= stop_loss:
        raise ValueError(logic_hint)
    if not profit_on_rise and take_profit >= stop_loss:
        raise ValueError(logic_hint)


def _validate_protection_live_price_availability(
    client: OkxRestClient,
    protection: OptionProtectionConfig,
    position: OkxPosition,
) -> None:
    if protection.trigger_price_type == "mark":
        try:
            client.get_trigger_price(protection.trigger_inst_id, "mark")
        except OkxApiError as exc:
            raise ValueError(
                f"{protection.option_inst_id} 当前拿不到标记价格，不能用“期权标记价格”触发。"
                "请改用“现货最新价”，或者等 OKX 返回 markPx 后再启动。"
            ) from exc

    requires_mark_for_order = (
        protection.take_profit_order_mode == "mark_with_slippage"
        or protection.stop_loss_order_mode == "mark_with_slippage"
    )
    if not requires_mark_for_order:
        return

    try:
        client.get_trigger_price(protection.option_inst_id, "mark")
    except OkxApiError as exc:
        raise ValueError(
            f"{protection.option_inst_id} 当前拿不到标记价格，不能用“标记价格加减滑点”报单。"
            "请把止盈/止损报单方式改成“设定价格”，或者等 OKX 返回 markPx 后再启动。"
        ) from exc

    instrument = client.get_instrument(protection.option_inst_id)
    close_side = "sell" if derive_position_direction(position) == "long" else "buy"
    open_avg_price = position.avg_price

    if protection.take_profit_trigger is not None:
        if protection.take_profit_order_mode == "mark_with_slippage":
            mark_price = client.get_trigger_price(protection.option_inst_id, "mark")
            preview_price = build_close_order_price_from_mark(
                mark_price=mark_price,
                close_side=close_side,
                tick_size=instrument.tick_size,
                mode=protection.take_profit_order_mode,
                fixed_price=protection.take_profit_order_price,
                slippage=protection.take_profit_slippage,
            )
        else:
            preview_price = protection.take_profit_order_price
        if preview_price is not None:
            try:
                validate_live_protection_order_price_guard(
                    client=client,
                    option_inst_id=protection.option_inst_id,
                    close_side=close_side,
                    order_price=preview_price,
                    tick_size=instrument.tick_size,
                    open_avg_price=open_avg_price,
                )
            except RuntimeError as exc:
                raise ValueError(str(exc)) from exc

    if protection.stop_loss_trigger is not None:
        if protection.stop_loss_order_mode == "mark_with_slippage":
            mark_price = client.get_trigger_price(protection.option_inst_id, "mark")
            preview_price = build_close_order_price_from_mark(
                mark_price=mark_price,
                close_side=close_side,
                tick_size=instrument.tick_size,
                mode=protection.stop_loss_order_mode,
                fixed_price=protection.stop_loss_order_price,
                slippage=protection.stop_loss_slippage,
            )
        else:
            preview_price = protection.stop_loss_order_price
        if preview_price is not None:
            try:
                validate_live_protection_order_price_guard(
                    client=client,
                    option_inst_id=protection.option_inst_id,
                    close_side=close_side,
                    order_price=preview_price,
                    tick_size=instrument.tick_size,
                    open_avg_price=open_avg_price,
                )
            except RuntimeError as exc:
                raise ValueError(str(exc)) from exc


def _resolve_protection_order_mode_value(mode_label: str) -> str:
    if mode_label in PROTECTION_ORDER_MODE_OPTIONS:
        return PROTECTION_ORDER_MODE_OPTIONS[mode_label]
    normalized = mode_label.strip().lower()
    if "mark" in normalized or "滑点" in mode_label or "slippage" in normalized:
        return "mark_with_slippage"
    return "fixed_price"


def _format_protection_order_mode_label(mode: str) -> str:
    return "设定价格" if mode == "fixed_price" else "标记价格加减滑点"


def _format_protection_order_price_detail(mode: str, price: Decimal | None) -> str:
    if mode == "fixed_price":
        return _format_optional_decimal(price)
    return "自动按标记价与滑点计算"


def _format_protection_trigger_price_type(trigger_price_type: str) -> str:
    return "标记价" if trigger_price_type == "mark" else "最新价"


def _format_okx_ms_timestamp(timestamp_ms: int | None) -> str:
    if timestamp_ms is None or timestamp_ms <= 0:
        return "-"
    try:
        return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _format_history_side(side: str | None, pos_side: str | None) -> str:
    parts = [part for part in (side, pos_side) if part and part.lower() != "net"]
    return " / ".join(parts) if parts else (side or pos_side or "-")


def _history_tree_index(item_id: str, prefix: str) -> int | None:
    marker = f"{prefix}-"
    if not item_id.startswith(marker):
        return None
    try:
        return int(item_id[len(marker) :])
    except ValueError:
        return None


def _filter_position_history_items(
    items: list[OkxPositionHistoryItem],
    *,
    inst_type: str = "",
    margin_mode: str = "",
    keyword: str = "",
) -> list[tuple[int, OkxPositionHistoryItem]]:
    normalized_inst_type = inst_type.strip().upper()
    normalized_margin_mode = margin_mode.strip().lower()
    normalized_keyword = keyword.strip().lower()
    filtered: list[tuple[int, OkxPositionHistoryItem]] = []

    for index, item in enumerate(items):
        if normalized_inst_type and (item.inst_type or "").upper() != normalized_inst_type:
            continue
        if normalized_margin_mode and (item.mgn_mode or "").lower() != normalized_margin_mode:
            continue
        if normalized_keyword:
            haystack = " ".join(
                part
                for part in (
                    item.inst_id,
                    item.inst_type,
                    item.mgn_mode,
                    item.pos_side,
                    item.direction,
                )
                if part
            ).lower()
            if normalized_keyword not in haystack:
                continue
        filtered.append((index, item))
    return filtered


def _filter_fill_history_items(
    items: list[OkxFillHistoryItem],
    *,
    inst_type: str = "",
    side: str = "",
    keyword: str = "",
) -> list[tuple[int, OkxFillHistoryItem]]:
    normalized_inst_type = inst_type.strip().upper()
    normalized_side = side.strip().lower()
    normalized_keyword = keyword.strip().lower()
    filtered: list[tuple[int, OkxFillHistoryItem]] = []

    for index, item in enumerate(items):
        if normalized_inst_type and (item.inst_type or "").upper() != normalized_inst_type:
            continue
        if normalized_side and (item.side or "").lower() != normalized_side:
            continue
        if normalized_keyword:
            haystack = " ".join(
                part
                for part in (
                    item.inst_id,
                    item.inst_type,
                    item.side,
                    item.pos_side,
                    item.exec_type,
                    item.fee_currency,
                )
                if part
            ).lower()
            if normalized_keyword not in haystack:
                continue
        filtered.append((index, item))
    return filtered


def _format_position_history_currency_totals(totals: dict[str, Decimal]) -> str:
    if not totals:
        return "-"
    parts: list[str] = []
    for currency, value in totals.items():
        normalized_currency = (currency or "-").upper()
        parts.append(f"{normalized_currency} {_format_history_amount(value, normalized_currency, with_sign=True)}")
    return " / ".join(parts)


def _format_position_history_filter_stats(
    filtered_items: list[tuple[int, OkxPositionHistoryItem]],
    usdt_prices: dict[str, Decimal],
) -> str:
    pnl_totals: dict[str, Decimal] = {}
    realized_totals: dict[str, Decimal] = {}
    realized_usdt_total = Decimal("0")
    realized_usdt_count = 0

    for _, item in filtered_items:
        currency = _infer_position_history_pnl_currency(item)
        if item.pnl is not None:
            pnl_totals[currency] = pnl_totals.get(currency, Decimal("0")) + item.pnl
        if item.realized_pnl is not None:
            realized_totals[currency] = realized_totals.get(currency, Decimal("0")) + item.realized_pnl
        realized_usdt = _position_history_realized_pnl_usdt(item, usdt_prices)
        if realized_usdt is not None:
            realized_usdt_total += realized_usdt
            realized_usdt_count += 1

    realized_usdt_text = (
        _format_optional_usdt(realized_usdt_total)
        if realized_usdt_count
        else "-"
    )
    return (
        f"\u76c8\u4e8f\u5408\u8ba1 { _format_position_history_currency_totals(pnl_totals) } | "
        f"\u5df2\u5b9e\u73b0\u5408\u8ba1 { _format_position_history_currency_totals(realized_totals) } | "
        f"\u6298\u5408USDT\u5408\u8ba1 {realized_usdt_text}"
    )


def _infer_position_history_pnl_currency(item: OkxPositionHistoryItem) -> str:
    quote_currency = _extract_quote_key(item.inst_id)
    if item.inst_type in {"SWAP", "SPOT", "FUTURES"} and quote_currency in {"USDT", "USD", "USDC"}:
        return quote_currency
    return _extract_asset_key(item.inst_id).upper()


def _position_history_realized_pnl_usdt(
    item: OkxPositionHistoryItem,
    upl_usdt_prices: dict[str, Decimal],
) -> Decimal | None:
    if item.inst_type != "OPTION":
        return None
    if item.realized_pnl is None:
        return None
    currency = _infer_position_history_pnl_currency(item)
    if currency in {"USDT", "USD", "USDC"}:
        return item.realized_pnl
    price = upl_usdt_prices.get(currency)
    if price is None:
        return None
    return item.realized_pnl * price


def _infer_fill_history_pnl_currency(item: OkxFillHistoryItem) -> str:
    if item.inst_type == "OPTION":
        return _extract_asset_key(item.inst_id).upper()
    quote_currency = _extract_quote_key(item.inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return quote_currency
    return _extract_asset_key(item.inst_id).upper()


def _format_optional_decimal_capped(value: Decimal | None, *, places: int, with_sign: bool = False) -> str:
    if value is None:
        return "-"
    quant = Decimal("1").scaleb(-places)
    rounded = value.quantize(quant, rounding=ROUND_HALF_UP)
    text = format_decimal(rounded)
    if with_sign and value > 0:
        return f"+{text}"
    return text


def _format_history_amount(value: Decimal | None, currency: str | None, *, with_sign: bool = False) -> str:
    normalized = (currency or "").upper()
    if normalized in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(value, places=2, with_sign=with_sign)
    return _format_optional_decimal_capped(value, places=8, with_sign=with_sign)


def _format_position_history_price(value: Decimal | None, inst_id: str, inst_type: str) -> str:
    if inst_type == "OPTION":
        return _format_optional_decimal_capped(value, places=8)
    quote_currency = _extract_quote_key(inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(value, places=2)
    return _format_optional_decimal_capped(value, places=8)


def _format_position_history_pnl(
    value: Decimal | None,
    item: OkxPositionHistoryItem,
    *,
    with_sign: bool = False,
) -> str:
    return _format_history_amount(value, _infer_position_history_pnl_currency(item), with_sign=with_sign)


def _format_fill_history_pnl(item: OkxFillHistoryItem) -> str:
    return _format_history_amount(item.pnl, _infer_fill_history_pnl_currency(item), with_sign=True)


def _format_fill_history_price(item: OkxFillHistoryItem) -> str:
    if item.exec_type == "行权/交割":
        return _format_optional_decimal_fixed(item.fill_price, places=2)
    if item.inst_type == "OPTION":
        return _format_optional_decimal(item.fill_price)
    quote_currency = _extract_quote_key(item.inst_id)
    if quote_currency in {"USDT", "USD", "USDC"}:
        return _format_optional_decimal_fixed(item.fill_price, places=2)
    return _format_optional_decimal(item.fill_price)


def _build_account_config_detail_text(
    config: OkxAccountConfig,
    overview: OkxAccountOverview,
    *,
    profile_name: str,
    environment: str,
) -> str:
    environment_label = "实盘 live" if environment == "live" else "模拟盘 demo"
    if config.auto_loan is True:
        auto_loan_text = "开启"
    elif config.auto_loan is False:
        auto_loan_text = "关闭"
    else:
        auto_loan_text = "-"
    return (
        f"API配置：{profile_name}\n"
        f"环境：{environment_label}\n"
        f"账户模式：{_format_account_level(config.account_level)}\n"
        f"持仓模式：{_format_account_position_mode(config.position_mode)}\n"
        f"Greeks类型：{_format_greeks_type(config.greeks_type)}\n"
        f"自动借币：{auto_loan_text}\n"
        f"总权益：{_format_optional_usdt_precise(overview.total_equity, places=2, with_sign=False)}\n"
        f"调整后权益：{_format_optional_usdt_precise(overview.adjusted_equity, places=2, with_sign=False)}\n"
        f"总名义价值(USD)：{_format_optional_usdt_precise(overview.notional_usd, places=2, with_sign=False)}\n"
        f"订单冻结：{_format_optional_usdt_precise(overview.order_frozen, places=2, with_sign=False)}"
    )


def _build_account_asset_detail_text(item: OkxAccountAssetItem) -> str:
    return (
        f"币种：{item.ccy or '-'}\n"
        f"权益：{_format_optional_decimal(item.equity)}\n"
        f"折合USD：{_format_optional_usdt_precise(item.equity_usd, places=2, with_sign=False)}\n"
        f"现金余额：{_format_optional_decimal(item.cash_balance)}\n"
        f"可用余额：{_format_optional_decimal(item.available_balance)}\n"
        f"可用权益：{_format_optional_decimal(item.available_equity)}\n"
        f"未实现盈亏：{_format_optional_decimal(item.unrealized_pnl, with_sign=True)}\n"
        f"折后权益：{_format_optional_decimal(item.discount_equity)}\n"
        f"冻结：{_format_optional_decimal(item.frozen_balance)}\n"
        f"负债：{_format_optional_decimal(item.liability)}\n"
        f"全仓负债：{_format_optional_decimal(item.cross_liability)}\n"
        f"利息：{_format_optional_decimal(item.interest)}"
    )


def _build_fill_history_detail_text(item: OkxFillHistoryItem) -> str:
    return (
        f"时间：{_format_okx_ms_timestamp(item.fill_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"方向：{_format_history_side(item.side, item.pos_side)}\n"
        f"成交价：{_format_fill_history_price(item)}\n"
        f"成交量：{_format_optional_decimal(item.fill_size)}\n"
        f"手续费：{_format_optional_decimal(item.fill_fee)} {item.fee_currency or ''}\n"
        f"已实现盈亏：{_format_fill_history_pnl(item)}\n"
        f"成交类型：{item.exec_type or '-'}\n"
        f"订单ID：{item.order_id or '-'}\n"
        f"成交ID：{item.trade_id or '-'}"
    )


def _build_position_history_detail_text(item: OkxPositionHistoryItem, upl_usdt_prices: dict[str, Decimal]) -> str:
    return (
        f"更新时间：{_format_okx_ms_timestamp(item.update_time)}\n"
        f"合约：{item.inst_id or '-'}\n"
        f"类型：{item.inst_type or '-'}\n"
        f"保证金模式：{_format_margin_mode(item.mgn_mode or '')}\n"
        f"方向：{_format_history_side(None, item.pos_side or item.direction)}\n"
        f"开仓均价：{_format_position_history_price(item.open_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓均价：{_format_position_history_price(item.close_avg_price, item.inst_id, item.inst_type)}\n"
        f"平仓数量：{_format_optional_decimal(item.close_size)}\n"
        f"盈亏：{_format_position_history_pnl(item.pnl, item)}\n"
        f"已实现盈亏：{_format_position_history_pnl(item.realized_pnl, item, with_sign=True)}\n"
        f"结算盈亏：{_format_optional_decimal(item.settle_pnl)}"
    )


def _tree_display_columns(tree: ttk.Treeview, columns: tuple[str, ...]) -> tuple[str, ...]:
    display_columns = tree.cget("displaycolumns")
    if display_columns in ("#all", ("#all",), "", None):
        return columns
    if isinstance(display_columns, str):
        return tuple(part for part in display_columns.split() if part)
    return tuple(display_columns)


def _tree_safe_token(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value.strip())
    return cleaned or "item"


def _asset_group_row_id(asset_label: str) -> str:
    return f"asset:{_tree_safe_token(asset_label)}"


def _bucket_group_row_id(asset_label: str, bucket_label: str) -> str:
    return f"bucket:{_tree_safe_token(asset_label)}:{_tree_safe_token(bucket_label)}"


def _position_tree_row_id(position: OkxPosition) -> str:
    pos_side = (position.pos_side or "net").lower()
    mgn_mode = (position.mgn_mode or "unknown").lower()
    return f"pos:{_tree_safe_token(position.inst_id)}:{_tree_safe_token(pos_side)}:{_tree_safe_token(mgn_mode)}"


def _find_position_by_key(positions: list[OkxPosition], key: str) -> OkxPosition | None:
    for position in positions:
        if _position_tree_row_id(position) == key:
            return position
    return None


def _resolve_position_selection_target(
    *,
    existing_ids: set[str],
    selected_position_key: str | None,
    protection_position_key: str | None,
    selected_before: str | None,
    top_items: tuple[str, ...],
) -> str | None:
    for candidate in (selected_position_key, protection_position_key, selected_before):
        if candidate and candidate in existing_ids:
            return candidate
    return top_items[0] if top_items else None


def _pnl_tag(value: Decimal | None) -> str:
    if value is None:
        return "group"
    if value > 0:
        return "profit"
    if value < 0:
        return "loss"
    return "group"


def run_app() -> None:
    app = QuantApp()
    app.root.mainloop()
