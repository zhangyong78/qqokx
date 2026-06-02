from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from tkinter import BooleanVar, Canvas, END, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Callable, Literal

from okx_quant.arbitrage.basis_calculator import mid_price
from okx_quant.arbitrage.arbitrage_executor import (
    ArbitrageCloseRequest,
    ArbitrageOpenRequest,
    ArbitrageRollRequest,
    _wait_order_fill,
)
from okx_quant.arbitrage.fill_reconciler import spot_base_from_derivative_fill
from okx_quant.arbitrage.arbitrage_manager import ArbitrageManager
from okx_quant.arbitrage.models import ArbitrageOpportunity, ArbitrageTradeRuntime
from okx_quant.models import Candle, Credentials, Instrument, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxOrderBook, OkxPosition, OkxTicker, infer_inst_type
from okx_quant.persistence import (
    DEFAULT_CREDENTIAL_PROFILE_NAME,
    load_arbitrage_settings_snapshot,
    load_credentials_profiles_snapshot,
    load_credentials_snapshot,
    save_arbitrage_settings_snapshot,
)
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.strategy_live_chart import StrategyLiveChartSnapshot, build_strategy_live_chart_snapshot, render_strategy_live_chart
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]
RuntimeConfigProvider = Callable[[], ArbitrageTradeRuntime | None]
REFRESH_INTERVAL_MS = 5000
MONITOR_UI_REFRESH_MS = 1000
PAIR_CLOSE_MONITOR_POLL_SECONDS = 2.0
MARKET_PANEL_WS_REFRESH_MS = 250
MARKET_PANEL_REST_REFRESH_MS = 1000
MARKET_PANEL_DEPTH = 10
_CHART_BAR_OPTIONS = ("1m", "5m", "15m", "1H", "4H", "1D")
_SIZE_UNIT_OPTIONS = {
    "币数": "coin",
    "USDT金额": "usdt",
    "合约张数": "contracts",
}
_TRIGGER_MODE_OPTIONS = {
    "价差率触发": "spread",
    "绝对价差触发": "spread_abs",
    "限价触发": "limit_price",
}
_CLOSE_TRIGGER_MODE_OPTIONS = {
    "价差率": "spread",
    "绝对价差": "spread_abs",
}
_PAIR_CLOSE_EXECUTION_MODE_OPTIONS = {
    "双腿吃单": "dual_taker",
    "现货挂单/合约吃单": "spot_maker_derivative_taker",
    "合约挂单/现货吃单": "derivative_maker_spot_taker",
}
_ARBITRAGE_EXECUTION_MODE_OPTIONS = _PAIR_CLOSE_EXECUTION_MODE_OPTIONS
_ROLL_EXECUTION_MODE_OPTIONS = {
    "双腿吃单": "dual_taker",
    "旧合约挂单/新合约吃单": "old_maker_new_taker",
    "新合约挂单/旧合约吃单": "new_maker_old_taker",
}


@dataclass(frozen=True)
class PairCloseLivePlan:
    spot_position: OkxPosition
    derivative_position: OkxPosition
    spot_instrument: Instrument
    derivative_instrument: Instrument
    derivative_qty: Decimal
    spot_qty: Decimal


@dataclass
class PairCloseAutoSession:
    spot_inst_id: str
    derivative_inst_id: str
    spot_direction: str
    derivative_direction: str
    target_derivative_qty: Decimal
    planned_batches: tuple[Decimal, ...]
    trigger_mode: str
    spread_pct_min: Decimal | None
    spread_abs_min: Decimal | None
    execution_mode: str
    status: str = "监控中"
    last_spread_pct: Decimal | None = None
    last_spread_abs: Decimal | None = None
    triggered: bool = False
    completed_batches: int = 0


@dataclass
class ArbitrageMarketPanel:
    spot_title_text: StringVar
    spot_quote_text: StringVar
    derivative_title_text: StringVar
    derivative_quote_text: StringVar
    spread_text: StringVar
    detail_text: StringVar
    status_text: StringVar
    spot_tree: ttk.Treeview
    derivative_tree: ttk.Treeview


def _spread_abs(spot_price: Decimal, derivative_price: Decimal) -> Decimal:
    return derivative_price - spot_price


def _instrument_quote_ccy(inst_id: str) -> str:
    parts = [part for part in inst_id.strip().upper().split("-") if part]
    if len(parts) >= 2:
        return parts[1]
    return "QUOTE"


def _market_depth_display_qty(
    size: Decimal,
    *,
    instrument: Instrument,
    price: Decimal,
) -> Decimal:
    if instrument.inst_type in {"SWAP", "FUTURES"}:
        converted = _pair_derivative_base_qty_from_contracts(
            size,
            instrument=instrument,
            reference_price=price,
        )
        if converted is not None:
            return converted
    return size


def _market_depth_rows(
    order_book: OkxOrderBook,
    *,
    instrument: Instrument,
    depth: int = MARKET_PANEL_DEPTH,
) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    asks = list(order_book.asks[:depth])
    bids = list(order_book.bids[:depth])
    for price, size in reversed(asks):
        rows.append(("ask", format_decimal(price), format_decimal(_market_depth_display_qty(size, instrument=instrument, price=price))))
    for price, size in bids:
        rows.append(("bid", format_decimal(price), format_decimal(_market_depth_display_qty(size, instrument=instrument, price=price))))
    return rows


def _best_book_price(
    ticker: OkxTicker,
    order_book: OkxOrderBook,
    *,
    side: Literal["buy", "sell"],
) -> Decimal | None:
    if side == "buy":
        if order_book.asks:
            return order_book.asks[0][0]
        return ticker.ask or ticker.last or ticker.bid
    if order_book.bids:
        return order_book.bids[0][0]
    return ticker.bid or ticker.last or ticker.ask


def _actionable_spread_abs(
    *,
    spot_ticker: OkxTicker,
    spot_order_book: OkxOrderBook,
    derivative_ticker: OkxTicker,
    derivative_order_book: OkxOrderBook,
    spot_side: Literal["buy", "sell"],
    derivative_side: Literal["buy", "sell"],
) -> Decimal | None:
    spot_price = _best_book_price(spot_ticker, spot_order_book, side=spot_side)
    derivative_price = _best_book_price(derivative_ticker, derivative_order_book, side=derivative_side)
    if spot_price is None or derivative_price is None:
        return None
    return derivative_price - spot_price


def _build_spread_candles(spot_candles: list[Candle], derivative_candles: list[Candle]) -> list[Candle]:
    derivative_by_ts = {item.ts: item for item in derivative_candles}
    spread_candles: list[Candle] = []
    for spot in spot_candles:
        derivative = derivative_by_ts.get(spot.ts)
        if derivative is None:
            continue
        values = (
            _spread_abs(spot.open, derivative.open),
            _spread_abs(spot.high, derivative.high),
            _spread_abs(spot.low, derivative.low),
            _spread_abs(spot.close, derivative.close),
        )
        spread_candles.append(
            Candle(
                ts=spot.ts,
                open=values[0],
                high=max(values),
                low=min(values),
                close=values[3],
                volume=Decimal("0"),
                confirmed=spot.confirmed and derivative.confirmed,
            )
        )
    return spread_candles


def _pair_position_closeable_size(position: OkxPosition) -> Decimal:
    base = position.avail_position
    if base is None or base == 0:
        base = position.position
    return abs(base)


def _pair_position_direction(position: OkxPosition) -> Literal["long", "short"]:
    if position.pos_side and position.pos_side.lower() != "net":
        return "long" if position.pos_side.lower() == "long" else "short"
    return "long" if position.position >= 0 else "short"


def _pair_position_close_side(position: OkxPosition) -> Literal["buy", "sell"]:
    return "sell" if _pair_position_direction(position) == "long" else "buy"


def _pair_position_base_ccy(position: OkxPosition) -> str:
    return (position.inst_id or "").strip().upper().split("-")[0]


def _pair_derivative_base_qty_from_contracts(
    contracts: Decimal,
    *,
    instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal | None:
    if instrument.inst_type not in {"SWAP", "FUTURES"}:
        return None
    ct_val = instrument.ct_val
    if ct_val is None or ct_val <= 0:
        return None
    multiplier = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    payout_ccy = (instrument.ct_val_ccy or "").strip().upper()
    if payout_ccy in {"USD", "USDT", "USDC"} and reference_price is not None and reference_price > 0:
        amount = max(contracts, Decimal("0")) * ct_val * multiplier / reference_price
    else:
        amount = max(contracts, Decimal("0")) * ct_val * multiplier
    return snap_to_increment(amount, Decimal("0.00000001"), "down")


def _pair_position_base_exposure(
    position: OkxPosition,
    instrument: Instrument | None = None,
    *,
    reference_price: Decimal | None = None,
) -> Decimal | None:
    if instrument is None:
        return None
    return _pair_derivative_base_qty_from_contracts(
        _pair_position_closeable_size(position),
        instrument=instrument,
        reference_price=reference_price,
    )


def _pair_position_label(position: OkxPosition, instrument: Instrument | None = None) -> str:
    direction = "多" if _pair_position_direction(position) == "long" else "空"
    closeable = format_decimal(_pair_position_closeable_size(position))
    pos_side = (position.pos_side or "net").strip().lower() or "net"
    mgn_mode = (position.mgn_mode or "-").strip().lower() or "-"
    parts = [position.inst_id, direction]
    if position.inst_type == "SPOT":
        parts.append(f"币数 {closeable}")
    else:
        parts.append(f"可平 {closeable}")
        base_exposure = _pair_position_base_exposure(
            position,
            instrument,
            reference_price=position.mark_price or position.last_price,
        )
        base_ccy = _pair_position_base_ccy(position)
        if base_exposure is not None and base_ccy:
            parts.append(f"折合 {format_decimal(base_exposure)} {base_ccy}")
    parts.append(position.inst_type)
    parts.append(f"{mgn_mode}/{pos_side}")
    return " | ".join(parts)


def _build_spot_positions_from_account(overview, client) -> list[OkxPosition]:
    positions: list[OkxPosition] = []
    for asset in getattr(overview, "details", ()):
        ccy = str(getattr(asset, "ccy", "") or "").strip().upper()
        if not ccy or ccy == "USDT":
            continue
        available = getattr(asset, "available_balance", None)
        equity = getattr(asset, "equity", None)
        closeable = available if available not in {None, Decimal("0")} else equity
        if closeable is None or closeable <= 0:
            continue
        inst_id = f"{ccy}-USDT"
        try:
            client.get_instrument(inst_id)
        except Exception:
            continue
        positions.append(
            OkxPosition(
                inst_id=inst_id,
                inst_type="SPOT",
                pos_side="net",
                mgn_mode="cash",
                position=closeable,
                avail_position=closeable,
                avg_price=None,
                mark_price=None,
                unrealized_pnl=None,
                unrealized_pnl_ratio=None,
                liquidation_price=None,
                leverage=None,
                margin_ccy=ccy,
                last_price=None,
                realized_pnl=None,
                margin_ratio=None,
                initial_margin=None,
                maintenance_margin=None,
                delta=None,
                gamma=None,
                vega=None,
                theta=None,
                raw={"source": "account_balance"},
            )
        )
    positions.sort(key=lambda item: item.inst_id)
    return positions


def _future_family_key(inst_id: str) -> str | None:
    normalized = inst_id.strip().upper()
    if infer_inst_type(normalized) != "FUTURES":
        return None
    parts = [part for part in normalized.split("-") if part]
    if len(parts) < 3:
        return None
    expiry = parts[-1]
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return "-".join(parts[:-1])


def _future_expiry_code(inst_id: str) -> str | None:
    normalized = inst_id.strip().upper()
    if infer_inst_type(normalized) != "FUTURES":
        return None
    parts = [part for part in normalized.split("-") if part]
    if len(parts) < 3:
        return None
    expiry = parts[-1]
    if len(expiry) != 6 or not expiry.isdigit():
        return None
    return expiry


def _roll_target_future_candidates(current_inst_id: str, instruments: list[Instrument]) -> list[str]:
    current_family = _future_family_key(current_inst_id)
    current_expiry = _future_expiry_code(current_inst_id)
    if current_family is None or current_expiry is None:
        return []
    candidates: list[str] = []
    for instrument in instruments:
        inst_id = instrument.inst_id.strip().upper()
        if inst_id == current_inst_id.strip().upper():
            continue
        if instrument.state and instrument.state.lower() not in {"live", "test"}:
            continue
        if _future_family_key(inst_id) != current_family:
            continue
        expiry = _future_expiry_code(inst_id)
        if expiry is None or expiry <= current_expiry:
            continue
        candidates.append(inst_id)
    candidates.sort(key=lambda item: (_future_expiry_code(item) or "", item))
    return candidates


def _pair_max_derivative_close_qty(
    spot_position: OkxPosition,
    derivative_position: OkxPosition,
    *,
    spot_instrument: Instrument,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    derivative_closeable = snap_to_increment(
        _pair_position_closeable_size(derivative_position),
        derivative_instrument.lot_size,
        "down",
    )
    spot_closeable = snap_to_increment(
        _pair_position_closeable_size(spot_position),
        spot_instrument.lot_size,
        "down",
    )
    base_per_contract = _pair_derivative_base_qty_from_contracts(
        Decimal("1"),
        instrument=derivative_instrument,
        reference_price=reference_price,
    )
    if base_per_contract is None or base_per_contract <= 0:
        return Decimal("0")
    contracts_from_spot = snap_to_increment(
        max(spot_closeable, Decimal("0")) / max(base_per_contract, Decimal("1e-18")),
        derivative_instrument.lot_size,
        "down",
    )
    return min(derivative_closeable, contracts_from_spot)


def _pair_spot_qty_from_derivative_qty(
    derivative_qty: Decimal,
    *,
    spot_instrument: Instrument,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    raw_spot_qty = _pair_derivative_base_qty_from_contracts(
        derivative_qty,
        instrument=derivative_instrument,
        reference_price=reference_price,
    )
    if raw_spot_qty is None:
        raw_spot_qty = spot_base_from_derivative_fill(
            derivative_filled_contracts=derivative_qty,
            derivative_instrument=derivative_instrument,
        )
    return snap_to_increment(
        raw_spot_qty,
        spot_instrument.lot_size,
        "down",
    )


def _pair_derivative_qty_from_spot_qty(
    spot_qty: Decimal,
    *,
    derivative_instrument: Instrument,
    reference_price: Decimal | None = None,
) -> Decimal:
    base_per_contract = _pair_derivative_base_qty_from_contracts(
        Decimal("1"),
        instrument=derivative_instrument,
        reference_price=reference_price,
    )
    if base_per_contract is None or base_per_contract <= 0:
        return Decimal("0")
    return snap_to_increment(
        max(spot_qty, Decimal("0")) / max(base_per_contract, Decimal("1e-18")),
        derivative_instrument.lot_size,
        "down",
    )


def _split_pair_close_batches(
    total_qty: Decimal,
    *,
    derivative_instrument: Instrument,
    batch_count: int = 1,
    batch_qty: Decimal | None = None,
) -> list[Decimal]:
    lot_size = derivative_instrument.lot_size
    min_size = derivative_instrument.min_size
    normalized_total = snap_to_increment(total_qty, lot_size, "down")
    if normalized_total <= 0:
        raise ValueError("总平仓数量按合约最小变动单位向下取整后为 0。")
    if batch_qty is not None:
        normalized_batch = snap_to_increment(batch_qty, lot_size, "down")
        if normalized_batch < min_size:
            raise ValueError("每批张数小于合约最小下单量。")
        batches: list[Decimal] = []
        remaining = normalized_total
        while remaining > 0:
            current = min(normalized_batch, remaining)
            current = snap_to_increment(current, lot_size, "down")
            if current < min_size:
                if not batches:
                    raise ValueError("剩余数量不足最小下单量。")
                batches[-1] += remaining
                remaining = Decimal("0")
                break
            batches.append(current)
            remaining -= current
        return batches
    if batch_count <= 1:
        return [normalized_total]
    base_batch = snap_to_increment(normalized_total / Decimal(batch_count), lot_size, "down")
    if base_batch < min_size:
        raise ValueError("分批次数过大，单批数量低于合约最小下单量。")
    batches = []
    remaining = normalized_total
    while len(batches) < batch_count - 1 and remaining - base_batch >= min_size:
        batches.append(base_batch)
        remaining -= base_batch
    if remaining < min_size:
        raise ValueError("剩余数量不足最小下单量，请减少分批次数。")
    batches.append(remaining)
    return batches


def _build_pair_close_strategy_config(position: OkxPosition, *, environment: str) -> StrategyConfig:
    normalized_mgn_mode = (position.mgn_mode or "").strip().lower()
    trade_mode = normalized_mgn_mode if normalized_mgn_mode in {"cross", "isolated", "cash"} else "cross"
    position_mode = "long_short" if position.pos_side and position.pos_side.lower() != "net" else "net"
    direction = _pair_position_direction(position)
    return StrategyConfig(
        inst_id=position.inst_id,
        bar="1m",
        ema_period=1,
        atr_period=1,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=abs(position.position),
        trade_mode=trade_mode,
        signal_mode="long_only" if direction == "long" else "short_only",
        position_mode=position_mode,
        environment=environment,  # type: ignore[arg-type]
        tp_sl_trigger_type="last",
        strategy_id="arbitrage_pair_flatten",
        poll_seconds=10.0,
        risk_amount=None,
        trade_inst_id=position.inst_id,
        tp_sl_mode="local_trade",
        local_tp_sl_inst_id=position.inst_id,
        entry_side_mode="follow_signal",
        run_mode="trade",
    )


def _credential_profile_names_from_snapshot(snapshot: dict[str, object]) -> list[str]:
    profiles = snapshot.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        return [DEFAULT_CREDENTIAL_PROFILE_NAME]
    return sorted(str(name).strip() for name in profiles.keys() if str(name).strip()) or [DEFAULT_CREDENTIAL_PROFILE_NAME]


def _credential_profile_environment(profile_snapshot: dict[str, str] | None, *, fallback: str = "demo") -> str:
    environment = str((profile_snapshot or {}).get("environment", "") or "").strip().lower()
    if environment in {"demo", "live"}:
        return environment
    return fallback if fallback in {"demo", "live"} else "demo"


def _environment_label(environment: str) -> str:
    return "实盘 live" if environment == "live" else "模拟盘 demo"


def _build_runtime_for_profile(
    profile_name: str,
    *,
    profile_snapshot: dict[str, str] | None,
    fallback_runtime: ArbitrageTradeRuntime | None,
) -> ArbitrageTradeRuntime | None:
    target_profile = profile_name.strip() or (
        fallback_runtime.credential_profile_name.strip() if fallback_runtime is not None else DEFAULT_CREDENTIAL_PROFILE_NAME
    )
    target_profile = target_profile or DEFAULT_CREDENTIAL_PROFILE_NAME
    snapshot = profile_snapshot or {}
    api_key = str(snapshot.get("api_key", "") or "").strip()
    secret_key = str(snapshot.get("secret_key", "") or "").strip()
    passphrase = str(snapshot.get("passphrase", "") or "").strip()
    if not api_key or not secret_key or not passphrase:
        if fallback_runtime is None:
            return None
        fallback_profile = fallback_runtime.credential_profile_name.strip() or target_profile
        if fallback_profile != target_profile:
            return None
        return ArbitrageTradeRuntime(
            credentials=Credentials(
                fallback_runtime.credentials.api_key,
                fallback_runtime.credentials.secret_key,
                fallback_runtime.credentials.passphrase,
                profile_name=target_profile,
            ),
            environment=fallback_runtime.environment,
            trade_mode=fallback_runtime.trade_mode,
            position_mode=fallback_runtime.position_mode,
            credential_profile_name=target_profile,
        )
    fallback_environment = fallback_runtime.environment if fallback_runtime is not None else "demo"
    environment = _credential_profile_environment(snapshot, fallback=fallback_environment)
    return ArbitrageTradeRuntime(
        credentials=Credentials(
            api_key,
            secret_key,
            passphrase,
            profile_name=target_profile,
        ),
        environment=environment,
        trade_mode=fallback_runtime.trade_mode if fallback_runtime is not None else "cross",
        position_mode=fallback_runtime.position_mode if fallback_runtime is not None else "net",
        credential_profile_name=target_profile,
    )


class ArbitrageWindow:
    def __init__(
        self,
        parent,
        client,
        *,
        runtime_config_provider: RuntimeConfigProvider | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.client = client
        self._runtime_config_provider = runtime_config_provider
        self._logger = logger or (lambda _message: None)
        self.manager = ArbitrageManager(client, logger=self._append_log)
        self._scan_thread: threading.Thread | None = None
        self._scan_busy = False
        self._destroying = False
        self._refresh_job: str | None = None
        self._monitor_job: str | None = None
        self._market_panel_job: str | None = None
        self._market_panel_refresh_busy = False
        self._market_panel_refresh_token = 0
        self._market_panel_refresh_interval_ms = MARKET_PANEL_REST_REFRESH_MS
        self._opportunities: list[ArbitrageOpportunity] = []
        self._scan_display_rows: list[ArbitrageOpportunity] = []
        self._selected_opportunity: ArbitrageOpportunity | None = None
        self._ledger_entries: list = []
        self._ledger_entry_by_id: dict[str, object] = {}
        self._open_ledger_entries: list = []
        self._close_entry_display_to_id: dict[str, str] = {}
        self._roll_entry_display_to_id: dict[str, str] = {}
        self._trade_tab = None
        self._close_tab = None
        self._chart_tab = None
        self._pair_close_tab = None
        self._roll_tab = None
        self._derivative_inst_id = StringVar(value="")
        self._close_entry_label = StringVar(value="")
        self._roll_entry_label = StringVar(value="")
        self.close_contract_qty = StringVar(value="")
        self.roll_target_derivative_inst_id = StringVar(value="")
        self.roll_contract_qty = StringVar(value="")
        self._chart_load_token = 0
        self._spot_chart_snapshot: StrategyLiveChartSnapshot | None = None
        self._derivative_chart_snapshot: StrategyLiveChartSnapshot | None = None
        self._spread_chart_snapshot: StrategyLiveChartSnapshot | None = None
        self._pair_close_positions: list[OkxPosition] = []
        self._pair_close_position_by_key: dict[str, OkxPosition] = {}
        self._pair_close_instruments: dict[str, Instrument] = {}
        self._pair_close_reference_prices: dict[str, Decimal] = {}
        self._roll_positions: list[OkxPosition] = []
        self._roll_position_by_key: dict[str, OkxPosition] = {}
        self._roll_instruments: dict[str, Instrument] = {}
        self._roll_reference_prices: dict[str, Decimal] = {}
        self._roll_spot_by_base: dict[str, OkxPosition] = {}
        self._roll_source_entry_id: str | None = None
        self._roll_future_instruments: list[Instrument] = []
        self._pair_close_spot_key = StringVar(value="")
        self._pair_close_derivative_key = StringVar(value="")
        self.pair_close_derivative_qty = StringVar(value="")
        self._pair_close_auto_thread: threading.Thread | None = None
        self._pair_close_auto_stop_event = threading.Event()
        self._pair_close_auto_session: PairCloseAutoSession | None = None
        self._market_instrument_cache: dict[str, Instrument] = {}
        self._trade_market_panel: ArbitrageMarketPanel | None = None
        self._close_market_panel: ArbitrageMarketPanel | None = None
        self._pair_close_market_panel: ArbitrageMarketPanel | None = None
        self._roll_market_panel: ArbitrageMarketPanel | None = None

        settings = load_arbitrage_settings_snapshot()
        profiles_snapshot = load_credentials_profiles_snapshot()
        self._api_profile_names = _credential_profile_names_from_snapshot(profiles_snapshot)
        runtime_profile_name = ""
        if self._runtime_config_provider is not None:
            try:
                runtime = self._runtime_config_provider()
            except Exception:
                runtime = None
            if runtime is not None:
                runtime_profile_name = runtime.credential_profile_name.strip()
        selected_api_profile = str(settings.get("api_profile_name", "") or "").strip()
        if not selected_api_profile:
            selected_api_profile = runtime_profile_name or str(profiles_snapshot.get("selected_profile", "") or "").strip()
        if selected_api_profile not in self._api_profile_names:
            selected_api_profile = self._api_profile_names[0]
        self.api_profile_name = StringVar(value=selected_api_profile)
        self.api_environment_text = StringVar(value="")
        self._api_profile_combo: ttk.Combobox | None = None
        self._last_api_profile_name = selected_api_profile
        self.auto_refresh_enabled = BooleanVar(value=bool(settings.get("auto_refresh_enabled", True)))
        self.alert_enabled = BooleanVar(value=bool(settings.get("alert_enabled", True)))
        self.scan_swap_enabled = BooleanVar(value=bool(settings.get("scan_swap_enabled", True)))
        self.scan_futures_enabled = BooleanVar(value=bool(settings.get("scan_futures_enabled", True)))
        self.scan_base_filter = StringVar(value=str(settings.get("scan_base_filter", "全部")))
        self._scan_base_filter_combo: ttk.Combobox | None = None
        self._scan_sort_column = str(settings.get("scan_sort_column", "net"))
        self._scan_sort_desc = bool(settings.get("scan_sort_desc", True))
        self.min_annual_threshold = StringVar(value=str(settings.get("min_annual_threshold", "5")))
        self.base_ccy = StringVar(value=str(settings.get("base_ccy", "BTC")))
        self.size_value = StringVar(value=str(settings.get("size_value", "1000")))
        self.size_unit_label = StringVar(value=str(settings.get("size_unit_label", "USDT金额")))
        self.max_slippage_percent = StringVar(value=str(settings.get("max_slippage_percent", "0.15")))
        self.trigger_mode_label = StringVar(value=str(settings.get("trigger_mode_label", "绝对价差触发")))
        self.open_spread_pct_max = StringVar(value=str(settings.get("open_spread_pct_max", "0.05")))
        self.open_spread_abs_max = StringVar(value=str(settings.get("open_spread_abs_max", "0.05")))
        self.close_trigger_mode_label = StringVar(value=str(settings.get("close_trigger_mode_label", "绝对价差")))
        self.close_spread_pct_min = StringVar(value=str(settings.get("close_spread_pct_min", "0.10")))
        self.close_spread_abs_min = StringVar(value=str(settings.get("close_spread_abs_min", "0.10")))
        self.pair_close_trigger_mode_label = StringVar(
            value=str(settings.get("pair_close_trigger_mode_label", settings.get("close_trigger_mode_label", "绝对价差")))
        )
        self.pair_close_spread_pct_min = StringVar(
            value=str(settings.get("pair_close_spread_pct_min", settings.get("close_spread_pct_min", "0.10")))
        )
        self.pair_close_spread_abs_min = StringVar(
            value=str(settings.get("pair_close_spread_abs_min", settings.get("close_spread_abs_min", "0.10")))
        )
        self.pair_close_batch_count = StringVar(value=str(settings.get("pair_close_batch_count", "1")))
        self.pair_close_batch_qty = StringVar(value=str(settings.get("pair_close_batch_qty", "")))
        self.open_batch_count = StringVar(value=str(settings.get("open_batch_count", "1")))
        self.open_batch_qty = StringVar(value=str(settings.get("open_batch_qty", "")))
        self.open_execution_mode_label = StringVar(value=str(settings.get("open_execution_mode_label", "双腿吃单")))
        self.open_maker_wait_seconds = StringVar(value=str(settings.get("open_maker_wait_seconds", "6")))
        self.open_chase_limit = StringVar(value=str(settings.get("open_chase_limit", "3")))
        self.close_batch_count = StringVar(value=str(settings.get("close_batch_count", "1")))
        self.close_batch_qty = StringVar(value=str(settings.get("close_batch_qty", "")))
        self.close_execution_mode_label = StringVar(value=str(settings.get("close_execution_mode_label", "双腿吃单")))
        self.close_maker_wait_seconds = StringVar(value=str(settings.get("close_maker_wait_seconds", "6")))
        self.close_chase_limit = StringVar(value=str(settings.get("close_chase_limit", "3")))
        self.roll_batch_count = StringVar(value=str(settings.get("roll_batch_count", "1")))
        self.roll_batch_qty = StringVar(value=str(settings.get("roll_batch_qty", "")))
        self.roll_execution_mode_label = StringVar(value=str(settings.get("roll_execution_mode_label", "双腿吃单")))
        self.roll_maker_wait_seconds = StringVar(value=str(settings.get("roll_maker_wait_seconds", "6")))
        self.roll_chase_limit = StringVar(value=str(settings.get("roll_chase_limit", "3")))
        self.pair_close_execution_mode_label = StringVar(
            value=str(settings.get("pair_close_execution_mode_label", "双腿吃单"))
        )
        self.pair_close_maker_wait_seconds = StringVar(value=str(settings.get("pair_close_maker_wait_seconds", "6")))
        self.pair_close_chase_limit = StringVar(value=str(settings.get("pair_close_chase_limit", "3")))
        self.spot_limit_price = StringVar(value=str(settings.get("spot_limit_price", "")))
        self.derivative_limit_price = StringVar(value=str(settings.get("derivative_limit_price", "")))
        self.roll_current_limit_price = StringVar(value=str(settings.get("roll_current_limit_price", "")))
        self.roll_target_limit_price = StringVar(value=str(settings.get("roll_target_limit_price", "")))
        self.use_limit_orders = BooleanVar(value=bool(settings.get("use_limit_orders", False)))
        self.chart_bar = StringVar(value=str(settings.get("chart_bar", "15m")))
        self.chart_candle_limit = StringVar(value=str(settings.get("chart_candle_limit", "240")))
        self.chart_spot_inst_id = StringVar(value=str(settings.get("chart_spot_inst_id", "")))
        self.chart_derivative_inst_id = StringVar(value=str(settings.get("chart_derivative_inst_id", "")))
        self._open_trigger_threshold_label_text = StringVar(value="开仓价差率 <")
        self._open_trigger_threshold_unit_text = StringVar(value="%")
        self._open_close_threshold_label_text = StringVar(value="平仓价差率 <")
        self._open_close_threshold_unit_text = StringVar(value="%")
        self._close_tab_threshold_label_text = StringVar(value="价差率 <")
        self._close_tab_threshold_unit_text = StringVar(value="% 时触发")
        self._pair_close_threshold_label_text = StringVar(value="价差率 <")
        self._pair_close_threshold_unit_text = StringVar(value="% 时执行")

        self.status_text = StringVar(value="套利模块已就绪。")
        self.scan_status_text = StringVar(value="尚未扫描。")
        self.preview_text = StringVar(value="选择机会或填写参数后，可预览现货/合约换算。")
        self.close_position_summary_text = StringVar(value="暂无未平仓套利持仓。")
        self.close_preview_text = StringVar(value="请先选择一条未平仓套利持仓。")
        self.roll_position_summary_text = StringVar(value="请先选择一条未平仓交割合约持仓。")
        self.roll_preview_text = StringVar(value="请选择当前交割合约持仓，并填写更远交割合约。")
        self.roll_status_text = StringVar(value="尚未执行移仓。")
        self.chart_status_text = StringVar(value="选择现货/衍生品后加载 K 线。")
        self.spot_chart_status_text = StringVar(value="现货图未加载")
        self.derivative_chart_status_text = StringVar(value="衍生品图未加载")
        self.spread_chart_status_text = StringVar(value="价差图未加载")
        self.pair_close_status_text = StringVar(value="请先刷新并选择一组当前持仓。")
        self.pair_close_preview_text = StringVar(value="选择现货和交割/永续持仓后，这里会显示本次可平数量。")
        self.trade_status_text = StringVar(value="未启动自动开仓。")
        self.monitor_status_text = StringVar(value="—")

        self.window = Toplevel(parent)
        self.window.title("现货套利")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.86,
            height_ratio=0.84,
            min_width=1280,
            min_height=820,
            max_width=1800,
            max_height=1100,
        )
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)
        self._style = ttk.Style(self.window)
        self._style.configure("Hint.TLabel", foreground="#666666")
        self._build_layout()
        self._sync_api_profile_controls()
        self._sync_spread_trigger_controls()
        self._sync_pair_close_trigger_controls()
        self._reload_ledger()
        self._schedule_scan(refresh_only=False)
        if self.auto_refresh_enabled.get():
            self._schedule_auto_refresh()
        self._schedule_monitor_refresh()
        self._schedule_market_panel_refresh(initial_delay_ms=200)

    def show(self) -> None:
        self._sync_api_profile_controls()
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def destroy(self) -> None:
        self._destroying = True
        try:
            self.manager.stop_auto_open()
            self.manager.stop_auto_close()
            self._stop_pair_close_auto(silent=True)
        except Exception:
            pass
        if self._refresh_job is not None:
            try:
                self.window.after_cancel(self._refresh_job)
            except Exception:
                pass
            self._refresh_job = None
        if self._monitor_job is not None:
            try:
                self.window.after_cancel(self._monitor_job)
            except Exception:
                pass
            self._monitor_job = None
        if self._market_panel_job is not None:
            try:
                self.window.after_cancel(self._market_panel_job)
            except Exception:
                pass
            self._market_panel_job = None
        if self.window.winfo_exists():
            self.window.destroy()

    def _on_close(self) -> None:
        self._save_settings()
        self.destroy()

    def _save_settings(self) -> None:
        save_arbitrage_settings_snapshot(
            {
                "auto_refresh_enabled": self.auto_refresh_enabled.get(),
                "alert_enabled": self.alert_enabled.get(),
                "scan_swap_enabled": self.scan_swap_enabled.get(),
                "scan_futures_enabled": self.scan_futures_enabled.get(),
                "scan_base_filter": self.scan_base_filter.get().strip(),
                "scan_sort_column": self._scan_sort_column,
                "scan_sort_desc": self._scan_sort_desc,
                "min_annual_threshold": self.min_annual_threshold.get().strip(),
                "base_ccy": self.base_ccy.get().strip().upper(),
                "size_value": self.size_value.get().strip(),
                "size_unit_label": self.size_unit_label.get().strip(),
                "max_slippage_percent": self.max_slippage_percent.get().strip(),
                "trigger_mode_label": self.trigger_mode_label.get().strip(),
                "open_spread_pct_max": self.open_spread_pct_max.get().strip(),
                "open_spread_abs_max": self.open_spread_abs_max.get().strip(),
                "close_trigger_mode_label": self.close_trigger_mode_label.get().strip(),
                "close_spread_pct_min": self.close_spread_pct_min.get().strip(),
                "close_spread_abs_min": self.close_spread_abs_min.get().strip(),
                "pair_close_trigger_mode_label": self.pair_close_trigger_mode_label.get().strip(),
                "pair_close_spread_pct_min": self.pair_close_spread_pct_min.get().strip(),
                "pair_close_spread_abs_min": self.pair_close_spread_abs_min.get().strip(),
                "pair_close_batch_count": self.pair_close_batch_count.get().strip(),
                "pair_close_batch_qty": self.pair_close_batch_qty.get().strip(),
                "open_batch_count": self.open_batch_count.get().strip(),
                "open_batch_qty": self.open_batch_qty.get().strip(),
                "open_execution_mode_label": self.open_execution_mode_label.get().strip(),
                "open_maker_wait_seconds": self.open_maker_wait_seconds.get().strip(),
                "open_chase_limit": self.open_chase_limit.get().strip(),
                "close_batch_count": self.close_batch_count.get().strip(),
                "close_batch_qty": self.close_batch_qty.get().strip(),
                "close_execution_mode_label": self.close_execution_mode_label.get().strip(),
                "close_maker_wait_seconds": self.close_maker_wait_seconds.get().strip(),
                "close_chase_limit": self.close_chase_limit.get().strip(),
                "roll_batch_count": self.roll_batch_count.get().strip(),
                "roll_batch_qty": self.roll_batch_qty.get().strip(),
                "roll_execution_mode_label": self.roll_execution_mode_label.get().strip(),
                "roll_maker_wait_seconds": self.roll_maker_wait_seconds.get().strip(),
                "roll_chase_limit": self.roll_chase_limit.get().strip(),
                "pair_close_execution_mode_label": self.pair_close_execution_mode_label.get().strip(),
                "pair_close_maker_wait_seconds": self.pair_close_maker_wait_seconds.get().strip(),
                "pair_close_chase_limit": self.pair_close_chase_limit.get().strip(),
                "spot_limit_price": self.spot_limit_price.get().strip(),
                "derivative_limit_price": self.derivative_limit_price.get().strip(),
                "roll_current_limit_price": self.roll_current_limit_price.get().strip(),
                "roll_target_limit_price": self.roll_target_limit_price.get().strip(),
                "use_limit_orders": self.use_limit_orders.get(),
                "chart_bar": self.chart_bar.get().strip(),
                "chart_candle_limit": self.chart_candle_limit.get().strip(),
                "chart_spot_inst_id": self.chart_spot_inst_id.get().strip().upper(),
                "chart_derivative_inst_id": self.chart_derivative_inst_id.get().strip().upper(),
                "api_profile_name": self.api_profile_name.get().strip(),
            }
        )

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(1, weight=1)

        header = ttk.Frame(self.window, padding=(14, 12, 14, 8))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)
        header.columnconfigure(2, weight=1)
        ttk.Label(header, text="现货套利 V1.1", font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        api_wrap = ttk.Frame(header)
        api_wrap.grid(row=0, column=1, sticky="e", padx=(12, 12))
        ttk.Label(api_wrap, text="API").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._api_profile_combo = ttk.Combobox(
            api_wrap,
            textvariable=self.api_profile_name,
            values=self._api_profile_names,
            state="readonly",
            width=12,
        )
        self._api_profile_combo.grid(row=0, column=1, sticky="w")
        self._api_profile_combo.bind("<<ComboboxSelected>>", self._on_api_profile_selected)
        ttk.Label(api_wrap, textvariable=self.api_environment_text).grid(row=0, column=2, sticky="w", padx=(8, 0))
        ttk.Label(header, textvariable=self.status_text).grid(row=0, column=2, sticky="e")

        notebook = ttk.Notebook(self.window)
        notebook.grid(row=1, column=0, sticky="nsew", padx=14, pady=(0, 12))
        self._notebook = notebook
        self._notebook.bind("<<NotebookTabChanged>>", self._on_notebook_tab_changed)

        self._build_scan_tab(notebook)
        self._build_chart_tab(notebook)
        self._build_trade_tab(notebook)
        self._build_pair_close_tab(notebook)
        self._build_roll_tab(notebook)
        self._build_close_tab(notebook)
        self._build_ledger_tab(notebook)
        self._build_log_tab(notebook)

    def _add_inline_hint(
        self,
        parent,
        *,
        row: int,
        text: str,
        wraplength: int,
        column: int = 1,
        columnspan: int = 1,
        pady: tuple[int, int] = (0, 4),
    ) -> None:
        ttk.Label(
            parent,
            text=text,
            style="Hint.TLabel",
            wraplength=wraplength,
            justify="left",
        ).grid(row=row, column=column, columnspan=columnspan, sticky="w", pady=pady)

    def _build_market_book_tree(self, parent) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=("price", "qty"), show="headings", height=20)
        tree.heading("price", text="价格")
        tree.heading("qty", text="数量")
        tree.column("price", width=110, anchor="e", stretch=True)
        tree.column("qty", width=110, anchor="e", stretch=True)
        tree.tag_configure("ask", foreground="#d14b4b")
        tree.tag_configure("bid", foreground="#0f8a63")
        return tree

    def _build_dual_market_panel(self, parent, *, row: int, title: str, wraplength: int) -> ArbitrageMarketPanel:
        parent.rowconfigure(row, weight=1)
        frame = ttk.LabelFrame(parent, text=title, padding=8)
        frame.grid(row=row, column=0, columnspan=2, sticky="nsew", pady=(10, 4))
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(0, weight=1)

        spot_title_text = StringVar(value="左腿盘口")
        spot_quote_text = StringVar(value="等待选择产品…")
        derivative_title_text = StringVar(value="右腿盘口")
        derivative_quote_text = StringVar(value="等待选择产品…")
        spread_text = StringVar(value="价差：-")
        detail_text = StringVar(value="")
        status_text = StringVar(value="选择产品后自动显示盘口。")

        left = ttk.Frame(frame)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(2, weight=1)
        ttk.Label(left, textvariable=spot_title_text).grid(row=0, column=0, sticky="w")
        ttk.Label(left, textvariable=spot_quote_text, style="Hint.TLabel", wraplength=wraplength // 2, justify="left").grid(
            row=1, column=0, sticky="w", pady=(2, 6)
        )
        spot_tree = self._build_market_book_tree(left)
        spot_tree.grid(row=2, column=0, sticky="nsew")

        right = ttk.Frame(frame)
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)
        ttk.Label(right, textvariable=derivative_title_text).grid(row=0, column=0, sticky="w")
        ttk.Label(
            right,
            textvariable=derivative_quote_text,
            style="Hint.TLabel",
            wraplength=wraplength // 2,
            justify="left",
        ).grid(row=1, column=0, sticky="w", pady=(2, 6))
        derivative_tree = self._build_market_book_tree(right)
        derivative_tree.grid(row=2, column=0, sticky="nsew")

        ttk.Label(frame, textvariable=spread_text).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 2))
        ttk.Label(frame, textvariable=detail_text, style="Hint.TLabel", wraplength=wraplength, justify="left").grid(
            row=2, column=0, columnspan=2, sticky="w"
        )
        ttk.Label(frame, textvariable=status_text, style="Hint.TLabel", wraplength=wraplength, justify="left").grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(2, 0)
        )

        return ArbitrageMarketPanel(
            spot_title_text=spot_title_text,
            spot_quote_text=spot_quote_text,
            derivative_title_text=derivative_title_text,
            derivative_quote_text=derivative_quote_text,
            spread_text=spread_text,
            detail_text=detail_text,
            status_text=status_text,
            spot_tree=spot_tree,
            derivative_tree=derivative_tree,
        )

    def _build_scan_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="机会扫描")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew")
        for col in range(13):
            controls.columnconfigure(col, weight=1 if col in {8, 10} else 0)

        ttk.Button(controls, text="立即扫描", command=lambda: self._schedule_scan(refresh_only=False)).grid(
            row=0, column=0, sticky="w", padx=(0, 8)
        )
        ttk.Label(controls, text="类型").grid(row=0, column=1, sticky="e", padx=(6, 4))
        ttk.Checkbutton(controls, text="永续", variable=self.scan_swap_enabled).grid(row=0, column=2, sticky="w")
        ttk.Checkbutton(controls, text="交割", variable=self.scan_futures_enabled).grid(row=0, column=3, sticky="w")
        ttk.Label(controls, text="币种").grid(row=0, column=4, sticky="e", padx=(10, 4))
        self._scan_base_filter_combo = ttk.Combobox(
            controls,
            textvariable=self.scan_base_filter,
            values=("全部",),
            state="readonly",
            width=10,
        )
        self._scan_base_filter_combo.grid(row=0, column=5, sticky="w")
        self._scan_base_filter_combo.bind("<<ComboboxSelected>>", self._on_scan_filter_changed)
        ttk.Checkbutton(controls, text="自动刷新(5s)", variable=self.auto_refresh_enabled).grid(
            row=0, column=6, sticky="w", padx=(8, 8)
        )
        ttk.Checkbutton(controls, text="超阈值弹窗", variable=self.alert_enabled).grid(row=0, column=9, sticky="w")
        ttk.Label(controls, text="净年化阈值(%)").grid(row=0, column=10, sticky="e", padx=(12, 4))
        ttk.Entry(controls, textvariable=self.min_annual_threshold, width=8).grid(row=0, column=11, sticky="w")
        ttk.Button(controls, text="一键建仓", command=self._open_trade_from_selection).grid(
            row=0, column=12, sticky="e"
        )

        ttk.Label(frame, textvariable=self.scan_status_text).grid(row=1, column=0, sticky="w", pady=(8, 6))

        columns = (
            "base",
            "kind",
            "spot",
            "derivative",
            "basis_abs",
            "basis",
            "funding",
            "fee",
            "slippage",
            "net",
            "expiry",
        )
        tree_wrap = ttk.Frame(frame)
        tree_wrap.grid(row=2, column=0, sticky="nsew")
        tree_wrap.columnconfigure(0, weight=1)
        tree_wrap.rowconfigure(0, weight=1)
        self.scan_tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", height=18)
        headings = {
            "base": "币种",
            "kind": "类型",
            "spot": "现货",
            "derivative": "衍生品",
            "basis_abs": "绝对价差",
            "basis": "基差%",
            "funding": "资金费年化%",
            "fee": "手续费%",
            "slippage": "滑点%",
            "net": "净年化%",
            "expiry": "到期天数",
        }
        for key, label in headings.items():
            self.scan_tree.heading(key, text=label, command=lambda current=key: self._sort_scan_by(current))
        widths = {
            "base": 70,
            "kind": 100,
            "spot": 120,
            "derivative": 150,
            "basis_abs": 90,
            "basis": 80,
            "funding": 100,
            "fee": 80,
            "slippage": 80,
            "net": 90,
            "expiry": 80,
        }
        for key, width in widths.items():
            anchor = "e" if key in {"basis_abs", "basis", "funding", "fee", "slippage", "net", "expiry"} else "w"
            self.scan_tree.column(key, width=width, anchor=anchor, stretch=key in {"spot", "derivative"})
        scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.scan_tree.yview)
        self.scan_tree.configure(yscrollcommand=scroll.set)
        self.scan_tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        self.scan_tree.bind("<<TreeviewSelect>>", self._on_scan_select)
        self.scan_tree.bind("<Double-1>", lambda _event: self._open_trade_from_selection())

    def _build_chart_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        self._chart_tab = frame
        notebook.add(frame, text="套利图表")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(2, weight=1)
        frame.rowconfigure(3, weight=1)

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew")
        for col in range(10):
            controls.columnconfigure(col, weight=1 if col in {1, 3} else 0)

        ttk.Label(controls, text="现货").grid(row=0, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(controls, textvariable=self.chart_spot_inst_id, width=18).grid(row=0, column=1, sticky="ew")
        ttk.Label(controls, text="衍生品").grid(row=0, column=2, sticky="e", padx=(10, 4))
        ttk.Entry(controls, textvariable=self.chart_derivative_inst_id, width=20).grid(row=0, column=3, sticky="ew")
        ttk.Label(controls, text="周期").grid(row=0, column=4, sticky="e", padx=(10, 4))
        self.chart_bar_combo = ttk.Combobox(
            controls,
            textvariable=self.chart_bar,
            values=_CHART_BAR_OPTIONS,
            state="readonly",
            width=8,
        )
        self.chart_bar_combo.grid(row=0, column=5, sticky="w")
        ttk.Label(controls, text="根数").grid(row=0, column=6, sticky="e", padx=(10, 4))
        ttk.Entry(controls, textvariable=self.chart_candle_limit, width=8).grid(row=0, column=7, sticky="w")
        ttk.Button(controls, text="从扫描带入", command=self._open_chart_from_selection).grid(row=0, column=8, padx=(10, 6))
        ttk.Button(controls, text="加载图表", command=self._load_arbitrage_charts).grid(row=0, column=9, sticky="e")

        ttk.Label(frame, textvariable=self.chart_status_text, wraplength=1160, justify="left").grid(
            row=1,
            column=0,
            sticky="w",
            pady=(8, 8),
        )

        top_charts = ttk.Frame(frame)
        top_charts.grid(row=2, column=0, sticky="nsew")
        top_charts.columnconfigure(0, weight=1)
        top_charts.columnconfigure(1, weight=1)
        top_charts.rowconfigure(0, weight=1)

        spot_wrap = ttk.LabelFrame(top_charts, text="现货 K 线", padding=8)
        spot_wrap.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        spot_wrap.columnconfigure(0, weight=1)
        spot_wrap.rowconfigure(1, weight=1)
        ttk.Label(spot_wrap, textvariable=self.spot_chart_status_text).grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.spot_chart_canvas = Canvas(spot_wrap, background="#ffffff", highlightthickness=1, highlightbackground="#d9dee7")
        self.spot_chart_canvas.grid(row=1, column=0, sticky="nsew")

        deriv_wrap = ttk.LabelFrame(top_charts, text="衍生品 K 线", padding=8)
        deriv_wrap.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        deriv_wrap.columnconfigure(0, weight=1)
        deriv_wrap.rowconfigure(1, weight=1)
        ttk.Label(deriv_wrap, textvariable=self.derivative_chart_status_text).grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.derivative_chart_canvas = Canvas(
            deriv_wrap,
            background="#ffffff",
            highlightthickness=1,
            highlightbackground="#d9dee7",
        )
        self.derivative_chart_canvas.grid(row=1, column=0, sticky="nsew")

        spread_wrap = ttk.LabelFrame(frame, text="价差 K 线", padding=8)
        spread_wrap.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        spread_wrap.columnconfigure(0, weight=1)
        spread_wrap.rowconfigure(1, weight=1)
        ttk.Label(spread_wrap, textvariable=self.spread_chart_status_text).grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.spread_chart_canvas = Canvas(
            spread_wrap,
            background="#ffffff",
            highlightthickness=1,
            highlightbackground="#d9dee7",
        )
        self.spread_chart_canvas.grid(row=1, column=0, sticky="nsew")

        for canvas in (self.spot_chart_canvas, self.derivative_chart_canvas, self.spread_chart_canvas):
            canvas.bind("<Configure>", self._on_arbitrage_chart_canvas_configure, add="+")

    def _build_trade_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        self._trade_tab = frame
        notebook.add(frame, text="套利开仓")
        frame.columnconfigure(1, weight=1)

        row = 0
        ttk.Label(frame, text="币种").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.base_ccy, width=16).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frame, text="衍生品").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self._derivative_inst_id, width=36).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frame, text="投入数量").grid(row=row, column=0, sticky="w", pady=4)
        qty_row = ttk.Frame(frame)
        qty_row.grid(row=row, column=1, sticky="ew", pady=4)
        ttk.Entry(qty_row, textvariable=self.size_value, width=16).pack(side="left")
        ttk.Combobox(
            qty_row,
            textvariable=self.size_unit_label,
            values=list(_SIZE_UNIT_OPTIONS.keys()),
            state="readonly",
            width=12,
        ).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="触发方式").grid(row=row, column=0, sticky="w", pady=4)
        trigger_row = ttk.Frame(frame)
        trigger_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            trigger_row,
            textvariable=self.trigger_mode_label,
            values=list(_TRIGGER_MODE_OPTIONS.keys()),
            state="readonly",
            width=14,
        ).pack(side="left")
        trigger_mode_combo = trigger_row.winfo_children()[-1]
        trigger_mode_combo.bind("<<ComboboxSelected>>", self._on_trigger_mode_changed)
        ttk.Label(trigger_row, textvariable=self._open_trigger_threshold_label_text).pack(side="left", padx=(12, 4))
        self._open_trigger_threshold_entry = ttk.Entry(trigger_row, textvariable=self.open_spread_pct_max, width=8)
        self._open_trigger_threshold_entry.pack(side="left")
        ttk.Label(trigger_row, textvariable=self._open_trigger_threshold_unit_text).pack(side="left", padx=(2, 0))
        ttk.Label(trigger_row, textvariable=self._open_close_threshold_label_text).pack(side="left", padx=(12, 4))
        self._open_close_threshold_entry = ttk.Entry(trigger_row, textvariable=self.close_spread_pct_min, width=8)
        self._open_close_threshold_entry.pack(side="left")
        ttk.Label(trigger_row, textvariable=self._open_close_threshold_unit_text).pack(side="left", padx=(2, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="常用填法：先用“绝对价差触发”。开仓阈值写你愿意入场的最小价差，后面的平仓阈值写收敛后的目标值。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="限价条件").grid(row=row, column=0, sticky="w", pady=4)
        limit_row = ttk.Frame(frame)
        limit_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(limit_row, text="现货买入 ≤").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.spot_limit_price, width=12).pack(side="left", padx=(4, 12))
        ttk.Label(limit_row, text="合约卖出 ≥").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.derivative_limit_price, width=12).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="执行方式").grid(row=row, column=0, sticky="w", pady=4)
        exec_row = ttk.Frame(frame)
        exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Checkbutton(exec_row, text="触发后按限价挂单", variable=self.use_limit_orders).pack(side="left", padx=(0, 12))
        ttk.Label(exec_row, text="最大滑点(%)").pack(side="left")
        ttk.Entry(exec_row, textvariable=self.max_slippage_percent, width=8).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="分批执行").grid(row=row, column=0, sticky="w", pady=4)
        batch_row = ttk.Frame(frame)
        batch_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(batch_row, text="分批次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.open_batch_count, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="每批张数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.open_batch_qty, width=8).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="开仓分批按合约张数拆分。数量单位就算选了币数或 USDT，系统也会先换算总张数，再按这里的分批次数或每批张数执行。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="双腿执行").grid(row=row, column=0, sticky="w", pady=4)
        dual_exec_row = ttk.Frame(frame)
        dual_exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            dual_exec_row,
            textvariable=self.open_execution_mode_label,
            values=list(_ARBITRAGE_EXECUTION_MODE_OPTIONS.keys()),
            state="readonly",
            width=22,
        ).pack(side="left")
        ttk.Label(dual_exec_row, text="挂单等待(s)").pack(side="left", padx=(10, 0))
        ttk.Entry(dual_exec_row, textvariable=self.open_maker_wait_seconds, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(dual_exec_row, text="追单次数").pack(side="left")
        ttk.Entry(dual_exec_row, textvariable=self.open_chase_limit, width=6).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="想稳一点可以选“现货挂单/合约吃单”或“合约挂单/现货吃单”。挂单腿会等、会追；双腿吃单时仍按滑点控制，‘按限价挂单’主要作用在双腿吃单路径。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="方向").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(frame, text="正向套利：买现货 + 空衍生品（Spot → Swap，Delta 中性）").grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        ttk.Label(frame, text="换算预览").grid(row=row, column=0, sticky="nw", pady=(12, 4))
        ttk.Label(frame, textvariable=self.preview_text, wraplength=760, justify="left").grid(
            row=row, column=1, sticky="w", pady=(12, 4)
        )
        row += 1

        self._trade_market_panel = self._build_dual_market_panel(
            frame,
            row=row,
            title="双腿盘口 / 价差",
            wraplength=920,
        )
        row += 1

        ttk.Label(frame, text="监控状态").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Label(frame, textvariable=self.monitor_status_text).grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=row, column=1, sticky="w", pady=(8, 4))
        ttk.Button(btn_row, text="刷新预览", command=self._refresh_preview).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动开仓", command=self._start_auto_open).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="立即开仓", command=self._open_now).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="停止监控", command=self._stop_auto_open).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动平仓", command=self._start_auto_close).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="全部平仓", command=self._close_all).pack(side="left")
        row += 1

        ttk.Label(frame, textvariable=self.trade_status_text, wraplength=760, justify="left").grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(12, 0)
        )
        base = self.base_ccy.get().strip().upper() or "BTC"
        self._derivative_inst_id.set(f"{base}-USDT-SWAP")
        if not self.chart_spot_inst_id.get().strip():
            self.chart_spot_inst_id.set(f"{base}-USDT")
        if not self.chart_derivative_inst_id.get().strip():
            self.chart_derivative_inst_id.set(f"{base}-USDT-SWAP")

    def _build_pair_close_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        self._pair_close_tab = frame
        notebook.add(frame, text="持仓配对平仓")
        frame.columnconfigure(1, weight=1)

        row = 0
        toolbar = ttk.Frame(frame)
        toolbar.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Button(toolbar, text="刷新当前持仓", command=self._refresh_pair_close_positions).pack(side="left")
        row += 1

        ttk.Label(frame, text="现货持仓").grid(row=row, column=0, sticky="w", pady=4)
        self.pair_close_spot_combo = ttk.Combobox(
            frame,
            textvariable=self._pair_close_spot_key,
            state="readonly",
            width=64,
        )
        self.pair_close_spot_combo.grid(row=row, column=1, sticky="ew", pady=4)
        self.pair_close_spot_combo.bind("<<ComboboxSelected>>", self._on_pair_close_selection_changed)
        row += 1

        ttk.Label(frame, text="交割/永续持仓").grid(row=row, column=0, sticky="w", pady=4)
        self.pair_close_derivative_combo = ttk.Combobox(
            frame,
            textvariable=self._pair_close_derivative_key,
            state="readonly",
            width=64,
        )
        self.pair_close_derivative_combo.grid(row=row, column=1, sticky="ew", pady=4)
        self.pair_close_derivative_combo.bind("<<ComboboxSelected>>", self._on_pair_close_selection_changed)
        row += 1

        ttk.Label(frame, text="本次平仓").grid(row=row, column=0, sticky="w", pady=4)
        qty_row = ttk.Frame(frame)
        qty_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Entry(qty_row, textvariable=self.pair_close_derivative_qty, width=16).pack(side="left")
        ttk.Label(qty_row, text="张").pack(side="left", padx=(6, 0))
        ttk.Button(qty_row, text="最大", command=self._fill_pair_close_qty_max).pack(side="left", padx=(10, 0))
        ttk.Button(qty_row, text="刷新预览", command=self._refresh_pair_close_preview).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="配对预览").grid(row=row, column=0, sticky="nw", pady=(8, 4))
        ttk.Label(frame, textvariable=self.pair_close_preview_text, wraplength=860, justify="left").grid(
            row=row,
            column=1,
            sticky="w",
            pady=(8, 4),
        )
        row += 1

        self._pair_close_market_panel = self._build_dual_market_panel(
            frame,
            row=row,
            title="双腿盘口 / 价差",
            wraplength=980,
        )
        row += 1

        ttk.Label(frame, text="自动平仓").grid(row=row, column=0, sticky="w", pady=4)
        auto_row = ttk.Frame(frame)
        auto_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            auto_row,
            textvariable=self.pair_close_trigger_mode_label,
            values=list(_CLOSE_TRIGGER_MODE_OPTIONS.keys()),
            state="readonly",
            width=10,
        ).pack(side="left")
        pair_close_mode_combo = auto_row.winfo_children()[-1]
        pair_close_mode_combo.bind("<<ComboboxSelected>>", self._on_pair_close_trigger_mode_changed)
        ttk.Label(auto_row, textvariable=self._pair_close_threshold_label_text).pack(side="left", padx=(10, 0))
        self._pair_close_threshold_entry = ttk.Entry(auto_row, textvariable=self.pair_close_spread_pct_min, width=8)
        self._pair_close_threshold_entry.pack(side="left", padx=(4, 0))
        ttk.Label(auto_row, textvariable=self._pair_close_threshold_unit_text).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="推荐先用“绝对价差”，意思是价差回落到你设定的值以下再自动平，不是越大越好。",
            wraplength=860,
        )
        row += 1

        ttk.Label(frame, text="分批执行").grid(row=row, column=0, sticky="w", pady=4)
        batch_row = ttk.Frame(frame)
        batch_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(batch_row, text="分批次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.pair_close_batch_count, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="每批张数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.pair_close_batch_qty, width=8).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="执行方式").pack(side="left")
        ttk.Combobox(
            batch_row,
            textvariable=self.pair_close_execution_mode_label,
            values=list(_PAIR_CLOSE_EXECUTION_MODE_OPTIONS.keys()),
            state="readonly",
            width=18,
        ).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="挂单等待(s)").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.pair_close_maker_wait_seconds, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="追单次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.pair_close_chase_limit, width=6).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="新手建议：先把分批次数设为 1、每批张数留空、执行方式选“双腿吃单”。只有总量较大时再考虑拆批。",
            wraplength=860,
        )
        row += 1

        ttk.Label(frame, text="执行方式").grid(row=row, column=0, sticky="w", pady=4)
        exec_row = ttk.Frame(frame)
        exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Checkbutton(exec_row, text="按限价挂单", variable=self.use_limit_orders).pack(side="left", padx=(0, 12))
        ttk.Label(exec_row, text="最大滑点(%)").pack(side="left")
        ttk.Entry(exec_row, textvariable=self.max_slippage_percent, width=8).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="想更快成交可以不勾“按限价挂单”。挂单等待和追单次数主要在挂单模式下才重要；最大滑点留默认 0.15 即可。",
            wraplength=860,
        )
        row += 1

        ttk.Label(frame, text="限价条件").grid(row=row, column=0, sticky="w", pady=4)
        limit_row = ttk.Frame(frame)
        limit_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(limit_row, text="现货平仓价").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.spot_limit_price, width=12).pack(side="left", padx=(4, 12))
        ttk.Label(limit_row, text="合约平仓价").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.derivative_limit_price, width=12).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="现货平仓价 / 合约平仓价只有你明确想卡价格时才填，日常使用通常留空就可以。",
            wraplength=860,
        )
        row += 1

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=row, column=1, sticky="w", pady=(10, 4))
        ttk.Button(btn_row, text="执行配对平仓", command=self._submit_pair_close).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动配对平仓", command=self._start_pair_close_auto).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="停止自动配对平仓", command=self._stop_pair_close_auto).pack(side="left")
        row += 1

        ttk.Label(frame, textvariable=self.pair_close_status_text, wraplength=860, justify="left").grid(
            row=row,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(12, 0),
        )

    def _build_roll_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        self._roll_tab = frame
        notebook.add(frame, text="交割移仓")
        frame.columnconfigure(1, weight=1)

        row = 0
        ttk.Label(frame, text="当前套利持仓").grid(row=row, column=0, sticky="w", pady=4)
        select_row = ttk.Frame(frame)
        select_row.grid(row=row, column=1, sticky="ew", pady=4)
        self.roll_entry_combo = ttk.Combobox(
            select_row,
            textvariable=self._roll_entry_label,
            state="readonly",
            width=56,
        )
        self.roll_entry_combo.pack(side="left", fill="x", expand=True)
        self.roll_entry_combo.bind("<<ComboboxSelected>>", self._on_roll_entry_selected)
        ttk.Button(select_row, text="刷新当前持仓", command=self._refresh_roll_positions).pack(side="left", padx=(8, 0))
        ttk.Button(select_row, text="带入账本选中", command=self._load_roll_from_ledger_selection).pack(side="left", padx=(8, 0))
        ttk.Button(select_row, text="刷新持仓", command=self._reload_ledger).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="移仓概览").grid(row=row, column=0, sticky="nw", pady=4)
        ttk.Label(frame, textvariable=self.roll_position_summary_text, wraplength=820, justify="left").grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        ttk.Label(frame, text="目标交割合约").grid(row=row, column=0, sticky="w", pady=4)
        target_row = ttk.Frame(frame)
        target_row.grid(row=row, column=1, sticky="w", pady=4)
        self.roll_target_derivative_combo = ttk.Combobox(
            target_row,
            textvariable=self.roll_target_derivative_inst_id,
            state="readonly",
            width=36,
        )
        self.roll_target_derivative_combo.pack(side="left")
        self.roll_target_derivative_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_roll_preview())
        ttk.Button(target_row, text="刷新目标", command=lambda: self._refresh_roll_target_options(fetch=True)).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="移仓数量").grid(row=row, column=0, sticky="w", pady=4)
        qty_row = ttk.Frame(frame)
        qty_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Entry(qty_row, textvariable=self.roll_contract_qty, width=16).pack(side="left")
        ttk.Label(qty_row, text="张").pack(side="left", padx=(6, 0))
        ttk.Button(qty_row, text="最大", command=self._fill_roll_qty_with_max).pack(side="left", padx=(10, 0))
        ttk.Button(qty_row, text="刷新预览", command=self._refresh_roll_preview).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="移仓预览").grid(row=row, column=0, sticky="nw", pady=(8, 4))
        ttk.Label(frame, textvariable=self.roll_preview_text, wraplength=820, justify="left").grid(
            row=row, column=1, sticky="w", pady=(8, 4)
        )
        row += 1

        self._roll_market_panel = self._build_dual_market_panel(
            frame,
            row=row,
            title="当前合约 / 目标合约盘口",
            wraplength=920,
        )
        row += 1

        ttk.Label(frame, text="执行方式").grid(row=row, column=0, sticky="w", pady=4)
        exec_row = ttk.Frame(frame)
        exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Checkbutton(exec_row, text="按限价挂单", variable=self.use_limit_orders).pack(side="left", padx=(0, 12))
        ttk.Label(exec_row, text="最大滑点(%)").pack(side="left")
        ttk.Entry(exec_row, textvariable=self.max_slippage_percent, width=8).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="分批执行").grid(row=row, column=0, sticky="w", pady=4)
        batch_row = ttk.Frame(frame)
        batch_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(batch_row, text="分批次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.roll_batch_count, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="每批张数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.roll_batch_qty, width=8).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="执行方式").pack(side="left")
        ttk.Combobox(
            batch_row,
            textvariable=self.roll_execution_mode_label,
            values=list(_ROLL_EXECUTION_MODE_OPTIONS.keys()),
            state="readonly",
            width=22,
        ).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="挂单等待(s)").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.roll_maker_wait_seconds, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="追单次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.roll_chase_limit, width=6).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="限价条件").grid(row=row, column=0, sticky="w", pady=4)
        limit_row = ttk.Frame(frame)
        limit_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(limit_row, text="旧合约买入 ≤").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.roll_current_limit_price, width=12).pack(side="left", padx=(4, 12))
        ttk.Label(limit_row, text="新合约卖出 ≥").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.roll_target_limit_price, width=12).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="移仓会保留现货腿不动，只做旧交割合约回补 + 新交割合约开出。分批、挂单等待、追单次数都按合约张数生效。",
            wraplength=820,
        )
        row += 1

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=row, column=1, sticky="w", pady=(10, 4))
        ttk.Button(btn_row, text="执行移仓", command=self._submit_roll).pack(side="left")
        row += 1

        ttk.Label(frame, textvariable=self.roll_status_text, wraplength=820, justify="left").grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(12, 0)
        )

    def _build_close_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        self._close_tab = frame
        notebook.add(frame, text="套利平仓")
        frame.columnconfigure(1, weight=1)

        row = 0
        ttk.Label(frame, text="套利持仓").grid(row=row, column=0, sticky="w", pady=4)
        select_row = ttk.Frame(frame)
        select_row.grid(row=row, column=1, sticky="ew", pady=4)
        self.close_entry_combo = ttk.Combobox(
            select_row,
            textvariable=self._close_entry_label,
            state="readonly",
            width=56,
        )
        self.close_entry_combo.pack(side="left", fill="x", expand=True)
        self.close_entry_combo.bind("<<ComboboxSelected>>", self._on_close_entry_selected)
        ttk.Button(select_row, text="带入账本选中", command=self._load_close_from_ledger_selection).pack(
            side="left",
            padx=(8, 0),
        )
        ttk.Button(select_row, text="刷新持仓", command=self._reload_ledger).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="持仓概览").grid(row=row, column=0, sticky="nw", pady=4)
        ttk.Label(frame, textvariable=self.close_position_summary_text, wraplength=760, justify="left").grid(
            row=row,
            column=1,
            sticky="w",
            pady=4,
        )
        row += 1

        ttk.Label(frame, text="平仓数量").grid(row=row, column=0, sticky="w", pady=4)
        qty_row = ttk.Frame(frame)
        qty_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Entry(qty_row, textvariable=self.close_contract_qty, width=16).pack(side="left")
        ttk.Label(qty_row, text="张").pack(side="left", padx=(6, 0))
        ttk.Button(qty_row, text="最大", command=self._fill_close_qty_with_max).pack(side="left", padx=(10, 0))
        ttk.Button(qty_row, text="刷新预览", command=self._refresh_close_preview).pack(side="left", padx=(8, 0))
        row += 1

        ttk.Label(frame, text="平仓预览").grid(row=row, column=0, sticky="nw", pady=(8, 4))
        ttk.Label(frame, textvariable=self.close_preview_text, wraplength=760, justify="left").grid(
            row=row,
            column=1,
            sticky="w",
            pady=(8, 4),
        )
        row += 1

        self._close_market_panel = self._build_dual_market_panel(
            frame,
            row=row,
            title="双腿盘口 / 价差",
            wraplength=920,
        )
        row += 1

        ttk.Label(frame, text="自动平仓").grid(row=row, column=0, sticky="w", pady=4)
        auto_row = ttk.Frame(frame)
        auto_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            auto_row,
            textvariable=self.close_trigger_mode_label,
            values=list(_CLOSE_TRIGGER_MODE_OPTIONS.keys()),
            state="readonly",
            width=10,
        ).pack(side="left")
        close_mode_combo = auto_row.winfo_children()[-1]
        close_mode_combo.bind("<<ComboboxSelected>>", self._on_close_trigger_mode_changed)
        ttk.Label(auto_row, textvariable=self._close_tab_threshold_label_text).pack(side="left", padx=(10, 0))
        self._close_tab_threshold_entry = ttk.Entry(auto_row, textvariable=self.close_spread_pct_min, width=8)
        self._close_tab_threshold_entry.pack(side="left", padx=(4, 0))
        ttk.Label(auto_row, textvariable=self._close_tab_threshold_unit_text).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="推荐先用“绝对价差”。这里填的是收敛后再平仓的条件，不是开仓时的入场条件。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="执行方式").grid(row=row, column=0, sticky="w", pady=4)
        exec_row = ttk.Frame(frame)
        exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Checkbutton(exec_row, text="按限价挂单", variable=self.use_limit_orders).pack(side="left", padx=(0, 12))
        ttk.Label(exec_row, text="最大滑点(%)").pack(side="left")
        ttk.Entry(exec_row, textvariable=self.max_slippage_percent, width=8).pack(side="left", padx=(4, 0))
        row += 1

        ttk.Label(frame, text="分批执行").grid(row=row, column=0, sticky="w", pady=4)
        batch_row = ttk.Frame(frame)
        batch_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(batch_row, text="分批次数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.close_batch_count, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(batch_row, text="每批张数").pack(side="left")
        ttk.Entry(batch_row, textvariable=self.close_batch_qty, width=8).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="平仓分批也按合约张数拆。你在上面填的是本次总平仓数量，这里再决定它是一次性平掉，还是拆成多批慢慢平。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="双腿执行").grid(row=row, column=0, sticky="w", pady=4)
        dual_exec_row = ttk.Frame(frame)
        dual_exec_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Combobox(
            dual_exec_row,
            textvariable=self.close_execution_mode_label,
            values=list(_ARBITRAGE_EXECUTION_MODE_OPTIONS.keys()),
            state="readonly",
            width=22,
        ).pack(side="left")
        ttk.Label(dual_exec_row, text="挂单等待(s)").pack(side="left", padx=(10, 0))
        ttk.Entry(dual_exec_row, textvariable=self.close_maker_wait_seconds, width=6).pack(side="left", padx=(4, 10))
        ttk.Label(dual_exec_row, text="追单次数").pack(side="left")
        ttk.Entry(dual_exec_row, textvariable=self.close_chase_limit, width=6).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="平仓也可以让一腿先挂、另一腿再市价跟。双腿吃单时仍按滑点控制；挂单等待和追单次数主要在挂单/吃单模式下生效。",
            wraplength=760,
        )
        row += 1

        ttk.Label(frame, text="限价条件").grid(row=row, column=0, sticky="w", pady=4)
        limit_row = ttk.Frame(frame)
        limit_row.grid(row=row, column=1, sticky="w", pady=4)
        ttk.Label(limit_row, text="合约买入 ≤").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.derivative_limit_price, width=12).pack(side="left", padx=(4, 12))
        ttk.Label(limit_row, text="现货卖出 ≥").pack(side="left")
        ttk.Entry(limit_row, textvariable=self.spot_limit_price, width=12).pack(side="left", padx=(4, 0))
        row += 1

        self._add_inline_hint(
            frame,
            row=row,
            text="合约买入 / 现货卖出限价属于可选保护条件，不强控价格时可以留空。",
            wraplength=760,
        )
        row += 1

        btn_row = ttk.Frame(frame)
        btn_row.grid(row=row, column=1, sticky="w", pady=(10, 4))
        ttk.Button(btn_row, text="立即平仓", command=self._close_selected_from_tab).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="启动自动平仓", command=self._start_auto_close_from_tab).pack(side="left", padx=(0, 8))
        ttk.Button(btn_row, text="全部平仓", command=self._close_all).pack(side="left")

    def _build_ledger_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="套利账本")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(frame)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        ttk.Button(toolbar, text="刷新账本", command=self._reload_ledger).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="平仓选中", command=self._close_selected).pack(side="left", padx=(0, 8))
        ttk.Button(toolbar, text="带入平仓", command=self._load_close_from_ledger_selection).pack(side="left")

        columns = ("base", "kind", "spot_qty", "swap_qty", "open_basis", "fee", "funding", "pnl", "status", "opened")
        tree_wrap = ttk.Frame(frame)
        tree_wrap.grid(row=1, column=0, sticky="nsew")
        tree_wrap.columnconfigure(0, weight=1)
        tree_wrap.rowconfigure(0, weight=1)
        self.ledger_tree = ttk.Treeview(tree_wrap, columns=columns, show="headings", height=16)
        for key, label in {
            "base": "币种",
            "kind": "类型",
            "spot_qty": "现货数量",
            "swap_qty": "合约数量",
            "open_basis": "开仓基差%",
            "fee": "手续费",
            "funding": "资金费",
            "pnl": "盈亏",
            "status": "状态",
            "opened": "开仓时间",
        }.items():
            self.ledger_tree.heading(key, text=label)
            anchor = "e" if key in {"spot_qty", "swap_qty", "open_basis", "fee", "funding", "pnl"} else "w"
            self.ledger_tree.column(key, width=100, anchor=anchor, stretch=key in {"opened"})
        scroll = ttk.Scrollbar(tree_wrap, orient="vertical", command=self.ledger_tree.yview)
        self.ledger_tree.configure(yscrollcommand=scroll.set)
        self.ledger_tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        self.ledger_tree.bind("<Double-1>", lambda _event: self._load_close_from_ledger_selection())


    def _build_log_tab(self, notebook: ttk.Notebook) -> None:
        frame = ttk.Frame(notebook, padding=12)
        notebook.add(frame, text="执行日志")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        self.log_text = Text(frame, wrap="word", height=24)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

    def _on_notebook_tab_changed(self, _event=None) -> None:
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _schedule_market_panel_refresh(self, *, initial_delay_ms: int | None = None) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        if self._market_panel_job is not None:
            try:
                self.window.after_cancel(self._market_panel_job)
            except Exception:
                pass
            self._market_panel_job = None
        delay = self._market_panel_refresh_interval_ms if initial_delay_ms is None else max(int(initial_delay_ms), 50)
        self._market_panel_job = self.window.after(delay, self._run_market_panel_refresh)

    def _run_market_panel_refresh(self) -> None:
        self._market_panel_job = None
        if self._destroying or not self.window.winfo_exists():
            return
        panel, context, empty_message = self._active_market_panel_context()
        if panel is None:
            self._schedule_market_panel_refresh()
            return
        if context is None:
            self._clear_market_panel(panel, message=empty_message)
            self._schedule_market_panel_refresh()
            return
        if self._market_panel_refresh_busy:
            self._schedule_market_panel_refresh()
            return
        self._market_panel_refresh_busy = True
        self._market_panel_refresh_token += 1
        refresh_token = self._market_panel_refresh_token
        threading.Thread(
            target=self._fetch_market_panel_snapshot_worker,
            args=(refresh_token, context),
            name=f"market-panel-{context['panel_key']}",
            daemon=True,
        ).start()

    def _active_market_panel_context(self):
        current_tab = self._notebook.select()
        environment = self._selected_api_environment()
        if self._trade_tab is not None and current_tab == str(self._trade_tab):
            panel = self._trade_market_panel
            base = self.base_ccy.get().strip().upper()
            derivative_inst_id = self._derivative_inst_id.get().strip().upper()
            if not base:
                return panel, None, "请先填写币种，再查看盘口。"
            if not derivative_inst_id:
                return panel, None, "请先选择或填写衍生品合约。"
            return panel, {
                "panel_key": "trade",
                "environment": environment,
                "spot_inst_id": f"{base}-USDT",
                "derivative_inst_id": derivative_inst_id,
                "spot_side": "buy",
                "derivative_side": "sell",
                "detail_label": "开仓可执行价差",
            }, ""
        if self._close_tab is not None and current_tab == str(self._close_tab):
            panel = self._close_market_panel
            entry = self._selected_close_entry()
            if entry is None:
                return panel, None, "请先选择一条未平仓套利持仓。"
            return panel, {
                "panel_key": "close",
                "environment": environment,
                "spot_inst_id": entry.spot_inst_id,
                "derivative_inst_id": entry.derivative_inst_id,
                "spot_side": "sell",
                "derivative_side": "buy",
                "detail_label": "平仓可执行价差",
            }, ""
        if self._pair_close_tab is not None and current_tab == str(self._pair_close_tab):
            panel = self._pair_close_market_panel
            selected = self._selected_pair_close_positions()
            if selected is None:
                return panel, None, "请先刷新并选择一组现货/合约持仓。"
            spot_position, derivative_position = selected
            return panel, {
                "panel_key": "pair_close",
                "environment": environment,
                "spot_inst_id": spot_position.inst_id,
                "derivative_inst_id": derivative_position.inst_id,
                "spot_side": _pair_position_close_side(spot_position),
                "derivative_side": _pair_position_close_side(derivative_position),
                "detail_label": "配对平仓可执行价差",
            }, ""
        if self._roll_tab is not None and current_tab == str(self._roll_tab):
            panel = self._roll_market_panel
            entry = self._selected_roll_entry()
            target_derivative_inst_id = self.roll_target_derivative_inst_id.get().strip().upper()
            if entry is None:
                return panel, None, "请先选择一条未平仓交割合约持仓。"
            if not target_derivative_inst_id:
                return panel, None, "请先填写更远交割合约。"
            return panel, {
                "panel_key": "roll",
                "environment": environment,
                "spot_inst_id": entry.derivative_inst_id,
                "derivative_inst_id": target_derivative_inst_id,
                "spot_side": "buy",
                "derivative_side": "sell",
                "detail_label": "移仓可执行价差",
            }, ""
        return None, None, ""

    def _get_cached_market_instrument(self, inst_id: str) -> Instrument:
        cached = self._market_instrument_cache.get(inst_id)
        if cached is not None:
            return cached
        instrument = self.client.get_instrument(inst_id)
        self._market_instrument_cache[inst_id] = instrument
        return instrument

    def _fetch_market_panel_snapshot_worker(self, refresh_token: int, context) -> None:
        panel_key = str(context["panel_key"])
        try:
            environment = str(context.get("environment") or "demo")
            spot_inst_id = str(context["spot_inst_id"])
            derivative_inst_id = str(context["derivative_inst_id"])
            spot_instrument = self._get_cached_market_instrument(spot_inst_id)
            derivative_instrument = self._get_cached_market_instrument(derivative_inst_id)
            self.client.ensure_public_ws_market_watch(spot_inst_id, environment=environment)
            self.client.ensure_public_ws_market_watch(derivative_inst_id, environment=environment)
            spot_ticker_payload = self.client.get_cached_public_ticker(spot_inst_id, environment=environment)
            derivative_ticker_payload = self.client.get_cached_public_ticker(derivative_inst_id, environment=environment)
            spot_order_book_payload = self.client.get_cached_public_order_book(spot_inst_id, environment=environment)
            derivative_order_book_payload = self.client.get_cached_public_order_book(derivative_inst_id, environment=environment)
            using_public_ws = all(
                payload is not None
                for payload in (
                    spot_ticker_payload,
                    derivative_ticker_payload,
                    spot_order_book_payload,
                    derivative_order_book_payload,
                )
            )
            if using_public_ws:
                assert spot_ticker_payload is not None
                assert derivative_ticker_payload is not None
                assert spot_order_book_payload is not None
                assert derivative_order_book_payload is not None
                _, spot_ticker = spot_ticker_payload
                _, derivative_ticker = derivative_ticker_payload
                _, spot_order_book = spot_order_book_payload
                _, derivative_order_book = derivative_order_book_payload
                source_mode = "WS"
                status_text = f"公共 WS 实时 | 最后更新：{time.strftime('%H:%M:%S')}"
                refresh_interval_ms = MARKET_PANEL_WS_REFRESH_MS
            else:
                spot_ticker = self.client.get_ticker(spot_inst_id)
                derivative_ticker = self.client.get_ticker(derivative_inst_id)
                spot_order_book = self.client.get_order_book(spot_inst_id, depth=MARKET_PANEL_DEPTH)
                derivative_order_book = self.client.get_order_book(derivative_inst_id, depth=MARKET_PANEL_DEPTH)
                ws_status = self.client.get_public_ws_debug_status(environment=environment)
                if ws_status.get("enabled") and ws_status.get("available"):
                    source_note = "公共 WS 预热中，暂用 REST"
                elif ws_status.get("enabled"):
                    source_note = "公共 WS 不可用，已回退 REST"
                else:
                    source_note = "公共 WS 已关闭，使用 REST"
                source_mode = "REST"
                status_text = f"{source_note} | 最后更新：{time.strftime('%H:%M:%S')}"
                refresh_interval_ms = MARKET_PANEL_REST_REFRESH_MS

            spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask) or spot_ticker.last
            derivative_mid = mid_price(derivative_ticker.bid, derivative_ticker.ask) or derivative_ticker.last
            spread_abs = None
            spread_pct = None
            if spot_mid is not None and spot_mid > 0 and derivative_mid is not None:
                spread_abs = derivative_mid - spot_mid
                spread_pct = (spread_abs / spot_mid) * Decimal("100")
            actionable_abs = _actionable_spread_abs(
                spot_ticker=spot_ticker,
                spot_order_book=spot_order_book,
                derivative_ticker=derivative_ticker,
                derivative_order_book=derivative_order_book,
                spot_side=context["spot_side"],
                derivative_side=context["derivative_side"],
            )
            spot_quote_ccy = _instrument_quote_ccy(spot_inst_id)
            derivative_quote_ccy = _instrument_quote_ccy(derivative_inst_id)
            spot_base_ccy = spot_inst_id.split("-")[0]
            derivative_base_ccy = derivative_inst_id.split("-")[0]
            spread_text = f"绝对价差：{format_decimal(spread_abs) if spread_abs is not None else '-'}"
            if spread_pct is not None:
                spread_text += f" | 价差率：{format_decimal_fixed(spread_pct, 4)}%"
            else:
                spread_text += " | 价差率：-"
            snapshot = {
                "panel_key": panel_key,
                "spot_title": f"{spot_inst_id} 盘口 [{source_mode}]",
                "spot_quote": (
                    f"价格({spot_quote_ccy}) / 数量({spot_base_ccy}) | "
                    f"最新 {format_decimal(spot_ticker.last) if spot_ticker.last is not None else '-'} | "
                    f"买一 {format_decimal(spot_ticker.bid) if spot_ticker.bid is not None else '-'} | "
                    f"卖一 {format_decimal(spot_ticker.ask) if spot_ticker.ask is not None else '-'}"
                ),
                "derivative_title": f"{derivative_inst_id} 盘口 [{source_mode}]",
                "derivative_quote": (
                    f"价格({derivative_quote_ccy}) / 数量({derivative_base_ccy}) | "
                    f"最新 {format_decimal(derivative_ticker.last) if derivative_ticker.last is not None else '-'} | "
                    f"买一 {format_decimal(derivative_ticker.bid) if derivative_ticker.bid is not None else '-'} | "
                    f"卖一 {format_decimal(derivative_ticker.ask) if derivative_ticker.ask is not None else '-'}"
                ),
                "spread_text": f"[{source_mode}] {spread_text}",
                "detail_text": (
                    f"{context['detail_label']}：{format_decimal(actionable_abs) if actionable_abs is not None else '-'}"
                    f" | 现货 {context['spot_side']} / 合约 {context['derivative_side']}"
                ),
                "status_text": status_text,
                "spot_rows": _market_depth_rows(spot_order_book, instrument=spot_instrument),
                "derivative_rows": _market_depth_rows(derivative_order_book, instrument=derivative_instrument),
                "spot_headers": (f"价格({spot_quote_ccy})", f"数量({spot_base_ccy})"),
                "derivative_headers": (f"价格({derivative_quote_ccy})", f"数量({derivative_base_ccy})"),
                "refresh_interval_ms": refresh_interval_ms,
            }
        except Exception as exc:
            snapshot = {
                "panel_key": panel_key,
                "error": str(exc),
                "refresh_interval_ms": MARKET_PANEL_REST_REFRESH_MS,
            }

        def _apply() -> None:
            if self._destroying or not self.window.winfo_exists():
                return
            if refresh_token != self._market_panel_refresh_token:
                self._market_panel_refresh_busy = False
                self._schedule_market_panel_refresh()
                return
            self._market_panel_refresh_busy = False
            self._market_panel_refresh_interval_ms = int(snapshot.get("refresh_interval_ms", MARKET_PANEL_REST_REFRESH_MS))
            panel = self._panel_for_key(panel_key)
            if panel is None:
                self._schedule_market_panel_refresh()
                return
            if "error" in snapshot:
                self._clear_market_panel(panel, message=f"盘口刷新失败：{snapshot['error']}")
                self._schedule_market_panel_refresh()
                return
            self._apply_market_panel_snapshot(panel, snapshot)
            self._schedule_market_panel_refresh()

        try:
            self.window.after(0, _apply)
        except Exception:
            self._market_panel_refresh_busy = False

    def _panel_for_key(self, panel_key: str) -> ArbitrageMarketPanel | None:
        if panel_key == "trade":
            return self._trade_market_panel
        if panel_key == "close":
            return self._close_market_panel
        if panel_key == "pair_close":
            return self._pair_close_market_panel
        return None

    def _clear_market_panel(self, panel: ArbitrageMarketPanel, *, message: str) -> None:
        panel.spot_title_text.set("左腿盘口")
        panel.spot_quote_text.set("等待产品信息…")
        panel.derivative_title_text.set("右腿盘口")
        panel.derivative_quote_text.set("等待产品信息…")
        panel.spread_text.set("绝对价差：- | 价差率：-")
        panel.detail_text.set("")
        panel.status_text.set(message)
        for tree in (panel.spot_tree, panel.derivative_tree):
            tree.heading("price", text="价格")
            tree.heading("qty", text="数量")
            for item_id in tree.get_children():
                tree.delete(item_id)

    def _apply_market_panel_snapshot(self, panel: ArbitrageMarketPanel, snapshot) -> None:
        panel.spot_title_text.set(str(snapshot["spot_title"]))
        panel.spot_quote_text.set(str(snapshot["spot_quote"]))
        panel.derivative_title_text.set(str(snapshot["derivative_title"]))
        panel.derivative_quote_text.set(str(snapshot["derivative_quote"]))
        panel.spread_text.set(str(snapshot["spread_text"]))
        panel.detail_text.set(str(snapshot["detail_text"]))
        panel.status_text.set(str(snapshot["status_text"]))
        spot_price_heading, spot_qty_heading = snapshot["spot_headers"]
        derivative_price_heading, derivative_qty_heading = snapshot["derivative_headers"]
        panel.spot_tree.heading("price", text=spot_price_heading)
        panel.spot_tree.heading("qty", text=spot_qty_heading)
        panel.derivative_tree.heading("price", text=derivative_price_heading)
        panel.derivative_tree.heading("qty", text=derivative_qty_heading)
        self._populate_market_book_tree(panel.spot_tree, snapshot["spot_rows"])
        self._populate_market_book_tree(panel.derivative_tree, snapshot["derivative_rows"])

    def _populate_market_book_tree(self, tree: ttk.Treeview, rows: list[tuple[str, str, str]]) -> None:
        for item_id in tree.get_children():
            tree.delete(item_id)
        for tag, price_text, qty_text in rows:
            tree.insert("", END, values=(price_text, qty_text), tags=(tag,))

    def _append_log(self, message: str) -> None:
        if self._destroying:
            return

        def _write() -> None:
            if self._destroying or not self.window.winfo_exists():
                return
            self.log_text.insert(END, message + "\n")
            self.log_text.see(END)
            self.status_text.set(message)

        try:
            self.window.after(0, _write)
        except Exception:
            pass
        self._logger(message)

    def _set_pair_close_status_async(self, message: str) -> None:
        def _apply() -> None:
            if self._destroying or not self.window.winfo_exists():
                return
            self.pair_close_status_text.set(message)

        try:
            self.window.after(0, _apply)
        except Exception:
            pass

    def _selected_api_profile_name(self) -> str:
        profile_name = self.api_profile_name.get().strip()
        if profile_name in self._api_profile_names:
            return profile_name
        if self._api_profile_names:
            return self._api_profile_names[0]
        return DEFAULT_CREDENTIAL_PROFILE_NAME

    def _selected_api_environment(self) -> str:
        profile_snapshot = load_credentials_snapshot(profile_name=self._selected_api_profile_name())
        return _credential_profile_environment(profile_snapshot, fallback="demo")

    def _sync_api_profile_controls(self) -> None:
        snapshot = load_credentials_profiles_snapshot()
        self._api_profile_names = _credential_profile_names_from_snapshot(snapshot)
        current = self._selected_api_profile_name()
        if current not in self._api_profile_names:
            current = self._api_profile_names[0]
        if self._api_profile_combo is not None:
            combo_width = max(10, min(16, max((len(item) for item in self._api_profile_names), default=10) + 1))
            self._api_profile_combo.configure(values=self._api_profile_names, width=combo_width)
        if self.api_profile_name.get().strip() != current:
            self.api_profile_name.set(current)
        profile_snapshot = load_credentials_snapshot(profile_name=current)
        environment = _credential_profile_environment(profile_snapshot, fallback="demo")
        self.api_environment_text.set(_environment_label(environment))
        self._last_api_profile_name = current

    def _clear_pair_close_selection(self) -> None:
        self._pair_close_positions = []
        self._pair_close_position_by_key = {}
        self._pair_close_instruments = {}
        self._pair_close_reference_prices = {}
        self._pair_close_spot_key.set("")
        self._pair_close_derivative_key.set("")
        self.pair_close_derivative_qty.set("")
        if hasattr(self, "pair_close_spot_combo"):
            self.pair_close_spot_combo.configure(values=())
        if hasattr(self, "pair_close_derivative_combo"):
            self.pair_close_derivative_combo.configure(values=())
        self.pair_close_preview_text.set("请先刷新并选择一组当前持仓。")
        self.pair_close_status_text.set("已切换 API，请重新刷新当前持仓。")
        if self._pair_close_market_panel is not None:
            self._clear_market_panel(self._pair_close_market_panel, message="请先刷新并选择一组当前持仓。")

    def _clear_roll_selection(self) -> None:
        self._roll_positions = []
        self._roll_position_by_key = {}
        self._roll_instruments = {}
        self._roll_reference_prices = {}
        self._roll_spot_by_base = {}
        self._roll_source_entry_id = None
        self._roll_future_instruments = []
        self._roll_entry_label.set("")
        self.roll_target_derivative_inst_id.set("")
        self.roll_contract_qty.set("")
        if hasattr(self, "roll_entry_combo"):
            self.roll_entry_combo.configure(values=())
        if hasattr(self, "roll_target_derivative_combo"):
            self.roll_target_derivative_combo.configure(values=())
        self.roll_position_summary_text.set("请先刷新并选择一条当前交割合约持仓。")
        self.roll_preview_text.set("请选择当前交割合约持仓，并填写更远交割合约。")
        self.roll_status_text.set("已切换 API，请重新刷新当前持仓。")
        if self._roll_market_panel is not None:
            self._clear_market_panel(self._roll_market_panel, message="请先刷新并选择一条当前交割合约持仓。")

    def _on_api_profile_selected(self, _event=None) -> None:
        selected = self.api_profile_name.get().strip()
        if not selected:
            self.api_profile_name.set(self._last_api_profile_name)
            return
        if selected == self._last_api_profile_name:
            self._sync_api_profile_controls()
            return
        if self.manager.auto_open.is_running or self.manager.auto_close.is_running or self._is_pair_close_auto_running():
            messagebox.showwarning("提示", "请先停止自动开仓/自动平仓/自动配对平仓监控，再切换 API。", parent=self.window)
            self.api_profile_name.set(self._last_api_profile_name)
            return
        self._sync_api_profile_controls()
        current = self._selected_api_profile_name()
        self.status_text.set(f"API 已切换：{current} | {self.api_environment_text.get()}")
        self._clear_pair_close_selection()
        self._clear_roll_selection()
        for panel in (self._trade_market_panel, self._close_market_panel, self._pair_close_market_panel, self._roll_market_panel):
            if panel is not None:
                self._clear_market_panel(panel, message="API 已切换，正在等待新的产品选择。")
        self._schedule_market_panel_refresh(initial_delay_ms=50)
        self._append_log(f"套利窗口已切换 API：{current} | {self.api_environment_text.get()}")

    def _schedule_auto_refresh(self) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        if self.auto_refresh_enabled.get():
            self._schedule_scan(refresh_only=True)
        self._refresh_job = self.window.after(REFRESH_INTERVAL_MS, self._schedule_auto_refresh)

    def _scan_sort_value(self, item: ArbitrageOpportunity, column: str) -> object:
        if column == "base":
            return item.base_ccy
        if column == "kind":
            return item.pair_kind_label
        if column == "spot":
            return item.spot_inst_id
        if column == "derivative":
            return item.derivative_inst_id
        if column == "basis_abs":
            return item.basis_abs
        if column == "basis":
            return item.basis_pct
        if column == "funding":
            return item.funding_annual_pct if item.funding_annual_pct is not None else Decimal("-999999")
        if column == "fee":
            return item.fee_round_trip_pct
        if column == "slippage":
            return item.slippage_est_pct
        if column == "expiry":
            return item.days_to_expiry if item.days_to_expiry is not None else 999999
        return item.net_annual_pct

    def _scan_filtered_sorted_rows(self) -> list[ArbitrageOpportunity]:
        base_filter = self.scan_base_filter.get().strip().upper()
        rows = self._opportunities
        if base_filter and base_filter not in {"全部", "ALL"}:
            rows = [item for item in rows if item.base_ccy.upper() == base_filter]
        return sorted(
            rows,
            key=lambda item: self._scan_sort_value(item, self._scan_sort_column),
            reverse=self._scan_sort_desc,
        )

    def _refresh_scan_filter_options(self) -> None:
        values = ["全部", *sorted({item.base_ccy for item in self._opportunities})]
        current = self.scan_base_filter.get().strip() or "全部"
        if current not in values:
            current = "全部"
            self.scan_base_filter.set(current)
        if self._scan_base_filter_combo is not None:
            self._scan_base_filter_combo.configure(values=values)

    def _update_scan_status(self, visible_count: int) -> None:
        total_count = len(self._opportunities)
        sort_label_map = {
            "base": "币种",
            "kind": "类型",
            "spot": "现货",
            "derivative": "衍生品",
            "basis_abs": "绝对价差",
            "basis": "基差",
            "funding": "资金费年化",
            "fee": "手续费",
            "slippage": "滑点",
            "net": "净年化",
            "expiry": "到期天数",
        }
        direction = "降序" if self._scan_sort_desc else "升序"
        if visible_count == total_count:
            self.scan_status_text.set(f"共 {total_count} 条机会，按{sort_label_map.get(self._scan_sort_column, '净年化')}{direction}。")
        else:
            self.scan_status_text.set(
                f"共 {total_count} 条机会，当前显示 {visible_count} 条，按{sort_label_map.get(self._scan_sort_column, '净年化')}{direction}。"
            )

    def _apply_scan_view(self) -> None:
        rows = self._scan_filtered_sorted_rows()
        self._scan_display_rows = rows
        if self._selected_opportunity not in rows:
            self._selected_opportunity = None
        self._render_scan_rows(rows)
        self._update_scan_status(len(rows))

    def _sort_scan_by(self, column: str) -> None:
        if self._scan_sort_column == column:
            self._scan_sort_desc = not self._scan_sort_desc
        else:
            self._scan_sort_column = column
            self._scan_sort_desc = column not in {"base", "kind", "spot", "derivative"}
        self._apply_scan_view()

    def _on_scan_filter_changed(self, _event=None) -> None:
        self._apply_scan_view()

    def _current_open_trigger_mode(self) -> str:
        return _TRIGGER_MODE_OPTIONS.get(self.trigger_mode_label.get().strip(), "spread")

    def _current_close_trigger_mode(self) -> str:
        return _CLOSE_TRIGGER_MODE_OPTIONS.get(self.close_trigger_mode_label.get().strip(), "spread")

    def _current_pair_close_trigger_mode(self) -> str:
        return _CLOSE_TRIGGER_MODE_OPTIONS.get(self.pair_close_trigger_mode_label.get().strip(), "spread")

    def _current_pair_close_execution_mode(self) -> str:
        return _PAIR_CLOSE_EXECUTION_MODE_OPTIONS.get(self.pair_close_execution_mode_label.get().strip(), "dual_taker")

    def _current_open_execution_mode(self) -> str:
        return _ARBITRAGE_EXECUTION_MODE_OPTIONS.get(self.open_execution_mode_label.get().strip(), "dual_taker")

    def _current_close_execution_mode(self) -> str:
        return _ARBITRAGE_EXECUTION_MODE_OPTIONS.get(self.close_execution_mode_label.get().strip(), "dual_taker")

    def _current_roll_execution_mode(self) -> str:
        return _ROLL_EXECUTION_MODE_OPTIONS.get(self.roll_execution_mode_label.get().strip(), "dual_taker")

    def _sync_spread_trigger_controls(self) -> None:
        open_mode = self._current_open_trigger_mode()
        close_mode = self._current_close_trigger_mode()

        if open_mode == "spread_abs":
            self._open_trigger_threshold_label_text.set("开仓绝对价差 >")
            self._open_trigger_threshold_unit_text.set("")
            if hasattr(self, "_open_trigger_threshold_entry"):
                self._open_trigger_threshold_entry.configure(textvariable=self.open_spread_abs_max)
        else:
            self._open_trigger_threshold_label_text.set("开仓价差率 <")
            self._open_trigger_threshold_unit_text.set("%")
            if hasattr(self, "_open_trigger_threshold_entry"):
                self._open_trigger_threshold_entry.configure(textvariable=self.open_spread_pct_max)

        if close_mode == "spread_abs":
            self._open_close_threshold_label_text.set("平仓绝对价差 <")
            self._open_close_threshold_unit_text.set("")
            self._close_tab_threshold_label_text.set("绝对价差 <")
            self._close_tab_threshold_unit_text.set("时触发")
            if hasattr(self, "_open_close_threshold_entry"):
                self._open_close_threshold_entry.configure(textvariable=self.close_spread_abs_min)
            if hasattr(self, "_close_tab_threshold_entry"):
                self._close_tab_threshold_entry.configure(textvariable=self.close_spread_abs_min)
        else:
            self._open_close_threshold_label_text.set("平仓价差率 <")
            self._open_close_threshold_unit_text.set("%")
            self._close_tab_threshold_label_text.set("价差率 <")
            self._close_tab_threshold_unit_text.set("% 时触发")
            if hasattr(self, "_open_close_threshold_entry"):
                self._open_close_threshold_entry.configure(textvariable=self.close_spread_pct_min)
            if hasattr(self, "_close_tab_threshold_entry"):
                self._close_tab_threshold_entry.configure(textvariable=self.close_spread_pct_min)

    def _sync_pair_close_trigger_controls(self) -> None:
        close_mode = self._current_pair_close_trigger_mode()
        if close_mode == "spread_abs":
            self._pair_close_threshold_label_text.set("绝对价差 <")
            self._pair_close_threshold_unit_text.set("时执行")
            if hasattr(self, "_pair_close_threshold_entry"):
                self._pair_close_threshold_entry.configure(textvariable=self.pair_close_spread_abs_min)
        else:
            self._pair_close_threshold_label_text.set("价差率 <")
            self._pair_close_threshold_unit_text.set("% 时执行")
            if hasattr(self, "_pair_close_threshold_entry"):
                self._pair_close_threshold_entry.configure(textvariable=self.pair_close_spread_pct_min)

    def _on_trigger_mode_changed(self, _event=None) -> None:
        if self._current_open_trigger_mode() == "spread_abs":
            self.close_trigger_mode_label.set("绝对价差")
        self._sync_spread_trigger_controls()

    def _on_close_trigger_mode_changed(self, _event=None) -> None:
        self._sync_spread_trigger_controls()

    def _on_pair_close_trigger_mode_changed(self, _event=None) -> None:
        self._sync_pair_close_trigger_controls()

    def _schedule_scan(self, *, refresh_only: bool) -> None:
        if self._scan_busy:
            return
        if not self.scan_swap_enabled.get() and not self.scan_futures_enabled.get():
            self.scan_status_text.set("请至少勾选一种扫描类型。")
            if not refresh_only:
                messagebox.showwarning("提示", "请至少勾选一种扫描类型：永续 或 交割。", parent=self.window)
            return
        self._scan_busy = True
        if not refresh_only:
            self.scan_status_text.set("扫描中…")

        def _worker() -> None:
            error: str | None = None
            rows: list[ArbitrageOpportunity] = []
            try:
                rows = self.manager.scan_opportunities(
                    include_swap=self.scan_swap_enabled.get(),
                    include_futures=self.scan_futures_enabled.get(),
                )
            except Exception as exc:
                error = str(exc)
            if self._destroying:
                return

            def _apply() -> None:
                self._scan_busy = False
                if error is not None:
                    self.scan_status_text.set(f"扫描失败：{error}")
                    self._append_log(f"套利扫描失败：{error}")
                    return
                self._opportunities = rows
                self._refresh_scan_filter_options()
                self._apply_scan_view()
                self._maybe_alert(self._scan_display_rows)

            try:
                self.window.after(0, _apply)
            except Exception:
                self._scan_busy = False

        self._scan_thread = threading.Thread(target=_worker, name="arbitrage-scan", daemon=True)
        self._scan_thread.start()

    def _render_scan_rows(self, rows: list[ArbitrageOpportunity]) -> None:
        self.scan_tree.delete(*self.scan_tree.get_children())
        for index, item in enumerate(rows):
            self.scan_tree.insert(
                "",
                END,
                iid=str(index),
                values=(
                    item.base_ccy,
                    item.pair_kind_label,
                    item.spot_inst_id,
                    item.derivative_inst_id,
                    format_decimal(item.basis_abs),
                    format_decimal_fixed(item.basis_pct, 4),
                    "-" if item.funding_annual_pct is None else format_decimal_fixed(item.funding_annual_pct, 4),
                    format_decimal_fixed(item.fee_round_trip_pct, 4),
                    format_decimal_fixed(item.slippage_est_pct, 4),
                    format_decimal_fixed(item.net_annual_pct, 4),
                    "-" if item.days_to_expiry is None else str(item.days_to_expiry),
                ),
            )

    def _maybe_alert(self, rows: list[ArbitrageOpportunity]) -> None:
        if not self.alert_enabled.get() or not rows:
            return
        try:
            threshold = Decimal(self.min_annual_threshold.get().strip() or "0")
        except InvalidOperation:
            return
        best = rows[0]
        if best.net_annual_pct >= threshold:
            messagebox.showinfo(
                "套利机会提醒",
                (
                    f"{best.base_ccy} {best.pair_kind_label}\n"
                    f"净年化：{format_decimal_fixed(best.net_annual_pct, 2)}%\n"
                    f"基差：{format_decimal_fixed(best.basis_pct, 4)}%\n"
                    f"可在「建仓/平仓」页一键带入参数。"
                ),
                parent=self.window,
            )

    def _on_scan_select(self, _event=None) -> None:
        selected = self.scan_tree.selection()
        if not selected:
            self._selected_opportunity = None
            return
        try:
            index = int(selected[0])
        except ValueError:
            return
        if 0 <= index < len(self._scan_display_rows):
            self._selected_opportunity = self._scan_display_rows[index]

    def _open_trade_from_selection(self) -> None:
        if self._selected_opportunity is None:
            selected = self.scan_tree.selection()
            if not selected:
                messagebox.showwarning("提示", "请先在扫描列表中选择一条机会。", parent=self.window)
                return
            try:
                index = int(selected[0])
                self._selected_opportunity = self._scan_display_rows[index]
            except (ValueError, IndexError):
                messagebox.showwarning("提示", "当前选择无效，请重新扫描。", parent=self.window)
                return
        opp = self._selected_opportunity
        self.base_ccy.set(opp.base_ccy)
        self._derivative_inst_id.set(opp.derivative_inst_id)
        self.chart_spot_inst_id.set(opp.spot_inst_id)
        self.chart_derivative_inst_id.set(opp.derivative_inst_id)
        if self._trade_tab is not None:
            self._notebook.select(self._trade_tab)
        self._refresh_preview(derivative_inst_id=opp.derivative_inst_id)
        self._schedule_market_panel_refresh(initial_delay_ms=50)
        self._append_log(f"已带入 {opp.base_ccy} {opp.pair_kind_label} 建仓参数。")

    def _open_chart_from_selection(self) -> None:
        if self._selected_opportunity is None:
            selected = self.scan_tree.selection()
            if not selected:
                messagebox.showwarning("提示", "请先在扫描列表中选择一条机会。", parent=self.window)
                return
            try:
                index = int(selected[0])
                self._selected_opportunity = self._scan_display_rows[index]
            except (ValueError, IndexError):
                messagebox.showwarning("提示", "当前选择无效，请重新扫描。", parent=self.window)
                return
        opp = self._selected_opportunity
        self.chart_spot_inst_id.set(opp.spot_inst_id)
        self.chart_derivative_inst_id.set(opp.derivative_inst_id)
        if self._chart_tab is not None:
            self._notebook.select(self._chart_tab)
        self._load_arbitrage_charts()

    def _close_entry_option_label(self, entry) -> str:
        return (
            f"{entry.base_ccy} | {entry.derivative_inst_id} | "
            f"可平 {format_decimal(entry.derivative_qty)} 张 | {entry.opened_at} | {entry.entry_id[:6]}"
        )

    def _selected_close_entry_id(self) -> str | None:
        return self._close_entry_display_to_id.get(self._close_entry_label.get().strip())

    def _selected_close_entry(self):
        entry_id = self._selected_close_entry_id()
        if not entry_id:
            return None
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return None
        return entry

    def _selected_ledger_entry(self):
        selected = self.ledger_tree.selection()
        if not selected:
            return None
        entry_id = selected[0]
        if entry_id.startswith("placeholder"):
            return None
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return None
        return entry

    def _refresh_close_entry_options(self) -> None:
        current_entry_id = self._selected_close_entry_id()
        self._open_ledger_entries = [item for item in self._ledger_entries if item.close_mode == "open"]
        labels = [self._close_entry_option_label(item) for item in self._open_ledger_entries]
        self._close_entry_display_to_id = {
            label: entry.entry_id for label, entry in zip(labels, self._open_ledger_entries, strict=False)
        }
        self.close_entry_combo.configure(values=labels)
        if not labels:
            self._close_entry_label.set("")
            self.close_contract_qty.set("")
            self.close_position_summary_text.set("暂无未平仓套利持仓。")
            self.close_preview_text.set("请先选择一条未平仓套利持仓。")
            if self._close_market_panel is not None:
                self._clear_market_panel(self._close_market_panel, message="暂无未平仓套利持仓。")
            return

        open_entry_ids = {item.entry_id for item in self._open_ledger_entries}
        target_entry_id = current_entry_id if current_entry_id in open_entry_ids else self._open_ledger_entries[0].entry_id
        self._set_close_entry(target_entry_id, fill_max=False, focus_tab=False)

    def _set_close_entry(self, entry_id: str, *, fill_max: bool, focus_tab: bool) -> None:
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return
        for label, mapped_entry_id in self._close_entry_display_to_id.items():
            if mapped_entry_id == entry_id:
                self._close_entry_label.set(label)
                break
        if fill_max or not self.close_contract_qty.get().strip():
            self.close_contract_qty.set(format_decimal(entry.derivative_qty))
        if focus_tab and self._close_tab is not None:
            self._notebook.select(self._close_tab)
        self._refresh_close_preview()

    def _on_close_entry_selected(self, _event=None) -> None:
        entry = self._selected_close_entry()
        if entry is None:
            self.close_position_summary_text.set("暂无未平仓套利持仓。")
            self.close_preview_text.set("请先选择一条未平仓套利持仓。")
            if self._close_market_panel is not None:
                self._clear_market_panel(self._close_market_panel, message="请先选择一条未平仓套利持仓。")
            return
        if not self.close_contract_qty.get().strip():
            self.close_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_close_preview()
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _fill_close_qty_with_max(self) -> None:
        entry = self._selected_close_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先选择一条未平仓套利持仓。", parent=self.window)
            return
        self.close_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_close_preview()

    def _load_close_from_ledger_selection(self) -> None:
        entry = self._selected_ledger_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先在账本中选择一条未平仓记录。", parent=self.window)
            return
        self._set_close_entry(entry.entry_id, fill_max=True, focus_tab=True)

    def _selected_roll_entry_id(self) -> str | None:
        return self._roll_entry_display_to_id.get(self._roll_entry_label.get().strip())

    def _selected_roll_entry(self):
        entry_id = self._selected_roll_entry_id()
        if not entry_id:
            return None
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return None
        return entry

    def _refresh_roll_entry_options(self) -> None:
        current_entry_id = self._selected_roll_entry_id()
        candidates = [
            item
            for item in self._ledger_entries
            if item.close_mode == "open" and infer_inst_type(item.derivative_inst_id) == "FUTURES"
        ]
        labels = [self._close_entry_option_label(item) for item in candidates]
        self._roll_entry_display_to_id = {
            label: entry.entry_id for label, entry in zip(labels, candidates, strict=False)
        }
        self.roll_entry_combo.configure(values=labels)
        if not labels:
            self._roll_entry_label.set("")
            self.roll_contract_qty.set("")
            self.roll_position_summary_text.set("暂无可移仓的交割合约套利持仓。")
            self.roll_preview_text.set("请先开出或导入一条交割合约套利持仓。")
            if self._roll_market_panel is not None:
                self._clear_market_panel(self._roll_market_panel, message="暂无可移仓的交割合约套利持仓。")
            return
        target_entry_id = current_entry_id if current_entry_id in {item.entry_id for item in candidates} else candidates[0].entry_id
        self._set_roll_entry(target_entry_id, fill_max=False, focus_tab=False)

    def _set_roll_entry(self, entry_id: str, *, fill_max: bool, focus_tab: bool) -> None:
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return
        for label, mapped_entry_id in self._roll_entry_display_to_id.items():
            if mapped_entry_id == entry_id:
                self._roll_entry_label.set(label)
                break
        if fill_max or not self.roll_contract_qty.get().strip():
            self.roll_contract_qty.set(format_decimal(entry.derivative_qty))
        if focus_tab and self._roll_tab is not None:
            self._notebook.select(self._roll_tab)
        self._refresh_roll_preview()

    def _on_roll_entry_selected(self, _event=None) -> None:
        entry = self._selected_roll_entry()
        if entry is None:
            self.roll_position_summary_text.set("暂无可移仓的交割合约套利持仓。")
            self.roll_preview_text.set("请先选择一条未平仓交割合约持仓。")
            if self._roll_market_panel is not None:
                self._clear_market_panel(self._roll_market_panel, message="请先选择一条未平仓交割合约持仓。")
            return
        if not self.roll_contract_qty.get().strip():
            self.roll_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_roll_preview()
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _fill_roll_qty_with_max(self) -> None:
        entry = self._selected_roll_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先选择一条未平仓交割合约持仓。", parent=self.window)
            return
        self.roll_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_roll_preview()

    def _load_roll_from_ledger_selection(self) -> None:
        entry = self._selected_ledger_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先在账本中选择一条未平仓记录。", parent=self.window)
            return
        if infer_inst_type(entry.derivative_inst_id) != "FUTURES":
            messagebox.showwarning("提示", "当前只支持交割合约移仓。", parent=self.window)
            return
        self._set_roll_entry(entry.entry_id, fill_max=True, focus_tab=True)

    def _selected_roll_position(self) -> OkxPosition | None:
        position = self._roll_position_by_key.get(self._roll_entry_label.get().strip())
        return position if isinstance(position, OkxPosition) else None

    def _selected_roll_positions(self) -> tuple[OkxPosition, OkxPosition] | None:
        derivative_position = self._selected_roll_position()
        if derivative_position is None:
            return None
        spot_position = self._roll_spot_by_base.get(_pair_position_base_ccy(derivative_position))
        if not isinstance(spot_position, OkxPosition):
            return None
        return spot_position, derivative_position

    def _roll_reference_price(self, position: OkxPosition, instrument: Instrument) -> Decimal | None:
        reference_price = position.mark_price or position.last_price
        if reference_price is not None and reference_price > 0:
            return reference_price
        cached = self._roll_reference_prices.get(instrument.inst_id)
        if cached is not None and cached > 0:
            return cached
        return None

    def _current_roll_source_entry(self, spot_position: OkxPosition, derivative_position: OkxPosition):
        entry_id = self._roll_source_entry_id
        if not entry_id:
            return None
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return None
        if entry.spot_inst_id != spot_position.inst_id or entry.derivative_inst_id != derivative_position.inst_id:
            return None
        return entry

    def _estimate_roll_spot_qty(
        self,
        *,
        spot_position: OkxPosition,
        derivative_position: OkxPosition,
        spot_instrument: Instrument,
        derivative_instrument: Instrument,
        derivative_qty: Decimal,
        source_entry=None,
    ) -> Decimal:
        spot_available = snap_to_increment(_pair_position_closeable_size(spot_position), spot_instrument.lot_size, "down")
        if spot_available <= 0:
            return Decimal("0")
        if source_entry is not None and getattr(source_entry, "derivative_qty", Decimal("0")) > 0:
            if derivative_qty >= source_entry.derivative_qty:
                planned = source_entry.spot_qty
            else:
                planned = source_entry.spot_qty * derivative_qty / max(source_entry.derivative_qty, Decimal("1e-18"))
            return snap_to_increment(min(spot_available, planned), spot_instrument.lot_size, "down")
        reference_price = self._roll_reference_price(derivative_position, derivative_instrument)
        total_exposure = _pair_position_base_exposure(
            derivative_position,
            derivative_instrument,
            reference_price=reference_price,
        )
        if total_exposure is None or total_exposure <= 0:
            return spot_available
        total_derivative_qty = max(_pair_position_closeable_size(derivative_position), Decimal("1e-18"))
        planned = total_exposure if derivative_qty >= total_derivative_qty else total_exposure * derivative_qty / total_derivative_qty
        return snap_to_increment(min(spot_available, planned), spot_instrument.lot_size, "down")

    def _selected_roll_entry(self):
        selected = self._selected_roll_positions()
        if selected is None:
            return None
        spot_position, derivative_position = selected
        spot_instrument = self._roll_instruments.get(spot_position.inst_id)
        derivative_instrument = self._roll_instruments.get(derivative_position.inst_id)
        if spot_instrument is None or derivative_instrument is None:
            return None
        source_entry = self._current_roll_source_entry(spot_position, derivative_position)
        derivative_qty = snap_to_increment(
            _pair_position_closeable_size(derivative_position),
            derivative_instrument.lot_size,
            "down",
        )
        if source_entry is not None:
            derivative_qty = min(derivative_qty, source_entry.derivative_qty)
        if derivative_qty <= 0:
            return None
        spot_qty = self._estimate_roll_spot_qty(
            spot_position=spot_position,
            derivative_position=derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            derivative_qty=derivative_qty,
            source_entry=source_entry,
        )
        return SimpleNamespace(
            entry_id=(source_entry.entry_id if source_entry is not None else ""),
            base_ccy=_pair_position_base_ccy(derivative_position),
            pair_kind=(source_entry.pair_kind if source_entry is not None else "spot_future"),
            spot_inst_id=spot_position.inst_id,
            derivative_inst_id=derivative_position.inst_id,
            spot_qty=spot_qty,
            derivative_qty=derivative_qty,
            open_spot_price=(getattr(source_entry, "open_spot_price", None) if source_entry is not None else None),
            open_derivative_price=(getattr(source_entry, "open_derivative_price", None) if source_entry is not None else None),
            notes=(getattr(source_entry, "notes", "") if source_entry is not None else "live_roll_source"),
        )

    def _refresh_roll_positions(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        self.roll_status_text.set("正在读取当前持仓…")

        def _worker() -> None:
            error: str | None = None
            positions: list[OkxPosition] = []
            instruments: dict[str, Instrument] = {}
            reference_prices: dict[str, Decimal] = {}
            try:
                derivative_positions = self.client.get_positions(runtime.credentials, environment=runtime.environment)
                overview = self.client.get_account_overview(runtime.credentials, environment=runtime.environment)
                spot_positions = _build_spot_positions_from_account(overview, self.client)
                positions = spot_positions + [item for item in derivative_positions if item.inst_type == "FUTURES"]
                for position in positions:
                    try:
                        instrument = self.client.get_instrument(position.inst_id)
                    except Exception:
                        instrument = None
                    if instrument is not None:
                        instruments[position.inst_id] = instrument
                    if position.inst_type != "FUTURES":
                        continue
                    reference_price = position.mark_price or position.last_price
                    if (reference_price is None or reference_price <= 0) and instrument is not None:
                        try:
                            ticker = self.client.get_ticker(position.inst_id)
                        except Exception:
                            ticker = None
                        if ticker is not None:
                            for candidate in (ticker.last, getattr(ticker, "mark_price", None), ticker.bid, ticker.ask):
                                if candidate is not None and candidate > 0:
                                    reference_price = candidate
                                    break
                    if reference_price is not None and reference_price > 0:
                        reference_prices[position.inst_id] = reference_price
            except Exception as exc:
                error = str(exc)

            def _apply() -> None:
                if error is not None:
                    self.roll_status_text.set(f"当前持仓读取失败：{error}")
                    return
                self._roll_positions = positions
                self._roll_instruments = instruments
                self._roll_reference_prices = reference_prices
                self._refresh_roll_entry_options()
                self.roll_status_text.set(f"已读取当前持仓 {len(positions)} 条。")
                self._schedule_market_panel_refresh(initial_delay_ms=50)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="roll-positions", daemon=True).start()

    def _selected_roll_entry_id(self) -> str | None:
        entry = self._selected_roll_entry()
        if entry is None or not getattr(entry, "entry_id", ""):
            return None
        return str(entry.entry_id)

    def _refresh_roll_entry_options(self) -> None:
        selected_position = self._selected_roll_position()
        selected_inst_id = selected_position.inst_id if selected_position is not None else ""
        spot_by_base: dict[str, OkxPosition] = {}
        for position in self._roll_positions:
            if position.inst_type != "SPOT" or _pair_position_closeable_size(position) <= 0:
                continue
            spot_by_base.setdefault(_pair_position_base_ccy(position), position)
        option_map: dict[str, OkxPosition] = {}
        derivative_values: list[str] = []
        for position in self._roll_positions:
            if position.inst_type != "FUTURES" or _pair_position_closeable_size(position) <= 0:
                continue
            if _pair_position_base_ccy(position) not in spot_by_base:
                continue
            instrument = self._roll_instruments.get(position.inst_id)
            label = _pair_position_label(position, instrument)
            option_map[label] = position
            derivative_values.append(label)
        self._roll_spot_by_base = spot_by_base
        self._roll_position_by_key = option_map
        if hasattr(self, "roll_entry_combo"):
            self.roll_entry_combo.configure(values=derivative_values)
        if not derivative_values:
            self._roll_entry_label.set("")
            self.roll_contract_qty.set("")
            self.roll_position_summary_text.set("当前没有可用于移仓的现货/交割合约持仓。")
            self.roll_preview_text.set("请先刷新当前持仓，或确认账户里有对应现货和交割合约。")
            if self._roll_market_panel is not None:
                self._clear_market_panel(self._roll_market_panel, message="当前没有可用于移仓的现货/交割合约持仓。")
            return
        target_label = next(
            (label for label, position in option_map.items() if position.inst_id == selected_inst_id),
            derivative_values[0],
        )
        self._roll_entry_label.set(target_label)
        self._refresh_roll_target_options()
        self._refresh_roll_preview()

    def _apply_roll_target_options(self, instruments: list[Instrument]) -> None:
        self._roll_future_instruments = instruments
        current_position = self._selected_roll_position()
        candidates = (
            _roll_target_future_candidates(current_position.inst_id, instruments)
            if isinstance(current_position, OkxPosition)
            else []
        )
        if hasattr(self, "roll_target_derivative_combo"):
            self.roll_target_derivative_combo.configure(values=candidates)
        selected_target = self.roll_target_derivative_inst_id.get().strip().upper()
        if selected_target in candidates:
            self.roll_target_derivative_inst_id.set(selected_target)
            return
        if candidates:
            self.roll_target_derivative_inst_id.set(candidates[0])
            return
        self.roll_target_derivative_inst_id.set("")

    def _refresh_roll_target_options(self, *, fetch: bool = False) -> None:
        current_position = self._selected_roll_position()
        if current_position is None:
            self._apply_roll_target_options(self._roll_future_instruments)
            return
        if not fetch and self._roll_future_instruments:
            self._apply_roll_target_options(self._roll_future_instruments)
            return
        self.roll_status_text.set("正在刷新目标交割合约…")

        def _worker() -> None:
            error: str | None = None
            instruments: list[Instrument] = []
            try:
                instruments = self.client.get_instruments("FUTURES")
            except Exception as exc:
                error = str(exc)

            def _apply() -> None:
                if error is not None:
                    self.roll_status_text.set(f"目标交割合约刷新失败：{error}")
                    return
                self._apply_roll_target_options(instruments)
                candidate_count = len(
                    _roll_target_future_candidates(current_position.inst_id, instruments)
                    if isinstance(current_position, OkxPosition)
                    else []
                )
                selected_target = self.roll_target_derivative_inst_id.get().strip().upper()
                if selected_target:
                    self.roll_status_text.set(f"已刷新目标交割合约，共 {candidate_count} 个候选。")
                else:
                    self.roll_status_text.set("当前没有更远的同系列交割合约可选。")
                self._refresh_roll_preview()

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="roll-target-futures", daemon=True).start()

    def _set_roll_entry(self, entry_id: str, *, fill_max: bool, focus_tab: bool) -> None:
        entry = self._ledger_entry_by_id.get(entry_id)
        if entry is None or getattr(entry, "close_mode", "") != "open":
            return
        if infer_inst_type(entry.derivative_inst_id) != "FUTURES":
            return
        target_label = next(
            (
                label
                for label, position in self._roll_position_by_key.items()
                if position.inst_id == entry.derivative_inst_id and _pair_position_base_ccy(position) == entry.base_ccy
            ),
            "",
        )
        if not target_label:
            self.roll_status_text.set("账本记录已定位，但当前账户里没有找到对应的交割持仓，请先刷新当前持仓。")
            return
        self._roll_source_entry_id = entry.entry_id
        self._roll_entry_label.set(target_label)
        if fill_max or not self.roll_contract_qty.get().strip():
            position = self._roll_position_by_key.get(target_label)
            max_qty = entry.derivative_qty
            if isinstance(position, OkxPosition):
                instrument = self._roll_instruments.get(position.inst_id)
                if instrument is not None:
                    live_qty = snap_to_increment(_pair_position_closeable_size(position), instrument.lot_size, "down")
                    max_qty = min(max_qty, live_qty)
            self.roll_contract_qty.set(format_decimal(max_qty))
        if focus_tab and self._roll_tab is not None:
            self._notebook.select(self._roll_tab)
        self._refresh_roll_target_options()
        self._refresh_roll_preview()

    def _on_roll_entry_selected(self, _event=None) -> None:
        selected = self._selected_roll_positions()
        if selected is None:
            self.roll_position_summary_text.set("请先刷新并选择一条当前交割合约持仓。")
            self.roll_preview_text.set("请选择一条当前交割合约持仓。")
            if self._roll_market_panel is not None:
                self._clear_market_panel(self._roll_market_panel, message="请选择一条当前交割合约持仓。")
            return
        spot_position, derivative_position = selected
        source_entry = self._current_roll_source_entry(spot_position, derivative_position)
        if source_entry is None:
            self._roll_source_entry_id = None
        if not self.roll_contract_qty.get().strip():
            entry = self._selected_roll_entry()
            if entry is not None:
                self.roll_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_roll_target_options()
        self._refresh_roll_preview()
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _fill_roll_qty_with_max(self) -> None:
        entry = self._selected_roll_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先刷新并选择一条当前交割合约持仓。", parent=self.window)
            return
        self.roll_contract_qty.set(format_decimal(entry.derivative_qty))
        self._refresh_roll_preview()

    def _load_roll_from_ledger_selection(self) -> None:
        entry = self._selected_ledger_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先在账本中选择一条未平仓记录。", parent=self.window)
            return
        if infer_inst_type(entry.derivative_inst_id) != "FUTURES":
            messagebox.showwarning("提示", "当前只支持交割合约移仓。", parent=self.window)
            return
        if not self._roll_position_by_key:
            messagebox.showwarning("提示", "请先刷新当前持仓，再按账本定位。", parent=self.window)
            return
        self._set_roll_entry(entry.entry_id, fill_max=True, focus_tab=True)

    def _refresh_roll_preview(self) -> None:
        selected = self._selected_roll_positions()
        entry = self._selected_roll_entry()
        if selected is None or entry is None:
            self.roll_position_summary_text.set("请先刷新并选择一条当前交割合约持仓。")
            self.roll_preview_text.set("请选择当前交割合约持仓，并填写更远交割合约。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        spot_position, derivative_position = selected
        spot_instrument = self._roll_instruments.get(spot_position.inst_id)
        derivative_instrument = self._roll_instruments.get(derivative_position.inst_id)
        if spot_instrument is None or derivative_instrument is None:
            self.roll_preview_text.set("当前缺少持仓对应的合约元数据，请先刷新当前持仓。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        source_entry = self._current_roll_source_entry(spot_position, derivative_position)
        self.roll_position_summary_text.set(
            "\n".join(
                [
                    f"现货持仓：{_pair_position_label(spot_position, spot_instrument)}",
                    f"当前交割：{_pair_position_label(derivative_position, derivative_instrument)}",
                    ("账本关联：已关联当前账本选中记录" if source_entry is not None else "账本关联：未绑定，将按现有持仓直接移仓"),
                ]
            )
        )
        target_derivative_inst_id = self.roll_target_derivative_inst_id.get().strip().upper()
        if not target_derivative_inst_id:
            self.roll_preview_text.set("请先填写更远交割合约。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        try:
            derivative_qty = self._parse_roll_derivative_qty(entry)
            target_instrument = self.manager.get_instrument(target_derivative_inst_id)
            if target_instrument.inst_type != "FUTURES":
                raise ValueError("当前移仓只支持更远交割合约作为目标。")
            if _pair_position_base_ccy(derivative_position) != target_derivative_inst_id.split("-")[0].strip().upper():
                raise ValueError("目标交割合约的币种必须和当前持仓一致。")
            current_ticker = self.client.get_ticker(entry.derivative_inst_id)
            target_ticker = self.client.get_ticker(target_derivative_inst_id)
            current_buy = current_ticker.ask or current_ticker.last
            target_sell = target_ticker.bid or target_ticker.last
            if current_buy is None or target_sell is None:
                raise ValueError("当前缺少有效盘口，无法预估移仓价差。")
            roll_spot_qty = self._estimate_roll_spot_qty(
                spot_position=spot_position,
                derivative_position=derivative_position,
                spot_instrument=spot_instrument,
                derivative_instrument=derivative_instrument,
                derivative_qty=derivative_qty,
                source_entry=source_entry,
            )
            spread_abs = target_sell - current_buy
            planned_batches = _split_pair_close_batches(
                derivative_qty,
                derivative_instrument=derivative_instrument,
                batch_count=self._parse_roll_batch_count(),
                batch_qty=self._parse_roll_batch_qty(),
            )
        except (InvalidOperation, ValueError, Exception) as exc:
            self.roll_preview_text.set(f"预览失败：{exc}")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.roll_preview_text.set(
            "\n".join(
                [
                    f"本次移仓：回补 {entry.derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"并开出 {target_derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"对应继续占用现货：{format_decimal(roll_spot_qty)} {entry.base_ccy}",
                    f"当前移仓绝对价差：{format_decimal(spread_abs)}",
                    f"分批计划：{len(planned_batches)} 批 | {', '.join(format_decimal(item) for item in planned_batches)}",
                    f"执行方式：{self.roll_execution_mode_label.get().strip()} | 最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _build_roll_request(self, *, entry, roll_derivative_qty: Decimal) -> ArbitrageRollRequest:
        selected = self._selected_roll_positions()
        if selected is None:
            raise ValueError("请先刷新并选择一条当前交割合约持仓。")
        spot_position, derivative_position = selected
        return ArbitrageRollRequest(
            entry_id=(entry.entry_id or None),
            target_derivative_inst_id=self.roll_target_derivative_inst_id.get().strip().upper(),
            max_slippage=self._parse_max_slippage(),
            use_limit_orders=self.use_limit_orders.get(),
            roll_derivative_qty=roll_derivative_qty,
            current_derivative_limit_price=self._parse_optional_decimal(self.roll_current_limit_price.get()),
            target_derivative_limit_price=self._parse_optional_decimal(self.roll_target_limit_price.get()),
            batch_count=self._parse_roll_batch_count(),
            batch_contract_qty=self._parse_roll_batch_qty(),
            execution_mode=self._current_roll_execution_mode(),
            maker_wait_seconds=self._parse_roll_maker_wait_seconds(),
            chase_limit=self._parse_roll_chase_limit(),
            base_ccy=entry.base_ccy,
            spot_inst_id=spot_position.inst_id,
            current_derivative_inst_id=derivative_position.inst_id,
            spot_qty=entry.spot_qty,
            current_derivative_qty=entry.derivative_qty,
        )

    def _submit_roll(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        entry = self._selected_roll_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先刷新并选择一条当前交割合约持仓。", parent=self.window)
            return
        try:
            roll_qty = self._parse_roll_derivative_qty(entry)
            request = self._build_roll_request(entry=entry, roll_derivative_qty=roll_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        if not request.target_derivative_inst_id:
            messagebox.showwarning("提示", "请先填写更远交割合约。", parent=self.window)
            return
        if not messagebox.askyesno(
            "确认移仓",
            (
                f"将回补 {entry.derivative_inst_id} {format_decimal(roll_qty)} 张，\n"
                f"并开出 {request.target_derivative_inst_id} 对应数量。\n确认继续？"
            ),
            parent=self.window,
        ):
            return

        self.roll_status_text.set("正在执行移仓…")
        self._append_log("交割合约移仓：提交中…")

        def _worker() -> None:
            result = self.manager.roll_now(request, runtime=runtime)

            def _apply() -> None:
                self.roll_status_text.set(result.message)
                self._append_log(result.message)
                if result.success:
                    self._reload_ledger()
                    self._refresh_roll_positions()
                    messagebox.showinfo("移仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("移仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-roll-now", daemon=True).start()

    def _refresh_pair_close_positions(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        self.pair_close_status_text.set("正在读取当前持仓…")

        def _worker() -> None:
            error: str | None = None
            positions: list[OkxPosition] = []
            instruments: dict[str, Instrument] = {}
            reference_prices: dict[str, Decimal] = {}
            try:
                derivative_positions = self.client.get_positions(runtime.credentials, environment=runtime.environment)
                overview = self.client.get_account_overview(runtime.credentials, environment=runtime.environment)
                spot_positions = _build_spot_positions_from_account(overview, self.client)
                positions = spot_positions + [item for item in derivative_positions if item.inst_type in {"SWAP", "FUTURES"}]
                for position in positions:
                    try:
                        instrument = self.client.get_instrument(position.inst_id)
                    except Exception:
                        instrument = None
                    if instrument is not None:
                        instruments[position.inst_id] = instrument
                    if position.inst_type not in {"SWAP", "FUTURES"}:
                        continue
                    reference_price = position.mark_price or position.last_price
                    if (reference_price is None or reference_price <= 0) and instrument is not None:
                        try:
                            ticker = self.client.get_ticker(position.inst_id)
                        except Exception:
                            ticker = None
                        if ticker is not None:
                            for candidate in (ticker.last, getattr(ticker, "mark_price", None), ticker.bid, ticker.ask):
                                if candidate is not None and candidate > 0:
                                    reference_price = candidate
                                    break
                    if reference_price is not None and reference_price > 0:
                        reference_prices[position.inst_id] = reference_price
            except Exception as exc:
                error = str(exc)

            def _apply() -> None:
                if error is not None:
                    self.pair_close_status_text.set(f"当前持仓读取失败：{error}")
                    return
                self._pair_close_positions = positions
                self._pair_close_instruments = instruments
                self._pair_close_reference_prices = reference_prices
                self._refresh_pair_close_position_options()
                self.pair_close_status_text.set(f"已读取当前持仓 {len(positions)} 条。")
                self._schedule_market_panel_refresh(initial_delay_ms=50)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="pair-close-positions", daemon=True).start()

    def _pair_close_reference_price(self, position: OkxPosition, instrument: Instrument) -> Decimal | None:
        reference_price = position.mark_price or position.last_price
        if reference_price is not None and reference_price > 0:
            return reference_price
        cached = self._pair_close_reference_prices.get(instrument.inst_id)
        if cached is not None and cached > 0:
            return cached
        return None

    def _refresh_pair_close_position_options(self) -> None:
        spot_positions = [item for item in self._pair_close_positions if item.inst_type == "SPOT" and _pair_position_closeable_size(item) > 0]
        derivative_positions = [
            item
            for item in self._pair_close_positions
            if item.inst_type in {"SWAP", "FUTURES"} and _pair_position_closeable_size(item) > 0
        ]
        selected_spot = self._pair_close_position_by_key.get(self._pair_close_spot_key.get().strip())
        selected_derivative = self._pair_close_position_by_key.get(self._pair_close_derivative_key.get().strip())

        option_map: dict[str, OkxPosition] = {}
        spot_values: list[str] = []
        for position in spot_positions:
            instrument = self._pair_close_instruments.get(position.inst_id)
            label = _pair_position_label(position, instrument)
            option_map[label] = position
            spot_values.append(label)
        self._pair_close_position_by_key = option_map
        self.pair_close_spot_combo.configure(values=spot_values)

        if selected_spot is not None:
            selected_spot_base = _pair_position_base_ccy(selected_spot)
            derivative_positions = [item for item in derivative_positions if _pair_position_base_ccy(item) == selected_spot_base] or derivative_positions

        derivative_values: list[str] = []
        for position in derivative_positions:
            instrument = self._pair_close_instruments.get(position.inst_id)
            label = _pair_position_label(position, instrument)
            self._pair_close_position_by_key[label] = position
            derivative_values.append(label)
        self.pair_close_derivative_combo.configure(values=derivative_values)

        self._pair_close_spot_key.set(
            next((label for label in spot_values if option_map.get(label) == selected_spot), spot_values[0] if spot_values else "")
        )
        self._pair_close_derivative_key.set(
            next(
                (label for label in derivative_values if self._pair_close_position_by_key.get(label) == selected_derivative),
                derivative_values[0] if derivative_values else "",
            )
        )
        if not spot_values or not derivative_values:
            self.pair_close_preview_text.set("当前没有可用的现货/交割(永续)持仓配对。")
            if self._pair_close_market_panel is not None:
                self._clear_market_panel(self._pair_close_market_panel, message="当前没有可用的现货/交割合约配对。")
            return
        self._refresh_pair_close_preview()
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _selected_pair_close_positions(self) -> tuple[OkxPosition, OkxPosition] | None:
        spot = self._pair_close_position_by_key.get(self._pair_close_spot_key.get().strip())
        derivative = self._pair_close_position_by_key.get(self._pair_close_derivative_key.get().strip())
        if not isinstance(spot, OkxPosition) or not isinstance(derivative, OkxPosition):
            return None
        return spot, derivative

    def _selected_pair_close_identity(self) -> tuple[str, str, str, str] | None:
        selected = self._selected_pair_close_positions()
        if selected is None:
            return None
        spot_position, derivative_position = selected
        return (
            spot_position.inst_id,
            derivative_position.inst_id,
            _pair_position_direction(spot_position),
            _pair_position_direction(derivative_position),
        )

    def _is_pair_close_auto_running(self) -> bool:
        return self._pair_close_auto_thread is not None and self._pair_close_auto_thread.is_alive()

    def _parse_pair_close_trigger_threshold(self) -> tuple[str, Decimal | None, Decimal | None]:
        close_mode = self._current_pair_close_trigger_mode()
        if close_mode == "spread_abs":
            return close_mode, None, Decimal(self.pair_close_spread_abs_min.get().strip() or "0")
        return close_mode, Decimal(self.pair_close_spread_pct_min.get().strip() or "0"), None

    def _parse_pair_close_batch_settings(self, derivative_instrument: Instrument, total_qty: Decimal) -> list[Decimal]:
        batch_count_text = self.pair_close_batch_count.get().strip() or "1"
        batch_count = int(batch_count_text)
        if batch_count <= 0:
            raise ValueError("分批次数必须大于 0。")
        batch_qty_text = self.pair_close_batch_qty.get().strip()
        batch_qty = Decimal(batch_qty_text) if batch_qty_text else None
        return _split_pair_close_batches(
            total_qty,
            derivative_instrument=derivative_instrument,
            batch_count=batch_count,
            batch_qty=batch_qty,
        )

    def _parse_open_batch_count(self) -> int:
        batch_count = int(self.open_batch_count.get().strip() or "1")
        if batch_count <= 0:
            raise ValueError("开仓分批次数必须大于 0。")
        return batch_count

    def _parse_open_batch_qty(self) -> Decimal | None:
        batch_qty_text = self.open_batch_qty.get().strip()
        if not batch_qty_text:
            return None
        batch_qty = Decimal(batch_qty_text)
        if batch_qty <= 0:
            raise ValueError("开仓每批张数必须大于 0。")
        return batch_qty

    def _parse_close_batch_count(self) -> int:
        batch_count = int(self.close_batch_count.get().strip() or "1")
        if batch_count <= 0:
            raise ValueError("平仓分批次数必须大于 0。")
        return batch_count

    def _parse_close_batch_qty(self) -> Decimal | None:
        batch_qty_text = self.close_batch_qty.get().strip()
        if not batch_qty_text:
            return None
        batch_qty = Decimal(batch_qty_text)
        if batch_qty <= 0:
            raise ValueError("平仓每批张数必须大于 0。")
        return batch_qty

    def _parse_roll_batch_count(self) -> int:
        batch_count = int(self.roll_batch_count.get().strip() or "1")
        if batch_count <= 0:
            raise ValueError("移仓分批次数必须大于 0。")
        return batch_count

    def _parse_roll_batch_qty(self) -> Decimal | None:
        batch_qty_text = self.roll_batch_qty.get().strip()
        if not batch_qty_text:
            return None
        batch_qty = Decimal(batch_qty_text)
        if batch_qty <= 0:
            raise ValueError("移仓每批张数必须大于 0。")
        return batch_qty

    def _parse_open_maker_wait_seconds(self) -> float:
        wait_seconds = float(self.open_maker_wait_seconds.get().strip() or "6")
        if wait_seconds <= 0:
            raise ValueError("开仓挂单等待秒数必须大于 0。")
        return wait_seconds

    def _parse_open_chase_limit(self) -> int:
        chase_limit = int(self.open_chase_limit.get().strip() or "3")
        if chase_limit < 0:
            raise ValueError("开仓追单次数不能小于 0。")
        return chase_limit

    def _parse_close_maker_wait_seconds(self) -> float:
        wait_seconds = float(self.close_maker_wait_seconds.get().strip() or "6")
        if wait_seconds <= 0:
            raise ValueError("平仓挂单等待秒数必须大于 0。")
        return wait_seconds

    def _parse_close_chase_limit(self) -> int:
        chase_limit = int(self.close_chase_limit.get().strip() or "3")
        if chase_limit < 0:
            raise ValueError("平仓追单次数不能小于 0。")
        return chase_limit

    def _parse_roll_maker_wait_seconds(self) -> float:
        wait_seconds = float(self.roll_maker_wait_seconds.get().strip() or "6")
        if wait_seconds <= 0:
            raise ValueError("移仓挂单等待秒数必须大于 0。")
        return wait_seconds

    def _parse_roll_chase_limit(self) -> int:
        chase_limit = int(self.roll_chase_limit.get().strip() or "3")
        if chase_limit < 0:
            raise ValueError("移仓追单次数不能小于 0。")
        return chase_limit

    def _parse_pair_close_maker_wait_seconds(self) -> float:
        wait_seconds = float(self.pair_close_maker_wait_seconds.get().strip() or "6")
        if wait_seconds <= 0:
            raise ValueError("挂单等待秒数必须大于 0。")
        return wait_seconds

    def _parse_pair_close_chase_limit(self) -> int:
        chase_limit = int(self.pair_close_chase_limit.get().strip() or "3")
        if chase_limit < 0:
            raise ValueError("追单次数不能小于 0。")
        return chase_limit

    def _refresh_pair_close_live_spread(self, spot_inst_id: str, derivative_inst_id: str) -> tuple[Decimal | None, Decimal | None]:
        environment = self._selected_api_environment()
        try:
            self.client.ensure_public_ws_market_watch(spot_inst_id, environment=environment)
            self.client.ensure_public_ws_market_watch(derivative_inst_id, environment=environment)
        except Exception:
            pass
        spot_payload = None
        derivative_payload = None
        try:
            spot_payload = self.client.get_cached_public_ticker(spot_inst_id, environment=environment)
            derivative_payload = self.client.get_cached_public_ticker(derivative_inst_id, environment=environment)
        except Exception:
            spot_payload = None
            derivative_payload = None
        if spot_payload is not None and derivative_payload is not None:
            _, spot_ticker = spot_payload
            _, derivative_ticker = derivative_payload
        else:
            spot_ticker = self.client.get_ticker(spot_inst_id)
            derivative_ticker = self.client.get_ticker(derivative_inst_id)
        spot_mid = mid_price(spot_ticker.bid, spot_ticker.ask)
        derivative_mid = mid_price(derivative_ticker.bid, derivative_ticker.ask)
        if spot_mid is None or derivative_mid is None or spot_mid <= 0:
            return None, None
        spread_abs = derivative_mid - spot_mid
        spread_pct = spread_abs / spot_mid * Decimal("100")
        return spread_pct, spread_abs

    def _build_pair_close_preview(self) -> tuple[OkxPosition, OkxPosition, Instrument, Instrument, Decimal, Decimal]:
        selected = self._selected_pair_close_positions()
        if selected is None:
            raise ValueError("请先选择一条现货持仓和一条交割/永续持仓。")
        spot_position, derivative_position = selected
        if _pair_position_base_ccy(spot_position) != _pair_position_base_ccy(derivative_position):
            raise ValueError("现货和交割/永续持仓不是同一币种，无法配对平仓。")
        spot_instrument = self._pair_close_instruments.get(spot_position.inst_id)
        derivative_instrument = self._pair_close_instruments.get(derivative_position.inst_id)
        if spot_instrument is None:
            raise ValueError(f"现货 {spot_position.inst_id} 的标的元数据尚未就绪，请先重新刷新当前持仓。")
        if derivative_instrument is None:
            raise ValueError(f"合约 {derivative_position.inst_id} 的标的元数据尚未就绪，请先重新刷新当前持仓。")
        derivative_reference_price = self._pair_close_reference_price(derivative_position, derivative_instrument)
        max_derivative_qty = _pair_max_derivative_close_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=derivative_reference_price,
        )
        if max_derivative_qty < derivative_instrument.min_size:
            raise ValueError("当前两腿可配对数量不足最小下单量，无法执行配对平仓。")
        raw_text = self.pair_close_derivative_qty.get().strip()
        if raw_text:
            derivative_qty = snap_to_increment(Decimal(raw_text), derivative_instrument.lot_size, "down")
        else:
            derivative_qty = max_derivative_qty
        if derivative_qty <= 0:
            raise ValueError("平仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
        if derivative_qty > max_derivative_qty:
            raise ValueError(f"平仓数量不能超过当前可配对最大数量 {format_decimal(max_derivative_qty)} 张。")
        spot_qty = _pair_spot_qty_from_derivative_qty(
            derivative_qty,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=derivative_reference_price,
        )
        if spot_qty <= 0:
            raise ValueError("按当前合约数量换算后的现货平仓数量为 0，请加大数量。")
        return spot_position, derivative_position, spot_instrument, derivative_instrument, derivative_qty, spot_qty

    def _load_live_pair_close_plan(
        self,
        runtime: ArbitrageTradeRuntime,
        *,
        spot_inst_id: str,
        derivative_inst_id: str,
        spot_direction: str,
        derivative_direction: str,
        target_derivative_qty: Decimal,
    ) -> PairCloseLivePlan:
        positions = list(self.client.get_positions(runtime.credentials, environment=runtime.environment))
        try:
            overview = self.client.get_account_overview(runtime.credentials, environment=runtime.environment)
            positions.extend(_build_spot_positions_from_account(overview, self.client))
        except Exception:
            pass
        spot_candidates = [item for item in positions if item.inst_id == spot_inst_id and item.inst_type == "SPOT"]
        derivative_candidates = [
            item
            for item in positions
            if item.inst_id == derivative_inst_id
            and item.inst_type in {"SWAP", "FUTURES"}
            and _pair_position_closeable_size(item) > 0
        ]
        if not spot_candidates:
            raise ValueError(f"当前找不到现货持仓：{spot_inst_id}")
        if not derivative_candidates:
            raise ValueError(f"当前找不到交割/永续持仓：{derivative_inst_id}")
        spot_position = next(
            (item for item in spot_candidates if _pair_position_direction(item) == spot_direction),
            spot_candidates[0],
        )
        derivative_position = next(
            (item for item in derivative_candidates if _pair_position_direction(item) == derivative_direction),
            derivative_candidates[0],
        )
        spot_instrument = self.client.get_instrument(spot_inst_id)
        derivative_instrument = self.client.get_instrument(derivative_inst_id)
        derivative_reference_price = self._pair_close_reference_price(derivative_position, derivative_instrument)
        if derivative_reference_price is None:
            ticker = self.client.get_ticker(derivative_inst_id)
            derivative_reference_price = mid_price(ticker.bid, ticker.ask) or ticker.last
        max_derivative_qty = _pair_max_derivative_close_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=derivative_reference_price,
        )
        derivative_qty = min(
            snap_to_increment(target_derivative_qty, derivative_instrument.lot_size, "down"),
            max_derivative_qty,
        )
        if derivative_qty < derivative_instrument.min_size:
            raise ValueError("当前两腿可配对数量不足最小下单量。")
        spot_qty = _pair_spot_qty_from_derivative_qty(
            derivative_qty,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=derivative_reference_price,
        )
        if spot_qty <= 0:
            raise ValueError("当前批次换算后的现货数量为 0。")
        return PairCloseLivePlan(
            spot_position=spot_position,
            derivative_position=derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            derivative_qty=derivative_qty,
            spot_qty=spot_qty,
        )

    def _on_pair_close_selection_changed(self, _event=None) -> None:
        if self._pair_close_positions:
            self._refresh_pair_close_position_options()
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _fill_pair_close_qty_max(self) -> None:
        try:
            _, _, _, _, derivative_qty, _ = self._build_pair_close_preview()
        except Exception as exc:
            messagebox.showwarning("提示", str(exc), parent=self.window)
            return
        self.pair_close_derivative_qty.set(format_decimal(derivative_qty))
        self._refresh_pair_close_preview()

    def _refresh_pair_close_preview(self) -> None:
        try:
            spot_position, derivative_position, spot_instrument, derivative_instrument, derivative_qty, spot_qty = self._build_pair_close_preview()
        except Exception as exc:
            self.pair_close_preview_text.set(str(exc))
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        max_qty = _pair_max_derivative_close_qty(
            spot_position,
            derivative_position,
            spot_instrument=spot_instrument,
            derivative_instrument=derivative_instrument,
            reference_price=self._pair_close_reference_price(derivative_position, derivative_instrument),
        )
        self.pair_close_preview_text.set(
            "\n".join(
                [
                    f"现货腿：{spot_position.inst_id} | {_pair_position_direction(spot_position)} -> {_pair_position_close_side(spot_position)} | 币数 {format_decimal(_pair_position_closeable_size(spot_position))}",
                    (
                        f"合约腿：{derivative_position.inst_id} | {_pair_position_direction(derivative_position)} -> {_pair_position_close_side(derivative_position)}"
                        f" | 可平 {format_decimal(_pair_position_closeable_size(derivative_position))} 张"
                        f" | 折合 {format_decimal(_pair_position_base_exposure(derivative_position, derivative_instrument, reference_price=self._pair_close_reference_price(derivative_position, derivative_instrument)) or Decimal('0'))} "
                        f"{_pair_position_base_ccy(derivative_position)}"
                    ),
                    f"当前最大可配对：{format_decimal(max_qty)} 张",
                    f"本次将平合约：{format_decimal(derivative_qty)} 张",
                    f"对应现货将平：{format_decimal(spot_qty)} {_pair_position_base_ccy(spot_position)}",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _resolve_pair_close_price(self, instrument: Instrument, *, side: str, slippage: Decimal) -> Decimal:
        order_book = None
        try:
            order_book = self.client.get_order_book(instrument.inst_id, depth=5)
        except Exception:
            order_book = None
        ticker = self.client.get_ticker(instrument.inst_id)
        normalized_side = side.strip().lower()
        if self.use_limit_orders.get():
            if normalized_side == "buy":
                raw = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
                if raw is None or raw <= 0:
                    raise ValueError(f"{instrument.inst_id} 当前缺少买一价，无法按限价挂平仓单。")
                return snap_to_increment(raw, instrument.tick_size, "down")
            raw = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
            if raw is None or raw <= 0:
                raise ValueError(f"{instrument.inst_id} 当前缺少卖一价，无法按限价挂平仓单。")
            return snap_to_increment(raw, instrument.tick_size, "up")
        if normalized_side == "buy":
            ask = ticker.ask
            if ask is None or ask <= 0:
                raise ValueError(f"{instrument.inst_id} 当前缺少卖一价。")
            return snap_to_increment(ask * (Decimal("1") + slippage), instrument.tick_size, "up")
        bid = ticker.bid
        if bid is None or bid <= 0:
            raise ValueError(f"{instrument.inst_id} 当前缺少买一价。")
        return snap_to_increment(bid * (Decimal("1") - slippage), instrument.tick_size, "down")

    def _resolve_pair_close_passive_price(self, instrument: Instrument, *, side: str) -> Decimal:
        order_book = None
        try:
            order_book = self.client.get_order_book(instrument.inst_id, depth=5)
        except Exception:
            order_book = None
        ticker = self.client.get_ticker(instrument.inst_id)
        normalized_side = side.strip().lower()
        if normalized_side == "buy":
            raw = order_book.bids[0][0] if order_book is not None and order_book.bids else ticker.bid
            if raw is None or raw <= 0:
                raise ValueError(f"{instrument.inst_id} 当前缺少买一价，无法挂被动单。")
            return snap_to_increment(raw, instrument.tick_size, "down")
        raw = order_book.asks[0][0] if order_book is not None and order_book.asks else ticker.ask
        if raw is None or raw <= 0:
            raise ValueError(f"{instrument.inst_id} 当前缺少卖一价，无法挂被动单。")
        return snap_to_increment(raw, instrument.tick_size, "up")

    def _wait_pair_close_order_until(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        config: StrategyConfig,
        inst_id: str,
        ord_id: str,
        expected_size: Decimal,
        timeout_seconds: float,
        label: str,
    ) -> tuple[Decimal, Decimal | None, bool]:
        deadline = time.time() + timeout_seconds
        last_filled = Decimal("0")
        avg_price: Decimal | None = None
        filled_completely = False
        while time.time() < deadline:
            status = self.client.get_order(runtime.credentials, config, inst_id=inst_id, ord_id=ord_id)
            filled = status.filled_size or Decimal("0")
            avg_price = status.avg_price
            state = (status.state or "").lower()
            if filled > last_filled:
                self._append_log(f"{label} 成交进度 {format_decimal(filled)} / {format_decimal(expected_size)}")
                last_filled = filled
            if state == "filled" or filled >= expected_size:
                filled_completely = True
                break
            if state in {"canceled", "cancelled"}:
                break
            time.sleep(1.0)
        return last_filled, avg_price, filled_completely

    def _execute_pair_close_taker_leg(
        self,
        *,
        runtime: ArbitrageTradeRuntime,
        config: StrategyConfig,
        inst_id: str,
        side: str,
        size: Decimal,
        label: str,
        pos_side: str | None = None,
        reduce_only: bool = False,
    ) -> tuple[Decimal, Decimal | None]:
        result = self.client.place_simple_order(
            runtime.credentials,
            config,
            inst_id=inst_id,
            side=side,
            size=size,
            ord_type="market",
            pos_side=pos_side,
            reduce_only=reduce_only,
            cl_ord_id=f"pair{uuid.uuid4().hex[:14]}",
        )
        return _wait_order_fill(
            self.client,
            credentials=runtime.credentials,
            config=config,
            inst_id=inst_id,
            ord_id=result.ord_id,
            expected_size=size,
            logger=self._append_log,
            label=label,
        )

    def _execute_pair_close_batch_dual_taker(
        self,
        runtime: ArbitrageTradeRuntime,
        plan: PairCloseLivePlan,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        slippage = self._parse_max_slippage()
        derivative_config = _build_pair_close_strategy_config(plan.derivative_position, environment=runtime.environment)
        spot_config = _build_pair_close_strategy_config(plan.spot_position, environment=runtime.environment)
        derivative_side = _pair_position_close_side(plan.derivative_position)
        spot_side = _pair_position_close_side(plan.spot_position)
        derivative_pos_side = None
        if derivative_config.position_mode == "long_short":
            normalized_pos_side = (plan.derivative_position.pos_side or "").strip().lower()
            derivative_pos_side = (
                normalized_pos_side if normalized_pos_side in {"long", "short"} else _pair_position_direction(plan.derivative_position)
            )
        derivative_price = self._resolve_pair_close_price(plan.derivative_instrument, side=derivative_side, slippage=slippage)
        spot_price = self._resolve_pair_close_price(plan.spot_instrument, side=spot_side, slippage=slippage)
        derivative_result = self.client.place_simple_order(
            runtime.credentials,
            derivative_config,
            inst_id=plan.derivative_position.inst_id,
            side=derivative_side,
            size=plan.derivative_qty,
            ord_type="limit" if self.use_limit_orders.get() else "market",
            pos_side=derivative_pos_side,
            price=derivative_price if self.use_limit_orders.get() else None,
            reduce_only=True,
            cl_ord_id=f"pair{uuid.uuid4().hex[:14]}",
        )
        derivative_filled, derivative_avg = _wait_order_fill(
            self.client,
            credentials=runtime.credentials,
            config=derivative_config,
            inst_id=plan.derivative_position.inst_id,
            ord_id=derivative_result.ord_id,
            expected_size=plan.derivative_qty,
            logger=self._append_log,
            label="配对平仓合约腿",
        )
        actual_spot_qty = _pair_spot_qty_from_derivative_qty(
            derivative_filled,
            spot_instrument=plan.spot_instrument,
            derivative_instrument=plan.derivative_instrument,
            reference_price=derivative_avg or self._pair_close_reference_price(plan.derivative_position, plan.derivative_instrument),
        )
        if actual_spot_qty <= 0:
            raise OkxApiError("合约腿成交后换算出的现货平仓数量为 0。")
        spot_result = self.client.place_simple_order(
            runtime.credentials,
            spot_config,
            inst_id=plan.spot_position.inst_id,
            side=spot_side,
            size=actual_spot_qty,
            ord_type="limit" if self.use_limit_orders.get() else "market",
            price=spot_price if self.use_limit_orders.get() else None,
            cl_ord_id=f"pair{uuid.uuid4().hex[:14]}",
        )
        spot_filled, spot_avg = _wait_order_fill(
            self.client,
            credentials=runtime.credentials,
            config=spot_config,
            inst_id=plan.spot_position.inst_id,
            ord_id=spot_result.ord_id,
            expected_size=actual_spot_qty,
            logger=self._append_log,
            label="配对平仓现货腿",
        )
        return derivative_filled, derivative_avg, spot_filled, spot_avg

    def _execute_pair_close_batch_maker_taker(
        self,
        runtime: ArbitrageTradeRuntime,
        plan: PairCloseLivePlan,
        *,
        maker_leg: str,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        maker_wait_seconds = self._parse_pair_close_maker_wait_seconds()
        chase_limit = self._parse_pair_close_chase_limit()
        derivative_config = _build_pair_close_strategy_config(plan.derivative_position, environment=runtime.environment)
        spot_config = _build_pair_close_strategy_config(plan.spot_position, environment=runtime.environment)
        derivative_side = _pair_position_close_side(plan.derivative_position)
        spot_side = _pair_position_close_side(plan.spot_position)
        derivative_pos_side = None
        if derivative_config.position_mode == "long_short":
            normalized_pos_side = (plan.derivative_position.pos_side or "").strip().lower()
            derivative_pos_side = (
                normalized_pos_side if normalized_pos_side in {"long", "short"} else _pair_position_direction(plan.derivative_position)
            )
        total_derivative_filled = Decimal("0")
        total_spot_filled = Decimal("0")
        derivative_avg: Decimal | None = None
        spot_avg: Decimal | None = None
        residual_spot_qty = Decimal("0")
        remaining_derivative_qty = plan.derivative_qty
        reference_price = self._pair_close_reference_price(plan.derivative_position, plan.derivative_instrument)

        for attempt in range(chase_limit + 1):
            if remaining_derivative_qty < plan.derivative_instrument.min_size:
                break
            if maker_leg == "derivative":
                maker_inst_id = plan.derivative_position.inst_id
                maker_config = derivative_config
                maker_side = derivative_side
                maker_pos_side = derivative_pos_side
                maker_reduce_only = True
                maker_size = remaining_derivative_qty
                maker_instrument = plan.derivative_instrument
                maker_label = f"配对平仓挂单腿(合约) 第 {attempt + 1} 次"
            else:
                maker_inst_id = plan.spot_position.inst_id
                maker_config = spot_config
                maker_side = spot_side
                maker_pos_side = None
                maker_reduce_only = False
                maker_size = _pair_spot_qty_from_derivative_qty(
                    remaining_derivative_qty,
                    spot_instrument=plan.spot_instrument,
                    derivative_instrument=plan.derivative_instrument,
                    reference_price=reference_price,
                )
                maker_instrument = plan.spot_instrument
                maker_label = f"配对平仓挂单腿(现货) 第 {attempt + 1} 次"
            if maker_size <= 0:
                raise ValueError("挂单腿当前批次数量为 0。")
            maker_price = self._resolve_pair_close_passive_price(maker_instrument, side=maker_side)
            maker_order = self.client.place_simple_order(
                runtime.credentials,
                maker_config,
                inst_id=maker_inst_id,
                side=maker_side,
                size=maker_size,
                ord_type="post_only",
                pos_side=maker_pos_side,
                price=maker_price,
                reduce_only=maker_reduce_only,
                cl_ord_id=f"pair{uuid.uuid4().hex[:14]}",
            )
            maker_filled, maker_avg, maker_done = self._wait_pair_close_order_until(
                runtime=runtime,
                config=maker_config,
                inst_id=maker_inst_id,
                ord_id=maker_order.ord_id,
                expected_size=maker_size,
                timeout_seconds=maker_wait_seconds,
                label=maker_label,
            )
            if not maker_done:
                try:
                    self.client.cancel_order(
                        runtime.credentials,
                        maker_config,
                        inst_id=maker_inst_id,
                        ord_id=maker_order.ord_id,
                    )
                except Exception:
                    pass
            if maker_leg == "derivative":
                if maker_filled <= 0:
                    if attempt >= chase_limit:
                        raise OkxApiError("合约挂单腿未成交，已达到最大追单次数。")
                    continue
                hedge_spot_qty = _pair_spot_qty_from_derivative_qty(
                    maker_filled,
                    spot_instrument=plan.spot_instrument,
                    derivative_instrument=plan.derivative_instrument,
                    reference_price=maker_avg or reference_price,
                )
                if hedge_spot_qty <= 0:
                    raise OkxApiError("合约挂单腿成交后换算出的现货数量为 0。")
                spot_filled_once, spot_avg_once = self._execute_pair_close_taker_leg(
                    runtime=runtime,
                    config=spot_config,
                    inst_id=plan.spot_position.inst_id,
                    side=spot_side,
                    size=hedge_spot_qty,
                    label="配对平仓现货吃单腿",
                )
                total_derivative_filled += maker_filled
                total_spot_filled += spot_filled_once
                derivative_avg = maker_avg
                spot_avg = spot_avg_once
                remaining_derivative_qty = max(plan.derivative_qty - total_derivative_filled, Decimal("0"))
            else:
                if maker_filled <= 0:
                    if attempt >= chase_limit:
                        raise OkxApiError("现货挂单腿未成交，已达到最大追单次数。")
                    continue
                residual_spot_qty += maker_filled
                hedge_derivative_qty = _pair_derivative_qty_from_spot_qty(
                    residual_spot_qty,
                    derivative_instrument=plan.derivative_instrument,
                    reference_price=reference_price,
                )
                if hedge_derivative_qty <= 0:
                    if attempt >= chase_limit:
                        raise OkxApiError("现货挂单腿已部分成交，但不足以换算成最小合约张数。")
                    continue
                derivative_filled_once, derivative_avg_once = self._execute_pair_close_taker_leg(
                    runtime=runtime,
                    config=derivative_config,
                    inst_id=plan.derivative_position.inst_id,
                    side=derivative_side,
                    size=hedge_derivative_qty,
                    label="配对平仓合约吃单腿",
                    pos_side=derivative_pos_side,
                    reduce_only=True,
                )
                used_spot_qty = _pair_spot_qty_from_derivative_qty(
                    derivative_filled_once,
                    spot_instrument=plan.spot_instrument,
                    derivative_instrument=plan.derivative_instrument,
                    reference_price=derivative_avg_once or reference_price,
                )
                residual_spot_qty = max(residual_spot_qty - used_spot_qty, Decimal("0"))
                total_derivative_filled += derivative_filled_once
                total_spot_filled += used_spot_qty
                derivative_avg = derivative_avg_once
                spot_avg = maker_avg
                remaining_derivative_qty = max(plan.derivative_qty - total_derivative_filled, Decimal("0"))

        if total_derivative_filled <= 0 or total_spot_filled <= 0:
            raise OkxApiError("当前批次未形成有效配对成交。")
        if maker_leg == "spot" and residual_spot_qty >= plan.spot_instrument.lot_size:
            self._append_log(
                f"现货挂单腿有剩余未完全对冲：{format_decimal(residual_spot_qty)} { _pair_position_base_ccy(plan.spot_position) }"
            )
        return total_derivative_filled, derivative_avg, total_spot_filled, spot_avg

    def _execute_pair_close_batch(
        self,
        runtime: ArbitrageTradeRuntime,
        plan: PairCloseLivePlan,
        *,
        execution_mode: str,
    ) -> tuple[Decimal, Decimal | None, Decimal, Decimal | None]:
        if execution_mode == "spot_maker_derivative_taker":
            return self._execute_pair_close_batch_maker_taker(runtime, plan, maker_leg="spot")
        if execution_mode == "derivative_maker_spot_taker":
            return self._execute_pair_close_batch_maker_taker(runtime, plan, maker_leg="derivative")
        return self._execute_pair_close_batch_dual_taker(runtime, plan)

    def _execute_pair_close_batches(
        self,
        runtime: ArbitrageTradeRuntime,
        *,
        spot_inst_id: str,
        derivative_inst_id: str,
        spot_direction: str,
        derivative_direction: str,
        total_derivative_qty: Decimal,
        planned_batches: list[Decimal],
        execution_mode: str,
        auto_stop_event: threading.Event | None = None,
    ) -> str:
        total_derivative_filled = Decimal("0")
        total_spot_filled = Decimal("0")
        batch_messages: list[str] = []
        for index, planned_batch_qty in enumerate(planned_batches, start=1):
            if auto_stop_event is not None and auto_stop_event.is_set():
                raise RuntimeError("自动配对平仓已停止。")
            remaining_target = max(total_derivative_qty - total_derivative_filled, Decimal("0"))
            current_target = min(planned_batch_qty, remaining_target)
            if current_target <= 0:
                break
            self._set_pair_close_status_async(
                f"正在执行第 {index}/{len(planned_batches)} 批配对平仓：目标 {format_decimal(current_target)} 张"
            )
            plan = self._load_live_pair_close_plan(
                runtime,
                spot_inst_id=spot_inst_id,
                derivative_inst_id=derivative_inst_id,
                spot_direction=spot_direction,
                derivative_direction=derivative_direction,
                target_derivative_qty=current_target,
            )
            derivative_filled, derivative_avg, spot_filled, spot_avg = self._execute_pair_close_batch(
                runtime,
                plan,
                execution_mode=execution_mode,
            )
            total_derivative_filled += derivative_filled
            total_spot_filled += spot_filled
            batch_message = (
                f"第 {index} 批完成：合约 {format_decimal(derivative_filled)} 张"
                f"{' @ ' + format_decimal(derivative_avg) if derivative_avg is not None else ''} | "
                f"现货 {format_decimal(spot_filled)}"
                f"{' @ ' + format_decimal(spot_avg) if spot_avg is not None else ''}"
            )
            batch_messages.append(batch_message)
            self._append_log(batch_message)
            if self._pair_close_auto_session is not None:
                self._pair_close_auto_session.completed_batches = index
        return (
            f"配对平仓完成：现货 {spot_inst_id} 累计 {format_decimal(total_spot_filled)} | "
            f"合约 {derivative_inst_id} 累计 {format_decimal(total_derivative_filled)} 张\n"
            + "\n".join(batch_messages)
        )

    def _submit_pair_close(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        try:
            spot_position, derivative_position, spot_instrument, derivative_instrument, derivative_qty, spot_qty = self._build_pair_close_preview()
            planned_batches = self._parse_pair_close_batch_settings(derivative_instrument, derivative_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        identity = self._selected_pair_close_identity()
        if identity is None:
            messagebox.showwarning("提示", "请先选择一组现货/交割合约持仓。", parent=self.window)
            return
        if self._is_pair_close_auto_running():
            messagebox.showwarning("提示", "自动配对平仓监控运行中，请先停止。", parent=self.window)
            return
        execution_mode = self._current_pair_close_execution_mode()
        if not messagebox.askyesno(
            "确认配对平仓",
            (
                f"现货：{spot_position.inst_id} | 本次 {format_decimal(spot_qty)}\n"
                f"合约：{derivative_position.inst_id} | 本次 {format_decimal(derivative_qty)} 张\n"
                f"执行方式：{self.pair_close_execution_mode_label.get().strip()} | 分批 {len(planned_batches)} 次\n\n"
                "将按配对方式执行平仓。\n确认继续？"
            ),
            parent=self.window,
        ):
            return
        self.pair_close_status_text.set("正在执行配对平仓…")
        self._append_log(
            f"配对平仓提交中 | 现货={spot_position.inst_id} {format_decimal(spot_qty)} | "
            f"合约={derivative_position.inst_id} {format_decimal(derivative_qty)} 张 | 分批={len(planned_batches)}"
        )

        def _worker() -> None:
            try:
                message = self._execute_pair_close_batches(
                    runtime,
                    spot_inst_id=identity[0],
                    derivative_inst_id=identity[1],
                    spot_direction=identity[2],
                    derivative_direction=identity[3],
                    total_derivative_qty=derivative_qty,
                    planned_batches=planned_batches,
                    execution_mode=execution_mode,
                )
                error = None
            except Exception as exc:
                message = str(exc)
                error = exc

            def _apply() -> None:
                self.pair_close_status_text.set(message)
                self._append_log(message)
                if error is None:
                    messagebox.showinfo("配对平仓完成", message, parent=self.window)
                    self._refresh_pair_close_positions()
                else:
                    messagebox.showerror("配对平仓失败", message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="pair-close-submit", daemon=True).start()

    def _start_pair_close_auto(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        if self.manager.auto_open.is_running or self.manager.auto_close.is_running:
            messagebox.showwarning("提示", "请先停止其他自动开平仓监控。", parent=self.window)
            return
        if self._is_pair_close_auto_running():
            messagebox.showwarning("提示", "自动配对平仓监控已经在运行。", parent=self.window)
            return
        identity = self._selected_pair_close_identity()
        if identity is None:
            messagebox.showwarning("提示", "请先选择一组现货/交割合约持仓。", parent=self.window)
            return
        try:
            _, _, _, derivative_instrument, derivative_qty, _ = self._build_pair_close_preview()
            trigger_mode, spread_pct_min, spread_abs_min = self._parse_pair_close_trigger_threshold()
            planned_batches = self._parse_pair_close_batch_settings(derivative_instrument, derivative_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        execution_mode = self._current_pair_close_execution_mode()
        session = PairCloseAutoSession(
            spot_inst_id=identity[0],
            derivative_inst_id=identity[1],
            spot_direction=identity[2],
            derivative_direction=identity[3],
            target_derivative_qty=derivative_qty,
            planned_batches=tuple(planned_batches),
            trigger_mode=trigger_mode,
            spread_pct_min=spread_pct_min,
            spread_abs_min=spread_abs_min,
            execution_mode=execution_mode,
            status="监控中",
        )
        self._pair_close_auto_stop_event.clear()
        self._pair_close_auto_session = session
        self.pair_close_status_text.set("自动配对平仓监控已启动。")
        self._append_log(
            f"已启动自动配对平仓监控 | 合约={identity[1]} {format_decimal(derivative_qty)} 张 | "
            f"条件={self.pair_close_trigger_mode_label.get().strip()} | 分批={len(planned_batches)}"
        )

        def _worker() -> None:
            message = ""
            error: Exception | None = None
            try:
                while not self._pair_close_auto_stop_event.is_set():
                    spread_pct, spread_abs = self._refresh_pair_close_live_spread(session.spot_inst_id, session.derivative_inst_id)
                    session.last_spread_pct = spread_pct
                    session.last_spread_abs = spread_abs
                    should_trigger = False
                    if session.trigger_mode == "spread_abs":
                        should_trigger = spread_abs is not None and session.spread_abs_min is not None and spread_abs <= session.spread_abs_min
                        session.status = (
                            f"监控中 | 绝对价差 {format_decimal(spread_abs) if spread_abs is not None else '-'}"
                            f" / 触发值 {format_decimal(session.spread_abs_min) if session.spread_abs_min is not None else '-'}"
                        )
                    else:
                        should_trigger = spread_pct is not None and session.spread_pct_min is not None and spread_pct <= session.spread_pct_min
                        session.status = (
                            f"监控中 | 价差率 {format_decimal_fixed(spread_pct, 4) + '%' if spread_pct is not None else '-'}"
                            f" / 触发值 {format_decimal(session.spread_pct_min) + '%' if session.spread_pct_min is not None else '-'}"
                        )
                    self._set_pair_close_status_async(session.status)
                    if should_trigger:
                        session.triggered = True
                        session.status = "条件满足，开始执行自动配对平仓…"
                        self._set_pair_close_status_async(session.status)
                        self._append_log(session.status)
                        message = self._execute_pair_close_batches(
                            runtime,
                            spot_inst_id=session.spot_inst_id,
                            derivative_inst_id=session.derivative_inst_id,
                            spot_direction=session.spot_direction,
                            derivative_direction=session.derivative_direction,
                            total_derivative_qty=session.target_derivative_qty,
                            planned_batches=list(session.planned_batches),
                            execution_mode=session.execution_mode,
                            auto_stop_event=self._pair_close_auto_stop_event,
                        )
                        break
                    time.sleep(PAIR_CLOSE_MONITOR_POLL_SECONDS)
                if self._pair_close_auto_stop_event.is_set() and not session.triggered:
                    message = "自动配对平仓监控已停止。"
            except Exception as exc:
                message = str(exc)
                error = exc

            def _apply() -> None:
                if not self.window.winfo_exists():
                    return
                self._pair_close_auto_thread = None
                self._pair_close_auto_session = None
                self.pair_close_status_text.set(message or "自动配对平仓监控已结束。")
                self._append_log(message or "自动配对平仓监控已结束。")
                if error is None and session.triggered:
                    messagebox.showinfo("自动配对平仓完成", message, parent=self.window)
                    self._refresh_pair_close_positions()
                elif error is not None:
                    messagebox.showerror("自动配对平仓失败", message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        self._pair_close_auto_thread = threading.Thread(target=_worker, name="pair-close-auto", daemon=True)
        self._pair_close_auto_thread.start()

    def _stop_pair_close_auto(self, *, silent: bool = False) -> None:
        self._pair_close_auto_stop_event.set()
        thread = self._pair_close_auto_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=PAIR_CLOSE_MONITOR_POLL_SECONDS + 1.0)
        self._pair_close_auto_thread = None
        self._pair_close_auto_session = None
        if not silent:
            self.pair_close_status_text.set("自动配对平仓监控已停止。")
            self._append_log("已停止自动配对平仓监控。")

    def _parse_close_derivative_qty(self, entry) -> Decimal:
        instrument = self.manager.get_instrument(entry.derivative_inst_id)
        text = self.close_contract_qty.get().strip()
        if not text:
            return entry.derivative_qty
        requested_qty = snap_to_increment(Decimal(text), instrument.lot_size, "down")
        if requested_qty <= 0:
            raise ValueError("平仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
        if requested_qty > entry.derivative_qty:
            raise ValueError(f"平仓数量不能超过当前持仓 {format_decimal(entry.derivative_qty)} 张。")
        return requested_qty

    def _parse_roll_derivative_qty(self, entry) -> Decimal:
        instrument = self.manager.get_instrument(entry.derivative_inst_id)
        text = self.roll_contract_qty.get().strip()
        if not text:
            return entry.derivative_qty
        requested_qty = snap_to_increment(Decimal(text), instrument.lot_size, "down")
        if requested_qty <= 0:
            raise ValueError("移仓数量按合约最小变动单位向下取整后为 0，请加大数量。")
        if requested_qty > entry.derivative_qty:
            raise ValueError(f"移仓数量不能超过当前持仓 {format_decimal(entry.derivative_qty)} 张。")
        return requested_qty

    def _refresh_close_preview(self) -> None:
        entry = self._selected_close_entry()
        if entry is None:
            self.close_position_summary_text.set("暂无未平仓套利持仓。")
            self.close_preview_text.set("请先选择一条未平仓套利持仓。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.close_position_summary_text.set(
            "\n".join(
                [
                    f"币种：{entry.base_ccy}",
                    f"现货持仓：{format_decimal(entry.spot_qty)} | 合约持仓：{format_decimal(entry.derivative_qty)} 张",
                    f"现货：{entry.spot_inst_id} | 合约：{entry.derivative_inst_id}",
                ]
            )
        )
        try:
            derivative_qty = self._parse_close_derivative_qty(entry)
            derivative_instrument = self.manager.get_instrument(entry.derivative_inst_id)
            spot_instrument = self.manager.get_instrument(entry.spot_inst_id)
            spot_qty = snap_to_increment(
                spot_base_from_derivative_fill(
                    derivative_filled_contracts=derivative_qty,
                    derivative_instrument=derivative_instrument,
                ),
                spot_instrument.lot_size,
                "down",
            )
            remaining_contracts = max(entry.derivative_qty - derivative_qty, Decimal("0"))
            close_kind = "全部平仓" if remaining_contracts <= 0 else "部分平仓"
        except (InvalidOperation, ValueError, Exception) as exc:
            self.close_preview_text.set(f"预览失败：{exc}")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.close_preview_text.set(
            "\n".join(
                [
                    f"本次{close_kind}：合约买入 {format_decimal(derivative_qty)} 张",
                    f"对应现货卖出：{format_decimal(spot_qty)}",
                    f"平仓后剩余：{format_decimal(remaining_contracts)} 张",
                    f"执行方式：{'限价挂单' if self.use_limit_orders.get() else '市价成交'} | 最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _refresh_roll_preview(self) -> None:
        entry = self._selected_roll_entry()
        if entry is None:
            self.roll_position_summary_text.set("暂无可移仓的交割合约套利持仓。")
            self.roll_preview_text.set("请先选择一条未平仓交割合约持仓。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.roll_position_summary_text.set(
            "\n".join(
                [
                    f"币种：{entry.base_ccy}",
                    f"现货持仓：{format_decimal(entry.spot_qty)} | 当前交割持仓：{format_decimal(entry.derivative_qty)} 张",
                    f"现货：{entry.spot_inst_id} | 当前交割：{entry.derivative_inst_id}",
                ]
            )
        )
        target_derivative_inst_id = self.roll_target_derivative_inst_id.get().strip().upper()
        if not target_derivative_inst_id:
            self.roll_preview_text.set("请先填写更远交割合约。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        try:
            derivative_qty = self._parse_roll_derivative_qty(entry)
            current_instrument = self.manager.get_instrument(entry.derivative_inst_id)
            target_instrument = self.manager.get_instrument(target_derivative_inst_id)
            if target_instrument.inst_type != "FUTURES":
                raise ValueError("当前移仓页只支持交割合约作为目标。")
            current_ticker = self.client.get_ticker(entry.derivative_inst_id)
            target_ticker = self.client.get_ticker(target_derivative_inst_id)
            current_buy = current_ticker.ask or current_ticker.last
            target_sell = target_ticker.bid or target_ticker.last
            if current_buy is None or target_sell is None:
                raise ValueError("当前缺少有效盘口，无法预估移仓价差。")
            roll_spot_qty = snap_to_increment(
                entry.spot_qty if derivative_qty >= entry.derivative_qty else entry.spot_qty * derivative_qty / max(entry.derivative_qty, Decimal("1e-18")),
                self.manager.get_instrument(entry.spot_inst_id).lot_size,
                "down",
            )
            spread_abs = target_sell - current_buy
            planned_batches = _split_pair_close_batches(
                derivative_qty,
                derivative_instrument=current_instrument,
                batch_count=self._parse_roll_batch_count(),
                batch_qty=self._parse_roll_batch_qty(),
            )
        except (InvalidOperation, ValueError, Exception) as exc:
            self.roll_preview_text.set(f"预览失败：{exc}")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.roll_preview_text.set(
            "\n".join(
                [
                    f"本次移仓：回补 {entry.derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"并开出 {target_derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"对应继续占用现货：{format_decimal(roll_spot_qty)}",
                    f"当前移仓价差：{format_decimal(spread_abs)}",
                    f"分批计划：{len(planned_batches)} 批 | {', '.join(format_decimal(item) for item in planned_batches)}",
                    f"执行方式：{self.roll_execution_mode_label.get().strip()} | 最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _runtime_or_warn(self) -> ArbitrageTradeRuntime | None:
        fallback_runtime = self._runtime_config_provider() if self._runtime_config_provider is not None else None
        profile_name = self._selected_api_profile_name()
        profile_snapshot = load_credentials_snapshot(profile_name=profile_name)
        runtime = _build_runtime_for_profile(
            profile_name,
            profile_snapshot=profile_snapshot,
            fallback_runtime=fallback_runtime,
        )
        if runtime is None:
            messagebox.showwarning("提示", f"请先为 API 配置 {profile_name} 补全并保存 API 凭证。", parent=self.window)
            return None
        return runtime

    def _parse_optional_decimal(self, text: str) -> Decimal | None:
        normalized = text.strip()
        if not normalized:
            return None
        return Decimal(normalized)

    def _parse_max_slippage(self) -> Decimal:
        return Decimal(self.max_slippage_percent.get().strip() or "0.15") / Decimal("100")

    def _parse_chart_candle_limit(self) -> int:
        limit = int(self.chart_candle_limit.get().strip() or "240")
        if limit <= 0:
            raise ValueError("K线根数必须大于 0。")
        return min(limit, 1000)

    def _build_open_request(self) -> ArbitrageOpenRequest:
        base = self.base_ccy.get().strip().upper()
        if not base:
            raise ValueError("请填写币种。")
        derivative_inst_id = self._derivative_inst_id.get().strip().upper()
        if not derivative_inst_id:
            derivative_inst_id = f"{base}-USDT-SWAP"
        trigger_mode = _TRIGGER_MODE_OPTIONS.get(self.trigger_mode_label.get().strip(), "spread")
        open_spread_pct = None
        open_spread_abs = None
        if trigger_mode == "spread_abs":
            spread_text = self.open_spread_abs_max.get().strip()
            open_spread_abs = Decimal(spread_text) if spread_text else None
        elif trigger_mode == "spread":
            spread_text = self.open_spread_pct_max.get().strip()
            open_spread_pct = Decimal(spread_text) if spread_text else None
        return ArbitrageOpenRequest(
            base_ccy=base,
            spot_inst_id=f"{base}-USDT",
            derivative_inst_id=derivative_inst_id,
            size=self._parse_size(),
            size_unit=self._parse_size_unit(),  # type: ignore[arg-type]
            trigger_mode=trigger_mode,
            open_spread_pct_max=open_spread_pct,
            open_spread_abs_max=open_spread_abs,
            spot_limit_price=self._parse_optional_decimal(self.spot_limit_price.get()),
            derivative_limit_price=self._parse_optional_decimal(self.derivative_limit_price.get()),
            use_limit_orders=self.use_limit_orders.get(),
            max_slippage=self._parse_max_slippage(),
            batch_count=self._parse_open_batch_count(),
            batch_contract_qty=self._parse_open_batch_qty(),
            execution_mode=self._current_open_execution_mode(),
            maker_wait_seconds=self._parse_open_maker_wait_seconds(),
            chase_limit=self._parse_open_chase_limit(),
        )

    def _parse_close_trigger_threshold(self) -> tuple[str, Decimal | None, Decimal | None]:
        close_mode = self._current_close_trigger_mode()
        if close_mode == "spread_abs":
            return close_mode, None, Decimal(self.close_spread_abs_min.get().strip() or "0")
        return close_mode, Decimal(self.close_spread_pct_min.get().strip() or "0"), None

    def _schedule_monitor_refresh(self) -> None:
        if self._destroying or not self.window.winfo_exists():
            return
        session = self.manager.auto_open.session
        if session is not None:
            parts = [session.status]
            if session.last_spread_pct is not None:
                parts.append(f"价差率 {format_decimal_fixed(session.last_spread_pct, 4)}%")
            if getattr(session, "last_spread_abs", None) is not None:
                parts.append(f"绝对价差 {format_decimal(getattr(session, 'last_spread_abs'))}")
            self.monitor_status_text.set(" | ".join(parts))
            self.trade_status_text.set(session.status)
            if session.result is not None:
                self._reload_ledger()
        else:
            close_session = self.manager.auto_close.session
            if close_session is not None:
                parts = [close_session.status]
                if close_session.last_spread_pct is not None:
                    parts.append(f"价差率 {format_decimal_fixed(close_session.last_spread_pct, 4)}%")
                if getattr(close_session, "last_spread_abs", None) is not None:
                    parts.append(f"绝对价差 {format_decimal(getattr(close_session, 'last_spread_abs'))}")
                self.monitor_status_text.set(" | ".join(parts))
                self.trade_status_text.set(close_session.status)
                if close_session.result is not None:
                    self._reload_ledger()
            elif not self.manager.auto_open.is_running and not self.manager.auto_close.is_running:
                self.monitor_status_text.set("未监控")
        self._monitor_job = self.window.after(MONITOR_UI_REFRESH_MS, self._schedule_monitor_refresh)

    def _start_auto_open(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        try:
            request = self._build_open_request()
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        if request.trigger_mode == "spread" and request.open_spread_pct_max is None:
            messagebox.showwarning("参数错误", "价差率触发需要填写「开仓价差率」。", parent=self.window)
            return
        if request.trigger_mode == "spread_abs" and request.open_spread_abs_max is None:
            messagebox.showwarning("参数错误", "绝对价差触发需要填写「开仓绝对价差」。", parent=self.window)
            return
        if request.trigger_mode == "limit_price" and (
            request.spot_limit_price is None and request.derivative_limit_price is None
        ):
            messagebox.showwarning("参数错误", "限价触发至少填写一侧限价。", parent=self.window)
            return
        if self.manager.auto_open.is_running:
            messagebox.showwarning("提示", "已有监控任务在运行，请先停止。", parent=self.window)
            return
        if self.manager.auto_close.is_running:
            messagebox.showwarning("提示", "自动平仓监控运行中，请先停止。", parent=self.window)
            return
        if self._is_pair_close_auto_running():
            messagebox.showwarning("提示", "自动配对平仓监控运行中，请先停止。", parent=self.window)
            return
        try:
            self.manager.start_auto_open(request, runtime=runtime)
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return
        self.trade_status_text.set("自动开仓监控已启动。")
        self._append_log("已启动自动开仓监控。")

    def _stop_auto_open(self) -> None:
        self.manager.stop_auto_open()
        self.manager.stop_auto_close()
        self._stop_pair_close_auto(silent=True)
        self.trade_status_text.set("监控已停止。")
        self.monitor_status_text.set("已停止")
        self._append_log("已停止套利监控。")

    def _open_now(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        if not messagebox.askyesno(
            "确认立即开仓",
            "将按当前价格与数量立即执行：买现货 → 空合约。\n确认继续？",
            parent=self.window,
        ):
            return
        try:
            request = self._build_open_request()
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return

        self.trade_status_text.set("正在开仓…")
        self._append_log("立即开仓：提交中…")

        def _worker() -> None:
            result = self.manager.open_now(request, runtime=runtime)

            def _apply() -> None:
                self.trade_status_text.set(result.message)
                self._append_log(result.message)
                if result.success:
                    self._reload_ledger()
                    messagebox.showinfo("开仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("开仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-open-now", daemon=True).start()

    def _build_close_request(
        self,
        *,
        entry_id: str | None = None,
        close_derivative_qty: Decimal | None = None,
    ) -> ArbitrageCloseRequest:
        return ArbitrageCloseRequest(
            entry_id=entry_id,
            max_slippage=self._parse_max_slippage(),
            use_limit_orders=self.use_limit_orders.get(),
            spot_limit_price=self._parse_optional_decimal(self.spot_limit_price.get()),
            derivative_limit_price=self._parse_optional_decimal(self.derivative_limit_price.get()),
            close_derivative_qty=close_derivative_qty,
            batch_count=self._parse_close_batch_count(),
            batch_contract_qty=self._parse_close_batch_qty(),
            execution_mode=self._current_close_execution_mode(),
            maker_wait_seconds=self._parse_close_maker_wait_seconds(),
            chase_limit=self._parse_close_chase_limit(),
        )

    def _build_roll_request(self, *, entry_id: str, roll_derivative_qty: Decimal) -> ArbitrageRollRequest:
        return ArbitrageRollRequest(
            entry_id=entry_id,
            target_derivative_inst_id=self.roll_target_derivative_inst_id.get().strip().upper(),
            max_slippage=self._parse_max_slippage(),
            use_limit_orders=self.use_limit_orders.get(),
            roll_derivative_qty=roll_derivative_qty,
            current_derivative_limit_price=self._parse_optional_decimal(self.roll_current_limit_price.get()),
            target_derivative_limit_price=self._parse_optional_decimal(self.roll_target_limit_price.get()),
            batch_count=self._parse_roll_batch_count(),
            batch_contract_qty=self._parse_roll_batch_qty(),
            execution_mode=self._current_roll_execution_mode(),
            maker_wait_seconds=self._parse_roll_maker_wait_seconds(),
            chase_limit=self._parse_roll_chase_limit(),
        )

    def _submit_roll(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        entry = self._selected_roll_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先选择一条未平仓交割合约持仓。", parent=self.window)
            return
        try:
            roll_qty = self._parse_roll_derivative_qty(entry)
            request = self._build_roll_request(entry_id=entry.entry_id, roll_derivative_qty=roll_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        if not request.target_derivative_inst_id:
            messagebox.showwarning("提示", "请先填写更远交割合约。", parent=self.window)
            return
        if not messagebox.askyesno(
            "确认移仓",
            (
                f"将回补 {entry.derivative_inst_id} {format_decimal(roll_qty)} 张，\n"
                f"并开出 {request.target_derivative_inst_id} 对应数量。\n确认继续？"
            ),
            parent=self.window,
        ):
            return

        self.roll_status_text.set("正在执行移仓…")
        self._append_log("交割合约移仓：提交中…")

        def _worker() -> None:
            result = self.manager.roll_now(request, runtime=runtime)

            def _apply() -> None:
                self.roll_status_text.set(result.message)
                self._append_log(result.message)
                self._reload_ledger()
                if result.success:
                    messagebox.showinfo("移仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("移仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-roll", daemon=True).start()

    def _refresh_roll_preview(self) -> None:
        selected = self._selected_roll_positions()
        entry = self._selected_roll_entry()
        if selected is None or entry is None:
            self.roll_position_summary_text.set("请先刷新并选择一条当前交割合约持仓。")
            self.roll_preview_text.set("请选择当前交割合约持仓，并填写更远交割合约。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        spot_position, derivative_position = selected
        spot_instrument = self._roll_instruments.get(spot_position.inst_id)
        derivative_instrument = self._roll_instruments.get(derivative_position.inst_id)
        if spot_instrument is None or derivative_instrument is None:
            self.roll_preview_text.set("当前缺少持仓对应的合约元数据，请先刷新当前持仓。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        source_entry = self._current_roll_source_entry(spot_position, derivative_position)
        self.roll_position_summary_text.set(
            "\n".join(
                [
                    f"现货持仓：{_pair_position_label(spot_position, spot_instrument)}",
                    f"当前交割：{_pair_position_label(derivative_position, derivative_instrument)}",
                    ("账本关联：已关联当前账本选中记录" if source_entry is not None else "账本关联：未绑定，将按现有持仓直接移仓"),
                ]
            )
        )
        target_derivative_inst_id = self.roll_target_derivative_inst_id.get().strip().upper()
        if not target_derivative_inst_id:
            self.roll_preview_text.set("请先填写更远交割合约。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        try:
            derivative_qty = self._parse_roll_derivative_qty(entry)
            target_instrument = self.manager.get_instrument(target_derivative_inst_id)
            if target_instrument.inst_type != "FUTURES":
                raise ValueError("当前移仓只支持更远交割合约作为目标。")
            if _pair_position_base_ccy(derivative_position) != target_derivative_inst_id.split("-")[0].strip().upper():
                raise ValueError("目标交割合约的币种必须和当前持仓一致。")
            current_ticker = self.client.get_ticker(entry.derivative_inst_id)
            target_ticker = self.client.get_ticker(target_derivative_inst_id)
            current_buy = current_ticker.ask or current_ticker.last
            target_sell = target_ticker.bid or target_ticker.last
            if current_buy is None or target_sell is None:
                raise ValueError("当前缺少有效盘口，无法预估移仓价差。")
            roll_spot_qty = self._estimate_roll_spot_qty(
                spot_position=spot_position,
                derivative_position=derivative_position,
                spot_instrument=spot_instrument,
                derivative_instrument=derivative_instrument,
                derivative_qty=derivative_qty,
                source_entry=source_entry,
            )
            spread_abs = target_sell - current_buy
            planned_batches = _split_pair_close_batches(
                derivative_qty,
                derivative_instrument=derivative_instrument,
                batch_count=self._parse_roll_batch_count(),
                batch_qty=self._parse_roll_batch_qty(),
            )
        except (InvalidOperation, ValueError, Exception) as exc:
            self.roll_preview_text.set(f"预览失败：{exc}")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.roll_preview_text.set(
            "\n".join(
                [
                    f"本次移仓：回补 {entry.derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"并开出 {target_derivative_inst_id} {format_decimal(derivative_qty)} 张",
                    f"对应继续占用现货：{format_decimal(roll_spot_qty)} {entry.base_ccy}",
                    f"当前移仓绝对价差：{format_decimal(spread_abs)}",
                    f"分批计划：{len(planned_batches)} 批 | {', '.join(format_decimal(item) for item in planned_batches)}",
                    f"执行方式：{self.roll_execution_mode_label.get().strip()} | 最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _build_roll_request(self, *, entry, roll_derivative_qty: Decimal) -> ArbitrageRollRequest:
        selected = self._selected_roll_positions()
        if selected is None:
            raise ValueError("请先刷新并选择一条当前交割合约持仓。")
        spot_position, derivative_position = selected
        return ArbitrageRollRequest(
            entry_id=(entry.entry_id or None),
            target_derivative_inst_id=self.roll_target_derivative_inst_id.get().strip().upper(),
            max_slippage=self._parse_max_slippage(),
            use_limit_orders=self.use_limit_orders.get(),
            roll_derivative_qty=roll_derivative_qty,
            current_derivative_limit_price=self._parse_optional_decimal(self.roll_current_limit_price.get()),
            target_derivative_limit_price=self._parse_optional_decimal(self.roll_target_limit_price.get()),
            batch_count=self._parse_roll_batch_count(),
            batch_contract_qty=self._parse_roll_batch_qty(),
            execution_mode=self._current_roll_execution_mode(),
            maker_wait_seconds=self._parse_roll_maker_wait_seconds(),
            chase_limit=self._parse_roll_chase_limit(),
            base_ccy=entry.base_ccy,
            spot_inst_id=spot_position.inst_id,
            current_derivative_inst_id=derivative_position.inst_id,
            spot_qty=entry.spot_qty,
            current_derivative_qty=entry.derivative_qty,
        )

    def _submit_roll(self) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        entry = self._selected_roll_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先刷新并选择一条当前交割合约持仓。", parent=self.window)
            return
        try:
            roll_qty = self._parse_roll_derivative_qty(entry)
            request = self._build_roll_request(entry=entry, roll_derivative_qty=roll_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        if not request.target_derivative_inst_id:
            messagebox.showwarning("提示", "请先填写更远交割合约。", parent=self.window)
            return
        if not messagebox.askyesno(
            "确认移仓",
            (
                f"将回补 {entry.derivative_inst_id} {format_decimal(roll_qty)} 张，\n"
                f"并开出 {request.target_derivative_inst_id} 对应数量。\n确认继续？"
            ),
            parent=self.window,
        ):
            return

        self.roll_status_text.set("正在执行移仓…")
        self._append_log("交割合约移仓：提交中…")

        def _worker() -> None:
            result = self.manager.roll_now(request, runtime=runtime)

            def _apply() -> None:
                self.roll_status_text.set(result.message)
                self._append_log(result.message)
                self._reload_ledger()
                self._refresh_roll_positions()
                if result.success:
                    messagebox.showinfo("移仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("移仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-roll", daemon=True).start()

    def _start_auto_close(self, *, entry_id: str | None = None) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        open_entries = self.manager.load_open_ledger()
        if not open_entries:
            messagebox.showwarning("提示", "当前没有未平仓的套利持仓。", parent=self.window)
            return
        try:
            close_mode, close_spread_pct, close_spread_abs = self._parse_close_trigger_threshold()
        except InvalidOperation:
            messagebox.showwarning("参数错误", "平仓触发阈值无效。", parent=self.window)
            return
        selected_entry_id = entry_id or self._selected_ledger_entry_id()
        if self.manager.auto_open.is_running:
            messagebox.showwarning("提示", "已有监控任务在运行，请先停止。", parent=self.window)
            return
        if self.manager.auto_close.is_running:
            messagebox.showwarning("提示", "自动平仓监控运行中，请先停止。", parent=self.window)
            return
        if self._is_pair_close_auto_running():
            messagebox.showwarning("提示", "自动配对平仓监控运行中，请先停止。", parent=self.window)
            return
        try:
            request = self._build_close_request(entry_id=selected_entry_id)
            self.manager.start_auto_close(
                request=request,
                runtime=runtime,
                close_trigger_mode=close_mode,
                close_spread_pct_min=close_spread_pct,
                close_spread_abs_min=close_spread_abs,
                entry_id=selected_entry_id,
            )
        except Exception as exc:
            messagebox.showerror("启动失败", str(exc), parent=self.window)
            return
        self.trade_status_text.set("自动平仓监控已启动。")
        self._append_log("已启动自动平仓监控。")

    def _start_auto_close_from_tab(self) -> None:
        entry = self._selected_close_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先选择一条未平仓套利持仓。", parent=self.window)
            return
        self._start_auto_close(entry_id=entry.entry_id)

    def _close_all(self) -> None:
        self._close_entries(
            entry_id=None,
            label="全部平仓",
            confirm_message="将执行全部平仓：平合约 → 卖现货。\n确认继续？",
        )

    def _close_selected(self) -> None:
        entry_id = self._selected_ledger_entry_id()
        if not entry_id:
            messagebox.showwarning("提示", "请先在账本中选择一条未平仓记录。", parent=self.window)
            return
        self._close_entries(
            entry_id=entry_id,
            label="平仓选中",
            confirm_message="将执行平仓选中：平合约 → 卖现货。\n确认继续？",
        )

    def _close_selected_from_tab(self) -> None:
        entry = self._selected_close_entry()
        if entry is None:
            messagebox.showwarning("提示", "请先选择一条未平仓套利持仓。", parent=self.window)
            return
        try:
            close_derivative_qty = self._parse_close_derivative_qty(entry)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        close_kind = "全部平仓" if close_derivative_qty >= entry.derivative_qty else "部分平仓"
        self._close_entries(
            entry_id=entry.entry_id,
            label=close_kind,
            close_derivative_qty=close_derivative_qty,
            confirm_message=(
                f"将对 {entry.base_ccy} 执行{close_kind}：\n"
                f"合约买入 {format_decimal(close_derivative_qty)} 张 → 卖出现货。\n"
                "确认继续？"
            ),
        )

    def _selected_ledger_entry_id(self) -> str | None:
        entry = self._selected_ledger_entry()
        return None if entry is None else entry.entry_id

    def _close_entries(
        self,
        *,
        entry_id: str | None,
        label: str,
        close_derivative_qty: Decimal | None = None,
        confirm_message: str | None = None,
    ) -> None:
        runtime = self._runtime_or_warn()
        if runtime is None:
            return
        prompt = confirm_message or f"将执行{label}：平合约 → 卖现货。\n确认继续？"
        if not messagebox.askyesno("确认平仓", prompt, parent=self.window):
            return
        try:
            request = self._build_close_request(entry_id=entry_id, close_derivative_qty=close_derivative_qty)
        except (InvalidOperation, ValueError) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        self.trade_status_text.set("正在平仓…")
        self._append_log(f"{label}：提交中…")

        def _worker() -> None:
            result = self.manager.close_now(request, runtime=runtime)

            def _apply() -> None:
                self.trade_status_text.set(result.message)
                self._append_log(result.message)
                self._reload_ledger()
                if result.success:
                    messagebox.showinfo("平仓完成", result.message, parent=self.window)
                else:
                    messagebox.showerror("平仓失败", result.message, parent=self.window)

            try:
                self.window.after(0, _apply)
            except Exception:
                pass

        threading.Thread(target=_worker, name="arbitrage-close", daemon=True).start()

    def _parse_size(self) -> Decimal:
        return Decimal(self.size_value.get().strip())

    def _parse_size_unit(self) -> str:
        label = self.size_unit_label.get().strip()
        return _SIZE_UNIT_OPTIONS.get(label, "usdt")

    def _refresh_preview(self, *, derivative_inst_id: str | None = None) -> None:
        base = self.base_ccy.get().strip().upper()
        if not base:
            self.preview_text.set("请先填写币种。")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        if derivative_inst_id is None:
            derivative_inst_id = self._derivative_inst_id.get().strip().upper()
            if not derivative_inst_id:
                if self._selected_opportunity is not None and self._selected_opportunity.base_ccy == base:
                    derivative_inst_id = self._selected_opportunity.derivative_inst_id
                else:
                    derivative_inst_id = f"{base}-USDT-SWAP"
        try:
            preview = self.manager.preview_size(
                base_ccy=base,
                derivative_inst_id=derivative_inst_id,
                size=self._parse_size(),
                unit=self._parse_size_unit(),  # type: ignore[arg-type]
            )
        except Exception as exc:
            self.preview_text.set(f"预览失败：{exc}")
            self._schedule_market_panel_refresh(initial_delay_ms=50)
            return
        self.preview_text.set(
            "\n".join(
                [
                    f"现货 {base}-USDT：{format_decimal(preview.spot_base_qty)}",
                    f"衍生品 {derivative_inst_id}：{format_decimal(preview.swap_contracts)} 张",
                    f"名义价值约：{format_decimal_fixed(preview.notional_usdt, 2)} USDT",
                    f"最大滑点：{self.max_slippage_percent.get().strip()}%",
                ]
            )
        )
        self._schedule_market_panel_refresh(initial_delay_ms=50)

    def _load_arbitrage_charts(self) -> None:
        spot_inst_id = self.chart_spot_inst_id.get().strip().upper()
        derivative_inst_id = self.chart_derivative_inst_id.get().strip().upper()
        if not spot_inst_id or not derivative_inst_id:
            base = self.base_ccy.get().strip().upper()
            if base and not spot_inst_id:
                spot_inst_id = f"{base}-USDT"
                self.chart_spot_inst_id.set(spot_inst_id)
            if not derivative_inst_id:
                derivative_inst_id = self._derivative_inst_id.get().strip().upper()
                if derivative_inst_id:
                    self.chart_derivative_inst_id.set(derivative_inst_id)
        if not spot_inst_id or not derivative_inst_id:
            messagebox.showwarning("提示", "请先填写现货和衍生品合约，或从扫描结果带入。", parent=self.window)
            return
        try:
            limit = self._parse_chart_candle_limit()
        except (ValueError, InvalidOperation) as exc:
            messagebox.showwarning("参数错误", str(exc), parent=self.window)
            return
        bar = self.chart_bar.get().strip() or "15m"
        self._chart_load_token += 1
        token = self._chart_load_token
        self.chart_status_text.set(f"正在加载 {spot_inst_id} / {derivative_inst_id} | 周期 {bar} | {limit} 根...")
        self.spot_chart_status_text.set("现货 K 线加载中…")
        self.derivative_chart_status_text.set("衍生品 K 线加载中…")
        self.spread_chart_status_text.set("价差 K 线加载中…")

        def _worker() -> None:
            try:
                spot_candles = self.client.get_candles_history(spot_inst_id, bar, limit=limit)
                derivative_candles = self.client.get_candles_history(derivative_inst_id, bar, limit=limit)
                spread_candles = _build_spread_candles(spot_candles, derivative_candles)
            except Exception as exc:
                if self._destroying:
                    return
                self.window.after(0, lambda e=str(exc): self._apply_arbitrage_chart_error(token, e))
                return
            if self._destroying:
                return
            self.window.after(
                0,
                lambda: self._apply_arbitrage_charts(
                    token,
                    spot_inst_id,
                    derivative_inst_id,
                    bar,
                    spot_candles,
                    derivative_candles,
                    spread_candles,
                ),
            )

        threading.Thread(target=_worker, name="arbitrage-charts", daemon=True).start()

    def _apply_arbitrage_charts(
        self,
        token: int,
        spot_inst_id: str,
        derivative_inst_id: str,
        bar: str,
        spot_candles: list[Candle],
        derivative_candles: list[Candle],
        spread_candles: list[Candle],
    ) -> None:
        if token != self._chart_load_token:
            return
        spot_visible = [item for item in spot_candles if item.confirmed] or list(spot_candles)
        derivative_visible = [item for item in derivative_candles if item.confirmed] or list(derivative_candles)
        spread_visible = [item for item in spread_candles if item.confirmed] or list(spread_candles)
        self._spot_chart_snapshot = build_strategy_live_chart_snapshot(
            session_id=f"arb-spot-{spot_inst_id}",
            candles=spot_visible,
            ema_period=5,
            trend_ema_period=10,
            reference_ema_period=20,
            latest_price=spot_visible[-1].close if spot_visible else None,
            note=f"{spot_inst_id} | {bar}",
        )
        self._derivative_chart_snapshot = build_strategy_live_chart_snapshot(
            session_id=f"arb-deriv-{derivative_inst_id}",
            candles=derivative_visible,
            ema_period=5,
            trend_ema_period=10,
            reference_ema_period=20,
            latest_price=derivative_visible[-1].close if derivative_visible else None,
            note=f"{derivative_inst_id} | {bar}",
        )
        self._spread_chart_snapshot = build_strategy_live_chart_snapshot(
            session_id=f"arb-spread-{spot_inst_id}-{derivative_inst_id}",
            candles=spread_visible,
            ema_period=5,
            trend_ema_period=10,
            reference_ema_period=20,
            latest_price=spread_visible[-1].close if spread_visible else None,
            note="价差口径：衍生品价格 - 现货价格",
        )
        self.chart_status_text.set(
            f"{spot_inst_id} vs {derivative_inst_id} | 周期 {bar} | 现货 {len(spot_visible)} 根 | 衍生品 {len(derivative_visible)} 根 | 价差 {len(spread_visible)} 根"
        )
        self.spot_chart_status_text.set(
            "现货最新 "
            + (format_decimal(spot_visible[-1].close) if spot_visible else "-")
            + f" | {spot_inst_id}"
        )
        self.derivative_chart_status_text.set(
            "衍生品最新 "
            + (format_decimal(derivative_visible[-1].close) if derivative_visible else "-")
            + f" | {derivative_inst_id}"
        )
        self.spread_chart_status_text.set(
            "最新价差 "
            + (format_decimal(spread_visible[-1].close) if spread_visible else "-")
        )
        self._render_arbitrage_chart_canvases()
        self._append_log(f"已加载套利图表：{spot_inst_id} / {derivative_inst_id} | {bar}")

    def _apply_arbitrage_chart_error(self, token: int, message: str) -> None:
        if token != self._chart_load_token:
            return
        self.chart_status_text.set(f"套利图表加载失败：{message}")
        self.spot_chart_status_text.set("现货图加载失败")
        self.derivative_chart_status_text.set("衍生品图加载失败")
        self.spread_chart_status_text.set("价差图加载失败")

    def _render_arbitrage_chart_canvases(self) -> None:
        if getattr(self, "spot_chart_canvas", None) is not None and self._spot_chart_snapshot is not None:
            render_strategy_live_chart(self.spot_chart_canvas, self._spot_chart_snapshot)
        if getattr(self, "derivative_chart_canvas", None) is not None and self._derivative_chart_snapshot is not None:
            render_strategy_live_chart(self.derivative_chart_canvas, self._derivative_chart_snapshot)
        if getattr(self, "spread_chart_canvas", None) is not None and self._spread_chart_snapshot is not None:
            render_strategy_live_chart(self.spread_chart_canvas, self._spread_chart_snapshot)

    def _on_arbitrage_chart_canvas_configure(self, _event=None) -> None:
        if self._destroying:
            return
        try:
            self.window.after(0, self._render_arbitrage_chart_canvases)
        except Exception:
            pass

    def _reload_ledger(self) -> None:
        self.ledger_tree.delete(*self.ledger_tree.get_children())
        entries = self.manager.load_ledger()
        self._ledger_entries = entries
        self._ledger_entry_by_id = {item.entry_id: item for item in entries}
        if not entries:
            self.ledger_tree.insert("", END, iid="placeholder", values=("—", "暂无套利账本记录", "", "", "", "", "", "", "", ""))
            self._refresh_close_entry_options()
            self._refresh_roll_entry_options()
            return
        for item in entries:
            self.ledger_tree.insert(
                item.entry_id,
                END,
                values=(
                    item.base_ccy,
                    item.pair_kind,
                    format_decimal(item.spot_qty),
                    format_decimal(item.derivative_qty),
                    "-" if item.basis_at_open_pct is None else format_decimal_fixed(item.basis_at_open_pct, 4),
                    format_decimal_fixed(item.fee_total, 4),
                    format_decimal_fixed(item.funding_total, 4),
                    "-" if item.realized_pnl is None else format_decimal_fixed(item.realized_pnl, 4),
                    item.close_mode,
                    item.opened_at,
                ),
            )
        self._refresh_close_entry_options()
        self._refresh_roll_entry_options()
