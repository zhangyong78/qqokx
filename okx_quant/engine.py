from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal, TypeVar

from okx_quant.indicators import atr, ema
from okx_quant.models import Credentials, Instrument, OrderPlan, ProtectionPlan, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import (
    OkxApiError,
    OkxOrderResult,
    OkxOrderStatus,
    OkxPosition,
    OkxRestClient,
    OkxTradeOrderItem,
    infer_inst_type,
)
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.engine_order_service import EngineOrderService
from okx_quant.engine_retry_policy import EngineRetryPolicy
from okx_quant.engine_session_runner import EngineSessionRunner
from okx_quant.engine_strategy_router import EngineStrategyRouter
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import (
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    is_dynamic_strategy_id,
    is_ema_atr_breakout_strategy,
    resolve_dynamic_signal_mode,
)


Logger = Callable[[str], None]
OKX_SINGLE_REQUEST_MAX_CANDLES = 300


def _live_dynamic_take_profit_enabled(config: StrategyConfig) -> bool:
    """与回测一致：EMA 动态委托与 EMA 突破/跌破在 take_profit_mode=dynamic 时启用动态止盈逻辑。"""
    if str(config.take_profit_mode or "") != "dynamic":
        return False
    return is_dynamic_strategy_id(config.strategy_id) or is_ema_atr_breakout_strategy(config.strategy_id)


def live_exchange_dynamic_take_profit_template_enabled(config: StrategyConfig) -> bool:
    """主界面模板是否属于「交易所托管 + 动态止盈上移」口径（供持仓接管等入口复用）。"""
    return _live_dynamic_take_profit_enabled(config) and not bool(config.trader_virtual_stop_loss)


def _take_profit_mode_description_for_signal_email(config: StrategyConfig) -> str:
    """仅发信号邮件中附带的止盈方式说明（与当前模板配置一致）。"""
    sid = str(config.strategy_id or "")
    if sid == STRATEGY_EMA5_EMA8_ID:
        return (
            "止盈止损说明：本策略以快慢线交叉为信号、慢线 EMA 为止损参考；"
            "与 ATR 固定/动态止盈倍数无关。"
        )
    if _live_dynamic_take_profit_enabled(config):
        return (
            "止盈方式：动态止盈（若在「交易」模式下按当前模板下单，将按此规则管理出场）| "
            f"2R保本={config.dynamic_two_r_break_even_label()} | "
            f"手续费偏移={config.dynamic_fee_offset_enabled_label()} | "
            f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
        )
    return (
        "止盈方式：固定止盈 | "
        f"ATR 止盈倍数×{format_decimal(config.atr_take_multiplier)} | "
        f"ATR 止损倍数×{format_decimal(config.atr_stop_multiplier)}"
    )
DEFAULT_DEBUG_ATR_PERIOD = 10
LIVE_DYNAMIC_MAKER_FEE_RATE = Decimal("0.00015")
LIVE_DYNAMIC_TAKER_FEE_RATE = Decimal("0.00036")


def _qqokx_env_int(name: str, default: int, *, min_value: int, max_value: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw, 10)
    except ValueError:
        return default
    return max(min_value, min(max_value, parsed))


def _qqokx_env_float(name: str, default: float, *, min_value: float, max_value: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    return max(min_value, min(max_value, parsed))


def get_okx_read_retry_config() -> tuple[int, float, float]:
    """OKX 读取重试：每次调用读取 os.environ，便于测试与部署后调参（弱网/VPN 可多试几次）。"""
    base = _qqokx_env_float(
        "QQOKX_READ_RETRY_BASE_DELAY_SECONDS",
        1.0,
        min_value=0.1,
        max_value=60.0,
    )
    max_delay = _qqokx_env_float(
        "QQOKX_READ_RETRY_MAX_DELAY_SECONDS",
        8.0,
        min_value=max(0.5, base),
        max_value=120.0,
    )
    attempts = _qqokx_env_int(
        "QQOKX_READ_RETRY_ATTEMPTS",
        8,
        min_value=1,
        max_value=30,
    )
    return attempts, base, max_delay


OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES = 6
OKX_WRITE_RECONCILE_ATTEMPTS = 3
OKX_WRITE_RECONCILE_BASE_DELAY_SECONDS = 1.0
OKX_WRITE_RECONCILE_MAX_DELAY_SECONDS = 3.0
IDLE_SIGNAL_MAX_WAIT_SECONDS = 60.0

T = TypeVar("T")


@dataclass(frozen=True)
class HourlyDebugSnapshot:
    candle_ts: int
    candle_close: Decimal
    ema_value: Decimal
    ema_period: int
    atr_value: Decimal
    atr_period: int
    lookback_used: int
    confirmed_count: int


@dataclass(frozen=True)
class AtrSnapshot:
    candle_ts: int
    candle_close: Decimal
    atr_value: Decimal
    lookback_used: int
    confirmed_count: int


@dataclass(frozen=True)
class ManagedEntryOrder:
    ord_id: str
    cl_ord_id: str | None
    candle_ts: int
    entry_reference: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    stop_loss_algo_cl_ord_id: str | None
    size: Decimal
    side: Literal["buy", "sell"]
    signal: Literal["long", "short"]


@dataclass(frozen=True)
class FilledPosition:
    ord_id: str
    cl_ord_id: str | None
    inst_id: str
    side: Literal["buy", "sell"]
    close_side: Literal["buy", "sell"]
    pos_side: Literal["long", "short"] | None
    size: Decimal
    entry_price: Decimal
    entry_ts: int
    price_delta_multiplier: Decimal = Decimal("1")


@dataclass(frozen=True)
class CancelActiveOrderResult:
    action: Literal["canceled", "pending", "filled", "partially_filled"]
    status: OkxOrderStatus | None = None


@dataclass(frozen=True)
class LocalSignalTrigger:
    signal: Literal["long", "short"]
    entry_reference: Decimal
    atr_value: Decimal
    candle_ts: int
    signal_candle_high: Decimal | None
    signal_candle_low: Decimal | None


@dataclass(frozen=True)
class DynamicStopMonitorStepResult:
    keep_monitoring: bool
    current_stop_loss: Decimal
    next_trigger_r: int
    amend_failures: int


class OrderSizeTooSmallError(RuntimeError):
    pass


@dataclass
class StartupSignalGateState:
    started_at_ms: int
    chase_window_seconds: int
    blocked_signal: Literal["long", "short"] | None = None


class StrategyEngine:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        notifier: EmailNotifier | None = None,
        strategy_name: str = "Strategy",
        session_id: str = "",
        direction_label: str = "",
        run_mode_label: str = "",
        trader_id: str = "",
        api_name: str = "",
    ) -> None:
        self._client = client
        self._logger = logger
        self._notifier = notifier
        self._strategy_name = strategy_name
        self._session_id = session_id
        self._direction_label = direction_label.strip()
        self._run_mode_label = run_mode_label.strip()
        self._trader_id = trader_id.strip()
        self._api_name = api_name.strip()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._order_ref_counter = 0
        self._session_runner = EngineSessionRunner(self)
        self._strategy_router = EngineStrategyRouter(self)
        self._retry_policy = EngineRetryPolicy(self)
        self._order_service = EngineOrderService(self)

    @property
    def is_running(self) -> bool:
        return self._session_runner.is_running

    def start(self, credentials: Credentials, config: StrategyConfig) -> None:
        self._session_runner.start(credentials, config)

    def stop(self) -> None:
        self._session_runner.stop()

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._session_runner.wait_stopped(timeout=timeout)

    def _run(self, credentials: Credentials, config: StrategyConfig) -> None:
        self._strategy_router.run(credentials, config)

    def _run_dynamic_exchange_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        if config.signal_mode == "both":
            raise RuntimeError("EMA 动态委托策略不支持双向，请选择只做多或只做空")
        has_risk_amount = config.risk_amount is not None and config.risk_amount > 0
        has_fixed_size = config.order_size > 0
        if not has_risk_amount and not has_fixed_size:
            raise RuntimeError("风险金必须大于 0，或固定数量必须大于 0")

        strategy = EmaDynamicOrderStrategy()
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None
        active_order: ManagedEntryOrder | None = None
        idle_signal_candle_ts: int | None = None
        dynamic_stop_only = config.take_profit_mode == "dynamic"
        trader_virtual_stop_loss_enabled = config.trader_virtual_stop_loss
        current_wave_signal: Literal["long", "short"] | None = None
        entries_in_current_wave = 0
        current_wave_index = 0
        startup_gate = StartupSignalGateState(
            started_at_ms=int(time.time() * 1000),
            chase_window_seconds=config.resolved_startup_chase_window_seconds(),
        )

        self._log_strategy_start(config, instrument, instrument)
        if trader_virtual_stop_loss_enabled:
            self._logger("交易员模式：止损价只做触发参考，不向 OKX 挂真实止损；只有止盈或人工平仓才释放额度。")
            self._logger("运行模式：同标的永续下单，交易员虚拟止损只记触发，不向 OKX 挂真实止损。")
        else:
            self._logger("运行模式：同标的永续下单，止盈止损交给 OKX 托管")
        self._logger(
            f"策略规则：以上一根已收盘 K 线的 {_dynamic_entry_reference_ema_text(config)} 作为开仓价直接挂限价单。"
            f"每根新 K 线确认后，撤掉旧单，再按最新上一根 {_dynamic_entry_reference_ema_text(config)} 重新挂单。"
        )
        mode_parts = [
            f"方向={_format_signal_mode(config.signal_mode)}",
            (
                f"风险金={format_decimal(config.risk_amount)}"
                if has_risk_amount
                else f"固定数量={_format_size_with_contract_equivalent(instrument, config.order_size)}"
            ),
            f"止损ATR倍数={format_decimal(config.atr_stop_multiplier)}",
            f"止盈ATR倍数={format_decimal(config.atr_take_multiplier)}",
            f"每波最多开仓次数={config.max_entries_per_trend or 0}",
            f"启动追单窗口={config.startup_chase_window_label()}",
        ]
        self._logger(" | ".join(mode_parts))
        if dynamic_stop_only and trader_virtual_stop_loss_enabled:
            self._logger(
                f"动态止盈已启用 | 2R保本={config.dynamic_two_r_break_even_label()} | "
                f"手续费偏移={config.dynamic_fee_offset_enabled_label()} | "
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根 | "
                "交易员模式：止损价只做触发参考，不会直接平仓。"
            )
        elif dynamic_stop_only:
            self._logger(
                f"动态止盈已启用 | 2R保本={config.dynamic_two_r_break_even_label()} | "
                f"手续费偏移={config.dynamic_fee_offset_enabled_label()} | "
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根 | "
                "初始仅在 OKX 挂止损"
            )
        self._logger(f"指标回看数量：{lookback} 根 K 线")

        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period,
                config.trend_ema_period,
                config.atr_period,
                entry_reference_ema_period,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts

            if active_order is not None:
                status = self._get_order_with_retry(
                    credentials,
                    config,
                    inst_id=config.inst_id,
                    ord_id=active_order.ord_id,
                )
                state = status.state.lower()
                if state == "filled":
                    filled_price = status.avg_price or status.price or active_order.entry_reference
                    entries_in_current_wave += 1
                    self._manage_filled_dynamic_entry(
                        credentials,
                        config,
                        trade_instrument=instrument,
                        active_order=active_order,
                        status=status,
                        newest_ts=newest_ts,
                        dynamic_stop_only=dynamic_stop_only,
                    )
                    active_order = None
                    idle_signal_candle_ts = None
                    if self._stop_event.is_set():
                        return
                    self._logger("本轮持仓已结束，继续监控下一次信号。")
                    continue
                if state == "partially_filled":
                    filled_price = status.avg_price or status.price or active_order.entry_reference
                    filled_size = status.filled_size or active_order.size
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 挂单部分成交 | ordId={status.ord_id} | "
                        "为避免重复撤单重挂，策略已停止，请手动检查剩余委托。"
                    )
                    self._notify_trade_fill(
                        config,
                        title="开仓委托部分成交",
                        symbol=config.inst_id,
                        side=active_order.side,
                        size=filled_size,
                        size_text=_format_notify_size_with_unit(instrument, filled_size),
                        price=filled_price,
                        tick_size=instrument.tick_size,
                        reason="EMA 动态委托出现部分成交，策略停止等待人工处理",
                    )
                    return
                if state not in {"live"}:
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 检测到挂单状态已变更为 {status.state}，准备重新同步挂单。"
                    )
                    active_order = None
                    idle_signal_candle_ts = None

            candle_changed = newest_ts != last_candle_ts
            if active_order is not None and candle_changed:
                cancel_result = self._cancel_active_order(credentials, config, active_order, newest_ts)
                if cancel_result.action == "pending":
                    self._stop_event.wait(config.poll_seconds)
                    continue
                if cancel_result.action == "filled":
                    status = cancel_result.status
                    if status is None:
                        raise RuntimeError(f"旧挂单成交回查缺少订单状态，ordId={active_order.ord_id}")
                    entries_in_current_wave += 1
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 旧挂单在撤单前已成交，转入持仓监控 | ordId={status.ord_id or active_order.ord_id}"
                    )
                    self._manage_filled_dynamic_entry(
                        credentials,
                        config,
                        trade_instrument=instrument,
                        active_order=active_order,
                        status=status,
                        newest_ts=newest_ts,
                        dynamic_stop_only=dynamic_stop_only,
                    )
                    active_order = None
                    idle_signal_candle_ts = None
                    if self._stop_event.is_set():
                        return
                    self._logger("本轮持仓已结束，继续监控下一次信号。")
                    continue
                if cancel_result.action == "partially_filled":
                    status = cancel_result.status
                    if status is None:
                        raise RuntimeError(f"旧挂单部分成交回查缺少订单状态，ordId={active_order.ord_id}")
                    self._log_partial_dynamic_fill_and_stop(
                        active_order, status, newest_ts, config, trade_instrument=instrument
                    )
                    return
                active_order = None

            if active_order is None and idle_signal_candle_ts == newest_ts:
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue

            should_place_order = candle_changed or active_order is None
            if not should_place_order:
                self._stop_event.wait(config.poll_seconds)
                continue

            decision = strategy.evaluate(confirmed, config, price_increment=instrument.tick_size)
            last_candle_ts = newest_ts
            if decision.signal is None:
                current_wave_signal = None
                entries_in_current_wave = 0
                _reset_startup_signal_gate(startup_gate)
                idle_signal_candle_ts = newest_ts
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成挂单 | {decision.reason}")
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成挂单计划")

            should_skip_startup_signal, startup_gate_message = _should_skip_startup_signal(
                startup_gate,
                signal=decision.signal,
                candle_ts=decision.candle_ts,
                bar=config.bar,
            )
            if startup_gate_message:
                self._logger(startup_gate_message)
            if should_skip_startup_signal:
                idle_signal_candle_ts = newest_ts
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue

            if current_wave_signal != decision.signal:
                current_wave_signal = decision.signal
                entries_in_current_wave = 0
                current_wave_index += 1
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开始 | 方向={decision.signal.upper()}"
                )

            if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
                idle_signal_candle_ts = newest_ts
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开仓次数已达上限 | "
                    f"方向={decision.signal.upper()} | 上限={config.max_entries_per_trend}"
                )
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue

            try:
                plan = build_order_plan(
                    instrument=instrument,
                    config=config,
                    order_size=config.order_size if config.order_size > 0 else None,
                    signal=decision.signal,
                    entry_reference=decision.entry_reference,
                    atr_value=decision.atr_value,
                    candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
            except OrderSizeTooSmallError as exc:
                idle_signal_candle_ts = newest_ts
                self._logger(f"{_fmt_ts(decision.candle_ts)} | 当前无法生成挂单 | {exc}")
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 准备挂单 | 方向={plan.signal.upper()} | "
                f"第{current_wave_index}波 | 本波第{entries_in_current_wave + 1}次委托 | "
                f"开仓价={format_decimal(plan.entry_reference)} | 数量={_format_size_with_contract_equivalent(instrument, plan.size)} | "
                f"止损={format_decimal(plan.stop_loss)} | "
                f"{'动态止盈=初始不挂止盈' if dynamic_stop_only else f'止盈={format_decimal(plan.take_profit)}'}"
            )
            cl_ord_id = self._next_client_order_id(role="entry")
            stop_loss_algo_cl_ord_id = (
                self._next_client_order_id(role="slg")
                if dynamic_stop_only and not trader_virtual_stop_loss_enabled
                else None
            )
            result = self._submit_order_with_recovery(
                credentials,
                config,
                inst_id=plan.inst_id,
                cl_ord_id=cl_ord_id,
                label="动态限价挂单",
                submit_fn=lambda: self._client.place_limit_order(
                    credentials,
                    config,
                    plan,
                    cl_ord_id=cl_ord_id,
                    include_take_profit=not dynamic_stop_only,
                    stop_loss_algo_cl_ord_id=stop_loss_algo_cl_ord_id,
                    include_attached_protection=not trader_virtual_stop_loss_enabled,
                ),
            )
            if not result.ord_id:
                raise RuntimeError("OKX 未返回挂单 ordId，无法继续监控该委托")
            active_order = ManagedEntryOrder(
                ord_id=result.ord_id,
                cl_ord_id=result.cl_ord_id or cl_ord_id,
                candle_ts=plan.candle_ts,
                entry_reference=plan.entry_reference,
                stop_loss=plan.stop_loss,
                take_profit=plan.take_profit,
                stop_loss_algo_cl_ord_id=stop_loss_algo_cl_ord_id,
                size=plan.size,
                side=plan.side,
                signal=plan.signal,
            )
            idle_signal_candle_ts = None
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 挂单已提交到 OKX | ordId={result.ord_id or '-'} | "
                f"sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            )
            self._logger(f"{_fmt_ts(plan.candle_ts)} | 委托追踪 | clOrdId={active_order.cl_ord_id or '-'}")
            self._stop_event.wait(config.poll_seconds)

    def _run_cross_exchange_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        strategy = EmaAtrStrategy()
        dynamic_stop_only = _live_dynamic_take_profit_enabled(config)
        startup_gate = StartupSignalGateState(
            started_at_ms=int(time.time() * 1000),
            chase_window_seconds=config.resolved_startup_chase_window_seconds(),
        )
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.big_ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None

        self._log_strategy_start(config, instrument, instrument)
        self._logger("运行模式：同标的永续下单，止盈止损交给 OKX 托管")
        self._logger(
            "策略规则：最近一根已收盘 K 线向上突破参考 EMA 做多（须 EMA 小周期>中周期），"
            "向下跌破参考 EMA 做空（须 EMA 小周期<中周期）。"
        )
        if dynamic_stop_only:
            self._logger(
                "止盈方式=动态止盈 | 初始仅挂止损，后续按 2R/3R… 在 OKX 上改价上移；"
                f"2R保本={config.dynamic_two_r_break_even_label()} | "
                f"手续费偏移={config.dynamic_fee_offset_enabled_label()} | "
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        else:
            self._logger(
                f"止盈方式=固定止盈（ATR×{format_decimal(config.atr_take_multiplier)}）| 开仓时一并挂好止盈止损"
            )
        self._logger(
            f"方向={_format_signal_mode(config.signal_mode)} | 风险金={format_decimal(config.risk_amount or Decimal('0'))}"
        )
        self._logger(f"启动追单窗口：{config.startup_chase_window_label()}")
        self._logger(f"指标回看数量：{lookback} 根 K 线")

        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period + 2,
                config.trend_ema_period + 2,
                config.big_ema_period + 2,
                config.atr_period + 2,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config, price_increment=instrument.tick_size)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成下单计划")

            should_skip_startup_signal, startup_gate_message = _should_skip_startup_signal(
                startup_gate,
                signal=decision.signal,
                candle_ts=decision.candle_ts,
                bar=config.bar,
            )
            if startup_gate_message:
                self._logger(startup_gate_message)
            if should_skip_startup_signal:
                self._stop_event.wait(config.poll_seconds)
                continue

            try:
                plan = build_order_plan(
                    instrument=instrument,
                    config=config,
                    order_size=config.order_size,
                    signal=decision.signal,
                    entry_reference=decision.entry_reference,
                    atr_value=decision.atr_value,
                    candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
            except OrderSizeTooSmallError as exc:
                self._logger(f"{_fmt_ts(decision.candle_ts)} | 当前无法生成挂单 | {exc}")
                self._stop_event.wait(config.poll_seconds)
                continue
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 准备市价单 | 方向={plan.signal.upper()} | "
                f"参考入场价={format_decimal(plan.entry_reference)} | 数量={_format_size_with_contract_equivalent(instrument, plan.size)} | "
                f"止损={format_decimal(plan.stop_loss)} | "
                f"{'动态止盈=初始不挂止盈' if dynamic_stop_only else f'止盈={format_decimal(plan.take_profit)}'}"
            )
            cl_ord_id = self._next_client_order_id(role="entry")
            stop_loss_algo_cl_ord_id = self._next_client_order_id(role="slg") if dynamic_stop_only else None
            result = self._submit_order_with_recovery(
                credentials,
                config,
                inst_id=plan.inst_id,
                cl_ord_id=cl_ord_id,
                label="市价附带止盈止损下单",
                submit_fn=lambda: self._client.place_market_order(
                    credentials,
                    config,
                    plan,
                    cl_ord_id=cl_ord_id,
                    include_take_profit=not dynamic_stop_only,
                    stop_loss_algo_cl_ord_id=stop_loss_algo_cl_ord_id,
                ),
            )
            self._logger(
                f"订单已提交到 OKX | ordId={result.ord_id or '-'} | "
                f"sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            )
            self._logger(f"{_fmt_ts(plan.candle_ts)} | 委托追踪 | clOrdId={result.cl_ord_id or cl_ord_id or '-'}")
            filled = self._wait_for_order_fill(
                credentials,
                config,
                trade_instrument=instrument,
                side=plan.side,
                pos_side=plan.pos_side,
                result=result,
                estimated_entry=plan.entry_reference,
            )
            self._logger(
                f"市价单成交 | ordId={filled.ord_id} | 标的={instrument.inst_id} | "
                f"方向={filled.side.upper()} | "
                f"成交均价={_format_notify_price_by_tick_size(filled.entry_price, instrument.tick_size)} | "
                f"成交数量={_format_size_with_contract_equivalent(instrument, filled.size)}"
            )
            fill_reason = (
                "EMA 突破/跌破市价成交：初始止损已交 OKX，后续将按动态止盈规则改价上移"
                if dynamic_stop_only
                else "EMA 突破/跌破市价信号成交，止盈止损已交给 OKX 托管"
            )
            self._notify_trade_fill(
                config,
                title="开仓成交",
                symbol=instrument.inst_id,
                side=filled.side,
                size=filled.size,
                size_text=_format_notify_size_with_unit(instrument, filled.size),
                price=filled.entry_price,
                tick_size=instrument.tick_size,
                reason=fill_reason,
            )
            if dynamic_stop_only and stop_loss_algo_cl_ord_id:
                self._logger(
                    f"初始 OKX 止损已提交 | algoClOrdId={stop_loss_algo_cl_ord_id} | "
                    f"止损={format_decimal(plan.stop_loss)} | 启动动态上移监控"
                )
                self._monitor_exchange_dynamic_stop(
                    credentials,
                    config,
                    trade_instrument=instrument,
                    position=filled,
                    initial_stop_loss=plan.stop_loss,
                    stop_loss_algo_cl_ord_id=stop_loss_algo_cl_ord_id,
                )
                return
            self._logger("止盈止损已交由 OKX 托管，本次策略结束。")
            return

    def _run_cross_local_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        strategy = EmaAtrStrategy()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.big_ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._log_local_mode_summary(config, signal_instrument, trade_instrument)
        self._logger(
            "策略规则：最近一根已收盘 K 线向上突破参考 EMA 做多（须 EMA 小周期>中周期），"
            "向下跌破参考 EMA 做空（须 EMA 小周期<中周期）；信号出现后立即对下单标的开仓。"
        )
        if _live_dynamic_take_profit_enabled(config):
            self._logger(
                "止盈方式=动态止盈 | 本地监控触发价，规则与回测动态止盈一致 | "
                f"2R保本={config.dynamic_two_r_break_even_label()} | "
                f"手续费偏移={config.dynamic_fee_offset_enabled_label()} | "
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        else:
            self._logger(
                f"止盈方式=固定止盈（ATR×{format_decimal(config.atr_take_multiplier)}）| 本地按触发价监控固定止盈止损"
            )
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period + 2,
                config.trend_ema_period + 2,
                config.big_ema_period + 2,
                config.atr_period + 2,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config, price_increment=signal_instrument.tick_size)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            try:
                position = self._open_local_position(
                    credentials,
                    config,
                    signal_instrument=signal_instrument,
                    trade_instrument=trade_instrument,
                    signal=decision.signal,
                    signal_entry_reference=decision.entry_reference,
                    signal_atr_value=decision.atr_value,
                    signal_candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
            except OrderSizeTooSmallError as exc:
                self._logger(f"{_fmt_ts(decision.candle_ts or newest_ts)} | 当前无法下单 | {exc}")
                self._stop_event.wait(config.poll_seconds)
                continue
            protection = self._build_local_protection_plan(
                config,
                signal_instrument=signal_instrument,
                trade_instrument=trade_instrument,
                signal=decision.signal,
                trade_side=position.side,
                estimated_trade_entry=position.entry_price,
                signal_entry_reference=decision.entry_reference,
                signal_atr_value=decision.atr_value,
                signal_candle_ts=decision.candle_ts,
                signal_candle_high=decision.signal_candle_high,
                signal_candle_low=decision.signal_candle_low,
            )
            self._monitor_local_exit(credentials, config, trade_instrument, position, protection)
            return

    def _run_dynamic_local_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        if config.signal_mode == "both":
            raise RuntimeError("EMA 动态委托策略不支持双向，请选择只做多或只做空")

        strategy = EmaDynamicOrderStrategy()
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None
        active_trigger: LocalSignalTrigger | None = None
        idle_signal_candle_ts: int | None = None

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._log_local_mode_summary(config, signal_instrument, trade_instrument)
        self._logger(
            f"策略规则：根据上一根已收盘 K 线的 {_dynamic_entry_reference_ema_text(config)} 生成动态委托价，不再直接往 OKX 挂单，"
            f"而是在本地轮询信号标的价格，触碰 {_dynamic_entry_reference_ema_text(config)} 后立即对下单标的开仓。"
        )
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period,
                config.trend_ema_period,
                config.atr_period,
                entry_reference_ema_period,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if active_trigger is None and idle_signal_candle_ts == newest_ts:
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            if newest_ts != last_candle_ts or active_trigger is None:
                decision = strategy.evaluate(confirmed, config, price_increment=signal_instrument.tick_size)
                last_candle_ts = newest_ts
                if decision.signal is None:
                    active_trigger = None
                    idle_signal_candle_ts = newest_ts
                    self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成动态开仓价 | {decision.reason}")
                    self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                    continue
                if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                    raise RuntimeError("策略返回的数据不完整，无法生成本地触发条件")
                active_trigger = LocalSignalTrigger(
                    signal=decision.signal,
                    entry_reference=decision.entry_reference,
                    atr_value=decision.atr_value,
                    candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
                idle_signal_candle_ts = None
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 动态等待中 | 信号方向={decision.signal.upper()} | "
                    f"信号标的触发价={format_decimal(decision.entry_reference)} | "
                    f"下单标的={trade_instrument.inst_id}"
                )

            current_signal_price = self._get_trigger_price_with_retry(config.inst_id, "last")
            if not local_entry_trigger_hit(active_trigger.signal, current_signal_price, active_trigger.entry_reference):
                self._stop_event.wait(config.poll_seconds)
                continue

            self._logger(
                f"{_fmt_ts(active_trigger.candle_ts)} | 信号标的已触发动态开仓条件 | "
                f"当前价={format_decimal(current_signal_price)} | 目标价={format_decimal(active_trigger.entry_reference)}"
            )
            try:
                position = self._open_local_position(
                    credentials,
                    config,
                    signal_instrument=signal_instrument,
                    trade_instrument=trade_instrument,
                    signal=active_trigger.signal,
                    signal_entry_reference=active_trigger.entry_reference,
                    signal_atr_value=active_trigger.atr_value,
                    signal_candle_ts=active_trigger.candle_ts,
                    signal_candle_high=active_trigger.signal_candle_high,
                    signal_candle_low=active_trigger.signal_candle_low,
                )
            except OrderSizeTooSmallError as exc:
                self._logger(f"{_fmt_ts(active_trigger.candle_ts)} | 当前无法下单 | {exc}")
                active_trigger = None
                idle_signal_candle_ts = newest_ts
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            protection = self._build_local_protection_plan(
                config,
                signal_instrument=signal_instrument,
                trade_instrument=trade_instrument,
                signal=active_trigger.signal,
                trade_side=position.side,
                estimated_trade_entry=position.entry_price,
                signal_entry_reference=active_trigger.entry_reference,
                signal_atr_value=active_trigger.atr_value,
                signal_candle_ts=active_trigger.candle_ts,
                signal_candle_high=active_trigger.signal_candle_high,
                signal_candle_low=active_trigger.signal_candle_low,
            )
            self._monitor_local_exit(credentials, config, trade_instrument, position, protection)
            return

    def _run_dynamic_signal_only(
        self,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        if config.signal_mode == "both":
            raise RuntimeError("EMA 动态委托策略不支持双向，请选择只做多或只做空")

        strategy = EmaDynamicOrderStrategy()
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None

        self._logger(f"启动信号监控 | 策略={self._strategy_name} | 标的={instrument.inst_id} | K线周期={config.bar}")
        self._logger(
            f"运行模式：只监控信号，不下单；每根新 K 线确认后，如生成新的 {_dynamic_entry_reference_ema_text(config)} 动态委托参考价，则发送邮件通知。"
        )
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period,
                config.trend_ema_period,
                config.atr_period,
                entry_reference_ema_period,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config, price_increment=instrument.tick_size)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无动态委托信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成信号邮件")

            protection = build_protection_plan(
                instrument=instrument,
                config=config,
                direction=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                trigger_inst_id=instrument.inst_id,
            )
            reason = (
                "EMA 动态委托参考价已更新"
                f" | 止损={format_decimal(protection.stop_loss)}"
                f" | 止盈={format_decimal(protection.take_profit)}"
            )
            self._logger(
                f"{_fmt_ts(decision.candle_ts)} | 信号触发 | 方向={decision.signal.upper()} | "
                f"参考价={format_decimal(decision.entry_reference)} | {reason}"
            )
            self._notify_signal(
                config,
                signal=decision.signal,
                trigger_symbol=instrument.inst_id,
                entry_reference=decision.entry_reference,
                tick_size=instrument.tick_size,
                reason=reason,
            )
            self._stop_event.wait(config.poll_seconds)

    def _run_dynamic_local_strategy_v2(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
        if effective_signal_mode == "both":
            raise RuntimeError("EMA 动态委托不支持双向，请选择只做多或只做空。")

        strategy = EmaDynamicOrderStrategy()
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None
        active_trigger: LocalSignalTrigger | None = None
        idle_signal_candle_ts: int | None = None
        current_wave_signal: Literal["long", "short"] | None = None
        entries_in_current_wave = 0
        current_wave_index = 0
        startup_gate = StartupSignalGateState(
            started_at_ms=int(time.time() * 1000),
            chase_window_seconds=config.resolved_startup_chase_window_seconds(),
        )

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._log_local_mode_summary(config, signal_instrument, trade_instrument)
        self._logger(
            f"策略规则：每根新 K 线确认后，上一根动态委托自动失效，再按最新 {_dynamic_entry_reference_ema_text(config)} 重新挂下一根委托。"
        )
        mode_parts = [
            f"方向={_format_signal_mode(effective_signal_mode)}",
            f"止盈方式={'动态止盈' if config.take_profit_mode == 'dynamic' else '固定止盈'}",
            f"每波最多开仓次数={config.max_entries_per_trend or 0}",
            f"挂单参考={_dynamic_entry_reference_ema_text(config)}",
            f"启动追单窗口={config.startup_chase_window_label()}",
        ]
        if config.take_profit_mode == "dynamic":
            mode_parts.append(f"2R保本={config.dynamic_two_r_break_even_label()}")
            mode_parts.append(f"手续费偏移={config.dynamic_fee_offset_enabled_label()}")
            mode_parts.append(
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        self._logger(" | ".join(mode_parts))
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period, config.atr_period, entry_reference_ema_period)
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if active_trigger is None and idle_signal_candle_ts == newest_ts:
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            if newest_ts != last_candle_ts or active_trigger is None:
                decision = strategy.evaluate(
                    confirmed,
                    replace(config, signal_mode=effective_signal_mode),
                    price_increment=signal_instrument.tick_size,
                )
                last_candle_ts = newest_ts
                if decision.signal is None:
                    active_trigger = None
                    current_wave_signal = None
                    entries_in_current_wave = 0
                    _reset_startup_signal_gate(startup_gate)
                    idle_signal_candle_ts = newest_ts
                    self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成动态开仓价 | {decision.reason}")
                    self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                    continue
                if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                    raise RuntimeError("策略返回的数据不完整，无法生成本地触发条件。")

                should_skip_startup_signal, startup_gate_message = _should_skip_startup_signal(
                    startup_gate,
                    signal=decision.signal,
                    candle_ts=decision.candle_ts,
                    bar=config.bar,
                )
                if startup_gate_message:
                    self._logger(startup_gate_message)
                if should_skip_startup_signal:
                    active_trigger = None
                    idle_signal_candle_ts = newest_ts
                    self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                    continue

                if current_wave_signal != decision.signal:
                    current_wave_signal = decision.signal
                    entries_in_current_wave = 0
                    current_wave_index += 1
                    self._logger(
                        f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开始 | 方向={decision.signal.upper()}"
                    )

                if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
                    active_trigger = None
                    idle_signal_candle_ts = newest_ts
                    self._logger(
                        f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开仓次数已达上限 | 方向={decision.signal.upper()} | "
                        f"上限={config.max_entries_per_trend}"
                    )
                    self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                    continue

                active_trigger = LocalSignalTrigger(
                    signal=decision.signal,
                    entry_reference=decision.entry_reference,
                    atr_value=decision.atr_value,
                    candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
                idle_signal_candle_ts = None
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 动态等待中 | 信号方向={decision.signal.upper()} | "
                    f"第{current_wave_index}波 | 本波第{entries_in_current_wave + 1}次委托 | 触发价={format_decimal(decision.entry_reference)} | "
                    f"下单标的={trade_instrument.inst_id}"
                )

            if active_trigger is None:
                self._stop_event.wait(config.poll_seconds)
                continue

            current_signal_price = self._get_trigger_price_with_retry(config.inst_id, "last")
            if not local_entry_trigger_hit(active_trigger.signal, current_signal_price, active_trigger.entry_reference):
                self._stop_event.wait(config.poll_seconds)
                continue

            self._logger(
                f"{_fmt_ts(active_trigger.candle_ts)} | 信号标的已触发动态开仓条件 | 当前价={format_decimal(current_signal_price)} | "
                f"目标价={format_decimal(active_trigger.entry_reference)}"
            )
            try:
                position = self._open_local_position(
                    credentials,
                    config,
                    signal_instrument=signal_instrument,
                    trade_instrument=trade_instrument,
                    signal=active_trigger.signal,
                    signal_entry_reference=active_trigger.entry_reference,
                    signal_atr_value=active_trigger.atr_value,
                    signal_candle_ts=active_trigger.candle_ts,
                    signal_candle_high=active_trigger.signal_candle_high,
                    signal_candle_low=active_trigger.signal_candle_low,
                )
            except OrderSizeTooSmallError as exc:
                self._logger(f"{_fmt_ts(active_trigger.candle_ts)} | ?????? | {exc}")
                active_trigger = None
                idle_signal_candle_ts = newest_ts
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            protection = self._build_local_protection_plan(
                config,
                signal_instrument=signal_instrument,
                trade_instrument=trade_instrument,
                signal=active_trigger.signal,
                trade_side=position.side,
                estimated_trade_entry=position.entry_price,
                signal_entry_reference=active_trigger.entry_reference,
                signal_atr_value=active_trigger.atr_value,
                signal_candle_ts=active_trigger.candle_ts,
                signal_candle_high=active_trigger.signal_candle_high,
                signal_candle_low=active_trigger.signal_candle_low,
            )
            entries_in_current_wave += 1
            active_trigger = None
            self._monitor_local_exit_v2(credentials, config, trade_instrument, position, protection)
            if not self._stop_event.is_set():
                self._logger("本轮持仓已结束，继续监控下一次信号。")

    def _run_dynamic_signal_only_v2(
        self,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)
        if effective_signal_mode == "both":
            raise RuntimeError("EMA 动态委托不支持双向，请选择只做多或只做空。")

        strategy = EmaDynamicOrderStrategy()
        entry_reference_ema_period = config.resolved_entry_reference_ema_period()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            entry_reference_ema_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None
        current_wave_signal: Literal["long", "short"] | None = None
        entries_in_current_wave = 0
        current_wave_index = 0
        startup_gate = StartupSignalGateState(
            started_at_ms=int(time.time() * 1000),
            chase_window_seconds=config.resolved_startup_chase_window_seconds(),
        )

        self._logger(f"启动信号监控 | 策略={self._strategy_name} | 标的={instrument.inst_id} | K线周期={config.bar}")
        mode_parts = [
            "运行模式：只监控信号，不下单",
            f"方向={_format_signal_mode(effective_signal_mode)}",
            f"止盈方式={'动态止盈' if config.take_profit_mode == 'dynamic' else '固定止盈'}",
            f"每波最多开仓次数={config.max_entries_per_trend or 0}",
            f"挂单参考={_dynamic_entry_reference_ema_text(config)}",
            f"启动追单窗口={config.startup_chase_window_label()}",
        ]
        if config.take_profit_mode == "dynamic":
            mode_parts.append(f"2R保本={config.dynamic_two_r_break_even_label()}")
            mode_parts.append(f"手续费偏移={config.dynamic_fee_offset_enabled_label()}")
            mode_parts.append(
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        self._logger(" | ".join(mode_parts))
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period, config.atr_period, entry_reference_ema_period)
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(
                confirmed,
                replace(config, signal_mode=effective_signal_mode),
                price_increment=instrument.tick_size,
            )
            if decision.signal is None:
                current_wave_signal = None
                entries_in_current_wave = 0
                _reset_startup_signal_gate(startup_gate)
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无动态委托信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成信号提醒。")

            should_skip_startup_signal, startup_gate_message = _should_skip_startup_signal(
                startup_gate,
                signal=decision.signal,
                candle_ts=decision.candle_ts,
                bar=config.bar,
            )
            if startup_gate_message:
                self._logger(startup_gate_message)
            if should_skip_startup_signal:
                self._stop_event.wait(config.poll_seconds)
                continue

            if current_wave_signal != decision.signal:
                current_wave_signal = decision.signal
                entries_in_current_wave = 0
                current_wave_index += 1
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开始 | 方向={decision.signal.upper()}"
                )

            if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势信号次数已达上限 | 方向={decision.signal.upper()} | "
                    f"上限={config.max_entries_per_trend}"
                )
                self._stop_event.wait(config.poll_seconds)
                continue

            protection = build_protection_plan(
                instrument=instrument,
                config=config,
                direction=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                trigger_inst_id=instrument.inst_id,
            )
            if config.take_profit_mode == "dynamic":
                reason = f"EMA 动态委托参考价已更新 | 止损={format_decimal(protection.stop_loss)} | 止盈方式=动态止盈"
            else:
                reason = (
                    f"EMA 动态委托参考价已更新 | 止损={format_decimal(protection.stop_loss)} | "
                    f"止盈={format_decimal(protection.take_profit)}"
                )
            self._logger(
                f"{_fmt_ts(decision.candle_ts)} | 信号触发 | 方向={decision.signal.upper()} | "
                f"第{current_wave_index}波 | 本波第{entries_in_current_wave + 1}次信号 | "
                f"参考价={format_decimal(decision.entry_reference)} | {reason}"
            )
            self._notify_signal(
                config,
                signal=decision.signal,
                trigger_symbol=instrument.inst_id,
                entry_reference=decision.entry_reference,
                tick_size=instrument.tick_size,
                reason=reason,
            )
            entries_in_current_wave += 1
            self._stop_event.wait(config.poll_seconds)

    def _run_cross_signal_only(
        self,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        strategy = EmaAtrStrategy()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.trend_ema_period,
            config.big_ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None

        self._logger(f"启动信号监控 | 策略={self._strategy_name} | 标的={instrument.inst_id} | K线周期={config.bar}")
        self._logger("运行模式：只监控信号，不下单；当 EMA 突破/跌破信号出现时发送邮件通知。")
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            current_bar=config.bar,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(
                config.ema_period + 2,
                config.trend_ema_period + 2,
                config.big_ema_period + 2,
                config.atr_period + 2,
            )
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config, price_increment=instrument.tick_size)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成信号邮件")

            protection = build_protection_plan(
                instrument=instrument,
                config=config,
                direction=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                trigger_inst_id=instrument.inst_id,
                use_signal_extrema=True,
                signal_candle_high=decision.signal_candle_high,
                signal_candle_low=decision.signal_candle_low,
            )
            reason = (
                "EMA 突破/跌破信号已确认"
                f" | 止损={format_decimal(protection.stop_loss)}"
                f" | 止盈={format_decimal(protection.take_profit)}"
            )
            self._logger(
                f"{_fmt_ts(decision.candle_ts)} | 信号触发 | 方向={decision.signal.upper()} | "
                f"参考价={format_decimal(decision.entry_reference)} | {reason}"
            )
            self._notify_signal(
                config,
                signal=decision.signal,
                trigger_symbol=instrument.inst_id,
                entry_reference=decision.entry_reference,
                tick_size=instrument.tick_size,
                reason=reason,
            )
            self._stop_event.wait(config.poll_seconds)

    def _run_ema5_ema8_signal_only(
        self,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        strategy = EmaCrossEmaStopStrategy()
        lookback = recommended_indicator_lookback(config.ema_period, config.trend_ema_period)
        last_candle_ts: int | None = None

        self._logger(f"启动信号监控 | 策略={self._strategy_name} | 标的={instrument.inst_id} | K线周期={config.bar}")
        self._logger(
            f"运行模式：只监控信号，不下单，EMA{config.ema_period}/EMA{config.trend_ema_period} "
            "出现金叉死叉时发送邮件通知。"
        )

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period) + 1
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config, price_increment=instrument.tick_size)
            if decision.signal is None or decision.entry_reference is None or decision.ema_value is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无 EMA5/EMA8 交叉信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            reason = (
                f"EMA{config.ema_period}/EMA{config.trend_ema_period} 交叉信号 | "
                f"EMA{config.trend_ema_period}={format_decimal(decision.ema_value)}"
            )
            self._logger(
                f"{_fmt_ts(decision.candle_ts or newest_ts)} | 信号触发 | 方向={decision.signal.upper()} | "
                f"参考价={format_decimal(decision.entry_reference)} | {reason}"
            )
            self._notify_signal(
                config,
                signal=decision.signal,
                trigger_symbol=instrument.inst_id,
                entry_reference=decision.entry_reference,
                tick_size=instrument.tick_size,
                reason=reason,
            )
            self._stop_event.wait(config.poll_seconds)

    def _run_ema5_ema8_local_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        if resolve_trade_inst_id(config) != config.inst_id:
            raise RuntimeError("4H EMA5/EMA8 策略目前只支持信号标的与下单标的相同")

        strategy = EmaCrossEmaStopStrategy()
        lookback = recommended_indicator_lookback(config.ema_period, config.trend_ema_period)
        last_candle_ts: int | None = None
        active_position: FilledPosition | None = None
        active_signal: Literal["long", "short"] | None = None

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._logger(
            f"运行模式：4H EMA{config.ema_period}/EMA{config.trend_ema_period} 交叉开仓 + EMA{config.trend_ema_period} 动态止损 "
            f"| 信号标的={signal_instrument.inst_id}"
        )
        self._logger(f"风险金={format_decimal(config.risk_amount or Decimal('100'))} | 信号方向={config.signal_mode}")

        while not self._stop_event.is_set():
            candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period) + 1
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            current_candle, current_stop_line = strategy.latest_stop_line(confirmed, config)

            if active_position is not None and active_signal is not None:
                stop_hit, stop_candle, stop_line = strategy.stop_triggered(confirmed, config, active_signal)
                self._logger(
                    f"{_fmt_ts(stop_candle.ts)} | 持仓监控 | 方向={active_signal.upper()} | "
                    f"当前收盘价={format_decimal(stop_candle.close)} | EMA{config.trend_ema_period}={format_decimal(stop_line)}"
                )
                if stop_hit:
                    self._logger(
                        f"{_fmt_ts(stop_candle.ts)} | EMA{config.trend_ema_period} 动态止损触发 | "
                        f"当前收盘价={format_decimal(stop_candle.close)} | 动态止损线={format_decimal(stop_line)}"
                    )
                    self._close_position(credentials, config, trade_instrument, active_position, "止损")
                    active_position = None
                    active_signal = None
                self._stop_event.wait(config.poll_seconds)
                continue

            decision = strategy.evaluate(confirmed, config, price_increment=signal_instrument.tick_size)
            if decision.signal is None or decision.entry_reference is None or decision.ema_value is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无 EMA5/EMA8 开仓信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            try:
                active_position = self._open_ema_stop_position(
                    credentials,
                    config,
                    trade_instrument=trade_instrument,
                    signal=decision.signal,
                    stop_loss=current_stop_line,
                    signal_candle_ts=decision.candle_ts or newest_ts,
                )
            except OrderSizeTooSmallError as exc:
                self._logger(f"{_fmt_ts(decision.candle_ts or newest_ts)} | 当前无法下单 | {exc}")
                self._stop_event.wait(config.poll_seconds)
                continue
            active_signal = decision.signal
            self._logger(
                f"{_fmt_ts(decision.candle_ts or newest_ts)} | 动态 EMA 止损策略已开仓 | "
                f"方向={decision.signal.upper()} | EMA{config.trend_ema_period} 止损线={format_decimal(current_stop_line)}"
            )
            self._stop_event.wait(config.poll_seconds)

    def _open_ema_stop_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        signal: Literal["long", "short"],
        stop_loss: Decimal,
        signal_candle_ts: int,
    ) -> FilledPosition:
        trade_side: Literal["buy", "sell"] = "buy" if signal == "long" else "sell"
        pos_side = resolve_open_pos_side(config, trade_side)
        price_for_size = self._estimate_trade_entry_price_with_retry(trade_instrument, trade_side)
        stop_price = snap_to_increment(stop_loss, trade_instrument.tick_size, "nearest")
        size = determine_order_size(
            instrument=trade_instrument,
            config=config,
            entry_price=price_for_size,
            stop_loss=stop_price,
            risk_price_compatible=True,
        )
        self._logger(
            f"{_fmt_ts(signal_candle_ts)} | 准备下单 | 方向={signal.upper()} | "
            f"预估入场价={format_decimal(price_for_size)} | EMA止损线={format_decimal(stop_price)} | "
            f"下单数量={_format_size_with_contract_equivalent(trade_instrument, size)}"
        )
        result = self._place_entry_order(credentials, config, trade_instrument, trade_side, size, pos_side)
        filled = self._wait_for_order_fill(
            credentials,
            config,
            trade_instrument=trade_instrument,
            side=trade_side,
            pos_side=pos_side,
            result=result,
            estimated_entry=price_for_size,
        )
        self._logger(
            f"EMA 交叉开仓已成交 | ordId={filled.ord_id} | 标的={trade_instrument.inst_id} | "
            f"方向={trade_side.upper()} | "
            f"成交均价={_format_notify_price_by_tick_size(filled.entry_price, trade_instrument.tick_size)} | "
            f"成交数量={_format_size_with_contract_equivalent(trade_instrument, filled.size)}"
        )
        self._notify_trade_fill(
            config,
            title="开仓成交",
            symbol=trade_instrument.inst_id,
            side=trade_side,
            size=filled.size,
            size_text=_format_notify_size_with_unit(trade_instrument, filled.size),
            price=filled.entry_price,
            tick_size=trade_instrument.tick_size,
            reason=f"EMA{config.ema_period}/EMA{config.trend_ema_period} 交叉信号成交",
        )
        self._logger(f"委托追踪 | clOrdId={filled.cl_ord_id or '-'} | ordId={filled.ord_id}")
        return filled

    def _open_local_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
        signal: Literal["long", "short"],
        signal_entry_reference: Decimal | None,
        signal_atr_value: Decimal | None,
        signal_candle_ts: int | None,
        signal_candle_high: Decimal | None,
        signal_candle_low: Decimal | None,
    ) -> FilledPosition:
        trade_side = resolve_entry_side(signal, config.entry_side_mode)
        pos_side = resolve_open_pos_side(config, trade_side)
        price_for_size = self._estimate_trade_entry_price_with_retry(trade_instrument, trade_side)

        protection = self._build_local_protection_plan(
            config,
            signal_instrument=signal_instrument,
            trade_instrument=trade_instrument,
            signal=signal,
            trade_side=trade_side,
            estimated_trade_entry=price_for_size,
            signal_entry_reference=signal_entry_reference,
            signal_atr_value=signal_atr_value,
            signal_candle_ts=signal_candle_ts,
            signal_candle_high=signal_candle_high,
            signal_candle_low=signal_candle_low,
        )
        size = determine_order_size(
            instrument=trade_instrument,
            config=config,
            entry_price=price_for_size,
            stop_loss=protection.stop_loss,
            risk_price_compatible=protection.trigger_inst_id == trade_instrument.inst_id,
        )
        self._logger(
            f"{_fmt_ts(signal_candle_ts or int(datetime.now().timestamp() * 1000))} | 准备本地下单 | "
            f"信号方向={signal.upper()} | 实际下单方向={trade_side.upper()} | 下单标的={trade_instrument.inst_id} | "
            f"预估入场价={format_decimal(price_for_size)} | 数量={_format_size_with_contract_equivalent(trade_instrument, size)}"
        )

        result = self._place_entry_order(credentials, config, trade_instrument, trade_side, size, pos_side)
        filled = self._wait_for_order_fill(
            credentials,
            config,
            trade_instrument=trade_instrument,
            side=trade_side,
            pos_side=pos_side,
            result=result,
            estimated_entry=price_for_size,
        )
        self._logger(
            f"本地下单成交 | ordId={filled.ord_id} | 标的={trade_instrument.inst_id} | "
            f"方向={trade_side.upper()} | "
            f"成交均价={_format_notify_price_by_tick_size(filled.entry_price, trade_instrument.tick_size)} | "
            f"成交数量={_format_size_with_contract_equivalent(trade_instrument, filled.size)}"
        )
        self._notify_trade_fill(
            config,
            title="开仓成交",
            symbol=trade_instrument.inst_id,
            side=trade_side,
            size=filled.size,
            size_text=_format_notify_size_with_unit(trade_instrument, filled.size),
            price=filled.entry_price,
            tick_size=trade_instrument.tick_size,
            reason="本地下单成交",
        )
        self._logger(f"委托追踪 | clOrdId={filled.cl_ord_id or '-'} | ordId={filled.ord_id}")
        return filled

    def _build_local_protection_plan(
        self,
        config: StrategyConfig,
        *,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
        signal: Literal["long", "short"],
        trade_side: Literal["buy", "sell"],
        estimated_trade_entry: Decimal,
        signal_entry_reference: Decimal | None,
        signal_atr_value: Decimal | None,
        signal_candle_ts: int | None,
        signal_candle_high: Decimal | None,
        signal_candle_low: Decimal | None,
    ) -> ProtectionPlan:
        mode = config.tp_sl_mode
        if mode == "local_signal":
            if signal_entry_reference is None or signal_atr_value is None or signal_candle_ts is None:
                raise RuntimeError("信号标的止盈止损缺少入场参考价或 ATR 数据")
            return build_protection_plan(
                instrument=signal_instrument,
                config=config,
                direction=signal,
                entry_reference=signal_entry_reference,
                atr_value=signal_atr_value,
                candle_ts=signal_candle_ts,
                trigger_inst_id=signal_instrument.inst_id,
                use_signal_extrema=is_ema_atr_breakout_strategy(config.strategy_id),
                signal_candle_high=signal_candle_high,
                signal_candle_low=signal_candle_low,
            )

        if mode in {"exchange", "local_trade"}:
            snapshot = self._fetch_atr_snapshot_with_retry(trade_instrument.inst_id, config.bar, config.atr_period)
            trade_direction: Literal["long", "short"] = "long" if trade_side == "buy" else "short"
            return build_protection_plan(
                instrument=trade_instrument,
                config=config,
                direction=trade_direction,
                entry_reference=estimated_trade_entry,
                atr_value=snapshot.atr_value,
                candle_ts=snapshot.candle_ts,
                trigger_inst_id=trade_instrument.inst_id,
            )

        if mode == "local_custom":
            trigger_inst_id = (config.local_tp_sl_inst_id or "").strip().upper()
            if not trigger_inst_id:
                raise RuntimeError("已选择自定义本地止盈止损，但没有填写触发标的")
            custom_instrument = self._get_instrument_with_retry(trigger_inst_id)
            snapshot = self._fetch_atr_snapshot_with_retry(custom_instrument.inst_id, config.bar, config.atr_period)
            return build_protection_plan(
                instrument=custom_instrument,
                config=config,
                direction=signal,
                entry_reference=snapshot.candle_close,
                atr_value=snapshot.atr_value,
                candle_ts=snapshot.candle_ts,
                trigger_inst_id=custom_instrument.inst_id,
            )

        raise RuntimeError("本地止盈止损模式错误")

    def _monitor_local_exit(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        position: FilledPosition,
        protection: ProtectionPlan,
    ) -> None:
        dynamic_take_profit_enabled = _live_dynamic_take_profit_enabled(config)
        current_stop_loss = protection.stop_loss
        current_take_profit = protection.take_profit
        next_trigger_r = 2
        risk_per_unit = abs(position.entry_price - protection.stop_loss)
        monitor_parts = [
            f"开始本地止盈止损监控 | 触发标的={protection.trigger_inst_id}",
            f"触发价格类型={protection.trigger_price_type}",
            f"止损={format_decimal(protection.stop_loss)}",
            f"止盈={format_decimal(protection.take_profit)}",
        ]
        if dynamic_take_profit_enabled:
            monitor_parts.append(f"2R保本={config.dynamic_two_r_break_even_label()}")
            monitor_parts.append(f"手续费偏移={config.dynamic_fee_offset_enabled_label()}")
            monitor_parts.append(
                f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根"
            )
        self._logger(" | ".join(monitor_parts))
        while not self._stop_event.is_set():
            current_price = self._get_trigger_price_with_retry(
                protection.trigger_inst_id,
                protection.trigger_price_type,
            )
            if dynamic_take_profit_enabled:
                holding_bars = _holding_bars_live(position.entry_ts, int(time.time() * 1000), config.bar)
                updated_stop_loss, updated_take_profit, updated_trigger_r, moved = _advance_dynamic_stop_live(
                    direction=protection.direction,
                    current_price=current_price,
                    entry_price=position.entry_price,
                    risk_per_unit=risk_per_unit,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    tick_size=trade_instrument.tick_size,
                    two_r_break_even=config.dynamic_two_r_break_even,
                    dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
                    holding_bars=holding_bars,
                    time_stop_break_even_enabled=config.time_stop_break_even_enabled,
                    time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                )
                if moved:
                    current_stop_loss = updated_stop_loss
                    current_take_profit = updated_take_profit
                    next_trigger_r = updated_trigger_r
                    self._logger(
                        f"动态止盈上移 | 当前价={format_decimal(current_price)} | "
                        f"新止损={format_decimal(current_stop_loss)} | 下一阶段={next_trigger_r}R | "
                        f"holding_bars={holding_bars}"
                    )
                stop_hit = current_price <= current_stop_loss if protection.direction == "long" else current_price >= current_stop_loss
                take_hit = False
            else:
                stop_hit, take_hit = evaluate_local_exit(
                    direction=protection.direction,
                    current_price=current_price,
                    stop_loss=current_stop_loss,
                    take_profit=current_take_profit,
                )
            if stop_hit or take_hit:
                reason = "止损" if stop_hit else "止盈"
                self._logger(
                    f"本地{reason}触发 | 触发标的={protection.trigger_inst_id} | "
                    f"当前价={format_decimal(current_price)} | "
                    f"止损={format_decimal(current_stop_loss)} | 止盈={format_decimal(current_take_profit)}"
                )
                self._close_position(credentials, config, trade_instrument, position, reason)
                return
            self._stop_event.wait(config.poll_seconds)

    def _close_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        position: FilledPosition,
        reason: str,
    ) -> None:
        remaining = position.size
        lot = trade_instrument.lot_size
        for _ in range(3):
            if remaining <= 0:
                break

            size = snap_to_increment(remaining, lot, "down")
            if size < trade_instrument.min_size:
                self._logger(
                    f"{reason}后剩余未平数量 {format_decimal(remaining)} 小于最小下单量 "
                    f"{format_decimal(trade_instrument.min_size)}，请手动处理。"
                )
                return

            result = self._place_exit_order(
                credentials,
                config,
                trade_instrument=trade_instrument,
                side=position.close_side,
                size=size,
                pos_side=position.pos_side,
            )
            filled = self._wait_for_order_fill(
                credentials,
                config,
                trade_instrument=trade_instrument,
                side=position.close_side,
                pos_side=position.pos_side,
                result=result,
                estimated_entry=self._estimate_trade_entry_price_with_retry(trade_instrument, position.close_side),
            )
            remaining -= filled.size
            self._logger(
                f"本地{reason}平仓已成交 | ordId={filled.ord_id} | 标的={trade_instrument.inst_id} | "
                f"方向={position.close_side.upper()} | "
                f"成交均价={_format_notify_price_by_tick_size(filled.entry_price, trade_instrument.tick_size)} | "
                f"成交数量={_format_size_with_contract_equivalent(trade_instrument, filled.size)} | 剩余={_format_size_with_contract_equivalent(trade_instrument, max(remaining, Decimal('0')))}"
            )
            self._notify_trade_fill(
                config,
                title=f"{reason}平仓成交",
                symbol=trade_instrument.inst_id,
                side=position.close_side,
                size=filled.size,
                size_text=_format_notify_size_with_unit(trade_instrument, filled.size),
                price=filled.entry_price,
                tick_size=trade_instrument.tick_size,
                reason=f"本地{reason}触发后平仓成交",
                trade_pnl=StrategyEngine._trade_fill_pnl_text_for_close(
                    position,
                    fill_size=filled.size,
                    fill_price=filled.entry_price,
                ),
            )

        if remaining > 0:
            raise RuntimeError(f"本地{reason}平仓后仍有剩余仓位 {format_decimal(remaining)}，请手动检查")

        self._logger("本次本地止盈止损流程已结束。")

    def _monitor_local_exit_v2(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        position: FilledPosition,
        protection: ProtectionPlan,
    ) -> None:
        self._monitor_local_exit(credentials, config, trade_instrument, position, protection)

    def _monitor_exchange_dynamic_stop(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        position: FilledPosition,
        initial_stop_loss: Decimal,
        stop_loss_algo_cl_ord_id: str | None,
        stop_loss_algo_id: str | None = None,
    ) -> None:
        algo_id_norm = (stop_loss_algo_id or "").strip()
        algo_cl_norm = (stop_loss_algo_cl_ord_id or "").strip()
        if not algo_id_norm and not algo_cl_norm:
            raise RuntimeError("动态止盈模式缺少 OKX 止损委托标识（algoId 或 algoClOrdId），无法继续动态上移。")

        current_stop_loss = initial_stop_loss
        next_trigger_r = 2
        amend_failures = 0
        consecutive_read_failures = 0
        risk_per_unit = abs(position.entry_price - initial_stop_loss)
        ref_parts: list[str] = []
        if algo_cl_norm:
            ref_parts.append(f"algoClOrdId={algo_cl_norm}")
        if algo_id_norm:
            ref_parts.append(f"algoId={algo_id_norm}")
        ref_label = " | ".join(ref_parts) if ref_parts else "-"
        pos_side_txt = (position.pos_side or "").strip()
        api_txt = (self._api_name or "").strip() or "-"
        pos_bits = [
            f"开始监控 OKX 动态止损 | API={api_txt} | 标的={trade_instrument.inst_id}",
            f"持仓方向={position.side.upper()}"
            + (f" | posSide={pos_side_txt}" if pos_side_txt else ""),
            f"开仓价={format_decimal(position.entry_price)}",
            f"数量={_format_size_with_contract_equivalent(trade_instrument, position.size)}",
            f"R价差(开仓-初始止损)={format_decimal(risk_per_unit)}",
            ref_label,
            f"触发价格类型={config.tp_sl_trigger_type}",
            f"初始止损={format_decimal(initial_stop_loss)}",
            f"2R保本={config.dynamic_two_r_break_even_label()}",
            f"手续费偏移={config.dynamic_fee_offset_enabled_label()}",
            f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根",
        ]
        self._logger(" | ".join(pos_bits))

        while not self._stop_event.is_set():
            try:
                should_continue = self._monitor_exchange_dynamic_stop_once(
                    credentials,
                    config,
                    trade_instrument=trade_instrument,
                    position=position,
                    stop_loss_algo_cl_ord_id=algo_cl_norm or None,
                    stop_loss_algo_id=algo_id_norm or None,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    risk_per_unit=risk_per_unit,
                    amend_failures=amend_failures,
                )
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                retryable_exc = _coerce_okx_read_exception(exc)
                if retryable_exc is None or not _is_transient_okx_error(retryable_exc):
                    raise
                consecutive_read_failures += 1
                detail = str(retryable_exc).strip() or f"code={retryable_exc.code or '-'}"
                if consecutive_read_failures < OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES:
                    self._logger(
                        " | ".join(
                            [
                                "OKX 动态止损监控读取异常，保留当前止损并继续重试",
                                f"连续失败={consecutive_read_failures}/{OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES}",
                                detail,
                            ]
                        )
                    )
                    self._stop_event.wait(max(config.poll_seconds, 1.0))
                    continue
                raise RuntimeError(
                    f"OKX 动态止损监控连续读取失败 {consecutive_read_failures} 次：{detail} | 已保留当前 OKX 止损，请人工检查"
                ) from exc
            else:
                consecutive_read_failures = 0
                current_stop_loss = should_continue.current_stop_loss
                next_trigger_r = should_continue.next_trigger_r
                amend_failures = should_continue.amend_failures
                if not should_continue.keep_monitoring:
                    return
                self._stop_event.wait(config.poll_seconds)
                continue

        self._logger("策略已停止，保留当前 OKX 动态止损。")

    def run_takeover_exchange_dynamic_stop(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        position: FilledPosition,
        initial_stop_loss: Decimal,
        stop_loss_algo_id: str | None,
        stop_loss_algo_cl_ord_id: str | None,
    ) -> None:
        """持仓大窗「接管」：沿用已有 OKX 止损算法单，按模板动态止盈规则改价上移。"""
        if not _live_dynamic_take_profit_enabled(config):
            raise RuntimeError("当前策略模板未启用动态止盈，无法接管。")
        if config.trader_virtual_stop_loss:
            raise RuntimeError("交易员虚拟止损模式下不存在可接管的交易所止损单。")
        if not (stop_loss_algo_id or "").strip() and not (stop_loss_algo_cl_ord_id or "").strip():
            raise RuntimeError("缺少止损算法单 algoId / algoClOrdId。")
        self._monitor_exchange_dynamic_stop(
            credentials,
            config,
            trade_instrument=trade_instrument,
            position=position,
            initial_stop_loss=initial_stop_loss,
            stop_loss_algo_cl_ord_id=(stop_loss_algo_cl_ord_id or "").strip() or None,
            stop_loss_algo_id=(stop_loss_algo_id or "").strip() or None,
        )

    def interrupt_takeover_monitor(self) -> None:
        """中断由 run_takeover_exchange_dynamic_stop 或独立引擎实例触发的动态止损轮询。"""
        self._stop_event.set()

    def _monitor_exchange_dynamic_stop_once(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        position: FilledPosition,
        stop_loss_algo_cl_ord_id: str | None,
        stop_loss_algo_id: str | None,
        current_stop_loss: Decimal,
        next_trigger_r: int,
        risk_per_unit: Decimal,
        amend_failures: int,
    ) -> DynamicStopMonitorStepResult:
        algo_ref = (
            f"algoClOrdId={stop_loss_algo_cl_ord_id}"
            if (stop_loss_algo_cl_ord_id or "").strip()
            else f"algoId={stop_loss_algo_id or '-'}"
        )
        live_position = self._find_managed_position(credentials, config, trade_instrument, position)
        if live_position is None:
            self._logger("未检测到策略持仓，OKX 动态止损监控结束。")
            return DynamicStopMonitorStepResult(
                keep_monitoring=False,
                current_stop_loss=current_stop_loss,
                next_trigger_r=next_trigger_r,
                amend_failures=amend_failures,
            )

        direction: Literal["long", "short"] = "long" if position.side == "buy" else "short"
        current_price = self._get_trigger_price_with_retry(trade_instrument.inst_id, config.tp_sl_trigger_type)
        holding_bars = _holding_bars_live(position.entry_ts, int(time.time() * 1000), config.bar)
        updated_stop_loss, next_trigger_price, updated_trigger_r, moved = _advance_dynamic_stop_live(
            direction=direction,
            current_price=current_price,
            entry_price=position.entry_price,
            risk_per_unit=risk_per_unit,
            current_stop_loss=current_stop_loss,
            next_trigger_r=next_trigger_r,
            tick_size=trade_instrument.tick_size,
            two_r_break_even=config.dynamic_two_r_break_even,
            dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            holding_bars=holding_bars,
            time_stop_break_even_enabled=config.time_stop_break_even_enabled,
            time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
        )
        should_amend = moved and (
            updated_stop_loss > current_stop_loss if position.side == "buy" else updated_stop_loss < current_stop_loss
        )
        if not should_amend:
            return DynamicStopMonitorStepResult(
                keep_monitoring=True,
                current_stop_loss=current_stop_loss,
                next_trigger_r=next_trigger_r,
                amend_failures=0,
            )

        fresh_price = self._get_trigger_price_with_retry(trade_instrument.inst_id, config.tp_sl_trigger_type)
        if not _is_exchange_dynamic_stop_candidate_valid(
            direction=direction,
            current_price=fresh_price,
            candidate_stop_loss=updated_stop_loss,
        ):
            self._logger(
                " | ".join(
                    [
                        "OKX 动态止损候选价已过期，跳过本次上移",
                        f"当前价={format_decimal(fresh_price)}",
                        f"当前止损={format_decimal(current_stop_loss)}",
                        f"候选止损={format_decimal(updated_stop_loss)}",
                        f"下一触发价={format_decimal(next_trigger_price)}",
                        algo_ref,
                    ]
                )
            )
            self._stop_event.wait(max(config.poll_seconds / 2, 0.5))
            return DynamicStopMonitorStepResult(
                keep_monitoring=True,
                current_stop_loss=current_stop_loss,
                next_trigger_r=next_trigger_r,
                amend_failures=amend_failures,
            )

        algo_order = self._find_pending_algo_order_for_dynamic_stop(
            credentials,
            config,
            trade_instrument=trade_instrument,
            algo_cl_ord_id=stop_loss_algo_cl_ord_id,
            algo_id=stop_loss_algo_id,
        )
        if algo_order is None:
            self._logger(
                " | ".join(
                    [
                        "OKX 动态止损委托暂未出现在挂单列表，稍后重试",
                        f"候选止损={format_decimal(updated_stop_loss)}",
                        algo_ref,
                    ]
                )
            )
            self._stop_event.wait(max(config.poll_seconds / 2, 0.5))
            return DynamicStopMonitorStepResult(
                keep_monitoring=True,
                current_stop_loss=current_stop_loss,
                next_trigger_r=next_trigger_r,
                amend_failures=amend_failures,
            )

        amend_cl_ord = (algo_order.algo_client_order_id or stop_loss_algo_cl_ord_id or "").strip() or None
        try:
            self._amend_algo_order_with_recovery(
                credentials,
                config,
                trade_instrument=trade_instrument,
                algo_id=algo_order.algo_id,
                algo_cl_ord_id=amend_cl_ord,
                req_id=self._next_client_order_id(role="amd"),
                new_stop_loss_trigger_price=updated_stop_loss,
                new_stop_loss_trigger_price_type=config.tp_sl_trigger_type,
                recover_algo_id=(stop_loss_algo_id or "").strip() or None,
                recover_algo_cl_ord_id=(stop_loss_algo_cl_ord_id or "").strip() or None,
            )
        except OkxApiError as exc:
            live_position = self._find_managed_position(credentials, config, trade_instrument, position)
            if live_position is None:
                self._logger("检测到持仓已关闭，停止 OKX 动态止损监控。")
                return DynamicStopMonitorStepResult(
                    keep_monitoring=False,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    amend_failures=amend_failures,
                )
            latest_price = self._get_trigger_price_with_retry(
                trade_instrument.inst_id,
                config.tp_sl_trigger_type,
            )
            detail = str(exc).strip() or f"code={exc.code or '-'}"
            if not _is_exchange_dynamic_stop_candidate_valid(
                direction=direction,
                current_price=latest_price,
                candidate_stop_loss=updated_stop_loss,
            ):
                self._logger(
                    " | ".join(
                        [
                            "OKX 动态止损上移遇到快速回抽，本次改价已放弃",
                            f"当前价={format_decimal(latest_price)}",
                            f"候选止损={format_decimal(updated_stop_loss)}",
                            algo_ref,
                            detail,
                        ]
                    )
                )
                self._stop_event.wait(max(config.poll_seconds / 2, 0.5))
                return DynamicStopMonitorStepResult(
                    keep_monitoring=True,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    amend_failures=amend_failures,
                )

            next_amend_failures = amend_failures + 1
            if next_amend_failures < 4:
                self._logger(
                    " | ".join(
                        [
                            "OKX 动态止损上移失败，稍后重试",
                            f"当前价={format_decimal(latest_price)}",
                            f"候选止损={format_decimal(updated_stop_loss)}",
                            algo_ref,
                            detail,
                        ]
                    )
                )
                self._stop_event.wait(max(config.poll_seconds / 2, 0.5))
                return DynamicStopMonitorStepResult(
                    keep_monitoring=True,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    amend_failures=next_amend_failures,
                )
            raise RuntimeError(f"OKX 动态止损上移失败：{detail}") from exc

        self._logger(
            f"OKX 动态止损已上移 | 当前价={format_decimal(fresh_price)} | "
            f"新止损={format_decimal(updated_stop_loss)} | 下一阶段={updated_trigger_r}R | "
            f"{algo_ref} | holding_bars={holding_bars}"
        )
        return DynamicStopMonitorStepResult(
            keep_monitoring=True,
            current_stop_loss=updated_stop_loss,
            next_trigger_r=updated_trigger_r,
            amend_failures=0,
        )

    def _monitor_exchange_managed_position_until_closed(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        position: FilledPosition,
    ) -> None:
        consecutive_read_failures = 0
        saw_live_position = False
        while not self._stop_event.is_set():
            try:
                live_position = self._find_managed_position(credentials, config, trade_instrument, position)
            except OkxApiError as exc:
                consecutive_read_failures += 1
                detail = str(exc).strip() or f"code={exc.code or '-'}"
                if consecutive_read_failures < OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES:
                    self._logger(
                        " | ".join(
                            [
                                "OKX 托管持仓读取异常，保留当前 OKX 止盈止损，稍后重试",
                                f"第{consecutive_read_failures}/{OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES}次",
                                detail,
                            ]
                        )
                    )
                    self._stop_event.wait(max(config.poll_seconds, 1.0))
                    continue
                raise RuntimeError(
                    "OKX 托管持仓连续读取失败 "
                    f"{OKX_DYNAMIC_STOP_MONITOR_MAX_READ_FAILURES} 次：{detail} | 已保留当前 OKX 止盈止损，请人工检查"
                ) from exc

            consecutive_read_failures = 0
            if live_position is None:
                if saw_live_position:
                    self._logger("检测到 OKX 托管持仓已结束。")
                else:
                    self._logger("未再检测到策略持仓，视为本轮 OKX 托管持仓已结束。")
                return

            saw_live_position = True
            self._stop_event.wait(config.poll_seconds)

        self._logger("策略已停止，保留当前 OKX 托管止盈止损。")

    def _find_pending_algo_order_for_dynamic_stop(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        algo_cl_ord_id: str | None,
        algo_id: str | None,
    ) -> OkxTradeOrderItem | None:
        aid = (algo_id or "").strip()
        acid = (algo_cl_ord_id or "").strip()
        if not aid and not acid:
            return None
        pending_orders = self._get_pending_orders_with_retry(
            credentials,
            config,
            inst_types=(trade_instrument.inst_type,),
            limit=100,
        )
        for item in pending_orders:
            if item.source_kind != "algo":
                continue
            if item.inst_id != trade_instrument.inst_id:
                continue
            if aid and (item.algo_id or "").strip() == aid:
                return item
            if not aid and acid and (item.algo_client_order_id or "").strip() == acid:
                return item
        if aid and acid:
            for item in pending_orders:
                if item.source_kind != "algo":
                    continue
                if item.inst_id != trade_instrument.inst_id:
                    continue
                if (item.algo_client_order_id or "").strip() == acid:
                    return item
        return None

    def _find_pending_algo_order_by_client_id(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        algo_cl_ord_id: str,
    ) -> OkxTradeOrderItem | None:
        return self._find_pending_algo_order_for_dynamic_stop(
            credentials,
            config,
            trade_instrument=trade_instrument,
            algo_cl_ord_id=algo_cl_ord_id,
            algo_id=None,
        )

    def _find_pending_entry_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str | None,
        cl_ord_id: str | None,
    ) -> OkxTradeOrderItem | None:
        pending_orders = self._get_pending_orders_with_retry(
            credentials,
            config,
            inst_types=(infer_inst_type(inst_id),),
            limit=100,
        )
        normalized_ord_id = (ord_id or "").strip()
        normalized_cl_ord_id = (cl_ord_id or "").strip()
        for item in pending_orders:
            if item.source_kind != "normal":
                continue
            if item.inst_id != inst_id:
                continue
            if normalized_ord_id and (item.order_id or "").strip() == normalized_ord_id:
                return item
            if normalized_cl_ord_id and (item.client_order_id or "").strip() == normalized_cl_ord_id:
                return item
        return None

    def _find_managed_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        position: FilledPosition,
    ) -> OkxPosition | None:
        positions = self._get_positions_with_retry(
            credentials,
            config,
            inst_type=trade_instrument.inst_type,
        )
        for item in positions:
            if item.inst_id != trade_instrument.inst_id:
                continue
            item_pos_side = (item.pos_side or "").strip().lower()
            expected_pos_side = (position.pos_side or "").strip().lower()
            if expected_pos_side:
                if item_pos_side != expected_pos_side:
                    continue
            elif config.position_mode == "net":
                if position.side == "buy" and item.position < 0:
                    continue
                if position.side == "sell" and item.position > 0:
                    continue
            return item
        return None

    def _call_okx_read_with_retry(self, label: str, fn: Callable[[], T]) -> T:
        return self._retry_policy.call_okx_read_with_retry(label, fn)

    def _get_instrument_with_retry(self, inst_id: str) -> Instrument:
        return self._retry_policy.get_instrument(inst_id)

    def _get_candles_with_retry(self, inst_id: str, bar: str, *, limit: int) -> list:
        return self._retry_policy.get_candles(inst_id, bar, limit=limit)

    def _get_order_with_retry(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ):
        return self._retry_policy.get_order(
            credentials,
            config,
            inst_id=inst_id,
            ord_id=ord_id,
            cl_ord_id=cl_ord_id,
        )

    def _get_trigger_price_with_retry(self, inst_id: str, price_type: str) -> Decimal:
        return self._retry_policy.get_trigger_price(inst_id, price_type)

    def _get_pending_orders_with_retry(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_types: tuple[str, ...],
        limit: int,
    ):
        return self._retry_policy.get_pending_orders(
            credentials,
            config,
            inst_types=inst_types,
            limit=limit,
        )

    def _get_positions_with_retry(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_type: str | None = None,
    ) -> list[OkxPosition]:
        return self._retry_policy.get_positions(credentials, config, inst_type=inst_type)

    def _fetch_atr_snapshot_with_retry(
        self,
        inst_id: str,
        bar: str,
        atr_period: int,
    ) -> AtrSnapshot:
        return self._call_okx_read_with_retry(
            f"读取ATR快照 {inst_id} {bar}",
            lambda: fetch_atr_snapshot(self._client, inst_id, bar, atr_period),
        )

    def _estimate_trade_entry_price_with_retry(
        self,
        instrument: Instrument,
        side: Literal["buy", "sell"],
    ) -> Decimal:
        return self._call_okx_read_with_retry(
            f"Estimate entry price {instrument.inst_id} {side}",
            lambda: estimate_trade_entry_price(self._client, instrument, side),
        )

    def _fetch_hourly_debug_snapshot_with_retry(
        self,
        inst_id: str,
        *,
        ema_period: int,
        trend_ema_period: int = 0,
        big_ema_period: int = 0,
        entry_reference_ema_period: int = 0,
    ) -> HourlyDebugSnapshot:
        return self._call_okx_read_with_retry(
            f"读取1小时调试值 {inst_id}",
            lambda: fetch_hourly_ema_debug(
                self._client,
                inst_id,
                ema_period=ema_period,
                trend_ema_period=trend_ema_period,
                big_ema_period=big_ema_period,
                entry_reference_ema_period=entry_reference_ema_period,
            ),
        )

    def _wait_for_write_reconcile(self, attempt: int) -> None:
        delay_seconds = min(
            OKX_WRITE_RECONCILE_BASE_DELAY_SECONDS * attempt,
            OKX_WRITE_RECONCILE_MAX_DELAY_SECONDS,
        )
        self._stop_event.wait(delay_seconds)

    def _try_get_order_status_for_write_recovery(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        label: str,
        ord_id: str | None = None,
        cl_ord_id: str | None = None,
    ) -> OkxOrderStatus | None:
        order_key = ord_id or cl_ord_id or "-"
        last_exc: OkxApiError | None = None
        for attempt in range(1, OKX_WRITE_RECONCILE_ATTEMPTS + 1):
            try:
                return self._client.get_order(
                    credentials,
                    config,
                    inst_id=inst_id,
                    ord_id=ord_id,
                    cl_ord_id=cl_ord_id,
                )
            except OkxApiError as exc:
                if _is_okx_order_not_found_error(exc):
                    return None
                last_exc = exc
                detail = str(exc).strip() or f"code={exc.code or '-'}"
                if _is_transient_okx_error(exc) and attempt < OKX_WRITE_RECONCILE_ATTEMPTS and not self._stop_event.is_set():
                    self._logger(
                        " | ".join(
                            [
                                "OKX 回查订单状态遇到读取异常，稍后重试",
                                f"操作={label}",
                                f"订单={order_key}",
                                f"第{attempt}/{OKX_WRITE_RECONCILE_ATTEMPTS}次",
                                detail,
                            ]
                        )
                    )
                    self._wait_for_write_reconcile(attempt)
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        return None

    def _recover_submitted_order_result(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        cl_ord_id: str,
        label: str,
    ) -> OkxOrderResult | None:
        for attempt in range(1, OKX_WRITE_RECONCILE_ATTEMPTS + 1):
            status = self._try_get_order_status_for_write_recovery(
                credentials,
                config,
                inst_id=inst_id,
                label=label,
                cl_ord_id=cl_ord_id,
            )
            if status is not None and status.ord_id:
                return OkxOrderResult(
                    ord_id=status.ord_id,
                    cl_ord_id=cl_ord_id,
                    s_code="0",
                    s_msg="recovered via order lookup",
                    raw={
                        "recovered": True,
                        "state": status.state,
                        "order": status.raw,
                    },
                )
            if attempt < OKX_WRITE_RECONCILE_ATTEMPTS and not self._stop_event.is_set():
                self._logger(
                    " | ".join(
                        [
                            "OKX 写入异常后回查暂未命中",
                            f"操作={label}",
                            f"clOrdId={cl_ord_id}",
                            f"第{attempt}/{OKX_WRITE_RECONCILE_ATTEMPTS}次",
                        ]
                    )
                )
                self._wait_for_write_reconcile(attempt)
        return None

    @staticmethod
    def _build_okx_write_failure_message(
        *,
        label: str,
        inst_id: str,
        cl_ord_id: str,
        detail: str,
        code: str | None = None,
    ) -> str:
        normalized_detail = str(detail or "").strip() or "-"
        normalized_code = str(code or "").strip()
        code_text = f" | code={normalized_code}" if normalized_code else ""
        if "操作全部失败" in normalized_detail:
            return (
                f"OKX {label}被交易所拒绝 | 标的={inst_id} | clOrdId={cl_ord_id} | "
                f"原始返回={normalized_detail}{code_text} | 常见原因："
                "1) clOrdId 含非法字符；2) 保证金/可用余额不足（含模拟盘虚拟金）；"
                "3) 下单参数不合法（数量/张数步长、价格 tick、持仓模式 posSide）；"
                "4) 附带止损/止盈与主单价位或触发类型不满足 OKX 规则（可在网页端用同参试挂对照）。"
            )
        return (
            f"OKX {label}失败 | 标的={inst_id} | clOrdId={cl_ord_id} | "
            f"原始返回={normalized_detail}{code_text}"
        )

    def _submit_order_with_recovery(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        inst_id: str,
        cl_ord_id: str,
        label: str,
        submit_fn: Callable[[], OkxOrderResult],
    ) -> OkxOrderResult:
        return self._order_service.submit_order_with_recovery(
            credentials,
            config,
            inst_id=inst_id,
            cl_ord_id=cl_ord_id,
            label=label,
            submit_fn=submit_fn,
        )

    def _does_pending_algo_stop_loss_match(
        self,
        order: OkxTradeOrderItem | None,
        *,
        stop_loss: Decimal,
        trigger_price_type: str,
    ) -> bool:
        if order is None or order.stop_loss_trigger_price != stop_loss:
            return False
        current_type = (order.stop_loss_trigger_price_type or "").strip().lower()
        expected_type = trigger_price_type.strip().lower()
        return not current_type or current_type == expected_type

    def _amend_algo_order_with_recovery(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        algo_id: str | None,
        algo_cl_ord_id: str | None,
        req_id: str,
        new_stop_loss_trigger_price: Decimal,
        new_stop_loss_trigger_price_type: str,
        recover_algo_id: str | None = None,
        recover_algo_cl_ord_id: str | None = None,
    ) -> None:
        if not (algo_id or "").strip() and not (algo_cl_ord_id or "").strip():
            raise ValueError("amend_algo_order 需要 algo_id 或 algo_cl_ord_id")

        def _submit() -> OkxOrderResult:
            return self._client.amend_algo_order(
                credentials,
                environment=config.environment,
                inst_id=trade_instrument.inst_id,
                algo_id=(algo_id or "").strip() or None,
                algo_cl_ord_id=(algo_cl_ord_id or "").strip() or None,
                req_id=req_id,
                new_stop_loss_trigger_price=new_stop_loss_trigger_price,
                new_stop_loss_trigger_price_type=new_stop_loss_trigger_price_type,
            )

        rid = (recover_algo_id or "").strip() or (algo_id or "").strip() or None
        rcid = (recover_algo_cl_ord_id or "").strip() or (algo_cl_ord_id or "").strip() or None
        ref_log = f"algoClOrdId={rcid}" if rcid else f"algoId={rid or '-'}"

        try:
            _submit()
            return
        except OkxApiError as exc:
            if not _is_transient_okx_error(exc):
                raise
            detail = str(exc).strip() or f"code={exc.code or '-'}"
            self._logger(
                " | ".join(
                    [
                        "OKX 动态止损改单响应异常，开始回查算法单状态",
                        f"标的={trade_instrument.inst_id}",
                        ref_log,
                        f"候选止损={format_decimal(new_stop_loss_trigger_price)}",
                        detail,
                    ]
                )
            )
            recovered_order = self._find_pending_algo_order_for_dynamic_stop(
                credentials,
                config,
                trade_instrument=trade_instrument,
                algo_cl_ord_id=rcid,
                algo_id=rid,
            )
            if self._does_pending_algo_stop_loss_match(
                recovered_order,
                stop_loss=new_stop_loss_trigger_price,
                trigger_price_type=new_stop_loss_trigger_price_type,
            ):
                self._logger(
                    " | ".join(
                        [
                            "OKX 动态止损改单响应丢失，但回查显示已生效",
                            f"标的={trade_instrument.inst_id}",
                            ref_log,
                            f"新止损={format_decimal(new_stop_loss_trigger_price)}",
                        ]
                    )
                )
                return

            if self._stop_event.is_set():
                raise RuntimeError(
                    f"OKX 动态止损改单中断，且回查未确认结果 | {ref_log}"
                ) from exc

            self._logger(
                " | ".join(
                    [
                        "OKX 动态止损改单回查未确认，准备使用同一 reqId 补发一次",
                        f"标的={trade_instrument.inst_id}",
                        ref_log,
                        f"reqId={req_id}",
                    ]
                )
            )
            try:
                _submit()
                return
            except OkxApiError as retry_exc:
                recovered_order = self._find_pending_algo_order_for_dynamic_stop(
                    credentials,
                    config,
                    trade_instrument=trade_instrument,
                    algo_cl_ord_id=rcid,
                    algo_id=rid,
                )
                if self._does_pending_algo_stop_loss_match(
                    recovered_order,
                    stop_loss=new_stop_loss_trigger_price,
                    trigger_price_type=new_stop_loss_trigger_price_type,
                ):
                    self._logger(
                        " | ".join(
                            [
                                "OKX 动态止损改单补发响应异常，但回查显示已生效",
                                f"标的={trade_instrument.inst_id}",
                                ref_log,
                                f"新止损={format_decimal(new_stop_loss_trigger_price)}",
                            ]
                        )
                    )
                    return
                raise retry_exc

    def _place_entry_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        size: Decimal,
        pos_side: Literal["long", "short"] | None,
        *,
        cl_ord_id: str | None = None,
        label: str = "开仓报单",
    ) -> OkxOrderResult:
        return self._order_service.place_entry_order(
            credentials,
            config,
            trade_instrument,
            side,
            size,
            pos_side,
            cl_ord_id=cl_ord_id,
            label=label,
        )

    def _place_exit_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        size: Decimal,
        pos_side: Literal["long", "short"] | None,
    ) -> OkxOrderResult:
        return self._order_service.place_exit_order(
            credentials,
            config,
            trade_instrument=trade_instrument,
            side=side,
            size=size,
            pos_side=pos_side,
        )

    def _wait_for_order_fill(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        pos_side: Literal["long", "short"] | None,
        result: OkxOrderResult,
        estimated_entry: Decimal,
    ) -> FilledPosition:
        if not result.ord_id:
            raise RuntimeError("OKX 未返回 ordId，无法确认订单成交情况")

        latest_state = ""
        price_delta_multiplier = _instrument_price_delta_multiplier(trade_instrument)
        for _ in range(12):
            status = self._get_order_with_retry(
                credentials,
                config,
                inst_id=trade_instrument.inst_id,
                ord_id=result.ord_id,
            )
            latest_state = status.state.lower()
            filled_size = status.filled_size or Decimal("0")
            if latest_state == "filled":
                return FilledPosition(
                    ord_id=result.ord_id,
                    cl_ord_id=result.cl_ord_id,
                    inst_id=trade_instrument.inst_id,
                    side=side,
                    close_side="sell" if side == "buy" else "buy",
                    pos_side=pos_side,
                    size=filled_size if filled_size > 0 else status.size or Decimal("0"),
                    entry_price=status.avg_price or status.price or estimated_entry,
                    entry_ts=int(time.time() * 1000),
                    price_delta_multiplier=price_delta_multiplier,
                )
            if latest_state == "partially_filled" and filled_size > 0:
                return FilledPosition(
                    ord_id=result.ord_id,
                    cl_ord_id=result.cl_ord_id,
                    inst_id=trade_instrument.inst_id,
                    side=side,
                    close_side="sell" if side == "buy" else "buy",
                    pos_side=pos_side,
                    size=filled_size,
                    entry_price=status.avg_price or status.price or estimated_entry,
                    entry_ts=int(time.time() * 1000),
                    price_delta_multiplier=price_delta_multiplier,
                )
            if latest_state in {"canceled", "order_failed"}:
                break
            self._stop_event.wait(max(config.poll_seconds / 2, 0.5))

        raise RuntimeError(f"订单未成交，ordId={result.ord_id}，状态={latest_state or 'unknown'}")

    def _next_client_order_id(self, *, role: str) -> str:
        return self._order_service.next_client_order_id(role=role)

    def _cancel_active_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        active_order: ManagedEntryOrder,
        newest_ts: int,
    ) -> CancelActiveOrderResult:
        try:
            result = self._client.cancel_order(
                credentials,
                config,
                inst_id=config.inst_id,
                ord_id=active_order.ord_id,
            )
            self._logger(
                f"{_fmt_ts(newest_ts)} | 新 K 线已确认，撤掉旧挂单 | ordId={result.ord_id or active_order.ord_id}"
            )
            return CancelActiveOrderResult("canceled")
        except OkxApiError as exc:
            detail = str(exc).strip() or f"code={exc.code or '-'}"
            latest_status: OkxOrderStatus | None = None
            recover_message = "OKX 撤单请求响应异常，开始回查订单状态" if _is_transient_okx_error(exc) else "OKX 撤单失败，开始回查订单状态"
            self._logger(
                " | ".join(
                    [
                        recover_message,
                        f"ordId={active_order.ord_id}",
                        detail,
                    ]
                )
            )
            latest_status = self._try_get_order_status_for_write_recovery(
                credentials,
                config,
                inst_id=config.inst_id,
                label="撤单回查",
                ord_id=active_order.ord_id,
            )
            if latest_status is not None and latest_status.state.lower() == "canceled":
                self._logger(f"{_fmt_ts(newest_ts)} | 旧挂单已确认撤单，继续按最新 EMA 重挂。")
                return CancelActiveOrderResult("canceled", latest_status)
            if latest_status is not None and latest_status.state.lower() == "filled":
                return CancelActiveOrderResult("filled", latest_status)
            if latest_status is not None and latest_status.state.lower() == "partially_filled":
                return CancelActiveOrderResult("partially_filled", latest_status)

            if not self._stop_event.is_set() and (
                latest_status is None or latest_status.state.lower() == "live"
            ):
                self._logger(
                    " | ".join(
                        [
                            "OKX 撤单回查未确认完成，准备补发一次撤单",
                            f"ordId={active_order.ord_id}",
                        ]
                    )
                )
                try:
                    result = self._client.cancel_order(
                        credentials,
                        config,
                        inst_id=config.inst_id,
                        ord_id=active_order.ord_id,
                    )
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 新 K 线已确认，撤掉旧挂单 | ordId={result.ord_id or active_order.ord_id}"
                    )
                    return CancelActiveOrderResult("canceled")
                except OkxApiError as retry_exc:
                    detail = str(retry_exc).strip() or f"code={retry_exc.code or '-'}"
                    latest_status = self._try_get_order_status_for_write_recovery(
                        credentials,
                        config,
                        inst_id=config.inst_id,
                        label="撤单回查",
                        ord_id=active_order.ord_id,
                    )
                    if latest_status is not None and latest_status.state.lower() == "canceled":
                        self._logger(f"{_fmt_ts(newest_ts)} | 旧挂单已确认撤单，继续按最新 EMA 重挂。")
                        return CancelActiveOrderResult("canceled", latest_status)
                    if latest_status is not None and latest_status.state.lower() == "filled":
                        return CancelActiveOrderResult("filled", latest_status)
                    if latest_status is not None and latest_status.state.lower() == "partially_filled":
                        return CancelActiveOrderResult("partially_filled", latest_status)

            if latest_status is not None:
                state = latest_status.state.lower()
                if state == "live":
                    self._logger(
                        " | ".join(
                            [
                                "撤单暂未完成，保留旧挂单继续回查",
                                f"ordId={active_order.ord_id}",
                                f"状态={latest_status.state}",
                                detail,
                            ]
                        )
                    )
                    return CancelActiveOrderResult("pending", latest_status)
                self._logger(
                    f"{_fmt_ts(newest_ts)} | 旧挂单状态已变更为 {latest_status.state}，继续按最新 EMA 重挂。"
                )
                return CancelActiveOrderResult("canceled", latest_status)

            try:
                pending_order = self._find_pending_entry_order(
                    credentials,
                    config,
                    inst_id=config.inst_id,
                    ord_id=active_order.ord_id,
                    cl_ord_id=active_order.cl_ord_id,
                )
            except Exception as pending_exc:
                pending_detail = str(pending_exc).strip() or pending_exc.__class__.__name__
                self._logger(
                    " | ".join(
                        [
                            "撤单结果暂未确认，挂单列表回查失败，保留旧挂单继续回查",
                            f"ordId={active_order.ord_id}",
                            pending_detail,
                        ]
                    )
                )
                return CancelActiveOrderResult("pending")

            if pending_order is not None:
                pending_state = (pending_order.state or "live").strip() or "live"
                self._logger(
                    " | ".join(
                        [
                            "撤单结果暂未确认，旧挂单仍在待成交列表，保留继续回查",
                            f"ordId={active_order.ord_id}",
                            f"状态={pending_state}",
                            detail,
                        ]
                    )
                )
                return CancelActiveOrderResult("pending")

            self._logger(
                " | ".join(
                    [
                        "撤单结果暂未确认，挂单列表已找不到旧单，先等待下一轮状态同步",
                        f"ordId={active_order.ord_id}",
                        detail,
                    ]
                )
            )
            return CancelActiveOrderResult("pending")

    def _manage_filled_dynamic_entry(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        active_order: ManagedEntryOrder,
        status: OkxOrderStatus,
        newest_ts: int,
        dynamic_stop_only: bool,
    ) -> None:
        filled_price = status.avg_price or status.price or active_order.entry_reference
        filled_size = status.filled_size or status.size or active_order.size
        ord_id = status.ord_id or active_order.ord_id
        price_delta_multiplier = _instrument_price_delta_multiplier(trade_instrument)
        if config.trader_virtual_stop_loss:
            self._logger(
                f"{_fmt_ts(newest_ts)} | 挂单已成交 | ordId={ord_id} | "
                f"开仓价={_format_notify_price_by_tick_size(filled_price, trade_instrument.tick_size)} | "
                f"数量={_format_size_with_contract_equivalent(trade_instrument, filled_size)}"
            )
            self._notify_trade_fill(
                config,
                title="开仓委托成交",
                symbol=config.inst_id,
                side=active_order.side,
                size=filled_size,
                size_text=_format_notify_size_with_unit(trade_instrument, filled_size),
                price=filled_price,
                tick_size=trade_instrument.tick_size,
                reason="交易员模式已开仓：止损价仅作触发参考，不向 OKX 挂真实止损。",
            )
            position = FilledPosition(
                ord_id=ord_id,
                cl_ord_id=active_order.cl_ord_id,
                inst_id=trade_instrument.inst_id,
                side=active_order.side,
                close_side="sell" if active_order.side == "buy" else "buy",
                pos_side=resolve_open_pos_side(config, active_order.side),
                size=filled_size,
                entry_price=filled_price,
                entry_ts=int(time.time() * 1000),
                price_delta_multiplier=price_delta_multiplier,
            )
            self._monitor_trader_virtual_position(
                credentials,
                config,
                trade_instrument=trade_instrument,
                position=position,
                initial_stop_loss=active_order.stop_loss,
                take_profit=active_order.take_profit,
                dynamic_take_profit_enabled=dynamic_stop_only,
            )
            return
        self._logger(
            f"{_fmt_ts(newest_ts)} | 挂单已成交 | ordId={ord_id} | "
            f"开仓价={_format_notify_price_by_tick_size(filled_price, trade_instrument.tick_size)} | "
            f"数量={_format_size_with_contract_equivalent(trade_instrument, filled_size)}"
        )
        fill_reason = (
            "EMA 动态委托已成交，初始止损已交给 OKX 托管，后续将动态上移"
            if dynamic_stop_only
            else "EMA 动态委托已成交，止盈止损已交给 OKX 托管"
        )
        self._notify_trade_fill(
            config,
            title="开仓委托成交",
            symbol=config.inst_id,
            side=active_order.side,
            size=filled_size,
            size_text=_format_notify_size_with_unit(trade_instrument, filled_size),
            price=filled_price,
            tick_size=trade_instrument.tick_size,
            reason=fill_reason,
        )
        position = FilledPosition(
            ord_id=ord_id,
            cl_ord_id=active_order.cl_ord_id,
            inst_id=trade_instrument.inst_id,
            side=active_order.side,
            close_side="sell" if active_order.side == "buy" else "buy",
            pos_side=resolve_open_pos_side(config, active_order.side),
            size=filled_size,
            entry_price=filled_price,
            entry_ts=int(time.time() * 1000),
            price_delta_multiplier=price_delta_multiplier,
        )
        if dynamic_stop_only:
            self._logger(
                f"初始 OKX 止损已提交 | algoClOrdId={active_order.stop_loss_algo_cl_ord_id or '-'} | "
                f"止损={format_decimal(active_order.stop_loss)} | 启动动态上移监控"
            )
            self._monitor_exchange_dynamic_stop(
                credentials,
                config,
                trade_instrument=trade_instrument,
                position=position,
                initial_stop_loss=active_order.stop_loss,
                stop_loss_algo_cl_ord_id=active_order.stop_loss_algo_cl_ord_id,
            )
        else:
            self._logger("止盈止损已附加到 OKX 主单，开始监控持仓是否结束。")
            self._monitor_exchange_managed_position_until_closed(
                credentials,
                config,
                trade_instrument=trade_instrument,
                position=position,
            )

    def _monitor_trader_virtual_position(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
        position: FilledPosition,
        initial_stop_loss: Decimal,
        take_profit: Decimal,
        dynamic_take_profit_enabled: bool,
    ) -> None:
        direction: Literal["long", "short"] = "long" if position.side == "buy" else "short"
        current_stop_loss = initial_stop_loss
        current_take_profit = take_profit
        next_trigger_r = 2
        risk_per_unit = abs(position.entry_price - initial_stop_loss)
        virtual_loss_logged = False
        monitor_parts = [
            f"交易员虚拟止损监控启动 | 标的={trade_instrument.inst_id}",
            f"触发价格类型={config.tp_sl_trigger_type}",
            f"策略止损={format_decimal(initial_stop_loss)}",
        ]
        if dynamic_take_profit_enabled:
            monitor_parts.extend(
                [
                    "止盈方式=动态",
                    f"2R保本={config.dynamic_two_r_break_even_label()}",
                    f"手续费偏移={config.dynamic_fee_offset_enabled_label()}",
                    f"时间保本={config.time_stop_break_even_enabled_label()}/{config.resolved_time_stop_break_even_bars()}根",
                ]
            )
        else:
            monitor_parts.append(f"固定止盈={format_decimal(take_profit)}")
        self._logger(" | ".join(monitor_parts))

        while not self._stop_event.is_set():
            live_position = self._find_managed_position(credentials, config, trade_instrument, position)
            if live_position is None:
                self._logger("未检测到策略持仓，交易员虚拟止损监控结束。")
                return

            current_price = self._get_trigger_price_with_retry(trade_instrument.inst_id, config.tp_sl_trigger_type)
            if dynamic_take_profit_enabled:
                holding_bars = _holding_bars_live(position.entry_ts, int(time.time() * 1000), config.bar)
                updated_stop_loss, next_trigger_price, updated_trigger_r, moved = _advance_dynamic_stop_live(
                    direction=direction,
                    current_price=current_price,
                    entry_price=position.entry_price,
                    risk_per_unit=risk_per_unit,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    tick_size=trade_instrument.tick_size,
                    two_r_break_even=config.dynamic_two_r_break_even,
                    dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
                    holding_bars=holding_bars,
                    time_stop_break_even_enabled=config.time_stop_break_even_enabled,
                    time_stop_break_even_bars=config.resolved_time_stop_break_even_bars(),
                )
                if moved:
                    current_stop_loss = updated_stop_loss
                    current_take_profit = next_trigger_price
                    next_trigger_r = updated_trigger_r
                    self._logger(
                        f"交易员动态止盈保护价已上移 | 当前价={format_decimal(current_price)} | "
                        f"新保护价={format_decimal(current_stop_loss)} | 下一阶段={next_trigger_r}R | "
                        f"holding_bars={holding_bars}"
                    )
            else:
                _, take_hit = evaluate_local_exit(
                    direction=direction,
                    current_price=current_price,
                    stop_loss=current_stop_loss,
                    take_profit=current_take_profit,
                )
                if take_hit:
                    self._logger(
                        f"交易员固定止盈已触发 | 当前价={format_decimal(current_price)} | "
                        f"止盈={format_decimal(current_take_profit)} | 开始平仓释放额度"
                    )
                    self._close_position(credentials, config, trade_instrument, position, "止盈")
                    return

            stop_hit = current_price <= current_stop_loss if direction == "long" else current_price >= current_stop_loss
            if stop_hit:
                if _is_profit_protecting_stop(
                    direction=direction,
                    entry_price=position.entry_price,
                    stop_loss=current_stop_loss,
                ):
                    self._logger(
                        f"交易员动态止盈保护价触发 | 当前价={format_decimal(current_price)} | "
                        f"保护价={format_decimal(current_stop_loss)} | 开始平仓释放额度"
                    )
                    self._close_position(credentials, config, trade_instrument, position, "动态止盈")
                    return
                if not virtual_loss_logged:
                    self._logger(
                        f"交易员虚拟止损已触发（不平仓） | 当前价={format_decimal(current_price)} | "
                        f"策略止损={format_decimal(current_stop_loss)} | 保留仓位等待后续止盈/人工处理"
                    )
                    virtual_loss_logged = True

            self._stop_event.wait(config.poll_seconds)

    def _log_partial_dynamic_fill_and_stop(
        self,
        active_order: ManagedEntryOrder,
        status: OkxOrderStatus,
        newest_ts: int,
        config: StrategyConfig,
        *,
        trade_instrument: Instrument,
    ) -> None:
        filled_price = status.avg_price or status.price or active_order.entry_reference
        filled_size = status.filled_size or status.size or active_order.size
        ord_id = status.ord_id or active_order.ord_id
        self._logger(
            f"{_fmt_ts(newest_ts)} | 挂单部分成交 | ordId={ord_id} | "
            "为避免重复撤单重挂，策略已停止，请手动检查剩余委托。"
        )
        self._notify_trade_fill(
            config,
            title="开仓委托部分成交",
            symbol=config.inst_id,
            side=active_order.side,
            size=filled_size,
            size_text=_format_notify_size_with_unit(trade_instrument, filled_size),
            price=filled_price,
            tick_size=trade_instrument.tick_size,
            reason="EMA 动态委托出现部分成交，策略停止等待人工处理",
        )

    def _log_strategy_start(
        self,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        message = (
            f"启动策略 | 信号标的={signal_instrument.inst_id} | 下单标的={trade_instrument.inst_id} | "
            f"K线周期={config.bar} | EMA={config.ema_period} | ATR={config.atr_period}"
        )
        if is_dynamic_strategy_id(config.strategy_id):
            message = (
                f"{message} | 趋势EMA={config.trend_ema_period} | "
                f"挂单参考EMA={config.resolved_entry_reference_ema_period()}"
            )
        self._logger(message)

    def _log_local_mode_summary(
        self,
        config: StrategyConfig,
        signal_instrument: Instrument,
        trade_instrument: Instrument,
    ) -> None:
        tp_sl_inst = resolve_tp_sl_inst_id(config, signal_instrument.inst_id, trade_instrument.inst_id)
        self._logger(
            f"运行模式：本地下单 / 本地止盈止损 | 信号标的={signal_instrument.inst_id} | "
            f"下单标的={trade_instrument.inst_id} | 止盈止损触发标的={tp_sl_inst}"
        )
        self._logger(
            f"下单方向模式={format_entry_side_mode(config.entry_side_mode)} | "
            f"止盈止损触发价类型={config.tp_sl_trigger_type}"
        )

    def _log_hourly_debug(
        self,
        inst_id: str,
        ema_period: int,
        *,
        current_bar: str = "",
        trend_ema_period: int = 0,
        big_ema_period: int = 0,
        entry_reference_ema_period: int = 0,
    ) -> None:
        normalized_bar = str(current_bar or "").strip()
        if normalized_bar and normalized_bar.upper() != "1H":
            return
        try:
            hourly_snapshot = self._fetch_hourly_debug_snapshot_with_retry(
                inst_id,
                ema_period=ema_period,
                trend_ema_period=trend_ema_period,
                big_ema_period=big_ema_period,
                entry_reference_ema_period=entry_reference_ema_period,
            )
            self._logger(format_hourly_debug(inst_id, hourly_snapshot, trading_bar=current_bar))
        except Exception as exc:
            if normalized_bar and normalized_bar.upper() != "1H":
                self._logger(f"1H参考调试值获取失败（当前交易周期={normalized_bar}）：{exc}")
            else:
                self._logger(f"1H调试值获取失败：{exc}")

    def _notify_signal(
        self,
        config: StrategyConfig,
        *,
        signal: Literal["long", "short"],
        trigger_symbol: str,
        entry_reference: Decimal,
        tick_size: Decimal | None = None,
        reason: str,
    ) -> None:
        if self._notifier is None:
            return
        entry_reference_text = _format_notify_price_by_tick_size(entry_reference, tick_size)
        reason_for_email = reason
        if config.run_mode == "signal_only":
            reason_for_email = f"{reason}\n{_take_profit_mode_description_for_signal_email(config)}"
        self._notifier.send_signal(
            strategy_name=self._strategy_name,
            config=config,
            signal=signal,
            trigger_symbol=trigger_symbol,
            entry_reference=entry_reference_text,
            reason=reason_for_email,
            api_name=self._api_name,
            session_id=self._session_id,
            trader_id=self._trader_id,
            direction_label=self._direction_label,
            run_mode_label=self._run_mode_label,
        )

    def _notify_trade_fill(
        self,
        config: StrategyConfig,
        *,
        title: str,
        symbol: str,
        side: str,
        size: Decimal,
        size_text: str = "",
        price: Decimal,
        tick_size: Decimal | None = None,
        reason: str,
        trade_pnl: str = "",
    ) -> None:
        if self._notifier is None:
            return
        self._notifier.send_trade_fill(
            strategy_name=self._strategy_name,
            config=config,
            title=title,
            symbol=symbol,
            side=side,
            size=size_text or format_decimal(size),
            price=_format_notify_price_by_tick_size(price, tick_size),
            reason=reason,
            trade_pnl=trade_pnl,
            api_name=self._api_name,
            session_id=self._session_id,
            trader_id=self._trader_id,
            direction_label=self._direction_label,
            run_mode_label=self._run_mode_label,
        )

    @staticmethod
    def _trade_fill_pnl_text_for_close(position: FilledPosition, *, fill_size: Decimal, fill_price: Decimal) -> str:
        matched_size = min(abs(fill_size), abs(position.size))
        if matched_size <= 0:
            return ""
        price_delta_multiplier = (
            position.price_delta_multiplier if position.price_delta_multiplier > 0 else Decimal("1")
        )
        if position.side == "buy":
            pnl = (fill_price - position.entry_price) * matched_size * price_delta_multiplier
        else:
            pnl = (position.entry_price - fill_price) * matched_size * price_delta_multiplier
        return format_decimal(pnl)

    def _notify_error(self, config: StrategyConfig | None, message: str) -> None:
        if self._notifier is None:
            return
        self._notifier.send_error(
            strategy_name=self._strategy_name,
            config=config,
            message=message,
            api_name=self._api_name,
            session_id=self._session_id,
            trader_id=self._trader_id,
            direction_label=self._direction_label,
            run_mode_label=self._run_mode_label,
        )


def can_use_exchange_managed_orders(
    config: StrategyConfig,
    signal_instrument: Instrument,
    trade_instrument: Instrument,
) -> bool:
    return (
        config.tp_sl_mode == "exchange"
        and signal_instrument.inst_id == trade_instrument.inst_id
        and trade_instrument.inst_type == "SWAP"
    )


def supports_fixed_entry_side_mode(strategy_id: str, run_mode: str, tp_sl_mode: str) -> bool:
    normalized_run_mode = str(run_mode or "").strip().lower()
    normalized_tp_sl_mode = str(tp_sl_mode or "").strip().lower()
    return (
        normalized_run_mode == "trade"
        and strategy_id != STRATEGY_EMA5_EMA8_ID
        and normalized_tp_sl_mode == "local_trade"
    )


def fixed_entry_side_mode_support_reason(strategy_id: str, run_mode: str, tp_sl_mode: str) -> str | None:
    if supports_fixed_entry_side_mode(strategy_id, run_mode, tp_sl_mode):
        return None
    normalized_run_mode = str(run_mode or "").strip().lower()
    normalized_tp_sl_mode = str(tp_sl_mode or "").strip().lower()
    if normalized_run_mode != "trade":
        return "只发信号邮件模式不下单，下单方向模式已固定为跟随信号。"
    if strategy_id == STRATEGY_EMA5_EMA8_ID:
        return "EMA5/EMA8 策略当前只支持跟随信号。"
    if normalized_tp_sl_mode == "exchange":
        return "OKX 托管模式当前只支持跟随信号；如需固定买入/固定卖出，请切到按交易标的价格（本地）。"
    if normalized_tp_sl_mode == "local_signal":
        return "按信号标的价格（本地）当前只支持跟随信号；如需固定买入/固定卖出，请切到按交易标的价格（本地）。"
    if normalized_tp_sl_mode == "local_custom":
        return "按自定义标的价格（本地）当前只支持跟随信号；如需固定买入/固定卖出，请切到按交易标的价格（本地）。"
    return "当前模式仅支持跟随信号。"


def validate_entry_side_mode_support(config: StrategyConfig) -> None:
    if config.entry_side_mode == "follow_signal":
        return
    reason = fixed_entry_side_mode_support_reason(config.strategy_id, config.run_mode, config.tp_sl_mode)
    if reason:
        raise RuntimeError(reason)


def resolve_trade_inst_id(config: StrategyConfig) -> str:
    return (config.trade_inst_id or config.inst_id).strip().upper()


def resolve_tp_sl_inst_id(config: StrategyConfig, signal_inst_id: str, trade_inst_id: str) -> str:
    if config.tp_sl_mode in {"exchange", "local_trade"}:
        return trade_inst_id
    if config.tp_sl_mode == "local_signal":
        return signal_inst_id
    trigger_inst_id = (config.local_tp_sl_inst_id or "").strip().upper()
    if not trigger_inst_id:
        raise RuntimeError("已选择自定义本地止盈止损，但没有填写触发标的")
    return trigger_inst_id


def resolve_entry_side(signal: str, entry_side_mode: str) -> Literal["buy", "sell"]:
    if entry_side_mode == "follow_signal":
        return "buy" if signal == "long" else "sell"
    if entry_side_mode == "fixed_buy":
        return "buy"
    if entry_side_mode == "fixed_sell":
        return "sell"
    raise RuntimeError(f"不支持的下单方向模式：{entry_side_mode}")


def resolve_open_pos_side(
    config: StrategyConfig,
    trade_side: Literal["buy", "sell"],
) -> Literal["long", "short"] | None:
    if config.position_mode != "long_short":
        return None
    return "long" if trade_side == "buy" else "short"


def local_entry_trigger_hit(signal: str, current_price: Decimal, target_price: Decimal) -> bool:
    return current_price <= target_price if signal == "long" else current_price >= target_price


def evaluate_local_exit(
    *,
    direction: Literal["long", "short"],
    current_price: Decimal,
    stop_loss: Decimal,
    take_profit: Decimal,
) -> tuple[bool, bool]:
    if direction == "long":
        return current_price <= stop_loss, current_price >= take_profit
    return current_price >= stop_loss, current_price <= take_profit


def _dynamic_two_taker_fee_offset_live(
    entry_price: Decimal,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
    *,
    enabled: bool = True,
) -> Decimal:
    if not enabled or taker_fee_rate <= 0:
        return Decimal("0")
    return abs(entry_price) * taker_fee_rate * Decimal("2")


def _bar_to_milliseconds_live(bar: str) -> int:
    normalized = str(bar or "").strip().lower()
    if normalized.endswith("m") and normalized[:-1].isdigit():
        return int(normalized[:-1]) * 60 * 1000
    if normalized.endswith("h") and normalized[:-1].isdigit():
        return int(normalized[:-1]) * 60 * 60 * 1000
    if normalized.endswith("d") and normalized[:-1].isdigit():
        return int(normalized[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f"不支持的 K 线周期：{bar}")


def _signal_confirmation_ts_ms(candle_ts: int, bar: str) -> int:
    return candle_ts + _bar_to_milliseconds_live(bar)


def _holding_bars_live(entry_ts: int, reference_ts: int, bar: str) -> int:
    if reference_ts <= entry_ts:
        return 0
    return int((reference_ts - entry_ts) // _bar_to_milliseconds_live(bar))


def _time_stop_break_even_price_live(
    *,
    direction: Literal["long", "short"],
    entry_price: Decimal,
    tick_size: Decimal,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
    dynamic_fee_offset_enabled: bool = True,
) -> Decimal:
    fee_offset = _dynamic_two_taker_fee_offset_live(
        entry_price,
        taker_fee_rate,
        enabled=dynamic_fee_offset_enabled,
    )
    raw = entry_price + fee_offset if direction == "long" else entry_price - fee_offset
    rounding = "up" if direction == "long" else "down"
    return snap_to_increment(raw, tick_size, rounding)


def _dynamic_trigger_price_live(
    *,
    direction: Literal["long", "short"],
    entry_price: Decimal,
    risk_per_unit: Decimal,
    trigger_r: int,
    tick_size: Decimal,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
    dynamic_fee_offset_enabled: bool = True,
) -> Decimal:
    distance = (risk_per_unit * Decimal(trigger_r)) + _dynamic_two_taker_fee_offset_live(
        entry_price,
        taker_fee_rate,
        enabled=dynamic_fee_offset_enabled,
    )
    raw = entry_price + distance if direction == "long" else entry_price - distance
    rounding = "up" if direction == "long" else "down"
    return snap_to_increment(raw, tick_size, rounding)


def _dynamic_stop_price_live(
    *,
    direction: Literal["long", "short"],
    entry_price: Decimal,
    risk_per_unit: Decimal,
    trigger_r: int,
    tick_size: Decimal,
    two_r_break_even: bool = False,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
    dynamic_fee_offset_enabled: bool = True,
) -> Decimal:
    lock_multiple = Decimal(max(trigger_r - 1, 0))
    if two_r_break_even and trigger_r == 2:
        lock_multiple = Decimal("0")
    fee_offset = _dynamic_two_taker_fee_offset_live(
        entry_price,
        taker_fee_rate,
        enabled=dynamic_fee_offset_enabled,
    )
    raw = (
        entry_price + (risk_per_unit * lock_multiple) + fee_offset
        if direction == "long"
        else entry_price - (risk_per_unit * lock_multiple) - fee_offset
    )
    rounding = "up" if direction == "long" else "down"
    return snap_to_increment(raw, tick_size, rounding)


def _advance_dynamic_stop_live(
    *,
    direction: Literal["long", "short"],
    current_price: Decimal,
    entry_price: Decimal,
    risk_per_unit: Decimal,
    current_stop_loss: Decimal,
    next_trigger_r: int,
    tick_size: Decimal,
    two_r_break_even: bool = False,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
    dynamic_fee_offset_enabled: bool = True,
    holding_bars: int = 0,
    time_stop_break_even_enabled: bool = False,
    time_stop_break_even_bars: int = 0,
) -> tuple[Decimal, Decimal, int, bool]:
    if risk_per_unit <= 0:
        next_take_profit = _dynamic_trigger_price_live(
            direction=direction,
            entry_price=entry_price,
            risk_per_unit=Decimal("0"),
            trigger_r=next_trigger_r,
            tick_size=tick_size,
            taker_fee_rate=taker_fee_rate,
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        )
        updated_stop = current_stop_loss
        moved = False
        if time_stop_break_even_enabled and time_stop_break_even_bars > 0 and holding_bars >= time_stop_break_even_bars:
            candidate = _time_stop_break_even_price_live(
                direction=direction,
                entry_price=entry_price,
                tick_size=tick_size,
                taker_fee_rate=taker_fee_rate,
                dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
            )
            if direction == "long":
                if current_price >= candidate and candidate > updated_stop:
                    updated_stop = candidate
                    moved = True
            elif current_price <= candidate and candidate < updated_stop:
                updated_stop = candidate
                moved = True
        return updated_stop, next_take_profit, next_trigger_r, moved

    moved = False
    updated_stop = current_stop_loss
    trigger_r = next_trigger_r
    if time_stop_break_even_enabled and time_stop_break_even_bars > 0 and holding_bars >= time_stop_break_even_bars:
        candidate = _time_stop_break_even_price_live(
            direction=direction,
            entry_price=entry_price,
            tick_size=tick_size,
            taker_fee_rate=taker_fee_rate,
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        )
        if direction == "long":
            if current_price >= candidate and candidate > updated_stop:
                updated_stop = candidate
                moved = True
        elif current_price <= candidate and candidate < updated_stop:
            updated_stop = candidate
            moved = True
    while True:
        trigger_price = _dynamic_trigger_price_live(
            direction=direction,
            entry_price=entry_price,
            risk_per_unit=risk_per_unit,
            trigger_r=trigger_r,
            tick_size=tick_size,
            taker_fee_rate=taker_fee_rate,
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        )
        reached = current_price >= trigger_price if direction == "long" else current_price <= trigger_price
        if not reached:
            return updated_stop, trigger_price, trigger_r, moved

        candidate = _dynamic_stop_price_live(
            direction=direction,
            entry_price=entry_price,
            risk_per_unit=risk_per_unit,
            trigger_r=trigger_r,
            tick_size=tick_size,
            two_r_break_even=two_r_break_even,
            taker_fee_rate=taker_fee_rate,
            dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        )
        updated_stop = max(updated_stop, candidate) if direction == "long" else min(updated_stop, candidate)
        trigger_r += 1
        moved = True


def _is_exchange_dynamic_stop_candidate_valid(
    *,
    direction: Literal["long", "short"],
    current_price: Decimal,
    candidate_stop_loss: Decimal,
) -> bool:
    if direction == "long":
        return current_price > candidate_stop_loss
    return current_price < candidate_stop_loss


def _is_profit_protecting_stop(
    *,
    direction: Literal["long", "short"],
    entry_price: Decimal,
    stop_loss: Decimal,
) -> bool:
    if direction == "long":
        return stop_loss >= entry_price
    return stop_loss <= entry_price


def _reset_startup_signal_gate(gate_state: StartupSignalGateState) -> None:
    gate_state.blocked_signal = None


def _should_skip_startup_signal(
    gate_state: StartupSignalGateState,
    *,
    signal: Literal["long", "short"],
    candle_ts: int,
    bar: str,
) -> tuple[bool, str | None]:
    if gate_state.blocked_signal is not None and gate_state.blocked_signal != signal:
        gate_state.blocked_signal = None

    if gate_state.blocked_signal == signal:
        return True, None

    confirmation_ts = _signal_confirmation_ts_ms(candle_ts, bar)
    if confirmation_ts > gate_state.started_at_ms:
        return False, None

    signal_age_seconds = max((gate_state.started_at_ms - confirmation_ts) // 1000, 0)
    window_seconds = max(int(gate_state.chase_window_seconds), 0)
    if window_seconds > 0 and signal_age_seconds <= window_seconds:
        return (
            False,
            f"{_fmt_ts(candle_ts)} | 启动追单窗口内接管当前波段 | 方向={signal.upper()} | "
            f"信号年龄={signal_age_seconds}秒 | 窗口={window_seconds}秒",
        )

    gate_state.blocked_signal = signal
    if window_seconds > 0:
        return (
            True,
            f"{_fmt_ts(candle_ts)} | 启动追单窗口已过期，当前不追单 | 方向={signal.upper()} | "
            f"信号年龄={signal_age_seconds}秒 | 窗口={window_seconds}秒 | 等待当前波失效后再接管",
        )
    return (
        True,
        f"{_fmt_ts(candle_ts)} | 启动默认不追老信号 | 方向={signal.upper()} | 当前波已在启动前确认 | "
        "等待当前波失效后再接管",
    )


def _coerce_okx_read_exception(exc: Exception) -> OkxApiError | None:
    if isinstance(exc, OkxApiError):
        return exc
    detail = str(exc).strip()
    lowered = detail.lower()
    transient_markers = (
        "timeout",
        "timed out",
        "handshake",
        "connection reset",
        "connection aborted",
        "connection refused",
        "eof occurred",
        "remote end closed connection without response",
        "remotedisconnected",
    )
    if any(marker in lowered for marker in transient_markers) and (
        isinstance(exc, OSError)
        or "remote end closed connection without response" in lowered
        or "remotedisconnected" in lowered
    ):
        return OkxApiError(f"网络错误：{detail or exc.__class__.__name__}")
    return None


def _is_transient_okx_error(exc: OkxApiError) -> bool:
    if exc.status is not None:
        return exc.status in {408, 409, 425, 429} or exc.status >= 500
    detail = str(exc).strip().lower()
    transient_markers = (
        "网络错误",
        "timeout",
        "timed out",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "connection refused",
        "handshake",
        "eof occurred",
        "remote end closed connection without response",
        "remotedisconnected",
    )
    return any(marker in detail for marker in transient_markers)

def _is_okx_order_not_found_error(exc: OkxApiError) -> bool:
    detail = str(exc).strip()
    if not detail:
        return False
    normalized = detail.lower()
    return (
        "未返回订单状态" in detail
        or "订单不存在" in detail
        or "order does not exist" in normalized
        or "order not exist" in normalized
    )


def _instrument_price_delta_multiplier(instrument: Instrument) -> Decimal:
    if instrument.inst_type not in {"SWAP", "FUTURES"}:
        return Decimal("1")
    settle_ccy = (instrument.settle_ccy or "").strip().upper()
    if settle_ccy not in {"USDT", "USDC"}:
        return Decimal("1")
    ct_val = instrument.ct_val if instrument.ct_val is not None and instrument.ct_val > 0 else None
    if ct_val is None:
        return Decimal("1")
    ct_mult = instrument.ct_mult if instrument.ct_mult is not None and instrument.ct_mult > 0 else Decimal("1")
    multiplier = ct_val * ct_mult
    return multiplier if multiplier > 0 else Decimal("1")


def _instrument_contract_base_currency(instrument: Instrument) -> str | None:
    candidate = (instrument.ct_val_ccy or "").strip().upper()
    if candidate:
        return candidate
    normalized_inst_id = instrument.inst_id.strip().upper()
    if "-" in normalized_inst_id:
        return normalized_inst_id.split("-", 1)[0]
    return None


def _format_size_with_contract_equivalent(instrument: Instrument, size: Decimal) -> str:
    contract_multiplier = _instrument_price_delta_multiplier(instrument)
    contract_ccy = _instrument_contract_base_currency(instrument)
    if instrument.inst_type in {"SWAP", "FUTURES", "OPTION"} and contract_multiplier > 0 and contract_ccy:
        amount = abs(size) * contract_multiplier
        amount_text = format_decimal(amount)
        if size < 0:
            amount_text = f"-{amount_text}"
        return f"{format_decimal(size)}张（折合{amount_text} {contract_ccy}）"
    return format_decimal(size)


def _format_notify_size_with_unit(instrument: Instrument, size: Decimal) -> str:
    contract_multiplier = _instrument_price_delta_multiplier(instrument)
    contract_ccy = _instrument_contract_base_currency(instrument)
    if instrument.inst_type in {"SWAP", "FUTURES", "OPTION"} and contract_multiplier > 0 and contract_ccy:
        amount = abs(size) * contract_multiplier
        amount_text = format_decimal(amount)
        if size < 0:
            amount_text = f"-{amount_text}"
        return f"{amount_text} {contract_ccy}"
    return format_decimal(size)


def _minimum_order_size_message(
    instrument: Instrument,
    *,
    size: Decimal,
    raw_size: Decimal | None = None,
    risk_per_unit: Decimal | None = None,
) -> str:
    lot_text = _format_size_with_contract_equivalent(instrument, instrument.lot_size)
    min_text = _format_size_with_contract_equivalent(instrument, instrument.min_size)
    snapped_text = _format_size_with_contract_equivalent(instrument, size)
    parts: list[str] = []
    if raw_size is not None:
        parts.append(
            f"按风险金计算原始数量={format_decimal(raw_size)}张，按步长 {lot_text} 向下取整后={snapped_text}"
        )
        if risk_per_unit is not None and risk_per_unit > 0:
            minimum_risk_amount = risk_per_unit * instrument.min_size
            parts.append(f"当前至少需要风险金 {format_decimal(minimum_risk_amount)} 才能下最小一笔")
    else:
        parts.append(f"下单数量 {snapped_text}")
    parts.append(f"小于最小下单量 {min_text}")
    return " | ".join(parts)


def determine_order_size(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    entry_price: Decimal,
    stop_loss: Decimal,
    risk_price_compatible: bool,
) -> Decimal:
    size_raw: Decimal | None = None
    risk_per_unit: Decimal | None = None
    if config.risk_amount is not None and config.risk_amount > 0 and risk_price_compatible:
        risk_per_unit = abs(entry_price - stop_loss) * _instrument_price_delta_multiplier(instrument)
        if risk_per_unit <= 0:
            raise RuntimeError("开仓价与止损价过于接近，无法根据风险金计算数量")
        size_raw = config.risk_amount / risk_per_unit
        size = snap_to_increment(size_raw, instrument.lot_size, "down")
    else:
        if config.order_size <= 0:
            if config.risk_amount is not None and config.risk_amount > 0 and not risk_price_compatible:
                raise RuntimeError("当前止盈止损触发标的与下单标的不同，风险金无法自动换算，请填写固定下单数量")
            raise RuntimeError("固定下单数量必须大于 0")
        size = snap_to_increment(config.order_size, instrument.lot_size, "down")

    if size < instrument.min_size:
        raise OrderSizeTooSmallError(
            _minimum_order_size_message(
                instrument,
                size=size,
                raw_size=size_raw,
                risk_per_unit=risk_per_unit,
            )
        )
    return size


def estimate_trade_entry_price(
    client: OkxRestClient,
    instrument: Instrument,
    side: Literal["buy", "sell"],
) -> Decimal:
    ticker = client.get_ticker(instrument.inst_id)
    if side == "buy":
        candidate = ticker.ask or ticker.last or ticker.bid
    else:
        candidate = ticker.bid or ticker.last or ticker.ask
    if candidate is None or candidate <= 0:
        raise RuntimeError(f"{instrument.inst_id} 当前没有可用盘口价格，无法估算下单价")
    return snap_to_increment(candidate, instrument.tick_size, "nearest")


def fetch_atr_snapshot(
    client: OkxRestClient,
    inst_id: str,
    bar: str,
    atr_period: int,
) -> AtrSnapshot:
    lookback = recommended_indicator_lookback(atr_period)
    candles = client.get_candles(inst_id, bar, limit=lookback)
    confirmed = [candle for candle in candles if candle.confirmed]
    if len(confirmed) < atr_period:
        raise RuntimeError(f"{inst_id} 已收盘 K 线不足，无法计算 ATR{atr_period}")

    atr_values = atr(confirmed, atr_period)
    last_closed_candle = confirmed[-1]
    last_closed_atr = atr_values[-1]
    if last_closed_atr is None:
        raise RuntimeError(f"{inst_id} 的 ATR{atr_period} 尚未准备好")

    return AtrSnapshot(
        candle_ts=last_closed_candle.ts,
        candle_close=last_closed_candle.close,
        atr_value=last_closed_atr,
        lookback_used=lookback,
        confirmed_count=len(confirmed),
    )


def build_protection_plan(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    direction: Literal["long", "short"],
    entry_reference: Decimal,
    atr_value: Decimal,
    candle_ts: int,
    trigger_inst_id: str,
    use_signal_extrema: bool = False,
    signal_candle_high: Decimal | None = None,
    signal_candle_low: Decimal | None = None,
) -> ProtectionPlan:
    if atr_value <= 0:
        raise RuntimeError("ATR 必须大于 0 才能计算止盈止损")

    reference_price = snap_to_increment(entry_reference, instrument.tick_size, "nearest")

    if direction == "long":
        take_profit_raw = reference_price + (atr_value * config.atr_take_multiplier)
        if use_signal_extrema:
            if signal_candle_low is None:
                raise RuntimeError("缺少信号 K 线最低价，无法计算做多止损")
            stop_loss_raw = signal_candle_low - atr_value
        else:
            stop_loss_raw = reference_price - (atr_value * config.atr_stop_multiplier)
        take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "down")
        stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "up")
    else:
        take_profit_raw = reference_price - (atr_value * config.atr_take_multiplier)
        if use_signal_extrema:
            if signal_candle_high is None:
                raise RuntimeError("缺少信号 K 线最高价，无法计算做空止损")
            stop_loss_raw = signal_candle_high + atr_value
        else:
            stop_loss_raw = reference_price + (atr_value * config.atr_stop_multiplier)
        take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "up")
        stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "down")

    if take_profit <= 0 or stop_loss <= 0:
        raise RuntimeError("计算出来的止盈止损价格必须大于 0")

    return ProtectionPlan(
        trigger_inst_id=trigger_inst_id,
        trigger_price_type=config.tp_sl_trigger_type,
        take_profit=take_profit,
        stop_loss=stop_loss,
        entry_reference=reference_price,
        atr_value=atr_value,
        direction=direction,
        candle_ts=candle_ts,
    )


def recommended_indicator_lookback(*periods: int) -> int:
    valid_periods = [period for period in periods if period > 0]
    if not valid_periods:
        raise ValueError("周期参数必须大于 0")
    requested = max(max(valid_periods) * 4, 120)
    return min(requested, OKX_SINGLE_REQUEST_MAX_CANDLES)


def _dynamic_entry_reference_ema_text(config: StrategyConfig) -> str:
    return f"EMA{config.resolved_entry_reference_ema_period()}"


def _decimal_places_for_tick_size(tick_size: Decimal) -> int:
    normalized = tick_size.normalize()
    exponent = normalized.as_tuple().exponent
    return max(-exponent, 0)


def _format_notify_price_by_tick_size(entry_reference: Decimal, tick_size: Decimal | None) -> str:
    if tick_size is None or tick_size <= 0:
        return format_decimal(entry_reference)
    snapped = snap_to_increment(entry_reference, tick_size, "nearest")
    return format_decimal_fixed(snapped, _decimal_places_for_tick_size(tick_size))


def fetch_hourly_ema_debug(
    client: OkxRestClient,
    inst_id: str,
    ema_period: int,
    atr_period: int = DEFAULT_DEBUG_ATR_PERIOD,
    trend_ema_period: int = 0,
    big_ema_period: int = 0,
    entry_reference_ema_period: int = 0,
) -> HourlyDebugSnapshot:
    lookback = recommended_indicator_lookback(
        ema_period,
        atr_period,
        trend_ema_period,
        big_ema_period,
        entry_reference_ema_period,
    )
    candles = client.get_candles(inst_id, "1H", limit=lookback)
    confirmed = [candle for candle in candles if candle.confirmed]
    minimum = max(ema_period, atr_period, trend_ema_period, big_ema_period, entry_reference_ema_period)
    if len(confirmed) < minimum:
        raise RuntimeError(f"已收盘 1 小时 K 线不足，无法计算 EMA{ema_period} / ATR{atr_period}")

    closes = [candle.close for candle in confirmed]
    ema_values = ema(closes, ema_period)
    atr_values = atr(confirmed, atr_period)
    last_closed_candle = confirmed[-1]
    last_closed_ema = ema_values[-1]
    last_closed_atr = atr_values[-1]
    if last_closed_atr is None:
        raise RuntimeError(f"ATR{atr_period} 尚未准备好")

    return HourlyDebugSnapshot(
        candle_ts=last_closed_candle.ts,
        candle_close=last_closed_candle.close,
        ema_value=last_closed_ema,
        ema_period=ema_period,
        atr_value=last_closed_atr,
        atr_period=atr_period,
        lookback_used=lookback,
        confirmed_count=len(confirmed),
    )


def format_hourly_debug(
    inst_id: str,
    snapshot: HourlyDebugSnapshot,
    *,
    trading_bar: str = "",
) -> str:
    normalized_bar = str(trading_bar or "").strip()
    if normalized_bar and normalized_bar.upper() != "1H":
        prefix = f"1H参考调试（当前交易周期={normalized_bar}）"
    else:
        prefix = "1H调试"
    return (
        f"{prefix} | {inst_id} | K线时间={_fmt_ts(snapshot.candle_ts)} | "
        f"上一根收盘价={format_decimal_fixed(snapshot.candle_close, 2)} | "
        f"上一根EMA{snapshot.ema_period}={format_decimal_fixed(snapshot.ema_value, 2)} | "
        f"上一根ATR{snapshot.atr_period}={format_decimal_fixed(snapshot.atr_value, 2)} | "
        f"回看K线数={snapshot.lookback_used} | 已收盘根数={snapshot.confirmed_count}"
    )


def build_order_plan(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    order_size: Decimal | None,
    signal: str,
    entry_reference: Decimal,
    atr_value: Decimal,
    candle_ts: int,
    signal_candle_high: Decimal | None = None,
    signal_candle_low: Decimal | None = None,
) -> OrderPlan:
    protection = build_protection_plan(
        instrument=instrument,
        config=config,
        direction=signal,
        entry_reference=entry_reference,
        atr_value=atr_value,
        candle_ts=candle_ts,
        trigger_inst_id=instrument.inst_id,
        use_signal_extrema=is_ema_atr_breakout_strategy(config.strategy_id),
        signal_candle_high=signal_candle_high,
        signal_candle_low=signal_candle_low,
    )

    side = "buy" if signal == "long" else "sell"
    pos_side = None
    if config.position_mode == "long_short":
        pos_side = "long" if side == "buy" else "short"

    if config.risk_amount is not None and config.risk_amount > 0:
        size = determine_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=True,
        )
    else:
        if order_size is None:
            raise RuntimeError("缺少下单数量，且未设置风险金")
        manual_config = replace(config, order_size=order_size, risk_amount=None)
        size = determine_order_size(
            instrument=instrument,
            config=manual_config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=False,
        )

    return OrderPlan(
        inst_id=instrument.inst_id,
        side=side,
        pos_side=pos_side,
        size=size,
        take_profit=protection.take_profit,
        stop_loss=protection.stop_loss,
        entry_reference=protection.entry_reference,
        atr_value=protection.atr_value,
        signal=signal,
        candle_ts=candle_ts,
        tp_sl_inst_id=instrument.inst_id,
        tp_sl_mode="exchange",
    )


def format_entry_side_mode(entry_side_mode: str) -> str:
    if entry_side_mode == "follow_signal":
        return "跟随信号"
    if entry_side_mode == "fixed_buy":
        return "固定买入"
    if entry_side_mode == "fixed_sell":
        return "固定卖出"
    return entry_side_mode


def _format_signal_mode(signal_mode: str) -> str:
    if signal_mode == "long_only":
        return "只做多"
    if signal_mode == "short_only":
        return "只做空"
    if signal_mode == "both":
        return "双向"
    return signal_mode


def _bar_interval_seconds(bar: str) -> int:
    normalized = bar.strip()
    if len(normalized) < 2:
        raise ValueError(f"不支持的K线周期：{bar}")
    unit = normalized[-1].lower()
    value = int(normalized[:-1])
    if value <= 0:
        raise ValueError(f"不支持的K线周期：{bar}")
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 60 * 60
    if unit == "d":
        return value * 60 * 60 * 24
    raise ValueError(f"不支持的K线周期：{bar}")


def _seconds_until_next_bar_close(bar: str, buffer_seconds: float, *, now_ts: float | None = None) -> float:
    current_ts = time.time() if now_ts is None else now_ts
    interval_seconds = _bar_interval_seconds(bar)
    next_close_ts = ((int(current_ts) // interval_seconds) + 1) * interval_seconds
    wait_seconds = (next_close_ts + float(buffer_seconds)) - current_ts
    return max(wait_seconds, 1.0)


def _idle_signal_wait_seconds(
    bar: str,
    poll_seconds: float,
    *,
    max_wait_seconds: float = IDLE_SIGNAL_MAX_WAIT_SECONDS,
    now_ts: float | None = None,
) -> float:
    base_wait_seconds = max(float(poll_seconds), 1.0)
    try:
        next_check_seconds = _seconds_until_next_bar_close(bar, base_wait_seconds, now_ts=now_ts)
    except ValueError:
        return base_wait_seconds
    return max(base_wait_seconds, min(float(max_wait_seconds), next_check_seconds))


def _fmt_ts(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _coerce_okx_read_exception(exc: Exception) -> OkxApiError | None:
    if isinstance(exc, OkxApiError):
        return exc
    detail = str(exc).strip()
    lowered = detail.lower()
    transient_markers = (
        "network error",
        "timeout",
        "timed out",
        "handshake",
        "ssl",
        "connection reset",
        "connection aborted",
        "connection refused",
        "temporarily unavailable",
        "temporary failure",
        "read timed out",
        "connect timeout",
        "proxy error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "eof occurred",
        "remote end closed connection without response",
        "remotedisconnected",
        "超时",
        "握手",
    )
    if any(marker in lowered for marker in transient_markers):
        return OkxApiError(f"网络错误：{detail or exc.__class__.__name__}")
    return None


def _is_transient_okx_error(exc: OkxApiError) -> bool:
    if exc.status is not None:
        return exc.status in {408, 409, 425, 429} or exc.status >= 500
    detail = str(exc).strip().lower()
    transient_markers = (
        "网络错误",
        "network error",
        "timeout",
        "timed out",
        "temporarily unavailable",
        "temporary failure",
        "connection reset",
        "connection aborted",
        "connection refused",
        "handshake",
        "ssl",
        "eof occurred",
        "remote end closed connection without response",
        "remotedisconnected",
        "read timed out",
        "connect timeout",
        "proxy error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
        "未返回有效触发价",
        "缺少标记价格",
        "缺少最新成交价",
        "缺少指数价格",
        "无法触发",
        "握手",
        "超时",
    )
    return any(marker in detail for marker in transient_markers)
