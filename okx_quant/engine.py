from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Callable, Literal

from okx_quant.indicators import atr, ema
from okx_quant.models import Credentials, Instrument, OrderPlan, ProtectionPlan, StrategyConfig
from okx_quant.notifications import EmailNotifier
from okx_quant.okx_client import OkxApiError, OkxOrderResult, OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_cross_ema_stop import EmaCrossEmaStopStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import (
    STRATEGY_CROSS_ID,
    STRATEGY_DYNAMIC_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_DYNAMIC_SHORT_ID,
    STRATEGY_EMA5_EMA8_ID,
    is_dynamic_strategy_id,
    resolve_dynamic_signal_mode,
)


Logger = Callable[[str], None]
OKX_SINGLE_REQUEST_MAX_CANDLES = 300
DEFAULT_DEBUG_ATR_PERIOD = 10
LIVE_DYNAMIC_MAKER_FEE_RATE = Decimal("0.00015")
LIVE_DYNAMIC_TAKER_FEE_RATE = Decimal("0.00036")


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
    candle_ts: int
    entry_reference: Decimal
    size: Decimal
    side: Literal["buy", "sell"]
    signal: Literal["long", "short"]


@dataclass(frozen=True)
class FilledPosition:
    ord_id: str
    inst_id: str
    side: Literal["buy", "sell"]
    close_side: Literal["buy", "sell"]
    pos_side: Literal["long", "short"] | None
    size: Decimal
    entry_price: Decimal


@dataclass(frozen=True)
class LocalSignalTrigger:
    signal: Literal["long", "short"]
    entry_reference: Decimal
    atr_value: Decimal
    candle_ts: int
    signal_candle_high: Decimal | None
    signal_candle_low: Decimal | None


class StrategyEngine:
    def __init__(
        self,
        client: OkxRestClient,
        logger: Logger,
        *,
        notifier: EmailNotifier | None = None,
        strategy_name: str = "Strategy",
    ) -> None:
        self._client = client
        self._logger = logger
        self._notifier = notifier
        self._strategy_name = strategy_name
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self, credentials: Credentials, config: StrategyConfig) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("策略已经在运行中")
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run,
                args=(credentials, config),
                daemon=True,
                name=f"okx-{config.strategy_id}",
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run(self, credentials: Credentials, config: StrategyConfig) -> None:
        try:
            signal_instrument = self._client.get_instrument(config.inst_id)
            if signal_instrument.state.lower() != "live":
                raise RuntimeError(f"{signal_instrument.inst_id} 当前不可交易，状态：{signal_instrument.state}")

            if config.run_mode == "signal_only":
                if is_dynamic_strategy_id(config.strategy_id):
                    self._run_dynamic_signal_only_v2(config, signal_instrument)
                elif config.strategy_id == STRATEGY_CROSS_ID:
                    self._run_cross_signal_only(config, signal_instrument)
                elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
                    self._run_ema5_ema8_signal_only(config, signal_instrument)
                else:
                    raise RuntimeError(f"未知策略：{config.strategy_id}")

                return

            trade_inst_id = resolve_trade_inst_id(config)
            trade_instrument = self._client.get_instrument(trade_inst_id)
            if trade_instrument.state.lower() != "live":
                raise RuntimeError(f"{trade_instrument.inst_id} 当前不可交易，状态：{trade_instrument.state}")
            if trade_instrument.inst_type == "SPOT":
                raise RuntimeError("当前版本只支持永续或期权下单，现货暂时仅支持作为触发价格来源")

            if is_dynamic_strategy_id(config.strategy_id):
                self._run_dynamic_local_strategy_v2(credentials, config, signal_instrument, trade_instrument)
            elif config.strategy_id == STRATEGY_CROSS_ID:
                if can_use_exchange_managed_orders(config, signal_instrument, trade_instrument):
                    self._run_cross_exchange_strategy(credentials, config, signal_instrument)
                else:
                    self._run_cross_local_strategy(credentials, config, signal_instrument, trade_instrument)
            elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
                self._run_ema5_ema8_local_strategy(credentials, config, signal_instrument, trade_instrument)
            else:
                raise RuntimeError(f"未知策略：{config.strategy_id}")
        except Exception as exc:
            self._notify_error(config, str(exc))
            self._logger(f"策略停止，原因：{exc}")
        finally:
            self._stop_event.set()

    def _run_dynamic_exchange_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        if config.signal_mode == "both":
            raise RuntimeError("EMA 动态委托策略不支持双向，请选择只做多或只做空")
        if config.risk_amount is None or config.risk_amount <= 0:
            raise RuntimeError("风险金必须大于 0")

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

        self._log_strategy_start(config, instrument, instrument)
        self._logger("运行模式：同标的永续下单，止盈止损交给 OKX 托管")
        self._logger(
            f"策略规则：以上一根已收盘 K 线的 {_dynamic_entry_reference_ema_text(config)} 作为开仓价直接挂限价单。"
            f"每根新 K 线确认后，撤掉旧单，再按最新上一根 {_dynamic_entry_reference_ema_text(config)} 重新挂单。"
        )
        self._logger(
            f"方向={_format_signal_mode(config.signal_mode)} | 风险金={format_decimal(config.risk_amount)} | "
            f"止损ATR倍数={format_decimal(config.atr_stop_multiplier)} | "
            f"止盈ATR倍数={format_decimal(config.atr_take_multiplier)}"
        )
        self._logger(f"指标回看数量：{lookback} 根 K 线")

        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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
                status = self._client.get_order(
                    credentials,
                    config,
                    inst_id=config.inst_id,
                    ord_id=active_order.ord_id,
                )
                state = status.state.lower()
                if state == "filled":
                    filled_price = status.avg_price or status.price or active_order.entry_reference
                    filled_size = status.filled_size or active_order.size
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 挂单已成交 | ordId={status.ord_id} | "
                        f"开仓价={format_decimal(filled_price)} | 数量={format_decimal(filled_size)}"
                    )
                    self._notify_trade_fill(
                        config,
                        title="开仓委托成交",
                        symbol=config.inst_id,
                        side=active_order.side,
                        size=filled_size,
                        price=filled_price,
                        reason="EMA 动态委托已成交，止盈止损已交给 OKX 托管",
                    )
                    self._logger("止盈止损已附加到 OKX 主单，本次策略结束。")
                    return
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
                        price=filled_price,
                        reason="EMA 动态委托出现部分成交，策略停止等待人工处理",
                    )
                    return
                if state not in {"live"}:
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 检测到挂单状态已变更为 {status.state}，准备重新同步挂单。"
                    )
                    active_order = None

            candle_changed = newest_ts != last_candle_ts
            if active_order is not None and candle_changed:
                self._cancel_active_order(credentials, config, active_order, newest_ts)
                active_order = None

            should_place_order = candle_changed or active_order is None
            if not should_place_order:
                self._stop_event.wait(config.poll_seconds)
                continue

            decision = strategy.evaluate(confirmed, config)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成挂单 | {decision.reason}")
                last_candle_ts = newest_ts
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成挂单计划")

            plan = build_order_plan(
                instrument=instrument,
                config=config,
                order_size=None,
                signal=decision.signal,
                entry_reference=decision.entry_reference,
                atr_value=decision.atr_value,
                candle_ts=decision.candle_ts,
                signal_candle_high=decision.signal_candle_high,
                signal_candle_low=decision.signal_candle_low,
            )
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 准备挂单 | 方向={plan.signal.upper()} | "
                f"开仓价={format_decimal(plan.entry_reference)} | 数量={format_decimal(plan.size)} | "
                f"止损={format_decimal(plan.stop_loss)} | 止盈={format_decimal(plan.take_profit)}"
            )
            result = self._client.place_limit_order(credentials, config, plan)
            if not result.ord_id:
                raise RuntimeError("OKX 未返回挂单 ordId，无法继续监控该委托")
            active_order = ManagedEntryOrder(
                ord_id=result.ord_id,
                candle_ts=plan.candle_ts,
                entry_reference=plan.entry_reference,
                size=plan.size,
                side=plan.side,
                signal=plan.signal,
            )
            last_candle_ts = newest_ts
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 挂单已提交到 OKX | ordId={result.ord_id or '-'} | "
                f"sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            )
            self._stop_event.wait(config.poll_seconds)

    def _run_cross_exchange_strategy(
        self,
        credentials: Credentials,
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

        self._log_strategy_start(config, instrument, instrument)
        self._logger("运行模式：同标的永续下单，止盈止损交给 OKX 托管")
        self._logger("策略规则：最近一根已收盘 K 线上穿 EMA 做多，下穿 EMA 做空。")
        self._logger(
            f"方向={_format_signal_mode(config.signal_mode)} | 风险金={format_decimal(config.risk_amount or Decimal('0'))}"
        )
        self._logger(f"指标回看数量：{lookback} 根 K 线")

        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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

            decision = strategy.evaluate(confirmed, config)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成下单计划")

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
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 准备市价单 | 方向={plan.signal.upper()} | "
                f"参考入场价={format_decimal(plan.entry_reference)} | 数量={format_decimal(plan.size)} | "
                f"止损={format_decimal(plan.stop_loss)} | 止盈={format_decimal(plan.take_profit)}"
            )
            result = self._client.place_market_order(credentials, config, plan)
            self._logger(
                f"订单已提交到 OKX | ordId={result.ord_id or '-'} | "
                f"sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            )
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
                f"方向={filled.side.upper()} | 成交均价={format_decimal(filled.entry_price)} | "
                f"成交数量={format_decimal(filled.size)}"
            )
            self._notify_trade_fill(
                config,
                title="开仓成交",
                symbol=instrument.inst_id,
                side=filled.side,
                size=filled.size,
                price=filled.entry_price,
                reason="EMA 穿越市价信号成交，止盈止损已交给 OKX 托管",
            )
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
        self._logger("策略规则：最近一根已收盘 K 线上穿 EMA 做多，下穿 EMA 做空，信号出现后立即对下单标的开仓。")
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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

            decision = strategy.evaluate(confirmed, config)
            if decision.signal is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

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

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._log_local_mode_summary(config, signal_instrument, trade_instrument)
        self._logger(
            f"策略规则：根据上一根已收盘 K 线的 {_dynamic_entry_reference_ema_text(config)} 生成动态委托价，不再直接往 OKX 挂单，"
            f"而是在本地轮询信号标的价格，触碰 {_dynamic_entry_reference_ema_text(config)} 后立即对下单标的开仓。"
        )
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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
            if newest_ts != last_candle_ts or active_trigger is None:
                decision = strategy.evaluate(confirmed, config)
                last_candle_ts = newest_ts
                if decision.signal is None:
                    active_trigger = None
                    self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成动态开仓价 | {decision.reason}")
                    self._stop_event.wait(config.poll_seconds)
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
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 动态等待中 | 信号方向={decision.signal.upper()} | "
                    f"信号标的触发价={format_decimal(decision.entry_reference)} | "
                    f"下单标的={trade_instrument.inst_id}"
                )

            current_signal_price = self._client.get_trigger_price(config.inst_id, "last")
            if not local_entry_trigger_hit(active_trigger.signal, current_signal_price, active_trigger.entry_reference):
                self._stop_event.wait(config.poll_seconds)
                continue

            self._logger(
                f"{_fmt_ts(active_trigger.candle_ts)} | 信号标的已触发动态开仓条件 | "
                f"当前价={format_decimal(current_signal_price)} | 目标价={format_decimal(active_trigger.entry_reference)}"
            )
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
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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

            decision = strategy.evaluate(confirmed, config)
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
        current_wave_signal: Literal["long", "short"] | None = None
        entries_in_current_wave = 0
        current_wave_index = 0

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
        ]
        if config.take_profit_mode == "dynamic":
            mode_parts.append(f"2R保本={config.dynamic_two_r_break_even_label()}")
        self._logger(" | ".join(mode_parts))
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period, config.atr_period, entry_reference_ema_period)
            if len(confirmed) < minimum:
                self._logger("已收盘 K 线数量不足，继续等待更多数据...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts != last_candle_ts or active_trigger is None:
                decision = strategy.evaluate(confirmed, replace(config, signal_mode=effective_signal_mode))
                last_candle_ts = newest_ts
                if decision.signal is None:
                    active_trigger = None
                    current_wave_signal = None
                    entries_in_current_wave = 0
                    self._logger(f"{_fmt_ts(newest_ts)} | 当前无法生成动态开仓价 | {decision.reason}")
                    self._stop_event.wait(config.poll_seconds)
                    continue
                if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                    raise RuntimeError("策略返回的数据不完整，无法生成本地触发条件。")

                if current_wave_signal != decision.signal:
                    current_wave_signal = decision.signal
                    entries_in_current_wave = 0
                    current_wave_index += 1
                    self._logger(
                        f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开始 | 方向={decision.signal.upper()}"
                    )

                if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
                    active_trigger = None
                    self._logger(
                        f"{_fmt_ts(decision.candle_ts)} | 第{current_wave_index}波趋势开仓次数已达上限 | 方向={decision.signal.upper()} | "
                        f"上限={config.max_entries_per_trend}"
                    )
                    self._stop_event.wait(config.poll_seconds)
                    continue

                active_trigger = LocalSignalTrigger(
                    signal=decision.signal,
                    entry_reference=decision.entry_reference,
                    atr_value=decision.atr_value,
                    candle_ts=decision.candle_ts,
                    signal_candle_high=decision.signal_candle_high,
                    signal_candle_low=decision.signal_candle_low,
                )
                self._logger(
                    f"{_fmt_ts(decision.candle_ts)} | 动态等待中 | 信号方向={decision.signal.upper()} | "
                    f"第{current_wave_index}波 | 本波第{entries_in_current_wave + 1}次委托 | 触发价={format_decimal(decision.entry_reference)} | "
                    f"下单标的={trade_instrument.inst_id}"
                )

            if active_trigger is None:
                self._stop_event.wait(config.poll_seconds)
                continue

            current_signal_price = self._client.get_trigger_price(config.inst_id, "last")
            if not local_entry_trigger_hit(active_trigger.signal, current_signal_price, active_trigger.entry_reference):
                self._stop_event.wait(config.poll_seconds)
                continue

            self._logger(
                f"{_fmt_ts(active_trigger.candle_ts)} | 信号标的已触发动态开仓条件 | 当前价={format_decimal(current_signal_price)} | "
                f"目标价={format_decimal(active_trigger.entry_reference)}"
            )
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

        self._logger(f"启动信号监控 | 策略={self._strategy_name} | 标的={instrument.inst_id} | K线周期={config.bar}")
        mode_parts = [
            "运行模式：只监控信号，不下单",
            f"方向={_format_signal_mode(effective_signal_mode)}",
            f"止盈方式={'动态止盈' if config.take_profit_mode == 'dynamic' else '固定止盈'}",
            f"每波最多开仓次数={config.max_entries_per_trend or 0}",
            f"挂单参考={_dynamic_entry_reference_ema_text(config)}",
        ]
        if config.take_profit_mode == "dynamic":
            mode_parts.append(f"2R保本={config.dynamic_two_r_break_even_label()}")
        self._logger(" | ".join(mode_parts))
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            entry_reference_ema_period=entry_reference_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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

            decision = strategy.evaluate(confirmed, replace(config, signal_mode=effective_signal_mode))
            if decision.signal is None:
                current_wave_signal = None
                entries_in_current_wave = 0
                self._logger(f"{_fmt_ts(newest_ts)} | 当前无动态委托信号 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
                raise RuntimeError("策略返回的数据不完整，无法生成信号提醒。")

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
        self._logger("运行模式：只监控信号，不下单；当 EMA 穿越信号出现时发送邮件通知。")
        self._log_hourly_debug(
            config.inst_id,
            config.ema_period,
            trend_ema_period=config.trend_ema_period,
            big_ema_period=config.big_ema_period,
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
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

            decision = strategy.evaluate(confirmed, config)
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
                "EMA 穿越信号已确认"
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

        self._logger(f"鍚姩淇″彿鐩戞帶 | 绛栫暐={self._strategy_name} | 鏍囩殑={instrument.inst_id} | K绾垮懆鏈?{config.bar}")
        self._logger(
            f"杩愯妯″紡锛氬彧鐩戞帶淇″彿锛屼笉涓嬪崟锛?EMA{config.ema_period}/EMA{config.trend_ema_period} "
            "鍑虹幇閲戝弶姝诲弶鏃跺彂閫侀偖浠堕€氱煡銆?"
        )

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period) + 1
            if len(confirmed) < minimum:
                self._logger("宸叉敹鐩?K 绾挎暟閲忎笉瓒筹紝缁х画绛夊緟鏇村鏁版嵁...")
                self._stop_event.wait(config.poll_seconds)
                continue

            newest_ts = confirmed[-1].ts
            if newest_ts == last_candle_ts:
                self._stop_event.wait(config.poll_seconds)
                continue
            last_candle_ts = newest_ts

            decision = strategy.evaluate(confirmed, config)
            if decision.signal is None or decision.entry_reference is None or decision.ema_value is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 褰撳墠鏃?EMA5/EMA8 浜ゅ弶淇″彿 | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            reason = (
                f"EMA{config.ema_period}/EMA{config.trend_ema_period} 浜ゅ弶淇″彿 | "
                f"EMA{config.trend_ema_period}={format_decimal(decision.ema_value)}"
            )
            self._logger(
                f"{_fmt_ts(decision.candle_ts or newest_ts)} | 淇″彿瑙﹀彂 | 鏂瑰悜={decision.signal.upper()} | "
                f"鍏ュ満鍙傝€冧环={format_decimal(decision.entry_reference)} | {reason}"
            )
            self._notify_signal(
                config,
                signal=decision.signal,
                trigger_symbol=instrument.inst_id,
                entry_reference=decision.entry_reference,
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
            raise RuntimeError("4H EMA5/EMA8 绛栫暐鐩墠鍙敮鎸佷俊鍙锋爣鐨勪笌涓嬪崟鏍囩殑鐩稿悓")

        strategy = EmaCrossEmaStopStrategy()
        lookback = recommended_indicator_lookback(config.ema_period, config.trend_ema_period)
        last_candle_ts: int | None = None
        active_position: FilledPosition | None = None
        active_signal: Literal["long", "short"] | None = None

        self._log_strategy_start(config, signal_instrument, trade_instrument)
        self._logger(
            f"杩愯妯″紡锛?4H EMA{config.ema_period}/EMA{config.trend_ema_period} 浜ゅ弶寮€浠?+ EMA{config.trend_ema_period} 鍔ㄦ€佹鎹? "
            f"| 淇″彿鏍囩殑={signal_instrument.inst_id}"
        )
        self._logger(f"椋庨櫓閲?{format_decimal(config.risk_amount or Decimal('100'))} | 淇″彿鏂瑰悜={config.signal_mode}")

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.trend_ema_period) + 1
            if len(confirmed) < minimum:
                self._logger("宸叉敹鐩?K 绾挎暟閲忎笉瓒筹紝缁х画绛夊緟鏇村鏁版嵁...")
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
                    f"{_fmt_ts(stop_candle.ts)} | 鎸佷粨鐩戞帶 | 鏂瑰悜={active_signal.upper()} | "
                    f"褰撳墠鏀剁洏={format_decimal(stop_candle.close)} | EMA{config.trend_ema_period}={format_decimal(stop_line)}"
                )
                if stop_hit:
                    self._logger(
                        f"{_fmt_ts(stop_candle.ts)} | EMA{config.trend_ema_period} 鍔ㄦ€佹鎹熻Е鍙? | "
                        f"褰撳墠鏀剁洏={format_decimal(stop_candle.close)} | 鍔ㄦ€佹鎹熺嚎={format_decimal(stop_line)}"
                    )
                    self._close_position(credentials, config, trade_instrument, active_position, "姝㈡崯")
                    active_position = None
                    active_signal = None
                self._stop_event.wait(config.poll_seconds)
                continue

            decision = strategy.evaluate(confirmed, config)
            if decision.signal is None or decision.entry_reference is None or decision.ema_value is None:
                self._logger(f"{_fmt_ts(newest_ts)} | 褰撳墠鏃?EMA5/EMA8 寮€浠撲俊鍙? | {decision.reason}")
                self._stop_event.wait(config.poll_seconds)
                continue

            active_position = self._open_ema_stop_position(
                credentials,
                config,
                trade_instrument=trade_instrument,
                signal=decision.signal,
                stop_loss=current_stop_line,
                signal_candle_ts=decision.candle_ts or newest_ts,
            )
            active_signal = decision.signal
            self._logger(
                f"{_fmt_ts(decision.candle_ts or newest_ts)} | 鍔ㄦ€?EMA 姝㈡崯绛栫暐宸插紑浠? | "
                f"鏂瑰悜={decision.signal.upper()} | EMA{config.trend_ema_period} 姝㈡崯绾?{format_decimal(current_stop_line)}"
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
        price_for_size = estimate_trade_entry_price(self._client, trade_instrument, trade_side)
        stop_price = snap_to_increment(stop_loss, trade_instrument.tick_size, "nearest")
        size = determine_order_size(
            instrument=trade_instrument,
            config=config,
            entry_price=price_for_size,
            stop_loss=stop_price,
            risk_price_compatible=True,
        )
        self._logger(
            f"{_fmt_ts(signal_candle_ts)} | 鍑嗗涓嬪崟 | 鏂瑰悜={signal.upper()} | "
            f"棰勪及鍏ュ満浠?{format_decimal(price_for_size)} | EMA姝㈡崯绾?{format_decimal(stop_price)} | "
            f"鏁伴噺={format_decimal(size)}"
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
            f"EMA 浜ゅ弶涓嬪崟鎴愪氦 | ordId={filled.ord_id} | 鏍囩殑={trade_instrument.inst_id} | "
            f"鏂瑰悜={trade_side.upper()} | 鎴愪氦鍧囦环={format_decimal(filled.entry_price)} | "
            f"鎴愪氦鏁伴噺={format_decimal(filled.size)}"
        )
        self._notify_trade_fill(
            config,
            title="寮€浠撴垚浜?",
            symbol=trade_instrument.inst_id,
            side=trade_side,
            size=filled.size,
            price=filled.entry_price,
            reason=f"EMA{config.ema_period}/EMA{config.trend_ema_period} 浜ゅ弶淇″彿鎴愪氦",
        )
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
        price_for_size = estimate_trade_entry_price(self._client, trade_instrument, trade_side)

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
            f"预估入场价={format_decimal(price_for_size)} | 数量={format_decimal(size)}"
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
            f"方向={trade_side.upper()} | 成交均价={format_decimal(filled.entry_price)} | "
            f"成交数量={format_decimal(filled.size)}"
        )
        self._notify_trade_fill(
            config,
            title="开仓成交",
            symbol=trade_instrument.inst_id,
            side=trade_side,
            size=filled.size,
            price=filled.entry_price,
            reason="本地下单成交",
        )
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
                use_signal_extrema=config.strategy_id == STRATEGY_CROSS_ID,
                signal_candle_high=signal_candle_high,
                signal_candle_low=signal_candle_low,
            )

        if mode in {"exchange", "local_trade"}:
            snapshot = fetch_atr_snapshot(self._client, trade_instrument.inst_id, config.bar, config.atr_period)
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
            custom_instrument = self._client.get_instrument(trigger_inst_id)
            snapshot = fetch_atr_snapshot(self._client, custom_instrument.inst_id, config.bar, config.atr_period)
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
        dynamic_take_profit_enabled = is_dynamic_strategy_id(config.strategy_id) and config.take_profit_mode == "dynamic"
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
        self._logger(" | ".join(monitor_parts))
        while not self._stop_event.is_set():
            current_price = self._client.get_trigger_price(protection.trigger_inst_id, protection.trigger_price_type)
            if dynamic_take_profit_enabled:
                updated_stop_loss, updated_take_profit, updated_trigger_r, moved = _advance_dynamic_stop_live(
                    direction=protection.direction,
                    current_price=current_price,
                    entry_price=position.entry_price,
                    risk_per_unit=risk_per_unit,
                    current_stop_loss=current_stop_loss,
                    next_trigger_r=next_trigger_r,
                    tick_size=trade_instrument.tick_size,
                    two_r_break_even=config.dynamic_two_r_break_even,
                )
                if moved:
                    current_stop_loss = updated_stop_loss
                    current_take_profit = updated_take_profit
                    next_trigger_r = updated_trigger_r
                    self._logger(
                        f"鍔ㄦ€佹鐩堝凡鏇存柊 | 瑙﹀彂浠?{format_decimal(current_price)} | "
                        f"鏂版鎹?{format_decimal(current_stop_loss)} | 涓嬩竴闃舵={next_trigger_r}R"
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
                    f"止损={format_decimal(protection.stop_loss)} | 止盈={format_decimal(protection.take_profit)}"
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
                estimated_entry=estimate_trade_entry_price(self._client, trade_instrument, position.close_side),
            )
            remaining -= filled.size
            self._logger(
                f"本地{reason}平仓已成交 | ordId={filled.ord_id} | 标的={trade_instrument.inst_id} | "
                f"方向={position.close_side.upper()} | 成交均价={format_decimal(filled.entry_price)} | "
                f"成交数量={format_decimal(filled.size)} | 剩余={format_decimal(max(remaining, Decimal('0')))}"
            )
            self._notify_trade_fill(
                config,
                title=f"{reason}平仓成交",
                symbol=trade_instrument.inst_id,
                side=position.close_side,
                size=filled.size,
                price=filled.entry_price,
                reason=f"本地{reason}触发后平仓成交",
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

    def _place_entry_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        trade_instrument: Instrument,
        side: Literal["buy", "sell"],
        size: Decimal,
        pos_side: Literal["long", "short"] | None,
    ) -> OkxOrderResult:
        if trade_instrument.inst_type == "OPTION":
            return self._client.place_aggressive_limit_order(
                credentials,
                config,
                trade_instrument,
                side=side,
                size=size,
                pos_side=pos_side,
            )
        return self._client.place_simple_order(
            credentials,
            config,
            inst_id=trade_instrument.inst_id,
            side=side,
            size=size,
            ord_type="market",
            pos_side=pos_side,
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
        return self._place_entry_order(credentials, config, trade_instrument, side, size, pos_side)

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
        for _ in range(12):
            status = self._client.get_order(
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
                    inst_id=trade_instrument.inst_id,
                    side=side,
                    close_side="sell" if side == "buy" else "buy",
                    pos_side=pos_side,
                    size=filled_size if filled_size > 0 else status.size or Decimal("0"),
                    entry_price=status.avg_price or status.price or estimated_entry,
                )
            if latest_state == "partially_filled" and filled_size > 0:
                return FilledPosition(
                    ord_id=result.ord_id,
                    inst_id=trade_instrument.inst_id,
                    side=side,
                    close_side="sell" if side == "buy" else "buy",
                    pos_side=pos_side,
                    size=filled_size,
                    entry_price=status.avg_price or status.price or estimated_entry,
                )
            if latest_state in {"canceled", "order_failed"}:
                break
            self._stop_event.wait(max(config.poll_seconds / 2, 0.5))

        raise RuntimeError(f"订单未成交，ordId={result.ord_id}，状态={latest_state or 'unknown'}")

    def _cancel_active_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        active_order: ManagedEntryOrder,
        newest_ts: int,
    ) -> None:
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
        except OkxApiError as exc:
            latest_status = self._client.get_order(
                credentials,
                config,
                inst_id=config.inst_id,
                ord_id=active_order.ord_id,
            )
            state = latest_status.state.lower()
            if state == "filled":
                raise RuntimeError(f"旧挂单在撤单前已成交，ordId={active_order.ord_id}") from exc
            if state == "partially_filled":
                raise RuntimeError(
                    f"旧挂单在撤单前已部分成交，ordId={active_order.ord_id}，请手动检查剩余委托"
                ) from exc
            if state == "canceled":
                self._logger(f"{_fmt_ts(newest_ts)} | 旧挂单已是撤单状态，继续按最新 EMA 重挂。")
                return
            raise RuntimeError(f"撤单失败：{exc}") from exc

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
        trend_ema_period: int = 0,
        big_ema_period: int = 0,
        entry_reference_ema_period: int = 0,
    ) -> None:
        try:
            hourly_snapshot = fetch_hourly_ema_debug(
                self._client,
                inst_id,
                ema_period=ema_period,
                trend_ema_period=trend_ema_period,
                big_ema_period=big_ema_period,
                entry_reference_ema_period=entry_reference_ema_period,
            )
            self._logger(format_hourly_debug(inst_id, hourly_snapshot))
        except Exception as exc:
            self._logger(f"1小时调试值获取失败：{exc}")

    def _notify_signal(
        self,
        config: StrategyConfig,
        *,
        signal: Literal["long", "short"],
        trigger_symbol: str,
        entry_reference: Decimal,
        reason: str,
    ) -> None:
        if self._notifier is None:
            return
        self._notifier.send_signal(
            strategy_name=self._strategy_name,
            config=config,
            signal=signal,
            trigger_symbol=trigger_symbol,
            entry_reference=format_decimal(entry_reference),
            reason=reason,
        )

    def _notify_trade_fill(
        self,
        config: StrategyConfig,
        *,
        title: str,
        symbol: str,
        side: str,
        size: Decimal,
        price: Decimal,
        reason: str,
    ) -> None:
        if self._notifier is None:
            return
        self._notifier.send_trade_fill(
            strategy_name=self._strategy_name,
            config=config,
            title=title,
            symbol=symbol,
            side=side,
            size=format_decimal(size),
            price=format_decimal(price),
            reason=reason,
        )

    def _notify_error(self, config: StrategyConfig | None, message: str) -> None:
        if self._notifier is None:
            return
        self._notifier.send_error(
            strategy_name=self._strategy_name,
            config=config,
            message=message,
        )


def can_use_exchange_managed_orders(
    config: StrategyConfig,
    signal_instrument: Instrument,
    trade_instrument: Instrument,
) -> bool:
    if is_dynamic_strategy_id(config.strategy_id):
        return False
    return (
        config.tp_sl_mode == "exchange"
        and signal_instrument.inst_id == trade_instrument.inst_id
        and trade_instrument.inst_type == "SWAP"
    )


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
) -> Decimal:
    if taker_fee_rate <= 0:
        return Decimal("0")
    return abs(entry_price) * taker_fee_rate * Decimal("2")


def _dynamic_trigger_price_live(
    *,
    direction: Literal["long", "short"],
    entry_price: Decimal,
    risk_per_unit: Decimal,
    trigger_r: int,
    tick_size: Decimal,
    taker_fee_rate: Decimal = LIVE_DYNAMIC_TAKER_FEE_RATE,
) -> Decimal:
    distance = (risk_per_unit * Decimal(trigger_r)) + _dynamic_two_taker_fee_offset_live(entry_price, taker_fee_rate)
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
) -> Decimal:
    lock_multiple = Decimal(max(trigger_r - 1, 0))
    if two_r_break_even and trigger_r == 2:
        lock_multiple = Decimal("0")
    fee_offset = _dynamic_two_taker_fee_offset_live(entry_price, taker_fee_rate)
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
) -> tuple[Decimal, Decimal, int, bool]:
    if risk_per_unit <= 0:
        next_take_profit = _dynamic_trigger_price_live(
            direction=direction,
            entry_price=entry_price,
            risk_per_unit=Decimal("0"),
            trigger_r=next_trigger_r,
            tick_size=tick_size,
            taker_fee_rate=taker_fee_rate,
        )
        return current_stop_loss, next_take_profit, next_trigger_r, False

    moved = False
    updated_stop = current_stop_loss
    trigger_r = next_trigger_r
    while True:
        trigger_price = _dynamic_trigger_price_live(
            direction=direction,
            entry_price=entry_price,
            risk_per_unit=risk_per_unit,
            trigger_r=trigger_r,
            tick_size=tick_size,
            taker_fee_rate=taker_fee_rate,
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
        )
        updated_stop = max(updated_stop, candidate) if direction == "long" else min(updated_stop, candidate)
        trigger_r += 1
        moved = True


def determine_order_size(
    *,
    instrument: Instrument,
    config: StrategyConfig,
    entry_price: Decimal,
    stop_loss: Decimal,
    risk_price_compatible: bool,
) -> Decimal:
    if config.risk_amount is not None and config.risk_amount > 0 and risk_price_compatible:
        risk_per_unit = abs(entry_price - stop_loss)
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
        raise RuntimeError(
            f"下单数量 {format_decimal(size)} 小于最小下单量 {format_decimal(instrument.min_size)}"
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


def format_hourly_debug(inst_id: str, snapshot: HourlyDebugSnapshot) -> str:
    return (
        f"1小时调试 | {inst_id} | K线时间={_fmt_ts(snapshot.candle_ts)} | "
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
        use_signal_extrema=config.strategy_id == STRATEGY_CROSS_ID,
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


def _fmt_ts(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S")
