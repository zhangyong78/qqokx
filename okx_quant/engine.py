from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable

from okx_quant.indicators import atr, ema
from okx_quant.models import Credentials, Instrument, OrderPlan, StrategyConfig
from okx_quant.okx_client import OkxApiError, OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed, snap_to_increment
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID, STRATEGY_DYNAMIC_ID


Logger = Callable[[str], None]
OKX_SINGLE_REQUEST_MAX_CANDLES = 300
DEFAULT_DEBUG_ATR_PERIOD = 10


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
class ManagedEntryOrder:
    ord_id: str
    candle_ts: int
    entry_reference: Decimal
    size: Decimal


class StrategyEngine:
    def __init__(self, client: OkxRestClient, logger: Logger) -> None:
        self._client = client
        self._logger = logger
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
            instrument = self._client.get_instrument(config.inst_id)
            if instrument.state.lower() != "live":
                raise RuntimeError(f"{instrument.inst_id} 当前不可交易，状态={instrument.state}")

            if config.strategy_id == STRATEGY_DYNAMIC_ID:
                self._run_dynamic_order_strategy(credentials, config, instrument)
            elif config.strategy_id == STRATEGY_CROSS_ID:
                self._run_cross_market_strategy(credentials, config, instrument)
            else:
                raise RuntimeError(f"未知策略：{config.strategy_id}")
        except Exception as exc:
            self._logger(f"策略停止，原因：{exc}")
        finally:
            self._stop_event.set()

    def _run_dynamic_order_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        if config.signal_mode == "both":
            raise RuntimeError("EMA 动态委托脚本不支持双向，请选择只做多或只做空")
        if config.risk_amount is None or config.risk_amount <= 0:
            raise RuntimeError("风险金必须大于 0")

        strategy = EmaDynamicOrderStrategy()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None
        active_order: ManagedEntryOrder | None = None

        self._logger(
            f"启动策略 {config.inst_id} | 周期={config.bar} | EMA={config.ema_period} | ATR={config.atr_period}"
        )
        self._logger(
            "策略规则：以上一根已收盘 K 线的 EMA 作为开仓价挂限价单；"
            "每根新 K 线确认后撤掉旧挂单，并按最新 EMA 数值重新挂单。"
        )
        self._logger(
            f"方向={_format_signal_mode(config.signal_mode)} | 风险金={format_decimal(config.risk_amount)} | "
            f"止损ATR倍数={format_decimal(config.atr_stop_multiplier)} | "
            f"止盈ATR倍数={format_decimal(config.atr_take_multiplier)}"
        )
        self._logger(f"指标回看数量：{lookback} 根 K 线。")

        try:
            hourly_snapshot = fetch_hourly_ema_debug(self._client, config.inst_id, ema_period=config.ema_period)
            self._logger(format_hourly_debug(config.inst_id, hourly_snapshot))
        except Exception as exc:
            self._logger(f"1小时调试值获取失败：{exc}")

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period, config.atr_period)
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
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 挂单已成交 | ordId={status.ord_id} | "
                        f"开仓价={format_decimal(active_order.entry_reference)} | 数量={format_decimal(active_order.size)}"
                    )
                    self._logger("止盈止损已附加到 OKX 主单，本次策略结束。")
                    return
                if state == "partially_filled":
                    self._logger(
                        f"{_fmt_ts(newest_ts)} | 挂单部分成交 | ordId={status.ord_id} | "
                        "为避免重复撤单重挂，策略已停止，请手动检查剩余委托。"
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
            )
            last_candle_ts = newest_ts
            self._logger(
                f"{_fmt_ts(plan.candle_ts)} | 挂单已提交到 OKX | ordId={result.ord_id or '-'} | "
                f"sCode={result.s_code} | sMsg={result.s_msg or 'accepted'}"
            )
            self._stop_event.wait(config.poll_seconds)

    def _run_cross_market_strategy(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        instrument: Instrument,
    ) -> None:
        strategy = EmaAtrStrategy()
        lookback = recommended_indicator_lookback(
            config.ema_period,
            config.atr_period,
            DEFAULT_DEBUG_ATR_PERIOD,
        )
        last_candle_ts: int | None = None

        self._logger(
            f"启动策略 {config.inst_id} | 周期={config.bar} | EMA={config.ema_period} | ATR={config.atr_period}"
        )
        self._logger("策略规则：最新已收盘 K 线上穿 EMA 做多，下穿 EMA 做空。")
        self._logger(
            f"方向={_format_signal_mode(config.signal_mode)} | 风险金="
            f"{format_decimal(config.risk_amount or Decimal('0'))}"
        )
        self._logger(f"指标回看数量：{lookback} 根 K 线。")

        try:
            hourly_snapshot = fetch_hourly_ema_debug(self._client, config.inst_id, ema_period=config.ema_period)
            self._logger(format_hourly_debug(config.inst_id, hourly_snapshot))
        except Exception as exc:
            self._logger(f"1小时调试值获取失败：{exc}")

        while not self._stop_event.is_set():
            candles = self._client.get_candles(config.inst_id, config.bar, limit=lookback)
            confirmed = [candle for candle in candles if candle.confirmed]
            minimum = max(config.ema_period + 2, config.atr_period + 2)
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
            self._logger("止盈止损已交由 OKX 托管，本次策略结束。")
            return

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


def recommended_indicator_lookback(*periods: int) -> int:
    valid_periods = [period for period in periods if period > 0]
    if not valid_periods:
        raise ValueError("周期参数必须大于 0")
    requested = max(max(valid_periods) * 4, 120)
    return min(requested, OKX_SINGLE_REQUEST_MAX_CANDLES)


def fetch_hourly_ema_debug(
    client: OkxRestClient,
    inst_id: str,
    ema_period: int,
    atr_period: int = DEFAULT_DEBUG_ATR_PERIOD,
) -> HourlyDebugSnapshot:
    lookback = recommended_indicator_lookback(ema_period, atr_period)
    candles = client.get_candles(inst_id, "1H", limit=lookback)
    confirmed = [candle for candle in candles if candle.confirmed]
    minimum = max(ema_period, atr_period)
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
) -> OrderPlan:
    if atr_value <= 0:
        raise RuntimeError("ATR 必须大于 0 才能下单")

    entry_price = snap_to_increment(entry_reference, instrument.tick_size, "nearest")

    if signal == "long":
        side = "buy"
        pos_side = "long" if config.position_mode == "long_short" else None
        take_profit_raw = entry_price + (atr_value * config.atr_take_multiplier)
        stop_loss_raw = entry_price - (atr_value * config.atr_stop_multiplier)
        take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "down")
        stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "up")
    elif signal == "short":
        side = "sell"
        pos_side = "short" if config.position_mode == "long_short" else None
        take_profit_raw = entry_price - (atr_value * config.atr_take_multiplier)
        stop_loss_raw = entry_price + (atr_value * config.atr_stop_multiplier)
        take_profit = snap_to_increment(take_profit_raw, instrument.tick_size, "up")
        stop_loss = snap_to_increment(stop_loss_raw, instrument.tick_size, "down")
    else:
        raise RuntimeError(f"不支持的信号方向：{signal}")

    if take_profit <= 0 or stop_loss <= 0:
        raise RuntimeError("计算出来的止盈止损价格必须大于 0")

    if config.risk_amount is not None:
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit <= 0:
            raise RuntimeError("开仓价与止损价过于接近，无法根据风险金计算数量")
        size_raw = config.risk_amount / risk_per_unit
        size = snap_to_increment(size_raw, instrument.lot_size, "down")
    else:
        if order_size is None:
            raise RuntimeError("缺少下单数量，且未设置风险金")
        size = snap_to_increment(order_size, instrument.lot_size, "down")

    if size < instrument.min_size:
        raise RuntimeError(
            f"下单数量 {format_decimal(size)} 小于最小下单量 {format_decimal(instrument.min_size)}"
        )

    return OrderPlan(
        inst_id=instrument.inst_id,
        side=side,
        pos_side=pos_side,
        size=size,
        take_profit=take_profit,
        stop_loss=stop_loss,
        entry_reference=entry_price,
        atr_value=atr_value,
        signal=signal,
        candle_ts=candle_ts,
    )


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
