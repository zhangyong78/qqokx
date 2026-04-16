from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal

from okx_quant.engine import build_protection_plan, determine_order_size
from okx_quant.indicators import atr, ema
from okx_quant.models import Candle, Instrument, OrderPlan, SignalDecision, StrategyConfig
from okx_quant.okx_client import OkxRestClient
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


MAX_BACKTEST_CANDLES = 10000
BACKTEST_RESERVED_CANDLES = 200
ATR_BATCH_MULTIPLIERS: tuple[Decimal, ...] = (
    Decimal("1"),
    Decimal("1.5"),
    Decimal("2"),
)
ATR_BATCH_TAKE_RATIOS: tuple[Decimal, ...] = (
    Decimal("1"),
    Decimal("2"),
    Decimal("3"),
)
BATCH_MAX_ENTRIES_OPTIONS: tuple[int, ...] = (0, 1, 2, 3)


@dataclass(frozen=True)
class BacktestTrade:
    signal: str
    entry_index: int
    exit_index: int
    entry_ts: int
    exit_ts: int
    entry_price: Decimal
    exit_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    size: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    exit_reason: str
    atr_value: Decimal = Decimal("0")
    entry_sequence: int = 0
    entry_fee: Decimal = Decimal("0")
    exit_fee: Decimal = Decimal("0")
    total_fee: Decimal = Decimal("0")
    entry_fee_type: str = "none"
    exit_fee_type: str = "none"
    slippage_cost: Decimal = Decimal("0")
    funding_cost: Decimal = Decimal("0")


@dataclass(frozen=True)
class BacktestReport:
    total_trades: int
    win_trades: int
    loss_trades: int
    breakeven_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    average_pnl: Decimal
    gross_profit: Decimal
    gross_loss: Decimal
    profit_factor: Decimal | None
    average_win: Decimal
    average_loss: Decimal
    profit_loss_ratio: Decimal | None
    average_r_multiple: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal = Decimal("0")
    take_profit_hits: int = 0
    stop_loss_hits: int = 0
    ending_equity: Decimal = Decimal("0")
    total_return_pct: Decimal = Decimal("0")
    maker_fees: Decimal = Decimal("0")
    taker_fees: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    slippage_costs: Decimal = Decimal("0")
    funding_costs: Decimal = Decimal("0")


@dataclass(frozen=True)
class BacktestPeriodStat:
    period_label: str
    trades: int
    win_rate: Decimal
    total_pnl: Decimal
    return_pct: Decimal
    start_equity: Decimal
    end_equity: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal


@dataclass(frozen=True)
class BacktestResult:
    candles: list[Candle]
    trades: list[BacktestTrade]
    report: BacktestReport
    instrument: Instrument
    ema_values: list[Decimal] = field(default_factory=list)
    trend_ema_values: list[Decimal] = field(default_factory=list)
    big_ema_values: list[Decimal] = field(default_factory=list)
    atr_values: list[Decimal | None] = field(default_factory=list)
    equity_curve: list[Decimal] = field(default_factory=list)
    net_value_curve: list[Decimal] = field(default_factory=list)
    drawdown_curve: list[Decimal] = field(default_factory=list)
    drawdown_pct_curve: list[Decimal] = field(default_factory=list)
    monthly_stats: list[BacktestPeriodStat] = field(default_factory=list)
    yearly_stats: list[BacktestPeriodStat] = field(default_factory=list)
    initial_capital: Decimal = Decimal("10000")
    ema_period: int = 21
    trend_ema_period: int = 55
    entry_reference_ema_period: int = 21
    big_ema_period: int = 233
    atr_period: int = 10
    strategy_id: str = STRATEGY_DYNAMIC_ID
    data_source_note: str = ""
    maker_fee_rate: Decimal = Decimal("0")
    taker_fee_rate: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")
    funding_rate: Decimal = Decimal("0")
    take_profit_mode: str = "fixed"
    dynamic_two_r_break_even: bool = False
    dynamic_fee_offset_enabled: bool = True
    max_entries_per_trend: int = 1
    sizing_mode: str = "fixed_risk"
    compounding: bool = False
    open_position: "BacktestOpenPosition | None" = None


@dataclass(frozen=True)
class BacktestOpenPosition:
    signal: str
    entry_index: int
    entry_ts: int
    current_ts: int
    entry_price: Decimal
    current_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    initial_stop_loss: Decimal
    initial_take_profit: Decimal
    size: Decimal
    gross_pnl: Decimal
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    entry_fee: Decimal = Decimal("0")
    funding_cost: Decimal = Decimal("0")


@dataclass
class _OpenPosition:
    signal: str
    entry_index: int
    entry_ts: int
    entry_price: Decimal
    entry_price_raw: Decimal = Decimal("0")
    stop_loss: Decimal = Decimal("0")
    take_profit: Decimal = Decimal("0")
    initial_stop_loss: Decimal = Decimal("0")
    initial_take_profit: Decimal = Decimal("0")
    atr_value: Decimal = Decimal("0")
    size: Decimal = Decimal("0")
    risk_per_unit: Decimal = Decimal("0")
    tick_size: Decimal = Decimal("0.1")
    entry_sequence: int = 0
    dynamic_take_profit_enabled: bool = False
    next_dynamic_trigger_r: int = 2
    dynamic_exit_fee_rate: Decimal = Decimal("0")
    dynamic_two_r_break_even: bool = False
    dynamic_fee_offset_enabled: bool = True
    entry_fee_rate: Decimal = Decimal("0")
    entry_fee_type: str = "none"
    entry_slippage_cost: Decimal = Decimal("0")
    slippage_rate: Decimal = Decimal("0")
    funding_rate: Decimal = Decimal("0")


def run_backtest(
    client: OkxRestClient,
    config: StrategyConfig,
    *,
    candle_limit: int = 200,
    start_ts: int | None = None,
    end_ts: int | None = None,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> BacktestResult:
    if candle_limit <= 0:
        raise ValueError("回测 K 线数量必须大于 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"回测最多支持 {MAX_BACKTEST_CANDLES} 根 K 线")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("开始时间不能晚于结束时间")

    instrument = client.get_instrument(config.inst_id)
    preload_count = _required_backtest_preload_candles(config)
    candles = _load_backtest_candles(
        client,
        config.inst_id,
        config.bar,
        candle_limit,
        start_ts=start_ts,
        end_ts=end_ts,
        preload_count=preload_count,
    )
    return _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=_build_backtest_data_source_note(client),
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
    )


def build_atr_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for stop_multiplier in atr_multipliers:
        for take_ratio in take_ratios:
            configs.append(
                replace(
                    base_config,
                    atr_stop_multiplier=stop_multiplier,
                    atr_take_multiplier=stop_multiplier * take_ratio,
                )
            )
    return configs


def build_dynamic_entry_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    max_entries_options: tuple[int, ...] = BATCH_MAX_ENTRIES_OPTIONS,
) -> list[StrategyConfig]:
    configs: list[StrategyConfig] = []
    for stop_multiplier in atr_multipliers:
        for max_entries in max_entries_options:
            configs.append(
                replace(
                    base_config,
                    atr_stop_multiplier=stop_multiplier,
                    max_entries_per_trend=max_entries,
                )
            )
    return configs


def build_parameter_batch_configs(
    base_config: StrategyConfig,
    *,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
    max_entries_options: tuple[int, ...] = BATCH_MAX_ENTRIES_OPTIONS,
) -> list[StrategyConfig]:
    if not is_dynamic_strategy_id(base_config.strategy_id):
        return build_atr_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
            take_ratios=take_ratios,
        )
    if base_config.take_profit_mode == "dynamic":
        return build_dynamic_entry_batch_configs(
            base_config,
            atr_multipliers=atr_multipliers,
            max_entries_options=max_entries_options,
        )

    configs: list[StrategyConfig] = []
    for max_entries in max_entries_options:
        layer_config = replace(base_config, max_entries_per_trend=max_entries)
        configs.extend(
            build_atr_batch_configs(
                layer_config,
                atr_multipliers=atr_multipliers,
                take_ratios=take_ratios,
            )
        )
    return configs


def _backtest_min_order_size(instrument: Instrument) -> Decimal:
    minimum = snap_to_increment(instrument.min_size, instrument.lot_size, "up")
    if minimum < instrument.min_size:
        return instrument.min_size
    return minimum


def _determine_backtest_order_size(
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
        if size < instrument.min_size:
            return _backtest_min_order_size(instrument)
        return size

    return determine_order_size(
        instrument=instrument,
        config=config,
        entry_price=entry_price,
        stop_loss=stop_loss,
        risk_price_compatible=risk_price_compatible,
    )


def _build_backtest_order_plan(
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
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=config,
            entry_price=protection.entry_reference,
            stop_loss=protection.stop_loss,
            risk_price_compatible=True,
        )
    else:
        if order_size is None:
            raise RuntimeError("缂哄皯涓嬪崟鏁伴噺锛屼笖鏈缃闄╅噾")
        manual_config = replace(config, order_size=order_size, risk_amount=None)
        size = _determine_backtest_order_size(
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


def run_backtest_batch(
    client: OkxRestClient,
    base_config: StrategyConfig,
    *,
    candle_limit: int = 200,
    start_ts: int | None = None,
    end_ts: int | None = None,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> list[tuple[StrategyConfig, BacktestResult]]:
    if base_config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        raise RuntimeError("4H EMA5/EMA8 金叉死叉策略不参与 ATR 批量矩阵回测，请使用单组回测。")
    if candle_limit <= 0:
        raise ValueError("\u56de\u6d4b K \u7ebf\u6570\u91cf\u5fc5\u987b\u5927\u4e8e 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"\u56de\u6d4b\u6700\u591a\u652f\u6301 {MAX_BACKTEST_CANDLES} \u6839 K \u7ebf")
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("开始时间不能晚于结束时间")

    preload_count = _required_backtest_preload_candles(base_config)
    instrument = client.get_instrument(base_config.inst_id)
    candles = _load_backtest_candles(
        client,
        base_config.inst_id,
        base_config.bar,
        candle_limit,
        start_ts=start_ts,
        end_ts=end_ts,
        preload_count=preload_count,
    )
    data_source_note = _build_backtest_data_source_note(client)
    results: list[tuple[StrategyConfig, BacktestResult]] = []
    for config in build_parameter_batch_configs(
        base_config,
        atr_multipliers=atr_multipliers,
        take_ratios=take_ratios,
    ):
        results.append(
            (
                config,
                _run_backtest_with_loaded_data(
                    candles,
                    instrument,
                    config,
                    data_source_note=data_source_note,
                    maker_fee_rate=maker_fee_rate,
                    taker_fee_rate=taker_fee_rate,
                ),
            )
        )
    return results


def _run_backtest_with_loaded_data(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    data_source_note: str = "",
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> BacktestResult:
    terminal_open_position: BacktestOpenPosition | None = None
    if is_dynamic_strategy_id(config.strategy_id):
        trades, terminal_open_position = _run_dynamic_backtest(
            candles,
            instrument,
            config,
            maker_fee_rate=maker_fee_rate,
            taker_fee_rate=taker_fee_rate,
        )
    elif config.strategy_id == STRATEGY_CROSS_ID:
        trades = _run_cross_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
        )
    elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        trades = _run_ema5_ema8_backtest(
            candles,
            instrument,
            config,
            taker_fee_rate=taker_fee_rate,
        )
    else:
        raise RuntimeError(f"鏆備笉鏀寔鐨勫洖娴嬬瓥鐣ワ細{config.strategy_id}")
    ema_values = ema([candle.close for candle in candles], config.ema_period) if candles else []
    trend_ema_values = ema([candle.close for candle in candles], config.trend_ema_period) if candles else []
    atr_values = atr(candles, config.atr_period) if candles else []
    if is_dynamic_strategy_id(config.strategy_id):
        big_ema_values: list[Decimal] = []
    else:
        big_ema_values = ema([candle.close for candle in candles], config.big_ema_period) if candles else []
    initial_capital = config.backtest_initial_capital
    equity_curve = _build_equity_curve(candles, trades)
    net_value_curve = [initial_capital + value for value in equity_curve]
    drawdown_curve, drawdown_pct_curve = _build_drawdown_curves(net_value_curve)
    report = _build_report(trades, initial_capital=initial_capital)

    return BacktestResult(
        candles=candles,
        trades=trades,
        report=report,
        instrument=instrument,
        ema_values=ema_values,
        trend_ema_values=trend_ema_values,
        big_ema_values=big_ema_values,
        atr_values=atr_values,
        equity_curve=equity_curve,
        net_value_curve=net_value_curve,
        drawdown_curve=drawdown_curve,
        drawdown_pct_curve=drawdown_pct_curve,
        monthly_stats=_build_period_stats(trades, initial_capital=initial_capital, by="month"),
        yearly_stats=_build_period_stats(trades, initial_capital=initial_capital, by="year"),
        initial_capital=initial_capital,
        ema_period=config.ema_period,
        trend_ema_period=config.trend_ema_period,
        entry_reference_ema_period=config.resolved_entry_reference_ema_period(),
        big_ema_period=config.big_ema_period,
        atr_period=config.atr_period,
        strategy_id=config.strategy_id,
        data_source_note=data_source_note,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        slippage_rate=config.backtest_slippage_rate,
        funding_rate=config.backtest_funding_rate,
        take_profit_mode=str(config.take_profit_mode),
        dynamic_two_r_break_even=bool(config.dynamic_two_r_break_even),
        dynamic_fee_offset_enabled=bool(config.dynamic_fee_offset_enabled),
        max_entries_per_trend=int(config.max_entries_per_trend),
        sizing_mode=config.backtest_sizing_mode,
        compounding=config.backtest_compounding,
        open_position=terminal_open_position,
    )


def format_backtest_report(result: BacktestResult) -> str:
    report = result.report
    start_time = _format_backtest_timestamp(result.candles[0].ts) if result.candles else "-"
    end_time = _format_backtest_timestamp(result.candles[-1].ts) if result.candles else "-"
    pnl_before_fees = report.total_pnl + report.total_fees
    average_fee = report.total_fees / Decimal(report.total_trades) if report.total_trades > 0 else Decimal("0")
    fee_to_prefee_pct = None if pnl_before_fees == 0 else (report.total_fees / abs(pnl_before_fees)) * Decimal("100")
    fee_to_net_pct = None if report.total_pnl == 0 else (report.total_fees / abs(report.total_pnl)) * Decimal("100")
    fee_to_capital_pct = (
        Decimal("0") if result.initial_capital <= 0 else (report.total_fees / result.initial_capital) * Decimal("100")
    )
    lines = [
        f"回测K线数：{len(result.candles)}",
        f"开始时间：{start_time}",
        f"结束时间：{end_time}",
        f"预热K线：前 {min(BACKTEST_RESERVED_CANDLES, len(result.candles))} 根仅用于指标预热与绘图，不参与回测",
        f"初始资金：{format_decimal_fixed(result.initial_capital, 2)}",
        f"结束权益：{format_decimal_fixed(report.ending_equity, 2)}",
        f"总收益率：{format_decimal_fixed(report.total_return_pct, 2)}%",
        f"仓位模式：{_format_backtest_sizing_mode(result.sizing_mode)}",
        f"复利模式：{'开启' if result.compounding else '关闭'}",
        f"Maker手续费：{_format_fee_rate_percent(result.maker_fee_rate)}",
        f"Taker手续费：{_format_fee_rate_percent(result.taker_fee_rate)}",
        f"滑点：{_format_fee_rate_percent(result.slippage_rate)}",
        f"资金费率/8h：{_format_fee_rate_percent(result.funding_rate)}",
        f"交易次数：{report.total_trades}",
        f"胜率：{format_decimal_fixed(report.win_rate, 2)}%",
        f"总盈亏：{format_decimal(report.total_pnl)}",
        f"平均每笔：{format_decimal_fixed(report.average_pnl, 4)}",
        f"平均R倍数：{format_decimal_fixed(report.average_r_multiple, 4)}",
        f"最大回撤：{format_decimal_fixed(report.max_drawdown, 4)}",
        f"最大回撤比例：{format_decimal_fixed(report.max_drawdown_pct, 2)}%",
        f"手续费合计：{format_decimal_fixed(report.total_fees, 4)}",
        f"Maker手续费合计：{format_decimal_fixed(report.maker_fees, 4)}",
        f"Taker手续费合计：{format_decimal_fixed(report.taker_fees, 4)}",
        f"手续费前盈亏：{format_decimal_fixed(pnl_before_fees, 4)}",
        f"平均单笔手续费：{format_decimal_fixed(average_fee, 4)}",
        (
            f"手续费占手续费前盈亏：{format_decimal_fixed(fee_to_prefee_pct, 2)}%"
            if fee_to_prefee_pct is not None
            else "手续费占手续费前盈亏：无"
        ),
        (
            f"手续费占净盈亏绝对值：{format_decimal_fixed(fee_to_net_pct, 2)}%"
            if fee_to_net_pct is not None
            else "手续费占净盈亏绝对值：无"
        ),
        f"手续费占初始资金：{format_decimal_fixed(fee_to_capital_pct, 2)}%",
        f"滑点成本合计：{format_decimal_fixed(report.slippage_costs, 4)}",
        f"资金费合计：{format_decimal_fixed(report.funding_costs, 4)}",
        f"止盈触发次数：{report.take_profit_hits}",
        f"止损触发次数：{report.stop_loss_hits}",
    ]
    if is_dynamic_strategy_id(result.strategy_id):
        direction_text = "做多" if result.strategy_id == STRATEGY_DYNAMIC_LONG_ID else "做空" if result.strategy_id == STRATEGY_DYNAMIC_SHORT_ID else "按方向参数"
        lines.append(
            f"趋势过滤：EMA{result.ema_period} 与 EMA{result.trend_ema_period} 组成趋势过滤，当前策略方向={direction_text}"
        )
        lines.append(f"挂单参考EMA：EMA{result.entry_reference_ema_period}")
        lines.append(
            f"委托规则：每根新 K 线按最新 EMA{result.entry_reference_ema_period} 重新撤旧挂新，未成交委托不跨 K 线保留"
        )
        lines.append(f"止盈方式：{'动态止盈' if result.take_profit_mode == 'dynamic' else '固定止盈'}")
        lines.append(f"每波最多开仓次数：{result.max_entries_per_trend if result.max_entries_per_trend > 0 else '不限'}")
        if result.take_profit_mode == "dynamic":
            lines.append(
                f"2R保本开关：{'开启' if result.dynamic_two_r_break_even else '关闭'}"
            )
            lines.append(
                f"手续费偏移开关：{'开启' if result.dynamic_fee_offset_enabled else '关闭'}"
            )
            if result.dynamic_two_r_break_even:
                description = (
                    "止盈说明：动态止盈在 2R 时先上移到开仓价+2倍Taker手续费，3R 起按 n-1R+2倍Taker手续费递推；固定止盈为 ATR 倍数止盈。"
                    if result.dynamic_fee_offset_enabled
                    else "止盈说明：动态止盈在 2R 时先上移到开仓价，3R 起按 n-1R 递推；固定止盈为 ATR 倍数止盈。"
                )
            else:
                description = (
                    "止盈说明：动态止盈在 2R 时上移到 1R+2倍Taker手续费，3R 起按 n-1R+2倍Taker手续费递推；固定止盈为 ATR 倍数止盈。"
                    if result.dynamic_fee_offset_enabled
                    else "止盈说明：动态止盈在 2R 时上移到 1R，3R 起按 n-1R 递推；固定止盈为 ATR 倍数止盈。"
                )
            lines.append(description)
        else:
            lines.append("止盈说明：固定止盈为 ATR 倍数止盈。")
        lines.append("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓")
    if result.strategy_id == STRATEGY_EMA5_EMA8_ID:
        lines.append(
            f"交易逻辑：固定 4H EMA{result.ema_period}/EMA{result.trend_ema_period} 金叉死叉开仓，"
            f"收盘价跌破/站回 EMA{result.trend_ema_period} 时按动态止损离场。"
        )
        lines.append("本策略不设固定止盈，回测使用收盘确认与收盘价离场。")
    if report.profit_factor is None:
        lines.append("Profit Factor：无亏损交易")
    else:
        lines.append(f"Profit Factor：{format_decimal_fixed(report.profit_factor, 4)}")
    if report.profit_loss_ratio is None:
        lines.append("盈亏比：无亏损交易")
    else:
        lines.append(f"盈亏比：{format_decimal_fixed(report.profit_loss_ratio, 4)}")
    lines.extend(
        [
            f"盈利笔数：{report.win_trades}",
            f"亏损笔数：{report.loss_trades}",
            f"持平笔数：{report.breakeven_trades}",
            f"平均盈利：{format_decimal_fixed(report.average_win, 4)}",
            f"平均亏损：{format_decimal_fixed(report.average_loss, 4)}",
            f"毛利润：{format_decimal_fixed(report.gross_profit, 4)}",
            f"毛亏损：{format_decimal_fixed(report.gross_loss, 4)}",
        ]
    )
    if result.open_position is not None:
        open_position = result.open_position
        lines.extend(
            [
                "期末未平仓：",
                f"方向：{'做多' if open_position.signal in ('buy', 'long') else '做空'}",
                f"开仓时间：{_format_backtest_timestamp(open_position.entry_ts)}",
                f"当前时间：{_format_backtest_timestamp(open_position.current_ts)}",
                f"开仓价格：{format_decimal_fixed(open_position.entry_price, 4)}",
                f"当前价格：{format_decimal_fixed(open_position.current_price, 4)}",
                f"初始止损：{format_decimal_fixed(open_position.initial_stop_loss, 4)}",
                f"当前止损：{format_decimal_fixed(open_position.stop_loss, 4)}",
                f"初始止盈：{format_decimal_fixed(open_position.initial_take_profit, 4)}",
                f"当前止盈：{format_decimal_fixed(open_position.take_profit, 4)}",
                f"开仓数量：{format_decimal_fixed(open_position.size, 4)}",
                f"浮动盈亏：{format_decimal_fixed(open_position.pnl, 4)}",
                f"R倍数：{format_decimal_fixed(open_position.r_multiple, 4)}",
                f"开仓手续费：{format_decimal_fixed(open_position.entry_fee, 4)}",
                f"资金费：{format_decimal_fixed(open_position.funding_cost, 4)}",
            ]
        )
    return "\n".join(lines)


def _format_backtest_timestamp(ts: int) -> str:
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    return str(ts)


def _load_backtest_candles(
    client: OkxRestClient,
    inst_id: str,
    bar: str,
    candle_limit: int,
    *,
    start_ts: int | None = None,
    end_ts: int | None = None,
    preload_count: int = 0,
) -> list[Candle]:
    range_fetcher = getattr(client, "get_candles_history_range", None)
    history_fetcher = getattr(client, "get_candles_history", None)
    used_range_fetcher = (start_ts is not None or end_ts is not None) and callable(range_fetcher)
    if used_range_fetcher:
        raw_candles = range_fetcher(
            inst_id,
            bar,
            start_ts=0 if start_ts is None else start_ts,
            end_ts=9999999999999 if end_ts is None else end_ts,
            limit=candle_limit,
            preload_count=max(0, preload_count),
        )
    elif callable(history_fetcher):
        raw_candles = history_fetcher(inst_id, bar, limit=candle_limit)
    else:
        raw_candles = client.get_candles(inst_id, bar, limit=candle_limit)
    candles = [candle for candle in raw_candles if candle.confirmed]
    if not used_range_fetcher and start_ts is not None:
        candles = [candle for candle in candles if candle.ts >= start_ts]
    if not used_range_fetcher and end_ts is not None:
        candles = [candle for candle in candles if candle.ts <= end_ts]
    return candles if used_range_fetcher else candles[-candle_limit:]


def _required_backtest_preload_candles(config: StrategyConfig) -> int:
    if config.strategy_id == STRATEGY_CROSS_ID:
        minimum = max(
            config.ema_period + 2,
            config.trend_ema_period + 2,
            config.big_ema_period + 2,
            config.atr_period + 2,
        )
    elif config.strategy_id == STRATEGY_EMA5_EMA8_ID:
        minimum = max(config.ema_period, config.trend_ema_period) + 1
    else:
        minimum = max(
            config.ema_period,
            config.trend_ema_period,
            config.atr_period,
            config.resolved_entry_reference_ema_period(),
        )
    return _backtest_trade_start_index(minimum)


def _build_backtest_data_source_note(client: OkxRestClient) -> str:
    stats = getattr(client, "last_candle_history_stats", None)
    if not isinstance(stats, dict):
        return ""
    if stats.get("range_mode"):
        returned_count = int(stats.get("returned_count", 0) or 0)
        requested_count = int(stats.get("requested_count", 0) or 0)
        selected_count = int(stats.get("selected_count", 0) or 0)
        preload_count = int(stats.get("preload_count", 0) or 0)
        start_ts = stats.get("start_ts")
        end_ts = stats.get("end_ts")
        range_text = ""
        if start_ts and end_ts:
            range_text = (
                f"{datetime.fromtimestamp(int(start_ts) / 1000).strftime('%Y-%m-%d %H:%M')}"
                f" ~ {datetime.fromtimestamp(int(end_ts) / 1000).strftime('%Y-%m-%d %H:%M')}"
            )
        parts = ["按时间段取数"]
        if range_text:
            parts.append(range_text)
        if requested_count > 0:
            parts.append(f"上限 {requested_count} 根")
        if selected_count > 0:
            parts.append(f"区间内返回 {selected_count} 根")
        if preload_count > 0:
            parts.append(f"前置补足 {preload_count} 根")
        if returned_count > 0:
            parts.append(f"实际载入 {returned_count} 根")
        return " | ".join(parts)
    cache_hit_count = int(stats.get("cache_hit_count", 0) or 0)
    latest_fetch_count = int(stats.get("latest_fetch_count", 0) or 0)
    older_fetch_count = int(stats.get("older_fetch_count", 0) or 0)
    returned_count = int(stats.get("returned_count", 0) or 0)
    parts = [
        f"\u672c\u6b21\u547d\u4e2d\u672c\u5730\u7f13\u5b58 {cache_hit_count} \u6839",
        f"\u8865\u62c9\u6700\u65b0 {latest_fetch_count} \u6839",
    ]
    if older_fetch_count > 0:
        parts.append(f"\u8865\u62c9\u66f4\u65e9 {older_fetch_count} \u6839")
    if returned_count > 0:
        parts.append(f"\u672c\u6b21\u56de\u6d4b\u53d6\u6570 {returned_count} \u6839")
    return " | ".join(parts)


def _run_ema5_ema8_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
) -> list[BacktestTrade]:
    strategy = EmaCrossEmaStopStrategy()
    minimum = max(config.ema_period, config.trend_ema_period) + 1
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return []

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]

        if open_position is not None:
            _, stop_line = strategy.latest_stop_line(candles[: index + 1], config)
            stop_hit = candle.close < stop_line if open_position.signal == "long" else candle.close > stop_line
            if stop_hit:
                exit_price_raw = candle.close
                exit_price = _apply_slippage_price(
                    exit_price_raw,
                    signal=open_position.signal,
                    tick_size=open_position.tick_size,
                    slippage_rate=open_position.slippage_rate,
                    is_entry=False,
                )
                trades.append(
                    _build_closed_trade(
                        open_position,
                        candle,
                        index,
                        exit_price_raw=exit_price_raw,
                        exit_price=exit_price,
                        exit_reason="stop_loss",
                        exit_fee_rate=taker_fee_rate,
                        exit_fee_type="taker",
                    )
                )
                open_position = None
                continue

        if open_position is not None:
            continue

        decision = strategy.evaluate(candles[: index + 1], config)
        if decision.signal is None or decision.ema_value is None or decision.candle_ts is None:
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        size = _determine_backtest_order_size(
            instrument=instrument,
            config=resolved_config,
            entry_price=decision.entry_reference,
            stop_loss=decision.ema_value,
            risk_price_compatible=True,
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal=decision.signal,
            entry_index=index,
            entry_ts=decision.candle_ts,
            entry_price_raw=decision.entry_reference,
            stop_loss=decision.ema_value,
            take_profit=decision.entry_reference,
            atr_value=decision.atr_value,
            size=size,
            entry_fee_rate=taker_fee_rate,
            entry_fee_type="taker",
            slippage_rate=config.backtest_slippage_rate,
            funding_rate=config.backtest_funding_rate,
        )

    return trades


def _run_cross_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    taker_fee_rate: Decimal = Decimal("0"),
) -> list[BacktestTrade]:
    strategy = EmaAtrStrategy()
    minimum = max(
        config.ema_period + 2,
        config.trend_ema_period + 2,
        config.big_ema_period + 2,
        config.atr_period + 2,
    )
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return []

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        if open_position is not None:
            closed_trade = _try_close_position(
                open_position,
                candle,
                index,
                exit_fee_rate=taker_fee_rate,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None

        if open_position is not None:
            continue

        decision = strategy.evaluate(candles[: index + 1], config)
        if decision.signal is None:
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        plan = _build_backtest_order_plan(
            instrument=instrument,
            config=resolved_config,
            order_size=resolved_config.order_size,
            signal=decision.signal,
            entry_reference=decision.entry_reference,
            atr_value=decision.atr_value,
            candle_ts=decision.candle_ts,
            signal_candle_high=decision.signal_candle_high,
            signal_candle_low=decision.signal_candle_low,
        )
        open_position = _create_open_position(
            instrument=instrument,
            signal=plan.signal,
            entry_index=index,
            entry_ts=plan.candle_ts,
            entry_price_raw=plan.entry_reference,
            stop_loss=plan.stop_loss,
            take_profit=plan.take_profit,
            atr_value=plan.atr_value,
            size=plan.size,
            entry_fee_rate=taker_fee_rate,
            entry_fee_type="taker",
            slippage_rate=config.backtest_slippage_rate,
            funding_rate=config.backtest_funding_rate,
        )

    return trades


def _run_dynamic_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    maker_fee_rate: Decimal = Decimal("0"),
    taker_fee_rate: Decimal = Decimal("0"),
) -> tuple[list[BacktestTrade], BacktestOpenPosition | None]:
    entry_reference_ema_period = config.resolved_entry_reference_ema_period()
    minimum = max(
        config.ema_period,
        config.trend_ema_period,
        config.atr_period,
        entry_reference_ema_period,
    )
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根。")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return [], None

    closes = [candle.close for candle in candles]
    ema_values = ema(closes, config.ema_period)
    entry_reference_ema_values = (
        ema_values if entry_reference_ema_period == config.ema_period else ema(closes, entry_reference_ema_period)
    )
    trend_ema_values = ema(closes, config.trend_ema_period)
    atr_values = atr(candles, config.atr_period)
    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    active_plan = None
    current_wave_signal: str | None = None
    entries_in_current_wave = 0
    entry_sequence = 0
    dynamic_take_profit_enabled = config.take_profit_mode == "dynamic"

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]

        if open_position is not None:
            closed_trade = _try_close_position(
                open_position,
                candle,
                index,
                exit_fee_rate=taker_fee_rate,
                exit_fee_type="taker",
            )
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None

        if active_plan is not None and open_position is None:
            filled_position = _try_fill_dynamic_order(
                instrument,
                active_plan,
                candle,
                index,
                entry_fee_rate=maker_fee_rate,
                entry_fee_type="maker",
                slippage_rate=config.backtest_slippage_rate,
                funding_rate=config.backtest_funding_rate,
                entry_sequence=entry_sequence + 1,
                dynamic_take_profit_enabled=dynamic_take_profit_enabled,
                dynamic_exit_fee_rate=taker_fee_rate,
                dynamic_two_r_break_even=config.dynamic_two_r_break_even,
                dynamic_fee_offset_enabled=config.dynamic_fee_offset_enabled,
            )
            active_plan = None
            if filled_position is not None:
                entry_sequence += 1
                entries_in_current_wave += 1
                closed_trade = _try_close_position_same_candle_after_fill(
                    filled_position,
                    candle,
                    index,
                    exit_fee_rate=taker_fee_rate,
                    exit_fee_type="taker",
                )
                if closed_trade is not None:
                    trades.append(closed_trade)
                else:
                    open_position = filled_position

        if open_position is not None or index >= len(candles) - 1:
            continue

        decision = _evaluate_dynamic_signal_precomputed(
            candles,
            index,
            ema_values,
            entry_reference_ema_values,
            trend_ema_values,
            atr_values,
            config,
        )
        if decision.signal is None:
            current_wave_signal = None
            entries_in_current_wave = 0
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue

        if current_wave_signal != decision.signal:
            current_wave_signal = decision.signal
            entries_in_current_wave = 0
        if config.max_entries_per_trend > 0 and entries_in_current_wave >= config.max_entries_per_trend:
            continue

        resolved_config = _resolve_backtest_config(config, trades)
        active_plan = _build_backtest_order_plan(
            instrument=instrument,
            config=resolved_config,
            order_size=resolved_config.order_size,
            signal=decision.signal,
            entry_reference=decision.entry_reference,
            atr_value=decision.atr_value,
            candle_ts=decision.candle_ts,
            signal_candle_high=decision.signal_candle_high,
            signal_candle_low=decision.signal_candle_low,
        )

    terminal_open_position: BacktestOpenPosition | None = None
    if open_position is not None and candles:
        last_candle = candles[-1]
        current_price = last_candle.close
        if open_position.signal == "long":
            gross_pnl = (current_price - open_position.entry_price) * open_position.size
        else:
            gross_pnl = (open_position.entry_price - current_price) * open_position.size
        entry_fee = abs(open_position.entry_price * open_position.size) * open_position.entry_fee_rate
        funding_periods = Decimal(str(max(last_candle.ts - open_position.entry_ts, 0))) / Decimal("28800000")
        funding_cost = abs(open_position.entry_price * open_position.size) * open_position.funding_rate * funding_periods
        pnl = gross_pnl - entry_fee - funding_cost
        risk_value = abs(open_position.entry_price - open_position.stop_loss) * open_position.size
        r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
        terminal_open_position = BacktestOpenPosition(
            signal=open_position.signal,
            entry_index=open_position.entry_index,
            entry_ts=open_position.entry_ts,
            current_ts=last_candle.ts,
            entry_price=open_position.entry_price,
            current_price=current_price,
            stop_loss=open_position.stop_loss,
            take_profit=open_position.take_profit,
            initial_stop_loss=open_position.initial_stop_loss,
            initial_take_profit=open_position.initial_take_profit,
            size=open_position.size,
            gross_pnl=gross_pnl,
            pnl=pnl,
            risk_value=risk_value,
            r_multiple=r_multiple,
            entry_fee=entry_fee,
            funding_cost=funding_cost,
        )

    return trades, terminal_open_position


def _evaluate_dynamic_signal_precomputed(
    candles: list[Candle],
    index: int,
    ema_values: list[Decimal],
    entry_reference_ema_values: list[Decimal],
    trend_ema_values: list[Decimal],
    atr_values: list[Decimal | None],
    config: StrategyConfig,
) -> SignalDecision:
    current_candle = candles[index]
    current_ema = ema_values[index]
    current_entry_reference = entry_reference_ema_values[index]
    trend_ema = trend_ema_values[index]
    current_atr = atr_values[index]
    if current_atr is None:
        return SignalDecision(
            signal=None,
            reason="atr_not_ready",
            candle_ts=current_candle.ts,
            entry_reference=None,
            atr_value=None,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    effective_signal_mode = resolve_dynamic_signal_mode(config.strategy_id, config.signal_mode)

    if effective_signal_mode == "long_only":
        if current_ema <= trend_ema:
            return SignalDecision(
                signal=None,
                reason="fast_ema_below_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if current_candle.close <= trend_ema:
            return SignalDecision(
                signal=None,
                reason="close_below_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="long",
            reason="dynamic_long",
            candle_ts=current_candle.ts,
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    if effective_signal_mode == "short_only":
        if current_ema >= trend_ema:
            return SignalDecision(
                signal=None,
                reason="fast_ema_above_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        if current_candle.close >= trend_ema:
            return SignalDecision(
                signal=None,
                reason="close_above_trend_ema",
                candle_ts=current_candle.ts,
                entry_reference=None,
                atr_value=current_atr,
                ema_value=current_ema,
                signal_candle_high=current_candle.high,
                signal_candle_low=current_candle.low,
            )
        return SignalDecision(
            signal="short",
            reason="dynamic_short",
            candle_ts=current_candle.ts,
            entry_reference=current_entry_reference,
            atr_value=current_atr,
            ema_value=current_ema,
            signal_candle_high=current_candle.high,
            signal_candle_low=current_candle.low,
        )

    return SignalDecision(
        signal=None,
        reason="unsupported_signal_mode",
        candle_ts=current_candle.ts,
        entry_reference=None,
        atr_value=current_atr,
        ema_value=current_ema,
        signal_candle_high=current_candle.high,
        signal_candle_low=current_candle.low,
    )


def _realized_pnl(trades: list[BacktestTrade]) -> Decimal:
    return sum((trade.pnl for trade in trades), Decimal("0"))


def _base_equity_for_sizing(config: StrategyConfig, trades: list[BacktestTrade]) -> Decimal:
    if config.backtest_compounding:
        return config.backtest_initial_capital + _realized_pnl(trades)
    return config.backtest_initial_capital


def _resolve_backtest_config(config: StrategyConfig, trades: list[BacktestTrade]) -> StrategyConfig:
    if config.backtest_sizing_mode == "fixed_size":
        return replace(config, risk_amount=None)

    if config.backtest_sizing_mode == "risk_percent":
        if config.backtest_risk_percent is None or config.backtest_risk_percent <= 0:
            raise RuntimeError("风险百分比模式下，风险百分比必须大于 0")
        base_equity = _base_equity_for_sizing(config, trades)
        if base_equity <= 0:
            raise RuntimeError("当前权益小于等于 0，无法继续按风险百分比回测。")
        risk_amount = base_equity * config.backtest_risk_percent / Decimal("100")
        return replace(config, risk_amount=risk_amount, order_size=Decimal("0"))

    if config.risk_amount is None or config.risk_amount <= 0:
        raise RuntimeError("固定风险模式下，风险金必须大于 0")
    return replace(config, risk_amount=config.risk_amount, order_size=Decimal("0"))


def _apply_slippage_price(
    price: Decimal,
    *,
    signal: str,
    tick_size: Decimal,
    slippage_rate: Decimal,
    is_entry: bool,
) -> Decimal:
    if slippage_rate <= 0:
        return price
    if signal == "long":
        raw_price = price * (Decimal("1") + slippage_rate) if is_entry else price * (Decimal("1") - slippage_rate)
        direction = "up" if is_entry else "down"
    else:
        raw_price = price * (Decimal("1") - slippage_rate) if is_entry else price * (Decimal("1") + slippage_rate)
        direction = "down" if is_entry else "up"
    return snap_to_increment(raw_price, tick_size, direction)


def _create_open_position(
    *,
    instrument: Instrument,
    signal: str,
    entry_index: int,
    entry_ts: int,
    entry_price_raw: Decimal,
    stop_loss: Decimal,
    take_profit: Decimal,
    atr_value: Decimal,
    size: Decimal,
    entry_fee_rate: Decimal,
    entry_fee_type: str,
    slippage_rate: Decimal,
    funding_rate: Decimal,
    entry_sequence: int = 0,
    dynamic_take_profit_enabled: bool = False,
    dynamic_exit_fee_rate: Decimal = Decimal("0"),
    dynamic_two_r_break_even: bool = False,
    dynamic_fee_offset_enabled: bool = True,
    next_dynamic_trigger_r: int = 2,
    current_take_profit: Decimal | None = None,
) -> _OpenPosition:
    entry_price = _apply_slippage_price(
        entry_price_raw,
        signal=signal,
        tick_size=instrument.tick_size,
        slippage_rate=slippage_rate,
        is_entry=True,
    )
    risk_per_unit = abs(entry_price - stop_loss)
    display_take_profit = take_profit if current_take_profit is None else current_take_profit
    open_position = _OpenPosition(
        signal=signal,
        entry_index=entry_index,
        entry_ts=entry_ts,
        entry_price=entry_price,
        entry_price_raw=entry_price_raw,
        stop_loss=stop_loss,
        take_profit=display_take_profit,
        initial_stop_loss=stop_loss,
        initial_take_profit=take_profit,
        atr_value=atr_value,
        size=size,
        risk_per_unit=risk_per_unit,
        tick_size=instrument.tick_size,
        entry_sequence=entry_sequence,
        dynamic_take_profit_enabled=dynamic_take_profit_enabled,
        next_dynamic_trigger_r=next_dynamic_trigger_r,
        dynamic_exit_fee_rate=dynamic_exit_fee_rate,
        dynamic_two_r_break_even=dynamic_two_r_break_even,
        dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
        entry_fee_rate=entry_fee_rate,
        entry_fee_type=entry_fee_type,
        entry_slippage_cost=abs(entry_price - entry_price_raw) * abs(size),
        slippage_rate=slippage_rate,
        funding_rate=funding_rate if instrument.inst_type == "SWAP" else Decimal("0"),
    )
    if current_take_profit is None and dynamic_take_profit_enabled:
        open_position.take_profit = _dynamic_trigger_price(open_position, next_dynamic_trigger_r)
    return open_position


def _try_fill_dynamic_order(
    instrument: Instrument,
    plan,
    candle: Candle,
    candle_index: int,
    *,
    entry_fee_rate: Decimal = Decimal("0"),
    entry_fee_type: str = "none",
    slippage_rate: Decimal = Decimal("0"),
    funding_rate: Decimal = Decimal("0"),
    entry_sequence: int = 0,
    dynamic_take_profit_enabled: bool = False,
    dynamic_exit_fee_rate: Decimal = Decimal("0"),
    dynamic_two_r_break_even: bool = False,
    dynamic_fee_offset_enabled: bool = True,
) -> _OpenPosition | None:
    filled = candle.low <= plan.entry_reference <= candle.high
    if not filled:
        return None

    return _create_open_position(
        instrument=instrument,
        signal=plan.signal,
        entry_index=candle_index,
        entry_ts=candle.ts,
        entry_price_raw=plan.entry_reference,
        stop_loss=plan.stop_loss,
        take_profit=plan.take_profit,
        atr_value=plan.atr_value,
        size=plan.size,
        entry_fee_rate=entry_fee_rate,
        entry_fee_type=entry_fee_type,
        slippage_rate=slippage_rate,
        funding_rate=funding_rate,
        entry_sequence=entry_sequence,
        dynamic_take_profit_enabled=dynamic_take_profit_enabled,
        dynamic_exit_fee_rate=dynamic_exit_fee_rate,
        dynamic_two_r_break_even=dynamic_two_r_break_even,
        dynamic_fee_offset_enabled=dynamic_fee_offset_enabled,
    )


def _segment_contains_price(start: Decimal, end: Decimal, price: Decimal) -> bool:
    return min(start, end) <= price <= max(start, end)


def _first_touched_exit_on_segment(
    start: Decimal,
    end: Decimal,
    *,
    stop_loss: Decimal,
    take_profit: Decimal,
) -> tuple[Decimal, str] | None:
    if end > start:
        touched: list[tuple[Decimal, str]] = []
        if start <= stop_loss <= end:
            touched.append((stop_loss, "stop_loss"))
        if start <= take_profit <= end:
            touched.append((take_profit, "take_profit"))
        if not touched:
            return None
        return min(touched, key=lambda item: item[0])
    if end < start:
        touched = []
        if end <= stop_loss <= start:
            touched.append((stop_loss, "stop_loss"))
        if end <= take_profit <= start:
            touched.append((take_profit, "take_profit"))
        if not touched:
            return None
        return max(touched, key=lambda item: item[0])
    if stop_loss == start:
        return stop_loss, "stop_loss"
    if take_profit == start:
        return take_profit, "take_profit"
    return None


def _same_candle_path_points(candle: Candle) -> tuple[Decimal, ...] | None:
    if candle.close > candle.open:
        return candle.open, candle.low, candle.high, candle.close
    if candle.close < candle.open:
        return candle.open, candle.high, candle.low, candle.close
    return None


def _candle_path_points(candle: Candle) -> tuple[Decimal, ...]:
    if candle.close >= candle.open:
        return candle.open, candle.low, candle.high, candle.close
    return candle.open, candle.high, candle.low, candle.close


def _dynamic_fee_offset(entry_price: Decimal, exit_fee_rate: Decimal, *, enabled: bool = True) -> Decimal:
    if not enabled or exit_fee_rate <= 0:
        return Decimal("0")
    return abs(entry_price) * exit_fee_rate * Decimal("2")


def _dynamic_trigger_price(position: _OpenPosition, trigger_r: int) -> Decimal:
    fee_offset = _dynamic_fee_offset(
        position.entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    offset = (position.risk_per_unit * Decimal(str(trigger_r))) + fee_offset
    raw = position.entry_price + offset if position.signal == "long" else position.entry_price - offset
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _dynamic_stop_price(position: _OpenPosition, trigger_r: int) -> Decimal:
    lock_multiple = max(trigger_r - 1, 0)
    if position.dynamic_two_r_break_even and trigger_r == 2:
        lock_multiple = 0
    locked_offset = position.risk_per_unit * Decimal(str(lock_multiple))
    fee_offset = _dynamic_fee_offset(
        position.entry_price,
        position.dynamic_exit_fee_rate,
        enabled=position.dynamic_fee_offset_enabled,
    )
    raw = (
        position.entry_price + locked_offset + fee_offset
        if position.signal == "long"
        else position.entry_price - locked_offset - fee_offset
    )
    rounding = "up" if position.signal == "long" else "down"
    return snap_to_increment(raw, position.tick_size, rounding)


def _advance_dynamic_stop(position: _OpenPosition, favorable_price: Decimal) -> None:
    while position.next_dynamic_trigger_r >= 2:
        trigger_price = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)
        if position.signal == "long":
            if favorable_price < trigger_price:
                break
            candidate_stop = _dynamic_stop_price(position, position.next_dynamic_trigger_r)
            if candidate_stop > position.stop_loss:
                position.stop_loss = candidate_stop
            position.next_dynamic_trigger_r += 1
        else:
            if favorable_price > trigger_price:
                break
            candidate_stop = _dynamic_stop_price(position, position.next_dynamic_trigger_r)
            if candidate_stop < position.stop_loss:
                position.stop_loss = candidate_stop
            position.next_dynamic_trigger_r += 1
    position.take_profit = _dynamic_trigger_price(position, position.next_dynamic_trigger_r)


def _process_dynamic_position_segment(
    position: _OpenPosition,
    start: Decimal,
    end: Decimal,
) -> tuple[Decimal, str] | None:
    if position.signal == "long":
        if end < start:
            if _segment_contains_price(start, end, position.stop_loss):
                return position.stop_loss, "stop_loss"
            return None
        _advance_dynamic_stop(position, end)
        return None
    if end > start:
        if _segment_contains_price(start, end, position.stop_loss):
            return position.stop_loss, "stop_loss"
        return None
    _advance_dynamic_stop(position, end)
    return None


def _build_closed_trade(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_price_raw: Decimal,
    exit_price: Decimal,
    exit_reason: str,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade:
    if position.signal == "long":
        gross_pnl = (exit_price - position.entry_price) * position.size
    else:
        gross_pnl = (position.entry_price - exit_price) * position.size
    entry_fee = abs(position.entry_price * position.size) * position.entry_fee_rate
    exit_fee = abs(exit_price * position.size) * exit_fee_rate
    total_fee = entry_fee + exit_fee
    funding_periods = Decimal(str(max(candle.ts - position.entry_ts, 0))) / Decimal("28800000")
    funding_cost = abs(position.entry_price * position.size) * position.funding_rate * funding_periods
    pnl = gross_pnl - total_fee - funding_cost
    risk_value = abs(position.entry_price - position.initial_stop_loss) * position.size
    r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
    slippage_cost = position.entry_slippage_cost + (abs(exit_price - exit_price_raw) * abs(position.size))
    return BacktestTrade(
        signal=position.signal,
        entry_index=position.entry_index,
        exit_index=candle_index,
        entry_ts=position.entry_ts,
        exit_ts=candle.ts,
        entry_price=position.entry_price,
        exit_price=exit_price,
        stop_loss=position.initial_stop_loss,
        take_profit=position.initial_take_profit,
        size=position.size,
        gross_pnl=gross_pnl,
        pnl=pnl,
        risk_value=abs(position.entry_price - position.initial_stop_loss) * position.size,
        r_multiple=r_multiple,
        exit_reason=exit_reason,
        atr_value=position.atr_value,
        entry_sequence=position.entry_sequence,
        entry_fee=entry_fee,
        exit_fee=exit_fee,
        total_fee=total_fee,
        entry_fee_type=position.entry_fee_type,
        exit_fee_type=exit_fee_type,
        slippage_cost=slippage_cost,
        funding_cost=funding_cost,
    )


def _try_close_position_same_candle_after_fill(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade | None:
    path_points = _same_candle_path_points(candle)
    if path_points is None:
        return None

    entry_price = position.entry_price
    entry_reached = False
    segment_start = path_points[0]

    for segment_end in path_points[1:]:
        if not entry_reached:
            if not _segment_contains_price(segment_start, segment_end, entry_price):
                segment_start = segment_end
                continue
            if position.dynamic_take_profit_enabled:
                touched_exit = _process_dynamic_position_segment(
                    position,
                    entry_price,
                    segment_end,
                )
            else:
                touched_exit = _first_touched_exit_on_segment(
                    entry_price,
                    segment_end,
                    stop_loss=position.stop_loss,
                    take_profit=position.take_profit,
                )
            if touched_exit is not None:
                exit_price_raw, exit_reason = touched_exit
                exit_price = _apply_slippage_price(
                    exit_price_raw,
                    signal=position.signal,
                    tick_size=position.tick_size,
                    slippage_rate=position.slippage_rate,
                    is_entry=False,
                )
                return _build_closed_trade(
                    position,
                    candle,
                    candle_index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    exit_fee_rate=exit_fee_rate,
                    exit_fee_type=exit_fee_type,
                )
            entry_reached = True
        else:
            if position.dynamic_take_profit_enabled:
                touched_exit = _process_dynamic_position_segment(
                    position,
                    segment_start,
                    segment_end,
                )
            else:
                touched_exit = _first_touched_exit_on_segment(
                    segment_start,
                    segment_end,
                    stop_loss=position.stop_loss,
                    take_profit=position.take_profit,
                )
            if touched_exit is not None:
                exit_price_raw, exit_reason = touched_exit
                exit_price = _apply_slippage_price(
                    exit_price_raw,
                    signal=position.signal,
                    tick_size=position.tick_size,
                    slippage_rate=position.slippage_rate,
                    is_entry=False,
                )
                return _build_closed_trade(
                    position,
                    candle,
                    candle_index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    exit_fee_rate=exit_fee_rate,
                    exit_fee_type=exit_fee_type,
                )
        segment_start = segment_end
    return None


def _try_close_position(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    allow_same_candle: bool = False,
    exit_fee_rate: Decimal = Decimal("0"),
    exit_fee_type: str = "none",
) -> BacktestTrade | None:
    if candle_index < position.entry_index:
        return None
    if not allow_same_candle and candle_index == position.entry_index:
        return None

    if position.dynamic_take_profit_enabled:
        path_points = _candle_path_points(candle)
        segment_start = path_points[0]
        for segment_end in path_points[1:]:
            touched_exit = _process_dynamic_position_segment(
                position,
                segment_start,
                segment_end,
            )
            if touched_exit is not None:
                exit_price_raw, exit_reason = touched_exit
                exit_price = _apply_slippage_price(
                    exit_price_raw,
                    signal=position.signal,
                    tick_size=position.tick_size,
                    slippage_rate=position.slippage_rate,
                    is_entry=False,
                )
                return _build_closed_trade(
                    position,
                    candle,
                    candle_index,
                    exit_price_raw=exit_price_raw,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                    exit_fee_rate=exit_fee_rate,
                    exit_fee_type=exit_fee_type,
                )
            segment_start = segment_end
        return None

    if position.signal == "long":
        stop_hit = candle.low <= position.stop_loss
        take_hit = candle.high >= position.take_profit
        if stop_hit:
            exit_price_raw = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price_raw = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
    else:
        stop_hit = candle.high >= position.stop_loss
        take_hit = candle.low <= position.take_profit
        if stop_hit:
            exit_price_raw = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price_raw = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
    exit_price = _apply_slippage_price(
        exit_price_raw,
        signal=position.signal,
        tick_size=position.tick_size,
        slippage_rate=position.slippage_rate,
        is_entry=False,
    )
    return _build_closed_trade(
        position,
        candle,
        candle_index,
        exit_price_raw=exit_price_raw,
        exit_price=exit_price,
        exit_reason=exit_reason,
        exit_fee_rate=exit_fee_rate,
        exit_fee_type=exit_fee_type,
    )


def _build_equity_curve(candles: list[Candle], trades: list[BacktestTrade]) -> list[Decimal]:
    if not candles:
        return []
    changes = [Decimal("0") for _ in candles]
    last_index = len(candles) - 1
    for trade in trades:
        exit_index = max(0, min(trade.exit_index, last_index))
        changes[exit_index] += trade.pnl
    equity_curve: list[Decimal] = []
    running_total = Decimal("0")
    for change in changes:
        running_total += change
        equity_curve.append(running_total)
    return equity_curve


def _build_drawdown_curves(net_value_curve: list[Decimal]) -> tuple[list[Decimal], list[Decimal]]:
    drawdown_curve: list[Decimal] = []
    drawdown_pct_curve: list[Decimal] = []
    if not net_value_curve:
        return drawdown_curve, drawdown_pct_curve
    peak = net_value_curve[0]
    for value in net_value_curve:
        if value > peak:
            peak = value
        drawdown = peak - value
        drawdown_curve.append(drawdown)
        if peak > 0:
            drawdown_pct_curve.append((drawdown / peak) * Decimal("100"))
        else:
            drawdown_pct_curve.append(Decimal("0"))
    return drawdown_curve, drawdown_pct_curve


def _build_period_stats(
    trades: list[BacktestTrade],
    *,
    initial_capital: Decimal,
    by: str,
) -> list[BacktestPeriodStat]:
    if by not in {"month", "year"}:
        raise ValueError("Unsupported period grouping")
    if not trades:
        return []

    sorted_trades = sorted(trades, key=lambda trade: trade.exit_ts)
    groups: dict[str, list[BacktestTrade]] = {}
    for trade in sorted_trades:
        dt = datetime.fromtimestamp(trade.exit_ts / 1000 if trade.exit_ts >= 10**12 else trade.exit_ts)
        key = dt.strftime("%Y-%m") if by == "month" else dt.strftime("%Y")
        groups.setdefault(key, []).append(trade)

    stats: list[BacktestPeriodStat] = []
    realized_before_period = Decimal("0")
    for period_label in sorted(groups):
        period_trades = groups[period_label]
        start_equity = initial_capital + realized_before_period
        period_equity = start_equity
        peak = start_equity
        max_drawdown = Decimal("0")
        wins = 0
        total_pnl = Decimal("0")
        for trade in period_trades:
            total_pnl += trade.pnl
            period_equity += trade.pnl
            if trade.pnl > 0:
                wins += 1
            if period_equity > peak:
                peak = period_equity
            drawdown = peak - period_equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        end_equity = start_equity + total_pnl
        return_pct = Decimal("0") if start_equity <= 0 else (total_pnl / start_equity) * Decimal("100")
        max_drawdown_pct = Decimal("0") if peak <= 0 else (max_drawdown / peak) * Decimal("100")
        stats.append(
            BacktestPeriodStat(
                period_label=period_label,
                trades=len(period_trades),
                win_rate=(Decimal(wins) / Decimal(len(period_trades))) * Decimal("100"),
                total_pnl=total_pnl,
                return_pct=return_pct,
                start_equity=start_equity,
                end_equity=end_equity,
                max_drawdown=max_drawdown,
                max_drawdown_pct=max_drawdown_pct,
            )
        )
        realized_before_period += total_pnl
    return stats


def _build_report(trades: list[BacktestTrade], *, initial_capital: Decimal) -> BacktestReport:
    total_trades = len(trades)
    if total_trades == 0:
        return BacktestReport(
            total_trades=0,
            win_trades=0,
            loss_trades=0,
            breakeven_trades=0,
            win_rate=Decimal("0"),
            total_pnl=Decimal("0"),
            average_pnl=Decimal("0"),
            gross_profit=Decimal("0"),
            gross_loss=Decimal("0"),
            profit_factor=None,
            average_win=Decimal("0"),
            average_loss=Decimal("0"),
            profit_loss_ratio=None,
            average_r_multiple=Decimal("0"),
            max_drawdown=Decimal("0"),
            max_drawdown_pct=Decimal("0"),
            take_profit_hits=0,
            stop_loss_hits=0,
            ending_equity=initial_capital,
            total_return_pct=Decimal("0"),
            maker_fees=Decimal("0"),
            taker_fees=Decimal("0"),
            total_fees=Decimal("0"),
            slippage_costs=Decimal("0"),
            funding_costs=Decimal("0"),
        )

    wins = [trade for trade in trades if trade.pnl > 0]
    losses = [trade for trade in trades if trade.pnl < 0]
    breakevens = [trade for trade in trades if trade.pnl == 0]
    gross_profit = sum((trade.pnl for trade in wins), Decimal("0"))
    gross_loss = abs(sum((trade.pnl for trade in losses), Decimal("0")))
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    slippage_costs = sum((trade.slippage_cost for trade in trades), Decimal("0"))
    funding_costs = sum((trade.funding_cost for trade in trades), Decimal("0"))
    maker_fees = Decimal("0")
    taker_fees = Decimal("0")
    for trade in trades:
        if trade.entry_fee_type == "maker":
            maker_fees += trade.entry_fee
        elif trade.entry_fee_type == "taker":
            taker_fees += trade.entry_fee
        if trade.exit_fee_type == "maker":
            maker_fees += trade.exit_fee
        elif trade.exit_fee_type == "taker":
            taker_fees += trade.exit_fee
    total_fees = maker_fees + taker_fees
    average_pnl = total_pnl / Decimal(total_trades)
    average_win = gross_profit / Decimal(len(wins)) if wins else Decimal("0")
    average_loss = gross_loss / Decimal(len(losses)) if losses else Decimal("0")
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    profit_loss_ratio = None if average_loss == 0 else average_win / average_loss
    average_r_multiple = sum((trade.r_multiple for trade in trades), Decimal("0")) / Decimal(total_trades)

    equity = initial_capital
    peak = initial_capital
    max_drawdown = Decimal("0")
    for trade in trades:
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    max_drawdown_pct = Decimal("0") if peak <= 0 else (max_drawdown / peak) * Decimal("100")
    ending_equity = initial_capital + total_pnl
    total_return_pct = Decimal("0") if initial_capital <= 0 else (total_pnl / initial_capital) * Decimal("100")

    return BacktestReport(
        total_trades=total_trades,
        win_trades=len(wins),
        loss_trades=len(losses),
        breakeven_trades=len(breakevens),
        win_rate=(Decimal(len(wins)) / Decimal(total_trades)) * Decimal("100"),
        total_pnl=total_pnl,
        average_pnl=average_pnl,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=profit_factor,
        average_win=average_win,
        average_loss=average_loss,
        profit_loss_ratio=profit_loss_ratio,
        average_r_multiple=average_r_multiple,
        max_drawdown=max_drawdown,
        max_drawdown_pct=max_drawdown_pct,
        take_profit_hits=sum(1 for trade in trades if trade.exit_reason == "take_profit"),
        stop_loss_hits=sum(1 for trade in trades if trade.exit_reason == "stop_loss"),
        ending_equity=ending_equity,
        total_return_pct=total_return_pct,
        maker_fees=maker_fees,
        taker_fees=taker_fees,
        total_fees=total_fees,
        slippage_costs=slippage_costs,
        funding_costs=funding_costs,
    )


def _backtest_trade_start_index(minimum_candles: int) -> int:
    return max(max(minimum_candles - 1, 0), BACKTEST_RESERVED_CANDLES)


def _format_fee_rate_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def _format_backtest_sizing_mode(value: str) -> str:
    if value == "fixed_risk":
        return "固定风险金"
    if value == "fixed_size":
        return "固定数量"
    if value == "risk_percent":
        return "风险百分比"
    return value
