from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal

from okx_quant.engine import build_order_plan
from okx_quant.indicators import ema
from okx_quant.models import Candle, Instrument, StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategies.ema_atr import EmaAtrStrategy
from okx_quant.strategies.ema_dynamic import EmaDynamicOrderStrategy
from okx_quant.strategy_catalog import STRATEGY_CROSS_ID, STRATEGY_DYNAMIC_ID


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
    pnl: Decimal
    risk_value: Decimal
    r_multiple: Decimal
    exit_reason: str


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
    take_profit_hits: int
    stop_loss_hits: int


@dataclass(frozen=True)
class BacktestResult:
    candles: list[Candle]
    trades: list[BacktestTrade]
    report: BacktestReport
    instrument: Instrument
    ema_values: list[Decimal] = field(default_factory=list)
    trend_ema_values: list[Decimal] = field(default_factory=list)
    ema_period: int = 21
    trend_ema_period: int = 55
    strategy_id: str = STRATEGY_DYNAMIC_ID
    data_source_note: str = ""


@dataclass
class _OpenPosition:
    signal: str
    entry_index: int
    entry_ts: int
    entry_price: Decimal
    stop_loss: Decimal
    take_profit: Decimal
    size: Decimal


def run_backtest(
    client: OkxRestClient,
    config: StrategyConfig,
    *,
    candle_limit: int = 200,
) -> BacktestResult:
    if candle_limit <= 0:
        raise ValueError("回测 K 线数量必须大于 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"回测最多支持 {MAX_BACKTEST_CANDLES} 根 K 线")

    instrument = client.get_instrument(config.inst_id)
    candles = _load_backtest_candles(client, config.inst_id, config.bar, candle_limit)
    return _run_backtest_with_loaded_data(
        candles,
        instrument,
        config,
        data_source_note=_build_backtest_data_source_note(client),
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


def run_backtest_batch(
    client: OkxRestClient,
    base_config: StrategyConfig,
    *,
    candle_limit: int = 200,
    atr_multipliers: tuple[Decimal, ...] = ATR_BATCH_MULTIPLIERS,
    take_ratios: tuple[Decimal, ...] = ATR_BATCH_TAKE_RATIOS,
) -> list[tuple[StrategyConfig, BacktestResult]]:
    if candle_limit <= 0:
        raise ValueError("\u56de\u6d4b K \u7ebf\u6570\u91cf\u5fc5\u987b\u5927\u4e8e 0")
    if candle_limit > MAX_BACKTEST_CANDLES:
        raise ValueError(f"\u56de\u6d4b\u6700\u591a\u652f\u6301 {MAX_BACKTEST_CANDLES} \u6839 K \u7ebf")

    instrument = client.get_instrument(base_config.inst_id)
    candles = _load_backtest_candles(client, base_config.inst_id, base_config.bar, candle_limit)
    data_source_note = _build_backtest_data_source_note(client)
    results: list[tuple[StrategyConfig, BacktestResult]] = []
    for config in build_atr_batch_configs(base_config, atr_multipliers=atr_multipliers, take_ratios=take_ratios):
        results.append((config, _run_backtest_with_loaded_data(candles, instrument, config, data_source_note=data_source_note)))
    return results


def _run_backtest_with_loaded_data(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
    *,
    data_source_note: str = "",
) -> BacktestResult:
    if config.strategy_id == STRATEGY_DYNAMIC_ID:
        trades = _run_dynamic_backtest(candles, instrument, config)
    elif config.strategy_id == STRATEGY_CROSS_ID:
        trades = _run_cross_backtest(candles, instrument, config)
    else:
        raise RuntimeError(f"暂不支持的回测策略：{config.strategy_id}")
    ema_values = ema([candle.close for candle in candles], config.ema_period) if candles else []
    trend_ema_values = ema([candle.close for candle in candles], config.trend_ema_period) if candles else []

    return BacktestResult(
        candles=candles,
        trades=trades,
        report=_build_report(trades),
        instrument=instrument,
        ema_values=ema_values,
        trend_ema_values=trend_ema_values,
        ema_period=config.ema_period,
        trend_ema_period=config.trend_ema_period,
        strategy_id=config.strategy_id,
        data_source_note=data_source_note,
    )


def format_backtest_report(result: BacktestResult) -> str:
    report = result.report
    start_time = _format_backtest_timestamp(result.candles[0].ts) if result.candles else "-"
    end_time = _format_backtest_timestamp(result.candles[-1].ts) if result.candles else "-"
    lines = [
        f"回测K线数：{len(result.candles)}",
        f"开始时间：{start_time}",
        f"结束时间：{end_time}",
        f"预热K线：前 {min(BACKTEST_RESERVED_CANDLES, len(result.candles))} 根仅用于指标预热与绘图，不参与回测",
        f"交易次数：{report.total_trades}",
        f"胜率：{format_decimal_fixed(report.win_rate, 2)}%",
        f"总盈亏：{format_decimal(report.total_pnl)}",
        f"平均每笔：{format_decimal_fixed(report.average_pnl, 4)}",
        f"平均R倍数：{format_decimal_fixed(report.average_r_multiple, 4)}",
        f"最大回撤：{format_decimal_fixed(report.max_drawdown, 4)}",
        f"止盈触发次数：{report.take_profit_hits}",
        f"止损触发次数：{report.stop_loss_hits}",
    ]
    if result.strategy_id == STRATEGY_DYNAMIC_ID:
        lines.append(
            f"趋势过滤：EMA{result.ema_period} > EMA{result.trend_ema_period} 才做多，"
            f"EMA{result.ema_period} < EMA{result.trend_ema_period} 才做空"
        )
        lines.append("同K线撮合：阳线按 O→L→H→C，阴线按 O→H→L→C，十字线不做同K线平仓")
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
) -> list[Candle]:
    history_fetcher = getattr(client, "get_candles_history", None)
    if callable(history_fetcher):
        raw_candles = history_fetcher(inst_id, bar, limit=candle_limit)
    else:
        raw_candles = client.get_candles(inst_id, bar, limit=candle_limit)
    candles = [candle for candle in raw_candles if candle.confirmed]
    return candles[-candle_limit:]


def _build_backtest_data_source_note(client: OkxRestClient) -> str:
    stats = getattr(client, "last_candle_history_stats", None)
    if not isinstance(stats, dict):
        return ""
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


def _run_cross_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
) -> list[BacktestTrade]:
    strategy = EmaAtrStrategy()
    minimum = max(config.ema_period + 2, config.atr_period + 2)
    if len(candles) < minimum:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum} 根")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return []

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]
        if open_position is not None:
            closed_trade = _try_close_position(open_position, candle, index)
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
        open_position = _OpenPosition(
            signal=plan.signal,
            entry_index=index,
            entry_ts=plan.candle_ts,
            entry_price=plan.entry_reference,
            stop_loss=plan.stop_loss,
            take_profit=plan.take_profit,
            size=plan.size,
        )

    return trades


def _run_dynamic_backtest(
    candles: list[Candle],
    instrument: Instrument,
    config: StrategyConfig,
) -> list[BacktestTrade]:
    strategy = EmaDynamicOrderStrategy()
    minimum = max(config.ema_period, config.atr_period)
    if len(candles) < minimum + 1:
        raise RuntimeError(f"已收盘 K 线不足，至少需要 {minimum + 1} 根")
    trade_start_index = _backtest_trade_start_index(minimum)
    if len(candles) <= trade_start_index:
        return []

    trades: list[BacktestTrade] = []
    open_position: _OpenPosition | None = None
    active_plan = None

    for index in range(trade_start_index, len(candles)):
        candle = candles[index]

        if open_position is not None:
            closed_trade = _try_close_position(open_position, candle, index)
            if closed_trade is not None:
                trades.append(closed_trade)
                open_position = None

        if active_plan is not None and open_position is None:
            filled_position = _try_fill_dynamic_order(active_plan, candle, index)
            active_plan = None
            if filled_position is not None:
                closed_trade = _try_close_position_same_candle_after_fill(filled_position, candle, index)
                if closed_trade is not None:
                    trades.append(closed_trade)
                else:
                    open_position = filled_position

        if open_position is not None or index >= len(candles) - 1:
            continue

        decision = strategy.evaluate(candles[: index + 1], config)
        if decision.signal is None:
            continue
        if decision.entry_reference is None or decision.atr_value is None or decision.candle_ts is None:
            continue

        active_plan = build_order_plan(
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

    return trades


def _try_fill_dynamic_order(plan, candle: Candle, candle_index: int) -> _OpenPosition | None:
    filled = candle.low <= plan.entry_reference <= candle.high
    if not filled:
        return None

    return _OpenPosition(
        signal=plan.signal,
        entry_index=candle_index,
        entry_ts=candle.ts,
        entry_price=plan.entry_reference,
        stop_loss=plan.stop_loss,
        take_profit=plan.take_profit,
        size=plan.size,
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


def _build_closed_trade(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    exit_price: Decimal,
    exit_reason: str,
) -> BacktestTrade:
    if position.signal == "long":
        pnl = (exit_price - position.entry_price) * position.size
    else:
        pnl = (position.entry_price - exit_price) * position.size
    risk_value = abs(position.entry_price - position.stop_loss) * position.size
    r_multiple = Decimal("0") if risk_value == 0 else pnl / risk_value
    return BacktestTrade(
        signal=position.signal,
        entry_index=position.entry_index,
        exit_index=candle_index,
        entry_ts=position.entry_ts,
        exit_ts=candle.ts,
        entry_price=position.entry_price,
        exit_price=exit_price,
        stop_loss=position.stop_loss,
        take_profit=position.take_profit,
        size=position.size,
        pnl=pnl,
        risk_value=risk_value,
        r_multiple=r_multiple,
        exit_reason=exit_reason,
    )


def _try_close_position_same_candle_after_fill(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
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
            touched_exit = _first_touched_exit_on_segment(
                entry_price,
                segment_end,
                stop_loss=position.stop_loss,
                take_profit=position.take_profit,
            )
            if touched_exit is not None:
                exit_price, exit_reason = touched_exit
                return _build_closed_trade(
                    position,
                    candle,
                    candle_index,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                )
            entry_reached = True
        else:
            touched_exit = _first_touched_exit_on_segment(
                segment_start,
                segment_end,
                stop_loss=position.stop_loss,
                take_profit=position.take_profit,
            )
            if touched_exit is not None:
                exit_price, exit_reason = touched_exit
                return _build_closed_trade(
                    position,
                    candle,
                    candle_index,
                    exit_price=exit_price,
                    exit_reason=exit_reason,
                )
        segment_start = segment_end
    return None


def _try_close_position(
    position: _OpenPosition,
    candle: Candle,
    candle_index: int,
    *,
    allow_same_candle: bool = False,
) -> BacktestTrade | None:
    if candle_index < position.entry_index:
        return None
    if not allow_same_candle and candle_index == position.entry_index:
        return None

    if position.signal == "long":
        stop_hit = candle.low <= position.stop_loss
        take_hit = candle.high >= position.take_profit
        if stop_hit:
            exit_price = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
    else:
        stop_hit = candle.high >= position.stop_loss
        take_hit = candle.low <= position.take_profit
        if stop_hit:
            exit_price = position.stop_loss
            exit_reason = "stop_loss"
        elif take_hit:
            exit_price = position.take_profit
            exit_reason = "take_profit"
        else:
            return None
    return _build_closed_trade(
        position,
        candle,
        candle_index,
        exit_price=exit_price,
        exit_reason=exit_reason,
    )


def _build_report(trades: list[BacktestTrade]) -> BacktestReport:
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
            take_profit_hits=0,
            stop_loss_hits=0,
        )

    wins = [trade for trade in trades if trade.pnl > 0]
    losses = [trade for trade in trades if trade.pnl < 0]
    breakevens = [trade for trade in trades if trade.pnl == 0]
    gross_profit = sum((trade.pnl for trade in wins), Decimal("0"))
    gross_loss = abs(sum((trade.pnl for trade in losses), Decimal("0")))
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    average_pnl = total_pnl / Decimal(total_trades)
    average_win = gross_profit / Decimal(len(wins)) if wins else Decimal("0")
    average_loss = gross_loss / Decimal(len(losses)) if losses else Decimal("0")
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    profit_loss_ratio = None if average_loss == 0 else average_win / average_loss
    average_r_multiple = sum((trade.r_multiple for trade in trades), Decimal("0")) / Decimal(total_trades)

    equity = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for trade in trades:
        equity += trade.pnl
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown

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
        take_profit_hits=sum(1 for trade in trades if trade.exit_reason == "take_profit"),
        stop_loss_hits=sum(1 for trade in trades if trade.exit_reason == "stop_loss"),
    )


def _backtest_trade_start_index(minimum_candles: int) -> int:
    return max(max(minimum_candles - 1, 0), BACKTEST_RESERVED_CANDLES)
