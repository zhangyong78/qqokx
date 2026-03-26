from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from okx_quant.backtest import (
    _build_backtest_data_source_note,
    _load_backtest_candles,
    _run_backtest_with_loaded_data,
    build_atr_batch_configs,
)
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_ID


ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = ROOT / "reports"
DETAIL_DIR = REPORTS_DIR / "backtest_exports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DETAIL_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "DOGE-USDT-SWAP",
)
TIMEFRAMES = (
    ("1H", "1小时"),
    ("4H", "4小时"),
)
SIGNALS = (
    ("long_only", "只做多"),
    ("short_only", "只做空"),
)
EXPORT_DATE = datetime.now()
CANDLE_LIMIT = 10000
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


def build_config(symbol: str, bar: str, signal_mode: str) -> StrategyConfig:
    return StrategyConfig(
        inst_id=symbol,
        bar=bar,
        ema_period=21,
        trend_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("1"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_ID,
        risk_amount=Decimal("100"),
    )


def format_timestamp(ts: int | None) -> str:
    if ts is None:
        return "-"
    if ts >= 10**12:
        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M")
    if ts >= 10**9:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    return str(ts)


def format_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def detail_report_path(symbol: str, bar: str, signal_mode: str) -> Path:
    return DETAIL_DIR / (
        f"ema_dynamic_{bar}_{symbol}_{signal_mode}_{CANDLE_LIMIT}_{EXPORT_DATE.strftime('%Y%m%d')}.txt"
    )


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8-sig")


def build_detail_report(
    *,
    symbol: str,
    bar_label: str,
    signal_label: str,
    successes: list[tuple[StrategyConfig, object]],
    failures: list[tuple[StrategyConfig, str]],
    data_source_note: str,
) -> str:
    starts = [result.candles[0].ts for _, result in successes if result.candles]
    ends = [result.candles[-1].ts for _, result in successes if result.candles]
    lines = [
        "EMA动态委托批量回测报告",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        f"交易对：{symbol}",
        f"周期：{bar_label}",
        f"信号方向：{signal_label}",
        f"回测K线数：{CANDLE_LIMIT}",
        f"开始时间：{format_timestamp(min(starts)) if starts else '-'}",
        f"结束时间：{format_timestamp(max(ends)) if ends else '-'}",
        f"策略参数：EMA21 / 趋势EMA55 / ATR10 / 风险金100 / 手续费M/T = {format_percent(MAKER_FEE_RATE)} / {format_percent(TAKER_FEE_RATE)}",
    ]
    if data_source_note:
        lines.append(f"数据来源：{data_source_note}")
    lines.extend(
        [
            f"成功组合：{len(successes)}/9",
            f"失败组合：{len(failures)}/9",
            "",
            "矩阵摘要",
            "-" * 88,
            "SL \\ TP | TP = SL x1 | TP = SL x2 | TP = SL x3",
        ]
    )
    for stop_multiplier in (Decimal("1"), Decimal("1.5"), Decimal("2")):
        row = [f"SL x{format_decimal(stop_multiplier)}"]
        for take_ratio in (1, 2, 3):
            take_multiplier = stop_multiplier * Decimal(str(take_ratio))
            hit = next(
                (
                    result
                    for cfg, result in successes
                    if cfg.atr_stop_multiplier == stop_multiplier and cfg.atr_take_multiplier == take_multiplier
                ),
                None,
            )
            if hit is not None:
                row.append(
                    f"{format_decimal_fixed(hit.report.total_pnl, 4)} | "
                    f"{format_decimal_fixed(hit.report.win_rate, 2)}% | "
                    f"{hit.report.total_trades}笔"
                )
                continue
            fail = next(
                (
                    message
                    for cfg, message in failures
                    if cfg.atr_stop_multiplier == stop_multiplier and cfg.atr_take_multiplier == take_multiplier
                ),
                "未生成",
            )
            row.append(f"失败：{fail}")
        lines.append(" | ".join(row))
    if failures:
        lines.extend(["", "失败组合", "-" * 88])
        for cfg, message in failures:
            lines.append(
                f"SL x{format_decimal(cfg.atr_stop_multiplier)} / TP x{format_decimal(cfg.atr_take_multiplier)} -> {message}"
            )
    if successes:
        lines.extend(["", "成功组合详细结果", "-" * 88])
        for index, (cfg, result) in enumerate(
            sorted(successes, key=lambda item: (item[0].atr_stop_multiplier, item[0].atr_take_multiplier)),
            start=1,
        ):
            lines.extend(
                [
                    f"[{index}] SL x{format_decimal(cfg.atr_stop_multiplier)} / TP x{format_decimal(cfg.atr_take_multiplier)}",
                    f"总盈亏：{format_decimal_fixed(result.report.total_pnl, 4)} | 胜率：{format_decimal_fixed(result.report.win_rate, 2)}% | "
                    f"交易数：{result.report.total_trades} | 最大回撤：{format_decimal_fixed(result.report.max_drawdown, 4)}",
                    "",
                ]
            )
    return "\n".join(lines).rstrip() + "\n"


def build_summary_file(timeframe_label: str, entries: list[dict[str, str]]) -> str:
    success_groups = sum(int(item["success_count"]) for item in entries)
    total_groups = sum(int(item["total_count"]) for item in entries)
    best_candidates = [item for item in entries if item["best_total_pnl"] != "-"]
    best_entry = max(best_candidates, key=lambda item: Decimal(item["best_total_pnl"])) if best_candidates else None
    lines = [
        f"EMA动态委托 {timeframe_label} 10000根K线 汇总报告",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        f"策略参数：EMA21 / 趋势EMA55 / ATR10 / 风险金100 / 手续费M/T = {format_percent(MAKER_FEE_RATE)} / {format_percent(TAKER_FEE_RATE)}",
        "回测口径：前200根K线只做预热，不参与交易；使用真实OKX历史K线与本地缓存。",
        f"成功组合统计：{success_groups}/{total_groups}",
    ]
    if best_entry is not None:
        lines.append(
            f"全局最佳：{best_entry['symbol']} | {best_entry['signal_label']} | "
            f"SLx{best_entry['best_sl']} / TPx{best_entry['best_tp']} | 总盈亏 {best_entry['best_total_pnl']}"
        )
    lines.append("")
    for item in entries:
        lines.extend(
            [
                f"{item['symbol']} | {item['signal_label']}",
                f"成功/失败：{item['success_count']}/{item['total_count']} 成功，{item['failure_count']} 失败",
                f"最佳参数：SLx{item['best_sl']} / TPx{item['best_tp']}",
                f"最佳结果：总盈亏 {item['best_total_pnl']} | 胜率 {item['best_win_rate']}% | 交易数 {item['best_trades']} | 最大回撤 {item['best_drawdown']}",
                f"详细报告：{item['report_path']}",
                "-" * 88,
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def build_concise_table(all_entries: dict[str, list[dict[str, str]]]) -> str:
    lines = [
        "EMA动态委托 浓缩表（1小时 + 4小时，10000根K线）",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        f"手续费：Maker {format_percent(MAKER_FEE_RATE)} / Taker {format_percent(TAKER_FEE_RATE)}",
        "",
    ]
    for timeframe_label in ("1小时", "4小时"):
        lines.extend([f"{timeframe_label}", "-" * 88])
        for item in all_entries[timeframe_label]:
            lines.append(
                f"{item['symbol']} | {item['signal_label']} | "
                f"最佳 SLx{item['best_sl']} / TPx{item['best_tp']} | "
                f"总盈亏 {item['best_total_pnl']} | 胜率 {item['best_win_rate']}% | "
                f"交易数 {item['best_trades']} | 最大回撤 {item['best_drawdown']} | "
                f"成功 {item['success_count']}/9"
            )
        lines.append("")

    combined = [item for values in all_entries.values() for item in values if item["best_total_pnl"] != "-"]
    combined.sort(key=lambda item: Decimal(item["best_total_pnl"]), reverse=True)
    lines.extend(["综合排序（按最佳总盈亏）", "-" * 88])
    for index, item in enumerate(combined[:12], start=1):
        lines.append(
            f"{index}. {item['timeframe']} | {item['symbol']} | {item['signal_label']} | "
            f"SLx{item['best_sl']} / TPx{item['best_tp']} | 总盈亏 {item['best_total_pnl']}"
        )
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    client = OkxRestClient()
    all_entries: dict[str, list[dict[str, str]]] = {"1小时": [], "4小时": []}

    for bar, bar_label in TIMEFRAMES:
        timeframe_entries: list[dict[str, str]] = []
        for symbol in SYMBOLS:
            for signal_mode, signal_label in SIGNALS:
                config = build_config(symbol, bar, signal_mode)
                instrument = client.get_instrument(symbol)
                candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
                data_source_note = _build_backtest_data_source_note(client)
                successes: list[tuple[StrategyConfig, object]] = []
                failures: list[tuple[StrategyConfig, str]] = []
                for cfg in build_atr_batch_configs(config):
                    try:
                        result = _run_backtest_with_loaded_data(
                            candles,
                            instrument,
                            cfg,
                            data_source_note=data_source_note,
                            maker_fee_rate=MAKER_FEE_RATE,
                            taker_fee_rate=TAKER_FEE_RATE,
                        )
                        successes.append((cfg, result))
                    except Exception as exc:
                        failures.append((cfg, str(exc)))

                report_path = detail_report_path(symbol, bar, signal_mode)
                write_text(
                    report_path,
                    build_detail_report(
                        symbol=symbol,
                        bar_label=bar_label,
                        signal_label=signal_label,
                        successes=successes,
                        failures=failures,
                        data_source_note=data_source_note,
                    ),
                )
                best_cfg = None
                best_result = None
                if successes:
                    best_cfg, best_result = max(successes, key=lambda item: item[1].report.total_pnl)
                entry = {
                    "timeframe": bar_label,
                    "symbol": symbol,
                    "signal_label": signal_label,
                    "success_count": str(len(successes)),
                    "failure_count": str(len(failures)),
                    "total_count": "9",
                    "best_sl": format_decimal(best_cfg.atr_stop_multiplier) if best_cfg else "-",
                    "best_tp": format_decimal(best_cfg.atr_take_multiplier) if best_cfg else "-",
                    "best_total_pnl": format_decimal_fixed(best_result.report.total_pnl, 4) if best_result else "-",
                    "best_win_rate": format_decimal_fixed(best_result.report.win_rate, 2) if best_result else "-",
                    "best_trades": str(best_result.report.total_trades) if best_result else "-",
                    "best_drawdown": format_decimal_fixed(best_result.report.max_drawdown, 4) if best_result else "-",
                    "report_path": str(report_path),
                }
                timeframe_entries.append(entry)
                print(f"completed {bar} {symbol} {signal_mode} -> {report_path.name}")

        summary_path = REPORTS_DIR / f"ema_dynamic_{bar.lower()}_10000_summary_{EXPORT_DATE.strftime('%Y%m%d')}.txt"
        write_text(summary_path, build_summary_file(bar_label, timeframe_entries))
        all_entries[bar_label] = timeframe_entries
        print(f"summary -> {summary_path}")

    concise_path = REPORTS_DIR / f"EMA动态委托_浓缩表_1小时4小时_{EXPORT_DATE.strftime('%Y%m%d')}.txt"
    concise_text = build_concise_table(all_entries)
    write_text(concise_path, concise_text)
    write_text(
        REPORTS_DIR / f"ema_dynamic_concise_1h_4h_{EXPORT_DATE.strftime('%Y%m%d')}.txt",
        concise_text,
    )
    print(f"concise -> {concise_path}")


if __name__ == "__main__":
    main()
