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
from okx_quant.persistence import backtest_report_export_dir_path, reports_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_MTF_SHORT_ID


REPORTS_DIR = reports_dir_path()
DETAIL_DIR = backtest_report_export_dir_path()
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
    ("15m", "15m", "1H"),
    ("1H", "1H", "4H"),
)
EXPORT_DATE = datetime.now()
CANDLE_LIMIT = 10000
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


def build_config(symbol: str, bar: str, filter_bar: str) -> StrategyConfig:
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
        signal_mode="short_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=STRATEGY_DYNAMIC_MTF_SHORT_ID,
        risk_amount=Decimal("100"),
        mtf_filter_bar=filter_bar,
        mtf_filter_fast_ema_period=21,
        mtf_filter_slow_ema_period=55,
    )


def format_percent(rate: Decimal) -> str:
    return f"{format_decimal_fixed(rate * Decimal('100'), 4)}%"


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8-sig")


def detail_report_path(symbol: str, bar: str, filter_bar: str) -> Path:
    stamp = EXPORT_DATE.strftime("%Y%m%d")
    return DETAIL_DIR / f"ema_dynamic_mtf_short_{bar}_{filter_bar}_{symbol}_{CANDLE_LIMIT}_{stamp}.txt"


def build_detail_report(
    *,
    symbol: str,
    bar_label: str,
    filter_bar_label: str,
    successes: list[tuple[StrategyConfig, object]],
    failures: list[tuple[StrategyConfig, str]],
    data_source_note: str,
) -> str:
    lines = [
        "EMA 动态委托 多周期空头 批量回测报告",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        f"交易对：{symbol}",
        f"入场周期：{bar_label}",
        f"过滤周期：{filter_bar_label}",
        f"回测K线数：{CANDLE_LIMIT}",
        (
            "策略参数：EMA21 / 趋势EMA55 / ATR10 / 风险金100 / "
            f"高周期EMA21/55 / 手续费 M/T = {format_percent(MAKER_FEE_RATE)} / {format_percent(TAKER_FEE_RATE)}"
        ),
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
                "未生成结果",
            )
            row.append(f"失败：{fail}")
        lines.append(" | ".join(row))
    if successes:
        lines.extend(["", "成功组合详情", "-" * 88])
        for index, (cfg, result) in enumerate(
            sorted(successes, key=lambda item: (item[0].atr_stop_multiplier, item[0].atr_take_multiplier)),
            start=1,
        ):
            lines.extend(
                [
                    f"[{index}] SL x{format_decimal(cfg.atr_stop_multiplier)} / TP x{format_decimal(cfg.atr_take_multiplier)}",
                    (
                        f"总盈亏：{format_decimal_fixed(result.report.total_pnl, 4)} | "
                        f"胜率：{format_decimal_fixed(result.report.win_rate, 2)}% | "
                        f"交易数：{result.report.total_trades} | "
                        f"最大回撤：{format_decimal_fixed(result.report.max_drawdown, 4)}"
                    ),
                    "",
                ]
            )
    if failures:
        lines.extend(["", "失败组合", "-" * 88])
        for cfg, message in failures:
            lines.append(
                f"SL x{format_decimal(cfg.atr_stop_multiplier)} / TP x{format_decimal(cfg.atr_take_multiplier)} -> {message}"
            )
    return "\n".join(lines).rstrip() + "\n"


def build_summary_file(group_label: str, entries: list[dict[str, str]]) -> str:
    best_candidates = [item for item in entries if item["best_total_pnl"] != "-"]
    best_entry = max(best_candidates, key=lambda item: Decimal(item["best_total_pnl"])) if best_candidates else None
    lines = [
        f"EMA 动态委托 多周期空头 {group_label} 汇总报告",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        (
            "策略参数：EMA21 / 趋势EMA55 / ATR10 / 风险金100 / "
            f"高周期EMA21/55 / 手续费 M/T = {format_percent(MAKER_FEE_RATE)} / {format_percent(TAKER_FEE_RATE)}"
        ),
    ]
    if best_entry is not None:
        lines.append(
            f"全局最佳：{best_entry['symbol']} | SLx{best_entry['best_sl']} / TPx{best_entry['best_tp']} | 总盈亏 {best_entry['best_total_pnl']}"
        )
    lines.append("")
    for item in entries:
        lines.extend(
            [
                f"{item['symbol']} | 入场 {item['entry_bar']} | 过滤 {item['filter_bar']}",
                f"成功/失败：{item['success_count']}/{item['failure_count']}",
                (
                    f"最佳参数：SLx{item['best_sl']} / TPx{item['best_tp']} | "
                    f"总盈亏 {item['best_total_pnl']} | 胜率 {item['best_win_rate']}% | "
                    f"交易数 {item['best_trades']} | 最大回撤 {item['best_drawdown']}"
                ),
                f"详情报告：{item['report_path']}",
                "-" * 88,
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def build_concise_table(entries: list[dict[str, str]]) -> str:
    ranked = [item for item in entries if item["best_total_pnl"] != "-"]
    ranked.sort(key=lambda item: Decimal(item["best_total_pnl"]), reverse=True)
    lines = [
        "EMA 动态委托 多周期空头 浓缩总表",
        "=" * 88,
        f"导出时间：{EXPORT_DATE.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    for item in entries:
        lines.append(
            f"{item['symbol']} | 入场 {item['entry_bar']} | 过滤 {item['filter_bar']} | "
            f"最佳 SLx{item['best_sl']} / TPx{item['best_tp']} | "
            f"总盈亏 {item['best_total_pnl']} | 胜率 {item['best_win_rate']}% | "
            f"交易数 {item['best_trades']} | 最大回撤 {item['best_drawdown']}"
        )
    lines.extend(["", "综合排序", "-" * 88])
    for index, item in enumerate(ranked, start=1):
        lines.append(
            f"{index}. {item['symbol']} | 入场 {item['entry_bar']} | 过滤 {item['filter_bar']} | 总盈亏 {item['best_total_pnl']}"
        )
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    client = OkxRestClient()
    all_entries: list[dict[str, str]] = []

    for bar, bar_label, filter_bar in TIMEFRAMES:
        group_entries: list[dict[str, str]] = []
        for symbol in SYMBOLS:
            config = build_config(symbol, bar, filter_bar)
            instrument = client.get_instrument(symbol)
            candles = _load_backtest_candles(client, symbol, bar, CANDLE_LIMIT)
            filter_candles = _load_backtest_candles(client, symbol, filter_bar, CANDLE_LIMIT)
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
                        mtf_filter_candles=filter_candles,
                    )
                    successes.append((cfg, result))
                except Exception as exc:
                    failures.append((cfg, str(exc)))

            report_path = detail_report_path(symbol, bar, filter_bar)
            write_text(
                report_path,
                build_detail_report(
                    symbol=symbol,
                    bar_label=bar_label,
                    filter_bar_label=filter_bar,
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
                "symbol": symbol,
                "entry_bar": bar,
                "filter_bar": filter_bar,
                "success_count": str(len(successes)),
                "failure_count": str(len(failures)),
                "best_sl": format_decimal(best_cfg.atr_stop_multiplier) if best_cfg else "-",
                "best_tp": format_decimal(best_cfg.atr_take_multiplier) if best_cfg else "-",
                "best_total_pnl": format_decimal_fixed(best_result.report.total_pnl, 4) if best_result else "-",
                "best_win_rate": format_decimal_fixed(best_result.report.win_rate, 2) if best_result else "-",
                "best_trades": str(best_result.report.total_trades) if best_result else "-",
                "best_drawdown": format_decimal_fixed(best_result.report.max_drawdown, 4) if best_result else "-",
                "report_path": str(report_path),
            }
            group_entries.append(entry)
            all_entries.append(entry)
            print(f"completed {bar}/{filter_bar} {symbol} -> {report_path.name}")

        summary_path = REPORTS_DIR / (
            f"ema_dynamic_mtf_short_{bar.lower()}_{filter_bar.lower()}_{CANDLE_LIMIT}_summary_{EXPORT_DATE.strftime('%Y%m%d')}.txt"
        )
        write_text(summary_path, build_summary_file(f"入场 {bar_label} / 过滤 {filter_bar}", group_entries))
        print(f"summary -> {summary_path}")

    concise_path = REPORTS_DIR / f"ema_dynamic_mtf_short_concise_{EXPORT_DATE.strftime('%Y%m%d')}.txt"
    write_text(concise_path, build_concise_table(all_entries))
    print(f"concise -> {concise_path}")


if __name__ == "__main__":
    main()
