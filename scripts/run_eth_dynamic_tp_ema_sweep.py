from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _run_backtest_with_loaded_data
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal, format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_DYNAMIC_SHORT_ID


REPORTS_DIR = analysis_report_dir_path()
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "ETH-USDT-SWAP"
TIMEFRAMES = (("1H", "1小时"), ("4H", "4小时"))
DIRECTIONS = (
    (STRATEGY_DYNAMIC_LONG_ID, "long_only", "做多"),
    (STRATEGY_DYNAMIC_SHORT_ID, "short_only", "做空"),
)
EMA_PROFILES = (
    (5, 13),
    (8, 21),
    (13, 34),
    (21, 55),
    (34, 89),
    (55, 144),
)
ENTRY_REFERENCE_EMAS = (0, 5, 8, 13, 21, 34, 55)
ATR_STOP_MULTIPLIERS = (Decimal("1"), Decimal("1.5"), Decimal("2"))
CANDLE_LIMIT = 10000
ATR_PERIOD = 10
RISK_AMOUNT = Decimal("100")
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


@dataclass(frozen=True)
class SweepRow:
    bar: str
    bar_label: str
    direction: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal
    total_pnl: Decimal
    win_rate: Decimal
    total_trades: int
    max_drawdown: Decimal
    avg_r: Decimal

    @property
    def entry_ema_label(self) -> str:
        if self.entry_reference_ema_period <= 0:
            return f"跟随快线EMA{self.ema_period}"
        return f"EMA{self.entry_reference_ema_period}"

    @property
    def trend_label(self) -> str:
        return f"EMA{self.ema_period}/EMA{self.trend_ema_period}"

    @property
    def pnl_dd_ratio(self) -> Decimal:
        if self.max_drawdown == 0:
            return Decimal("0")
        return self.total_pnl / self.max_drawdown


def build_config(
    *,
    bar: str,
    strategy_id: str,
    signal_mode: str,
    ema_period: int,
    trend_ema_period: int,
    entry_reference_ema_period: int,
    stop_atr: Decimal,
) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=bar,
        ema_period=ema_period,
        trend_ema_period=trend_ema_period,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=stop_atr,
        atr_take_multiplier=stop_atr * Decimal("2"),
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
    )


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8-sig")


def load_market_data(client: OkxRestClient) -> dict[str, tuple[object, list, str]]:
    cache: dict[str, tuple[object, list, str]] = {}
    instrument = client.get_instrument(SYMBOL)
    for bar, bar_label in TIMEFRAMES:
        print(f"load {SYMBOL} {bar_label} {CANDLE_LIMIT} candles")
        candles = _load_backtest_candles(client, SYMBOL, bar, CANDLE_LIMIT)
        note = _build_backtest_data_source_note(client)
        cache[bar] = (instrument, candles, note)
    return cache


def run_sweep(client: OkxRestClient) -> tuple[list[SweepRow], str]:
    cache = load_market_data(client)
    rows: list[SweepRow] = []
    total = len(TIMEFRAMES) * len(DIRECTIONS) * len(EMA_PROFILES) * len(ENTRY_REFERENCE_EMAS) * len(ATR_STOP_MULTIPLIERS)
    sequence = 0
    data_source_note = ""
    for bar, bar_label in TIMEFRAMES:
        instrument, candles, note = cache[bar]
        data_source_note = note
        for strategy_id, signal_mode, direction_label in DIRECTIONS:
            for ema_period, trend_ema_period in EMA_PROFILES:
                if ema_period >= trend_ema_period:
                    continue
                for entry_reference_ema_period in ENTRY_REFERENCE_EMAS:
                    resolved_entry_ema = ema_period if entry_reference_ema_period <= 0 else entry_reference_ema_period
                    if resolved_entry_ema > trend_ema_period:
                        continue
                    for stop_atr in ATR_STOP_MULTIPLIERS:
                        sequence += 1
                        print(
                            f"[{sequence}/{total}] {bar_label} {direction_label} "
                            f"EMA{ema_period}/EMA{trend_ema_period} 入场{resolved_entry_ema} "
                            f"动态止盈 SLx{format_decimal(stop_atr)}"
                        )
                        config = build_config(
                            bar=bar,
                            strategy_id=strategy_id,
                            signal_mode=signal_mode,
                            ema_period=ema_period,
                            trend_ema_period=trend_ema_period,
                            entry_reference_ema_period=entry_reference_ema_period,
                            stop_atr=stop_atr,
                        )
                        result = _run_backtest_with_loaded_data(
                            candles,
                            instrument,
                            config,
                            data_source_note=note,
                            maker_fee_rate=MAKER_FEE_RATE,
                            taker_fee_rate=TAKER_FEE_RATE,
                        )
                        rows.append(
                            SweepRow(
                                bar=bar,
                                bar_label=bar_label,
                                direction=direction_label,
                                ema_period=ema_period,
                                trend_ema_period=trend_ema_period,
                                entry_reference_ema_period=entry_reference_ema_period,
                                atr_stop_multiplier=stop_atr,
                                total_pnl=result.report.total_pnl,
                                win_rate=result.report.win_rate,
                                total_trades=result.report.total_trades,
                                max_drawdown=result.report.max_drawdown,
                                avg_r=result.report.average_r_multiple,
                            )
                        )
    return rows, data_source_note


def export_csv(path: Path, rows: list[SweepRow]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.writer(fp)
        writer.writerow(
            [
                "bar",
                "bar_label",
                "direction",
                "ema_period",
                "trend_ema_period",
                "entry_reference_ema_period",
                "entry_ema_label",
                "atr_stop_multiplier",
                "total_pnl",
                "win_rate",
                "total_trades",
                "max_drawdown",
                "pnl_dd_ratio",
                "avg_r",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.bar,
                    row.bar_label,
                    row.direction,
                    row.ema_period,
                    row.trend_ema_period,
                    row.entry_reference_ema_period,
                    row.entry_ema_label,
                    format_decimal(row.atr_stop_multiplier),
                    format_decimal_fixed(row.total_pnl, 4),
                    format_decimal_fixed(row.win_rate, 2),
                    row.total_trades,
                    format_decimal_fixed(row.max_drawdown, 4),
                    format_decimal_fixed(row.pnl_dd_ratio, 4),
                    format_decimal_fixed(row.avg_r, 4),
                ]
            )


def build_text_report(rows: list[SweepRow], data_source_note: str, exported_at: datetime, csv_path: Path) -> str:
    best_by_group: dict[tuple[str, str], SweepRow] = {}
    best_by_profile: dict[tuple[str, str, int, int], SweepRow] = {}
    for row in rows:
        key = (row.bar_label, row.direction)
        current = best_by_group.get(key)
        if current is None or row.total_pnl > current.total_pnl:
            best_by_group[key] = row
        pkey = (row.bar_label, row.direction, row.ema_period, row.trend_ema_period)
        current_profile = best_by_profile.get(pkey)
        if current_profile is None or row.total_pnl > current_profile.total_pnl:
            best_by_profile[pkey] = row

    lines = [
        "ETH EMA动态委托动态止盈均线扫参报告",
        "=" * 88,
        f"导出时间：{exported_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"标的：{SYMBOL}",
        "策略：EMA动态委托 做多/做空",
        "出场口径：动态止盈",
        "周期：1H、4H",
        "均线组合：5/13、8/21、13/34、21/55、34/89、55/144",
        "挂单参考EMA：跟随快线、5、8、13、21、34、55",
        "止损：1 / 1.5 / 2 ATR",
        "动态止盈设置：2R保本=开启；手续费偏移=开启；每波开仓次数=1",
        f"数据来源：{data_source_note}",
        f"全量结果：{csv_path}",
        "",
        "一、各方向各周期最佳结果",
    ]
    for key in sorted(best_by_group):
        row = best_by_group[key]
        lines.append(
            f"{row.bar_label} | {row.direction} | {row.trend_label} | 挂单{row.entry_ema_label} | "
            f"SLx{format_decimal(row.atr_stop_multiplier)} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | "
            f"胜率 {format_decimal_fixed(row.win_rate, 2)}% | 交易数 {row.total_trades} | "
            f"最大回撤 {format_decimal_fixed(row.max_drawdown, 4)} | 盈亏/回撤 {format_decimal_fixed(row.pnl_dd_ratio, 4)}"
        )

    lines.extend(["", "二、每组均线的最佳形态"])
    for key in sorted(best_by_profile):
        row = best_by_profile[key]
        lines.append(
            f"{row.bar_label} | {row.direction} | {row.trend_label} | 最佳挂单{row.entry_ema_label} | "
            f"SLx{format_decimal(row.atr_stop_multiplier)} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | "
            f"胜率 {format_decimal_fixed(row.win_rate, 2)}% | 交易数 {row.total_trades} | "
            f"最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    top_rows = sorted(rows, key=lambda item: item.total_pnl, reverse=True)[:20]
    lines.extend(["", "三、全局前20"])
    for index, row in enumerate(top_rows, start=1):
        lines.append(
            f"{index}. {row.bar_label} | {row.direction} | {row.trend_label} | 挂单{row.entry_ema_label} | "
            f"SLx{format_decimal(row.atr_stop_multiplier)} | 总盈亏 {format_decimal_fixed(row.total_pnl, 4)} | "
            f"胜率 {format_decimal_fixed(row.win_rate, 2)}% | 交易数 {row.total_trades} | "
            f"最大回撤 {format_decimal_fixed(row.max_drawdown, 4)}"
        )

    lines.extend(
        [
            "",
            "四、交易员分析角度",
            "1. 动态止盈下，核心不是固定TP倍数，而是止损宽度决定了后续2R、3R、4R的触发节奏。",
            "2. 同一组信号EMA下，如果挂单EMA更靠近价格，通常会换来更多成交和更高收益，但也可能带来更大回撤。",
            "3. 如果挂单34优于挂单55，要继续看是因为进场更积极，还是因为在同样止损下能更早吃到趋势段。",
            "4. 比较时要优先看同一止损ATR下的挂单EMA差异，避免把挂单锚点和止损宽度混在一起解释。",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    exported_at = datetime.now()
    stamp = exported_at.strftime("%Y%m%d_%H%M%S")
    client = OkxRestClient()
    rows, data_source_note = run_sweep(client)

    csv_path = REPORTS_DIR / f"ETH_dynamic_tp_ema_sweep_{stamp}.csv"
    txt_path = REPORTS_DIR / f"ETH_dynamic_tp_ema_sweep_{stamp}.txt"
    json_path = REPORTS_DIR / f"ETH_dynamic_tp_ema_sweep_{stamp}.json"

    export_csv(csv_path, rows)
    write_text(txt_path, build_text_report(rows, data_source_note, exported_at, csv_path))
    json_path.write_text(
        json.dumps(
            {
                "exported_at": exported_at.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": SYMBOL,
                "timeframes": [item[0] for item in TIMEFRAMES],
                "ema_profiles": [list(item) for item in EMA_PROFILES],
                "entry_reference_emas": list(ENTRY_REFERENCE_EMAS),
                "atr_stop_multipliers": [str(item) for item in ATR_STOP_MULTIPLIERS],
                "take_profit_mode": "dynamic",
                "dynamic_two_r_break_even": True,
                "dynamic_fee_offset_enabled": True,
                "rows": len(rows),
                "csv_path": str(csv_path),
                "txt_path": str(txt_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"report -> {txt_path}")
    print(f"csv -> {csv_path}")
    print(f"json -> {json_path}")


if __name__ == "__main__":
    main()
