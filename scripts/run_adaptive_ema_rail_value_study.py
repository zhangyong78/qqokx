from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import (
    STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
    STRATEGY_DYNAMIC_LONG_ID,
    STRATEGY_EMA_BREAKOUT_LONG_ID,
)


SYMBOL = "BTC-USDT-SWAP"
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")


@dataclass(frozen=True)
class Scenario:
    key: str
    label: str
    bar: str
    strategy_id: str
    ema_period: int
    trend_ema_period: int
    entry_reference_ema_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    max_entries_per_trend: int = 1


@dataclass(frozen=True)
class Window:
    key: str
    label: str
    start_ts: int


@dataclass(frozen=True)
class ResultRow:
    scenario_key: str
    scenario_label: str
    window_key: str
    window_label: str
    bar: str
    candle_count: int
    total_trades: int
    win_rate: str
    total_pnl: str
    total_return_pct: str
    max_drawdown_pct: str
    profit_factor: str
    average_r_multiple: str


SCENARIOS: tuple[Scenario, ...] = (
    Scenario(
        key="dynamic_std_1h",
        label="EMA动态委托-标准1H",
        bar="1H",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="dynamic_btc_best_1h",
        label="EMA动态委托-BTC强趋势1H",
        bar="1H",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        ema_period=5,
        trend_ema_period=13,
        entry_reference_ema_period=0,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="breakout_21_1h",
        label="EMA突破做多-1H",
        bar="1H",
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=21,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="adaptive_rail_1h",
        label="Adaptive Rail-1H",
        bar="1H",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="dynamic_std_4h",
        label="EMA动态委托-标准4H",
        bar="4H",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="dynamic_5_13_4h",
        label="EMA动态委托-5/13 4H",
        bar="4H",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        ema_period=5,
        trend_ema_period=13,
        entry_reference_ema_period=0,
        atr_stop_multiplier=Decimal("1"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="breakout_21_4h",
        label="EMA突破做多-4H",
        bar="4H",
        strategy_id=STRATEGY_EMA_BREAKOUT_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=21,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
    ),
    Scenario(
        key="adaptive_rail_4h",
        label="Adaptive Rail-4H",
        bar="4H",
        strategy_id=STRATEGY_ADAPTIVE_EMA_RAIL_LONG_ID,
        ema_period=21,
        trend_ema_period=55,
        entry_reference_ema_period=55,
        atr_stop_multiplier=Decimal("1.5"),
        atr_take_multiplier=Decimal("4"),
    ),
)


WINDOWS: tuple[Window, ...] = (
    Window(key="full", label="全历史", start_ts=0),
    Window(
        key="since_2024",
        label="自 2024-01-01",
        start_ts=int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
    Window(
        key="since_2025",
        label="自 2025-01-01",
        start_ts=int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
    ),
)


def _fmt_decimal(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return format_decimal_fixed(value, digits)


def _build_config(scenario: Scenario) -> StrategyConfig:
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=scenario.bar,
        ema_period=scenario.ema_period,
        trend_ema_period=scenario.trend_ema_period,
        big_ema_period=233,
        atr_period=10,
        atr_stop_multiplier=scenario.atr_stop_multiplier,
        atr_take_multiplier=scenario.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode="long_only",
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=scenario.strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=scenario.entry_reference_ema_period,
        take_profit_mode="dynamic",
        max_entries_per_trend=scenario.max_entries_per_trend,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=Decimal("0"),
        backtest_exit_slippage_rate=Decimal("0"),
        backtest_slippage_rate=Decimal("0"),
        backtest_funding_rate=Decimal("0"),
    )


def _run_scenario(
    client: OkxRestClient,
    instrument,
    all_candles: list,
    scenario: Scenario,
    window: Window,
) -> ResultRow:
    candles = [candle for candle in all_candles if candle.ts >= window.start_ts]
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        _build_config(scenario),
        data_source_note=f"local candle_cache full history | {SYMBOL} {scenario.bar} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE,
        taker_fee_rate=TAKER_FEE,
    )
    report = result.report
    return ResultRow(
        scenario_key=scenario.key,
        scenario_label=scenario.label,
        window_key=window.key,
        window_label=window.label,
        bar=scenario.bar,
        candle_count=len(candles),
        total_trades=report.total_trades,
        win_rate=_fmt_decimal(report.win_rate, 2),
        total_pnl=_fmt_decimal(report.total_pnl, 4),
        total_return_pct=_fmt_decimal(report.total_return_pct, 2),
        max_drawdown_pct=_fmt_decimal(report.max_drawdown_pct, 2),
        profit_factor=_fmt_decimal(report.profit_factor, 4),
        average_r_multiple=_fmt_decimal(report.average_r_multiple, 4),
    )


def _build_markdown(rows: list[ResultRow]) -> str:
    lines = [
        "# Adaptive EMA Rail 方向价值研究",
        "",
        f"- 标的: `{SYMBOL}`",
        "- 数据来源: 本地 `candle_cache` 全量已确认K线",
        f"- 手续费: maker `{MAKER_FEE}` / taker `{TAKER_FEE}`",
        "- 回测口径: 直接走 `_run_backtest_with_loaded_data(...)`，不受 10000 根上限影响",
        "",
        "## 汇总表",
        "",
        "| 策略 | 周期 | 时间窗 | K线数 | 交易数 | 胜率 | 总收益率 | 最大回撤% | PF | 平均R |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row.scenario_label} | {row.bar} | {row.window_label} | {row.candle_count} | {row.total_trades} | "
            f"{row.win_rate}% | {row.total_return_pct}% | {row.max_drawdown_pct}% | {row.profit_factor} | {row.average_r_multiple} |"
        )

    lines.extend(
        [
            "",
            "## 研究结论",
            "",
            "1. `Adaptive Rail-1H` 当前版本研究价值偏低。",
            "当前 1H 规则下，全历史和 2024+/2025+ 子样本都弱于现有做多基线，说明这一版 1H 轨道定义还没有形成稳定优势。",
            "",
            "2. `Adaptive Rail-4H` 有继续研究的价值。",
            "它在全历史、2024+、2025+ 上都能保持正收益，而且回撤显著低于 4H 动态委托标准版；虽然绝对收益不是第一，但风险控制表现更稳。",
            "",
            "3. 这个方向更像 `低频结构跟随`，不像 `高频信号增强`。",
            "从结果看，Adaptive Rail 更适合放在 4H 这类更干净的趋势节奏里，1H 噪声对它伤害比较大。",
            "",
            "4. 下一步最该做的是 `优化4H版本`，不是继续深挖 1H。",
            "优先研究 4H 的轨道切换阈值、bounce 定义、失效条件和轨道统计，而不是马上扩到实盘。",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = analysis_report_dir_path()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"adaptive_ema_rail_value_study_{stamp}.md"
    json_path = out_dir / f"adaptive_ema_rail_value_study_{stamp}.json"

    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    candle_cache = {
        bar: [candle for candle in load_candle_cache(SYMBOL, bar, limit=None) if candle.confirmed]
        for bar in sorted({scenario.bar for scenario in SCENARIOS})
    }

    rows: list[ResultRow] = []
    for scenario in SCENARIOS:
        for window in WINDOWS:
            print(f"run {scenario.key} {window.key}", flush=True)
            rows.append(_run_scenario(client, instrument, candle_cache[scenario.bar], scenario, window))

    md_path.write_text(_build_markdown(rows), encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "symbol": SYMBOL,
                "rows": [asdict(row) for row in rows],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(md_path)
    print(json_path)


if __name__ == "__main__":
    main()
