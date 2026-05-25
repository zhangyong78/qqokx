from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from okx_quant.backtest import _run_backtest_with_loaded_data
from okx_quant.candle_cache import load_candle_cache
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.pricing import format_decimal_fixed
from okx_quant.strategy_catalog import STRATEGY_DYNAMIC_LONG_ID, STRATEGY_DYNAMIC_SHORT_ID


REPORTS_DIR = analysis_report_dir_path()
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SYMBOL = "BTC-USDT-SWAP"
MAKER_FEE_RATE = Decimal("0.0001")
TAKER_FEE_RATE = Decimal("0.00028")


@dataclass(frozen=True)
class StrategyScenario:
    key: str
    label: str
    strategy_id: str
    bar: str
    ema_type: str
    ema_period: int
    trend_ema_type: str
    trend_ema_period: int
    entry_reference_ema_type: str
    entry_reference_ema_period: int
    atr_period: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    risk_amount: Decimal
    source_note: str


@dataclass(frozen=True)
class ScenarioResult:
    scenario: StrategyScenario
    candle_count: int
    first_ts: int
    last_ts: int
    total_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    total_return_pct: Decimal
    max_drawdown: Decimal
    max_drawdown_pct: Decimal
    average_r_multiple: Decimal


SCENARIOS = (
    StrategyScenario(
        key="btc_1h_long",
        label="BTC 1H 做多",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        bar="1H",
        ema_type="ema",
        ema_period=21,
        trend_ema_type="ma",
        trend_ema_period=50,
        entry_reference_ema_type="ma",
        entry_reference_ema_period=50,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("2"),
        risk_amount=Decimal("20"),
        source_note="实盘快照 S191 / 2026-05-18，EMA21 + MA50，挂单参考 MA50。",
    ),
    StrategyScenario(
        key="btc_1h_short",
        label="BTC 1H 做空",
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        bar="1H",
        ema_type="ema",
        ema_period=21,
        trend_ema_type="ema",
        trend_ema_period=55,
        entry_reference_ema_type="ema",
        entry_reference_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        risk_amount=Decimal("10"),
        source_note="实盘快照 S171 / 2026-05-17，EMA21 + EMA55，挂单参考 EMA55。",
    ),
    StrategyScenario(
        key="btc_4h_long",
        label="BTC 4H 做多",
        strategy_id=STRATEGY_DYNAMIC_LONG_ID,
        bar="4H",
        ema_type="ema",
        ema_period=21,
        trend_ema_type="ema",
        trend_ema_period=55,
        entry_reference_ema_type="ema",
        entry_reference_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        risk_amount=Decimal("10"),
        source_note="实盘日志 S02 / 2026-04-18，仅找到这条 4H 做多实盘记录，按 EMA21 + EMA55，挂单参考 EMA55 复现。",
    ),
    StrategyScenario(
        key="btc_4h_short",
        label="BTC 4H 做空",
        strategy_id=STRATEGY_DYNAMIC_SHORT_ID,
        bar="4H",
        ema_type="ema",
        ema_period=21,
        trend_ema_type="ema",
        trend_ema_period=55,
        entry_reference_ema_type="ema",
        entry_reference_ema_period=55,
        atr_period=10,
        atr_stop_multiplier=Decimal("2"),
        atr_take_multiplier=Decimal("4"),
        risk_amount=Decimal("10"),
        source_note="实盘快照 S55 / S85，EMA21 + EMA55，挂单参考 EMA55。",
    ),
)


def _fmt_ts(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def build_config(scenario: StrategyScenario) -> StrategyConfig:
    signal_mode = "long_only" if scenario.strategy_id == STRATEGY_DYNAMIC_LONG_ID else "short_only"
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=scenario.bar,
        ema_period=scenario.ema_period,
        ema_type=scenario.ema_type,
        trend_ema_period=scenario.trend_ema_period,
        trend_ema_type=scenario.trend_ema_type,
        entry_reference_ema_period=scenario.entry_reference_ema_period,
        entry_reference_ema_type=scenario.entry_reference_ema_type,
        big_ema_period=233,
        atr_period=scenario.atr_period,
        atr_stop_multiplier=scenario.atr_stop_multiplier,
        atr_take_multiplier=scenario.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=scenario.strategy_id,
        risk_amount=scenario.risk_amount,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        trend_ema_slope_filter_enabled=False,
        trend_ema_slope_filter_lookback_bars=5,
        trend_ema_slope_filter_min_ratio=Decimal("0"),
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=0,
    )


def run_scenario(client: OkxRestClient, scenario: StrategyScenario) -> ScenarioResult:
    instrument = client.get_instrument(SYMBOL)
    candles = [candle for candle in load_candle_cache(SYMBOL, scenario.bar) if candle.confirmed]
    if not candles:
        raise RuntimeError(f"{SYMBOL} {scenario.bar} 本地没有可用已确认K线")
    result = _run_backtest_with_loaded_data(
        candles,
        instrument,
        build_config(scenario),
        data_source_note=f"local candle_cache full history | {SYMBOL} {scenario.bar} | candles={len(candles)}",
        maker_fee_rate=MAKER_FEE_RATE,
        taker_fee_rate=TAKER_FEE_RATE,
    )
    report = result.report
    return ScenarioResult(
        scenario=scenario,
        candle_count=len(candles),
        first_ts=candles[0].ts,
        last_ts=candles[-1].ts,
        total_trades=report.total_trades,
        win_rate=report.win_rate,
        total_pnl=report.total_pnl,
        total_return_pct=report.total_return_pct,
        max_drawdown=report.max_drawdown,
        max_drawdown_pct=report.max_drawdown_pct,
        average_r_multiple=report.average_r_multiple,
    )


def write_report(results: list[ScenarioResult]) -> Path:
    generated_at = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = REPORTS_DIR / f"btc_live_strategy_full_history_backtest_{generated_at}.md"
    summary_payload = []
    lines = [
        "# BTC 1H / 4H 实盘策略全量本地数据回测",
        "",
        f"- 标的: `{SYMBOL}`",
        f"- 手续费: maker `{MAKER_FEE_RATE}` / taker `{TAKER_FEE_RATE}`",
        "- 回测口径: 使用本地 `candle_cache` 全量已确认K线，不走 10000 根上限。",
        "- 说明: 当前代码里默认开启的慢线斜率过滤，在本报告中已显式关闭，以复现原实盘参数。",
        "",
        "## 汇总",
        "",
        "| 策略 | 参数 | 样本区间 | K线数 | 交易数 | 胜率 | 总盈亏 | 总收益率 | 最大回撤 | 回撤比例 | 平均R |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in results:
        scenario = item.scenario
        params = (
            f"{scenario.ema_type.upper()}{scenario.ema_period} / "
            f"{scenario.trend_ema_type.upper()}{scenario.trend_ema_period} / "
            f"挂单{scenario.entry_reference_ema_type.upper()}{scenario.entry_reference_ema_period} / "
            f"ATR止损{scenario.atr_stop_multiplier} / ATR止盈{scenario.atr_take_multiplier} / "
            f"风险{scenario.risk_amount}"
        )
        period = f"{_fmt_ts(item.first_ts)} -> {_fmt_ts(item.last_ts)}"
        lines.append(
            "| "
            + " | ".join(
                [
                    scenario.label,
                    params,
                    period,
                    str(item.candle_count),
                    str(item.total_trades),
                    f"{format_decimal_fixed(item.win_rate, 2)}%",
                    format_decimal_fixed(item.total_pnl, 4),
                    f"{format_decimal_fixed(item.total_return_pct, 2)}%",
                    format_decimal_fixed(item.max_drawdown, 4),
                    f"{format_decimal_fixed(item.max_drawdown_pct, 2)}%",
                    format_decimal_fixed(item.average_r_multiple, 4),
                ]
            )
            + " |"
        )
        summary_payload.append(
            {
                "key": scenario.key,
                "label": scenario.label,
                "bar": scenario.bar,
                "strategy_id": scenario.strategy_id,
                "source_note": scenario.source_note,
                "candle_count": item.candle_count,
                "first_ts": item.first_ts,
                "last_ts": item.last_ts,
                "total_trades": item.total_trades,
                "win_rate": str(item.win_rate),
                "total_pnl": str(item.total_pnl),
                "total_return_pct": str(item.total_return_pct),
                "max_drawdown": str(item.max_drawdown),
                "max_drawdown_pct": str(item.max_drawdown_pct),
                "average_r_multiple": str(item.average_r_multiple),
            }
        )

    lines.extend(
        [
            "",
            "## 参数来源",
            "",
        ]
    )
    for item in results:
        lines.append(f"- `{item.scenario.label}`: {item.scenario.source_note}")

    json_path = report_path.with_suffix(".json")
    json_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main() -> int:
    client = OkxRestClient()
    results: list[ScenarioResult] = []
    for index, scenario in enumerate(SCENARIOS, start=1):
        print(f"[{index}/{len(SCENARIOS)}] backtest {scenario.label} ({scenario.bar})", flush=True)
        results.append(run_scenario(client, scenario))
    report_path = write_report(results)
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
