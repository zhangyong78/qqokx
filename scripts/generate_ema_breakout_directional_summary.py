from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from okx_quant.backtest import _build_backtest_data_source_note, _load_backtest_candles, _required_backtest_preload_candles, _run_backtest_with_loaded_data, run_backtest
from okx_quant.backtest_export import export_single_backtest_report
from okx_quant.models import StrategyConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.persistence import analysis_report_dir_path
from okx_quant.strategy_catalog import STRATEGY_EMA_BREAKDOWN_SHORT_ID, STRATEGY_EMA_BREAKOUT_LONG_ID

GROUP_DIR = Path(r"D:\qqokx_data\reports\analysis\parallel_ema_groups")
MAIN_SLIPPAGE = Decimal("0.0003")
STRESS_SLIPPAGE = Decimal("0.0005")
MAKER_FEE = Decimal("0.00015")
TAKER_FEE = Decimal("0.00036")
INITIAL_CAPITAL = Decimal("10000")
RISK_AMOUNT = Decimal("10")
SYMBOL = "ETH-USDT-SWAP"
EMA_PERIOD = 21
TREND_EMA_PERIOD = 55
ATR_PERIOD = 10
BAR_LABELS = {"15m": "15分钟", "1H": "1小时", "4H": "4小时"}
STRATEGY_LABELS = {
    STRATEGY_EMA_BREAKOUT_LONG_ID: "EMA 突破做多策略",
    STRATEGY_EMA_BREAKDOWN_SHORT_ID: "EMA 跌破做空策略",
}
LONG_SAMPLE_LIMITS = {"15m": 30_000, "1H": 0, "4H": 0}


@dataclass(frozen=True)
class WinnerConfig:
    strategy_id: str
    bar: str
    entry_reference_ema: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    main_total_pnl: Decimal
    main_profit_factor: str
    main_max_drawdown_pct: Decimal
    main_total_trades: int


def build_strategy_config(
    *,
    winner: WinnerConfig,
    slippage_rate: Decimal,
) -> StrategyConfig:
    signal_mode = "long_only" if winner.strategy_id == STRATEGY_EMA_BREAKOUT_LONG_ID else "short_only"
    return StrategyConfig(
        inst_id=SYMBOL,
        bar=winner.bar,
        ema_period=EMA_PERIOD,
        trend_ema_period=TREND_EMA_PERIOD,
        big_ema_period=233,
        atr_period=ATR_PERIOD,
        atr_stop_multiplier=winner.atr_stop_multiplier,
        atr_take_multiplier=winner.atr_take_multiplier,
        order_size=Decimal("0"),
        trade_mode="cross",
        signal_mode=signal_mode,
        position_mode="net",
        environment="demo",
        tp_sl_trigger_type="mark",
        strategy_id=winner.strategy_id,
        risk_amount=RISK_AMOUNT,
        entry_reference_ema_period=winner.entry_reference_ema,
        take_profit_mode="dynamic",
        max_entries_per_trend=1,
        dynamic_two_r_break_even=True,
        dynamic_fee_offset_enabled=True,
        time_stop_break_even_enabled=False,
        time_stop_break_even_bars=10,
        hold_close_exit_bars=0,
        backtest_initial_capital=INITIAL_CAPITAL,
        backtest_sizing_mode="fixed_risk",
        backtest_compounding=False,
        backtest_entry_slippage_rate=slippage_rate,
        backtest_exit_slippage_rate=slippage_rate,
        backtest_slippage_rate=slippage_rate,
        backtest_funding_rate=Decimal("0"),
    )


def load_winners() -> list[WinnerConfig]:
    winners: list[WinnerConfig] = []
    for path in sorted(GROUP_DIR.glob("*_main.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        item = data["winner"]
        winners.append(
            WinnerConfig(
                strategy_id=item["strategy_id"],
                bar=item["bar"],
                entry_reference_ema=item["entry_reference_ema"],
                atr_stop_multiplier=Decimal(item["atr_stop_multiplier"]),
                atr_take_multiplier=Decimal(item["atr_take_multiplier"]),
                main_total_pnl=Decimal(item["total_pnl"]),
                main_profit_factor=item["profit_factor"],
                main_max_drawdown_pct=Decimal(item["max_drawdown_pct"]),
                main_total_trades=int(item["total_trades"]),
            )
        )
    return winners


def fmt(value: Decimal | None, digits: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def main() -> None:
    winners = load_winners()
    client = OkxRestClient()
    instrument = client.get_instrument(SYMBOL)
    data_source_note = _build_backtest_data_source_note(client)
    exported_at = datetime.now()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_dir = analysis_report_dir_path()
    report_dir.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, object]] = []
    for winner in winners:
        print(
            f"winner main: {winner.strategy_id} {winner.bar} EMA{winner.entry_reference_ema} "
            f"SLx{winner.atr_stop_multiplier} TPx{winner.atr_take_multiplier}",
            flush=True,
        )
        main_config = build_strategy_config(winner=winner, slippage_rate=MAIN_SLIPPAGE)
        main_result = run_backtest(
            client,
            main_config,
            candle_limit=10_000,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        main_report_path = export_single_backtest_report(
            main_result,
            main_config,
            10_000,
            exported_at=exported_at,
        )

        stress_config = build_strategy_config(winner=winner, slippage_rate=STRESS_SLIPPAGE)
        stress_result = run_backtest(
            client,
            stress_config,
            candle_limit=10_000,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        stress_report_path = export_single_backtest_report(
            stress_result,
            stress_config,
            10_000,
            exported_at=exported_at,
        )

        long_limit = LONG_SAMPLE_LIMITS[winner.bar]
        print(
            f"long sample: {winner.strategy_id} {winner.bar} limit={'full' if long_limit == 0 else long_limit}",
            flush=True,
        )
        preload_count = _required_backtest_preload_candles(main_config)
        long_candles = _load_backtest_candles(
            client,
            SYMBOL,
            winner.bar,
            long_limit,
            start_ts=0 if long_limit == 0 else None,
            end_ts=int(datetime.now(timezone.utc).timestamp() * 1000) if long_limit == 0 else None,
            preload_count=preload_count,
        )
        long_result = _run_backtest_with_loaded_data(
            long_candles,
            instrument,
            main_config,
            data_source_note=data_source_note,
            maker_fee_rate=MAKER_FEE,
            taker_fee_rate=TAKER_FEE,
        )
        long_report_path = export_single_backtest_report(
            long_result,
            main_config,
            long_limit,
            exported_at=exported_at,
        )

        summary_rows.append(
            {
                "strategy_id": winner.strategy_id,
                "strategy_label": STRATEGY_LABELS[winner.strategy_id],
                "bar": winner.bar,
                "bar_label": BAR_LABELS[winner.bar],
                "entry_reference_ema": winner.entry_reference_ema,
                "atr_stop_multiplier": str(winner.atr_stop_multiplier),
                "atr_take_multiplier": str(winner.atr_take_multiplier),
                "main_10000_total_pnl": fmt(main_result.report.total_pnl),
                "main_10000_return_pct": fmt(main_result.report.total_return_pct, 2),
                "main_10000_profit_factor": fmt(main_result.report.profit_factor),
                "main_10000_max_drawdown_pct": fmt(main_result.report.max_drawdown_pct, 2),
                "main_10000_trades": main_result.report.total_trades,
                "stress_10000_total_pnl": fmt(stress_result.report.total_pnl),
                "stress_10000_return_pct": fmt(stress_result.report.total_return_pct, 2),
                "stress_10000_profit_factor": fmt(stress_result.report.profit_factor),
                "stress_10000_max_drawdown_pct": fmt(stress_result.report.max_drawdown_pct, 2),
                "stress_10000_trades": stress_result.report.total_trades,
                "long_sample_limit": "full" if long_limit == 0 else long_limit,
                "long_sample_total_pnl": fmt(long_result.report.total_pnl),
                "long_sample_return_pct": fmt(long_result.report.total_return_pct, 2),
                "long_sample_profit_factor": fmt(long_result.report.profit_factor),
                "long_sample_max_drawdown_pct": fmt(long_result.report.max_drawdown_pct, 2),
                "long_sample_trades": long_result.report.total_trades,
                "main_report_path": str(main_report_path),
                "stress_report_path": str(stress_report_path),
                "long_sample_report_path": str(long_report_path),
            }
        )

    json_path = report_dir / f"ema_breakout_directional_summary_{stamp}.json"
    report_path = report_dir / f"ema_breakout_directional_summary_{stamp}.txt"
    leader_path = report_dir / f"ema_breakout_directional_summary_{stamp}_leader_onepage.txt"

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "symbol": SYMBOL,
        "notes": [
            "主滑点口径：开仓/平仓各 0.03%。",
            "压力滑点口径：开仓/平仓各 0.05%。",
            "15m 长样本使用 30000 根扩展样本；1H 和 4H 使用全量历史。",
        ],
        "rows": summary_rows,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "EMA 突破做多 / EMA 跌破做空 汇总报告",
        "=" * 72,
        f"生成时间(UTC)：{payload['generated_at']}",
        f"标的：{SYMBOL}",
        "主滑点：0.03% / 0.03%",
        "压力滑点：0.05% / 0.05%",
        "长样本规则：15m = 30000 根；1H / 4H = 全量历史。",
        "",
    ]
    for row in summary_rows:
        lines.append(
            (
                f"{row['strategy_label']} | {row['bar_label']} | EMA{row['entry_reference_ema']} / "
                f"SLx{row['atr_stop_multiplier']} / TPx{row['atr_take_multiplier']}"
            )
        )
        lines.append(
            (
                f"  10000根主滑点：总盈亏 {row['main_10000_total_pnl']} | 收益率 {row['main_10000_return_pct']}% | "
                f"PF {row['main_10000_profit_factor']} | 回撤 {row['main_10000_max_drawdown_pct']}% | 交易 {row['main_10000_trades']}"
            )
        )
        lines.append(
            (
                f"  10000根压力滑点：总盈亏 {row['stress_10000_total_pnl']} | 收益率 {row['stress_10000_return_pct']}% | "
                f"PF {row['stress_10000_profit_factor']} | 回撤 {row['stress_10000_max_drawdown_pct']}% | 交易 {row['stress_10000_trades']}"
            )
        )
        lines.append(
            (
                f"  长样本确认：总盈亏 {row['long_sample_total_pnl']} | 收益率 {row['long_sample_return_pct']}% | "
                f"PF {row['long_sample_profit_factor']} | 回撤 {row['long_sample_max_drawdown_pct']}% | "
                f"交易 {row['long_sample_trades']} | 样本={row['long_sample_limit']}"
            )
        )
        lines.append(f"  主报告：{row['main_report_path']}")
        lines.append(f"  压力报告：{row['stress_report_path']}")
        lines.append(f"  长样本报告：{row['long_sample_report_path']}")
        lines.append("")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")

    leader_lines = [
        "EMA 突破/跌破 一页结论",
        "=" * 60,
        f"生成时间(UTC)：{payload['generated_at']}",
        f"标的：{SYMBOL}",
        "正式口径：主滑点 0.03%/0.03%，压力滑点 0.05%/0.05%。",
        "长样本：15m 扩展 30000 根；1H / 4H 全量历史。",
        "",
    ]
    for row in summary_rows:
        leader_lines.append(
            (
                f"- {row['strategy_label']} | {row['bar_label']} | EMA{row['entry_reference_ema']} "
                f"| 主 {row['main_10000_total_pnl']} | 压力 {row['stress_10000_total_pnl']} | 长样本 {row['long_sample_total_pnl']}"
            )
        )
    leader_lines.append("")
    leader_lines.append(f"完整汇总：{report_path}")
    leader_lines.append(f"JSON：{json_path}")
    leader_path.write_text("\n".join(leader_lines) + "\n", encoding="utf-8-sig")

    print(f"report_txt={report_path}", flush=True)
    print(f"leader_txt={leader_path}", flush=True)
    print(f"summary_json={json_path}", flush=True)


if __name__ == "__main__":
    main()
