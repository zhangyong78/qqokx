from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

GROUP_DIR = Path(r"D:\qqokx_data\reports\analysis\multi_coin_groups")
OUT_DIR = Path(r"D:\qqokx_data\reports\analysis")

STRATEGY_LABELS = {
    "ema_breakout_long": "EMA突破做多",
    "ema_breakdown_short": "EMA跌破做空",
}
BAR_ORDER = {"15m": 0, "1H": 1, "4H": 2}
BAR_LABELS = {"15m": "15分钟", "1H": "1小时", "4H": "4小时"}


@dataclass(frozen=True)
class WinnerRow:
    symbol: str
    strategy_id: str
    bar: str
    entry_reference_ema: int
    atr_stop_multiplier: Decimal
    atr_take_multiplier: Decimal
    total_trades: int
    win_rate: Decimal
    total_pnl: Decimal
    total_return_pct: Decimal
    average_r_multiple: Decimal
    max_drawdown_pct: Decimal
    profit_factor: Decimal | None
    total_fees: Decimal
    slippage_costs: Decimal
    source_path: str

    @property
    def strategy_label(self) -> str:
        return STRATEGY_LABELS[self.strategy_id]

    @property
    def bar_label(self) -> str:
        return BAR_LABELS[self.bar]

    @property
    def recommendation(self) -> str:
        if self.total_pnl > 0 and (self.profit_factor or Decimal("0")) >= Decimal("1.10"):
            return "优先关注"
        if self.total_pnl > 0 and (self.profit_factor or Decimal("0")) >= Decimal("1.00"):
            return "可继续观察"
        return "暂不采用"


def d(value: str | int | float | None) -> Decimal:
    if value in (None, "-"):
        return Decimal("0")
    return Decimal(str(value))


def load_rows() -> list[WinnerRow]:
    rows: list[WinnerRow] = []
    for path in sorted(GROUP_DIR.glob("*_main.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        winner = data["winner"]
        pf = winner.get("profit_factor")
        rows.append(
            WinnerRow(
                symbol=data["symbol"],
                strategy_id=winner["strategy_id"],
                bar=winner["bar"],
                entry_reference_ema=int(winner["entry_reference_ema"]),
                atr_stop_multiplier=d(winner["atr_stop_multiplier"]),
                atr_take_multiplier=d(winner["atr_take_multiplier"]),
                total_trades=int(winner["total_trades"]),
                win_rate=d(winner["win_rate"]),
                total_pnl=d(winner["total_pnl"]),
                total_return_pct=d(winner["total_return_pct"]),
                average_r_multiple=d(winner["average_r_multiple"]),
                max_drawdown_pct=d(winner["max_drawdown_pct"]),
                profit_factor=None if pf in (None, "-") else d(pf),
                total_fees=d(winner["total_fees"]),
                slippage_costs=d(winner["slippage_costs"]),
                source_path=str(path),
            )
        )
    rows.sort(key=lambda item: (item.symbol, item.strategy_id, BAR_ORDER[item.bar]))
    return rows


def fmt(value: Decimal | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def ranking_key(item: WinnerRow) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    return (
        item.total_pnl,
        item.profit_factor or Decimal("0"),
        -item.max_drawdown_pct,
        item.average_r_multiple,
    )


def build_payload(rows: list[WinnerRow]) -> dict[str, object]:
    strategy_views: dict[str, list[dict[str, object]]] = defaultdict(list)
    symbol_views: dict[str, list[dict[str, object]]] = defaultdict(list)

    for row in rows:
        entry = {
            "symbol": row.symbol,
            "strategy_id": row.strategy_id,
            "strategy_label": row.strategy_label,
            "bar": row.bar,
            "bar_label": row.bar_label,
            "entry_reference_ema": row.entry_reference_ema,
            "atr_stop_multiplier": str(row.atr_stop_multiplier),
            "atr_take_multiplier": str(row.atr_take_multiplier),
            "total_trades": row.total_trades,
            "win_rate_pct": fmt(row.win_rate, 2),
            "total_pnl": fmt(row.total_pnl, 4),
            "total_return_pct": fmt(row.total_return_pct, 2),
            "average_r_multiple": fmt(row.average_r_multiple, 4),
            "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
            "profit_factor": fmt(row.profit_factor, 4) if row.profit_factor is not None else "-",
            "total_fees": fmt(row.total_fees, 4),
            "slippage_costs": fmt(row.slippage_costs, 4),
            "recommendation": row.recommendation,
            "source_path": row.source_path,
        }
        strategy_views[row.strategy_id].append(entry)
        symbol_views[row.symbol].append(entry)

    top_candidates = sorted(
        [row for row in rows if row.recommendation != "暂不采用"],
        key=ranking_key,
        reverse=True,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "scope": {
            "symbols": sorted({row.symbol for row in rows}),
            "strategies": STRATEGY_LABELS,
            "bars": [BAR_LABELS[item] for item in ("15m", "1H", "4H")],
            "reference_emas": [21, 55],
            "atr_stop_grid": ["1", "1.5", "2"],
            "atr_take_grid": ["2", "3", "4"],
            "candle_limit_per_run": 10000,
            "initial_capital": "10000",
            "risk_amount": "10",
            "maker_fee_rate": "0.015%",
            "taker_fee_rate": "0.036%",
            "entry_slippage_rate": "0.03%",
            "exit_slippage_rate": "0.03%",
        },
        "winner_rows": [
            {
                "symbol": row.symbol,
                "strategy_id": row.strategy_id,
                "strategy_label": row.strategy_label,
                "bar": row.bar,
                "bar_label": row.bar_label,
                "entry_reference_ema": row.entry_reference_ema,
                "atr_stop_multiplier": str(row.atr_stop_multiplier),
                "atr_take_multiplier": str(row.atr_take_multiplier),
                "total_trades": row.total_trades,
                "win_rate_pct": fmt(row.win_rate, 2),
                "total_pnl": fmt(row.total_pnl, 4),
                "total_return_pct": fmt(row.total_return_pct, 2),
                "average_r_multiple": fmt(row.average_r_multiple, 4),
                "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                "profit_factor": fmt(row.profit_factor, 4) if row.profit_factor is not None else "-",
                "total_fees": fmt(row.total_fees, 4),
                "slippage_costs": fmt(row.slippage_costs, 4),
                "recommendation": row.recommendation,
                "source_path": row.source_path,
            }
            for row in rows
        ],
        "top_candidates": [
            {
                "symbol": row.symbol,
                "strategy_label": row.strategy_label,
                "bar_label": row.bar_label,
                "entry_reference_ema": row.entry_reference_ema,
                "atr_stop_multiplier": str(row.atr_stop_multiplier),
                "atr_take_multiplier": str(row.atr_take_multiplier),
                "total_pnl": fmt(row.total_pnl, 4),
                "profit_factor": fmt(row.profit_factor, 4) if row.profit_factor is not None else "-",
                "max_drawdown_pct": fmt(row.max_drawdown_pct, 2),
                "recommendation": row.recommendation,
            }
            for row in top_candidates[:10]
        ],
        "strategy_views": strategy_views,
        "symbol_views": symbol_views,
    }


def build_markdown(rows: list[WinnerRow], payload: dict[str, object]) -> str:
    lines: list[str] = []
    lines.append("# EMA突破/跌破多币种主矩阵汇总")
    lines.append("")
    lines.append(f"- 生成时间(UTC): {payload['generated_at']}")
    lines.append("- 币种: BTC-USDT-SWAP / ETH-USDT-SWAP / SOL-USDT-SWAP / DOGE-USDT-SWAP")
    lines.append("- 周期: 15分钟 / 1小时 / 4小时")
    lines.append("- 口径: 每组10000根K线, 本金10000, 固定风险金10, Maker 0.015%, Taker 0.036%, 开平滑点各0.03%")
    lines.append("- 说明: 本报告是多币种主矩阵筛选结果, 不是长样本确认报告。")
    lines.append("")

    positive_rows = [row for row in rows if row.total_pnl > 0 and (row.profit_factor or Decimal("0")) >= Decimal("1.0")]
    positive_rows.sort(key=ranking_key, reverse=True)
    lines.append("## 快速结论")
    if positive_rows:
        for row in positive_rows[:8]:
            lines.append(
                f"- {row.symbol} | {row.strategy_label} | {row.bar_label} | EMA{row.entry_reference_ema} / SLx{row.atr_stop_multiplier} / TPx{row.atr_take_multiplier} | "
                f"净利 {fmt(row.total_pnl, 4)} | PF {fmt(row.profit_factor, 4)} | 回撤 {fmt(row.max_drawdown_pct, 2)}% | {row.recommendation}"
            )
    else:
        lines.append("- 本轮没有跑出满足正收益且PF>=1.0的组合。")
    lines.append("")

    lines.append("## 全部胜出组")
    for row in rows:
        lines.append(
            f"- {row.symbol} | {row.strategy_label} | {row.bar_label} | EMA{row.entry_reference_ema} / SLx{row.atr_stop_multiplier} / TPx{row.atr_take_multiplier} | "
            f"净利 {fmt(row.total_pnl, 4)} | 收益率 {fmt(row.total_return_pct, 2)}% | PF {fmt(row.profit_factor, 4)} | 回撤 {fmt(row.max_drawdown_pct, 2)}% | "
            f"交易数 {row.total_trades} | 胜率 {fmt(row.win_rate, 2)}% | 结论 {row.recommendation}"
        )
    lines.append("")

    lines.append("## 币种观察")
    for symbol in sorted({row.symbol for row in rows}):
        symbol_rows = [row for row in rows if row.symbol == symbol]
        symbol_rows.sort(key=ranking_key, reverse=True)
        best = symbol_rows[0]
        positives = sum(1 for row in symbol_rows if row.total_pnl > 0 and (row.profit_factor or Decimal("0")) >= Decimal("1.0"))
        lines.append(
            f"- {symbol}: 胜出组共 {len(symbol_rows)} 个, 其中正向可观察 {positives} 个。当前最好的是 {best.strategy_label} {best.bar_label} "
            f"(EMA{best.entry_reference_ema} / SLx{best.atr_stop_multiplier} / TPx{best.atr_take_multiplier}, 净利 {fmt(best.total_pnl, 4)}, PF {fmt(best.profit_factor, 4)})"
        )
    lines.append("")

    lines.append("## 提醒")
    lines.append("- 这份结果只覆盖主滑点口径, 还没有把每个多币种胜出组继续拉到压力滑点和全量历史确认。")
    lines.append("- 15分钟结果更容易受近期结构影响, 需要后续长样本复核。")
    return "\n".join(lines) + "\n"


def build_gpt_brief(payload_path: Path, markdown_path: Path, rows: list[WinnerRow]) -> str:
    candidate_lines = []
    sorted_rows = sorted(rows, key=ranking_key, reverse=True)
    for row in sorted_rows[:12]:
        candidate_lines.append(
            f"- {row.symbol} | {row.strategy_label} | {row.bar_label} | EMA{row.entry_reference_ema} / SLx{row.atr_stop_multiplier} / TPx{row.atr_take_multiplier} | "
            f"净利 {fmt(row.total_pnl, 4)} | PF {fmt(row.profit_factor, 4)} | 回撤 {fmt(row.max_drawdown_pct, 2)}%"
        )

    lines = [
        "# 给 GPT-5.5 的分析输入",
        "",
        "建议优先使用 `GPT-5.5 高分析`。",
        "只有当你要它进一步做跨币种优先级、组合配置、二轮优化路线时，再切到 `GPT-5.5 超高分析`。",
        "",
        "## 建议直接粘贴给模型的任务",
        "",
        "请基于以下多币种主矩阵回测结果，完成一份结构化分析：",
        "1. 先判断 EMA突破做多 与 EMA跌破做空，分别在哪些币种和周期上更有稳定性。",
        "2. 按“优先推进 / 可继续观察 / 暂不采用”三档给出排序，并说明理由。",
        "3. 重点识别哪些结果可能只是15分钟近期样本幸运，哪些更像可以进入下一轮压力测试与长样本确认。",
        "4. 说明 EMA21 参考突破与 EMA55 参考突破分别更适合哪些场景。",
        "5. 输出下一轮建议：哪些组合需要做压力滑点0.05%验证，哪些组合需要做全量历史确认。",
        "",
        "## 文件",
        "",
        f"- 结构化JSON: {payload_path}",
        f"- 人类汇总Markdown: {markdown_path}",
        "",
        "## 当前值得优先看的候选",
        "",
        *candidate_lines,
        "",
        "## 额外提醒",
        "",
        "- 本轮是主矩阵筛选，不是最终正式采用结论。",
        "- 所有结果口径一致：10000根K线、本金10000、固定风险金10、Maker 0.015%、Taker 0.036%、开平滑点各0.03%。",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    rows = load_rows()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    payload = build_payload(rows)
    json_path = OUT_DIR / f"ema_breakout_multi_coin_main_summary_{stamp}.json"
    md_path = OUT_DIR / f"ema_breakout_multi_coin_main_summary_{stamp}.md"
    gpt_path = OUT_DIR / f"ema_breakout_multi_coin_gpt55_brief_{stamp}.md"

    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(build_markdown(rows, payload), encoding="utf-8")
    gpt_path.write_text(build_gpt_brief(json_path, md_path, rows), encoding="utf-8")

    print(json_path)
    print(md_path)
    print(gpt_path)


if __name__ == "__main__":
    main()
