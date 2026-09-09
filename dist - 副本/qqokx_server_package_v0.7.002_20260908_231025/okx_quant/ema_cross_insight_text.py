"""
由 EMA 突破/跌破矩阵回测 CSV 行数据生成「五币种深度洞察」风格长文（客户沟通版）。
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any, Iterable


def _f(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).replace(",", ""))


def _i(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(float(str(value)))


def _coin_label(inst_id: str) -> str:
    return inst_id.replace("-USDT-SWAP", "").replace("-USDT", "")


def _filter_rows(rows: Iterable[dict[str, Any]], **kwargs: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        ok = True
        for k, v in kwargs.items():
            if str(row.get(k)) != str(v):
                ok = False
                break
        if ok:
            out.append(row)
    return out


def _positive_rate(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return 100.0 * sum(1 for r in rows if _f(r.get("total_pnl")) > 0) / len(rows)


def _mean_pnl(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    return float(statistics.mean(_f(r.get("total_pnl")) for r in rows))


def _best_by_mean(rows: list[dict[str, Any]], key: str) -> tuple[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        buckets[str(r.get(key))].append(_f(r.get("total_pnl")))
    if not buckets:
        return "", 0.0
    best_k = max(buckets, key=lambda k: statistics.mean(buckets[k]))
    return best_k, float(statistics.mean(buckets[best_k]))


def _best_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return max(rows, key=lambda r: _f(r.get("total_pnl")))


def _format_best_row(r: dict[str, Any]) -> str:
    return (
        f"{r.get('direction_label_zh', '')}，EMA{r.get('entry_reference_ema')}，"
        f"止损×{r.get('stop_atr')}，止盈×{r.get('take_atr')}，{r.get('take_profit_mode')}，"
        f"时间保本{r.get('time_stop_break_even_bars')}根，满{r.get('hold_close_exit_bars')}根收盘平仓，"
        f"总盈亏 {_f(r.get('total_pnl')):.4f}，最大回撤 {_f(r.get('max_drawdown')):.4f}"
    )


def build_client_deep_insight(
    rows: list[dict[str, Any]],
    *,
    utc_ts: str,
    spec: dict[str, Any],
    smoke: bool,
) -> str:
    if not rows:
        return "（无回测数据，无法生成洞察报告。）\n"

    n = len(rows)
    avg_all = _mean_pnl(rows)
    pos_rate_all = _positive_rate(rows)
    best_global = _best_row(rows)
    worst_global = min(rows, key=lambda r: _f(r.get("total_pnl")))

    bars_order = ("5m", "15m", "1H", "4H")
    bar_zh = {"5m": "5分钟", "15m": "15分钟", "1H": "1小时", "4H": "4小时"}
    coins_order_all = ("BTC-USDT-SWAP", "ETH-USDT-SWAP", "BNB-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP")
    coins_order = [c for c in coins_order_all if any(r.get("inst_id") == c for r in rows)]
    if not coins_order:
        coins_order = sorted({str(r.get("inst_id")) for r in rows if r.get("inst_id")})

    # 周期 × 方向 平均盈亏（粗规律）
    bar_dir_lines: list[str] = []
    for bar in bars_order:
        sub_bar = _filter_rows(rows, bar=bar)
        if not sub_bar:
            continue
        for dlab in ("做多", "做空"):
            sub = [r for r in sub_bar if r.get("direction_label_zh") == dlab]
            if sub:
                bar_dir_lines.append(
                    f"{bar_zh.get(bar, bar)}{dlab}：平均总盈亏 {_mean_pnl(sub):.4f}，正收益占比 {_positive_rate(sub):.2f}%"
                )

    # 止损 ATR 全局平均排序
    stop_rank: list[tuple[str, float]] = []
    for s in ("1", "1.5", "2"):
        g = [r for r in rows if str(r.get("stop_atr")) == s]
        if g:
            stop_rank.append((s, _mean_pnl(g)))
    stop_rank.sort(key=lambda x: x[1], reverse=True)
    stop_rank_txt = "、".join(f"×{a}（平均 {b:.4f}）" for a, b in stop_rank) if stop_rank else "（数据不足）"

    # 参考 EMA 21 vs 55
    ema21 = [r for r in rows if _i(r.get("entry_reference_ema")) == 21]
    ema55 = [r for r in rows if _i(r.get("entry_reference_ema")) == 55]
    ema_cmp = (
        f"EMA21 全样本平均 {_mean_pnl(ema21):.4f}（{len(ema21)} 组）"
        f" vs EMA55 平均 {_mean_pnl(ema55):.4f}（{len(ema55)} 组）。"
    )

    # 固定 vs 动态
    fixed_r = [r for r in rows if r.get("take_profit_mode") == "fixed"]
    dyn_r = [r for r in rows if r.get("take_profit_mode") == "dynamic"]
    tp_cmp = (
        f"固定止盈平均 {_mean_pnl(fixed_r):.4f}，动态止盈平均 {_mean_pnl(dyn_r):.4f}；"
        f"正收益占比分别为 {_positive_rate(fixed_r):.2f}% 与 {_positive_rate(dyn_r):.2f}%。"
    )

    # 时间保本档 1～10 平均排序（取最好与最差档）
    be_buckets: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        be_buckets[_i(r.get("time_stop_break_even_bars"))].append(_f(r.get("total_pnl")))
    be_avgs = [(k, statistics.mean(v)) for k, v in sorted(be_buckets.items())]
    be_best = max(be_avgs, key=lambda x: x[1]) if be_avgs else (0, 0.0)
    be_worst = min(be_avgs, key=lambda x: x[1]) if be_avgs else (0, 0.0)

    # 满 N 根收盘平仓
    hold_buckets: dict[int, list[float]] = defaultdict(list)
    for r in rows:
        hold_buckets[_i(r.get("hold_close_exit_bars"))].append(_f(r.get("total_pnl")))
    hold_avgs = [(k, statistics.mean(v)) for k, v in sorted(hold_buckets.items())]
    hold_best = max(hold_avgs, key=lambda x: x[1]) if hold_avgs else (0, 0.0)

    # 币种「风险代理」：按币在全样本上的平均最大回撤%
    risk_by_coin: list[tuple[str, float]] = []
    for coin in coins_order:
        cr = _filter_rows(rows, inst_id=coin)
        if cr:
            risk_by_coin.append((coin, float(statistics.mean(_f(r.get("max_drawdown_pct")) for r in cr))))
    risk_by_coin.sort(key=lambda x: x[1], reverse=True)
    risk_lines = [
        f"{_coin_label(c)}：平均最大回撤（占净值%）约 {v:.2f}%"
        for c, v in risk_by_coin
    ]

    coins_disp: list[str] = list(spec.get("coins") or coins_order)
    bars_disp: list[str] = list(spec.get("bars") or [b for b in bars_order if any(r.get("bar") == b for r in rows)])

    lines: list[str] = [
        "五币种 EMA 突破/跌破策略 · 全参数矩阵深度洞察报告（客户沟通版）",
        "",
        f"出具时间（UTC）：{utc_ts}",
        f"数据来源：EMA 突破/跌破矩阵回测全量 CSV（{'试跑子集' if smoke else '全量'}，共 {n} 组）。",
        f"策略口径：{spec.get('strategy', 'EMA突破跌破')}；K 线目标 {spec.get('candle_limit', '?')} 根/组；"
        f"开/平滑点各 {spec.get('slippage_each_side', '')}；手续费 Maker/Taker 见 JSON。",
        "",
        "一、先说最重要的结论",
        f"1. 全样本平均总盈亏约为 {avg_all:.4f}；正收益参数组合占比约 {pos_rate_all:.2f}%。",
        "2. 本批为「参数压力测试」，价值在于跨币种、跨周期、跨止损/止盈/时间保本/收盘平仓规则是否呈现稳定规律，而非单组暴利。",
        f"3. 止损 ATR 倍数从全样本平均看，排序为：{stop_rank_txt}。",
        f"4. {ema_cmp}",
        f"5. {tp_cmp}",
        f"6. 时间保本 K 线数：全样本平均下，{be_best[0]} 根档最好（约 {be_best[1]:.4f}），"
        f"{be_worst[0]} 根档最弱（约 {be_worst[1]:.4f}）；"
        f"满 N 根收盘平仓在 N={hold_best[0]} 时平均最优（约 {hold_best[1]:.4f}）。",
        "",
        "二、为什么这份结论比「挑一组最高收益」更有说服力",
        "很多展示只拿全历史里最好的一组参数，客户一问「换半年还行不行」就答不上来。",
        "本次把大量参数组合摊开，看同一结论是否在多个币、多个周期、多个成本假设下反复出现；",
        "只有「重复出现的规律」才适合作为产品默认值或客户沟通的主线，而不是单点极值。",
        "",
        "三、样本与数据口径（务必与客户对齐）",
        f"1. 币种：{', '.join(_coin_label(c) for c in coins_disp)}（USDT 永续）。",
        f"2. 周期：{', '.join(bar_zh.get(b, b) for b in bars_disp)}。",
        "3. 方向：只做多、只做空分开统计后合并观察。",
        f"4. 实际载入 K 线根数以 CSV 中 candle_count、data_source_note 为准（OKX 历史上限与本地缓存会影响是否满 {spec.get('candle_limit', 10000)} 根）。",
        "5. 短周期（5m/15m）覆盖的日历时间通常远短于 4H，结论更适合做「战术参考」；1H/4H 更适合做「主结论」。",
        "",
        "四、不同周期 × 方向的平均表现（粗览）",
    ]
    lines.extend(bar_dir_lines if bar_dir_lines else ["（当前子集无分周期数据）"])
    lines += [
        "",
        "五、止损 ATR 与突破参考 EMA 的全局规律",
        f"1. 止损倍数平均盈亏排序已体现在第一节；可结合分周期章节判断「紧止损 vs 宽止损」是否在特定周期更占优。",
        f"2. {ema_cmp}",
        "3. 人话总结：若分周期里出现「短周期偏好宽止损、高周期偏好紧止损」的分化，与旧版动态委托报告的结论往往同向——本质是噪音 vs 趋势推进的差异。",
        "",
        "六、五个币的「风险画像」（以平均最大回撤%为代理指标）",
        "说明：本段未重算真实波动率，仅用各参数组的最大回撤占净值百分比的样本均值，衡量「在该矩阵下该币的平均回撤痛感」。",
    ]
    lines.extend(risk_lines if risk_lines else ["（无分币数据）"])
    lines.append("")

    lines += ["七、按币种拆开讲（每组下均为该币×该周期的全部参数组合）", ""]

    for coin in coins_order:
        label = _coin_label(coin)
        lines.append(f"【{label}】")
        coin_rows = _filter_rows(rows, inst_id=coin)
        if not coin_rows:
            lines.append("（本批数据中无该币。）")
            lines.append("")
            continue
        lines.append(
            f"全周期合并：正收益组合占比 {_positive_rate(coin_rows):.2f}%，"
            f"平均总盈亏 {_mean_pnl(coin_rows):.4f}。"
        )
        for bar in bars_order:
            br = _filter_rows(rows, inst_id=coin, bar=bar)
            if not br:
                continue
            long_r = [x for x in br if x.get("direction") == "long_only"]
            short_r = [x for x in br if x.get("direction") == "short_only"]
            best_k, best_v = _best_by_mean(br, "stop_atr")
            ema_k, ema_v = _best_by_mean(br, "entry_reference_ema")
            br_best = _best_row(br)
            lines.append(f"1）{bar_zh.get(bar, bar)}")
            lines.append(
                f"   正收益占比 {_positive_rate(br):.2f}%（做多子集 {_positive_rate(long_r):.2f}%，"
                f"做空子集 {_positive_rate(short_r):.2f}%）。"
            )
            lines.append(
                f"   全参数平均下，止损×{best_k} 对应的平均总盈亏相对最优（约 {best_v:.4f}）；"
                f"参考 EMA {ema_k} 期相对更优（均值约 {ema_v:.4f}）。"
            )
            if br_best:
                lines.append(f"   本周期内总盈亏最佳的一组：{_format_best_row(br_best)}。")
            lines.append("")
        lines.append("")

    lines += [
        "八、时间保本与「满 N 根收盘价平仓」",
        "1. 时间保本各档平均总盈亏已在第一节概括；整体上它更像「局部增强器」，是否默认开启要结合分周期最佳档再看 CSV。",
        f"2. 满 N 根收盘平仓：本次矩阵下 N={hold_best[0]} 平均表现最好；具体是否采用需与交易频率、手续费成本一并评估。",
        "",
        "九、给客户怎么讲（话术建议）",
        "1. 若客户要「稳健、可解释、主仓位」：优先从 1H/4H 里挑正收益占比高、回撤可控的币与方向，再落到具体止损/止盈档。",
        "2. 若客户要「增强收益」：在充分理解回撤代理指标后，再考虑在数据上弹性更大的币与周期上小仓位试验。",
        "3. 若客户纠结「固定还是动态止盈」：用第五节的全局对比 + CSV 分周期切片一起讲，避免口头只讲单一模式。",
        "",
        "十、全局最优与最弱（参数可追溯）",
    ]
    if best_global:
        lines.append(f"总盈亏最高：{_format_best_row(best_global)}。")
    lines.append(f"总盈亏最弱：{_format_best_row(worst_global)}。")
    lines += [
        "",
        "十一、一句话版结论",
        "矩阵回测的价值在于：用同一套规则在大量参数下「压测」出周期与币种的脾气；",
        "真正可落地的是「分周期、分币种的重复规律」，而不是全市场通用的一组魔法数字。",
        "",
        "---",
        "附：若需与旧稿完全同款的「真实波动率排名」，需在行情侧另行计算 TR% 并合并；本报告仅基于矩阵 CSV 可得字段生成。",
    ]
    return "\n".join(lines) + "\n"
