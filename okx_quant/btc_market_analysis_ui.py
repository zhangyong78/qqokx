from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from tkinter import END, BooleanVar, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Any, Callable

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalysis,
    BtcMarketAnalyzerConfig,
    analyze_btc_market_from_client,
    save_btc_market_analysis,
    send_btc_market_analysis_email,
)
from okx_quant.market_analysis import MarketAnalysisConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]

DEFAULT_ANALYSIS_SYMBOLS: tuple[str, ...] = ("BTC-USDT-SWAP", "BTC-USDT")
DEFAULT_ANALYSIS_TIMEFRAMES: tuple[str, ...] = ("1H", "4H", "1D")
DEFAULT_ANALYSIS_HISTORY_LIMITS: tuple[tuple[str, int], ...] = (("1H", 5000), ("4H", 5000), ("1D", 0))

_DIRECTION_TERMS: dict[str, tuple[str, str]] = {
    "long": ("看多", "多头占优，短线更倾向继续上行。"),
    "short": ("看空", "空头占优，短线更倾向继续走弱。"),
    "neutral": ("中性", "多空分歧较大，暂未形成明确方向。"),
}

_TREND_CONTEXT_TERMS: dict[str, tuple[str, str]] = {
    "uptrend": ("上升趋势", "EMA结构与动能偏多，走势相对更强。"),
    "downtrend": ("下降趋势", "EMA结构与动能偏空，走势相对更弱。"),
    "sideways": ("震荡整理", "方向性不强，更多表现为区间波动。"),
    "unknown": ("待确认", "当前信号不足，趋势语境还不明确。"),
    "multi_timeframe": ("多周期共振", "这是跨周期汇总后的共振语境。"),
}

_SIGNAL_CATEGORY_TERMS: dict[str, tuple[str, str]] = {
    "indicator": ("指标", "由均线、MACD、布林带等技术指标触发。"),
    "pattern": ("K线形态", "由单根K线形态识别触发。"),
    "probability": ("概率因子", "基于历史连阳、回调和波动率统计触发。"),
    "resonance": ("多周期共振", "由多个周期方向一致性触发。"),
}

_SIGNAL_BIAS_TERMS: dict[str, tuple[str, str]] = {
    "long": ("偏多", "该信号更支持上涨方向。"),
    "short": ("偏空", "该信号更支持下跌方向。"),
    "caution": ("谨慎", "提示风险抬升，不宜激进追涨杀跌。"),
    "neutral": ("中性", "该信号暂未提供明确方向倾向。"),
}

_SIGNAL_NAME_LABELS: dict[str, str] = {
    "ema_bullish_alignment": "EMA多头排列",
    "ema_bearish_alignment": "EMA空头排列",
    "ema_structure_support": "EMA结构支撑",
    "ema_structure_pressure": "EMA结构压制",
    "macd_bullish_cross": "MACD金叉",
    "macd_bearish_cross": "MACD死叉",
    "macd_positive_zone": "MACD多头区",
    "macd_negative_zone": "MACD空头区",
    "boll_upper_breakout": "布林上轨突破",
    "boll_lower_breakdown": "布林下轨跌破",
    "multi_timeframe_resonance": "多周期共振",
    "streak_momentum_peak_5_6": "5-6连阳顺势",
    "streak_exhaustion_ge_7": "7连阳以上衰减预警",
    "post_streak_pullback_support_hold": "连阳后守支撑回踩",
    "post_streak_breakdown": "连阳后破位转弱",
    "low_volatility_streak_quality": "低波动连阳质量",
    "high_volatility_streak_noise": "高波动连阳噪音",
    "post_streak_small_pullback_reentry": "连阳后轻回踩再入场",
    "streak_momentum_peak_low_volatility": "低波动5-6连阳顺势",
    "streak_momentum_building_low_volatility": "低波动2-3连阳观察",
    "high_volatility_streak_caution": "高波动连阳谨慎",
    "doji": "十字星",
    "dragonfly_doji": "蜻蜓十字",
    "gravestone_doji": "墓碑十字",
    "long_legged_doji": "长腿十字",
    "hammer": "锤子线",
    "hanging_man": "上吊线",
    "inverted_hammer": "倒锤子线",
    "shooting_star": "流星线",
    "bullish_marubozu": "光头光脚阳线",
    "bearish_marubozu": "光头光脚阴线",
    "spinning_top": "纺锤线",
}

_PROBABILITY_DIRECTION_TERMS: dict[str, tuple[str, str]] = {
    "bullish": ("看多因子", "更支持上涨延续或回踩后再走强。"),
    "bearish": ("看空因子", "更支持趋势减弱或下行延续。"),
    "caution": ("谨慎因子", "提示趋势可靠性下降，需要降低追价意愿。"),
}

_DIRECTION_MODE_LABELS: dict[str, str] = {
    "close_to_close": "按前收涨跌",
    "candle_body": "按K线实体",
}

_VOLATILITY_REGIME_TERMS: dict[str, tuple[str, str]] = {
    "low": ("低波动", "波动收敛，连阳延续质量通常更高。"),
    "medium": ("中波动", "趋势惯性存在，但噪音也会增加。"),
    "high": ("高波动", "更像情绪脉冲，连阳可靠性会下降。"),
    "unknown": ("待确认", "暂时无法判断当前波动率环境。"),
}

_PULLBACK_BUCKET_TERMS: dict[str, tuple[str, str]] = {
    "micro": ("微阴线", "回调小于 2%，更像健康回踩。"),
    "small": ("小阴线", "回调约 2%-5%，需要开始警惕。"),
    "medium": ("中阴线", "回调约 5%-8%，转弱风险明显抬升。"),
    "large": ("大阴线", "回调超过 8%，趋势破坏风险较高。"),
    "unknown": ("待确认", "当前没有足够样本判断首阴级别。"),
}

_PATTERN_BIAS_TERMS: dict[str, tuple[str, str]] = {
    "bullish_reversal": ("看多反转", "更像下跌后的止跌反转信号。"),
    "bearish_reversal": ("看空反转", "更像上涨后的转弱反转信号。"),
    "bullish_continuation": ("看多延续", "更像趋势中的顺势继续上行。"),
    "bearish_continuation": ("看空延续", "更像趋势中的顺势继续走弱。"),
    "neutral": ("中性", "形态本身没有明显方向倾向。"),
}

_EMA_STATE_TERMS: dict[str, tuple[str, str]] = {
    "bullish_alignment": ("多头排列", "短中长均线有序上行，结构偏强。"),
    "bearish_alignment": ("空头排列", "短中长均线有序下行，结构偏弱。"),
    "early_bullish_stack": ("初步多头排列", "短均线刚开始站上中长均线，强势在建立。"),
    "early_bearish_stack": ("初步空头排列", "短均线刚开始跌破中长均线，弱势在建立。"),
    "mixed": ("均线混合", "均线方向不一致，结构还不够清晰。"),
}

_MACD_STATE_TERMS: dict[str, tuple[str, str]] = {
    "bullish_cross": ("MACD金叉", "快线向上穿越信号线，动能转强。"),
    "bearish_cross": ("MACD死叉", "快线向下穿越信号线，动能转弱。"),
    "bullish_zone": ("MACD多头区", "MACD 位于零轴上方，多头动能占优。"),
    "bearish_zone": ("MACD空头区", "MACD 位于零轴下方，空头动能占优。"),
    "neutral": ("MACD中性", "MACD 暂未给出清晰方向。"),
}

_BOLL_STATE_TERMS: dict[str, tuple[str, str]] = {
    "upper_breakout": ("上轨突破", "价格贴近或突破布林上轨，偏强。"),
    "lower_breakdown": ("下轨跌破", "价格贴近或跌破布林下轨，偏弱。"),
    "squeeze": ("布林收口", "波动率压缩，后续可能迎来方向选择。"),
    "neutral": ("布林中性", "布林带暂未给出明显边缘信号。"),
}

_TEXT_REPLACEMENTS: dict[str, str] = {
    **_SIGNAL_NAME_LABELS,
    "uptrend": "上升趋势",
    "downtrend": "下降趋势",
    "sideways": "震荡整理",
    "unknown": "待确认",
    "multi_timeframe": "多周期共振",
    "bullish_reversal": "看多反转",
    "bearish_reversal": "看空反转",
    "bullish_continuation": "看多延续",
    "bearish_continuation": "看空延续",
    "body_ratio": "实体占比",
    "upper_shadow_ratio": "上影占比",
    "lower_shadow_ratio": "下影占比",
}


def build_market_analysis_display_payload(
    analysis: BtcMarketAnalysis,
    *,
    latest_report_path: Path | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "标的": analysis.symbol,
        "生成时间(UTC)": analysis.generated_at,
        "综合方向": {
            "结果": _term_label(analysis.direction, _DIRECTION_TERMS),
            "解释": _term_explanation(analysis.direction, _DIRECTION_TERMS),
        },
        "综合评分": analysis.score,
        "综合置信度": _format_pct(analysis.confidence),
        "多周期共振": {
            "方向": _term_label(analysis.resonance.direction, _DIRECTION_TERMS),
            "评分": analysis.resonance.score,
            "置信度": _format_pct(analysis.resonance.confidence),
            "共振周期": list(analysis.resonance.aligned_timeframes) or ["无"],
            "说明": _localize_text(analysis.resonance.summary),
        },
        "核心原因": [_localize_text(item) for item in analysis.reason],
        "周期分析": [_build_timeframe_display_payload(item) for item in analysis.timeframes],
    }
    if latest_report_path is not None:
        payload["已保存报告"] = str(latest_report_path)
    return payload


def build_market_analysis_overview_text(
    analysis: BtcMarketAnalysis,
    *,
    latest_report_path: Path | None = None,
) -> str:
    lines = [
        f"标的：{analysis.symbol}",
        f"生成时间(UTC)：{analysis.generated_at}",
        f"综合方向：{_describe_direction(analysis.direction)}",
        f"综合评分：{analysis.score}",
        f"综合置信度：{_format_pct(analysis.confidence)}",
        f"多周期共振：{_localize_text(analysis.resonance.summary)}",
        f"共振周期：{', '.join(analysis.resonance.aligned_timeframes) or '无'}",
    ]
    if latest_report_path is not None:
        lines.append(f"已保存报告：{latest_report_path}")
    lines.extend(["", "核心原因："])
    for reason in analysis.reason:
        lines.append(f"- {_localize_text(reason)}")
    for timeframe in analysis.timeframes:
        lines.extend(
            [
                "",
                (
                    f"[{timeframe.timeframe}] 方向={_term_label(timeframe.direction, _DIRECTION_TERMS)}"
                    f" | 评分={timeframe.score} | 置信度={_format_pct(timeframe.confidence)}"
                ),
                f"趋势语境：{_describe_trend_context(timeframe.trend_context)}",
            ]
        )
        for signal in timeframe.signals[:6]:
            lines.append(f"- {_describe_signal(signal.category, signal.name, signal.bias, signal.score, signal.reason)}")
    return "\n".join(lines)


class BtcMarketAnalysisWindow:
    def __init__(
        self,
        master,
        client: OkxRestClient,
        *,
        logger: Logger | None = None,
    ) -> None:
        self.client = client
        self.logger = logger
        self._analysis: BtcMarketAnalysis | None = None
        self._latest_report_path: Path | None = None
        self._request_token = 0
        self._loading = False

        self.window = Toplevel(master)
        self.window.title("BTC 行情分析")
        apply_window_icon(self.window)
        apply_adaptive_window_geometry(
            self.window,
            width_ratio=0.82,
            height_ratio=0.82,
            min_width=1180,
            min_height=860,
            max_width=1760,
            max_height=1280,
        )

        self.symbol = StringVar(value="BTC-USDT-SWAP")
        self.direction_mode_label = StringVar(value="按前收涨跌")
        self.send_email_after_run = BooleanVar(value=False)
        self.status_text = StringVar(value="点击“立即分析”后拉取 1H / 4H / 1D 数据并生成综合结论。")
        self.summary_text = StringVar(value="暂无结果。")
        self.report_path_text = StringVar(value="-")
        self.generated_at_text = StringVar(value="-")
        self.direction_text = StringVar(value="-")
        self.score_text = StringVar(value="-")
        self.confidence_text = StringVar(value="-")
        self.resonance_text = StringVar(value="-")
        self._timeframe_vars = {item: BooleanVar(value=True) for item in DEFAULT_ANALYSIS_TIMEFRAMES}

        self._overview_text: Text | None = None
        self._json_text: Text | None = None
        self._timeframe_tree: ttk.Treeview | None = None
        self._signal_tree: ttk.Treeview | None = None

        self._build_layout()
        self.window.protocol("WM_DELETE_WINDOW", self.destroy)

    def show(self) -> None:
        self.window.deiconify()
        self.window.lift()
        self.window.focus_force()

    def destroy(self) -> None:
        try:
            self.window.destroy()
        except Exception:
            pass

    def _build_layout(self) -> None:
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(2, weight=1)

        controls = ttk.LabelFrame(self.window, text="分析参数", padding=16)
        controls.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        for column in range(8):
            controls.columnconfigure(column, weight=1)

        ttk.Label(controls, text="标的").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.symbol,
            values=DEFAULT_ANALYSIS_SYMBOLS,
            state="readonly",
        ).grid(row=0, column=1, sticky="ew", padx=(0, 12))

        ttk.Label(controls, text="概率口径").grid(row=0, column=2, sticky="w")
        direction_combo = ttk.Combobox(
            controls,
            textvariable=self.direction_mode_label,
            values=("按前收涨跌", "按K线实体"),
            state="readonly",
        )
        direction_combo.grid(row=0, column=3, sticky="ew", padx=(0, 12))

        timeframe_frame = ttk.Frame(controls)
        timeframe_frame.grid(row=0, column=4, columnspan=2, sticky="w")
        ttk.Label(timeframe_frame, text="周期").grid(row=0, column=0, sticky="w", padx=(0, 8))
        for index, timeframe in enumerate(DEFAULT_ANALYSIS_TIMEFRAMES, start=1):
            ttk.Checkbutton(timeframe_frame, text=timeframe, variable=self._timeframe_vars[timeframe]).grid(
                row=0,
                column=index,
                sticky="w",
                padx=(0, 8),
            )

        ttk.Checkbutton(
            controls,
            text="分析完成后发邮件",
            variable=self.send_email_after_run,
        ).grid(row=0, column=6, sticky="w")

        action_frame = ttk.Frame(controls)
        action_frame.grid(row=0, column=7, sticky="e")
        ttk.Button(action_frame, text="立即分析", command=self.run_analysis).grid(row=0, column=0)
        ttk.Button(action_frame, text="发送当前邮件", command=self.send_current_email).grid(row=0, column=1, padx=(8, 0))

        ttk.Label(controls, textvariable=self.status_text, wraplength=1040, justify="left").grid(
            row=1,
            column=0,
            columnspan=8,
            sticky="w",
            pady=(12, 0),
        )

        summary = ttk.LabelFrame(self.window, text="综合结论", padding=16)
        summary.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))
        for column in range(6):
            summary.columnconfigure(column, weight=1)

        _build_stat(summary, row=0, column=0, label="生成时间", textvariable=self.generated_at_text)
        _build_stat(summary, row=0, column=1, label="综合方向", textvariable=self.direction_text)
        _build_stat(summary, row=0, column=2, label="综合评分", textvariable=self.score_text)
        _build_stat(summary, row=0, column=3, label="置信度", textvariable=self.confidence_text)
        _build_stat(summary, row=0, column=4, label="多周期共振", textvariable=self.resonance_text)
        _build_stat(summary, row=0, column=5, label="最新报告", textvariable=self.report_path_text)

        body = ttk.Panedwindow(self.window, orient="horizontal")
        body.grid(row=2, column=0, sticky="nsew", padx=16, pady=(0, 16))

        left = ttk.Frame(body, padding=0)
        right = ttk.Frame(body, padding=0)
        body.add(left, weight=5)
        body.add(right, weight=4)

        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        ttk.Label(left, textvariable=self.summary_text, wraplength=720, justify="left").grid(
            row=0,
            column=0,
            sticky="w",
            pady=(0, 8),
        )

        notebook = ttk.Notebook(left)
        notebook.grid(row=1, column=0, sticky="nsew")

        overview_tab = ttk.Frame(notebook, padding=10)
        overview_tab.columnconfigure(0, weight=1)
        overview_tab.rowconfigure(0, weight=1)
        self._overview_text = Text(overview_tab, wrap="word", font=("Microsoft YaHei UI", 10))
        self._overview_text.grid(row=0, column=0, sticky="nsew")
        notebook.add(overview_tab, text="分析摘要")

        json_tab = ttk.Frame(notebook, padding=10)
        json_tab.columnconfigure(0, weight=1)
        json_tab.rowconfigure(0, weight=1)
        self._json_text = Text(json_tab, wrap="none", font=("Consolas", 10))
        self._json_text.grid(row=0, column=0, sticky="nsew")
        notebook.add(json_tab, text="JSON（中文展示）")

        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        right.rowconfigure(3, weight=1)

        ttk.Label(right, text="周期结论", font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        timeframe_tree = ttk.Treeview(
            right,
            columns=("timeframe", "direction", "score", "confidence", "trend"),
            show="headings",
            height=7,
        )
        timeframe_tree.grid(row=1, column=0, sticky="nsew", pady=(6, 12))
        for key, label, width in (
            ("timeframe", "周期", 80),
            ("direction", "方向", 90),
            ("score", "评分", 70),
            ("confidence", "置信度", 90),
            ("trend", "趋势语境", 120),
        ):
            timeframe_tree.heading(key, text=label)
            timeframe_tree.column(key, width=width, anchor="center")
        self._timeframe_tree = timeframe_tree

        ttk.Label(right, text="信号明细", font=("Microsoft YaHei UI", 11, "bold")).grid(row=2, column=0, sticky="w")
        signal_tree = ttk.Treeview(
            right,
            columns=("timeframe", "category", "name", "bias", "score"),
            show="headings",
            height=12,
        )
        signal_tree.grid(row=3, column=0, sticky="nsew", pady=(6, 0))
        for key, label, width in (
            ("timeframe", "周期", 70),
            ("category", "类别", 90),
            ("name", "信号", 180),
            ("bias", "偏向", 70),
            ("score", "分值", 60),
        ):
            signal_tree.heading(key, text=label)
            signal_tree.column(key, width=width, anchor="center")
        self._signal_tree = signal_tree

    def run_analysis(self) -> None:
        if self._loading:
            return
        timeframes = tuple(item for item in DEFAULT_ANALYSIS_TIMEFRAMES if self._timeframe_vars[item].get())
        if not timeframes:
            messagebox.showinfo("提示", "请至少选择一个分析周期。", parent=self.window)
            return

        symbol = self.symbol.get().strip().upper()
        direction_mode = "close_to_close" if self.direction_mode_label.get() == "按前收涨跌" else "candle_body"
        should_send_email = self.send_email_after_run.get()
        config = BtcMarketAnalyzerConfig(
            timeframes=timeframes,
            history_limits=tuple(item for item in DEFAULT_ANALYSIS_HISTORY_LIMITS if item[0] in timeframes),
            probability_config=MarketAnalysisConfig(direction_mode=direction_mode),
        )

        self._loading = True
        self._request_token += 1
        request_token = self._request_token
        self.status_text.set(f"正在拉取 {symbol} 的 {', '.join(timeframes)} 数据并生成行情分析，请稍候。")
        self._log(f"BTC 行情分析开始 | {symbol} | 周期={','.join(timeframes)} | 口径={direction_mode}")

        def _worker() -> None:
            try:
                analysis = analyze_btc_market_from_client(self.client, symbol=symbol, config=config)
                report_path = save_btc_market_analysis(analysis)
                email_sent = False
                if should_send_email:
                    email_sent = send_btc_market_analysis_email(analysis)
                self.window.after(
                    0,
                    lambda: self._on_analysis_success(
                        request_token,
                        analysis=analysis,
                        report_path=report_path,
                        email_sent=email_sent,
                    ),
                )
            except Exception as exc:
                self.window.after(0, lambda: self._on_analysis_error(request_token, exc))

        threading.Thread(target=_worker, daemon=True, name="btc-market-analysis").start()

    def send_current_email(self) -> None:
        if self._analysis is None:
            messagebox.showinfo("提示", "请先运行一次行情分析。", parent=self.window)
            return
        self.status_text.set("正在发送当前行情分析邮件，请稍候。")

        def _worker() -> None:
            try:
                delivered = send_btc_market_analysis_email(self._analysis)
                self.window.after(0, lambda: self._on_email_finished(delivered))
            except Exception as exc:
                self.window.after(0, lambda: self._on_email_error(exc))

        threading.Thread(target=_worker, daemon=True, name="btc-market-email").start()

    def _on_analysis_success(
        self,
        request_token: int,
        *,
        analysis: BtcMarketAnalysis,
        report_path: Path,
        email_sent: bool,
    ) -> None:
        if request_token != self._request_token:
            return
        self._loading = False
        self._analysis = analysis
        self._latest_report_path = report_path
        self.generated_at_text.set(analysis.generated_at)
        self.direction_text.set(_term_label(analysis.direction, _DIRECTION_TERMS))
        self.score_text.set(str(analysis.score))
        self.confidence_text.set(_format_pct(analysis.confidence))
        self.resonance_text.set(_localize_text(analysis.resonance.summary))
        self.report_path_text.set(str(report_path))
        self.summary_text.set(
            "；".join(_localize_text(item) for item in analysis.reason[:3]) if analysis.reason else "暂无明确结论。"
        )
        self.status_text.set(
            f"分析完成：{analysis.symbol} | 方向={_term_label(analysis.direction, _DIRECTION_TERMS)} | 评分={analysis.score} | "
            f"邮件={'已发送' if email_sent else '未发送'}"
        )
        self._render_analysis(analysis, report_path)
        self._log(
            f"BTC 行情分析完成 | {analysis.symbol} | 方向={_term_label(analysis.direction, _DIRECTION_TERMS)} | 评分={analysis.score} | 报告={report_path}"
        )

    def _on_analysis_error(self, request_token: int, exc: Exception) -> None:
        if request_token != self._request_token:
            return
        self._loading = False
        self.status_text.set(f"分析失败：{exc}")
        self._log(f"BTC 行情分析失败 | {exc}")
        messagebox.showerror("分析失败", f"生成行情分析时出错：\n{exc}", parent=self.window)

    def _on_email_finished(self, delivered: bool) -> None:
        self.status_text.set("邮件已发送。" if delivered else "当前邮件配置不可用，未发送。")
        if delivered:
            self._log("BTC 行情分析邮件已发送")

    def _on_email_error(self, exc: Exception) -> None:
        self.status_text.set(f"邮件发送失败：{exc}")
        self._log(f"BTC 行情分析邮件发送失败 | {exc}")
        messagebox.showerror("发送失败", f"发送行情分析邮件时出错：\n{exc}", parent=self.window)

    def _render_analysis(self, analysis: BtcMarketAnalysis, report_path: Path) -> None:
        overview = build_market_analysis_overview_text(analysis, latest_report_path=report_path)
        display_payload = build_market_analysis_display_payload(analysis, latest_report_path=report_path)
        if self._overview_text is not None:
            self._overview_text.delete("1.0", END)
            self._overview_text.insert("1.0", overview)
        if self._json_text is not None:
            self._json_text.delete("1.0", END)
            self._json_text.insert("1.0", json.dumps(display_payload, ensure_ascii=False, indent=2))

        if self._timeframe_tree is not None:
            self._timeframe_tree.delete(*self._timeframe_tree.get_children())
            for item in analysis.timeframes:
                self._timeframe_tree.insert(
                    "",
                    END,
                    values=(
                        item.timeframe,
                        _term_label(item.direction, _DIRECTION_TERMS),
                        item.score,
                        _format_pct(item.confidence),
                        _term_label(item.trend_context, _TREND_CONTEXT_TERMS),
                    ),
                )

        if self._signal_tree is not None:
            self._signal_tree.delete(*self._signal_tree.get_children())
            for item in analysis.signals:
                self._signal_tree.insert(
                    "",
                    END,
                    values=(
                        item.timeframe or "-",
                        _term_label(item.category, _SIGNAL_CATEGORY_TERMS),
                        _signal_name_label(item.name),
                        _term_label(item.bias, _SIGNAL_BIAS_TERMS),
                        item.score,
                    ),
                )

    def _log(self, message: str) -> None:
        if self.logger is not None:
            timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
            self.logger(f"[{timestamp}] {message}")


def _build_stat(parent, *, row: int, column: int, label: str, textvariable: StringVar) -> None:
    frame = ttk.Frame(parent)
    frame.grid(row=row, column=column, sticky="nsew", padx=(0, 8))
    ttk.Label(frame, text=label, font=("Microsoft YaHei UI", 9)).grid(row=0, column=0, sticky="w")
    ttk.Label(frame, textvariable=textvariable, font=("Microsoft YaHei UI", 10, "bold"), wraplength=220, justify="left").grid(
        row=1,
        column=0,
        sticky="w",
        pady=(4, 0),
    )


def _format_pct(value) -> str:
    return f"{value * 100:.2f}%"


def _term_label(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    label, _ = mapping.get(value, (value, ""))
    return label


def _term_explanation(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    _, explanation = mapping.get(value, (value, ""))
    return explanation or "暂无补充说明。"


def _describe_direction(value: str | None) -> str:
    return _describe_term(value, _DIRECTION_TERMS)


def _describe_trend_context(value: str | None) -> str:
    return _describe_term(value, _TREND_CONTEXT_TERMS)


def _describe_bias(value: str | None) -> str:
    return _describe_term(value, _SIGNAL_BIAS_TERMS)


def _describe_term(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    label, explanation = mapping.get(value, (value, ""))
    if explanation:
        return f"{label}（{explanation}）"
    return label


def _signal_name_label(name: str | None) -> str:
    if not name:
        return "-"
    return _SIGNAL_NAME_LABELS.get(name, name)


def _localize_text(text: str | None) -> str:
    if not text:
        return "-"
    localized = text
    for source in sorted(_TEXT_REPLACEMENTS, key=len, reverse=True):
        localized = localized.replace(source, _TEXT_REPLACEMENTS[source])
    return localized


def _describe_signal(category: str, name: str, bias: str, score: int, reason: str) -> str:
    return (
        f"{_term_label(category, _SIGNAL_CATEGORY_TERMS)} / {_signal_name_label(name)}"
        f" | 偏向={_term_label(bias, _SIGNAL_BIAS_TERMS)}"
        f" | 分值={score}"
        f" | {_localize_text(reason)}"
    )


def _build_timeframe_display_payload(timeframe) -> dict[str, object]:
    payload: dict[str, object] = {
        "周期": timeframe.timeframe,
        "方向": {
            "结果": _term_label(timeframe.direction, _DIRECTION_TERMS),
            "解释": _term_explanation(timeframe.direction, _DIRECTION_TERMS),
        },
        "评分": timeframe.score,
        "置信度": _format_pct(timeframe.confidence),
        "趋势语境": {
            "结果": _term_label(timeframe.trend_context, _TREND_CONTEXT_TERMS),
            "解释": _term_explanation(timeframe.trend_context, _TREND_CONTEXT_TERMS),
        },
        "核心原因": [_localize_text(item) for item in timeframe.reason],
        "信号": [_build_signal_display_payload(signal) for signal in timeframe.signals],
    }
    probability_payload = _build_probability_display_payload(timeframe.probability)
    if probability_payload:
        payload["概率快照"] = probability_payload
    indicator_payload = _build_indicator_display_payload(timeframe.indicators)
    if indicator_payload:
        payload["指标快照"] = indicator_payload
    pattern_payload = _build_pattern_display_payload(timeframe)
    if pattern_payload:
        payload["K线形态"] = pattern_payload
    return payload


def _build_signal_display_payload(signal) -> dict[str, object]:
    return {
        "类别": _term_label(signal.category, _SIGNAL_CATEGORY_TERMS),
        "名称": _signal_name_label(signal.name),
        "偏向": {
            "结果": _term_label(signal.bias, _SIGNAL_BIAS_TERMS),
            "解释": _term_explanation(signal.bias, _SIGNAL_BIAS_TERMS),
        },
        "分值": signal.score,
        "趋势语境": {
            "结果": _term_label(signal.trend_context, _TREND_CONTEXT_TERMS),
            "解释": _term_explanation(signal.trend_context, _TREND_CONTEXT_TERMS),
        },
        "说明": _localize_text(signal.reason),
    }


def _build_probability_display_payload(probability: dict[str, object]) -> dict[str, object]:
    if not probability:
        return {}
    payload: dict[str, object] = {
        "统计口径": _DIRECTION_MODE_LABELS.get(probability.get("direction_mode"), probability.get("direction_mode") or "-"),
        "无条件收阳概率": _format_probability(probability.get("baseline_bullish_probability")),
        "当前连阳天数": probability.get("current_bullish_streak"),
        "最近一轮完整连阳": probability.get("last_completed_bullish_streak"),
        "首阴回调等级": _describe_optional_term(probability.get("latest_pullback_bucket"), _PULLBACK_BUCKET_TERMS),
        "关键支撑状态": _describe_support_break(probability.get("latest_support_break")),
        "波动率环境": _describe_optional_term(probability.get("latest_volatility_regime"), _VOLATILITY_REGIME_TERMS),
    }
    active_factors = probability.get("active_factors") or []
    if active_factors:
        payload["激活因子"] = [
            {
                "名称": factor.get("label") or _signal_name_label(factor.get("key")),
                "偏向": _describe_optional_term(factor.get("direction_bias"), _PROBABILITY_DIRECTION_TERMS),
                "强度": factor.get("score"),
                "说明": _localize_text(factor.get("reason")),
            }
            for factor in active_factors
        ]
    return payload


def _build_indicator_display_payload(indicators: dict[str, object]) -> dict[str, object]:
    if not indicators:
        return {}
    payload: dict[str, object] = {}
    if indicators.get("close") is not None:
        payload["最新收盘"] = indicators.get("close")
    ema_payload = indicators.get("ema") or {}
    if ema_payload:
        payload["EMA结构"] = {
            "排列状态": _describe_optional_term(ema_payload.get("state"), _EMA_STATE_TERMS),
            "周期": ema_payload.get("periods"),
            "数值": ema_payload.get("values"),
            "斜率": ema_payload.get("slopes"),
        }
    macd_payload = indicators.get("macd") or {}
    if macd_payload:
        payload["MACD"] = {
            "状态": _describe_optional_term(macd_payload.get("state"), _MACD_STATE_TERMS),
            "快线": macd_payload.get("line"),
            "信号线": macd_payload.get("signal"),
            "柱体": macd_payload.get("histogram"),
        }
    boll_payload = indicators.get("boll") or {}
    if boll_payload:
        payload["布林带"] = {
            "状态": _describe_optional_term(boll_payload.get("state"), _BOLL_STATE_TERMS),
            "上轨": boll_payload.get("upper"),
            "中轨": boll_payload.get("middle"),
            "下轨": boll_payload.get("lower"),
            "%B": boll_payload.get("percent_b"),
            "带宽": boll_payload.get("bandwidth"),
        }
    return payload


def _build_pattern_display_payload(timeframe) -> dict[str, object]:
    pattern_payload = timeframe.pattern or {}
    if not pattern_payload:
        return {}
    matches = pattern_payload.get("matches") or []
    primary_match = matches[0] if matches else {}
    signal = next((item for item in timeframe.signals if item.category == "pattern"), None)
    payload: dict[str, object] = {}
    primary_pattern = pattern_payload.get("primary_pattern")
    if primary_pattern:
        payload["主形态"] = _signal_name_label(primary_pattern)
    if primary_match.get("bias"):
        payload["形态偏向"] = _describe_optional_term(primary_match.get("bias"), _PATTERN_BIAS_TERMS)
    if pattern_payload.get("trend_context"):
        payload["趋势语境"] = _describe_trend_context(pattern_payload.get("trend_context"))
    if signal is not None:
        payload["说明"] = _localize_text(signal.reason)
    elif primary_match.get("reason"):
        payload["说明"] = _localize_text(primary_match.get("reason"))
    return payload


def _describe_optional_term(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "当前未触发"
    return _describe_term(value, mapping)


def _describe_support_break(value: Any) -> str:
    if value is True:
        return "已跌破关键支撑（连阳结构被破坏，需防继续走弱）。"
    if value is False:
        return "未跌破关键支撑（更像正常回踩，结构仍可观察）。"
    return "当前未触发首阴破位场景。"


def _format_probability(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return _format_pct(float(value))
    except (TypeError, ValueError):
        return str(value)
