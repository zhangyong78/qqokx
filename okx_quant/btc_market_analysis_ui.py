from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tkinter import END, BooleanVar, StringVar, Text, Toplevel
from tkinter import messagebox, ttk
from typing import Any, Callable

from okx_quant.btc_market_analyzer import (
    BtcMarketAnalysis,
    BtcMarketAnalyzerConfig,
    analyze_btc_market_at_time,
    analyze_btc_market_from_client,
    save_btc_market_analysis,
    send_btc_market_analysis_email,
)
from okx_quant.market_analysis import MarketAnalysisConfig
from okx_quant.okx_client import OkxRestClient
from okx_quant.window_layout import apply_adaptive_window_geometry, apply_window_icon


Logger = Callable[[str], None]

try:
    from zoneinfo import ZoneInfo

    _ANALYSIS_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    _ANALYSIS_TZ = timezone(timedelta(hours=8))


DEFAULT_ANALYSIS_SYMBOLS: tuple[str, ...] = ("BTC-USDT-SWAP", "BTC-USDT")
DEFAULT_ANALYSIS_TIMEFRAMES: tuple[str, ...] = ("1H", "4H", "1D")
DEFAULT_ANALYSIS_HISTORY_LIMITS: tuple[tuple[str, int], ...] = (("1H", 5000), ("4H", 5000), ("1D", 0))
DEFAULT_ANALYSIS_MODE_LABELS: tuple[str, str] = ("实时分析", "历史复盘")
REPLAY_TIME_HINT = "YYYY-MM-DD HH:MM"

_DIRECTION_TERMS: dict[str, tuple[str, str]] = {
    "long": ("看多", "多头占优"),
    "short": ("看空", "空头占优"),
    "neutral": ("中性", "方向未明"),
}
_TREND_CONTEXT_TERMS: dict[str, tuple[str, str]] = {
    "uptrend": ("上升趋势", "结构偏多"),
    "downtrend": ("下降趋势", "结构偏空"),
    "sideways": ("震荡整理", "方向混合"),
    "unknown": ("待确认", "信息不足"),
    "multi_timeframe": ("多周期共振", "跨周期汇总"),
}
_SIGNAL_CATEGORY_TERMS: dict[str, tuple[str, str]] = {
    "indicator": ("指标", "来自 EMA/MACD/BOLL"),
    "pattern": ("K线形态", "来自单根K线形态"),
    "probability": ("概率因子", "来自历史统计"),
    "resonance": ("多周期共振", "来自周期一致性"),
}
_SIGNAL_BIAS_TERMS: dict[str, tuple[str, str]] = {
    "long": ("偏多", "支持上涨"),
    "short": ("偏空", "支持下跌"),
    "caution": ("谨慎", "提示风险"),
    "neutral": ("中性", "无明显倾向"),
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
}
_TEXT_REPLACEMENTS: dict[str, str] = {
    **_SIGNAL_NAME_LABELS,
    "uptrend": "上升趋势",
    "downtrend": "下降趋势",
    "sideways": "震荡整理",
    "unknown": "待确认",
    "multi_timeframe": "多周期共振",
}


def build_market_analysis_display_payload(
    analysis: BtcMarketAnalysis,
    *,
    latest_report_path: Path | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "综合方向": {
            "结果": _term_label(analysis.direction, _DIRECTION_TERMS),
            "解释": _term_explanation(analysis.direction, _DIRECTION_TERMS),
        },
        "综合评分": analysis.score,
        "综合置信度": _format_pct(analysis.confidence),
        "标的": analysis.symbol,
        "生成时间(北京时间)": _display_generated_at(analysis.generated_at),
        "多周期共振": {
            "方向": _term_label(analysis.resonance.direction, _DIRECTION_TERMS),
            "评分": analysis.resonance.score,
            "置信度": _format_pct(analysis.resonance.confidence),
            "共振周期": list(analysis.resonance.aligned_timeframes) or ["无"],
            "说明": _localize_text(analysis.resonance.summary),
        },
        "K线时间(北京时间)": _display_candle_time(analysis),
        "核心原因": [_localize_text(item) for item in analysis.reason],
        "周期分析": [_build_timeframe_display_payload(item) for item in analysis.timeframes],
    }
    if analysis.mode != "realtime":
        payload["分析模式"] = analysis.mode
        payload["复盘时点"] = analysis.analysis_point or "-"
    if analysis.validation is not None:
        payload["验证"] = {
            "结论": analysis.validation.verdict,
            "状态": analysis.validation.status,
            "评估周期": [f"{item.hours}H" for item in analysis.validation.windows],
            "最大顺行(%)": _maybe_pct(analysis.validation.max_favorable_excursion_pct),
            "最大逆行(%)": _maybe_pct(analysis.validation.max_adverse_excursion_pct),
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
        f"生成时间(北京时间)：{_display_generated_at(analysis.generated_at)}",
        f"K线时间(北京时间)：{_display_candle_time(analysis)}",
        f"综合方向：{_describe_direction(analysis.direction)}",
        f"综合评分：{analysis.score}",
        f"综合置信度：{_format_pct(analysis.confidence)}",
        f"多周期共振：{_localize_text(analysis.resonance.summary)}",
        f"共振周期：{', '.join(analysis.resonance.aligned_timeframes) or '无'}",
    ]
    if analysis.mode != "realtime":
        lines.append(f"分析模式：{analysis.mode}")
        lines.append(f"复盘时点：{analysis.analysis_point or '-'}")
    if analysis.validation is not None:
        lines.append(f"验证结论：{analysis.validation.verdict}")
    if latest_report_path is not None:
        lines.append(f"已保存报告：{latest_report_path}")
    lines.extend(["", "核心原因："])
    for reason in analysis.reason:
        lines.append(f"- {_localize_text(reason)}")
    for timeframe in analysis.timeframes:
        lines.extend(
            [
                "",
                f"[{timeframe.timeframe}] 方向={_term_label(timeframe.direction, _DIRECTION_TERMS)} | 评分={timeframe.score} | 置信度={_format_pct(timeframe.confidence)}",
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
        self.analysis_mode_label = StringVar(value=DEFAULT_ANALYSIS_MODE_LABELS[0])
        self.direction_mode_label = StringVar(value="按前一根收盘比较")
        self.replay_time_text = StringVar(value=datetime.now(_ANALYSIS_TZ).strftime("%Y-%m-%d %H:00"))
        self.send_email_after_run = BooleanVar(value=False)
        self.status_text = StringVar(value="点击“立即分析”后生成 BTC 多周期分析。")
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

        ttk.Label(controls, text="模式").grid(row=0, column=0, sticky="w")
        ttk.Combobox(controls, textvariable=self.analysis_mode_label, values=DEFAULT_ANALYSIS_MODE_LABELS, state="readonly").grid(
            row=0, column=1, sticky="ew", padx=(0, 12)
        )

        ttk.Label(controls, text="标的").grid(row=0, column=2, sticky="w")
        ttk.Combobox(controls, textvariable=self.symbol, values=DEFAULT_ANALYSIS_SYMBOLS, state="readonly").grid(
            row=0, column=3, sticky="ew", padx=(0, 12)
        )

        ttk.Label(controls, text="概率口径").grid(row=0, column=4, sticky="w")
        ttk.Combobox(controls, textvariable=self.direction_mode_label, values=("按前一根收盘比较", "按本根开收比较"), state="readonly").grid(
            row=0, column=5, sticky="ew", padx=(0, 12)
        )

        timeframe_frame = ttk.Frame(controls)
        timeframe_frame.grid(row=1, column=0, columnspan=4, sticky="w", pady=(10, 0))
        ttk.Label(timeframe_frame, text="周期").grid(row=0, column=0, sticky="w", padx=(0, 8))
        for index, timeframe in enumerate(DEFAULT_ANALYSIS_TIMEFRAMES, start=1):
            ttk.Checkbutton(timeframe_frame, text=timeframe, variable=self._timeframe_vars[timeframe]).grid(row=0, column=index, sticky="w", padx=(0, 8))

        ttk.Label(controls, text=f"复盘时间 ({REPLAY_TIME_HINT})").grid(row=1, column=4, sticky="w", pady=(10, 0))
        ttk.Entry(controls, textvariable=self.replay_time_text).grid(row=1, column=5, sticky="ew", padx=(0, 12), pady=(10, 0))

        ttk.Checkbutton(controls, text="分析完成后发邮件", variable=self.send_email_after_run).grid(
            row=1, column=6, sticky="w", pady=(10, 0)
        )

        action_frame = ttk.Frame(controls)
        action_frame.grid(row=1, column=7, sticky="e", pady=(10, 0))
        ttk.Button(action_frame, text="立即分析", command=self.run_analysis).grid(row=0, column=0)
        ttk.Button(action_frame, text="发送当前邮件", command=self.send_current_email).grid(row=0, column=1, padx=(8, 0))

        ttk.Label(controls, textvariable=self.status_text, wraplength=1040, justify="left").grid(
            row=2, column=0, columnspan=8, sticky="w", pady=(12, 0)
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
        ttk.Label(left, textvariable=self.summary_text, wraplength=720, justify="left").grid(row=0, column=0, sticky="w", pady=(0, 8))

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
        notebook.add(json_tab, text="JSON")

        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        right.rowconfigure(3, weight=1)

        ttk.Label(right, text="周期结论", font=("Microsoft YaHei UI", 11, "bold")).grid(row=0, column=0, sticky="w")
        timeframe_tree = ttk.Treeview(right, columns=("timeframe", "direction", "score", "confidence", "trend"), show="headings", height=7)
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
        signal_tree = ttk.Treeview(right, columns=("timeframe", "category", "name", "bias", "score"), show="headings", height=12)
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
        direction_mode = "close_to_close" if self.direction_mode_label.get() == "按前一根收盘比较" else "candle_body"
        should_send_email = self.send_email_after_run.get()
        config = BtcMarketAnalyzerConfig(
            timeframes=timeframes,
            history_limits=tuple(item for item in DEFAULT_ANALYSIS_HISTORY_LIMITS if item[0] in timeframes),
            probability_config=MarketAnalysisConfig(direction_mode=direction_mode),
        )
        replay_dt = None
        if self.analysis_mode_label.get() == "历史复盘":
            replay_dt = parse_replay_time_text(self.replay_time_text.get())
            if replay_dt is None:
                messagebox.showinfo("提示", f"复盘时间格式不正确，请使用 {REPLAY_TIME_HINT}。", parent=self.window)
                return

        self._loading = True
        self._request_token += 1
        request_token = self._request_token
        if replay_dt is None:
            self.status_text.set(f"正在拉取 {symbol} 的 {', '.join(timeframes)} 数据并生成实时分析，请稍候。")
        else:
            self.status_text.set(f"正在按 {replay_dt.strftime('%Y-%m-%d %H:%M %Z')} 之前的已确认K线做复盘分析，请稍候。")
        self._log(f"BTC 行情分析开始 | {symbol} | 模式={self.analysis_mode_label.get()} | 周期={','.join(timeframes)}")

        def _worker() -> None:
            try:
                if replay_dt is None:
                    analysis = analyze_btc_market_from_client(self.client, symbol=symbol, config=config)
                else:
                    analysis = analyze_btc_market_at_time(self.client, symbol=symbol, analysis_dt=replay_dt, config=config)
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
        self.generated_at_text.set(_display_generated_at(analysis.generated_at))
        self.direction_text.set(_term_label(analysis.direction, _DIRECTION_TERMS))
        self.score_text.set(str(analysis.score))
        self.confidence_text.set(_format_pct(analysis.confidence))
        self.resonance_text.set(_localize_text(analysis.resonance.summary))
        self.report_path_text.set(str(report_path))
        summary_lines = [_localize_text(item) for item in analysis.reason[:3]] if analysis.reason else ["暂无明确结论。"]
        if analysis.validation is not None:
            summary_lines.append(f"验证结论：{analysis.validation.verdict}")
        self.summary_text.set("；".join(summary_lines))
        self.status_text.set(
            f"分析完成：{analysis.symbol} | 方向={_term_label(analysis.direction, _DIRECTION_TERMS)} | 评分={analysis.score} | 邮件={'已发送' if email_sent else '未发送'}"
        )
        self._render_analysis(analysis, report_path)
        self._log(f"BTC 行情分析完成 | {analysis.symbol} | 方向={analysis.direction} | 评分={analysis.score} | 报告={report_path}")

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


def parse_replay_time_text(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=_ANALYSIS_TZ)
        except ValueError:
            continue
    return None



def _display_generated_at(value: str | None) -> str:
    return _format_beijing_time_text(value)


def _display_candle_time(analysis: BtcMarketAnalysis) -> str:
    candle_ts = _analysis_candle_ts(analysis)
    if candle_ts is None:
        return "-"
    return _format_beijing_ts_ms(candle_ts)


def _analysis_candle_ts(analysis: BtcMarketAnalysis) -> int | None:
    for timeframe in analysis.timeframes:
        if timeframe.candle_ts is not None:
            return int(timeframe.candle_ts)
    return None


def _format_beijing_time_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_ANALYSIS_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _format_beijing_ts_ms(value: int | None) -> str:
    if value is None:
        return "-"
    try:
        dt = datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)
    except Exception:
        return "-"
    return dt.astimezone(_ANALYSIS_TZ).strftime("%Y-%m-%d %H:%M")

def _build_stat(parent, *, row: int, column: int, label: str, textvariable: StringVar) -> None:
    frame = ttk.Frame(parent)
    frame.grid(row=row, column=column, sticky="nsew", padx=(0, 8))
    ttk.Label(frame, text=label, font=("Microsoft YaHei UI", 9)).grid(row=0, column=0, sticky="w")
    ttk.Label(frame, textvariable=textvariable, font=("Microsoft YaHei UI", 10, "bold"), wraplength=220, justify="left").grid(
        row=1, column=0, sticky="w", pady=(4, 0)
    )


def _format_pct(value) -> str:
    return f"{value * 100:.2f}%"


def _maybe_pct(value) -> str:
    return "-" if value is None else f"{value:.2f}%"


def _term_label(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    return mapping.get(value, (value, ""))[0]


def _term_explanation(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    return mapping.get(value, (value, ""))[1] or "暂无补充说明"


def _describe_direction(value: str | None) -> str:
    return _describe_term(value, _DIRECTION_TERMS)


def _describe_trend_context(value: str | None) -> str:
    return _describe_term(value, _TREND_CONTEXT_TERMS)


def _describe_term(value: str | None, mapping: dict[str, tuple[str, str]]) -> str:
    if value is None:
        return "-"
    label, explanation = mapping.get(value, (value, ""))
    return f"{label}（{explanation}）" if explanation else label


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
