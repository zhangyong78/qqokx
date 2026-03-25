from decimal import Decimal
from unittest import TestCase

from okx_quant.models import Candle
from okx_quant.signal_monitor import (
    MonitorRoundDiagnostic,
    MonitorSignalEvent,
    MonitorSymbolDiagnostic,
    SignalMonitorConfig,
    _bar_interval_seconds,
    _seconds_until_next_check,
    detect_candle_pattern_signal,
    detect_ema55_breakout,
    detect_ema55_slope_turn,
    detect_ema_cross_signal,
    evaluate_monitor_signal_report,
    evaluate_monitor_signals,
)
from okx_quant.signal_monitor_ui import SignalMonitorWindow, _format_monitor_diagnostic_round


class _FakeNotifier:
    def __init__(self, *, signal_notifications_enabled: bool) -> None:
        self.signal_notifications_enabled = signal_notifications_enabled
        self.sent_messages: list[tuple[str, str]] = []

    def notify_async(self, subject: str, body: str) -> None:
        self.sent_messages.append((subject, body))


def _make_candles(closes: list[Decimal]) -> list[Candle]:
    candles: list[Candle] = []
    for index, close in enumerate(closes, start=1):
        candles.append(
            Candle(
                ts=index,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("1"),
                confirmed=True,
            )
        )
    return candles


class SignalMonitorTest(TestCase):
    def test_ema21_55_cross_detects_long_signal(self) -> None:
        closes = [Decimal("100")] * 55 + [Decimal("95"), Decimal("96"), Decimal("97"), Decimal("120")]
        candles = _make_candles(closes)

        event = detect_ema_cross_signal("BTC-USDT-SWAP", candles, [item.close for item in candles])

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.direction, "long")
        self.assertEqual(event.signal_type, "ema21_55_cross")

    def test_ema55_breakout_and_slope_turn_detect_short_signal(self) -> None:
        closes = [Decimal("100")] * 55 + [Decimal("105"), Decimal("104"), Decimal("103"), Decimal("80")]
        candles = _make_candles(closes)
        close_values = [item.close for item in candles]

        breakout = detect_ema55_breakout("BTC-USDT-SWAP", candles, close_values)
        slope_turn = detect_ema55_slope_turn("BTC-USDT-SWAP", candles, close_values)

        self.assertIsNotNone(breakout)
        self.assertIsNotNone(slope_turn)
        assert breakout is not None
        assert slope_turn is not None
        self.assertEqual(breakout.direction, "short")
        self.assertEqual(slope_turn.direction, "short")

    def test_candle_pattern_detects_long_signal_near_ema(self) -> None:
        candles: list[Candle] = []
        for index in range(1, 70):
            if index < 69:
                candles.append(
                    Candle(
                        ts=index,
                        open=Decimal("100"),
                        high=Decimal("102"),
                        low=Decimal("98"),
                        close=Decimal("100"),
                        volume=Decimal("1"),
                        confirmed=True,
                    )
                )
                continue

            candles.append(
                Candle(
                    ts=index,
                    open=Decimal("99"),
                    high=Decimal("102"),
                    low=Decimal("98.9"),
                    close=Decimal("101"),
                    volume=Decimal("1"),
                    confirmed=True,
                )
            )

        event = detect_candle_pattern_signal(
            "BTC-USDT-SWAP",
            candles,
            [item.close for item in candles],
            SignalMonitorConfig(symbols=("BTC-USDT-SWAP",)),
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.direction, "long")
        self.assertEqual(event.signal_type, "candle_pattern")

    def test_signal_flags_only_emit_enabled_types(self) -> None:
        closes = [Decimal("100")] * 55 + [Decimal("105"), Decimal("104"), Decimal("103"), Decimal("80")]
        candles = _make_candles(closes)
        symbol = "BTC-USDT-SWAP"

        breakout_only = evaluate_monitor_signals(
            candles,
            symbol,
            SignalMonitorConfig(
                symbols=(symbol,),
                enable_ema21_55_cross=False,
                enable_ema55_slope_turn=False,
                enable_ema55_breakout=True,
                enable_candle_pattern=False,
            ),
        )
        slope_only = evaluate_monitor_signals(
            candles,
            symbol,
            SignalMonitorConfig(
                symbols=(symbol,),
                enable_ema21_55_cross=False,
                enable_ema55_slope_turn=True,
                enable_ema55_breakout=False,
                enable_candle_pattern=False,
            ),
        )

        self.assertEqual([event.signal_type for event in breakout_only], ["ema55_breakout"])
        self.assertEqual([event.signal_type for event in slope_only], ["ema55_slope_turn"])

    def test_signal_report_tracks_filtered_signal_types(self) -> None:
        closes = [Decimal("100")] * 55 + [Decimal("105"), Decimal("104"), Decimal("103"), Decimal("80")]
        candles = _make_candles(closes)
        symbol = "BTC-USDT-SWAP"

        report = evaluate_monitor_signal_report(
            candles,
            symbol,
            SignalMonitorConfig(
                symbols=(symbol,),
                enable_ema21_55_cross=False,
                enable_ema55_slope_turn=True,
                enable_ema55_breakout=False,
                enable_candle_pattern=False,
            ),
        )

        self.assertEqual([event.signal_type for event in report.matched_events], ["ema55_slope_turn"])
        self.assertEqual([event.signal_type for event in report.filtered_events], ["ema21_55_cross", "ema55_breakout"])

    def test_signal_monitor_email_sender_respects_signal_toggle(self) -> None:
        window = SignalMonitorWindow.__new__(SignalMonitorWindow)
        notifier = _FakeNotifier(signal_notifications_enabled=False)

        sender = window._build_email_sender(notifier, "M01")

        self.assertIsNone(sender)

    def test_signal_monitor_email_sender_formats_subject_and_body(self) -> None:
        window = SignalMonitorWindow.__new__(SignalMonitorWindow)
        notifier = _FakeNotifier(signal_notifications_enabled=True)
        sender = window._build_email_sender(notifier, "M01")

        assert sender is not None
        sender(
            MonitorSignalEvent(
                symbol="BTC-USDT-SWAP",
                signal_type="ema21_55_cross",
                direction="long",
                candle_ts=1,
                trigger_price=Decimal("123.45"),
                reason="测试信号",
                tick_size=Decimal("0.1"),
            ),
            "15m",
        )

        self.assertEqual(len(notifier.sent_messages), 1)
        subject, body = notifier.sent_messages[0]
        self.assertIn("M01", subject)
        self.assertIn("15m", subject)
        self.assertIn("BTC-USDT-SWAP", subject)
        self.assertIn("ema21_55_cross", subject)
        self.assertIn("模块：多币种信号监控", body)
        self.assertIn("方向：long", body)
        self.assertIn("参考价：123.5", body)
        self.assertIn("说明：测试信号", body)

    def test_signal_reason_formats_values_by_tick_size(self) -> None:
        closes = [Decimal("100")] * 55 + [Decimal("105"), Decimal("104"), Decimal("103"), Decimal("80")]
        candles = _make_candles(closes)
        close_values = [item.close for item in candles]

        slope_turn = detect_ema55_slope_turn(
            "BTC-USDT-SWAP",
            candles,
            close_values,
            tick_size=Decimal("0.1"),
        )

        self.assertIsNotNone(slope_turn)
        assert slope_turn is not None
        self.assertIn("前斜率=0.1", slope_turn.reason)
        self.assertIn("当前斜率=-0.7", slope_turn.reason)

    def test_candle_pattern_reason_includes_wick_length_with_tick_size(self) -> None:
        candles: list[Candle] = []
        for index in range(1, 70):
            if index < 69:
                candles.append(
                    Candle(
                        ts=index,
                        open=Decimal("100"),
                        high=Decimal("102"),
                        low=Decimal("98"),
                        close=Decimal("100"),
                        volume=Decimal("1"),
                        confirmed=True,
                    )
                )
                continue

            candles.append(
                Candle(
                    ts=index,
                    open=Decimal("101.2"),
                    high=Decimal("102.76"),
                    low=Decimal("98.95"),
                    close=Decimal("99.1"),
                    volume=Decimal("1"),
                    confirmed=True,
                )
            )

        event = detect_candle_pattern_signal(
            "BTC-USDT-SWAP",
            candles,
            [item.close for item in candles],
            SignalMonitorConfig(symbols=("BTC-USDT-SWAP",)),
            tick_size=Decimal("0.1"),
        )

        self.assertIsNotNone(event)
        assert event is not None
        self.assertIn("上影线=1.6", event.reason)

    def test_format_monitor_diagnostic_round_shows_filtered_and_duplicates(self) -> None:
        text = _format_monitor_diagnostic_round(
            "M01",
            MonitorRoundDiagnostic(
                bar="15m",
                checked_at=1_710_000_000_000,
                reports=(
                    MonitorSymbolDiagnostic(
                        symbol="BTC-USDT-SWAP",
                        candle_ts=1,
                        new_events=(
                            MonitorSignalEvent(
                                symbol="BTC-USDT-SWAP",
                                signal_type="ema21_55_cross",
                                direction="long",
                                candle_ts=1,
                                trigger_price=Decimal("1"),
                                reason="A",
                            ),
                        ),
                        filtered_events=(
                            MonitorSignalEvent(
                                symbol="BTC-USDT-SWAP",
                                signal_type="ema55_breakout",
                                direction="short",
                                candle_ts=1,
                                trigger_price=Decimal("1"),
                                reason="B",
                            ),
                        ),
                        duplicate_events=(
                            MonitorSignalEvent(
                                symbol="BTC-USDT-SWAP",
                                signal_type="candle_pattern",
                                direction="short",
                                candle_ts=1,
                                trigger_price=Decimal("1"),
                                reason="C",
                            ),
                        ),
                    ),
                ),
            ),
        )

        self.assertIn("M01", text)
        self.assertIn("新触发: ema21_55_cross/做多", text)
        self.assertIn("已过滤: ema55_breakout/做空", text)
        self.assertIn("重复抑制: candle_pattern/做空", text)

    def test_bar_interval_seconds_supports_monitor_bars(self) -> None:
        self.assertEqual(_bar_interval_seconds("15m"), 900)
        self.assertEqual(_bar_interval_seconds("1H"), 3600)
        self.assertEqual(_bar_interval_seconds("4H"), 14400)

    def test_seconds_until_next_check_uses_bar_close_and_buffer(self) -> None:
        wait = _seconds_until_next_check("15m", 10, now_ts=1_710_000_001)
        self.assertEqual(wait, 909)
