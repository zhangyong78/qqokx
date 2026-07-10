# EMA Dynamic Reentry Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 保留同波多次开仓能力，同时确保每次平仓后只使用平仓后新确认的 K 线再次开仓，并禁止附带止损失败后提交裸单。

**Architecture:** 在动态委托主循环中增加“平仓后最新已确认 K 线基线”刷新，把持仓期间积压的 K 线统一标记为已处理；`entries_in_current_wave` 和波次状态保持不变。动态限价提交函数把 `51051` 转换为可跳过的 `None` 结果，两个调用循环标记当前 K 线已处理并继续等待。会话追踪器统一识别“已提交”和“已补挂”两类初始止损日志。

**Tech Stack:** Python 3.11+、`unittest`、`pytest`、`Decimal`、现有 OKX 客户端抽象。

## Global Constraints

- 同一波继续受 `max_entries_per_trend` 控制，不得退化为每波只能开一次。
- 平仓后不使用持仓期间已经确认的 K 线；只允许平仓后新确认的下一根 K 线触发下一次委托。
- 不改变 EMA、ATR、结构止损、风险金和数量计算公式。
- OKX 拒绝附带止损时放弃本次开仓，不允许裸限价单降级。
- 不增加配置项、固定冷却时间或无关重构。
- 只修改 `okx_quant/engine.py`、`okx_quant/ui_strategy_sessions.py` 和对应测试。

## File Map

- `okx_quant/engine.py`：刷新平仓后的 K 线等待基线；将 `51051` 处理为跳过当前候选。
- `tests/test_strategy_engine.py`：覆盖持仓期间跨 K、平仓后新 K 再入场、多次开仓计数及 `51051` 无裸单。
- `okx_quant/ui_strategy_sessions.py`：让补挂止损进入运行态保护单追踪。
- `tests/test_ui.py`：覆盖补挂止损的 `algoClOrdId` 与止损价解析。

---

### Task 1: 平仓后等待真正的新 K 线

**Files:**
- Modify: `tests/test_strategy_engine.py`，在动态交易所策略测试区增加可复用探针和两个回归用例。
- Modify: `okx_quant/engine.py:890-1019`，在四个持仓结束分支刷新 K 线基线。
- Modify: `okx_quant/engine.py`，在动态策略辅助方法区增加 `_refresh_dynamic_entry_candle_baseline`。

**Interfaces:**
- Consumes: `StrategyEngine._get_candles_with_retry(inst_id, bar, limit)`、`StrategyConfig`、当前 `lookback` 和旧 `newest_ts`。
- Produces: `StrategyEngine._refresh_dynamic_entry_candle_baseline(config, *, lookback, fallback_ts) -> int`。
- Invariant: 只更新 `idle_signal_candle_ts` 与 `last_candle_ts`，不修改 `current_wave_signal`、`current_wave_index`、`entries_in_current_wave`。

- [ ] **Step 1: 增加循环测试探针**

在 `StrategyEngineTest` 中加入以下测试辅助方法；它运行真实 `_run_dynamic_exchange_strategy` 循环，只替换 OKX I/O：

```python
    def _run_dynamic_exchange_reentry_probe(
        self,
        *,
        candle_counts: list[int],
        stop_after_waits: int,
        max_entries_per_trend: int = 3,
        rejected_candle_ts: set[int] | None = None,
        no_signal_candle_ts: set[int] | None = None,
    ) -> tuple[list[int], list[int], int, list[float], list[str]]:
        messages: list[str] = []
        waits: list[float] = []
        attempted_candles: list[int] = []
        accepted_candles: list[int] = []
        evaluate_calls = 0
        candle_read_index = 0
        rejected = rejected_candle_ts or set()
        no_signal = no_signal_candle_ts or set()

        class _StopStub:
            def __init__(self) -> None:
                self._stopped = False

            def is_set(self) -> bool:
                return self._stopped

            def wait(self, timeout: float) -> bool:
                waits.append(timeout)
                if len(waits) >= stop_after_waits:
                    self._stopped = True
                return self._stopped

        engine = StrategyEngine(
            None,  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托-多头",
            session_id="S-reentry-probe",
        )
        engine._stop_event = _StopStub()  # type: ignore[assignment]
        engine._log_strategy_start = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._log_hourly_debug = lambda *args, **kwargs: None  # type: ignore[assignment]
        engine._get_trigger_price_with_retry = lambda *args, **kwargs: Decimal("2305")  # type: ignore[assignment]
        engine._manage_filled_dynamic_entry = lambda *args, **kwargs: None  # type: ignore[assignment]

        def _candles(*_args, **_kwargs):  # noqa: ANN001
            nonlocal candle_read_index
            index = min(candle_read_index, len(candle_counts) - 1)
            candle_read_index += 1
            return self._make_candles([str(2000 + item) for item in range(candle_counts[index])])

        def _decision(confirmed, *_args, **_kwargs):  # noqa: ANN001
            nonlocal evaluate_calls
            evaluate_calls += 1
            if confirmed[-1].ts in no_signal:
                return SignalDecision(
                    signal=None,
                    reason="趋势失效",
                    candle_ts=confirmed[-1].ts,
                    entry_reference=None,
                    atr_value=Decimal("10"),
                    ema_value=Decimal("2290"),
                )
            return SignalDecision(
                signal="long",
                reason="趋势成立",
                candle_ts=confirmed[-1].ts,
                entry_reference=Decimal("2300"),
                atr_value=Decimal("10"),
                ema_value=Decimal("2310"),
            )

        def _submit(*_args, plan: OrderPlan, **_kwargs):  # noqa: ANN001
            attempted_candles.append(plan.candle_ts)
            if plan.candle_ts in rejected:
                return None
            accepted_candles.append(plan.candle_ts)
            suffix = len(accepted_candles)
            return (
                f"cl-{suffix}",
                OkxOrderResult(
                    ord_id=f"ord-{suffix}",
                    cl_ord_id=f"cl-{suffix}",
                    s_code="0",
                    s_msg="accepted",
                    raw={},
                ),
                f"slg-{suffix}",
            )

        engine._get_candles_with_retry = _candles  # type: ignore[assignment]
        engine._evaluate_dynamic_signal_decision = _decision  # type: ignore[assignment]
        engine._submit_dynamic_limit_entry_order = _submit  # type: ignore[assignment]
        engine._get_order_with_retry = lambda *args, **kwargs: OkxOrderStatus(  # type: ignore[assignment]
            ord_id="ord-filled",
            state="filled",
            side="buy",
            ord_type="limit",
            price=Decimal("2300"),
            avg_price=Decimal("2299"),
            size=Decimal("0.01"),
            filled_size=Decimal("0.01"),
            raw={},
        )

        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            risk_amount=Decimal("10"),
            max_entries_per_trend=max_entries_per_trend,
            take_profit_mode="dynamic",
        )
        instrument = Instrument(
            inst_id="ETH-USDT-SWAP",
            inst_type="SWAP",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.01"),
            min_size=Decimal("0.01"),
            state="live",
        )

        with patch("okx_quant.engine.time.time", return_value=0.0):
            engine._run_dynamic_exchange_strategy(None, config, instrument)  # type: ignore[arg-type]

        return attempted_candles, accepted_candles, evaluate_calls, waits, messages
```

- [ ] **Step 2: 写入五个回归测试**

```python
    def test_dynamic_exchange_strategy_skips_candles_confirmed_while_position_was_open(self) -> None:
        attempted, accepted, evaluate_calls, waits, _messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 82, 82],
            stop_after_waits=2,
        )

        self.assertEqual(attempted, [80])
        self.assertEqual(accepted, [80])
        self.assertEqual(evaluate_calls, 1)
        self.assertEqual(waits, [10, 60.0])

    def test_dynamic_exchange_strategy_reenters_on_first_candle_confirmed_after_close(self) -> None:
        attempted, accepted, evaluate_calls, waits, messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 82, 82, 83],
            stop_after_waits=3,
        )

        self.assertEqual(attempted, [80, 83])
        self.assertEqual(accepted, [80, 83])
        self.assertEqual(evaluate_calls, 2)
        self.assertEqual(waits, [10, 60.0, 10])
        self.assertTrue(any("本波第2次委托" in message for message in messages))

    def test_dynamic_exchange_strategy_allows_third_entry_after_another_post_close_candle(self) -> None:
        attempted, accepted, evaluate_calls, waits, messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 82, 82, 83, 83, 84, 84, 85],
            stop_after_waits=5,
            max_entries_per_trend=3,
        )

        self.assertEqual(attempted, [80, 83, 85])
        self.assertEqual(accepted, [80, 83, 85])
        self.assertEqual(evaluate_calls, 3)
        self.assertEqual(waits, [10, 60.0, 10, 60.0, 10])
        self.assertTrue(any("本波第3次委托" in message for message in messages))

    def test_dynamic_exchange_strategy_stops_reentry_at_configured_wave_limit(self) -> None:
        attempted, accepted, evaluate_calls, waits, messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 82, 82, 83, 83, 84, 84, 85],
            stop_after_waits=5,
            max_entries_per_trend=2,
        )

        self.assertEqual(attempted, [80, 83])
        self.assertEqual(accepted, [80, 83])
        self.assertEqual(evaluate_calls, 3)
        self.assertEqual(waits, [10, 60.0, 10, 60.0, 60.0])
        self.assertTrue(any("开仓次数已达上限" in message for message in messages))

    def test_dynamic_exchange_strategy_resets_wave_after_trend_invalidates(self) -> None:
        attempted, accepted, evaluate_calls, waits, messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 82, 82, 83, 84],
            stop_after_waits=4,
            max_entries_per_trend=3,
            no_signal_candle_ts={83},
        )

        self.assertEqual(attempted, [80, 84])
        self.assertEqual(accepted, [80, 84])
        self.assertEqual(evaluate_calls, 3)
        self.assertEqual(waits, [10, 60.0, 60.0, 10])
        self.assertTrue(any("第2波趋势开始" in message for message in messages))
        self.assertTrue(any("第2波 | 本波第1次委托" in message for message in messages))
```

- [ ] **Step 3: 运行测试确认 RED**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_skips_candles_confirmed_while_position_was_open `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_reenters_on_first_candle_confirmed_after_close `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_allows_third_entry_after_another_post_close_candle `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_stops_reentry_at_configured_wave_limit `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_resets_wave_after_trend_invalidates
```

Expected: 前四个测试至少一个失败；当前实现会在 `82` 号 K 线上立即提交第 2 次委托。趋势重置测试用于锁定既有行为。

- [ ] **Step 4: 实现平仓后 K 线基线刷新**

在 `StrategyEngine` 中增加：

```python
    def _refresh_dynamic_entry_candle_baseline(
        self,
        config: StrategyConfig,
        *,
        lookback: int,
        fallback_ts: int,
    ) -> int:
        candles = self._get_candles_with_retry(config.inst_id, config.bar, limit=lookback)
        confirmed = [candle for candle in candles if candle.confirmed]
        return confirmed[-1].ts if confirmed else fallback_ts
```

在 `_run_dynamic_exchange_strategy` 的四个持仓结束分支中，把原来的：

```python
                    idle_signal_candle_ts = newest_ts
```

替换为：

```python
                    idle_signal_candle_ts = self._refresh_dynamic_entry_candle_baseline(
                        config,
                        lookback=lookback,
                        fallback_ts=newest_ts,
                    )
                    last_candle_ts = idle_signal_candle_ts
```

四个分支分别是：完整成交、部分成交接管完成、撤单回查发现完整成交、撤单回查发现部分成交并完成接管。

- [ ] **Step 5: 运行 Task 1 测试确认 GREEN**

Run: 使用 Step 3 相同命令。

Expected: `5 passed`，且无 warning。

- [ ] **Step 6: 运行已有同波次数测试**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_waits_next_candle_before_second_entry_in_same_wave
```

Expected: `1 passed`。

- [ ] **Step 7: 提交 Task 1**

```powershell
git add -- okx_quant/engine.py tests/test_strategy_engine.py
git commit -m "fix: wait for post-close candle before reentry"
```

---

### Task 2: 附带止损拒绝时放弃当前开仓

**Files:**
- Modify: `tests/test_strategy_engine.py:2988-3080`，把裸单降级测试改成拒单放弃测试。
- Modify: `tests/test_strategy_engine.py`，使用 Task 1 探针覆盖调用循环继续等待下一根 K。
- Modify: `okx_quant/engine.py:1123-1157` 和 `okx_quant/engine.py:1434-1460`，处理可跳过提交结果。
- Modify: `okx_quant/engine.py:5290-5353`，删除裸单降级请求。

**Interfaces:**
- Consumes: `_submit_dynamic_limit_entry_order(...)` 的现有参数。
- Produces: 返回类型 `tuple[str, OkxOrderResult, str | None] | None`；`None` 表示保护单被拒，本次候选已安全放弃。
- Caller contract: 收到 `None` 时把当前 K 线设为 idle，等待下一根 K，不创建 `ManagedEntryOrder`。

- [ ] **Step 1: 将现有 51051 测试改为失败测试**

把 `test_dynamic_limit_entry_retries_without_attached_stop_after_51051_reject` 整体替换为：

```python
    def test_dynamic_limit_entry_abandons_order_after_51051_reject(self) -> None:
        messages: list[str] = []
        place_limit_calls: list[dict[str, object]] = []
        next_ids = iter(("entry-1", "slg-1"))

        class _StubClient:
            @staticmethod
            def place_limit_order(
                credentials,
                config,
                plan,
                *,
                cl_ord_id=None,
                include_take_profit=True,
                stop_loss_algo_cl_ord_id=None,
                include_attached_protection=True,
            ):  # noqa: ANN001
                place_limit_calls.append(
                    {
                        "cl_ord_id": cl_ord_id,
                        "include_take_profit": include_take_profit,
                        "stop_loss_algo_cl_ord_id": stop_loss_algo_cl_ord_id,
                        "include_attached_protection": include_attached_protection,
                    }
                )
                raise OkxApiError(
                    "操作全部失败 | 止损价格应低于开仓价格 | sCode=51051",
                    code="1",
                )

        engine = StrategyEngine(
            _StubClient(),  # type: ignore[arg-type]
            messages.append,
            strategy_name="EMA 动态委托多头",
            session_id="S01",
        )
        engine._next_client_order_id = lambda *, role: next(next_ids)  # type: ignore[assignment]
        config = StrategyConfig(
            inst_id="ETH-USDT-SWAP",
            bar="1m",
            ema_period=21,
            trend_ema_period=55,
            big_ema_period=233,
            atr_period=10,
            atr_stop_multiplier=Decimal("2"),
            atr_take_multiplier=Decimal("4"),
            order_size=Decimal("0.01"),
            trade_mode="cross",
            signal_mode="long_only",
            position_mode="long_short",
            environment="demo",
            tp_sl_trigger_type="last",
            strategy_id=STRATEGY_DYNAMIC_LONG_ID,
            poll_seconds=10,
            take_profit_mode="dynamic",
        )
        plan = OrderPlan(
            inst_id="ETH-USDT-SWAP",
            side="buy",
            pos_side="long",
            size=Decimal("0.01"),
            take_profit=Decimal("2340"),
            stop_loss=Decimal("2280"),
            entry_reference=Decimal("2300"),
            atr_value=Decimal("10"),
            signal="long",
            candle_ts=1,
            tp_sl_inst_id="ETH-USDT-SWAP",
            tp_sl_mode="exchange",
        )

        result = engine._submit_dynamic_limit_entry_order(
            None,  # type: ignore[arg-type]
            config,
            plan=plan,
            dynamic_stop_only=True,
            trader_virtual_stop_loss_enabled=False,
        )

        self.assertIsNone(result)
        self.assertEqual(len(place_limit_calls), 1)
        self.assertTrue(place_limit_calls[0]["include_attached_protection"])
        self.assertEqual(place_limit_calls[0]["stop_loss_algo_cl_ord_id"], "slg-1")
        self.assertTrue(any("附带止损被拒" in message and "本次开仓已放弃" in message for message in messages))
```

- [ ] **Step 2: 增加主循环继续等待测试**

```python
    def test_dynamic_exchange_strategy_waits_next_candle_after_protected_entry_is_rejected(self) -> None:
        attempted, accepted, evaluate_calls, waits, _messages = self._run_dynamic_exchange_reentry_probe(
            candle_counts=[80, 80, 81],
            stop_after_waits=3,
            rejected_candle_ts={80},
        )

        self.assertEqual(attempted, [80, 81])
        self.assertEqual(accepted, [81])
        self.assertEqual(evaluate_calls, 2)
        self.assertEqual(waits, [60.0, 60.0, 10])
```

- [ ] **Step 3: 运行测试确认 RED**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_limit_entry_abandons_order_after_51051_reject `
  tests/test_strategy_engine.py::StrategyEngineTest::test_dynamic_exchange_strategy_waits_next_candle_after_protected_entry_is_rejected
```

Expected: 旧实现会进行第二次裸单请求，或者调用循环尝试解包 `None`，测试失败。

- [ ] **Step 4: 修改提交函数为安全放弃**

将签名改为：

```python
    def _submit_dynamic_limit_entry_order(
        self,
        credentials: Credentials,
        config: StrategyConfig,
        *,
        plan: OrderPlan,
        dynamic_stop_only: bool,
        trader_virtual_stop_loss_enabled: bool,
    ) -> tuple[str, OkxOrderResult, str | None] | None:
```

保留首次带保护提交。捕获匹配的 `51051` 后删除第二次 `_submit_limit_order`，改为：

```python
        self._logger(
            " | ".join(
                [
                    f"{_fmt_ts(plan.candle_ts)} | OKX 附带止损被拒，本次开仓已放弃",
                    f"标的={plan.inst_id}",
                    f"方向={plan.signal.upper()}",
                    f"计划开仓价={format_decimal(plan.entry_reference)}",
                    f"止损={format_decimal(plan.stop_loss)}",
                    f"clOrdId={cl_ord_id}",
                    detail,
                ]
            )
        )
        return None
```

- [ ] **Step 5: 修改两个调用循环处理 `None`**

在 `_run_dynamic_exchange_strategy` 中：

```python
            submission = self._submit_dynamic_limit_entry_order(
                credentials,
                config,
                plan=plan,
                dynamic_stop_only=dynamic_stop_only,
                trader_virtual_stop_loss_enabled=trader_virtual_stop_loss_enabled,
            )
            if submission is None:
                idle_signal_candle_ts = newest_ts
                last_candle_ts = newest_ts
                self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                continue
            cl_ord_id, result, stop_loss_algo_cl_ord_id = submission
```

在 `_resume_dynamic_exchange_pending_order_loop` 的新挂单分支中：

```python
                submission = self._submit_dynamic_limit_entry_order(
                    credentials,
                    config,
                    plan=plan,
                    dynamic_stop_only=dynamic_stop_only,
                    trader_virtual_stop_loss_enabled=bool(config.trader_virtual_stop_loss),
                )
                if submission is None:
                    idle_signal_candle_ts = newest_candle_ts
                    last_candle_ts = newest_candle_ts
                    self._stop_event.wait(_idle_signal_wait_seconds(config.bar, config.poll_seconds))
                    continue
                cl_ord_id, result, stop_loss_algo_cl_ord_id = submission
```

- [ ] **Step 6: 运行 Task 2 测试确认 GREEN**

Run: 使用 Step 3 相同命令。

Expected: `2 passed`，只发生一次带保护下单调用。

- [ ] **Step 7: 提交 Task 2**

```powershell
git add -- okx_quant/engine.py tests/test_strategy_engine.py
git commit -m "fix: abandon unprotected dynamic entries"
```

---

### Task 3: 追踪成交后补挂的止损

**Files:**
- Modify: `tests/test_ui.py:3604-3642` 附近，增加补挂止损追踪测试。
- Modify: `okx_quant/ui_strategy_sessions.py:6220`、`:8206`、`:9650`，统一匹配两种初始止损日志。

**Interfaces:**
- Consumes: 策略日志字符串。
- Produces: `StrategyTradeRuntimeState.protective_algo_cl_ord_id`、`initial_stop_price`、`current_stop_price`。

- [ ] **Step 1: 写入失败测试**

```python
    def test_track_session_trade_runtime_captures_backfilled_exchange_stop(self) -> None:
        session = self._make_session()
        app = self._make_app_for_tracking()

        QuantApp._track_session_trade_runtime(
            app,
            session,
            "2026-07-07 20:00:00 | 挂单已成交 | ordId=2001 | 开仓价=80.65 | 数量=5.88",
        )
        QuantApp._track_session_trade_runtime(
            app,
            session,
            "初始 OKX 止损已补挂 | algoClOrdId=s211emaslg070713373471228 | "
            "algoId=3722161598581456896 | 止损=80.65 | 启动动态上移监控",
        )

        self.assertIsNotNone(session.active_trade)
        self.assertEqual(
            session.active_trade.protective_algo_cl_ord_id,
            "s211emaslg070713373471228",
        )
        self.assertEqual(session.active_trade.initial_stop_price, Decimal("80.65"))
        self.assertEqual(session.active_trade.current_stop_price, Decimal("80.65"))
```

- [ ] **Step 2: 运行测试确认 RED**

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider `
  tests/test_ui.py::StrategyTradeTrackingTest::test_track_session_trade_runtime_captures_backfilled_exchange_stop
```

Expected: FAIL，`protective_algo_cl_ord_id` 仍为空。

- [ ] **Step 3: 扩展三处日志匹配条件**

把三处：

```python
        if "初始 OKX 止损已提交" in message:
```

替换为：

```python
        if "初始 OKX 止损已提交" in message or "初始 OKX 止损已补挂" in message:
```

保持后续 `algoClOrdId`、止损价和管理模式赋值不变。

- [ ] **Step 4: 运行 Task 3 测试确认 GREEN**

Run: 使用 Step 2 中确认后的精确节点 ID。

Expected: `1 passed`。

- [ ] **Step 5: 提交 Task 3**

```powershell
git add -- okx_quant/ui_strategy_sessions.py tests/test_ui.py
git commit -m "fix: track backfilled dynamic stops"
```

---

### Task 4: 完整验证与交付检查

**Files:**
- Verify only: `okx_quant/engine.py`
- Verify only: `okx_quant/ui_strategy_sessions.py`
- Verify only: `tests/test_strategy_engine.py`
- Verify only: `tests/test_ui.py`

**Interfaces:**
- Consumes: Tasks 1-3 的实现和测试。
- Produces: 可交付的测试证据与干净的相关文件差异。

- [ ] **Step 1: 运行策略引擎测试文件**

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider tests/test_strategy_engine.py
```

Expected: 全部通过，0 failed，0 warnings。

- [ ] **Step 2: 运行 UI 测试文件**

```powershell
.\.venv\Scripts\python.exe -m pytest -q -c NUL -p no:cacheprovider tests/test_ui.py
```

Expected: 全部通过，0 failed，0 warnings。

- [ ] **Step 3: 运行语法编译检查**

```powershell
.\.venv\Scripts\python.exe -m py_compile okx_quant\engine.py okx_quant\ui_strategy_sessions.py tests\test_strategy_engine.py tests\test_ui.py
```

Expected: exit code 0，无输出。

- [ ] **Step 4: 检查差异和工作区**

```powershell
git diff --check HEAD~3..HEAD
git status --short
git log -4 --oneline
```

Expected: `git diff --check` 无输出；状态中没有遗漏的本次相关修改；最近三个实现提交分别对应 Tasks 1-3。

- [ ] **Step 5: 对照设计自查**

确认以下事实均有测试证据：

- 持仓期间跨过的 K 线不触发立即再入场。
- 平仓后第一根新 K 可触发本波下一次委托。
- 波次开仓次数持续累加并受上限约束。
- `51051` 后只有一次带保护请求，线程继续等待。
- 补挂止损能进入会话保护单追踪。
