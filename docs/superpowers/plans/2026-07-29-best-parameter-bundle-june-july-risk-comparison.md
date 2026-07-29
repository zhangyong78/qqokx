# 最佳参数组合 6-7 月风险金对比 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 生成 4 币种、8 策略在 2026 年 6 月与 7 月的默认风险金和统一 10U 风险金回测与差异报告。

**Architecture:** 读取现有最佳参数组合与本地 1H K 线，对每种风险方案独立运行策略并按平仓的 Asia/Shanghai 自然月筛选。汇总模块按组合、策略和币种生成同口径的指标及方案差异。

**Tech Stack:** Python 3、pytest、pandas、现有 `okx_quant` 回测引擎。

## Global Constraints

- 仅使用 BTC、ETH、SOL、DOGE 的 8 个现有最佳参数策略。
- 月份按平仓时间的 Asia/Shanghai 自然月归属。
- 固定风险、非复利；不启用组合资金与仓位/敞口约束。
- 默认风险金为 BTC 多 20U、ETH 多 12U、SOL 多 4U、DOGE 多 4U；BTC 空 10U、ETH 空 8U、SOL 空 6U、DOGE 空 6U。
- 对照组为所有策略固定 10U/笔。

---

### Task 1: 可配置风险金的候选交易生成

**Files:**
- Modify: `scripts/run_best_parameter_bundle_1h_standard_portfolio.py`
- Test: `tests/test_best_parameter_bundle_period_tables.py`

**Interfaces:**
- Consumes: `StrategyBundle.profiles` 与每个 profile 的 `config_snapshot["risk_amount"]`。
- Produces: `build_candidate_trades(..., risk_amount_resolver=...)`，将每个 profile 的固定风险金传入策略回测。

- [ ] **Step 1: Write the failing test**

```python
def test_build_candidate_trades_resolves_profile_risk_amount():
    assert resolve_profile_risk_amount({"risk_amount": "12"}) == Decimal("12")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_best_parameter_bundle_period_tables.py -q`

Expected: FAIL because `resolve_profile_risk_amount` is not defined.

- [ ] **Step 3: Write minimal implementation**

```python
def resolve_profile_risk_amount(config_snapshot: dict[str, object], fallback: Decimal) -> Decimal:
    value = config_snapshot.get("risk_amount")
    return fallback if value in (None, "") else Decimal(str(value))
```

Pass the resolved value into the copied `StrategyConfig` for each profile.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_best_parameter_bundle_period_tables.py -q`

Expected: PASS.

### Task 2: 双风险方案的月度回测与导出

**Files:**
- Create: `scripts/run_best_parameter_bundle_june_july_risk_compare.py`
- Test: `tests/test_best_parameter_bundle_period_tables.py`

**Interfaces:**
- Consumes: candidate trades and risk mapping from Task 1.
- Produces: `reports/best_parameter_bundle_june_july_risk_compare/` 下的明细 CSV、月度汇总 CSV、差异 CSV 与报告。

- [ ] **Step 1: Write the failing test**

```python
def test_month_filter_uses_shanghai_exit_month():
    assert exit_month_in_shanghai(utc_timestamp) == "2026-06"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_best_parameter_bundle_period_tables.py -q`

Expected: FAIL because `exit_month_in_shanghai` is not defined.

- [ ] **Step 3: Write minimal implementation**

```python
def exit_month_in_shanghai(exit_ts: int) -> str:
    return pd.to_datetime(exit_ts, unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m")
```

Run each risk scheme independently, retain only `2026-06` and `2026-07`, and aggregate PnL, trade count, win rate, fees, slippage, and max drawdown.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_best_parameter_bundle_period_tables.py -q`

Expected: PASS.

### Task 3: 执行与核验

**Files:**
- Create: `reports/best_parameter_bundle_june_july_risk_compare/*.csv`
- Create: `reports/best_parameter_bundle_june_july_risk_compare/report.md`

**Interfaces:**
- Consumes: Task 2 脚本。
- Produces: 用户可查看的两方案回测结果与差异结论。

- [ ] **Step 1: Run the report generator**

Run: `.\\.venv\\Scripts\\python.exe scripts\\run_best_parameter_bundle_june_july_risk_compare.py`

- [ ] **Step 2: Verify exported consistency**

Run: `.\\.venv\\Scripts\\python.exe -m pytest tests\\test_best_parameter_bundle_period_tables.py -q`

Expected: PASS, and each scheme-month's strategy rows sum to its combination row.

- [ ] **Step 3: Inspect data completeness**

Run: `.\\.venv\\Scripts\\python.exe -c "..."`

Expected: outputs record the last confirmed 1H candle time for every coin.
