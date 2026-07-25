# BTC Dynamic Long 1R Slope Exit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Compare the current BTC EMA dynamic-long best configuration with a research-only variant that exits after 1R when EMA55 is flat or falling.

**Architecture:** Add an explicit enable flag and trigger-R field to the generic dynamic-long backtest configuration. In the dynamic-order exit loop, arm the rule after the position reaches 1R, then close the long at the completed candle's close when EMA55[t] <= EMA55[t-1]. A focused runner will build the two configurations from BTC defaults and write auditable reports without changing live/default settings.

**Tech Stack:** Python 3.11, unittest, pandas, existing okx_quant.backtest engine, local candle_cache.

## Global Constraints

- BTC-USDT-SWAP only, using local full 1H confirmed K-line cache.
- Fixed 100U risk per trade, 10,000U initial capital, no compounding.
- Both variants share the data, fees, slippage, entry logic, and existing protection rules.
- Do not modify BTC default parameters, live parameters, or the best-parameter bundle.
- The slope signal uses completed K data and closes at that K's close with the standard Taker exit cost.

---

### Task 1: Add regression coverage and the conditional EMA55 slope exit

**Files:**
- Modify: okx_quant/models.py
- Modify: okx_quant/backtest.py
- Modify: tests/test_backtest.py

**Interfaces:**
- Produces StrategyConfig.ema55_slope_exit_after_trigger_r_enabled: bool = False.
- Produces StrategyConfig.ema55_slope_exit_after_trigger_r: int = 1.
- Produces BacktestTrade.exit_reason == "trend_ema_slope_flat_exit".

- [ ] **Step 1: Write the failing tests**

Add three tests using the existing dynamic-order backtest fixtures, zero slippage, and a deterministic EMA55 sequence:

~~~
def test_dynamic_long_slope_flat_exit_requires_one_r_to_have_been_reached(self) -> None:
    result = run_dynamic_long_with(ema55_flat=True, high_reaches_one_r=False)
    self.assertNotIn("trend_ema_slope_flat_exit", [trade.exit_reason for trade in result.trades])

def test_dynamic_long_slope_flat_exit_keeps_position_while_ema55_rises(self) -> None:
    result = run_dynamic_long_with(ema55_flat=False, high_reaches_one_r=True)
    self.assertNotIn("trend_ema_slope_flat_exit", [trade.exit_reason for trade in result.trades])

def test_dynamic_long_slope_flat_exit_closes_at_current_close_after_one_r(self) -> None:
    result = run_dynamic_long_with(ema55_flat=True, high_reaches_one_r=True)
    trade = next(item for item in result.trades if item.exit_reason == "trend_ema_slope_flat_exit")
    self.assertEqual(trade.exit_price, Decimal("101"))
~~~

The fixture must have initial risk of one price unit, cross 1R in a preceding candle, and close the exit candle at 101.

- [ ] **Step 2: Run the test to verify the RED state**

Run: python -m pytest tests/test_backtest.py -k "dynamic_long_slope_flat_exit" -v

Expected: FAIL because the two configuration fields and slope-flat exit reason do not exist.

- [ ] **Step 3: Implement the minimum behavior**

Add the two fields and propagate them to BacktestResult. In the generic dynamic-order exit loop, after existing stop/protection checks, calculate current and preceding trend EMA and add:

~~~
elif (
    config.ema55_slope_exit_after_trigger_r_enabled
    and open_position.signal == "long"
    and trend_ema is not None
    and previous_trend_ema is not None
    and trend_ema <= previous_trend_ema
    and _dynamic_trigger_r_reached(
        open_position,
        config.ema55_slope_exit_after_trigger_r,
    )
):
    # close at candle.close using the existing Taker/slippage helper
~~~

Use only the exit reason trend_ema_slope_flat_exit. Do not edit the parameter defaults.

- [ ] **Step 4: Run the test to verify the GREEN state**

Run: python -m pytest tests/test_backtest.py -k "dynamic_long_slope_flat_exit" -v

Expected: all three tests PASS.

- [ ] **Step 5: Run relevant regressions**

Run: python -m pytest tests/test_backtest.py tests/test_dynamic_strategy.py -q

Expected: exit code 0.

- [ ] **Step 6: Commit the tested engine change**

~~~
git add okx_quant/models.py okx_quant/backtest.py tests/test_backtest.py
git commit -m "feat: add conditional EMA slope exit for dynamic longs"
~~~

### Task 2: Create a BTC full-history comparison runner

**Files:**
- Create: scripts/run_btc_dynamic_long_one_r_slope_exit_compare.py
- Create: tests/test_btc_dynamic_long_one_r_slope_exit_compare.py
- Create: reports/btc_dynamic_long_one_r_slope_exit/compare.csv
- Create: reports/btc_dynamic_long_one_r_slope_exit/trades_baseline.csv
- Create: reports/btc_dynamic_long_one_r_slope_exit/trades_one_r_slope_exit.csv
- Create: reports/btc_dynamic_long_one_r_slope_exit/summary.json
- Create: reports/btc_dynamic_long_one_r_slope_exit/report.html

**Interfaces:**
- Consumes get_strategy_symbol_parameter_defaults, load_candle_cache, and _run_backtest_with_loaded_data.
- Produces build_configs() -> tuple[StrategyConfig, StrategyConfig].
- Produces two BacktestResult values and the listed report artifacts.

- [ ] **Step 1: Write the failing runner-config test**

~~~
def test_build_configs_only_changes_the_research_exit_rule() -> None:
    baseline, variant = build_configs()
    self.assertFalse(baseline.ema55_slope_exit_after_trigger_r_enabled)
    self.assertTrue(variant.ema55_slope_exit_after_trigger_r_enabled)
    self.assertEqual(variant.ema55_slope_exit_after_trigger_r, 1)
    self.assertEqual(baseline.risk_amount, Decimal("100"))
    self.assertEqual(variant.risk_amount, Decimal("100"))
~~~

Also assert every other field in asdict(baseline) equals asdict(variant).

- [ ] **Step 2: Run the test to verify the RED state**

Run: python -m pytest tests/test_btc_dynamic_long_one_r_slope_exit_compare.py -v

Expected: FAIL because the runner and build_configs function do not exist.

- [ ] **Step 3: Implement the narrow runner**

Build baseline from current BTC dynamic-long backtest defaults. Set initial capital to 10000, fixed-risk sizing, risk_amount to 100, and non-compounding. Create the variant only by setting the two new research fields to True and 1. Load confirmed 1H candles from local cache, sort ascending, and run both with identical Maker/Taker fees from the standard report.

Write metrics, data range, candle count, yearly stats, exit-reason counts, and every trade to CSV/JSON. Add new_rule_exit to trade output when exit_reason equals trend_ema_slope_flat_exit. Build an HTML table that names the exact new rule and says it does not change live/default profiles.

- [ ] **Step 4: Run the runner-config test to verify the GREEN state**

Run: python -m pytest tests/test_btc_dynamic_long_one_r_slope_exit_compare.py -v

Expected: PASS.

- [ ] **Step 5: Run the full local-history comparison**

Run: python scripts/run_btc_dynamic_long_one_r_slope_exit_compare.py

Expected: prints the report directory and creates all five report artifacts.

- [ ] **Step 6: Verify data parity**

Run:

~~~
python -c "import json; p=json.load(open('reports/btc_dynamic_long_one_r_slope_exit/summary.json', encoding='utf-8')); assert p['baseline']['range'] == p['one_r_slope_exit']['range']; assert p['baseline']['risk_amount'] == p['one_r_slope_exit']['risk_amount'] == '100'"
~~~

Expected: exit code 0.

- [ ] **Step 7: Commit runner, tests, and artifacts**

~~~
git add scripts/run_btc_dynamic_long_one_r_slope_exit_compare.py tests/test_btc_dynamic_long_one_r_slope_exit_compare.py reports/btc_dynamic_long_one_r_slope_exit
git commit -m "research: compare BTC one-R EMA slope exits"
~~~

### Task 3: Final verification and handoff

**Files:**
- Verify: okx_quant/backtest.py
- Verify: tests/test_backtest.py
- Verify: tests/test_dynamic_strategy.py
- Verify: tests/test_btc_dynamic_long_one_r_slope_exit_compare.py
- Verify: reports/btc_dynamic_long_one_r_slope_exit/report.html

**Interfaces:**
- Consumes the completed Tasks 1 and 2.
- Produces verified report links and a concise outcome summary.

- [ ] **Step 1: Run focused tests and full comparison**

~~~
python -m pytest tests/test_backtest.py tests/test_dynamic_strategy.py tests/test_btc_dynamic_long_one_r_slope_exit_compare.py -q
python scripts/run_btc_dynamic_long_one_r_slope_exit_compare.py
~~~

Expected: all tests pass and report generation exits 0.

- [ ] **Step 2: Inspect metrics and the audit field**

~~~
Get-Content reports/btc_dynamic_long_one_r_slope_exit/compare.csv
python -c "import pandas as pd; d=pd.read_csv('reports/btc_dynamic_long_one_r_slope_exit/trades_one_r_slope_exit.csv'); assert 'new_rule_exit' in d; print(d['new_rule_exit'].sum())"
~~~

Expected: two variant rows and an auditable new_rule_exit field.

- [ ] **Step 3: Confirm scope**

Run: git status --short; git log --oneline -3

Expected: only the two intended engine/research commits and no default-profile changes.

