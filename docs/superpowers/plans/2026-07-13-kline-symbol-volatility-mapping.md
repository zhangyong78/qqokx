# K线交易对与波动率映射 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 K线分析页改为五个固定交易对下拉框，并让波动率副图按 BTC/ETH 映射加载，SOL/DOGE 不显示波动率。

**Architecture:** 在 `kline_analysis_window.py` 维护交易对与波动率币种的单一配置。窗口通过 `_selected_symbol()` 和 `_volatility_currency_for_symbol()` 获取当前选择；`SecondaryVolatilityDataLoader` 接收币种，缓存、请求和文案均使用该参数。

**Tech Stack:** Python 3、PySide6、OKX REST K线、Deribit 波动率 REST、unittest。

## Global Constraints

- 下拉顺序固定为 `BTC-USDT-SWAP`、`ETH-USDT-SWAP`、`SOL-USDT-SWAP`、`DOGE-USDT-SWAP`、`ETH-BTC`。
- BTC 永续映射 BTC DVOL；ETH 永续和 ETH-BTC 映射 ETH DVOL；SOL/DOGE 不接入新数据源。
- 仅改 `roll_terminal_qt/kline_analysis_window.py` 与相关测试。
- 保持双图、分屏、主均副普、平均K线和账户抽屉现有行为。

---

### Task 1: 交易对配置与动态波动率加载器

**Files:**

- Modify: `D:/qqokx/roll_terminal_qt/kline_analysis_window.py:66-70,4492-4686`
- Test: `D:/qqokx/tests/test_roll_terminal_qt_windows.py`

**Interfaces:**

- Produces: `KLINE_SYMBOL_OPTIONS: tuple[str, ...]` and `_volatility_currency_for_symbol(symbol: str) -> str | None`.
- Produces: `SecondaryVolatilityDataLoader(..., currency: str, ...)`, where currency is BTC or ETH.

- [ ] **Step 1: Write the failing configuration test**

~~~python
from roll_terminal_qt.kline_analysis_window import KLINE_SYMBOL_OPTIONS, _volatility_currency_for_symbol

def test_kline_symbol_options_and_volatility_currency_mapping(self) -> None:
    self.assertEqual(KLINE_SYMBOL_OPTIONS, (
        "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "ETH-BTC",
    ))
    self.assertEqual(_volatility_currency_for_symbol("BTC-USDT-SWAP"), "BTC")
    self.assertEqual(_volatility_currency_for_symbol("ETH-USDT-SWAP"), "ETH")
    self.assertEqual(_volatility_currency_for_symbol("ETH-BTC"), "ETH")
    self.assertIsNone(_volatility_currency_for_symbol("SOL-USDT-SWAP"))
    self.assertIsNone(_volatility_currency_for_symbol("DOGE-USDT-SWAP"))
~~~

- [ ] **Step 2: Verify the test fails**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_kline_symbol_options_and_volatility_currency_mapping`

Expected: FAIL because the configuration symbols are not yet importable.

- [ ] **Step 3: Implement mapping and pass currency into the loader**

~~~python
KLINE_SYMBOL_OPTIONS = (
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "DOGE-USDT-SWAP", "ETH-BTC",
)
_VOLATILITY_CURRENCY_BY_SYMBOL = {
    "BTC-USDT-SWAP": "BTC", "ETH-USDT-SWAP": "ETH", "ETH-BTC": "ETH",
}

def _volatility_currency_for_symbol(symbol: str) -> str | None:
    return _VOLATILITY_CURRENCY_BY_SYMBOL.get(symbol.strip().upper())
~~~

Add `currency: str` to `SecondaryVolatilityDataLoader.__init__`, normalize it once, and replace every hard-coded BTC cache key, Deribit request, `OKX_SPOT_SYMBOLS` lookup, error and source label inside that class with the selected currency.

- [ ] **Step 4: Verify mapping passes**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_kline_symbol_options_and_volatility_currency_mapping`

Expected: PASS.

- [ ] **Step 5: Commit task one**

~~~powershell
git add -- roll_terminal_qt/kline_analysis_window.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat: map kline symbols to volatility currencies"
~~~

### Task 2: 下拉选择、波动率可用性与副图回退

**Files:**

- Modify: `D:/qqokx/roll_terminal_qt/kline_analysis_window.py:4989-4995,5655-5660,5760-5868,6170-6482,7220-7233`
- Modify: `D:/qqokx/tests/test_kline_account_drawer.py:471-475`
- Modify: `D:/qqokx/tests/test_roll_terminal_qt_windows.py:694-707,2551-2558`

**Interfaces:**

- Consumes: `KLINE_SYMBOL_OPTIONS` and `_volatility_currency_for_symbol()`.
- Produces: `_selected_symbol() -> str`, `_current_volatility_currency() -> str | None`, and `_volatility_available_for_current_symbol() -> bool`.
- Produces: read-only `_symbol_combo: QComboBox` connected through `currentTextChanged` to `_on_symbol_confirmed`.

- [ ] **Step 1: Write failing UI and fallback tests**

~~~python
def test_kline_symbol_combo_offers_only_configured_symbols(self) -> None:
    window = KlineAnalysisWindow()
    try:
        self.assertEqual(
            [window._symbol_combo.itemText(i) for i in range(window._symbol_combo.count())],
            list(KLINE_SYMBOL_OPTIONS),
        )
        self.assertFalse(window._symbol_combo.isEditable())
    finally:
        self.dispose_widget(window)

def test_selecting_symbol_without_volatility_reverts_secondary_chart_to_kline(self) -> None:
    with patch("roll_terminal_qt.kline_analysis_window.QTimer.singleShot", return_value=None):
        window = KlineAnalysisWindow()
    try:
        window._secondary_chart_check.setChecked(True)
        window._secondary_chart_kind_mode = "volatility"
        window._symbol_combo.setCurrentText("SOL-USDT-SWAP")
        self.assertEqual(window._secondary_chart_kind(), "kline")
        self.assertFalse(window._secondary_chart_kind_btn.isEnabled())
    finally:
        self.dispose_widget(window)
~~~

- [ ] **Step 2: Verify UI tests fail**

Run: `python -m unittest tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_kline_symbol_combo_offers_only_configured_symbols tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_selecting_symbol_without_volatility_reverts_secondary_chart_to_kline`

Expected: FAIL because `_symbol_combo` does not exist.

- [ ] **Step 3: Implement selector and fallback**

~~~python
self._symbol_combo = QComboBox()
self._symbol_combo.addItems(KLINE_SYMBOL_OPTIONS)
self._symbol_combo.currentTextChanged.connect(lambda _symbol: self._on_symbol_confirmed())

def _selected_symbol(self) -> str:
    return self._symbol_combo.currentText().strip().upper()

def _current_volatility_currency(self) -> str | None:
    return _volatility_currency_for_symbol(self._selected_symbol())
~~~

Replace every `self._symbol_input.text()` read with `self._selected_symbol()`. In `_update_secondary_controls_state`, enable the chart-kind button only when dual charts are enabled and a current volatility currency exists. In `_on_symbol_confirmed`, if no currency exists while the secondary kind is volatility, set `_secondary_chart_kind_mode = "kline"`, clear the secondary pending payload, refresh controls, then reload. Pass the current currency to the volatility loader and use it in the request key, title, display symbol and note.

- [ ] **Step 4: Update old tests and run focused regression**

Replace `_symbol_input.setText(...)` with `_symbol_combo.setCurrentText(...)`, and `.text()` reads with `.currentText()`. Run:

`python -m unittest tests.test_kline_account_drawer tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_kline_symbol_combo_offers_only_configured_symbols tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_selecting_symbol_without_volatility_reverts_secondary_chart_to_kline tests.test_roll_terminal_qt_windows.RollTerminalQtWindowHelperTests.test_primary_average_secondary_normal_request_keys_use_primary_average_only`

Expected: PASS.

- [ ] **Step 5: Compile, run the window suite, and commit**

Run:

`python -m py_compile roll_terminal_qt/kline_analysis_window.py tests/test_kline_account_drawer.py tests/test_roll_terminal_qt_windows.py`

`python -m unittest tests.test_roll_terminal_qt_windows`

Expected: New selector tests pass. If the suite still reports existing account-loading or close-message failures, record them as unrelated to this feature.

~~~powershell
git add -- roll_terminal_qt/kline_analysis_window.py tests/test_kline_account_drawer.py tests/test_roll_terminal_qt_windows.py
git commit -m "feat: add kline symbol selector and volatility fallback"
~~~

