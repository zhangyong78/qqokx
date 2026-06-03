# Research Report

## 1. Data Source

- source: `local_okx_candle_cache`
- data_root: `D:\qqokx_data`
- symbol: `BTC-USDT-SWAP`
- timeframe: `1D`

## 2. Data Range

- first bar: 2019-11-27T16:00:00+00:00
- last bar: 2026-06-01T16:00:00+00:00
- bar count: 2379

## 3. Indicator Definitions

- EMA21 = EMA(close, 21)
- EMA55 = EMA(close, 55)
- ATR14 = Wilder ATR(high, low, close, 14)
- RSI14 = RSI(close, 14)
- VOL_MA20 = SMA(volume, 20)

## 4. Backtest Assumptions

- initial_capital: 100000
- risk_per_trade: 0.005
- fee_rate: 0.0004
- slippage_rate: 0.0002
- direction: short_only
- max_open_positions: 1
- compound_interest: true

## 5. Fees And Slippage

- short entry fill = open[t+1] * (1 - slippage_rate)
- short exit fill on trend exit = open[t+1] * (1 + slippage_rate)
- short stop exit fill = stop_loss * (1 + slippage_rate)
- fees are charged on both entry and exit notionals

## 6. Strategy Summary

| strategy_name                 | start_date                | end_date                  |   initial_capital |   final_equity |   total_return |   annual_return |   max_drawdown |   profit_factor |   win_rate |   trade_count |   average_R |   median_R |   largest_win_R |   largest_loss_R |   average_holding_days |   max_consecutive_losses |    sharpe |    sortino |    calmar |     score |
|:------------------------------|:--------------------------|:--------------------------|------------------:|---------------:|---------------:|----------------:|---------------:|----------------:|-----------:|--------------:|------------:|-----------:|----------------:|-----------------:|-----------------------:|-------------------------:|----------:|-----------:|----------:|----------:|
| strategy_g_dual_bear_volume   | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       103494   |     0.0349431  |     0.0052894   |    -0.00779425 |       2.61685   |   0.451613 |            31 |   0.223177  | -0.0497556 |       2.20267   |        -0.652813 |               13.0645  |                        5 |  0.609475 |  0.361237  |  0.678629 |   7.43038 |
| strategy_b_ema55_pullback     | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       102598   |     0.0259768  |     0.00394674  |    -0.009811   |       2.09758   |   0.451613 |            31 |   0.166704  | -0.0497556 |       2.15232   |        -0.652813 |               12.1935  |                        5 |  0.51274  |  0.22838   |  0.402277 |   5.88348 |
| strategy_f_dual_bear_rsi      | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       102070   |     0.0206985  |     0.0031517   |    -0.0108224  |       1.70185   |   0.393939 |            33 |   0.125613  | -0.0783342 |       2.20267   |        -1.01952  |                9.66667 |                        5 |  0.370458 |  0.146963  |  0.291221 |   4.71737 |
| strategy_c_dual_bear_pullback | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       101971   |     0.0197144  |     0.00300309  |    -0.0134733  |       1.41665   |   0.358491 |            53 |   0.0748466 | -0.133494  |       2.20267   |        -1.01952  |               10.4151  |                        7 |  0.310432 |  0.162129  |  0.222892 |   3.78419 |
| strategy_a_ema21_pullback     | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       102000   |     0.0199955  |     0.00304555  |    -0.0283188  |       1.23285   |   0.272727 |            99 |   0.0409952 | -0.117251  |       2.20267   |        -1.01952  |               10.2525  |                       11 |  0.252197 |  0.168373  |  0.107545 |   3.10055 |
| strategy_e_second_pullback    | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |       100423   |     0.00423282 |     0.00064898  |     0          |     999         |   1        |             1 |   0.846564  |  0.846564  |       0.846564  |         0.846564 |               27       |                        0 |  0.391778 |  0         |  0        |  -6.55362 |
| strategy_d_first_pullback     | 2019-11-27T16:00:00+00:00 | 2026-06-01T16:00:00+00:00 |            100000 |        99640.4 |    -0.00359603 |    -0.000553176 |    -0.00359603 |       0.0169305 |   0.333333 |             3 |  -0.239927  | -0.266945  |       0.0124028 |        -0.465239 |               12.3333  |                        1 | -0.525682 | -0.0583007 | -0.15383  | -10.0943  |

## 7. Yearly Performance Analysis

|   year | strategy_name                 |   year_start_equity |   year_end_equity |   year_return |   trade_count |   win_rate |   max_drawdown |   profit_factor |
|-------:|:------------------------------|--------------------:|------------------:|--------------:|--------------:|-----------:|---------------:|----------------:|
|   2020 | strategy_a_ema21_pullback     |            100000   |           99872.2 |  -0.00127842  |            18 |   0.111111 |   -0.00952476  |       0.907089  |
|   2021 | strategy_a_ema21_pullback     |             99872.2 |           99634.8 |  -0.00237646  |            14 |   0.214286 |   -0.00743473  |       0.829872  |
|   2022 | strategy_a_ema21_pullback     |             99634.8 |          101737   |   0.0211      |            15 |   0.533333 |   -0.00666913  |       2.50591   |
|   2023 | strategy_a_ema21_pullback     |            101737   |          100284   |  -0.0142867   |            15 |   0.266667 |   -0.0145248   |       0.188102  |
|   2024 | strategy_a_ema21_pullback     |            100284   |           99683.3 |  -0.00598644  |            12 |   0.333333 |   -0.00649828  |       0.369481  |
|   2025 | strategy_a_ema21_pullback     |             99683.3 |          100544   |   0.00863126  |            20 |   0.2      |   -0.00887576  |       1.64153   |
|   2026 | strategy_a_ema21_pullback     |            100544   |          102000   |   0.0144801   |             5 |   0.4      |   -0.00331351  |       5.3224    |
|   2020 | strategy_b_ema55_pullback     |            100000   |          100381   |   0.00381416  |             3 |   0.333333 |   -0.00280263  |       2.35195   |
|   2021 | strategy_b_ema55_pullback     |            100381   |           99685   |  -0.0069379   |             3 |   0        |   -0.00499567  |       0         |
|   2022 | strategy_b_ema55_pullback     |             99685   |          100406   |   0.00723091  |             6 |   0.666667 |   -0.00113559  |       4.47015   |
|   2023 | strategy_b_ema55_pullback     |            100406   |          100337   |  -0.000690121 |             6 |   0.333333 |   -0.00310182  |       0.794376  |
|   2024 | strategy_b_ema55_pullback     |            100337   |           99868.1 |  -0.00466819  |             5 |   0.2      |   -0.0033307   |       0.410309  |
|   2025 | strategy_b_ema55_pullback     |             99868.1 |          101935   |   0.0206936   |             5 |   0.6      |   -0.000290352 |      43.0278    |
|   2026 | strategy_b_ema55_pullback     |            101935   |          102598   |   0.00650352  |             3 |   1        |    0           |     999         |
|   2020 | strategy_c_dual_bear_pullback |            100000   |           99176.7 |  -0.00823319  |             6 |   0        |   -0.0061569   |       0         |
|   2021 | strategy_c_dual_bear_pullback |             99176.7 |           98798.6 |  -0.00381245  |             5 |   0.2      |   -0.00363177  |       0.0139398 |
|   2022 | strategy_c_dual_bear_pullback |             98798.6 |          100270   |   0.0148977   |            18 |   0.5      |   -0.00847149  |       1.97466   |
|   2023 | strategy_c_dual_bear_pullback |            100270   |           99994.4 |  -0.00275321  |             7 |   0.285714 |   -0.00459345  |       0.555092  |
|   2024 | strategy_c_dual_bear_pullback |             99994.4 |           99764.6 |  -0.00229766  |             3 |   0.333333 |   -0.00326407  |       0.416051  |
|   2025 | strategy_c_dual_bear_pullback |             99764.6 |          101136   |   0.0137486   |             8 |   0.375    |   -0.00344248  |       3.16822   |
|   2026 | strategy_c_dual_bear_pullback |            101136   |          101971   |   0.00825808  |             6 |   0.5      |   -0.00310173  |       3.26809   |
|   2020 | strategy_d_first_pullback     |            100000   |           99866.5 |  -0.00133473  |             1 |   0        |    0           |       0         |
|   2022 | strategy_d_first_pullback     |             99866.5 |           99872.7 |   6.20139e-05 |             1 |   1        |    0           |     999         |
|   2025 | strategy_d_first_pullback     |             99872.7 |           99640.4 |  -0.0023262   |             1 |   0        |    0           |       0         |
|   2022 | strategy_e_second_pullback    |            100000   |          100423   |   0.00423282  |             1 |   1        |    0           |     999         |
|   2020 | strategy_f_dual_bear_rsi      |            100000   |           99659.2 |  -0.00340767  |             3 |   0        |   -0.00310635  |       0         |
|   2021 | strategy_f_dual_bear_rsi      |             99659.2 |           99470.8 |  -0.00189123  |             2 |   0        |   -0.00165638  |       0         |
|   2022 | strategy_f_dual_bear_rsi      |             99470.8 |           99611   |   0.00141008  |            13 |   0.461538 |   -0.00711284  |       1.10529   |
|   2023 | strategy_f_dual_bear_rsi      |             99611   |           99467.4 |  -0.00144174  |             4 |   0.25     |   -0.0032844   |       0.561843  |
|   2024 | strategy_f_dual_bear_rsi      |             99467.4 |           99238.9 |  -0.00229766  |             3 |   0.333333 |   -0.00326407  |       0.416051  |
|   2025 | strategy_f_dual_bear_rsi      |             99238.9 |          101234   |   0.020103    |             2 |   1        |    0           |     999         |
|   2026 | strategy_f_dual_bear_rsi      |            101234   |          102070   |   0.00825808  |             6 |   0.5      |   -0.00310173  |       3.26809   |
|   2020 | strategy_g_dual_bear_volume   |            100000   |           99414.4 |  -0.00585641  |             4 |   0        |   -0.00441798  |       0         |
|   2021 | strategy_g_dual_bear_volume   |             99414.4 |           99396.3 |  -0.000181339 |             2 |   0.5      |    0           |       0.229116  |
|   2022 | strategy_g_dual_bear_volume   |             99396.3 |          101256   |   0.0187063   |             8 |   0.75     |   -0.00130231  |       8.58528   |
|   2023 | strategy_g_dual_bear_volume   |            101256   |          100817   |  -0.00433571  |             6 |   0.166667 |   -0.00617303  |       0.298932  |
|   2024 | strategy_g_dual_bear_volume   |            100817   |          100652   |  -0.00163128  |             2 |   0.5      |   -0.00326407  |       0.501047  |
|   2025 | strategy_g_dual_bear_volume   |            100652   |          102563   |   0.0189857   |             5 |   0.6      |   -0.00111889  |      17.8129    |
|   2026 | strategy_g_dual_bear_volume   |            102563   |          103494   |   0.00907898  |             4 |   0.5      |   -0.00229007  |       4.91983   |

## 8. Max Drawdown Analysis

- best overall max_drawdown: -0.78%
- low drawdown candidate: strategy_g_dual_bear_volume with max_drawdown -0.78%

## 9. Concentration Check

The project uses yearly performance and total trade count to reduce the chance of selecting a strategy that only worked in one isolated period.

## 10. Worthy V2 Candidates

- best overall: strategy_g_dual_bear_volume
- best low drawdown: strategy_g_dual_bear_volume

## 11. Strategies Not Recommended To Continue

- strategy_e_second_pullback: profit_factor=999.00, average_R=0.85, trade_count=1
- strategy_d_first_pullback: profit_factor=0.02, average_R=-0.24, trade_count=3

## 12. Next Optimization Ideas

- daily direction plus 4H entry timing
- retest entry logic after a confirmed pullback failure
- compare trend exit against ATR trailing in a dedicated V2 study

## Ambiguities And Choices

- The original task brief used Binance spot daily data, but this implementation uses local OKX daily data by explicit instruction.
- First and second pullbacks are counted as discrete new touch events, not every consecutive bar that remains in contact with EMA21.
- If the final bar triggers a trend exit without a next open, the engine exits on the same bar close with slippage and records a dedicated exit reason.

## Required Answers

1. EMA21 vs EMA55: strategy_b_ema55_pullback
2. Does EMA21 < EMA55 improve quality: Yes, the dual-bear alignment improved the baseline EMA21 pullback.
3. First vs second pullback: Insufficient sample to decide reliably. strategy_d_first_pullback (3 trades), strategy_e_second_pullback (1 trades).
4. RSI filter value: Positive for strategy_f_dual_bear_rsi.
5. Volume filter value: Positive for strategy_g_dual_bear_volume.
6. Highest return strategy: strategy_g_dual_bear_volume
7. Lowest drawdown strategy: strategy_g_dual_bear_volume
8. Most comfortable overall strategy: strategy_g_dual_bear_volume
9. Best V2 candidate: strategy_g_dual_bear_volume
10. Daily trend plus 4H entry: recommended as the clearest V2 direction if the chosen daily strategy still lacks trade frequency.

Final Recommendation:

Best overall strategy:
- strategy_name: strategy_g_dual_bear_volume
- reason: profit_factor=2.62, max_drawdown=-0.78%, trade_count=31

Best low-drawdown strategy:
- strategy_name: strategy_g_dual_bear_volume
- reason: max_drawdown=-0.78%, profit_factor=2.62

Rejected strategies:
- strategy_e_second_pullback: trade_count 1 < 30
- strategy_d_first_pullback: trade_count 3 < 30; profit_factor 0.02 <= 1.00; average_R -0.24 <= 0

Recommended V2 direction:
- direction 1: daily direction plus 4H entry timing
- direction 2: pullback event definition refinement for first and second retests
- direction 3: exit rule comparison between EMA21 reclaim, ATR trail, and fixed R