# BTCUSDT EMA Short Research Alignment

当前先按下面这套口径对齐，行情源明确使用本地 `OKX` candle cache，不走外部 CSV。

## 当前口径

- 数据源：`local_okx_candle_cache`
- 默认数据根目录：`QQOKX_DATA_DIR`，未设置时回落到项目同级 `qqokx_data/`
- 当前本机实际数据根目录：`D:\qqokx_data`
- 默认研究标的：`BTC-USDT-SWAP`
- 默认研究周期：`1H`
- 主研究入口：`scripts/run_btc_1h_short_strategy_research.py`

## 已对齐的点

- 主研究脚本现在支持显式传入 `--data-dir`
- 主研究脚本现在支持显式传入 `--inst-id`
- 主研究脚本现在支持显式传入 `--bar`
- 主研究脚本现在支持显式传入 `--report-dir`
- 运行后会额外输出 `research_runtime_context.md`，把本次研究使用的数据根目录和标的写清楚

## 推荐运行方式

```powershell
python scripts/run_btc_1h_short_strategy_research.py `
  --data-dir D:\qqokx_data `
  --inst-id BTC-USDT-SWAP `
  --bar 1H `
  --report-dir D:\qqokx\reports
```

## 当前本地数据检查

- `BTC-USDT-SWAP 1H`：56643 根
- `BTC-USDT-SWAP 4H`：14162 根
- `BTC-USDT-SWAP 15m`：224850 根
- `BTC-USDT-SWAP 1D`：2379 根

## 说明

你提到的 `BTCUSDT_EMA_SHORT_RESEARCH_TASK_FOR_CODEX.md` 这个文件名，在这台机器上没有搜到同名文件，所以这次先按仓库里现有的 BTC 空头研究主链路和你确认的“本地 OKX 行情”做了统一入口对齐。
