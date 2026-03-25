# OKX Quant MVP

当前版本已经升级为 `多策略工作台`，内置：

- `EMA 动态委托`
- `EMA 穿越市价`
- `菜单独立回测窗口`

## 策略说明

### EMA 动态委托

- 选择 `OKX SWAP` 永续合约
- 设置 `EMA 周期`
- 设置 `ATR 周期 / 止损倍数 / 止盈倍数`
- 设置 `风险金`
- 选择方向：`只做多` 或 `只做空`
- 点击启动后，程序会立即以上一根已收盘 K 线的 `EMA` 作为开仓价格挂限价委托
- 每一根新 K 线确认后，会先撤掉旧挂单，再按最新的上一根 `EMA` 数值重新挂单
- 仓位按公式自动计算：`开仓数量 = 风险金 / abs(开仓价格 - 止损价格)`
- 止盈止损通过 `attachAlgoOrds` 附加在 OKX 主单上，由 OKX 托管

这套逻辑适用于：

- 上升趋势中的回调做多
- 下降趋势中的反弹做空

### EMA 穿越市价

- 最近一根已收盘 K 线上穿 EMA 做多，下穿 EMA 做空
- 做多止损：信号 K 线最低价减去 `1 ATR`
- 做空止损：信号 K 线最高价加上 `1 ATR`
- 仓位同样按风险金自动计算

## 回测

- 菜单 `工具 > 打开回测窗口`
- 会根据当前策略参数启动独立回测窗口
- 第一版支持最近 `300` 根已收盘 K 线
- 可显示 K 线图、进出场连线、止盈止损触发位置
- 输出基础报告：交易次数、胜率、总盈亏、盈亏比、Profit Factor、最大回撤等

## 运行

```powershell
python main.py
```

如果你的环境里没有 `python` 命令，请改用本机实际的 Python 启动方式。

## 当前约束

- 只支持 `OKX SWAP` 永续合约
- `EMA 动态委托` 不支持 `双向`，只能二选一：`只做多` / `只做空`
- 回测第一版最多使用最近 `300` 根已收盘 K 线
- 敏感凭证文件 `.okx_quant_credentials.json` 已加入 `.gitignore`

## 项目结构

- `main.py`：启动桌面程序
- `okx_quant/ui.py`：主工作台界面
- `okx_quant/backtest_ui.py`：回测窗口界面
- `okx_quant/backtest.py`：回测逻辑与报告计算
- `okx_quant/engine.py`：实盘策略执行逻辑
- `okx_quant/okx_client.py`：OKX REST API 封装
- `okx_quant/strategies/ema_dynamic.py`：EMA 动态委托策略
- `okx_quant/strategies/ema_atr.py`：EMA 穿越市价策略
- `okx_quant/strategy_catalog.py`：策略目录与说明
- `tests/`：核心逻辑测试
