# OKX Quant MVP

当前版本已经切换为 `EMA 动态委托 + ATR 止盈止损` 流程。

## 策略说明

- 选择 `OKX SWAP` 永续合约
- 设置 `EMA 周期`
- 设置 `ATR 周期 / 止损倍数 / 止盈倍数`
- 设置 `风险金`
- 选择方向：`只做多` 或 `只做空`
- 点击开始后，程序会立即以上一根已收盘 K 线的 `EMA` 作为开仓价格挂限价委托
- 每一根新 K 线确认后，会先撤掉旧挂单，再按最新的上一根 `EMA` 数值重新挂单
- 仓位按公式自动计算：`开仓数量 = 风险金 / abs(开仓价格 - 止损价格)`
- 止盈止损通过 `attachAlgoOrds` 附加在 OKX 主单上，由 OKX 托管

这套逻辑适用于：

- 上升趋势中的回调做多
- 下降趋势中的反弹做空

## 运行

```powershell
python main.py
```

如果你的环境里没有 `python` 命令，请改用本机实际的 Python 启动方式。

## 当前约束

- 只支持 `OKX SWAP` 永续合约
- 当前脚本不支持 `双向`，只能二选一：`只做多` / `只做空`
- 下单使用限价委托，挂单价格取上一根已收盘 K 线的 `EMA`
- 每次启动会先弹出确认框
- 如果挂单已经成交，程序会停止本次监控
- 如果挂单部分成交，程序会停止重挂，避免重复撤单

## 项目结构

- `main.py`：启动桌面程序
- `okx_quant/ui.py`：Tkinter 界面
- `okx_quant/engine.py`：EMA 动态挂单与撤单重挂逻辑
- `okx_quant/okx_client.py`：OKX REST API 封装
- `okx_quant/strategies/ema_dynamic.py`：EMA 动态委托策略
- `okx_quant/strategies/ema_atr.py`：旧版 EMA 穿越策略（保留）
- `tests/`：核心逻辑测试
