# QQOKX Roll Terminal Qt Prototype

独立 Qt 极速移仓终端，用来替代旧 Tk 版里卡顿、轮询阻塞、切换合约慢的问题。

## 运行

```powershell
python run_roll_terminal_qt.py
```

也可以双击：

```text
start_roll_terminal_qt.bat
```

## 当前完成

- 独立 Qt 桌面窗口，不依赖旧 Tk 窗口。
- 公共行情后台线程：优先公共 WS 缓存，REST 兜底。
- 私有账户后台线程：读取交割持仓，并在顶部显示私有 WS、持仓版本、订单版本。
- 合约切换不阻塞主 UI，切换后盘口由后台线程刷新。
- 自动读取现有 API profile，默认使用 `2211`。
- 启动或切换到带密码的 API profile 时会弹出“API 切换密码”，验证通过后才启动私有持仓/订单线程。
- API 切换成功后会重建行情、持仓、订单后台线程，避免界面显示新 API 但后台仍使用旧 API。
- 根据当前交割合约自动刷新更远交割合约候选。
- 执行前弹出真实订单确认，不会误点直接下单。
- 底部执行日志、执行状态表、相关订单状态表。
- 已接入手动移仓参数：按限价挂单、最大滑点、分批次数、每批张数、挂单等待、追单次数、旧合约买入限价、新合约卖出限价。
- 已接入执行方式：双腿吃单、旧合约挂单/新合约吃单、新合约挂单/旧合约吃单、双方挂单/先成后市价。
- 已接入自动移仓：按“目标中间价 - 当前中间价 >= 阈值”触发一次真实移仓，并提供启动/停止按钮。

## 已验证

- `python -m compileall -q roll_terminal_qt run_roll_terminal_qt.py`
- 构造执行请求时，界面参数会进入 `ArbitrageRollRequest`：
  - `execution_mode=both_maker_first_taker`
  - `use_limit_orders=True`
  - `current_derivative_limit_price`
  - `target_derivative_limit_price`
  - `batch_count`
  - `batch_contract_qty`
  - `maker_wait_seconds`
  - `chase_limit`

## 下一步

- 把执行过程中的订单成交回报和 UI 阶段状态合并得更细：挂单中、单腿成交、对腿市价补齐、追单、完成/失败。
- 增加一次完整沙盒/小张数实盘演练记录，确认真实下单链路和订单回报链路都稳定。
