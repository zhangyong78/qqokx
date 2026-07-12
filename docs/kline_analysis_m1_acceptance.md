# Qt 本地交易终端验收记录

## 边界

- Qt 负责短时看盘、手动交易、条件单与套利会话。
- Tk/服务端继续负责长期实盘策略、回测与服务器执行。
- 两套程序继续使用同一数据目录；Qt 不修改策略引擎或回测逻辑。

## 实现验收

- 主窗口复用账户、K线、专业套利三个页面；K线不再作为交易型第二窗口打开。
- 账户/订单初始读取使用 REST，普通订单、持仓、账户使用私有 WS，算法单使用业务 `orders-algo` WS。
- 每次 WS 更新经过 Qt 合帧；REST 仅用于启动、手动/断线恢复和 60 秒安全校验。
- 当前 K线完成首次历史加载后订阅业务 candle WS；开 K 覆盖与新 K 追加不会调用历史加载或全量图表渲染。
- 隐藏图表或切换页面仅暂停图表自动刷新，RR 监控与本地条件任务继续运行。
- 关闭时检测本地 RR/线条交易任务，并明确说明交易所已挂订单不会被撤销。
- Qt 主进程不再创建隐藏 Tk 根窗口或运行 Tk 事件泵；遗留 Tk 工具通过独立进程启动。

## 自动化验证

核心实时链路的针对性测试已通过。当前仓库另有独立、预先存在的失败：

- `test_kline_account_drawer_load_thread_fetches_positions_and_orders_in_parallel`
- `test_line_trading_flatten_coin_input_exceeds_available_after_convert`
- `test_line_trading_flatten_rejects_close_size_above_available`
- `test_scan_can_filter_only_futures`

这些失败不涉及本次修改的文件或调用路径，未为了测试数字而改动稳定的套利/平仓逻辑。
