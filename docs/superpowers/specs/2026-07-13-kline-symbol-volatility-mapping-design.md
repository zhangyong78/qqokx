# K线分析交易对与波动率映射设计

## 目标

将 K线分析页的交易对输入框改为固定下拉选择，并让副图波动率随交易对的基础币种自动匹配。

## 交易对

下拉框只提供以下五个品种，默认值保持 `BTC-USDT-SWAP`：

- `BTC-USDT-SWAP`
- `ETH-USDT-SWAP`
- `SOL-USDT-SWAP`
- `DOGE-USDT-SWAP`
- `ETH-BTC`

## 波动率规则

交易对配置集中维护其波动率币种：

- `BTC-USDT-SWAP` 使用 Deribit `BTC DVOL`。
- `ETH-USDT-SWAP` 与 `ETH-BTC` 使用 Deribit `ETH DVOL`。
- `SOL-USDT-SWAP` 与 `DOGE-USDT-SWAP` 没有波动率数据。

当选择没有波动率的交易对时，副图类型固定为普通 K线；波动率切换控件禁用或隐藏。若用户先显示波动率再切换至 SOL 或 DOGE，副图立即回退为普通 K线并重新加载。

## 实现边界

- 只调整 `roll_terminal_qt/kline_analysis_window.py` 与对应窗口测试。
- 波动率加载器接收币种参数，缓存键、Deribit 请求、OKX 现货对比数据与图表标题都使用该参数。
- 双图、分屏、主均副普、普通平均K线及账户抽屉行为不作改动。
- 不接入新的 SOL/DOGE 波动率数据源。

## 验收

- 下拉框顺序与上述五个品种一致。
- BTC 和 ETH 品种分别选择 BTC/ETH 波动率。
- ETH-BTC 选择 ETH 波动率。
- SOL/DOGE 不能显示波动率副图，且切换时不会保留旧的 BTC/ETH 波动率内容。
- 既有双图 K线加载与平均K线模式不回归。
