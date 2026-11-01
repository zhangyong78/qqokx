# 行情故障中转部署说明

行情中转供本地新版分析程序在 OKX 公共行情直连发生网络故障时使用。原请求优先直连；中转服务只代理固定白名单内的 OKX 只读公共行情 GET，不接受交易、账户、任意 URL 或交易所凭证。服务端可独立运行在旧服务器上，不要求升级整套应用，也不需要同步数据库、配置目录或行情缓存。

## 安全要求

- 每个服务单独生成随机高熵 token，至少 32 个字符。不得复用 OKX API Key、Secret 或 Passphrase，也不要写入代码、配置仓库、日志或 URL。
- 客户端公网地址必须是 HTTPS。可由服务自身加载 TLS 证书和私钥，也可通过 TLS 反向代理转发到本机 `127.0.0.1:8765`。
- 明文 HTTP 只用于 loopback 反代和本机测试；不要将服务明文绑定到公网网卡。
- token 轮换时，在服务端更新 `MARKET_RELAY_TOKEN` 并重启服务，再在客户端设置新 token 并重启客户端。

## 启动独立服务

在 PowerShell 7 生成 token 并放入当前进程环境：

```powershell
$bytes = [byte[]]::new(32)
[System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
$token = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
$env:MARKET_RELAY_TOKEN = $token
```

在新版仓库目录中启动 loopback 服务（适合 TLS 反代）：

```powershell
python -m okx_quant.market_relay_server --host 127.0.0.1 --port 8765
```

服务也可以自行终止 TLS 后公网监听。请使用受信任证书并妥善保护私钥：

```powershell
python -m okx_quant.market_relay_server --host 0.0.0.0 --port 8765 --cert C:\certs\relay.crt --key C:\certs\relay.key
```

旧服务器若不具备新版 `okx_quant` 包，可把 `okx_quant\market_relay_server.py` 与 `okx_quant\market_relay_protocol.py` 复制到服务器的独立目录。在该目录运行：

```powershell
$env:MARKET_RELAY_TOKEN = '<与客户端设置一致的随机 token>'
python .\market_relay_server.py --host 127.0.0.1 --port 8765
```

独立目录方式应配合本机 TLS 反代；服务端脚本不需要账户数据、API 凭证或交易模块。使用 Python 3.11 或更高版本，不需要安装第三方依赖。停止独立服务即可关闭服务器中转功能，不影响原服务器程序。

默认最多同时处理 16 个连接（包含未鉴权连接），读取连接有 6 秒超时；通过鉴权的有效请求全局限制为每分钟 240 次，上游请求超时 5 秒，单次上游响应最多 8 MB。未通过鉴权、参数不合法或超过请求频率的请求不会访问交易所。仅允许名单中的公开 GET：行情、盘口、普通/标记/指数 K 线及其历史分页、产品规格、价格限制和期权价格分档；不接受任意目标网址。接口不缓存上游行情，响应绑定本次请求的随机标识与返回时间，并禁止 HTTP 缓存；本地和服务器应保持系统时间同步。

专用 token 表示访问资格，持有它的客户端可以使用中转服务；软件版本号或 User-Agent 不能作为身份凭证。token 不随安装包分发，泄漏后需轮换。鉴权和请求限制用于阻止未授权的上游行情请求，不能替代网络入口对恶意流量的防护。

## 配置本地客户端

Tk 界面打开“设置 → 网络代理设置”，Qt 界面打开“API 与通知设置”的网络区域，勾选“启用行情中转”，填写 HTTPS 根地址和服务 token，点击“保存行情中转”，然后重启所有 QQOKX 窗口。token 保存在当前共享数据目录的独立文件 `config/market_relay.json`，通过 Windows DPAPI 加密；它不写入 `settings.json`，不要求两端使用同一个数据目录。地址示例为 `https://relay.example.com:8765`（服务自身提供 TLS）或 `https://relay.example.com`（反代提供 TLS）。证书必须受信任并匹配地址；客户端不提供跳过证书校验选项。

默认关闭，关闭时保留原直连超时。开启后直连尝试最多 4 秒（调用者指定更短超时仍有效），首次网络故障切换中转，在 30 秒冷却期内直接使用中转；冷却结束后由一个请求探测直连，其余请求继续中转，直连成功后自动恢复，并在应用日志记录通道切换。中转本身超时 8 秒；失败时返回明确错误，上层已有离线缓存策略保持原样。

仅 OKX REST 公共行情请求在网络连接/超时故障时可走中转；HTTP/API 业务错误和私有交易请求不触发故障切换。模拟盘标志请求、自定义上游地址、Deribit 和 OKX WebSocket 不由该服务转发。已有 WS 缓存及依赖它的入口保持原行为，不能视为已接入中转。两条 REST 路径都不可用时，上层现有离线分析/本地历史缓存行为保持原样；此服务不保证这些历史缓存是最新行情，也不改变止盈止损触发策略。last、mark、index 使用各自原接口，历史分页参数原样传递，不互相替代价格类型。

本说明描述部署方式，不表示服务已经部署或正在运行。部署后应从客户端实际验证 HTTPS、token 和公开行情请求，并确认公网入口只开放 TLS。
