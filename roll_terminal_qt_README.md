# QQOKX Roll Terminal Qt Prototype

独立 Qt 极速移仓终端第一版。

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
- 双腿合约选择。
- 双盘口表格。
- 后台行情线程，UI 不直接做网络请求。
- 公共 WS 优先，REST 兜底。
- 合约切换不阻塞主线程。

## 下一步

- 接入私有 WS：持仓、委托、成交。
- 接入移仓执行状态机。
- 接入四种执行模式：双腿吃单、旧挂新吃、新挂旧吃、双方挂单先成后市价。
