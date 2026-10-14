# Tk_GetPixmap / CreateDIBSection 故障评估

检查日期：2026-10-13（北京时间）

## 判断

用户随后确认：服务器运行的是打包服务器版本的 `RUN`。已核对 `scripts/build_server_package.py`：服务器包的 `RUN.bat` 和 `RUN.ps1` 都启动 `main.py`，默认进入 Tkinter 策略工作台。因此在目前比较的两个程序中，本次弹窗更可能直接由 QQOKX 服务器版发出，应作为第一排查对象。另一个仓库 `zhangyong78/50qq` 当前公开版本使用 PyQt6，未发现 Tkinter 使用，直接发出这个 Tk 弹窗的可能性低。服务器包的具体版本及故障进程 PID 仍未核验。

如果服务器只运行 QQOKX 的 Qt 终端以及上述版本的 50qq，则应查找额外运行的旧 Tk 工作台、旧版打包程序或其他 Tk 程序。当前证据不足以确定谁首先消耗了资源，也不能把弹窗所属进程等同于泄漏源。

本次仅检查源码并进行隔离测试，没有修改策略、下单逻辑或系统配置，也没有启动实盘程序或连接交易账户。

## 用户补充的服务器环境

用户提供任务管理器截图并说明：此前单独运行数月没有同类故障，近期在同一台服务器同时运行 50QQ。

- 截图显示内存 1.4/2.0 GB（70%），CPU 使用率 9%，虚拟处理器 2 个。处理器型号中的“48-Core”是宿主处理器名称，不代表该虚拟机拥有 48 核。
- 截图的 32,503 是系统普通句柄总数，不是某个进程的 GDI 对象数，不能与 GDI 进程配额直接比较。
- 系统运行时间为 47 分 58 秒，不能反映之前长期运行或发生故障时的资源峰值；截图未显示内存提交量和两个程序各自的 GDI 数量。
- 这一时间关联提高了“并行运行增加资源压力，触发 Tk 位图分配失败”的排查优先级，但未证明 50QQ 存在泄漏，也未排除 QQOKX 自身泄漏、版本变化或 RDP 故障。
- 2 GB 环境的物理内存余量约 0.6 GB；70% 本身不是耗尽证据。应查看内存页的“已提交 x/y”以及进程 GDI/USER/提交大小趋势。
- 首先在合适时段暂停 50QQ 做同负载对照，保持 QQOKX 策略正常运行。观察时间应覆盖以往故障出现的时间尺度；短时正常只能说明短时未复现。

## 检查范围及代码证据

QQOKX：当前本地工作区，包含检查前已有的未提交修改；不是服务器实际安装版本的校验。

- `main.py` 默认调用 `okx_quant.ui.run_app`；`okx_quant/ui.py` 转到 `ui_shell.py`，其中 `QuantApp.__init__` 创建 Tk 根窗口。
- 实时图表在 `strategy_live_chart.py` 中重绘前调用 `canvas.delete("all")`，没有发现不断追加旧图形的直接证据。
- `ui_strategy_sessions.py` 按会话复用图表窗口；关闭时移除窗口状态、取消刷新定时器、销毁悬浮提示和图表窗口；刷新前取消已有定时任务，并用 `refresh_inflight` 避免同一图窗重复并发读取。
- 默认图表刷新间隔为 5 秒；运行日志界面有 2,000 行上限。
- 在 `okx_quant` 的 Python 源码搜索中，未找到 PhotoImage、ImageTk、FigureCanvasTkAgg 或 create_image 的使用。存在 Tk Canvas 和 Windows ico 窗口图标，因此仍使用图形资源。
- `run_roll_terminal_qt.py` 走 PySide6 / Qt 启动路径，并配置软件图形回退。`roll_terminal_qt` 中没有找到 Tk()/Toplevel()/PhotoImage 的创建调用。
- Qt 模块确实导入部分 Tk 模块中的辅助函数，但导入本身不创建 Tk 根窗口。`LegacyOptionToolsHost.open_option_roll` 保留启动 `main.py` 的旧方法，未找到该方法的调用点；当前展期按钮调用 `OptionRollQtDialog`。该遗留方法传入的部分参数也不在当前 `main.py` 解析器中，不能把它当作当前实际启动路径。

50qq：检查公开 main 分支，提交 `a0cbf6a5cb9c5c91fa84af9b1e1d822fdb65a4df`，版本 `V2026.09.16.01`。

- `run_windows.bat` 启动 `option_arbitrage_scanner.py`。
- 扫描器使用 PyQt6 QApplication / QMainWindow；配套账本也使用 PyQt6。
- 检查仓库 Python / spec 文件，未找到 tkinter、_tkinter、TkAgg、PhotoImage、ImageTk、CreateDIBSection 或 CreateBitmap 的直接使用。
- 行情界面反复创建 QTableWidgetItem 并 setItem，但表格数量和行数按当前数据设置，不能仅凭每轮创建条目就认定泄漏。Qt 表格接管条目所有权。
- 没有运行其行情程序或 QMT 依赖；其自身、QMT 及系统的长期资源趋势尚未验证。服务器若部署不同版本，此结论需重新核对。

## 隔离测试结果

环境：本机 Windows，64 位 Python 3.12.10，Tcl/Tk 8.6.15。不是出故障的服务器 RDP 会话。

测试仅调用实际策略图表渲染函数，使用 180 根模拟 K 线、15/55/100 三组均线及 1120×620 Canvas。测试窗口放在屏幕可见区域外。采样使用 Windows GetGuiResources，普通进程句柄总数没有作为 GDI 数量使用。

| 阶段 | GDI 对象 | USER 对象 | Canvas 图形项 |
| --- | ---: | ---: | ---: |
| 预热重绘 10 次 | 28 | 18 | 394 |
| 连续重绘 250 次 | 28 | 18 | 394 |
| 连续重绘 500 次 | 28 | 18 | 394 |
| 连续重绘 750 次 | 28 | 18 | 394 |
| 连续重绘 1,000 次 | 28 | 18 | 394 |
| 销毁 Canvas | 27 | 17 | — |
| 图窗新建/销毁 25、50、75、100 次后 | 27 | 17 | — |

未复现弹窗，也未观察到本次图表重绘和窗口新建/销毁路径的 GDI 持续累积。这不覆盖整个工作台、其他窗口、真实行情线程、屏幕尺寸变化或数天运行；不能据此保证服务器不会发生。

现有 `test_strategy_live_chart.py` 和 `test_runtime_resource_limits.py`：24 项通过。普通 pytest 启动受项目已有配置解析错误阻挡（pyproject.toml 第一行）；使用 `-c NUL` 绕过自动配置后执行上述测试。未修改该无关配置。原有资源限制测试主要检查日志队列，不是服务器 GDI 泄漏测试。

## 对故障说明的校正

1. 弹窗标题能定位到 Tk 的 Windows 绘图实现，但不能单凭标题判定 GDI 句柄泄漏。Tk 8.6 的实现先调用 CreateBitmap，失败后回退到 CreateDIBSection；第二次也失败才显示该弹窗。需要结合句柄趋势、内存提交量、位图尺寸及 RDP 事件判断。
2. 不能直接断言“不是内存问题”。DIB 本身需要分配内存，物理内存充足也不代表所有相关内存或图形资源都可分配。
3. Windows 有进程级 GDI 配额和会话级资源约束；没有本次证据证明 RDP 会话具有一律更低的进程 GDI 配额。也不能凭 0x112f 就确定是某个程序泄漏导致。
4. 句柄接近配额前持续增长就值得排查，不应等突破 10,000 才处理；具体上限应读取实际配置。调高配额或关闭 WDDM 不能替代归因，本次未做这些更改。
5. Tk 内部临时 Pixmap 由 Tk 负责释放；不能把“每次刷新后销毁整个 Canvas”当作普遍修复。应先确认哪类对象未释放，再修复其生命周期。

## 服务器下一步

- 确认 QQOKX 启动入口和两个程序实际版本。策略工作台通常显示“OKX 策略工作台（服务器版本）”；Qt 终端使用另一套窗口。
- 在任务管理器详细信息中同时显示 PID、GDI 对象、USER 对象、提交大小，记录启动后、运行期间和出错前的数值。用命令行列区分多个 python.exe / pythonw.exe。
- 用 Process Explorer 的窗口定位工具确定该弹窗所属 PID。弹窗由 Tk 创建为无父窗口，不能靠它覆盖在哪个应用上判断。
- 对比同一工作负载下各进程资源的变化：哪个进程持续增长，哪个才更值得怀疑；若各程序稳定而故障跟 RDP 重连、分辨率变化同时出现，则优先检查远程图形会话及相关系统事件。避免为试验直接终止实盘策略。

## 来源

- [50qq 扫描器源码](https://github.com/zhangyong78/50qq/blob/a0cbf6a5cb9c5c91fa84af9b1e1d822fdb65a4df/option_arbitrage_scanner.py)
- [Tk 8.6 Windows Pixmap 实现](https://github.com/tcltk/tk/blob/core-8-6-branch/win/tkWinPixmap.c)
- [Microsoft：CreateDIBSection](https://learn.microsoft.com/en-us/windows/win32/api/wingdi/nf-wingdi-createdibsection)
- [Microsoft：GDI Objects](https://learn.microsoft.com/en-us/windows/win32/sysinfo/gdi-objects)
- [Qt：QTableWidget 所有权](https://doc.qt.io/qt-6/qtablewidget.html#setItem)
