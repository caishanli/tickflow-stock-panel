# 2026-08-05 stock data 服务设计（网络行情数据服务）

## 背景与目标

现状：量化模拟盘/回测的行情数据来自两条本机路径——(a) 直接读 `data/` 下的 parquet 分区，(b) 进程内直连 mootdx/astock 网络回源。mootdx 盘中实时回源（`mootdx_intraday.py`，由 FastAPI 主进程托管守护）把当日 ETF 分钟写进分区供模拟盘纯分区读。

目标：把「mootdx intraday 实时分钟服务」进化为一个**独立的网络行情数据服务（stock data 服务）**：

1. 服务接受客户端请求，自身通过多源（本地 parquet 历史 / mootdx / astock 等）获取数据，返回客户端，并把现场回源结果落盘；
2. 量化回测、量化模拟盘**全部通过网络**连接该服务获取分钟/日线/因子/列表数据，**自身不从任何本地文件或其他网络接口获取行情数据**；
3. 所有 mootdx/baostock 回源落盘（启动 backfill、15:35 定时同步、盘中 intraday、股票分钟）**全部挪进服务自治调度**；FastAPI 主后端只负责启动并守护该服务；
4. 封装一套 **jqdata 风格**的客户端 API 给量化侧使用。

## 范围

**包含**
- stock data 服务（独立进程）：TCP server + msgpack 协议 + 方法分发 + 自治调度（backfill / intraday / 15:35 cron）+ 多源聚合 + 落盘。
- 客户端 SDK（jqdata 风格），供量化侧使用。
- 量化侧三个取数调用面改走网络客户端：`DataManager`（jqengine，回测 + 模拟盘策略模式）、`QuantDataProvider`（看护模式）、`live_feed`（模拟盘盘中分钟）。
- 主后端集成：guardian 守护 stock data 服务；删除启动 backfill 调用与 15:35 mootdx_sync cron。
- 客户端前复权（fq='pre'）在客户端计算（保留 DataManager 动态前复权语义），服务端只出原始价。
- 数据源可插拔：当前注册本地 parquet 读 + mootdx + astock（a-stock-data skill）；baostock 目前**不是依赖**，接口预留（未来新增源只需注册一个实现）。

**不包含（明确不动）**
- 主后端前端展示：kline/screener/监控等继续用现有机制（DuckDB 直读共享 `data/` 目录），`kline_sync`/`quote_service`/`financial_sync` 等一律不改。
- 模拟盘 `minute_realtime_backfill`（模拟盘侧联网回源开关）**不移植**——新设计里实时由服务端保证，该功能自然消失。
- 订阅/推送（YAGNI：模拟盘每分钟低频轮询即可）。
- 跨机部署与鉴权（本阶段同机独立进程；协议预留后续扩展，不做认证）。

## 架构

```
FastAPI 主进程（前端 + 守护）
  └─ stockdata_guardian（PID 锁 + 3s 自愈，泛化自 intraday_guardian）
        └─ Popen ──► scripts/run_stockdata_service.py
                         └─ TCP server（ThreadingTCPServer, msgpack 帧）
                              ├─ 读取: 本地 parquet 分区 / mootdx / astock
                              ├─ 现场回源: 缺口分钟/日线 → 返回 + 原子落盘
                              └─ 自治调度线程:
                                   ├─ 启动 backfill（补齐到当前时间缺失数据）
                                   ├─ intraday 循环（交易时段全量 ETF 分钟）
                                   └─ 15:35 定时同步（ETF 分钟 + 因子表 + 股票分钟增量）

量化侧进程（模拟盘/回测）── TCP ──► 客户端 SDK ──► stock data 服务
```

**新增/改造文件**
- `backend/scripts/run_stockdata_service.py` — 服务进程入口（TCP server + 调度线程）。
- `backend/app/services/stockdata/server.py` — ThreadingTCPServer + msgpack 帧编解码 + method 分发（薄层，委托给既有 `mootdx_service` / 分区读取器 / 实时回源）。
- `backend/app/services/stockdata/scheduler.py` — 自治调度：启动 backfill + intraday 循环 + 15:35 cron（线程）。
- `backend/app/services/stockdata/sources.py` — 数据源聚合（可插拔：parquet 读 / mootdx / astock），带墙钟守护与失败计数。
- `backend/app/services/stockdata_guardian.py` — 由 `intraday_guardian.py` 泛化（守护任意脚本，PID 锁 + 3s 自愈）。
- `backend/app/quant/datasource/network_client.py` — 客户端 SDK（jqdata 风格，阻塞式，重连/超时/请求 id 关联）。
- 删除/停用：`mootdx_intraday.py` 的进程托管逻辑并入 `scheduler.py`；`run_mootdx_intraday.py` 被 `run_stockdata_service.py` 取代；`intraday_guardian.py` 泛化为 `stockdata_guardian.py`。

## 传输协议

- TCP，**4 字节大端长度前缀 + msgpack 帧**。
- 请求：`{"v":1, "id":<int>, "m":<method>, "p":{<params>}}`
- 响应：`{"v":1, "id":<int>, "ok":true, "t":"parquet"|"json", "d":<bytes|dict>}`
  - `parquet`：批量行情（全市场日线、分钟池）以 parquet 字节嵌入 msgpack 二进制，避免 JSON 体积爆炸；
  - `json`：元数据/列表/错误。
- 错误响应 `ok=false`，`d` 为 `{"code":<str>, "msg":<str>}`；未知 method / 帧错误 → 错误码 + 服务端日志。
- 依赖：新增 `msgpack`（无 C 扩展也能用）。

## 客户端 API（jqdata 风格）

**对齐 jqdata 的常规接口**

| 方法 | 语义 |
|------|------|
| `get_price(security, start_date, end_date, frequency='daily'\|'1m'\|'5m', fields=None, fq='pre'\|'none')` | OHLCV 行情，单只或多只 → DataFrame |
| `get_bars(security, count, unit='1m'\|'5m'\|'daily', fields=None, include_now=True, end_dt=None)` | 最近 N 根 |
| `get_trade_days(start_date, end_date)` | 交易日历 |
| `get_all_securities(types=['stock','etf','index'], date=None)` | 全市场标的列表（name/上市日/退市日） |
| `get_security_info(code)` | 单只标的元数据 |
| `get_index_stocks(index_code, date=None)` | 指数成分股 |
| `current_snapshot(codes)` | **批量最新行情**（当日分区 + 未覆盖标的 mootdx 并发补实时），对应模拟盘每轮 tick 的批量最新分钟线 |
| `get_adj_factors(code)` | 前复权因子表 |

**服务专用批量扩展**（jqdata 无全市场批量，模拟盘盘前/回测预载需要）
- `preload_daily(lookback_days=400, force=False)` — 全市场日线整帧批量返回。
- `get_minute_pool(codes, lo_ts, hi_ts)` — 多只在时间窗内的 1m。

**映射关系**（量化侧现有接口 → 新 API）
- `get_daily` → `get_price(frequency='daily')`
- `get_minute` → `get_price(frequency='1m')` / `get_bars`
- `get_minute_recent`（实时） → `current_snapshot`
- `get_adj_factor_map` → `get_adj_factors`
- 列表 → `get_all_securities` / `get_index_stocks`

**运维方法**
- `ping`（心跳）/ `status`（回源进度、失败统计）
- `trigger_sync(kind)`（手动触发日线/分钟/因子/backfill 同步，供主后端或人工调用）

**语义决策**
- 前复权（`fq='pre'`）**在客户端计算**：客户端用 `get_adj_factors` 取因子表本地折算，保留 DataManager「动态前复权」（只用事件日 ≤ 决策日的除权事件）的正确性。服务端只出原始价。
- `current_snapshot` 服务端并发回源口径沿用现有 `_guarded_get_minute`（墙钟 30s/只，超时重建 `MootdxSource`，指数标的排除）。
- 服务端单只失败只标记该标的，不拖垮整批。

## 数据流

**模拟盘策略模式一轮 tick**
```
runner._pre_market（盘前）: client.preload_daily(force=True)   ← 全市场日线到昨收（TCP 批量）
runner 主循环每分钟      : feed.refresh → client.current_snapshot(watch_codes)
                           → 服务端读当日 ETF 分钟分区 + 未覆盖标的 mootdx 并发补实时
                           → 返回 {code: 最新 bar}
策略 get_history/get_price: DataManager → client.get_price / get_minute_pool → TCP
```

**回测**
```
预载: client.preload_daily(lookback_days)   （一次批量，1600+ 标的）
按需: client.get_price(frequency='1m') / client.get_minute_pool(codes, lo, hi)
qfq : client.get_adj_factors(code) → 客户端本地折算
```

**看护模式**：`QuantDataProvider` 各方法改为委托 `network_client`。

## 错误处理

**服务端**
- 单只标的失败（超时/异常/空）→ 该标的错误标记，不中断整批；墙钟守护（分钟 30s、日线 20s），超时重建 `MootdxSource`。
- 落盘失败仅告警；回源写 0 保持现有告警铁律（禁止静默）。
- 崩溃/重启：客户端重连，guardian 3s 自愈，重启后 backfill 幂等补齐。

**客户端（量化侧）**
- 连接失败 → 指数退避重连（0.5s×2^n，上限 30s），持续 warning；**模拟盘不因服务中断而崩**（沿用「分区读失败沿用旧帧 + 告警」语义）。
- 请求超时 → 按标的粒度失败，单标的保留内存旧帧。
- 回测：服务不可达时**快速失败**并给出明确错误（如「stock data 服务未启动」），不静默降级。

## 测试

**新单测**
- 协议层：帧编解码 roundtrip、请求 id 关联、parquet 大帧收发。
- 服务端 handler：临时 `data/` 目录 + 注入 fake mootdx source，验证 `get_price`/`current_snapshot`/`preload_daily` 的分区读 + 实时补齐 + 落盘。
- 客户端 SDK：对进程内测试 TCP server，验证重连、超时、批量分帧。

**量化侧改造回归**
- 现有 `test_minute_realtime_backfill` / `test_live_feed` / `test_fix_datamanager` 等改为注入 fake `network_client` 继续验证；`DataManager` 内部结构保留，多数用例只换取数对象。
- **wufu_v52 fixture 验收（核心回归门槛）**：
  - 回测对齐：本地跑 wufu-v5.2 回测 `260401-260716`，与 fixture `tests/fixtures/wufu_v52/backtest_260401-260716/`（交易记录 + 收益）用 `scripts/diff_jq_vs_local.py` 对比，结果与改造前基线一致（收益逐日差 ≤0.05%、交易组对齐口径同现状）；
  - 模拟盘对齐：本地跑 wufu-v5.2 模拟盘对齐 `260710`，与 fixture `tests/fixtures/wufu_v52/sim_260710/live_transaction_list.csv` 对比，交易对齐口径同现状。
  - 验收前提：以上两条在改造**前**先跑出基线（保存结果），改造后 diff 结果与基线逐位一致。

**验证命令**（backend/ 下）
```bash
uv run --extra dev pytest
uv run --extra dev ruff check app
uv run --extra dev mypy app
```

## 实施顺序

1. **分支与移植**：从 `custom-main` 新建 `feature/stockdata-service`；从 `feature/mootdx-intraday` 移植 `mootdx_intraday.py`（并入服务调度）、`run_mootdx_intraday.py`/`intraday_guardian.py`（泛化为服务守护）、`mootdx_service.py` 的 `sync_index_daily` 及 staleness 检查。`minute_realtime_backfill` 不移植。
2. **服务骨架**：TCP server + msgpack 协议 + 客户端 SDK 最小可用（`ping`/`get_price`/`current_snapshot`）。
3. **服务自治调度**：启动 backfill + intraday 循环 + 15:35 cron 从主后端挪进服务；服务内自跑。
4. **主后端集成**：guardian 泛化守护 stock data 服务；删除启动 backfill 与 15:35 mootdx_sync cron；前端不动。
5. **量化侧改造**：`DataManager` 改走 network_client（回测 + 模拟盘策略模式核心路径）。
6. **量化侧改造**：`QuantDataProvider`（看护）+ `live_feed`（盘中分钟）改走 network_client；删除 `minute_realtime_backfill` 开关。
7. **测试与回归**：新单测 + 既有测试改造 + wufu_v52 回测/模拟盘对齐验收 + 真实交易时段线上验证。

每阶段独立可验证、可回退；先让服务「能独立跑、能返回数据」，再让主后端只做守护，最后才动量化侧。

## 成功标准

- [ ] 量化回测/模拟盘运行期**零本地 parquet 读取、零 mootdx/astock 直连**，全部数据经 TCP 从 stock data 服务获取（可 grep 验证量化进程不再引用分区读取/mootdx 网络源代码路径）。
- [ ] stock data 服务自治完成启动 backfill、盘中 intraday、15:35 同步；FastAPI 主进程只做守护（PID 锁 + 3s 自愈）。
- [ ] 服务端单只失败不影响整批；客户端服务中断不崩模拟盘（沿用旧帧 + 重连）。
- [ ] `get_price`/`current_snapshot`/`preload_daily` 与改造前输出一致。
- [ ] wufu_v52 回归：回测对齐 `backtest_260401-260716`、模拟盘对齐 `sim_260710`，结果与改造前基线一致。
- [ ] 无新增测试失败、无新增 ruff/mypy 错误类别（repo 基线本就脏，以 base/head 逐字节对比确认零回归）。
- [ ] 数据源可插拔接口就绪（新增 baostock 等源只需注册一个实现）。
