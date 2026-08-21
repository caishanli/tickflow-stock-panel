# AGENTS.md — tickflow-stock-panel

A 股「选股 + 监控 + 回测」量化工作台。后端 FastAPI + Polars/DuckDB/Parquet，前端 React 18 + Vite + TS。数据来自 TickFlow 数据源。

## 运行

- 一键前后端：`./dev.sh`（后端 `http://localhost:3018`，前端 `http://localhost:3011`）。`Ctrl-C` 同时关闭两端。
  - 改端口：`BACKEND_PORT=8000 ./dev.sh` / `FRONTEND_PORT=5173 ./dev.sh`。
  - 老 CPU（无 AVX2/FMA）：根 `.env` 设 `BACKEND_EXTRAS=legacy-cpu`（dev.sh 与 Docker 都会读取）。
  - `./dev.sh` 会 `kill` 掉已占用端口的进程，注意别误杀。
- 后台运行：**一律用 `setsid` 脱离**——在 agent/bash 会话里后台拉起（裸 `nohup ... &` 或 `&` 会让工具/父 shell 等待 dev.sh 的 stdout 管道直到超时）：
  `setsid ./dev.sh > /tmp/tickflow-dev.log 2>&1 </dev/null & disown`
  启动后**另起一个命令**验证端口（`ss -tlnp | grep -E ":3018|:3011|:3322"`），不要在同一个调用里 `sleep`/轮询（会连带超时）。确认存活后再操作。
- 若后端 `:3018` 卡死不响应：多为 uvicorn `--reload`（改代码触发）与 stockdata guardian 重拉子进程的竞态，kill 掉 3018/3011/3322 端口的进程后用上面 setsid 命令重启即可。
- 首次启动需 `cp .env.example .env`（留空 `TICKFLOW_API_KEY` = None/Free 模式，仅历史日 K）。`AUTH_PASSWORD` 仅首次初始化生效，改密码用页面 UI。

## 验证命令（易踩坑）

后端用 `uv`，**dev 依赖（pytest/ruff/mypy）不在基础 venv 里**，必须带 `--extra dev`：

```bash
cd backend
uv run --extra dev pytest                 # 全部测试
uv run --extra dev pytest tests/test_x.py # 单个测试文件
uv run --extra dev ruff check app         # lint（line-length 100，select E,F,I,N,UP,B,SIM,RUF，忽略 E501）
uv run --extra dev mypy app               # 类型检查
```

- 测试从 `backend/` 目录运行，用例 `from app...` 导入。**不要**裸跑 `uv run pytest`（会落到系统 pytest，缺依赖）。
- `asyncio_mode = "auto"`，pytest 配置在 `backend/pyproject.toml` 的 `[tool.pytest.ini_options]`。
- 前端无测试脚本。Lint/类型检查：`cd frontend && pnpm lint`；类型检查+构建：`pnpm build`（`tsc -b && vite build`）。

## 架构要点

- 入口：`backend/app/main.py`（`app.main:app`，uvicorn `--reload`）。路由在 `backend/app/api/`，按功能拆分（screener/backtest/monitor 等）。
- 数据层：`backend/app/tickflow/repository.py` 加载 instruments + 日 K，计算 **enriched** 表落 Parquet（路径 `data/kline_daily_enriched/date=YYYY-MM-DD/`）。`data/` 整体 **不入库**（见 `.gitignore`），更新代码不会覆盖本地数据。
- **quant.db 唯一路径 = 仓库根 `data/quant.db`**（`CONFIG.db_path` / `QUANT_DB_PATH`）。脚本里引用 quant.db 一律用 `CONFIG.db_path`，不要用 `"data/quant.db"` 相对路径（从 `backend/` 运行会写到 `backend/data/` 遗留库，前端/模拟盘读不到）。
- 计算主力用 **Polars**；查询用 **DuckDB**；存储 **Parquet**。唯一 pandas 边界是回测（`vectorbt`，ADR-19），别在其它处引入 pandas。
- 回测依赖可选：`uv sync --extra backtest`（vectorbt 体积大、macOS/Intel 可能需现场编译）。
- 策略系统：`backend/app/strategy/`，18 个内置策略在 `builtin/`。扩展方式见 `docs/strategy.md` 与 `backend/app/strategy/prompts/strategy-guide.md`（AI 生成与手写规范）。
- 插件/数据源：`backend/app/plugins/`（内置 `stocksdk`）。第三方数据接入走 YAML，见 `docs/custom-data-source.md` 与 `docs/plugin-development.md`。
- 多数 `/api/*` 需鉴权（未登录返回 401）。
- 模拟盘启动内存守卫：`account_start`（手动）与 SimDaemon 自动重拉（`account_ensure_running`）spawn 前都检查系统空闲内存（`simulate/memory.py`），不足则拦截——手动启动返回 400 提示，daemon 本轮跳过下轮重试。单账户估算 = `max(活模拟盘进程 RSS 均值, SIM_ACCOUNT_MEM_MIN_MB)`，无活进程样本回退 `SIM_ACCOUNT_MEM_MB`（默认 400MB/下限 300MB，env 可调）。

## stock data 服务（网络行情数据服务，quant/回测侧唯一取数入口）

- 独立进程：`backend/scripts/run_stockdata_service.py`（TCP server + 4 字节大端长度前缀 + msgpack 帧，代码在 `backend/app/services/stockdata/`）。手动启动：`cd backend && uv run --extra dev python scripts/run_stockdata_service.py`。
- 配置（env）：`STOCKDATA_HOST`（默认 127.0.0.1）/ `STOCKDATA_PORT`（默认 3322）/ `STOCKDATA_FETCH_WORKERS`（默认 16，服务端共享网络拉取线程池，mootdx socket 不线程安全、每 worker 独立源）。
- 主后端守护：`app/main.py` lifespan 由 `stockdata_guardian`（`backend/app/services/stockdata_guardian.py`）托管子进程——单实例 PID 锁 + 3s 自愈，`STOCKDATA_ENABLED=0` 关闭。回源落盘全在服务内自治，主后端只做守护。
- **量化侧唯一取数入口** = `StockDataClient`（`backend/app/quant/datasource/network_client.py`，jqdata 风格：`get_price`/`current_snapshot`/`preload_daily`/`get_minute_pool`/`get_adj_factors`/`get_trade_days`/`get_all_securities`/`get_index_stocks`/...）。`DataManager`（`quant/jqengine/datasource/manager.py`）/ `QuantDataProvider` / `live_feed` 全走网络客户端——**零本地 parquet 读取、零 mootdx/astock 直连**。前复权（fq='pre'）在客户端用 `get_adj_factors` 本地折算。主后端前端展示（kline/screener 等）仍 DuckDB 直读共享 `data/`，不受影响。
- 服务自治回源（`stockdata/scheduler.py`，不再由主后端执行）：启动 backfill（`backfill_to_now`）、工作日 15:35 收盘批量同步（ETF 分钟 + 前复权因子表 + 股票分钟增量）、00:00 清空前一日分钟内存库。**无主动盘中全市场轮询**——实时分钟只在客户端 `current_snapshot` 请求时按需回源：`rt:{code}` 标的级 single-flight 去重 + 共享拉取线程池，当日分钟内存库纯 lazy（未请求标的零内存），非交易时段不触网。
- 验收命令（backend/ 下，详情见 `docs/superpowers/specs/2026-08-05-stockdata-service-design.md`）：
  - wufu_v52 回测对齐 `260401-260716`：跑 `run_quant_backtest.py`（区间 2026-04-01~2026-07-16）后用 `scripts/diff_jq_vs_local.py` 对比 fixture `tests/fixtures/wufu_v52/backtest_260401-260716/`（收益逐日差 ≤0.05%、交易组对齐口径同现状）。
  - 模拟盘对齐 `sim_260710`：`run_quant_sim.py --account wufu_v52_sim --strategy tests/fixtures/wufu_v52/wufu-v5.2.py --date 2026-07-10`，成交对比 `tests/fixtures/wufu_v52/sim_260710/live_transaction_list.csv`。
  - 回测性能门禁：wufu-v5.2 `260401-260716` 全程 ≤120s——`uv run --extra dev pytest -m integration tests/quant/test_wufu_backtest_perf.py -q`。

## mootdx 数据服务（stock data 服务的数据源/回源实现）

- `backend/app/services/mootdx_service.py`：stock data 服务内部多源之一（mootdx 回源），被 `stockdata/scheduler.py` 与 `stockdata/sources.py` 消费。
  - `sync_etf_minute(day)`：拉当日全部 ETF 真实 1m → `data/kline_etf_minute/date=YYYY-MM-DD/part.parquet`。
  - `sync_adj_factor()`：mootdx xdxr 事件重建逐日前复权因子 → 增量合并 `data/adj_factor_etf/all.parquet`。
  - `sync_daily(day)`：mootdx 回源全市场日线 → `data/kline_daily`（股票，volume 手）+ `data/kline_etf_daily`（ETF，volume 股）。**北交所（920xxx.BJ）mootdx 无数据，跳过**。
  - `sync_stock_minute(limit=None)`：回源 **4/1 起全市场 A 股分钟** → `data/kline_minute/date=*/`。每只拉一次全量（~3 个月 22560 bar），按交易日分组后**每攒满 100 只批量写分区**（避免逐只逐分区 IO）；北交所跳过。全市场 ~5200 只约 2.2 小时。`limit=N` 只处理前 N 只缺口（增量慢跑：resume 按**最新分区**跳过已覆盖，多轮后自动补齐）；调度场景传 `STOCK_MINUTE_BATCH_LIMIT`（20 只/批）。**收盘后最新分区覆盖率 < `STOCK_MINUTE_RESUME_COVERAGE`（默认 0.95）时忽略 limit 直接全量补齐**——15:35 全量回源被重启打断留下的残缺最新日（如 08-19 只写 3600/5209）靠这个自愈，否则增量 20 只/轮 + 残片检测跳过最新分区会让当天永久缺失。
  - `backfill_to_now()`：补齐到当前时间缺失的 ETF 分钟 + 全市场日线 + **一批股票分钟**（幂等，只补最新分区之后的交易日）。
- 触发：
  - **系统启动**：stock data 服务内 `scheduler` 后台线程调 `backfill_to_now()`（~13 分钟，不阻塞启动）——不再由主后端 lifespan 调用。
  - **收盘后**：stock data 服务内 `scheduler` 注册 15:35 cron（工作日）——ETF 分钟 + 因子表 + 一批股票分钟增量回源；主后端 `daily_pipeline` 不再注册 `mootdx_sync` cron。
  - **盘中实时**：无主动轮询（`mootdx_intraday` 已取消）——实时分钟只由客户端 `current_snapshot` 按需回源（见上节）。
- 量化侧不再直接读落盘分区（`DataManager`/`QuantDataProvider`/`live_feed` 已改走 `StockDataClient`，见上节）；模拟盘 `minute_realtime_backfill` 联网开关已删除。
- 失败标的（超时/异常/空）追加写入 `data/mootdx_sync_failures.csv`（symbol, 原因, 时间）；`get_minute`/`get_daily` 均有墙钟守护（30s/20s）防 socket 挂起卡死整批，超时重建 `MootdxSource`。
- `mootdx_src._is_index` 注意：**深市 000xxx 是股票**（000001 平安银行），仅沪市 000xxx 是指数。误判会把 SZ 000xxx 走 index_bars 返回空，触发全服务器轮换（每只 ~8.5s）。

## baostock 回源脚本（一次性全量回源）

- `backend/scripts/backfill_baostock_3y.py`（逻辑在 `backend/app/services/baostock_backfill.py`）：
  回源全市场近 3 年 **股票 5min 真实数据** → `data/kline_5min/date=YYYY-MM-DD/part.parquet`
  （baostock 无 1min/ETF分钟/指数分钟，实测 `frequency="1"` 返回错误）；
  ETF/指数**日线** → `kline_etf_daily` / `kline_index_daily`（指数 volume 股÷100 转手，
  ETF 不换算；baostock ETF 日线仅 2026-01-05 起）；复权因子（分红/送转/配股/缩股净效果，
  累计锚定最新=1.0 的事件行）→ `data/adj_factor_baostock/all.parquet`
  （DataManager 已接线读取，供回测前复权；不写 TickFlow 实时资产 `data/adj_factor/`）；
  分红送转明细 → `data/dividends/all.parquet`。
- 断点续传：`data/baostock_backfill_state.json`；`--retry-failed` 重试失败，
  `--reset-state` 清空重跑；失败记录 `data/baostock_backfill_failures.csv`。
- baostock 服务器吞吐波动大（单只 3 年 5min 实测 47s~100s+），串行执行，全量约几十小时，
  靠 resume 分多轮跑完。运行：`cd backend && uv run python scripts/backfill_baostock_3y.py [--stage minute|daily|corporate|all]`。

## 部署

- 单容器 Docker：`Dockerfile` 两阶段，前端 `dist` 拷进后端镜像，`docker compose up --build` 后访问 `http://localhost:3018`。进阶见 `docs/deployment.md`。
- CI：`.github/workflows/docker.yml` 推镜像到 ghcr（main/tag/手动）；`release.yml` 仅手动构建桌面客户端。
