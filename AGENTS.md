# AGENTS.md — tickflow-stock-panel

A 股「选股 + 监控 + 回测」量化工作台。后端 FastAPI + Polars/DuckDB/Parquet，前端 React 18 + Vite + TS。数据来自 TickFlow 数据源。

## 运行

- 一键前后端：`./dev.sh`（后端 `http://localhost:3018`，前端 `http://localhost:3011`）。`Ctrl-C` 同时关闭两端。
  - 改端口：`BACKEND_PORT=8000 ./dev.sh` / `FRONTEND_PORT=5173 ./dev.sh`。
  - 老 CPU（无 AVX2/FMA）：根 `.env` 设 `BACKEND_EXTRAS=legacy-cpu`（dev.sh 与 Docker 都会读取）。
  - `./dev.sh` 会 `kill` 掉已占用端口的进程，注意别误杀。
- 后台运行：`nohup ./dev.sh > /tmp/tickflow-dev.log 2>&1 &`
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
- 计算主力用 **Polars**；查询用 **DuckDB**；存储 **Parquet**。唯一 pandas 边界是回测（`vectorbt`，ADR-19），别在其它处引入 pandas。
- 回测依赖可选：`uv sync --extra backtest`（vectorbt 体积大、macOS/Intel 可能需现场编译）。
- 策略系统：`backend/app/strategy/`，18 个内置策略在 `builtin/`。扩展方式见 `docs/strategy.md` 与 `backend/app/strategy/prompts/strategy-guide.md`（AI 生成与手写规范）。
- 插件/数据源：`backend/app/plugins/`（内置 `stocksdk`）。第三方数据接入走 YAML，见 `docs/custom-data-source.md` 与 `docs/plugin-development.md`。
- 多数 `/api/*` 需鉴权（未登录返回 401）。

## mootdx 数据服务（quant/回测侧）

- `backend/app/services/mootdx_service.py`：独立于模拟盘的 mootdx 数据服务。
  - `sync_etf_minute(day)`：拉当日全部 ETF 真实 1m → `data/kline_etf_minute/date=YYYY-MM-DD/part.parquet`。
  - `sync_adj_factor()`：mootdx xdxr 事件重建逐日前复权因子 → 增量合并 `data/adj_factor_etf/all.parquet`。
  - `sync_daily(day)`：mootdx 回源全市场日线 → `data/kline_daily`（股票，volume 手）+ `data/kline_etf_daily`（ETF，volume 股）。**北交所（920xxx.BJ）mootdx 无数据，跳过**。
  - `sync_stock_minute(limit=None)`：回源 **4/1 起全市场 A 股分钟** → `data/kline_minute/date=*/`。每只拉一次全量（~3 个月 22560 bar），按交易日分组后**每攒满 100 只批量写分区**（避免逐只逐分区 IO）；北交所跳过。全市场 ~5200 只约 2.2 小时。`limit=N` 只处理前 N 只缺口（增量慢跑：resume 按**最新分区**跳过已覆盖，多轮后自动补齐）；调度场景传 `STOCK_MINUTE_BATCH_LIMIT`（20 只/批）。
  - `backfill_to_now()`：补齐到当前时间缺失的 ETF 分钟 + 全市场日线 + **一批股票分钟**（幂等，只补最新分区之后的交易日）。
- 触发：
  - **系统启动**：`app/main.py` lifespan 后台线程调 `backfill_to_now()`（~13 分钟，不阻塞启动）。
  - **收盘后**：`daily_pipeline.start_scheduler` 注册 `mootdx_sync` cron（工作日 15:35）——ETF 分钟 + 因子表 + 一批股票分钟增量回源。
- 回测/模拟盘只读落盘分区（`DataManager._adj_factor_map` / `_load_minute_from_partitions`），不联网。
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
