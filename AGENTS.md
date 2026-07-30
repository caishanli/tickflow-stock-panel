# AGENTS.md — tickflow-stock-panel

A 股「选股 + 监控 + 回测」量化工作台。后端 FastAPI + Polars + DuckDB，前端 React 18 + Vite + TS。数据来自 TickFlow 数据源。

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
- 计算主力用 **Polars**；查询用 **DuckDB**；存储 **DuckDB**（`data/tickflow.duckdb`），Parquet 用于分区导出。唯一 pandas 边界是回测（`vectorbt`，ADR-19），别在其它处引入 pandas。
- 回测依赖可选：`uv sync --extra backtest`（vectorbt 体积大、macOS/Intel 可能需现场编译）。
- 策略系统：`backend/app/strategy/`，18 个内置策略在 `builtin/`。扩展方式见 `docs/strategy.md` 与 `backend/app/strategy/prompts/strategy-guide.md`（AI 生成与手写规范）。
- 插件/数据源：`backend/app/plugins/`（内置 `stocksdk`）。第三方数据接入走 YAML，见 `docs/custom-data-source.md` 与 `docs/plugin-development.md`。
- 多数 `/api/*` 需鉴权（未登录返回 401）。

## 部署

- 单容器 Docker：`Dockerfile` 两阶段，前端 `dist` 拷进后端镜像，`docker compose up --build` 后访问 `http://localhost:3018`。进阶见 `docs/deployment.md`。
- CI：`.github/workflows/docker.yml` 推镜像到 ghcr（main/tag/手动）；`release.yml` 仅手动构建桌面客户端。
