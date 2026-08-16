# 编译运行数据落 /tmp 设计

日期：2026-08-16
分支：feat/backtest-strategy-id（基于 custom-main）

## 背景

量化回测详情页的「编译运行」（quick 校验）与「开始回测」走同一接口（`POST /api/quant/backtest/run`），都在 quant.db 的 `backtest_runs` 落一条记录。需求：编译运行**不记录进回测历史**（历史下拉、列表页「最新回测周期」「回测次数」均不含），且**quant.db 零残留、不产生无法删除的积累行**（编译运行频繁，调试期尤甚）。

方案：编译运行数据落系统临时目录（`tempfile.gettempdir()`，Linux 为 /tmp）的**独立 SQLite 文件**，quant.db 完全不写入 compile 数据。

## 核心机制

### run_id 前缀路由

- 编译运行 run_id = `c_` + 8 位 hex（如 `c_3f2a91c4`）；主库 run_id 为 8 位纯 hex。
- 下划线是关键：主库 id 是 0-9a-f 纯 hex，`c_` 前缀与主库 id **保证零碰撞**。
- `db.get_conn(run_id)` 路由：`run_id` 以 `c_` 开头 → 连接编译库文件；否则连接主库。
- 跨进程唯一传递的信息就是 run_id（提交时 worker 仅拿到 run_id，前端之后也仅以 run_id 调接口），因此"只看字符串即可判断存储位置"，无需跨进程注册表或先查主库再回退。

### 编译库文件

- 路径：`Path(tempfile.gettempdir()) / "quant_compile" / f"{run_id}.db"`（跨平台，macOS/Linux 均为 /tmp）。
- Schema：编译库经 worker 侧 `db.init_db(db_path)` 初始化，实际含**主库完整 schema**（`backtest_runs`/`backtest_equity`/`backtest_trades`/`backtest_logs` 四个回测表，以及 strategies/sim_*/quant_settings 等主库同款空表）；编译运行实际用到的是前四张回测表。
- 首次连接懒创建（`CREATE TABLE IF NOT EXISTS` + 目录 mkdir）。

### worker / SSE / 前端路由

- worker（run_quant_backtest.py / rqalpha_bridge.py）：主库直读函数带 run_id 自动路由，唯一显式改动是 worker 侧桥接落库路径改为 `db.routed_db_path(run_id)`（`run_jq_backtest` / `run_backtest` 的 `db_path` 参数）——compile run 的 `c_` 前缀使其落到编译库文件。
- SSE 增量推送：rowid 偏移在单文件内单调，协议不受影响。
- 历史查询（`list_runs` / `list_strategies_with_latest`）只读主库 → compile 天然不可见。

## 改动清单

1. **backend/app/quant/db.py**
   - `get_conn(run_id: str | None = None)`：run_id 以 `c_` 开头 → 编译库文件（懒建 schema）；否则主库。现有 50 处无 run_id 调用不受影响。
   - 所有 per-run 函数内部 `with get_conn() as c` → `with get_conn(run_id) as c`（约 20 个：insert_run/upsert_run/update_run/set_run_pid/get_run、equity 5 个、trades 4 个、logs 5 个、delete_run、max_id 相关）。
   - 编译库轻量 schema 常量 + 懒初始化 helper。
2. **backend/app/quant/service.py**
   - `submit_backtest(params, compile_mode=False)`：compile_mode 时 run_id = `f"c_{uuid4().hex[:8]}"`（覆盖外部传入 run_id）。
   - 提交 compile 时清扫：删除 tempdir/quant_compile/ 下 mtime > 7 天的 `*.db`，以及 `CONFIG.bundle_dir` 下 mtime > 7 天的 `c_*` 目录（编译 bundle 目录兜底清理）。
3. **backend/app/quant/api/quant.py**
   - `BacktestIn` 加 `record: bool = True`（默认 True 向后兼容）。
   - `run_backtest`：`record` 从 params 中 pop（不写入 params_json、不传给 worker），`submit_backtest(params, compile_mode=not body.record)`。
4. **frontend/src/quant/pages/QuantBacktest.tsx**
   - 「编译运行」按钮 payload 加 `record: false`；「开始回测」不加（默认 true）。

## 边界情况

- **API 重启**：/tmp 文件仍在 → 编译运行结果可继续查看（运行中/结束后）。
- **OS 重启**：/tmp 清空 → 旧 runId 请求 404 → 前端错误态；编译记录本就是临时数据，可接受。
- **并发**：编译运行按钮运行中禁用（既有行为）；并发编译提交为不同 run_id 不同文件，互不干扰；清扫只删 7 天前文件。
- **terminate / delete_run**：按 run_id 路由，语义不变（compile 行 UI 不可见，接口直调亦正确）。
- **删除策略**：不删 run（既有行为），compile 文件由清扫兜底。
- **旧编译记录**：历史中已存在的旧 compile 行无法区分，保持原样（一次性，不迁移）。

## 验证

- 后端：`cd backend && uv run --extra dev pytest`（含新增测试）。
- 新增测试（tests/quant/）：
  - 路由：`get_conn('c_...')` 落编译库、纯 hex/无 run_id 落主库；编译运行写主库零残留。
  - API：`record=false` → 主库 list_runs/list_strategies_with_latest 不含；`record=true`（默认）→ 主库。
  - 清扫：7 天前文件被删、新文件保留。
- 前端：`cd frontend && pnpm build`（pnpm lint 仓库级预置失败，与改动无关）。
- 手测：编译运行实时曲线/日志正常、结束后可查看；历史下拉与列表页不出现编译运行。
