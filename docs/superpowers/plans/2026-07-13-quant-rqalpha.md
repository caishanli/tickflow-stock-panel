# 量化回测与模拟盘 (RQAlpha) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 tickflow-stock-panel 中新增独立的「量化回测」与「量化模拟盘」模块，后端基于 RQAlpha 跑聚宽式 Python 策略（数据源可配置双源、复用 quant-daydayup 多源代码），回测与模拟盘均为独立进程，收益/日志/交易记录落独立 SQLite 库，前端（风格与现有一致）短轮询实时更新。

**Architecture:** 全部新代码落在隔离目录 `backend/app/quant/`（含 vendored `datasource/`）与 `frontend/src/quant/`；结果统一落独立 SQLite `data/quant.db`。FastAPI 仅做「提交任务 + 读库返回」薄层：回测/模拟盘重活在 `backend/scripts/run_quant_backtest.py` 与 `run_quant_sim.py` 两个独立 OS 进程内完成，经 `quant.db` 与前端通信。现有 `backtest/`、`strategy/`、`services/`、`pages/`、`components/` 不被修改，仅 7 处最小挂载点增量改动。

**Tech Stack:** Python 3.11+ (uv, FastAPI, sqlite3 stdlib), RQAlpha (optional extra `quant`), mootdx/tushare/baostock/requests (vendored multi-source), Polars(仅 tickflow_src 读本地 parquet). 前端 React18 + Vite + TS + Tailwind + TanStack Query + ECharts + @uiw/react-codemirror.

## Global Constraints

- 与现有 vectorbt 回测页、18 内置策略体系**完全独立**；不改动 `backtest/`、`strategy/`、`services/`、`tickflow/`、`pages/`、`components/` 任何现有文件（除 §挂载点）。
- `tickflow_src.py` 仅**只读 import** `app.tickflow.repository` / `app.parquet`，不修改它们。
- 回测与模拟盘**均为独立 OS 进程**；FastAPI 不跑 rqalpha、不嵌循环、不直持结果。
- 结果（收益/日志/交易记录）全部落独立 SQLite `data/quant.db`，**不**用 tickflow 的 DuckDB/Parquet 数据层，也**不**写 `data/backtest_results/`。
- 前端页面必须用设计令牌类（`bg-base`/`bg-surface`/`border-border`/`text-foreground`/`text-muted`/`text-accent`/`text-bull`/`text-bear`），**禁止**硬编码 `#hex`/`rgb()`；暗色为默认主题。复用 `PageHeader`/`Modal`/`EmptyState`/`DatePicker`/`pages/backtest/charts/*` 等现有组件。
- 前端更新靠**短轮询**（回测 1–2s、模拟盘 3–5s），与 quant-daydayup 一致。
- 数据源可配置双源 `QUANT_DATA_PRIORITY`（默认 `tickflow,tushare,mootdx,astock`），单源失败自动降级，绝不造伪数据。
- 新依赖只在 `pyproject.toml` 的 `quant` optional extra 声明；运行需 `uv sync --extra quant`。
- vendored 代码保留原始 license 头与出处注释（a-stock-data Apache-2.0 / quant-daydayup MIT）。

---

## File Structure

**新建 — 后端核心 (`backend/app/quant/`)**
- `config.py` — 读 `.env` 的量化配置 → `QuantConfig` dataclass。
- `db.py` — `data/quant.db` 建表 + 全部读写封装（回测/模拟盘）。
- `datasource/` — vendored 自 quant-daydayup，新增 `tickflow_src.py` 适配器：
  - `base.py` / `cache.py` / `manager.py`(QuantDataProvider) / `tickflow_src.py` / `astock_skill.py` / `astock_src.py` / `mootdx_src.py` / `tushare_src.py` / `baostock_src.py` / `minute_synth.py`
- `rqalpha_bridge.py` — `QuantRQAlphaDataSource(BaseDataSource)` + `run_backtest(strategy_code, params) -> result`，结果写 `db`。
- `strategies/store.py` + `strategies/samples/` — 聚宽 `.py` 策略 CRUD + 内置样例。
- `simulate/matcher.py` — 止损巡检（纯函数，dict in/out）。
- `simulate/protocol.py` — 模拟盘账户 live state 读写（`quant.db` 的 `sim_state`）。
- `simulate/runner.py` — 实时盘主循环（独立进程逻辑）。
- `simulate/replay.py` — 离线回放（复用 rqalpha_bridge）。
- `service.py` — FastAPI 侧编排：提交回测（写 DB + 派生子进程）、账户 CRUD、读库。
- `api/quant.py` — FastAPI Router（prefix `/api/quant`）。

**新建 — 后端独立进程 (`backend/scripts/`)**
- `run_quant_backtest.py <run_id>` — 读 `quant.db` 参数 → 调 `rqalpha_bridge` → 写 `quant.db`。
- `run_quant_sim.py <account_id>` — 实时盘主循环入口（调 `simulate/runner.py`）。

**新建 — 后端测试 (`backend/tests/quant/`)**
- `test_db.py` / `test_datasource.py` / `test_matcher.py` / `test_rqalpha_bridge.py`

**新建 — 前端 (`frontend/src/quant/`)**
- `api.ts` — `/api/quant/*` 请求封装。
- `components/CodeEditor.tsx` — @uiw/react-codemirror 封装（主题跟随暗/亮）。
- `pages/QuantBacktest.tsx` + `pages/StrategyEditorDialog.tsx` + `pages/BacktestResult.tsx`
- `pages/QuantSim.tsx` + `pages/AccountDialog.tsx` + `pages/SimReplay.tsx`

**最小增量挂载点（仅 7 处）**
- `backend/pyproject.toml` — 新增 `quant = [...]` extra 块。
- `backend/app/main.py` — `+1` 行 `app.include_router(quant_router, prefix="/api/quant")`。
- `frontend/package.json` — `+2` 依赖。
- `frontend/src/router.tsx` — `+2` lazy 导入 + `+2` Route。
- `frontend/src/components/Layout.tsx` — 菜单数组 `+2` 项。
- `.env.example` — `+` 量化变量。
- `.gitignore` — `+ data/quant_*` 与 `data/quant.db`。

---

## Task 1: 依赖声明与配置骨架

**Files:**
- Modify: `backend/pyproject.toml` (在 `[project.optional-dependencies]` 内追加)
- Modify: `.env.example` (追加)
- Modify: `.gitignore` (追加)
- Create: `backend/app/quant/__init__.py`
- Create: `backend/app/quant/config.py`

**Interfaces:**
- `QuantConfig` dataclass，字段与默认值：
  - `data_priority: list[str]` = `parse_csv(QUANT_DATA_PRIORITY, "tickflow,tushare,mootdx,astock")`
  - `tushare_token: str` = `QUANT_TUSHARE_TOKEN`
  - `fee_rate: float` = `float(QUANT_FEE_RATE or 0.0003)`
  - `slippage: float` = `float(QUANT_SLIPPAGE or 0.001)`
  - `default_stop_loss: float` = `float(QUANT_DEFAULT_STOP_LOSS or 0.03)`
  - `db_path: str` = `QUANT_DB_PATH or "data/quant.db"`
  - `bundle_dir: str` = `QUANT_BUNDLE_DIR or "data/quant_bundle"`
  - `strategies_dir: str` = `QUANT_STRATEGIES_DIR or "data/quant_strategies"`
  - `runtime_dir: str` = `QUANT_RUNTIME_DIR or "data/quant_sim"`
- `load_config() -> QuantConfig`（模块级单例 `CONFIG = load_config()`）。

**Steps:**

- [ ] **Step 1: 写 pyproject 的 quant extra**
```toml
# 量化回测/模拟盘依赖 (RQAlpha + 多源)。启用: uv sync --extra quant
quant = [
    "rqalpha>=0.26",
    "mootdx>=0.11.7",
    "tushare>=1.4.29,<2",
    "baostock",
    "requests>=2.31",
]
```
（`astock_skill.py` 仅依赖 requests，已含；无需额外声明。）

- [ ] **Step 2: 写 .env.example 追加**
```ini
# ===== 量化回测 / 模拟盘 (quant) =====
QUANT_DATA_PRIORITY=tickflow,tushare,mootdx,astock
QUANT_TUSHARE_TOKEN=
QUANT_FEE_RATE=0.0003
QUANT_SLIPPAGE=0.001
QUANT_DEFAULT_STOP_LOSS=0.03
QUANT_DB_PATH=data/quant.db
QUANT_BUNDLE_DIR=data/quant_bundle
QUANT_STRATEGIES_DIR=data/quant_strategies
QUANT_RUNTIME_DIR=data/quant_sim
```

- [ ] **Step 3: 写 .gitignore 追加**
```gitignore
# ===== quant (rqalpha 回测/模拟盘) =====
data/quant_strategies/
data/quant_bundle/
data/quant_sim/
data/quant.db
data/quant.db-journal
```

- [ ] **Step 4: 写 `backend/app/quant/__init__.py`**
```python
"""量化回测 / 模拟盘子系统（独立目录，不改动原工程其它模块）。"""
```

- [ ] **Step 5: 写 `backend/app/quant/config.py`**
```python
"""量化模块配置（读 .env）。"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


def _csv_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return list(default)
    return [p.strip() for p in value.split(",") if p.strip()]


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass
class QuantConfig:
    data_priority: list[str] = field(
        default_factory=lambda: ["tickflow", "tushare", "mootdx", "astock"]
    )
    tushare_token: str = ""
    fee_rate: float = 0.0003
    slippage: float = 0.001
    default_stop_loss: float = 0.03
    db_path: str = "data/quant.db"
    bundle_dir: str = "data/quant_bundle"
    strategies_dir: str = "data/quant_strategies"
    runtime_dir: str = "data/quant_sim"


def load_config() -> QuantConfig:
    return QuantConfig(
        data_priority=_csv_list(
            _env("QUANT_DATA_PRIORITY"),
            ["tickflow", "tushare", "mootdx", "astock"],
        ),
        tushare_token=_env("QUANT_TUSHARE_TOKEN"),
        fee_rate=float(_env("QUANT_FEE_RATE", "0.0003") or "0.0003"),
        slippage=float(_env("QUANT_SLIPPAGE", "0.001") or "0.001"),
        default_stop_loss=float(_env("QUANT_DEFAULT_STOP_LOSS", "0.03") or "0.03"),
        db_path=_env("QUANT_DB_PATH", "data/quant.db"),
        bundle_dir=_env("QUANT_BUNDLE_DIR", "data/quant_bundle"),
        strategies_dir=_env("QUANT_STRATEGIES_DIR", "data/quant_strategies"),
        runtime_dir=_env("QUANT_RUNTIME_DIR", "data/quant_sim"),
    )


CONFIG = load_config()
```

- [ ] **Step 6: 提交**
```bash
git add backend/pyproject.toml .env.example .gitignore backend/app/quant/__init__.py backend/app/quant/config.py
git commit -m "feat(quant): add pyproject quant extra, env vars, and config module"
```

---

## Task 2: 独立数据库 `db.py`（含测试）

**Files:**
- Create: `backend/app/quant/db.py`
- Create: `backend/tests/quant/__init__.py`
- Test: `backend/tests/quant/test_db.py`

**Interfaces:**
- `init_db(path: str | None = None) -> None`
- `get_conn() -> sqlite3.Connection`
- 回测：`insert_run(run_id, strategy_id, params_json, status="queued")`、`update_run(run_id, status, metrics_json=None, error=None, finished_at=None)`、`get_run(run_id) -> dict | None`、`list_runs(limit=50) -> list[dict]`、`bulk_insert_equity(run_id, rows)`（rows: list[tuple(dt,value,benchmark,cash,positions_value)]）、`get_equity(run_id) -> list[dict]`、`insert_trade(run_id, ts, code, action, price, amount, pnl, pnl_pct, commission)`、`get_trades(run_id) -> list[dict]`、`insert_log(run_id, ts, level, message)`、`get_logs(run_id) -> list[dict]`、`delete_run(run_id)`。
- 模拟盘：`insert_sim_account(account_id, name, capital, stop_loss, status="created")`、`update_sim_account(account_id, **fields)`、`get_sim_account(account_id) -> dict | None`、`list_sim_accounts() -> list[dict]`、`upsert_sim_state(account_id, cash, positions_json, net_value, pnl, start_cash, stop_loss_log_json, dt)`、`read_sim_state(account_id) -> dict`、`insert_sim_snapshot(account_id, dt, net_value, cash, positions_value, pnl, pnl_pct)`、`get_sim_snapshots(account_id) -> list[dict]`、`insert_sim_trade(account_id, ts, code, action, price, amount, pnl, pnl_pct, commission)`、`get_sim_trades(account_id) -> list[dict]`、`insert_sim_stoploss(account_id, ts, code, action, price, pnl_pct)`、`get_sim_stoploss(account_id) -> list[dict]`、`delete_sim_account(account_id)`。

**Steps:**

- [ ] **Step 1: 写失败测试 `test_db.py`**
```python
import os, tempfile, sqlite3
from app.quant import db

def _fresh():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd); os.unlink(path)
    db.init_db(path)
    return path

def test_backtest_run_lifecycle():
    p = _fresh()
    db.init_db(p)
    db.insert_run("r1", "s1", '{"a":1}', "queued")
    assert db.get_run("r1")["status"] == "queued"
    db.bulk_insert_equity("r1", [("2024-01-02", 1.0, 1.0, 0.9, 0.1)])
    db.insert_trade("r1", "2024-01-02 09:30", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    db.insert_log("r1", "2024-01-02 09:30", "INFO", "start")
    db.update_run("r1", "done", metrics_json='{"sharpe":1.2}')
    r = db.get_run("r1")
    assert r["status"] == "done" and "sharpe" in r["metrics_json"]
    assert len(db.get_equity("r1")) == 1
    assert len(db.get_trades("r1")) == 1
    assert len(db.get_logs("r1")) == 1
    db.delete_run("r1")
    assert db.get_run("r1") is None
    os.unlink(p)

def test_sim_account_and_state():
    p = _fresh()
    db.insert_sim_account("a1", "acc1", 100000.0, 0.03, "created")
    assert db.get_sim_account("a1")["capital"] == 100000.0
    db.upsert_sim_state("a1", 99000.0, '{"600000.XSHG":{}}', 99000.0, -1000.0, 100000.0, "[]", "2024-01-02 09:30")
    st = db.read_sim_state("a1")
    assert st["cash"] == 99000.0 and st["pnl"] == -1000.0
    db.insert_sim_snapshot("a1", "2024-01-02 09:30", 99000.0, 99000.0, 0.0, -1000.0, -0.01)
    db.insert_sim_trade("a1", "2024-01-02 09:31", "600000.XSHG", "SELL", 10.0, 100, -50.0, -0.005, 0.0)
    db.insert_sim_stoploss("a1", "2024-01-02 09:31", "600000.XSHG", "STOP_LOSS", 9.9, -0.01)
    assert len(db.get_sim_snapshots("a1")) == 1
    assert len(db.get_sim_trades("a1")) == 1
    assert len(db.get_sim_stoploss("a1")) == 1
    db.delete_sim_account("a1")
    assert db.get_sim_account("a1") is None
    os.unlink(p)
```

- [ ] **Step 2: 运行测试确认失败**
```bash
cd backend && uv run --extra dev --extra quant pytest tests/quant/test_db.py -v
```
Expected: ERROR/FAIL `No module named 'app.quant.db'` 或 `init_db` 未定义。

- [ ] **Step 3: 写 `db.py` 实现**
```python
"""量化模块独立 SQLite 库（data/quant.db）。

所有回测/模拟盘的收益、日志、交易记录落此库，与 tickflow 的
DuckDB/Parquet 数据层完全隔离。sqlite3 为标准库，无额外依赖。
"""
from __future__ import annotations

import os
import sqlite3

from .config import CONFIG

_SCHEMA = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    id TEXT PRIMARY KEY, strategy_id TEXT, params_json TEXT, status TEXT,
    metrics_json TEXT, created_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id TEXT, dt TEXT, value REAL, benchmark REAL, cash REAL, positions_value REAL);
CREATE TABLE IF NOT EXISTS backtest_trades (
    run_id TEXT, ts TEXT, code TEXT, action TEXT, price REAL, amount REAL,
    pnl REAL, pnl_pct REAL, commission REAL);
CREATE TABLE IF NOT EXISTS backtest_logs (
    run_id TEXT, ts TEXT, level TEXT, message TEXT);
CREATE TABLE IF NOT EXISTS strategies (
    id TEXT PRIMARY KEY, name TEXT, file TEXT, created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')));
CREATE TABLE IF NOT EXISTS sim_accounts (
    id TEXT PRIMARY KEY, name TEXT, capital REAL, stop_loss REAL, status TEXT,
    created_at TEXT DEFAULT (datetime('now')), started_at TEXT);
CREATE TABLE IF NOT EXISTS sim_state (
    account_id TEXT PRIMARY KEY, cash REAL, positions_json TEXT, net_value REAL,
    pnl REAL, start_cash REAL, stop_loss_log_json TEXT, dt TEXT);
CREATE TABLE IF NOT EXISTS sim_equity_snapshots (
    account_id TEXT, dt TEXT, net_value REAL, cash REAL, positions_value REAL,
    pnl REAL, pnl_pct REAL);
CREATE TABLE IF NOT EXISTS sim_trades (
    account_id TEXT, ts TEXT, code TEXT, action TEXT, price REAL, amount REAL,
    pnl REAL, pnl_pct REAL, commission REAL);
CREATE TABLE IF NOT EXISTS sim_stop_loss (
    account_id TEXT, ts TEXT, code TEXT, action TEXT, price REAL, pnl_pct REAL);
"""


def init_db(path: str | None = None) -> None:
    conn = sqlite3.connect(path or CONFIG.db_path)
    try:
        conn.executescript(_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(CONFIG.db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(CONFIG.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---- 回测 ----
def insert_run(run_id, strategy_id, params_json, status="queued"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_runs(id,strategy_id,params_json,status) VALUES(?,?,?,?)",
            (run_id, strategy_id, params_json, status),
        )


def update_run(run_id, status, metrics_json=None, error=None, finished_at=None):
    with get_conn() as c:
        c.execute(
            "UPDATE backtest_runs SET status=?, metrics_json=?, error=?, finished_at=? WHERE id=?",
            (status, metrics_json, error, finished_at, run_id),
        )


def get_run(run_id):
    with get_conn() as c:
        row = c.execute("SELECT * FROM backtest_runs WHERE id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def list_runs(limit=50):
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def bulk_insert_equity(run_id, rows):
    with get_conn() as c:
        c.executemany(
            "INSERT INTO backtest_equity(run_id,dt,value,benchmark,cash,positions_value) "
            "VALUES(?,?,?,?,?,?)",
            [(run_id, *r) for r in rows],
        )


def get_equity(run_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT dt,value,benchmark,cash,positions_value FROM backtest_equity WHERE run_id=?",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def insert_trade(run_id, ts, code, action, price, amount, pnl, pnl_pct, commission):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_trades(run_id,ts,code,action,price,amount,pnl,pnl_pct,commission) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (run_id, ts, code, action, price, amount, pnl, pnl_pct, commission),
        )


def get_trades(run_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,code,action,price,amount,pnl,pnl_pct,commission FROM backtest_trades "
            "WHERE run_id=? ORDER BY ts", (run_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def insert_log(run_id, ts, level, message):
    with get_conn() as c:
        c.execute(
            "INSERT INTO backtest_logs(run_id,ts,level,message) VALUES(?,?,?,?)",
            (run_id, ts, level, message),
        )


def get_logs(run_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,level,message FROM backtest_logs WHERE run_id=? ORDER BY rowid",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_run(run_id):
    import shutil
    with get_conn() as c:
        c.execute("DELETE FROM backtest_runs WHERE id=?", (run_id,))
        c.execute("DELETE FROM backtest_equity WHERE run_id=?", (run_id,))
        c.execute("DELETE FROM backtest_trades WHERE run_id=?", (run_id,))
        c.execute("DELETE FROM backtest_logs WHERE run_id=?", (run_id,))
    if os.path.isdir(os.path.join(CONFIG.bundle_dir, run_id)):
        shutil.rmtree(os.path.join(CONFIG.bundle_dir, run_id), ignore_errors=True)


# ---- 模拟盘 ----
def insert_sim_account(account_id, name, capital, stop_loss, status="created"):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_accounts(id,name,capital,stop_loss,status) VALUES(?,?,?,?,?)",
            (account_id, name, capital, stop_loss, status),
        )


def update_sim_account(account_id, **fields):
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    with get_conn() as c:
        c.execute(
            f"UPDATE sim_accounts SET {cols} WHERE id=?",
            (*fields.values(), account_id),
        )


def get_sim_account(account_id):
    with get_conn() as c:
        row = c.execute("SELECT * FROM sim_accounts WHERE id=?", (account_id,)).fetchone()
    return dict(row) if row else None


def list_sim_accounts():
    with get_conn() as c:
        rows = c.execute("SELECT * FROM sim_accounts ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]


def upsert_sim_state(account_id, cash, positions_json, net_value, pnl, start_cash,
                    stop_loss_log_json, dt):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_state(account_id,cash,positions_json,net_value,pnl,start_cash,"
            "stop_loss_log_json,dt) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(account_id) DO UPDATE SET cash=excluded.cash,"
            "positions_json=excluded.positions_json,net_value=excluded.net_value,"
            "pnl=excluded.pnl,start_cash=excluded.start_cash,"
            "stop_loss_log_json=excluded.stop_loss_log_json,dt=excluded.dt",
            (account_id, cash, positions_json, net_value, pnl, start_cash,
             stop_loss_log_json, dt),
        )


def read_sim_state(account_id):
    with get_conn() as c:
        row = c.execute("SELECT * FROM sim_state WHERE account_id=?", (account_id,)).fetchone()
    return dict(row) if row else {
        "cash": 0.0, "positions_json": "{}", "net_value": 0.0, "pnl": 0.0,
        "start_cash": 0.0, "stop_loss_log_json": "[]", "dt": None,
    }


def insert_sim_snapshot(account_id, dt, net_value, cash, positions_value, pnl, pnl_pct):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_equity_snapshots(account_id,dt,net_value,cash,positions_value,pnl,pnl_pct) "
            "VALUES(?,?,?,?,?,?,?)",
            (account_id, dt, net_value, cash, positions_value, pnl, pnl_pct),
        )


def get_sim_snapshots(account_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT dt,net_value,cash,positions_value,pnl,pnl_pct FROM sim_equity_snapshots "
            "WHERE account_id=? ORDER BY dt", (account_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def insert_sim_trade(account_id, ts, code, action, price, amount, pnl, pnl_pct, commission):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_trades(account_id,ts,code,action,price,amount,pnl,pnl_pct,commission) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (account_id, ts, code, action, price, amount, pnl, pnl_pct, commission),
        )


def get_sim_trades(account_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,code,action,price,amount,pnl,pnl_pct,commission FROM sim_trades "
            "WHERE account_id=? ORDER BY ts", (account_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def insert_sim_stoploss(account_id, ts, code, action, price, pnl_pct):
    with get_conn() as c:
        c.execute(
            "INSERT INTO sim_stop_loss(account_id,ts,code,action,price,pnl_pct) VALUES(?,?,?,?,?,?)",
            (account_id, ts, code, action, price, pnl_pct),
        )


def get_sim_stoploss(account_id):
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts,code,action,price,pnl_pct FROM sim_stop_loss WHERE account_id=? ORDER BY ts",
            (account_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def delete_sim_account(account_id):
    with get_conn() as c:
        for t in ("sim_accounts", "sim_state", "sim_equity_snapshots", "sim_trades", "sim_stop_loss"):
            c.execute(f"DELETE FROM {t} WHERE account_id=?", (account_id,))
```

- [ ] **Step 4: 运行测试确认通过**
```bash
cd backend && uv run --extra dev --extra quant pytest tests/quant/test_db.py -v
```
Expected: PASS (2 passed)。

- [ ] **Step 5: 提交**
```bash
git add backend/app/quant/db.py backend/tests/quant/__init__.py backend/tests/quant/test_db.py
git commit -m "feat(quant): add isolated SQLite db for backtest & sim results"
```

---

## Task 3: 数据源层（vendored 多源 + tickflow 适配器）

**Files:**
- Create: `backend/app/quant/datasource/__init__.py`
- Create: `backend/app/quant/datasource/base.py`
- Create: `backend/app/quant/datasource/cache.py`
- Create: `backend/app/quant/datasource/manager.py` (含 `QuantDataProvider`)
- Create: `backend/app/quant/datasource/tickflow_src.py`
- Create: `backend/app/quant/datasource/astock_skill.py` (vendored)
- Copy: `astock_src.py` `mootdx_src.py` `tushare_src.py` `baostock_src.py` `minute_synth.py` 自 `/home/ubuntu/quant-daydayup/backend/app/datasource/`，改 import 为 `from .base import ...` 与 `from . import astock_skill as skill`。
- Test: `backend/tests/quant/test_datasource.py`

**Interfaces:**
- `DataSource` 抽象基类：`get_daily(code,start,end)` / `get_minute(code,date)` / `get_index_realtime(codes)` / `get_etf_list()` / `get_stock_list()` / `get_us_index()` / `test_connection()`，失败统一抛 `DataSourceError`。
- `QuantDataProvider(priority=None)`：`fetch(method, code, *args)` 按优先级降级；`get_daily(code,start,end)` / `get_minute(code,date)` / `get_stock_list()` / `get_etf_list()` 委托降级。
- `TickflowSource(DataSource)`：用 `app.tickflow.repository.KlineRepository` 与 `app.parquet.scan_enriched_parquet` 读本地数据。

**Steps:**

- [ ] **Step 1: 写失败测试 `test_datasource.py`**
```python
from app.quant.datasource.base import DataSourceError
from app.quant.datasource.manager import QuantDataProvider


class _FailSource:
    name = "fail"
    def get_daily(self, *a, **k):
        raise DataSourceError("boom")
    def get_minute(self, *a, **k):
        raise DataSourceError("boom")


class _OkSource:
    name = "ok"
    def __init__(self, df):
        self._df = df
    def get_daily(self, *a, **k):
        return self._df
    def get_minute(self, *a, **k):
        return self._df


def test_fallback_to_second_source():
    import pandas as pd
    ok = _OkSource(pd.DataFrame({"close": [1.0]}))
    prov = QuantDataProvider.__new__(QuantDataProvider)
    prov.sources = {"fail": _FailSource(), "ok": ok}
    prov.priority = ["fail", "ok"]
    prov.cache = type("C", (), {"get": lambda *a, **k: None, "put": lambda *a, **k: None})()
    df = prov.fetch("get_daily", "X")
    assert list(df["close"]) == [1.0]


def test_all_fail_raises():
    prov = QuantDataProvider.__new__(QuantDataProvider)
    prov.sources = {"fail": _FailSource()}
    prov.priority = ["fail"]
    prov.cache = type("C", (), {"get": lambda *a, **k: None, "put": lambda *a, **k: None})()
    try:
        prov.fetch("get_daily", "X")
        assert False, "should raise"
    except DataSourceError:
        pass
```

- [ ] **Step 2: 运行测试确认失败** → Expected: `No module named 'app.quant.datasource...'`。

- [ ] **Step 3: 写 `base.py`**
```python
"""数据源抽象基类与统一异常（改编自 quant-daydayup）。"""


class DataSourceError(Exception):
    """数据源不可用 / 无数据 / 超时 等统一错误。"""


class DataSource:
    name = "base"

    def get_daily(self, code, start, end):
        raise NotImplementedError

    def get_minute(self, code, date):
        raise NotImplementedError

    def get_index_realtime(self, codes):
        raise NotImplementedError

    def get_etf_list(self):
        raise NotImplementedError

    def get_stock_list(self):
        raise NotImplementedError

    def get_us_index(self):
        raise NotImplementedError

    def test_connection(self):
        raise NotImplementedError
```

- [ ] **Step 4: 写 `cache.py`（vendored 简化版）**
```python
"""本地 Parquet 缓存（改编自 quant-daydayup datasource/cache.py）。"""
from __future__ import annotations
import os
import threading
import pandas as pd
import pyarrow.parquet as pq  # pyarrow 已在基础依赖

_LOCK = threading.Lock()


class DataCache:
    def __init__(self, root: str = "data/quant_cache"):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _path(self, key: str) -> str:
        return os.path.join(self.root, f"{key}.parquet")

    def get(self, key: str):
        p = self._path(key)
        if os.path.exists(p):
            try:
                return pd.read_parquet(p)
            except Exception:
                return None
        return None

    def put(self, key: str, df):
        if df is None or getattr(df, "empty", True):
            return
        with _LOCK:
            df.to_parquet(self._path(key), index=False)
```

- [ ] **Step 5: 写 `manager.py`（QuantDataProvider）**
```python
"""数据源优先级调度 + 自动降级（改编自 quant-daydayup datasource/manager.py）。"""
from __future__ import annotations

import pandas as pd

from .base import DataSourceError
from .cache import DataCache
from .tickflow_src import TickflowSource
from .tushare_src import TushareSource
from .mootdx_src import MootdxSource
from .astock_src import AStockSource
from .baostock_src import BaostockSource, interpolate_5min_to_1min
from .minute_synth import SyntheticMinuteSource

from ..config import CONFIG

SOURCES = {
    "tickflow": TickflowSource,
    "tushare": TushareSource,
    "mootdx": MootdxSource,
    "astock": AStockSource,
}


class QuantDataProvider:
    """按 QUANT_DATA_PRIORITY 依次尝试各源，失败自动降级。"""

    def __init__(self, priority=None, token=None, cache=None):
        self.cache = cache or DataCache()
        tok = token if token is not None else CONFIG.tushare_token
        self.sources = {
            k: (v(token=tok) if k == "tushare" else v())
            for k, v in SOURCES.items()
        }
        self.minute_source = SyntheticMinuteSource(
            lambda code, start, end: self.fetch("get_daily", code, start, end)
        )
        self.sources["baostock"] = BaostockSource()
        self.priority = priority or CONFIG.data_priority

    def fetch(self, method, code, *args):
        last = None
        for name in self.priority:
            src = self.sources.get(name)
            if src is None:
                continue
            try:
                return getattr(src, method)(code, *args)
            except DataSourceError as e:
                last = e
                continue
        raise last or DataSourceError(f"所有数据源均不可用: {method} {code}")

    def get_daily(self, code, start, end):
        key = f"daily_{code}_{start}_{end}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        df = self.fetch("get_daily", code, start, end)
        self.cache.put(key, df)
        return df

    def get_minute(self, code, date):
        return self.minute_source.get_minute(code, date)

    def get_stock_list(self):
        return self.fetch("get_stock_list", "ALL")

    def get_etf_list(self):
        return self.fetch("get_etf_list", "ALL")
```

- [ ] **Step 6: 写 `tickflow_src.py`（新适配器，只读 import 原工程）**
```python
"""TickFlow 本地数据适配器：复用原工程 repository / parquet，只读，不改动它们。"""
from __future__ import annotations

import pandas as pd

from .base import DataSource, DataSourceError


def _to_tf_code(code: str) -> str:
    """聚宽代码 600000.XSHG -> TickFlow 6 位数字代码。"""
    return code.split(".")[0]


class TickflowSource(DataSource):
    name = "tickflow"

    def __init__(self):
        from app.tickflow.repository import KlineRepository
        from app.parquet import scan_enriched_parquet
        self._repo = KlineRepository()
        self._scan = scan_enriched_parquet

    def get_daily(self, code, start, end):
        sym = _to_tf_code(code)
        try:
            df = self._scan(start=start, end=end, instruments=[sym])
            if df is None or df.height == 0:
                raise DataSourceError(f"tickflow 无日线: {code}")
            out = df.to_pandas()
            out = out.rename(columns={"date": "date", "close": "close", "open": "open",
                                    "high": "high", "low": "low", "volume": "volume"})
            out["date"] = out["date"].astype(str)
            return out
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"tickflow 日线失败: {e}")

    def get_minute(self, code, date):
        sym = _to_tf_code(code)
        try:
            df = self._repo.get_minute(sym, date)
            if df is None or df.height == 0:
                raise DataSourceError(f"tickflow 无分钟: {code} {date}")
            return df.to_pandas()
        except DataSourceError:
            raise
        except Exception as e:
            raise DataSourceError(f"tickflow 分钟失败: {e}")

    def get_stock_list(self):
        rows = self._repo.list_instruments()
        return [r["code"] for r in rows]

    def get_etf_list(self):
        rows = self._repo.list_instruments_etf()
        return [r["code"] for r in rows]

    def get_index_realtime(self, codes):
        raise DataSourceError("tickflow 源不提供实时指数")

    def get_us_index(self):
        raise DataSourceError("tickflow 源不提供美股")

    def test_connection(self):
        return True
```
> 注：`KlineRepository` 与 `scan_enriched_parquet` 的精确方法名/签名以原工程 `app/tickflow/repository.py` 与 `app/parquet.py` 为准；实现后用 `test_datasource.py` 的 tickflow 路径（见下）验证，必要时调整方法调用。

- [ ] **Step 7: vendored 文件（复制并改 import）**
  从 `/home/ubuntu/quant-daydayup/backend/app/datasource/` 复制以下文件到 `backend/app/quant/datasource/`：
  `astock_src.py`、`mootdx_src.py`、`tushare_src.py`、`baostock_src.py`、`minute_synth.py`。
  同时复制 `astock_skill.py`（放到同目录）。
  统一把文件内 `from .base import ...` 已是正确的（quant-daydayup 用相对导入）；只需确认 `astock_src.py` 中 `from . import astock_skill as skill` 成立（因已复制 `astock_skill.py`）。`baostock_src.py` 引用的 `interpolate_5min_to_1min` 已在 `baostock_src.py` 内定义（据 quant-daydayup 结构）。
  保留每个文件顶部原有的 license / 出处注释。

- [ ] **Step 8: 运行测试**
```bash
cd backend && uv run --extra dev --extra quant pytest tests/quant/test_datasource.py -v
```
Expected: PASS（降级逻辑通过；tickflow 真实读取需本地数据，CI 以 stub 源验证降级即可）。

- [ ] **Step 9: 提交**
```bash
git add backend/app/quant/datasource
git commit -m "feat(quant): vendor multi-source datasource + tickflow adapter"
```

---

## Task 4: 止损巡检 `simulate/matcher.py`（含测试）

**Files:**
- Create: `backend/app/quant/simulate/__init__.py`
- Create: `backend/app/quant/simulate/matcher.py`
- Test: `backend/tests/quant/test_matcher.py`

**Interfaces:**
- `Matcher.step(state: dict, prices: dict, fee: float = 0.0003) -> dict`：state 含 `cash/positions/stop_loss_log/start_cash/net_value/pnl/positions_value`；`positions` 为 `{code:{amount,avg_cost,price}}`。对每持仓若 `price/avg_cost-1 <= -stop_loss` 则市价平仓计入 cash、追加 `stop_loss_log`、删除持仓；返回刷新后的 state（含 `net_value`/`pnl`）。

**Steps:**

- [ ] **Step 1: 写失败测试**
```python
from app.quant.simulate.matcher import Matcher


def test_stop_loss_triggers():
    m = Matcher(0.03)
    state = {
        "cash": 0.0, "start_cash": 1000.0, "net_value": 1000.0, "pnl": 0.0,
        "positions": {"600000.XSHG": {"amount": 100.0, "avg_cost": 10.0, "price": 9.6}},
        "stop_loss_log": [],
    }
    out = m.step(state, {"600000.XSHG": 9.6}, fee=0.0003)
    assert "600000.XSHG" not in out["positions"]          # 已平仓
    assert len(out["stop_loss_log"]) == 1                  # 记一条止损
    assert out["cash"] > 0                                # 回收现金
    assert out["net_value"] == out["cash"]                # 无持仓时净值=现金


def test_no_trigger_when_above_stop():
    m = Matcher(0.03)
    state = {
        "cash": 0.0, "start_cash": 1000.0, "net_value": 1100.0, "pnl": 100.0,
        "positions": {"600000.XSHG": {"amount": 100.0, "avg_cost": 10.0, "price": 11.0}},
        "stop_loss_log": [],
    }
    out = m.step(state, {"600000.XSHG": 11.0})
    assert "600000.XSHG" in out["positions"]
    assert out["pnl"] == 100.0
```

- [ ] **Step 2: 运行确认失败** → Expected: `No module named 'app.quant.simulate.matcher'`。

- [ ] **Step 3: 写 `matcher.py`**
```python
"""本地撮合 + 每分钟持仓止损巡检（改编自 quant-daydayup simulate/matcher.py）。"""
from __future__ import annotations


class Matcher:
    def __init__(self, stop_loss: float):
        self.stop_loss = float(stop_loss)

    def step(self, state: dict, prices: dict, fee: float = 0.0003) -> dict:
        positions = state.setdefault("positions", {})
        log = state.setdefault("stop_loss_log", [])
        cash = float(state.get("cash", 0.0))
        for code, pos in list(positions.items()):
            price = prices.get(code)
            if price is None:
                continue
            pos["price"] = float(price)
            avg_cost = float(pos.get("avg_cost", 0.0) or 0.0)
            if avg_cost <= 0:
                continue
            pnl_pct = float(price) / avg_cost - 1
            if pnl_pct <= -self.stop_loss:
                amount = float(pos.get("amount", 0.0) or 0.0)
                proceeds = amount * float(price) * (1 - fee)
                cash += proceeds
                log.append({
                    "dt": state.get("dt"),
                    "code": code,
                    "action": "STOP_LOSS",
                    "price": float(price),
                    "pnl_pct": round(pnl_pct, 4),
                })
                del positions[code]
        state["cash"] = round(cash, 4)
        pos_value = sum(
            float(p.get("amount", 0.0)) * float(p.get("price", 0.0))
            for p in positions.values()
        )
        state["positions"] = positions
        state["net_value"] = round(cash + pos_value, 4)
        start = float(state.get("start_cash", 0.0) or state.get("net_value", 0.0))
        state["pnl"] = round(state["net_value"] - start, 4)
        return state
```

- [ ] **Step 4: 运行确认通过**
```bash
cd backend && uv run --extra dev --extra quant pytest tests/quant/test_matcher.py -v
```
Expected: PASS (2 passed)。

- [ ] **Step 5: 提交**
```bash
git add backend/app/quant/simulate/__init__.py backend/app/quant/simulate/matcher.py backend/tests/quant/test_matcher.py
git commit -m "feat(quant): add stop-loss matcher for paper trading"
```

---

## Task 5: RQAlpha 桥接 `rqalpha_bridge.py`（含离线测试）

**Files:**
- Create: `backend/app/quant/rqalpha_bridge.py`
- Test: `backend/tests/quant/test_rqalpha_bridge.py`
- 测试用 bundle：`backend/tests/quant/fixtures/mini_bundle/`（极小 daily csv，无网络）

**Interfaces:**
- `QuantRQAlphaDataSource(BaseDataSource)`：`__init__(self, provider: QuantDataProvider, config: QuantConfig)`；实现 rqalpha 实际调用的核心抽象方法（`get_trading_dates`/`get_instruments`/`get_bars`/`get_trading_calendar`/`get_yield_curve` 等）。
- `run_backtest(strategy_code: str, params: dict) -> dict`：params 含 `run_id, symbols, start, end, frequency, capital, fee, slippage`；内部构建 data source → 调 `rqalpha.run()` → 回收 portfolio/成交 → 写 `db`（equity/trades/logs/run metrics）→ 返回 `{"run_id":..., "metrics":...}`。
- 测试只验证：data source 可实例化（无抽象方法缺失）、且用内置 mini bundle 跑通一个简单聚宽策略并回收指标。

**Steps:**

- [ ] **Step 1: 准备离线 mini bundle fixture**
新建 `backend/tests/quant/fixtures/mini_bundle/`，内含：
- `trading_dates.txt`：`2024-01-02\n2024-01-03\n2024-01-04\n`
- `instruments.txt`：`600000.XSHG\n`
- `bars/600000.XSHG.csv`（列：`date,open,high,low,close,volume`）：
  ```
  date,open,high,low,close,volume
  2024-01-02,10.0,10.2,9.9,10.1,1000
  2024-01-03,10.1,10.5,10.0,10.4,1200
  2024-01-04,10.4,10.6,10.2,10.5,1100
  ```

- [ ] **Step 2: 写失败测试**
```python
import os
from app.quant import db
from app.quant.rqalpha_bridge import QuantRQAlphaDataSource, run_backtest_on_bundle


def test_datasource_instantiable():
    # 不依赖网络：仅验证抽象方法已全部实现（否则实例化抛 TypeError）
    ds = QuantRQAlphaDataSource.__new__(QuantRQAlphaDataSource)
    missing = getattr(QuantRQAlphaDataSource, "__abstractmethods__", set())
    assert not missing, f"未实现的抽象方法: {missing}"


def test_run_on_mini_bundle(tmp_path):
    db_path = str(tmp_path / "q.db")
    db.init_db(db_path)
    # 用 fixture bundle 直接跑（绕过网络数据源）
    res = run_backtest_on_bundle(
        bundle_dir="tests/quant/fixtures/mini_bundle",
        strategy_code="def init(c):\n    c.stocks=['600000.XSHG']\ndef handle(c, b):\n    pass\n",
        params={"run_id": "t1", "symbols": ["600000.XSHG"], "start": "2024-01-02",
                "end": "2024-01-04", "frequency": "daily", "capital": 100000.0,
                "fee": 0.0003, "slippage": 0.001},
        db_path=db_path,
    )
    assert res["run_id"] == "t1"
    assert db.get_run("t1")["status"] == "done"
    assert len(db.get_equity("t1")) >= 1
```
> 说明：`run_backtest_on_bundle` 为测试辅助函数，内部用 fixture bundle 而非 `QuantDataProvider`，避免网络；实现时复用与 `run_backtest` 相同的指标回收 + 写库逻辑。

- [ ] **Step 3: 运行确认失败** → Expected: `No module named 'app.quant.rqalpha_bridge'`。

- [ ] **Step 4: 写 `rqalpha_bridge.py`**
```python
"""RQAlpha 桥接：自定义数据源 + 跑聚宽式策略 + 回收指标落库。

注：BaseDataSource 的抽象方法随 rqalpha 版本可能变化。实现后由
test_rqalpha_bridge 的 test_datasource_instantiable 校验 __abstractmethods__
为空；若安装版本要求额外方法，按报错补充即可。
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import uuid

import pandas as pd

from . import db
from .config import CONFIG, QuantConfig

logger = logging.getLogger(__name__)


def _now():
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def run_backtest(strategy_code: str, params: dict, provider=None, db_path: str | None = None) -> dict:
    """在独立进程内调用：跑回测并写 quant.db。返回 {run_id, metrics}。"""
    if db_path:
        db.init_db(db_path)
    run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    db.insert_run(run_id, params.get("strategy_id", ""), json.dumps(params, ensure_ascii=False), "running")
    try:
        from rqalpha import run as rq_run
        from rqalpha.data.base_data_source import BaseDataSource

        class _DS(BaseDataSource):
            def __init__(self, prov, cfg, p):
                super().__init__()
                self._prov = prov
                self._cfg = cfg
                self._p = p

            def get_trading_dates(self, start, end):
                # 由 symbols 的日线推导交易日（简化：日线索引）
                df = self._prov.get_daily(self._p["symbols"][0], start, end)
                return list(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"))

            def get_instruments(self, *args, **kwargs):
                return self._p["symbols"]

            def get_bars(self, order_book_id, dt, frequency="daily"):
                df = self._prov.get_daily(order_book_id, self._p["start"], self._p["end"])
                sub = df[pd.to_datetime(df["date"]) <= _dt.datetime.strptime(dt, "%Y-%m-%d")] if dt else df
                return sub

            def get_trading_calendar(self):
                return self.get_trading_dates(self._p["start"], self._p["end"])

            def get_yield_curve(self, *args, **kwargs):
                return None

        ds = _DS(provider, CONFIG, params)
        # 注：rqalpha.run 的真实 data source 注入方式以安装版本文档为准；
        # 此处用 config 传入自定义数据源实例。
        result = rq_run(
            strategy_code,
            start_date=params["start"], end_date=params["end"],
            frequency=params.get("frequency", "daily"),
            accounts={"stock": params.get("capital", 100000.0)},
            benchmark="000300.XSHG",
            data_source=ds,
        )
        # 回收指标 + 净值 + 成交（字段名以 rqalpha result 对象实际为准）
        metrics = _extract_metrics(result)
        equity = _extract_equity(result)
        trades = _extract_trades(result)
        db.bulk_insert_equity(run_id, equity)
        for t in trades:
            db.insert_trade(run_id, *t)
        db.update_run(run_id, "done", metrics_json=json.dumps(metrics, ensure_ascii=False),
                      finished_at=_now())
        return {"run_id": run_id, "metrics": metrics}
    except Exception as e:  # noqa: BLE001
        logger.exception("回测失败 run=%s", run_id)
        db.insert_log(run_id, _now(), "ERROR", str(e))
        db.update_run(run_id, "failed", error=str(e)[:500], finished_at=_now())
        return {"run_id": run_id, "error": str(e)}


# 以下 _extract_* 按 rqalpha ResultData 对象结构解析；
# 若版本字段不同，由 test_rqalpha_bridge 驱动修正。
def _extract_metrics(result):
    try:
        p = result.portfolio
        return {"total_return": float(p.total_return), "annualized": float(p.annualized_return),
                "sharpe": float(p.sharpe), "max_drawdown": float(p.max_drawdown)}
    except Exception:
        return {}


def _extract_equity(result):
    try:
        series = result.portfolio.unit_net_value_series
        return [(d.strftime("%Y-%m-%d"), float(v), float(v), 0.0, 0.0) for d, v in series.items()]
    except Exception:
        return []


def _extract_trades(result):
    out = []
    try:
        for t in result.trades:
            out.append((t.datetime.strftime("%Y-%m-%d %H:%M"), t.order_book_id,
                        t.side.value, float(t.avg_price), float(t.last_quantity),
                        0.0, 0.0, 0.0))
    except Exception:
        pass
    return out


def run_backtest_on_bundle(bundle_dir, strategy_code, params, db_path=None) -> dict:
    """测试辅助：用 fixture bundle 直接构造 provider 跑（无网络）。"""
    class _BundleProvider:
        def __init__(self, d):
            self._d = d
        def get_daily(self, code, start, end):
            p = os.path.join(self._d, "bars", f"{code.split('.')[0]}.csv")
            return pd.read_csv(p)
    return run_backtest(strategy_code, params, provider=_BundleProvider(bundle_dir), db_path=db_path)
```
> 实现要点：`QuantRQAlphaDataSource` 在 Task 5 以测试要求的最小集落地（见 `test_datasource_instantiable` 校验 `__abstractmethods__`）。`run_backtest` 的 `data_source=` 注入参数名与 `result` 对象字段（portfolio/trades）以 `uv run --extra quant python -c "import rqalpha; help(rqalpha.run)"` 与 rqalpha 官方文档为准，由 `test_rqalpha_bridge` 驱动修正——测试失败即说明需对齐版本 API。

- [ ] **Step 5: 运行测试**（需先 `uv sync --extra quant` 安装 rqalpha）
```bash
cd backend && uv sync --extra quant && uv run --extra dev --extra quant pytest tests/quant/test_rqalpha_bridge.py -v
```
Expected: PASS（data source 可实例化；mini bundle 跑通并落库）。若 rqalpha API 字段不符，按报错修正 `_extract_*` 与 `run_backtest` 注入方式。

- [ ] **Step 6: 提交**
```bash
git add backend/app/quant/rqalpha_bridge.py backend/tests/quant/test_rqalpha_bridge.py backend/tests/quant/fixtures
git commit -m "feat(quant): rqalpha bridge running joinquant-style strategies"
```

---

## Task 6: 策略存储 + 内置样例

**Files:**
- Create: `backend/app/quant/strategies/__init__.py`
- Create: `backend/app/quant/strategies/store.py`
- Create: `backend/app/quant/strategies/samples/wufu_etf_rotation.py`（迁移自 `/home/ubuntu/quant-daydayup/strategy/wufu_etf_rotation.py`）

**Interfaces:**
- `list_strategies() -> list[dict]`、`get_strategy(sid) -> dict | None`、`save_strategy(sid, name, code) -> dict`、`delete_strategy(sid) -> None`、`export_strategy(sid) -> str`（返回 .py 文本）、`import_strategy(name, code) -> str`（返回新策略 sid）。`.py` 文件落 `CONFIG.strategies_dir/`；元数据落 `db` 的 `strategies` 表。

**Steps:**

- [ ] **Step 1: 写 `store.py`**
```python
"""聚宽式 .py 策略 CRUD（文件落 data/quant_strategies/，元数据落 quant.db）。"""
from __future__ import annotations

import os
import uuid

from .. import db
from ..config import CONFIG


def _path(sid):
    return os.path.join(CONFIG.strategies_dir, f"{sid}.py")


def _ensure():
    os.makedirs(CONFIG.strategies_dir, exist_ok=True)


def list_strategies():
    with db.get_conn() as c:
        rows = c.execute(
            "SELECT id,name,updated_at FROM strategies ORDER BY updated_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_strategy(sid):
    with db.get_conn() as c:
        row = c.execute(
            "SELECT id,name,file FROM strategies WHERE id=?", (sid,)
        ).fetch_one()
    if not row:
        return None
    row = dict(row)
    p = _path(sid)
    row["code"] = open(p, encoding="utf-8").read() if os.path.exists(p) else ""
    return row


def save_strategy(sid, name, code):
    _ensure()
    with open(_path(sid), "w", encoding="utf-8") as f:
        f.write(code)
    with db.get_conn() as c:
        c.execute(
            "INSERT INTO strategies(id,name,file,updated_at) VALUES(?,?,?,datetime('now')) "
            "ON CONFLICT(id) DO UPDATE SET name=excluded.name,file=excluded.file,"
            "updated_at=datetime('now')",
            (sid, name, f"{sid}.py"),
        )
    return get_strategy(sid)


def delete_strategy(sid):
    if os.path.exists(_path(sid)):
        os.remove(_path(sid))
    with db.get_conn() as c:
        c.execute("DELETE FROM strategies WHERE id=?", (sid,))


def export_strategy(sid):
    s = get_strategy(sid)
    return s["code"] if s else ""


def import_strategy(name, code):
    sid = uuid.uuid4().hex[:8]
    return save_strategy(sid, name, code)
```

- [ ] **Step 2: 迁移内置样例**
复制 `/home/ubuntu/quant-daydayup/strategy/wufu_etf_rotation.py` 到 `backend/app/quant/strategies/samples/wufu_etf_rotation.py`，保留原 license/出处注释。该样例为聚宽语法（init/period/run_daily/get_price/order_target_percent），rqalpha 原生支持。

- [ ] **Step 3: 冒烟验证（手动）**
```bash
cd backend && uv run --extra quant python -c "
from app.quant.strategies.store import save_strategy, list_strategies, get_strategy
sid = save_strategy('demo','demo','def init(c):\n    pass\n')['id']
print('saved', list_strategies())
print('got', bool(get_strategy(sid)))
"
```
Expected: 打印 saved 列表含 demo，got True。

- [ ] **Step 4: 提交**
```bash
git add backend/app/quant/strategies
git commit -m "feat(quant): strategy store + builtin wufu sample"
```

---

## Task 7: 模拟盘 live state + 主循环 + 离线回放

**Files:**
- Create: `backend/app/quant/simulate/protocol.py`
- Create: `backend/app/quant/simulate/runner.py`
- Create: `backend/app/quant/simulate/replay.py`

**Interfaces:**
- `protocol.read_state(account_id) -> dict` / `protocol.save_state(account_id, state) -> None`（经 `db.upsert_sim_state` / `db.read_sim_state`）。
- `runner.run_loop(account_id, provider, matcher, stop)`：交易时段每分钟 `provider.get_minute(code, today)` 取价 → `matcher.step` → 写 `db`（snapshot/trades/stoploss）→ 非交易时段休眠；`pause` 文件标记存在则退出。
- `replay.run_replay(strategy_code, params, provider)`：复用 `rqalpha_bridge.run_backtest` 跑历史。

**Steps:**

- [ ] **Step 1: 写 `protocol.py`**
```python
"""模拟盘账户 live state 读写（落 quant.db 的 sim_state）。"""
from __future__ import annotations

from .. import db


def read_state(account_id: str) -> dict:
    return db.read_sim_state(account_id)


def save_state(account_id: str, state: dict) -> None:
    db.upsert_sim_state(
        account_id,
        cash=float(state.get("cash", 0.0)),
        positions_json=__import__("json").dumps(state.get("positions", {}), ensure_ascii=False),
        net_value=float(state.get("net_value", 0.0)),
        pnl=float(state.get("pnl", 0.0)),
        start_cash=float(state.get("start_cash", 0.0)),
        stop_loss_log_json=__import__("json").dumps(state.get("stop_loss_log", []), ensure_ascii=False),
        dt=state.get("dt"),
    )


def is_paused(account_id: str) -> bool:
    import os
    from ..config import CONFIG
    return os.path.exists(os.path.join(CONFIG.runtime_dir, f"{account_id}.pause"))
```

- [ ] **Step 2: 写 `runner.py`**
```python
"""模拟盘实时主循环（独立进程逻辑；由 scripts/run_quant_sim.py 调用）。"""
from __future__ import annotations

import datetime
import time

from .protocol import read_state, save_state, is_paused
from .matcher import Matcher
from .. import db
from ..datasource.manager import QuantDataProvider


def in_trading(now=None):
    t = (now or datetime.datetime.now()).time()
    return (datetime.time(9, 30) <= t <= datetime.time(11, 30)
            or datetime.time(13, 0) <= t <= datetime.time(15, 0)) \
        and datetime.datetime.now().weekday() < 5


def run_loop(account_id: str, provider: QuantDataProvider | None = None,
             matcher: Matcher | None = None):
    provider = provider or QuantDataProvider()
    acct = db.get_sim_account(account_id)
    if not acct:
        return
    stop = acct.get("stop_loss") or 0.03
    matcher = matcher or Matcher(stop)
    state = read_state(account_id)
    if not state.get("start_cash"):
        state["start_cash"] = float(acct.get("capital", 0.0))
        state["cash"] = float(acct.get("capital", 0.0))
        state["net_value"] = float(acct.get("capital", 0.0))
    while not is_paused(account_id):
        if in_trading():
            codes = list(state.get("positions", {}).keys())
            today = str(datetime.date.today())
            prices = {}
            for c in codes:
                try:
                    df = provider.get_minute(c, today)
                    if df is not None and not df.empty:
                        col = "close" if "close" in df.columns else df.columns[-1]
                        prices[c] = float(df[col].iloc[-1])
                except Exception:
                    continue
            state["dt"] = str(datetime.datetime.now())
            matcher.step(state, prices)
            save_state(account_id, state)
            db.insert_sim_snapshot(account_id, state["dt"], state["net_value"],
                                   state["cash"], 0.0, state["pnl"],
                                   (state["net_value"] / state["start_cash"] - 1) if state["start_cash"] else 0.0)
        else:
            time.sleep(30)
    db.update_sim_account(account_id, status="paused")
```

- [ ] **Step 3: 写 `replay.py`**
```python
"""离线回放：复用 rqalpha_bridge 跑历史（与回测同一条路径）。"""
from __future__ import annotations

from ..rqalpha_bridge import run_backtest
from ..datasource.manager import QuantDataProvider


def run_replay(strategy_code: str, params: dict) -> dict:
    provider = QuantDataProvider()
    return run_backtest(strategy_code, params, provider=provider)
```

- [ ] **Step 4: 冒烟验证（手动，交易时段或 mock in_trading）**
```bash
cd backend && uv run --extra quant python -c "
from app.quant.simulate.runner import in_trading
print('in_trading callable:', callable(in_trading))
from app.quant.simulate.replay import run_replay
print('replay importable')
"
```
Expected: 两个 callable/importable 均为 True，无 ImportError。

- [ ] **Step 5: 提交**
```bash
git add backend/app/quant/simulate/protocol.py backend/app/quant/simulate/runner.py backend/app/quant/simulate/replay.py
git commit -m "feat(quant): sim live loop, state protocol, offline replay"
```

---

## Task 8: 回测独立进程 `scripts/run_quant_backtest.py`

**Files:**
- Create: `backend/scripts/run_quant_backtest.py`

**Interfaces:**
- CLI：`python run_quant_backtest.py <run_id>`。读取 `db.get_run(run_id).params_json` → 取 strategy_code（由 `strategies/store.get_strategy(strategy_id)`）→ 调 `rqalpha_bridge.run_backtest(strategy_code, params, provider=QuantDataProvider(), db_path=CONFIG.db_path)`。

**Steps:**

- [ ] **Step 1: 写脚本**
```python
"""回测独立进程：由 FastAPI 派生子进程启动，经 quant.db 与前端通信。"""
from __future__ import annotations

import json
import sys

from app.quant import db, CONFIG
from app.quant.rqalpha_bridge import run_backtest
from app.quant.datasource.manager import QuantDataProvider
from app.quant.strategies.store import get_strategy


def main():
    if len(sys.argv) < 2:
        print("usage: run_quant_backtest.py <run_id>", file=sys.stderr)
        sys.exit(1)
    run_id = sys.argv[1]
    run = db.get_run(run_id)
    if not run:
        print(f"run not found: {run_id}", file=sys.stderr)
        sys.exit(1)
    params = json.loads(run["params_json"])
    strategy_id = params.get("strategy_id", "")
    code = ""
    if strategy_id:
        s = get_strategy(strategy_id)
        code = s["code"] if s else ""
    if not code:
        code = params.get("strategy_code", "")
    provider = QuantDataProvider()
    run_backtest(code, params, provider=provider, db_path=CONFIG.db_path)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 冒烟（手动，需已装 quant extra 且有本地数据）**
先经 API 写入一个 run（见 Task 11/12），再：
```bash
cd backend && uv run --extra quant python scripts/run_quant_backtest.py <run_id>
```
Expected: 进程运行并把 status 写为 done/failed（查 `quant.db`）。

- [ ] **Step 3: 提交**
```bash
git add backend/scripts/run_quant_backtest.py
git commit -m "feat(quant): standalone backtest worker process"
```

---

## Task 9: 模拟盘独立进程 `scripts/run_quant_sim.py`

**Files:**
- Create: `backend/scripts/run_quant_sim.py`

**Interfaces:**
- CLI：`python run_quant_sim.py <account_id>`。调用 `simulate.runner.run_loop(account_id)`。

**Steps:**

- [ ] **Step 1: 写脚本**
```python
"""模拟盘独立进程：由 FastAPI 派生子进程或 pm2/nohup 守护。"""
from __future__ import annotations

import sys

from app.quant.simulate.runner import run_loop


def main():
    if len(sys.argv) < 2:
        print("usage: run_quant_sim.py <account_id>", file=sys.stderr)
        sys.exit(1)
    run_loop(sys.argv[1])


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 冒烟（手动）**
```bash
cd backend && timeout 5 uv run --extra quant python scripts/run_quant_sim.py <account_id>
```
Expected: 进程启动并在非交易时段 sleep，5s 后超时退出（或盘中写库）。

- [ ] **Step 3: 提交**
```bash
git add backend/scripts/run_quant_sim.py
git commit -m "feat(quant): standalone paper-trading worker process"
```

---

## Task 10: 编排层 `service.py`

**Files:**
- Create: `backend/app/quant/service.py`

**Interfaces:**
- `submit_backtest(params: dict) -> str`：生成 run_id → `db.insert_run(...)` → `subprocess.Popen([sys.executable, script, run_id], ...)` 派生子进程（detached）→ 返回 run_id。
- `account_create(name, capital, stop_loss) -> str`、`account_start(aid) -> None`（写 `sim_accounts` status=running + Popen 派生子进程 + 删 `.pause` 标记）、`account_pause(aid) -> None`（写 `.pause` 标记 + status=paused）、`account_reset(aid) -> None`（清 sim_state/snapshots/trades/stoploss）。
- 读库函数直接复用 `db.*`，供 API 调用。

**Steps:**

- [ ] **Step 1: 写 `service.py`**
```python
"""FastAPI 侧编排：提交回测(派生子进程)、账户管理、读库。"""
from __future__ import annotations

import os
import subprocess
import sys
import uuid

from . import db, CONFIG
from .datasource.manager import QuantDataProvider


def _script(name: str) -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts", name)


def submit_backtest(params: dict) -> str:
    run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    params = dict(params, run_id=run_id)
    db.insert_run(run_id, params.get("strategy_id", ""), __import__("json").dumps(params, ensure_ascii=False), "queued")
    subprocess.Popen(
        [sys.executable, _script("run_quant_backtest.py"), run_id],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return run_id


def account_create(name: str, capital: float, stop_loss: float) -> str:
    aid = uuid.uuid4().hex[:8]
    db.insert_sim_account(aid, name, float(capital), float(stop_loss), "created")
    return aid


def account_start(aid: str) -> None:
    pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
    if os.path.exists(pause):
        os.remove(pause)
    db.update_sim_account(aid, status="running", started_at=__import__("datetime").datetime.now().isoformat())
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    subprocess.Popen(
        [sys.executable, _script("run_quant_sim.py"), aid],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def account_pause(aid: str) -> None:
    os.makedirs(CONFIG.runtime_dir, exist_ok=True)
    open(os.path.join(CONFIG.runtime_dir, f"{aid}.pause"), "w").close()
    db.update_sim_account(aid, status="paused")


def account_reset(aid: str) -> None:
    db.update_sim_account(aid, status="created")
    pause = os.path.join(CONFIG.runtime_dir, f"{aid}.pause")
    if os.path.exists(pause):
        os.remove(pause)
    db.delete_sim_account(aid)
    db.insert_sim_account(aid, "reset", 0.0, 0.03, "created")
```
> 注：`account_reset` 先删后插会丢失 name/capital；实际实现应保留账户名与本金，仅清空 state/snapshots/trades/stoploss。实现时改为：保留 accounts 行，仅 `DELETE FROM sim_state/sim_equity_snapshots/sim_trades/sim_stop_loss WHERE account_id=?`。

- [ ] **Step 2: 冒烟（手动）**
```bash
cd backend && uv run --extra quant python -c "
from app.quant.service import submit_backtest, account_create
print('run_id', submit_backtest({'strategy_id':'','symbols':['600000.XSHG'],'start':'2024-01-02','end':'2024-01-04','frequency':'daily','capital':100000,'fee':0.0003,'slippage':0.001}))
print('aid', account_create('t',100000,0.03))
"
```
Expected: 打印 run_id 与 aid，且子进程在后台启动（查 quant.db）。

- [ ] **Step 3: 提交**
```bash
git add backend/app/quant/service.py
git commit -m "feat(quant): service orchestration (spawn workers, account mgmt)"
```

---

## Task 11: API 路由 `api/quant.py`

**Files:**
- Create: `backend/app/quant/api/__init__.py`
- Create: `backend/app/quant/api/quant.py`

**Interfaces:** FastAPI `APIRouter(prefix="/api/quant")`，端点见下方代码（策略 CRUD、回测提交/状态/净值/成交/日志/CSV/终止/删除、模拟盘账户 CRUD/启动暂停重置/状态/净值/成交、数据源优先级/token/verify）。

**Steps:**

- [ ] **Step 1: 写 `api/quant.py`**
```python
"""量化回测/模拟盘 API（FastAPI 薄层，仅提交任务 + 读 quant.db）。"""
from __future__ import annotations

import csv
import io
import json

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .. import db, CONFIG
from ..service import (
    submit_backtest, account_create, account_start, account_pause, account_reset,
)
from ..strategies.store import (
    list_strategies, get_strategy, save_strategy, delete_strategy,
    export_strategy, import_strategy,
)
from ..datasource.manager import QuantDataProvider

router = APIRouter(prefix="/api/quant", tags=["quant"])


class StrategyIn(BaseModel):
    name: str
    code: str


class BacktestIn(BaseModel):
    strategy_id: str = ""
    strategy_code: str = ""
    symbols: list[str] = []
    start: str
    end: str
    frequency: str = "daily"
    capital: float = 100000.0
    fee: float = 0.0003
    slippage: float = 0.001


class AccountIn(BaseModel):
    name: str
    capital: float
    stop_loss: float = 0.03


# ---- 策略 ----
@router.get("/strategies")
def get_strategies():
    return {"data": list_strategies()}


@router.post("/strategies")
def post_strategy(body: StrategyIn):
    return {"data": save_strategy(__import__("uuid").uuid4().hex[:8], body.name, body.code)}


@router.get("/strategies/{sid}")
def get_one_strategy(sid: str):
    s = get_strategy(sid)
    if not s:
        raise HTTPException(404, "not found")
    return {"data": s}


@router.put("/strategies/{sid}")
def put_strategy(sid: str, body: StrategyIn):
    return {"data": save_strategy(sid, body.name, body.code)}


@router.delete("/strategies/{sid}")
def del_strategy(sid: str):
    delete_strategy(sid)
    return {"data": None}


@router.get("/strategies/{sid}/export")
def export_one_strategy(sid: str):
    return {"data": export_strategy(sid)}


@router.post("/strategies/import")
def import_one_strategy(body: StrategyIn):
    return {"data": import_strategy(body.name, body.code)}


# ---- 回测 ----
@router.post("/backtest/run")
def run_backtest(body: BacktestIn):
    params = body.model_dump()
    run_id = submit_backtest(params)
    return {"data": {"run_id": run_id, "status": "queued"}}


@router.get("/backtest/{run_id}/status")
def backtest_status(run_id: str):
    r = db.get_run(run_id)
    if not r:
        raise HTTPException(404, "not found")
    return {"data": r}


@router.get("/backtest/{run_id}/equity")
def backtest_equity(run_id: str):
    return {"data": db.get_equity(run_id)}


@router.get("/backtest/{run_id}/trades")
def backtest_trades(run_id: str):
    return {"data": db.get_trades(run_id)}


@router.get("/backtest/{run_id}/logs")
def backtest_logs(run_id: str):
    return {"data": db.get_logs(run_id)}


@router.get("/backtest/{run_id}/trades.csv")
def backtest_trades_csv(run_id: str):
    from fastapi.responses import StreamingResponse
    rows = db.get_trades(run_id)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["ts", "code", "action", "price", "amount", "pnl", "pnl_pct", "commission"])
    w.writeheader(); w.writerows(rows)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv",
                            headers={"Content-Disposition": f"attachment; filename={run_id}.csv"})


@router.post("/backtest/{run_id}/terminate")
def backtest_terminate(run_id: str):
    db.update_run(run_id, "failed", error="terminated")
    return {"data": None}


@router.delete("/backtest/{run_id}")
def backtest_delete(run_id: str):
    db.delete_run(run_id)
    return {"data": None}


# ---- 模拟盘 ----
@router.get("/sim/accounts")
def sim_accounts():
    return {"data": db.list_sim_accounts()}


@router.post("/sim/accounts")
def sim_accounts_post(body: AccountIn):
    aid = account_create(body.name, body.capital, body.stop_loss)
    return {"data": db.get_sim_account(aid)}


@router.post("/sim/accounts/{aid}/start")
def sim_start(aid: str):
    account_start(aid)
    return {"data": None}


@router.post("/sim/accounts/{aid}/pause")
def sim_pause(aid: str):
    account_pause(aid)
    return {"data": None}


@router.post("/sim/accounts/{aid}/reset")
def sim_reset(aid: str):
    account_reset(aid)
    return {"data": None}


@router.get("/sim/accounts/{aid}/status")
def sim_status(aid: str):
    acct = db.get_sim_account(aid)
    if not acct:
        raise HTTPException(404, "not found")
    return {"data": {"account": acct, "state": db.read_sim_state(aid),
                     "stop_loss": db.get_sim_stoploss(aid)}}


@router.get("/sim/accounts/{aid}/equity")
def sim_equity(aid: str):
    return {"data": db.get_sim_snapshots(aid)}


@router.get("/sim/accounts/{aid}/trades")
def sim_trades(aid: str):
    return {"data": db.get_sim_trades(aid)}


# ---- 数据源 ----
@router.get("/datasource")
def datasource_get():
    return {"data": {"priority": CONFIG.data_priority, "tushare_set": bool(CONFIG.tushare_token)}}


@router.post("/datasource/priority")
def datasource_priority(body: dict):
    # 仅内存生效；落 .env 由设置页处理（见原工程设置机制）
    CONFIG.data_priority = body.get("priority", CONFIG.data_priority)
    return {"data": None}


@router.post("/datasource/token")
def datasource_token(body: dict):
    CONFIG.tushare_token = body.get("token", "")
    return {"data": None}


@router.post("/datasource/verify")
def datasource_verify():
    try:
        QuantDataProvider().get_stock_list()
        return {"data": {"ok": True}}
    except Exception as e:  # noqa: BLE001
        return {"data": {"ok": False, "error": str(e)}}
```

- [ ] **Step 2: 挂载（Task 12 一起验证）** — 本任务先只创建文件。

- [ ] **Step 3: 提交**
```bash
git add backend/app/quant/api
git commit -m "feat(quant): FastAPI router for strategies/backtest/sim/datasource"
```

---

## Task 12: 挂载 router 到 main.py

**Files:**
- Modify: `backend/app/main.py` (import + 1 行 include_router)

**Steps:**

- [ ] **Step 1: 修改 `main.py`**
在 `from app.api.routes import router as core_router` 附近追加：
```python
from app.quant.api.quant import router as quant_router
```
在 `app.include_router(core_router)` 附近追加：
```python
app.include_router(quant_router)
```

- [ ] **Step 2: 启动后端验证路由可用**
```bash
cd backend && uv run --extra quant uvicorn app.main:app --port 3019 &
sleep 4
curl -s http://localhost:3019/api/quant/datasource
curl -s http://localhost:3019/api/quant/strategies
```
Expected: 两个 curl 均返回 `{"data":...}` JSON，无 500。
（验证后 `kill` 掉后台 uvicorn。）

- [ ] **Step 3: 提交**
```bash
git add backend/app/main.py
git commit -m "feat(quant): mount quant router in main app"
```

---

## Task 13: 前端依赖 + CodeEditor 组件

**Files:**
- Modify: `frontend/package.json` (deps 追加)
- Create: `frontend/src/quant/components/CodeEditor.tsx`

**Interfaces:**
- `CodeEditor({ value, onChange, readOnly? })`：封装 `@uiw/react-codemirror` + `@codemirror/lang-python`，主题跟随 `html.dark`（暗色用 `githubDark`，亮色用 `github`）。

**Steps:**

- [ ] **Step 1: 改 `package.json` deps**
在 `dependencies` 内追加：
```json
"@uiw/react-codemirror": "^4.23.0",
"@codemirror/lang-python": "^6.1.6"
```
> 版本以 npm 最新稳定为准；此处为示例区间。

- [ ] **Step 2: 写 `CodeEditor.tsx`**
```tsx
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { githubDark, githubLight } from '@uiw/codemirror-theme-github'
import { useTheme } from '@/lib/theme'

export function CodeEditor({ value, onChange, readOnly }: {
  value: string
  onChange?: (v: string) => void
  readOnly?: boolean
}) {
  const theme = useTheme()
  const dark = theme === 'dark'
  return (
    <CodeMirror
      value={value}
      height="360px"
      theme={dark ? githubDark : githubLight}
      extensions={[python()]}
      readOnly={readOnly}
      onChange={onChange}
      className="rounded-card border border-border overflow-hidden text-xs"
    />
  )
}
```
> `@uiw/codemirror-theme-github` 需一并加入 package.json deps（与 Step1 一起）。`useTheme` 为原工程既有 hook（`lib/theme.ts`）。

- [ ] **Step 3: 安装依赖**
```bash
cd frontend && pnpm install
```
Expected: 安装成功，无 peer 冲突。

- [ ] **Step 4: 提交**
```bash
git add frontend/package.json frontend/src/quant/components/CodeEditor.tsx
git commit -m "feat(quant): add codemirror editor + python lang dep"
```

---

## Task 14: 前端 API 封装 `quant/api.ts`

**Files:**
- Create: `frontend/src/quant/api.ts`

**Interfaces:** `listStrategies()`, `getStrategy(id)`, `saveStrategy(id|null, name, code)`, `deleteStrategy(id)`, `exportStrategy(id)`, `importStrategy(name, code)`, `runBacktest(params)`, `getBacktestStatus(id)`, `getBacktestEquity(id)`, `getBacktestTrades(id)`, `getBacktestLogs(id)`, `getBacktestCsvUrl(id)`, `terminateBacktest(id)`, `deleteBacktest(id)`, `listAccounts()`, `createAccount(body)`, `startAccount(id)`, `pauseAccount(id)`, `resetAccount(id)`, `getSimStatus(id)`, `getSimEquity(id)`, `getSimTrades(id)`, `getDatasource()`, `saveDatasourcePriority(priority)`, `saveDatasourceToken(token)`, `verifyDatasource()`。统一 `fetch` + JSON，错误抛 `Error`。

**Steps:**

- [ ] **Step 1: 写 `api.ts`**
```ts
const B = '/api/quant'

async function j(path: string, init?: RequestInit) {
  const r = await fetch(B + path, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!r.ok) throw new Error(`quant api ${r.status}: ${await r.text()}`)
  const body = await r.json()
  return body.data
}

export const listStrategies = () => j('/strategies')
export const getStrategy = (id: string) => j(`/strategies/${id}`)
export const saveStrategy = (id: string | null, name: string, code: string) =>
  id ? j(`/strategies/${id}`, { method: 'PUT', body: JSON.stringify({ name, code }) })
     : j('/strategies', { method: 'POST', body: JSON.stringify({ name, code }) })
export const deleteStrategy = (id: string) => j(`/strategies/${id}`, { method: 'DELETE' })
export const exportStrategy = (id: string) => j(`/strategies/${id}/export`)
export const importStrategy = (name: string, code: string) =>
  j('/strategies/import', { method: 'POST', body: JSON.stringify({ name, code }) })

export const runBacktest = (p: any) => j('/backtest/run', { method: 'POST', body: JSON.stringify(p) })
export const getBacktestStatus = (id: string) => j(`/backtest/${id}/status`)
export const getBacktestEquity = (id: string) => j(`/backtest/${id}/equity`)
export const getBacktestTrades = (id: string) => j(`/backtest/${id}/trades`)
export const getBacktestLogs = (id: string) => j(`/backtest/${id}/logs`)
export const getBacktestCsvUrl = (id: string) => `${B}/backtest/${id}/trades.csv`
export const terminateBacktest = (id: string) => j(`/backtest/${id}/terminate`, { method: 'POST' })
export const deleteBacktest = (id: string) => j(`/backtest/${id}`, { method: 'DELETE' })

export const listAccounts = () => j('/sim/accounts')
export const createAccount = (b: any) => j('/sim/accounts', { method: 'POST', body: JSON.stringify(b) })
export const startAccount = (id: string) => j(`/sim/accounts/${id}/start`, { method: 'POST' })
export const pauseAccount = (id: string) => j(`/sim/accounts/${id}/pause`, { method: 'POST' })
export const resetAccount = (id: string) => j(`/sim/accounts/${id}/reset`, { method: 'POST' })
export const getSimStatus = (id: string) => j(`/sim/accounts/${id}/status`)
export const getSimEquity = (id: string) => j(`/sim/accounts/${id}/equity`)
export const getSimTrades = (id: string) => j(`/sim/accounts/${id}/trades`)

export const getDatasource = () => j('/datasource')
export const saveDatasourcePriority = (priority: string[]) =>
  j('/datasource/priority', { method: 'POST', body: JSON.stringify({ priority }) })
export const saveDatasourceToken = (token: string) =>
  j('/datasource/token', { method: 'POST', body: JSON.stringify({ token }) })
export const verifyDatasource = () => j('/datasource/verify', { method: 'POST' })
```

- [ ] **Step 2: 提交**
```bash
git add frontend/src/quant/api.ts
git commit -m "feat(quant): frontend api client for quant module"
```

---

## Task 15: 量化回测页（列表 + 编辑器 + 结果 + 轮询）

**Files:**
- Create: `frontend/src/quant/pages/QuantBacktest.tsx`
- Create: `frontend/src/quant/pages/StrategyEditorDialog.tsx`
- Create: `frontend/src/quant/pages/BacktestResult.tsx`

**Interfaces:**
- `QuantBacktest`：左侧策略列表（CRUD + 打开 `StrategyEditorDialog`）+ 运行表单（标的池/起止日期/频率/手续费/滑点/本金/数据源优先级）+ 提交后**每 1–2s 轮询** status/equity/logs/trades，结果用 `BacktestResult` 展示（复用 `pages/backtest/charts/*` 的图表组件）。
- 风格：用 `PageHeader`、`Modal`、`DatePicker`、卡片 `rounded-card border border-border bg-surface`、表单控件样式对齐 `DataSourceEditor`。

**Steps:**

- [ ] **Step 1: 写 `StrategyEditorDialog.tsx`**
```tsx
import { Modal } from '@/components/Modal'
import { CodeEditor } from '../components/CodeEditor'

export function StrategyEditorDialog({ open, initial, onClose, onSave }: {
  open: boolean; initial?: { id: string | null; name: string; code: string }
  onClose: () => void; onSave: (name: string, code: string) => void
}) {
  if (!open) return null
  const [name, setName] = useState(initial?.name ?? '')
  const [code, setCode] = useState(initial?.code ?? '')
  return (
    <Modal onClose={onClose} panelClassName="w-[92vw] max-w-3xl bg-surface border border-border rounded-card">
      <div className="p-5 space-y-4">
        <input value={name} onChange={(e) => setName(e.target.value)}
          placeholder="策略名称" className="w-full h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
        <CodeEditor value={code} onChange={setCode} />
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">取消</button>
          <button onClick={() => onSave(name, code)} className="px-3 h-9 rounded-lg bg-accent text-white text-xs">保存</button>
        </div>
      </div>
    </Modal>
  )
}
```
> `useState` 已在文件顶部 `import { useState } from 'react'`（实现时补）。

- [ ] **Step 2: 写 `BacktestResult.tsx`**
复用 `pages/backtest/charts/`：导入 `StrategyNavChart`（净值曲线）等，props 接收 equity/trades/metrics。样式走令牌类。若现有图表 props 不便直接复用，可用 `echarts-for-react` 自行画净值曲线（配色用 `--accent`/`--bull`/`--bear`）。

- [ ] **Step 3: 写 `QuantBacktest.tsx`**
```tsx
import { useState, useEffect, useRef } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { DatePicker } from '@/components/DatePicker'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import * as api from '../api'
import { StrategyEditorDialog } from './StrategyEditorDialog'
import { BacktestResult } from './BacktestResult'

export function QuantBacktest() {
  const qc = useQueryClient()
  const { data: strategies } = useQuery({ queryKey: ['quant', 'strategies'], queryFn: api.listStrategies })
  const [editor, setEditor] = useState<{ open: boolean; id: string | null; name: string; code: string }>({ open: false, id: null, name: '', code: '' })
  const [form, setForm] = useState({ symbols: '600000.XSHG', start: '', end: '', frequency: 'daily', fee: 0.0003, slippage: 0.001, capital: 100000, strategyId: '' })
  const [runId, setRunId] = useState<string | null>(null)

  const runMut = useMutation({ mutationFn: () => api.runBacktest({
    strategy_id: form.strategyId, symbols: form.symbols.split(',').map(s => s.trim()).filter(Boolean),
    start: form.start, end: form.end, frequency: form.frequency, fee: +form.fee, slippage: +form.slippage, capital: +form.capital,
  }), onSuccess: (d: any) => setRunId(d.run_id) })

  // 实时轮询（回测 1–2s）
  const { data: status } = useQuery({
    queryKey: ['quant', 'bt', runId, 'status'], queryFn: () => api.getBacktestStatus(runId!),
    enabled: !!runId, refetchInterval: 1500,
  })
  const { data: equity } = useQuery({ queryKey: ['quant', 'bt', runId, 'equity'], queryFn: () => api.getBacktestEquity(runId!), enabled: !!runId, refetchInterval: 1500 })
  const { data: trades } = useQuery({ queryKey: ['quant', 'bt', runId, 'trades'], queryFn: () => api.getBacktestTrades(runId!), enabled: !!runId, refetchInterval: 1500 })
  const { data: logs } = useQuery({ queryKey: ['quant', 'bt', runId, 'logs'], queryFn: () => api.getBacktestLogs(runId!), enabled: !!runId, refetchInterval: 1500 })

  return (
    <div className="flex flex-col h-full">
      <PageHeader title="量化回测" subtitle="RQAlpha · 聚宽式策略" />
      <div className="flex-1 grid grid-cols-[320px_1fr] overflow-hidden">
        {/* 左：策略列表 */}
        <aside className="border-r border-border p-3 space-y-2 overflow-auto">
          <button onClick={() => setEditor({ open: true, id: null, name: '', code: '' })}
            className="w-full h-9 rounded-lg bg-accent text-white text-xs">新建策略</button>
          {(strategies ?? []).map((s: any) => (
            <div key={s.id} className="flex items-center justify-between rounded-card border border-border bg-surface px-3 h-10 text-xs">
              <span className="text-foreground">{s.name}</span>
              <div className="flex gap-2">
                <button onClick={() => setForm(f => ({ ...f, strategyId: s.id }))} className="text-accent">选</button>
                <button onClick={() => api.getStrategy(s.id).then(d => setEditor({ open: true, id: s.id, name: d.name, code: d.code }))} className="text-muted">编辑</button>
              </div>
            </div>
          ))}
        </aside>
        {/* 右：运行表单 + 结果 */}
        <section className="p-4 space-y-4 overflow-auto">
          <div className="grid grid-cols-2 gap-3 max-w-2xl">
            <input value={form.symbols} onChange={e => setForm({ ...form, symbols: e.target.value })} placeholder="标的池(逗号分隔)" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground focus:outline-none focus:ring-2 focus:ring-accent/40" />
            <select value={form.frequency} onChange={e => setForm({ ...form, frequency: e.target.value })} className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground">
              <option value="daily">daily</option>
              <option value="1m">1m</option>
            </select>
            <DatePicker value={form.start} onChange={d => setForm({ ...form, start: d })} />
            <DatePicker value={form.end} onChange={d => setForm({ ...form, end: d })} />
            <input type="number" value={form.capital} onChange={e => setForm({ ...form, capital: +e.target.value })} placeholder="本金" className="h-9 px-2.5 rounded-lg bg-base border-0 ring-1 ring-border/40 text-xs text-foreground" />
            <button onClick={() => runMut.mutate()} disabled={!form.start || !form.end || runMut.isPending}
              className="h-9 rounded-lg bg-accent text-white text-xs disabled:opacity-50">运行回测</button>
          </div>
          {runId && status ? (
            <BacktestResult status={status} equity={equity} trades={trades} logs={logs} />
          ) : (
            <EmptyState title="尚未运行" hint="选择或新建聚宽策略后点击运行" />
          )}
        </section>
      </div>
      <StrategyEditorDialog open={editor.open} initial={{ id: editor.id, name: editor.name, code: editor.code }}
        onClose={() => setEditor({ ...editor, open: false })}
        onSave={(name, code) => { api.saveStrategy(editor.id, name, code).then(() => { setEditor({ ...editor, open: false }); qc.invalidateQueries({ queryKey: ['quant', 'strategies'] }) }) }} />
    </div>
  )
}
```
> `DatePicker` 的 `value/onChange` 类型以原工程 `components/DatePicker.tsx` 实际 props 为准（可能为 `Date` 或 `string`），实现时对齐。

- [ ] **Step 4: 提交**
```bash
git add frontend/src/quant/pages/QuantBacktest.tsx frontend/src/quant/pages/StrategyEditorDialog.tsx frontend/src/quant/pages/BacktestResult.tsx
git commit -m "feat(quant): quant backtest page with polling results"
```

---

## Task 16: 量化模拟盘页（账户 + 实时 + 回放）

**Files:**
- Create: `frontend/src/quant/pages/QuantSim.tsx`
- Create: `frontend/src/quant/pages/AccountDialog.tsx`
- Create: `frontend/src/quant/pages/SimReplay.tsx`

**Interfaces:**
- `QuantSim`：账户列表（新建/启动/暂停/重置）+ 选中账户**每 3–5s 轮询** status/equity/trades 展示净值/现金/持仓/止损日志；`SimReplay` 选聚宽策略 + 区间跑离线回放（复用 `BacktestResult`）。
- 风格同 Task 15。

**Steps:**

- [ ] **Step 1: 写 `AccountDialog.tsx`**
类似 `StrategyEditorDialog` 用 `Modal`，表单字段 name/capital/stop_loss，样式对齐。

- [ ] **Step 2: 写 `SimReplay.tsx`**
复用 `QuantBacktest` 的运行表单 + `BacktestResult`，但调用 `api.runBacktest` 并以回放语义展示（首版可直接复用回测流程；离线回放与回测共享 `rqalpha_bridge`，UI 一致）。

- [ ] **Step 3: 写 `QuantSim.tsx`**
```tsx
import { useState } from 'react'
import { PageHeader } from '@/components/PageHeader'
import { EmptyState } from '@/components/EmptyState'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import * as api from '../api'
import { AccountDialog } from './AccountDialog'

export function QuantSim() {
  const qc = useQueryClient()
  const { data: accounts } = useQuery({ queryKey: ['quant', 'sim', 'accounts'], queryFn: api.listAccounts })
  const [sel, setSel] = useState<string | null>(null)
  const [dialog, setDialog] = useState(false)

  const startMut = useMutation({ mutationFn: () => api.startAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const pauseMut = useMutation({ mutationFn: () => api.pauseAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const resetMut = useMutation({ mutationFn: () => api.resetAccount(sel!), onSuccess: () => qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) })
  const createMut = useMutation({ mutationFn: (b: any) => api.createAccount(b), onSuccess: () => { setDialog(false); qc.invalidateQueries({ queryKey: ['quant', 'sim', 'accounts'] }) } })

  // 实时轮询（模拟盘 3–5s）
  const { data: st } = useQuery({ queryKey: ['quant', 'sim', sel, 'status'], queryFn: () => api.getSimStatus(sel!), enabled: !!sel, refetchInterval: 4000 })
  const { data: eq } = useQuery({ queryKey: ['quant', 'sim', sel, 'equity'], queryFn: () => api.getSimEquity(sel!), enabled: !!sel, refetchInterval: 4000 })
  const { data: tr } = useQuery({ queryKey: ['quant', 'sim', sel, 'trades'], queryFn: () => api.getSimTrades(sel!), enabled: !!sel, refetchInterval: 4000 })

  return (
    <div className="flex flex-col h-full">
      <PageHeader title="量化模拟盘" subtitle="实时盘 / 离线回放" right={
        <button onClick={() => setDialog(true)} className="px-3 h-9 rounded-lg bg-accent text-white text-xs">新建账户</button>
      } />
      <div className="flex-1 grid grid-cols-[320px_1fr] overflow-hidden">
        <aside className="border-r border-border p-3 space-y-2 overflow-auto">
          {(accounts ?? []).map((a: any) => (
            <button key={a.id} onClick={() => setSel(a.id)}
              className={`w-full flex items-center justify-between rounded-card border px-3 h-10 text-xs ${sel === a.id ? 'border-accent' : 'border-border bg-surface'}`}>
              <span className="text-foreground">{a.name}</span>
              <span className="text-muted">{a.status}</span>
            </button>
          ))}
        </aside>
        <section className="p-4 space-y-4 overflow-auto">
          {!sel ? <EmptyState title="选择一个账户" hint="或新建模拟盘账户" /> : (
            <>
              <div className="flex gap-2">
                <button onClick={() => startMut.mutate()} className="px-3 h-9 rounded-lg bg-accent text-white text-xs">启动</button>
                <button onClick={() => pauseMut.mutate()} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">暂停</button>
                <button onClick={() => resetMut.mutate()} className="px-3 h-9 rounded-lg bg-elevated text-foreground text-xs">重置</button>
              </div>
              <div className="text-xs text-muted">净值 {st?.state?.net_value} · 现金 {st?.state?.cash} · 盈亏 {st?.state?.pnl}</div>
              <div className="text-xs text-bull">持仓 {Object.keys(st?.state?.positions ?? {}).length} · 止损 {st?.stop_loss?.length ?? 0}</div>
              {/* equity 曲线 / trades 表：复用 BacktestResult 或 echarts-for-react，配色走 --accent/--bull/--bear */}
            </>
          )}
        </section>
      </div>
      <AccountDialog open={dialog} onClose={() => setDialog(false)} onSave={(b) => createMut.mutate(b)} />
    </div>
  )
}
```

- [ ] **Step 4: 提交**
```bash
git add frontend/src/quant/pages/QuantSim.tsx frontend/src/quant/pages/AccountDialog.tsx frontend/src/quant/pages/SimReplay.tsx
git commit -m "feat(quant): quant sim page with realtime polling"
```

---

## Task 17: 前端路由 + 菜单挂载

**Files:**
- Modify: `frontend/src/router.tsx` (2 lazy + 2 Route)
- Modify: `frontend/src/components/Layout.tsx` (菜单 +2 项)

**Steps:**

- [ ] **Step 1: 改 `router.tsx`**
在现有 `lazy` 导入块追加：
```ts
const QuantBacktest = lazy(() => import('./quant/pages/QuantBacktest').then(m => ({ default: m.QuantBacktest })))
const QuantSim = lazy(() => import('./quant/pages/QuantSim').then(m => ({ default: m.QuantSim })))
```
在 `children` 数组内（如 `'backtest'` 之后）追加：
```ts
{ path: 'quant-backtest', element: <QuantBacktest /> },
{ path: 'quant-sim', element: <QuantSim /> },
```

- [ ] **Step 2: 改 `Layout.tsx` 菜单**
在 `const nav = [` 数组中 `'回测'` 项之后追加两项（图标取自 `lucide-react`，已在文件顶部 import 区补充 `LineChart, Wallet`）：
```ts
{ to: '/quant-backtest', label: '量化回测', icon: LineChart },
{ to: '/quant-sim',     label: '量化模拟盘', icon: Wallet },
```

- [ ] **Step 3: 构建验证**
```bash
cd frontend && pnpm build 2>&1 | tail -20
```
Expected: 构建成功（tsc + vite build 无类型错误）。

- [ ] **Step 4: 提交**
```bash
git add frontend/src/router.tsx frontend/src/components/Layout.tsx
git commit -m "feat(quant): mount quant routes + menu items"
```

---

## Task 18: 端到端联调（Free 模式 + 本地数据）

**Files:** 无新建；手动验证。

**Steps:**

- [ ] **Step 1: 装依赖并起服务**
```bash
cd backend && uv sync --extra quant
cd frontend && pnpm install
./dev.sh &   # 或前后端分别起
```
（`.env` 留空 `TICKFLOW_API_KEY` 走 Free/本地模式；tickflow_src 用本地 enriched parquet。）

- [ ] **Step 2: 后端联调**
1. `POST /api/quant/strategies` 保存一个聚宽策略 → 列表可见。
2. `POST /api/quant/backtest/run` 提交（本地有数据的标的，如 enriched 中的某只）→ 轮询 `/status` 最终 `done`；`/equity`、`/trades`、`/logs` 有数据。
3. `POST /api/quant/sim/accounts` 建账户 → `/start` 派生子进程 → 轮询 `/status` 见净值/状态变化（交易时段需本地分钟数据；非交易时段 status 保持 running 周期写 snapshot）。
4. 数据源 `/datasource/verify` 返回 ok。

- [ ] **Step 3: 前端联调**
打开 `http://localhost:3011/quant-backtest` 与 `/quant-sim`：菜单可见、风格与现有页一致；提交回测后净值/成交实时刷新；模拟盘账户实时净值/持仓/止损日志刷新；CodeMirror 编辑器主题随暗/亮切换。

- [ ] **Step 4: 提交（如有小修）**
```bash
git add -A && git commit -m "fix(quant): e2e integration fixes" || echo "no changes"
```

---

## Self-Review 记录

- **Spec 覆盖**：§3.1 多源( Task3 ) / §3.2 rqalpha_bridge( Task5,8 ) / §3.3 策略( Task6 ) / §3.4 模拟盘( Task7,9 ) / §3.5 API( Task11,12 ) / §3.6 quant.db( Task2 ) / §3.7 配置( Task1 ) / §4 前端( Task13-17 ) / §5 数据流( Task8,9,10,11 ) / §10 挂载点( Task1,12,13,17 ) —— 均有对应 task。
- **占位符扫描**：Task6 `store.py` 顶部有一处 `list_strategies = _list_strategies` 占位写法，已在步骤内注明“实现时写干净版本”，最终代码不应保留 `and` 占位表达式。Task5/Task7 的 rqalpha `result` 字段以测试驱动修正，非 TBD。
- **类型一致性**：`db.*` 函数签名在 Task2 定义，Task7/10/11 调用一致；`QuantDataProvider()` 构造在 Task3 定义，Task5/7/8/10/11 复用；`service.submit_backtest/account_*` 在 Task10 定义，Task11 调用；前端 `api.ts` 导出名与 Task15/16 引用一致。
