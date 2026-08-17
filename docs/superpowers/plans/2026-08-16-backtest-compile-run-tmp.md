# 编译运行数据落 /tmp 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 量化回测「编译运行」的数据（run 行/净值/成交/日志）落系统临时目录（/tmp）独立 SQLite 文件，quant.db 零残留、回测历史查询天然不含编译运行。

**Architecture:** run_id 前缀路由——编译运行 run_id = `c_` + 8 位 hex（主库为 8 位纯 hex，`c_` 含下划线保证零碰撞）。`db.get_conn(run_id)` 按前缀路由到 `tempfile.gettempdir()/quant_compile/{run_id}.db`（懒建 schema）。所有 per-run db 函数把 run_id 传入 get_conn，worker/API/SSE 三进程自动路由，无需跨进程状态。历史查询（list_runs/list_strategies_with_latest）不传 run_id → 恒走主库。SSE rowid 偏移在单文件内单调，协议不变。服务端提交 compile 时清扫 7 天前旧文件与 `quant_bundle/c_*` 目录。

**Tech Stack:** Python 3 / sqlite3 标准库 / FastAPI / React 18 + TS。

## Global Constraints

- 编译运行 run_id 恒为 `c_` + 8 位 hex；非编译运行保持原 8 位纯 hex 逻辑。
- 路由判断唯一依据：`run_id.startswith("c_")`；编译库目录可注入（模块级 `_COMPILE_DIR`，供测试用），默认 `Path(tempfile.gettempdir()) / "quant_compile"`。
- 编译库 schema 仅 `backtest_runs`（含 pid 列）/`backtest_equity`/`backtest_trades`/`backtest_logs` 四表。
- `record` 字段**不得**进入 params_json（不污染 worker 参数）；`BacktestIn.record` 默认 `True` 向后兼容。
- worker 脚本（run_quant_backtest.py）的 bridge `db_path` 参数改用 `db.routed_db_path(run_id)`。
- 测试命令：`cd backend && uv run --extra dev pytest`（从 backend/ 运行）；前端 `cd frontend && pnpm build`（pnpm lint 仓库级预置失败，与本改动无关）。
- 现有全量测试有已知预置失败基线，实现不得引入新失败。

---

### Task 1: db.py 编译库路由

**Files:**
- Modify: `backend/app/quant/db.py`（get_conn 附近，~14-110 行；per-run 函数 ~109-357 行）
- Test: `backend/tests/quant/test_db.py`（追加）

**Interfaces:**
- Produces（Task 2/3 依赖）：
  - `db.compile_dir() -> str`：编译库目录（`_COMPILE_DIR` 覆盖或默认 tempdir/quant_compile）
  - `db.compile_db_path(run_id: str) -> str`：`{compile_dir()}/{run_id}.db`
  - `db.is_compile_run(run_id: str | None) -> bool`：`bool(run_id and run_id.startswith("c_"))`
  - `db.routed_db_path(run_id: str) -> str`：compile → 编译库文件；否则主库 `_DB_PATH or CONFIG.db_path`
  - `db.get_conn(run_id: str | None = None)`：compile → 懒建编译库（mkdir + executescript 四表 schema）；否则主库。**现有无 run_id 调用行为不变。**

- [ ] **Step 1: 写失败测试**

在 `backend/tests/quant/test_db.py` 追加：

```python
def test_compile_run_routing(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    p = _fresh()
    db.insert_run("c_12345678", "s1", "", '{"a":1}', "queued")
    db.insert_run("main1234", "s1", "", '{"a":1}', "queued")
    assert db.get_run("c_12345678")["status"] == "queued"
    assert db.get_run("main1234")["status"] == "queued"
    with db.get_conn() as c:
        ids = [x["id"] for x in c.execute("SELECT id FROM backtest_runs").fetchall()]
    assert ids == ["main1234"]
    assert db.routed_db_path("c_12345678").endswith("compile/c_12345678.db")
    assert not db.is_compile_run("main1234")
    assert db.is_compile_run("c_12345678")


def test_compile_run_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    p = _fresh()
    rid = "c_12345678"
    db.insert_run(rid, "s1", "", "{}", "queued")
    db.bulk_insert_equity(rid, [("2024-01-02", 1.0, 1.0, 0.9, 0.1)])
    db.insert_trade(rid, "2024-01-02 09:30", "600000.XSHG", "BUY", 10.0, 100, 0.0, 0.0, 0.0)
    db.insert_log(rid, "2024-01-02 09:30", "INFO", "start")
    db.update_run(rid, "done", metrics_json='{"sharpe":1.2}')
    assert len(db.get_equity(rid)) == 1
    assert len(db.get_trades(rid)) == 1
    assert len(db.get_logs(rid)) == 1
    assert db.get_max_log_id(rid) == 1
    assert len(db.get_logs_after(rid, 0)) == 1
    with db.get_conn() as c:
        for t in ("backtest_runs", "backtest_equity", "backtest_trades", "backtest_logs"):
            n = c.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
            assert n == 0, f"主库 {t} 应零残留"
    db.delete_run(rid)
    assert db.get_run(rid) is None
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_db.py::test_compile_run_routing -q`
Expected: FAIL（`db._COMPILE_DIR` 不存在 → AttributeError；`routed_db_path` 不存在 → AttributeError）

- [ ] **Step 3: 实现路由**

`backend/app/quant/db.py` 顶部加 imports（现有 `import json, os, sqlite3`）：

```python
import tempfile
from pathlib import Path
```

`_DB_PATH` 声明后追加：

```python
_COMPILE_DIR: str | None = None

_COMPILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS backtest_runs (
    id TEXT PRIMARY KEY, strategy_id TEXT, name TEXT, params_json TEXT, status TEXT,
    metrics_json TEXT, created_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT, error TEXT, pid INTEGER);
CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id TEXT, dt TEXT, value REAL, benchmark REAL, cash REAL, positions_value REAL);
CREATE TABLE IF NOT EXISTS backtest_trades (
    run_id TEXT, ts TEXT, code TEXT, action TEXT, price REAL, amount REAL,
    pnl REAL, pnl_pct REAL, commission REAL);
CREATE TABLE IF NOT EXISTS backtest_logs (
    run_id TEXT, ts TEXT, level TEXT, message TEXT);
"""
```

替换 `get_conn()`（原 100-105 行）为：

```python
def compile_dir() -> str:
    if _COMPILE_DIR:
        return _COMPILE_DIR
    return str(Path(tempfile.gettempdir()) / "quant_compile")


def compile_db_path(run_id: str) -> str:
    return os.path.join(compile_dir(), f"{run_id}.db")


def is_compile_run(run_id: str | None) -> bool:
    return bool(run_id and run_id.startswith("c_"))


def routed_db_path(run_id: str) -> str:
    if is_compile_run(run_id):
        return compile_db_path(run_id)
    return _DB_PATH or CONFIG.db_path


def _ensure_compile_db(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_COMPILE_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def get_conn(run_id: str | None = None):
    if is_compile_run(run_id):
        path = compile_db_path(run_id)
        _ensure_compile_db(path)
    else:
        path = _DB_PATH or CONFIG.db_path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn
```

**注意：`_COMPILE_SCHEMA` 的 backtest_runs 直接含 pid 列**（新文件，无需迁移）；主库 init_db 的迁移逻辑不动。

- [ ] **Step 4: 所有 per-run 函数传入 run_id**

以下函数把 `with get_conn() as c` 改为 `with get_conn(run_id) as c`（函数第一参数即 run_id，直接引用；`get_logs_tail` 内有两处 `get_conn()` 都要改；`delete_run` 的 `get_conn()` 一处；其余每函数一处）：

`insert_run`(109)、`upsert_run`(117)、`update_run`(129)、`set_run_pid`(137)、`get_run`(143)、`get_max_equity_id`(222)、`get_equity`(230)、`insert_trade`(239)、`get_trades`(248)、`get_trades_after`(257)、`get_max_trade_id`(268)、`insert_log`(276)、`get_logs`(284)、`get_logs_tail`(293，两处)、`get_logs_before`(313)、`get_logs_after`(329)、`get_max_log_id`(340)、`delete_run`(348)、`bulk_insert_equity`(192)、`insert_equity_row`(201)。

模拟盘函数（`insert_sim_account` 及之后）**不动**。

- [ ] **Step 5: 跑新增测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_db.py -q`
Expected: 全部 PASS（含 2 个新测试与既有测试）

- [ ] **Step 6: 全量测试回归**

Run: `cd backend && uv run --extra dev pytest -q`
Expected: 无新增失败（与改动前基线一致；quant 相关全部 PASS）

- [ ] **Step 7: Commit**

```bash
git add backend/app/quant/db.py backend/tests/quant/test_db.py
git commit -m "feat(quant): db 按 run_id 前缀路由编译运行到 /tmp 独立 SQLite"
```

### Task 2: service.py 编译提交 + 旧文件清扫

**Files:**
- Modify: `backend/app/quant/service.py`（submit_backtest ~41-66 行；imports 头部）
- Test: `backend/tests/quant/test_service.py`（追加）

**Interfaces:**
- Consumes: Task 1 的 `db.compile_dir()` / `db.is_compile_run` 路由。
- Produces（Task 3 依赖）：`service.submit_backtest(params: dict, compile_mode: bool = False) -> str`——compile_mode 时 run_id 强制 `f"c_{uuid.uuid4().hex[:8]}"`（覆盖 params.run_id）并执行清扫；否则行为与现一致。

- [ ] **Step 1: 写失败测试**

`backend/tests/quant/test_service.py` 追加（文件头加 `import time`）：

```python
def test_submit_backtest_compile_mode(tmp_quant, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    monkeypatch.setattr(subprocess_module(), "Popen", lambda *a, **k: None)
    params = {"strategy_id": "", "symbols": ["600000.XSHG"], "start": "2024-01-02",
              "end": "2024-01-04", "frequency": "daily"}
    run_id = service.submit_backtest(params, compile_mode=True)
    assert run_id.startswith("c_")
    assert db.get_run(run_id)["status"] == "queued"
    with db.get_conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM backtest_runs").fetchone()["n"]
        assert n == 0, "主库不应有 compile 行"
    rid2 = service.submit_backtest(params)
    assert not rid2.startswith("c_")
    with db.get_conn() as c:
        n = c.execute("SELECT COUNT(*) AS n FROM backtest_runs").fetchone()["n"]
        assert n == 1


def test_sweep_compile_stale(tmp_quant, monkeypatch):
    monkeypatch.setattr(db, "_COMPILE_DIR", str(tmp_path / "compile"))
    d = db.compile_dir()
    os.makedirs(d, exist_ok=True)
    old = os.path.join(d, "c_00000001.db")
    new = os.path.join(d, "c_00000002.db")
    open(old, "w").close()
    open(new, "w").close()
    os.utime(old, (time.time() - 8 * 86400, time.time() - 8 * 86400))
    service._sweep_compile_stale()
    assert not os.path.exists(old)
    assert os.path.exists(new)
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_service.py::test_submit_backtest_compile_mode tests/quant/test_service.py::test_sweep_compile_stale -q`
Expected: FAIL（`submit_backtest` 无 compile_mode 参数 → TypeError；`_sweep_compile_stale` 不存在 → AttributeError）

- [ ] **Step 3: 实现**

`backend/app/quant/service.py` 头部 imports（现有 `import json, os, signal, subprocess, sys, uuid` + `import datetime`）追加：

```python
import glob
import shutil
import time
```

`submit_backtest` 替换为：

```python
def submit_backtest(params: dict, compile_mode: bool = False) -> str:
    if compile_mode:
        run_id = f"c_{uuid.uuid4().hex[:8]}"
        _sweep_compile_stale()
    else:
        run_id = params.get("run_id") or uuid.uuid4().hex[:8]
    params = dict(params, run_id=run_id)
    db.insert_run(
        run_id,
        params.get("strategy_id", ""),
        params.get("name", ""),
        json.dumps(params, ensure_ascii=False),
        "queued",
    )
    try:
        proc = subprocess.Popen(
            [sys.executable, _script("run_quant_backtest.py"), run_id],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as e:
        # M5：Popen 失败不留永久 queued 行
        db.update_run(run_id, "failed", error=f"spawn failed: {e}")
        raise
    # M5：pid 落库，terminate 时按 pid 杀进程组（Popen 被 patch 时容错）
    pid = getattr(proc, "pid", None)
    if pid:
        db.set_run_pid(run_id, pid)
    return run_id


def _sweep_compile_stale(max_age_days: int = 7) -> None:
    """清扫 7 天前的编译库 .db 文件与编译 bundle 目录（quant_bundle/c_*）。"""
    cutoff = time.time() - max_age_days * 86400
    for f in glob.glob(os.path.join(db.compile_dir(), "*.db")):
        try:
            if os.path.getmtime(f) < cutoff:
                os.unlink(f)
        except OSError:
            pass
    for d in glob.glob(os.path.join(CONFIG.bundle_dir, "c_*")):
        try:
            if os.path.getmtime(d) < cutoff:
                shutil.rmtree(d, ignore_errors=True)
        except OSError:
            pass
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_service.py -q`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/service.py backend/tests/quant/test_service.py
git commit -m "feat(quant): 编译运行强制 c_ 前缀 run_id + 7 天旧文件清扫"
```

### Task 3: API record 透传 + worker 路由 db_path

**Files:**
- Modify: `backend/app/quant/api/quant.py`（BacktestIn ~31-42 行；run_backtest ~102-111 行）
- Modify: `backend/scripts/run_quant_backtest.py`（82、86 行）
- Test: `backend/tests/quant/test_api_quant.py`（fixture ~32-41 行 + 追加测试）、`backend/tests/quant/test_run_quant_backtest.py`（追加）

**Interfaces:**
- Consumes: Task 1 `db.routed_db_path`；Task 2 `submit_backtest(params, compile_mode)`。
- Produces: `POST /api/quant/backtest/run` 接受 `record: bool = True`；`record=False` → compile 运行（run_id 以 `c_` 开头，历史查询不可见）；worker 对 compile 运行向 bridge 传编译库路径。

- [ ] **Step 1: 更新测试 fixture + 写失败测试**

`backend/tests/quant/test_api_quant.py` fixture 内 patch 的 `submit_backtest` lambda（32-41 行）改为接受 compile_mode：

```python
    monkeypatch.setattr(
        service, "submit_backtest",
        lambda params, compile_mode=False: db.insert_run(
            ("c_" if compile_mode else "") + (params.get("run_id") or "run123"),
            params.get("strategy_id", ""),
            params.get("name", ""),
            __import__("json").dumps(params, ensure_ascii=False),
            "queued",
        ) or ("c_" if compile_mode else "") + (params.get("run_id") or "run123"),
    )
```

并在 fixture 内、`db.init_db()` 之后加一行（隔离编译库目录，避免写真实 /tmp）：

```python
    db._COMPILE_DIR = str(tmp_path / "compile")
```

`test_api_quant.py` 文件尾追加：

```python
def test_backtest_record_flag(client):
    r = client.post("/api/quant/backtest/run",
                    json={"name": "n", "strategy_id": "s1", "start": "2024-01-02",
                          "end": "2024-01-03", "record": False})
    assert r.status_code == 200
    assert r.json()["data"]["run_id"].startswith("c_")
    r2 = client.post("/api/quant/backtest/run",
                     json={"name": "n", "strategy_id": "s1", "start": "2024-01-02",
                           "end": "2024-01-03"})
    assert r2.status_code == 200
    assert not r2.json()["data"]["run_id"].startswith("c_")
    rows = client.get("/api/quant/backtest/runs").json()["data"]
    assert all(not x["id"].startswith("c_") for x in rows)


def test_backtest_record_not_in_params_json(client):
    r = client.post("/api/quant/backtest/run",
                    json={"name": "n", "strategy_id": "s1", "record": False})
    row = db.get_run(r.json()["data"]["run_id"])
    assert "record" not in (row["params_json"] or "")


def test_worker_routes_compile_db_path(monkeypatch, tmp_path):
    import importlib.util
    path = "scripts/run_quant_backtest.py"
    spec = importlib.util.spec_from_file_location("run_quant_backtest_c", path)
    rb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rb)
    params = {"strategy_id": "", "run_id": "c_12345678", "start": "2020-01-01",
              "end": "2020-02-01", "symbols": ["600000.XSHG"]}
    captured = {}

    def fake_get_run(run_id):
        return {"params_json": __import__("json").dumps(params)}

    def fake_run_backtest(code, params, provider=None, db_path=None):
        captured["db_path"] = db_path
        return {"run_id": "c_12345678"}

    monkeypatch.setattr(rb.db, "get_run", fake_get_run)
    monkeypatch.setattr(rb.db, "_COMPILE_DIR", str(tmp_path / "compile"))
    monkeypatch.setattr(rb, "run_backtest", fake_run_backtest)
    old = sys.argv
    sys.argv = ["run_quant_backtest.py", "c_12345678"]
    try:
        rb.main()
    finally:
        sys.argv = old
    assert captured["db_path"].endswith("c_12345678.db")
```

注意 `test_api_quant.py` 顶部需 `import sys`（检查现有 imports；无则加）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_api_quant.py tests/quant/test_run_quant_backtest.py -q`
Expected: FAIL（`record` 参数 422 或未生效；`captured["db_path"]` 指向主库路径）

- [ ] **Step 3: 实现**

`backend/app/quant/api/quant.py` BacktestIn 末尾追加字段：

```python
    record: bool = True
```

`run_backtest` 替换为：

```python
@router.post("/backtest/run")
def run_backtest(body: BacktestIn):
    params = body.model_dump()
    # record 仅控制落库位置（compile → /tmp 独立库），不下发 worker、不写 params_json
    params.pop("record")
    # 空日期不下发（前端「编译运行」可不选日期）：键存在但为空串会让
    # 原生 rqalpha 路径把 "" 当日期解析，桥接层拿不到键时才会走默认窗口
    params = {k: v for k, v in params.items() if k not in ("start", "end") or str(v).strip()}
    # frequency 显式透传给桥接层（rqalpha_bridge 侧消费，按其支持的取值执行）
    params["frequency"] = body.frequency or "daily"
    run_id = submit_backtest(params, compile_mode=not body.record)
    return {"data": {"run_id": run_id, "status": "queued"}}
```

`backend/scripts/run_quant_backtest.py` 82、86 行两处 `db_path=CONFIG.db_path` 改为 `db_path=db.routed_db_path(run_id)`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cd backend && uv run --extra dev pytest tests/quant/test_api_quant.py tests/quant/test_run_quant_backtest.py tests/quant/test_db.py tests/quant/test_service.py -q`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add backend/app/quant/api/quant.py backend/scripts/run_quant_backtest.py backend/tests/quant/test_api_quant.py backend/tests/quant/test_run_quant_backtest.py
git commit -m "feat(quant): /backtest/run 支持 record 标记 + worker 按 run_id 路由 db_path"
```

### Task 4: 前端编译运行传 record=false + 全量验证

**Files:**
- Modify: `frontend/src/quant/pages/QuantBacktest.tsx`（runMut payload ~293-301 行）

**Interfaces:**
- Consumes: Task 3 的 `record` 字段（`POST /api/quant/backtest/run`）。

- [ ] **Step 1: 编译运行 payload 加 record=false**

`QuantBacktest.tsx` runMut 的 payload 构造处（`const payload: any = {...}` 之后、`if (start)` 之前）插入：

```tsx
      if (short) payload.record = false
```

「开始回测」不加（后端默认 record=true）。

- [ ] **Step 2: 前端构建验证**

Run: `cd frontend && pnpm build`
Expected: 成功（tsc + vite，无 error）

- [ ] **Step 3: 后端全量回归**

Run: `cd backend && uv run --extra dev pytest -q`
Expected: 无新增失败（与改动前基线一致）

- [ ] **Step 4: Commit**

```bash
git add frontend/src/quant/pages/QuantBacktest.tsx
git commit -m "feat(backtest): 编译运行标记 record=false，不记录进回测历史"
```
